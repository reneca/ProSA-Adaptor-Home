//! Read-only EcoFlow STREAM metrics through the IoT Open HTTP API.

use std::{
    collections::{BTreeSet, VecDeque},
    convert::Infallible,
    fmt::Write as _,
    io,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use hmac::{Hmac, Mac as _};
use http::{Method, Request, Response, StatusCode, request};
use http_body_util::{BodyExt as _, Limited, combinators::BoxBody};
use hyper::body::Incoming;
use prosa::{
    core::{adaptor::Adaptor, proc::ProcConfig as _},
    otel::KeyValue,
};
use prosa_fetcher::{
    adaptor::FetcherAdaptor,
    proc::{FetchAction, FetcherError, FetcherProc},
};
use serde::Deserialize;
use serde_json::{Map, Value};
use sha2::Sha256;
use tokio::sync::watch;

const MAX_RESPONSE_BYTES: usize = 256 * 1024;
/// Serial number prefixes of STREAM models that report system telemetry through the Developer API:
/// Ultra, Pro, AC Pro, Max, AC and Ultra X.
const STREAM_SERIAL_PREFIXES: [&str; 6] = ["BK11", "BK12", "BK31", "BK41", "BK51", "BK61"];
/// Metric type, power, voltage and current quota keys of each solar input.
const SOLAR_INPUT_KEYS: [(&str, &str, &str, &str); 4] = [
    ("solar_1", "powGetPv", "plugInInfoPvVol", "plugInInfoPvAmp"),
    (
        "solar_2",
        "powGetPv2",
        "plugInInfoPv2Vol",
        "plugInInfoPv2Amp",
    ),
    (
        "solar_3",
        "powGetPv3",
        "plugInInfoPv3Vol",
        "plugInInfoPv3Amp",
    ),
    (
        "solar_4",
        "powGetPv4",
        "plugInInfoPv4Vol",
        "plugInInfoPv4Amp",
    ),
];

#[derive(Debug, Default, Copy, Clone, PartialEq)]
enum EcoFlowFetchState {
    #[default]
    DeviceList,
    MainDevice,
    Quota,
}

#[derive(Debug, Deserialize)]
struct EcoFlowApiResponse {
    code: Value,
    #[serde(default)]
    message: String,
    #[serde(default)]
    data: Value,
}

impl EcoFlowApiResponse {
    fn into_data(self) -> Result<Value, String> {
        let success = self.code.as_str() == Some("0") || self.code.as_i64() == Some(0);
        if success {
            Ok(self.data)
        } else {
            Err(format!("EcoFlow API error {}: {}", self.code, self.message))
        }
    }
}

#[derive(Debug, Default, Copy, Clone, PartialEq)]
struct SolarInput {
    power: Option<f64>,
    voltage: Option<f64>,
    current: Option<f64>,
}

impl SolarInput {
    fn from_data(
        data: &Map<String, Value>,
        (_, power_key, voltage_key, current_key): (&str, &str, &str, &str),
    ) -> Result<Self, String> {
        let voltage = number(data, voltage_key)?;
        let current = number(data, current_key)?;
        // Firmware 1.0.1.41 and later reports 0 for per-input power and only exposes voltage and current.
        let power = match number(data, power_key)? {
            Some(power) if power != 0.0 => Some(power),
            power => voltage
                .zip(current)
                .map(|(voltage, current)| voltage * current)
                .or(power),
        };
        Ok(Self {
            power,
            voltage,
            current,
        })
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
struct EcoFlowSnapshot {
    serial_number: String,
    battery_charge: f64,
    solar_power: Option<f64>,
    solar_inputs: [SolarInput; 4],
    battery_power: Option<f64>,
    system_load_power: Option<f64>,
    load_from_solar_power: Option<f64>,
    load_from_battery_power: Option<f64>,
    load_from_grid_power: Option<f64>,
    system_grid_power: Option<f64>,
    grid_connection_power: Option<f64>,
    ac_output_power: Option<f64>,
    ac_outlet_1_power: Option<f64>,
    ac_outlet_2_power: Option<f64>,
}

impl EcoFlowSnapshot {
    fn from_data(serial_number: String, data: &Value) -> Result<Self, String> {
        let data = data
            .as_object()
            .ok_or("EcoFlow quota data must be an object")?;
        let battery_charge =
            number(data, "cmsBattSoc")?.ok_or("EcoFlow quota is missing cmsBattSoc")?;
        if !(0.0..=100.0).contains(&battery_charge) {
            return Err("EcoFlow cmsBattSoc must be between 0 and 100".into());
        }
        let mut solar_inputs = [SolarInput::default(); 4];
        for (solar_input, keys) in solar_inputs.iter_mut().zip(SOLAR_INPUT_KEYS) {
            *solar_input = SolarInput::from_data(data, keys)?;
        }

        Ok(Self {
            serial_number,
            battery_charge,
            solar_power: number(data, "powGetPvSum")?,
            solar_inputs,
            battery_power: number(data, "powGetBpCms")?,
            system_load_power: number(data, "powGetSysLoad")?,
            load_from_solar_power: number(data, "powGetSysLoadFromPv")?,
            load_from_battery_power: number(data, "powGetSysLoadFromBp")?,
            load_from_grid_power: number(data, "powGetSysLoadFromGrid")?,
            system_grid_power: number(data, "powGetSysGrid")?,
            grid_connection_power: number(data, "gridConnectionPower")?,
            ac_output_power: number(data, "acTotalActivePower")?,
            ac_outlet_1_power: number(data, "powGetSchuko1")?,
            ac_outlet_2_power: number(data, "powGetSchuko2")?,
        })
    }

    fn powers(&self) -> impl Iterator<Item = (&'static str, f64)> {
        [
            ("solar", self.solar_power),
            ("battery", self.battery_power),
            ("system_load", self.system_load_power),
            ("load_from_solar", self.load_from_solar_power),
            ("load_from_battery", self.load_from_battery_power),
            ("load_from_grid", self.load_from_grid_power),
            ("system_grid", self.system_grid_power),
            ("grid_connection", self.grid_connection_power),
            ("ac_output", self.ac_output_power),
            ("ac_outlet_1", self.ac_outlet_1_power),
            ("ac_outlet_2", self.ac_outlet_2_power),
        ]
        .into_iter()
        .filter_map(|(power_type, value)| value.map(|value| (power_type, value)))
        .chain(self.solar_values(|input| input.power))
    }

    fn solar_values(
        &self,
        value: fn(&SolarInput) -> Option<f64>,
    ) -> impl Iterator<Item = (&'static str, f64)> {
        SOLAR_INPUT_KEYS.iter().zip(&self.solar_inputs).filter_map(
            move |((input_type, ..), input)| value(input).map(|value| (*input_type, value)),
        )
    }
}

fn number(data: &Map<String, Value>, key: &str) -> Result<Option<f64>, String> {
    let Some(value) = data.get(key) else {
        return Ok(None);
    };
    let value = match value {
        Value::Null => return Ok(None),
        Value::Number(value) => value.as_f64(),
        Value::String(value) if value.is_empty() => return Ok(None),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
    .filter(|value: &f64| value.is_finite())
    .ok_or_else(|| format!("EcoFlow quota {key} must be a finite number"))?;
    Ok(Some(value))
}

fn valid_serial_number(serial_number: &str) -> bool {
    (8..=64).contains(&serial_number.len())
        && serial_number
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric())
}

fn stream_serials(data: &Value) -> Result<VecDeque<String>, String> {
    let devices = data
        .as_array()
        .ok_or("EcoFlow device list data must be an array")?;
    let mut serial_numbers = BTreeSet::new();
    for device in devices {
        let Some(serial_number) = device.get("sn").and_then(Value::as_str) else {
            continue;
        };
        if STREAM_SERIAL_PREFIXES
            .iter()
            .any(|prefix| serial_number.starts_with(prefix))
        {
            if !valid_serial_number(serial_number) {
                return Err("EcoFlow returned an invalid STREAM serial number".into());
            }
            serial_numbers.insert(serial_number.to_owned());
        }
    }
    if serial_numbers.is_empty() {
        return Err("No directly bound EcoFlow STREAM device was found".into());
    }
    Ok(serial_numbers.into_iter().collect())
}

fn main_serial_number(data: &Value) -> Result<String, String> {
    let serial_number = data
        .get("sn")
        .and_then(Value::as_str)
        .ok_or("EcoFlow main-device response is missing sn")?;
    if !valid_serial_number(serial_number) {
        return Err("EcoFlow returned an invalid main-device serial number".into());
    }
    Ok(serial_number.to_owned())
}

fn signature(
    secret_key: &[u8],
    parameters: &str,
    access_key: &str,
    nonce: u32,
    timestamp: u128,
) -> Result<String, String> {
    let authentication = format!("accessKey={access_key}&nonce={nonce}&timestamp={timestamp}");
    let input = if parameters.is_empty() {
        authentication
    } else {
        format!("{parameters}&{authentication}")
    };
    let mut mac = Hmac::<Sha256>::new_from_slice(secret_key)
        .map_err(|_| "Invalid EcoFlow secret key".to_string())?;
    mac.update(input.as_bytes());
    let mut output = String::with_capacity(64);
    for byte in mac.finalize().into_bytes() {
        write!(&mut output, "{byte:02x}").expect("writing to a String cannot fail");
    }
    Ok(output)
}

fn timestamp_millis() -> Result<u128, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .map_err(|_| "System time is before the Unix epoch".into())
}

/// Fetches telemetry for one directly bound EcoFlow STREAM system.
#[derive(Adaptor)]
pub struct FetcherEcoFlowAdaptor {
    access_key: String,
    secret_key: Vec<u8>,
    state: EcoFlowFetchState,
    pending_serial_numbers: VecDeque<String>,
    main_serial_numbers: BTreeSet<String>,
    main_serial_number: Option<String>,
    snapshot: watch::Sender<Option<EcoFlowSnapshot>>,
}

impl FetcherEcoFlowAdaptor {
    fn signed_request<M: Send>(
        &self,
        mut request_builder: request::Builder,
        uri: String,
        parameters: &str,
    ) -> Result<Request<BoxBody<Bytes, Infallible>>, FetcherError<M>> {
        let nonce = rand::random_range(100_000..=999_999);
        let timestamp = timestamp_millis().map_err(FetcherError::Other)?;
        let sign = signature(
            &self.secret_key,
            parameters,
            &self.access_key,
            nonce,
            timestamp,
        )
        .map_err(FetcherError::Other)?;
        request_builder = request_builder
            .method(Method::GET)
            .uri(uri)
            .header(http::header::ACCEPT, "application/json")
            .header("accessKey", &self.access_key)
            .header("nonce", nonce)
            .header("timestamp", timestamp.to_string())
            .header("sign", sign);
        Ok(request_builder.body(BoxBody::default())?)
    }

    fn process_data<M: Send>(&mut self, data: Value) -> Result<FetchAction<M>, FetcherError<M>> {
        match self.state {
            EcoFlowFetchState::DeviceList => {
                self.pending_serial_numbers = stream_serials(&data).map_err(FetcherError::Other)?;
                self.main_serial_numbers.clear();
                self.state = EcoFlowFetchState::MainDevice;
                Ok(FetchAction::Http)
            }
            EcoFlowFetchState::MainDevice => {
                self.pending_serial_numbers.pop_front().ok_or_else(|| {
                    FetcherError::Other("No pending EcoFlow serial number".into())
                })?;
                self.main_serial_numbers
                    .insert(main_serial_number(&data).map_err(FetcherError::Other)?);
                if self.pending_serial_numbers.is_empty() {
                    if self.main_serial_numbers.len() != 1 {
                        return Err(FetcherError::Other(
                            "Multiple independent EcoFlow STREAM systems were found".into(),
                        ));
                    }
                    self.main_serial_number = self.main_serial_numbers.first().cloned();
                    self.state = EcoFlowFetchState::Quota;
                }
                Ok(FetchAction::Http)
            }
            EcoFlowFetchState::Quota => {
                let serial_number = self.main_serial_number.clone().ok_or_else(|| {
                    FetcherError::Other("EcoFlow main serial number is unavailable".into())
                })?;
                let snapshot = EcoFlowSnapshot::from_data(serial_number, &data)
                    .map_err(FetcherError::Other)?;
                let _ = self.snapshot.send(Some(snapshot));
                Ok(FetchAction::None)
            }
        }
    }
}

impl<M> FetcherAdaptor<M> for FetcherEcoFlowAdaptor
where
    M: 'static + Send + Sync + Sized + Clone + std::fmt::Debug + prosa::core::msg::Tvf + Default,
{
    fn new(proc: &FetcherProc<M>) -> Result<Self, FetcherError<M>> {
        if proc.settings.authorization {
            return Err(FetcherError::Other(
                "EcoFlow configuration must set authorization to false".into(),
            ));
        }
        let access_key = proc
            .settings
            .username()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| FetcherError::Other("Missing EcoFlow access key".into()))?
            .to_owned();
        let secret_key = proc
            .settings
            .password()?
            .filter(|value| !value.is_empty())
            .ok_or_else(|| FetcherError::Other("Missing EcoFlow secret key".into()))?;
        let (snapshot, watch_snapshot) = watch::channel(None::<EcoFlowSnapshot>);

        let battery = watch_snapshot.clone();
        let _battery_charge = proc
            .get_proc_param()
            .meter("ecoflow")
            .f64_observable_gauge("prosa_ecoflow_battery_charge")
            .with_description("EcoFlow STREAM system battery charge")
            .with_unit("%")
            .with_callback(move |observer| {
                if let Some(snapshot) = battery.borrow().as_ref() {
                    observer.observe(
                        snapshot.battery_charge,
                        &[KeyValue::new("sn", snapshot.serial_number.clone())],
                    );
                }
            })
            .build();

        let power = watch_snapshot.clone();
        let _power = proc
            .get_proc_param()
            .meter("ecoflow")
            .f64_observable_gauge("prosa_ecoflow_power")
            .with_description("EcoFlow STREAM instantaneous power")
            .with_unit("W")
            .with_callback(move |observer| {
                if let Some(snapshot) = power.borrow().as_ref() {
                    for (power_type, value) in snapshot.powers() {
                        observer.observe(
                            value,
                            &[
                                KeyValue::new("sn", snapshot.serial_number.clone()),
                                KeyValue::new("type", power_type),
                            ],
                        );
                    }
                }
            })
            .build();

        let voltage = watch_snapshot.clone();
        let _voltage = proc
            .get_proc_param()
            .meter("ecoflow")
            .f64_observable_gauge("prosa_ecoflow_voltage")
            .with_description("EcoFlow STREAM input voltage")
            .with_unit("V")
            .with_callback(move |observer| {
                if let Some(snapshot) = voltage.borrow().as_ref() {
                    for (voltage_type, value) in snapshot.solar_values(|input| input.voltage) {
                        observer.observe(
                            value,
                            &[
                                KeyValue::new("sn", snapshot.serial_number.clone()),
                                KeyValue::new("type", voltage_type),
                            ],
                        );
                    }
                }
            })
            .build();

        let _current = proc
            .get_proc_param()
            .meter("ecoflow")
            .f64_observable_gauge("prosa_ecoflow_current")
            .with_description("EcoFlow STREAM input current")
            .with_unit("A")
            .with_callback(move |observer| {
                if let Some(snapshot) = watch_snapshot.borrow().as_ref() {
                    for (current_type, value) in snapshot.solar_values(|input| input.current) {
                        observer.observe(
                            value,
                            &[
                                KeyValue::new("sn", snapshot.serial_number.clone()),
                                KeyValue::new("type", current_type),
                            ],
                        );
                    }
                }
            })
            .build();

        Ok(Self {
            access_key,
            secret_key,
            state: EcoFlowFetchState::default(),
            pending_serial_numbers: VecDeque::new(),
            main_serial_numbers: BTreeSet::new(),
            main_serial_number: None,
            snapshot,
        })
    }

    fn fetch(&mut self) -> Result<FetchAction<M>, FetcherError<M>> {
        self.state = if self.main_serial_number.is_some() {
            EcoFlowFetchState::Quota
        } else {
            EcoFlowFetchState::DeviceList
        };
        Ok(FetchAction::Http)
    }

    fn create_http_request(
        &self,
        request_builder: request::Builder,
    ) -> Result<Request<BoxBody<Bytes, Infallible>>, FetcherError<M>> {
        match self.state {
            EcoFlowFetchState::DeviceList => {
                self.signed_request(request_builder, "/iot-open/sign/device/list".into(), "")
            }
            EcoFlowFetchState::MainDevice => {
                let serial_number = self.pending_serial_numbers.front().ok_or_else(|| {
                    FetcherError::Other("No pending EcoFlow serial number".into())
                })?;
                let parameters = format!("sn={serial_number}");
                self.signed_request(
                    request_builder,
                    format!("/iot-open/sign/device/system/main/sn?{parameters}"),
                    &parameters,
                )
            }
            EcoFlowFetchState::Quota => {
                let serial_number = self.main_serial_number.as_ref().ok_or_else(|| {
                    FetcherError::Other("EcoFlow main serial number is unavailable".into())
                })?;
                let parameters = format!("sn={serial_number}");
                self.signed_request(
                    request_builder,
                    format!("/iot-open/sign/device/quota/all?{parameters}"),
                    &parameters,
                )
            }
        }
    }

    async fn process_http_response(
        &mut self,
        response: Result<Response<Incoming>, FetcherError<M>>,
    ) -> Result<FetchAction<M>, FetcherError<M>> {
        let state = self.state;
        let result = async {
            let response = response?;
            if response.status() != StatusCode::OK {
                return Err(FetcherError::Other(format!(
                    "EcoFlow API returned HTTP {}",
                    response.status()
                )));
            }
            let body = Limited::new(response.into_body(), MAX_RESPONSE_BYTES)
                .collect()
                .await
                .map_err(|error| {
                    FetcherError::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Invalid EcoFlow response body: {error}"),
                    ))
                })?
                .to_bytes();
            let response: EcoFlowApiResponse = serde_json::from_slice(&body).map_err(|error| {
                FetcherError::Io(io::Error::new(io::ErrorKind::InvalidData, error))
            })?;
            self.process_data(response.into_data().map_err(FetcherError::Other)?)
        }
        .await;
        if result.is_err() && state == EcoFlowFetchState::Quota {
            let _ = self.snapshot.send(None);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn matches_ecoflow_signature_test_vector() {
        let parameters = "params.cmdSet=11&params.eps=0&params.id=24&sn=123456789";
        assert_eq!(
            signature(
                b"WIbFEKre0s6sLnh4ei7SPUeYnptHG6V",
                parameters,
                "Fp4SvIprYSDPXtYJidEtUAd1o",
                345164,
                1671171709428,
            )
            .unwrap(),
            "07c13b65e037faf3b153d51613638fa80003c4c38d2407379a7f52851af1473e"
        );
    }

    #[test]
    fn parses_battery_and_power_snapshot() {
        let data = json!({
            "cmsBattSoc": 33.0,
            "powGetPvSum": "850.5",
            "powGetPv": 400.0,
            "powGetPv2": 450.5,
            "powGetBpCms": -225.5,
            "powGetSysLoad": 358.5,
            "powGetSysGrid": -53.0,
            "gridConnectionPower": 383.0,
            "powGetSysLoadFromPv": 133.0,
            "powGetSysLoadFromBp": 225.5,
            "powGetSysLoadFromGrid": 0.0,
            "powGetSchuko1": 120.0,
            "powGetSchuko2": 0.0,
            "powGetPv3": null
        });
        let snapshot = EcoFlowSnapshot::from_data("BK11ZEBB2H350011".into(), &data).unwrap();
        assert_eq!(snapshot.battery_charge, 33.0);
        assert_eq!(snapshot.solar_power, Some(850.5));
        assert_eq!(snapshot.battery_power, Some(-225.5));
        assert_eq!(snapshot.ac_outlet_1_power, Some(120.0));
        assert_eq!(snapshot.powers().count(), 12);
    }

    #[test]
    fn keeps_reported_solar_input_power_on_legacy_firmware() {
        let data = json!({
            "cmsBattSoc": 50,
            "powGetPv": 400.0,
            "plugInInfoPvVol": 40.0,
            "plugInInfoPvAmp": 0.0,
            "powGetPv2": 0.0
        });
        let snapshot = EcoFlowSnapshot::from_data("BK11ZEBB2H350011".into(), &data).unwrap();
        assert_eq!(
            snapshot
                .solar_values(|input| input.power)
                .collect::<Vec<_>>(),
            [("solar_1", 400.0), ("solar_2", 0.0)]
        );
    }

    #[test]
    fn computes_solar_input_power_from_voltage_and_current() {
        let data = json!({
            "cmsBattSoc": 50,
            "powGetPv": 0.0,
            "plugInInfoPvVol": 40.0,
            "plugInInfoPvAmp": 10.0,
            "plugInInfoPv2Vol": "32.5",
            "plugInInfoPv2Amp": "2",
            "plugInInfoPv3Vol": 30.0
        });
        let snapshot = EcoFlowSnapshot::from_data("BK11ZEBB2H350011".into(), &data).unwrap();
        assert_eq!(
            snapshot
                .solar_values(|input| input.power)
                .collect::<Vec<_>>(),
            [("solar_1", 400.0), ("solar_2", 65.0)]
        );
        assert_eq!(
            snapshot
                .solar_values(|input| input.voltage)
                .collect::<Vec<_>>(),
            [("solar_1", 40.0), ("solar_2", 32.5), ("solar_3", 30.0)]
        );
        assert_eq!(
            snapshot
                .solar_values(|input| input.current)
                .collect::<Vec<_>>(),
            [("solar_1", 10.0), ("solar_2", 2.0)]
        );
    }

    #[test]
    fn rejects_invalid_solar_input_measurement() {
        let data = json!({"cmsBattSoc": 50, "plugInInfoPv4Amp": "invalid"});
        assert!(EcoFlowSnapshot::from_data("BK11ZEBB2H350011".into(), &data).is_err());
    }

    #[test]
    fn rejects_missing_or_invalid_battery_charge() {
        for data in [
            json!({}),
            json!({"cmsBattSoc": -1}),
            json!({"cmsBattSoc": 101}),
            json!({"cmsBattSoc": "invalid"}),
        ] {
            assert!(EcoFlowSnapshot::from_data("BK11ZEBB2H350011".into(), &data).is_err());
        }
    }

    #[test]
    fn discovers_only_stream_devices() {
        let serial_numbers = stream_serials(&json!([
            {"sn": "BK61ZEBB2H350012", "online": 1},
            {"sn": "BK11ZEBB2H350011", "online": 1},
            {"sn": "BK01ZEBB2H350013", "online": 1},
            {"sn": "HW52ZEBB2H350014", "online": 1},
            {"online": 1},
            {"sn": "BK11ZEBB2H350011", "online": 1}
        ]))
        .unwrap();
        assert_eq!(
            serial_numbers.into_iter().collect::<Vec<_>>(),
            ["BK11ZEBB2H350011", "BK61ZEBB2H350012"]
        );
    }

    #[test]
    fn rejects_device_list_without_stream_devices() {
        assert!(stream_serials(&json!([{"sn": "HW52ZEBB2H350014", "online": 1}])).is_err());
        assert!(stream_serials(&json!([{"sn": "BK12bad/sn", "online": 1}])).is_err());
    }

    #[test]
    fn parses_main_device_serial_number() {
        assert_eq!(
            main_serial_number(&json!({"sn": "BK11ZEBB2H350011"})).unwrap(),
            "BK11ZEBB2H350011"
        );
        assert!(main_serial_number(&json!({"sn": "bad/sn"})).is_err());
    }
}
