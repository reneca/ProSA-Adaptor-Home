//! Read-only EcoFlow STREAM Ultra metrics through the IoT Open HTTP API.

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

#[derive(Debug, Default, Clone, PartialEq)]
struct EcoFlowSnapshot {
    serial_number: String,
    battery_charge: f64,
    solar_power: Option<f64>,
    solar_1_power: Option<f64>,
    solar_2_power: Option<f64>,
    solar_3_power: Option<f64>,
    solar_4_power: Option<f64>,
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

        Ok(Self {
            serial_number,
            battery_charge,
            solar_power: number(data, "powGetPvSum")?,
            solar_1_power: number(data, "powGetPv")?,
            solar_2_power: number(data, "powGetPv2")?,
            solar_3_power: number(data, "powGetPv3")?,
            solar_4_power: number(data, "powGetPv4")?,
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
            ("solar_1", self.solar_1_power),
            ("solar_2", self.solar_2_power),
            ("solar_3", self.solar_3_power),
            ("solar_4", self.solar_4_power),
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

fn stream_ultra_serials(data: &Value) -> Result<VecDeque<String>, String> {
    let devices = data
        .as_array()
        .ok_or("EcoFlow device list data must be an array")?;
    let mut serial_numbers = BTreeSet::new();
    for device in devices {
        let Some(serial_number) = device.get("sn").and_then(Value::as_str) else {
            continue;
        };
        if serial_number.starts_with("BK11") {
            if !valid_serial_number(serial_number) {
                return Err("EcoFlow returned an invalid STREAM Ultra serial number".into());
            }
            serial_numbers.insert(serial_number.to_owned());
        }
    }
    if serial_numbers.is_empty() {
        return Err("No directly bound EcoFlow STREAM Ultra was found".into());
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

/// Fetches telemetry for one directly bound EcoFlow STREAM Ultra system.
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
                self.pending_serial_numbers =
                    stream_ultra_serials(&data).map_err(FetcherError::Other)?;
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
                            "Multiple independent EcoFlow STREAM Ultra systems were found".into(),
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
            .with_description("EcoFlow STREAM Ultra system battery charge")
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

        let _power = proc
            .get_proc_param()
            .meter("ecoflow")
            .f64_observable_gauge("prosa_ecoflow_power")
            .with_description("EcoFlow STREAM Ultra instantaneous power")
            .with_unit("W")
            .with_callback(move |observer| {
                if let Some(snapshot) = watch_snapshot.borrow().as_ref() {
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
    fn discovers_only_stream_ultra_devices() {
        let serial_numbers = stream_ultra_serials(&json!([
            {"sn": "BK11ZEBB2H350011", "online": 1},
            {"sn": "BK61ZEBB2H350012", "online": 1},
            {"sn": "BK11ZEBB2H350011", "online": 1}
        ]))
        .unwrap();
        assert_eq!(
            serial_numbers.into_iter().collect::<Vec<_>>(),
            ["BK11ZEBB2H350011"]
        );
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
