//! Fetcher adaptor for [Shelly](https://shelly-api-docs.shelly.cloud/gen2/General/RPCChannels) appliances
//! Use to read electricity power values out of it

use std::{collections::HashMap, convert::Infallible};

use bytes::Buf as _;
use http::{Method, Request, Response, StatusCode, Uri, request};
use http_body_util::{BodyExt as _, combinators::BoxBody};
use hyper::body::Incoming;
use opentelemetry::KeyValue;
use prosa::core::{adaptor::Adaptor, proc::ProcConfig as _};
use prosa_fetcher::{
    adaptor::FetcherAdaptor,
    proc::{FetchAction, FetcherError, FetcherProc},
};
use serde::Deserialize;
use tokio::sync::watch;
use tracing::{debug, warn};

#[allow(unused)]
#[derive(Debug, Default, Deserialize)]
struct ShellyEMStatus {
    /// Id of the EM component instance
    id: u8,

    /// Phase A current measurement value, [A]
    a_current: Option<f64>,
    /// Phase A voltage measurement value, [V]
    a_voltage: Option<f64>,
    /// Phase A active power measurement value, [W]
    a_act_power: Option<f64>,
    /// Phase A apparent power measurement value, [VA]
    a_aprt_power: Option<f64>,
    /// Phase A power factor measurement value
    a_pf: Option<f64>,
    /// Phase A network frequency measurement value
    a_freq: Option<f64>,
    /// Phase A error conditions occurred. May contain `out_of_range:active_power`, `out_of_range:apparent_power`, `out_of_range:voltage`, `out_of_range:current`, (shown if at least one error is present)
    #[serde(default)]
    a_errors: Vec<String>,

    /// Phase B current measurement value, [A]
    b_current: Option<f64>,
    /// Phase B voltage measurement value, [V]
    b_voltage: Option<f64>,
    /// Phase B active power measurement value, [W]
    b_act_power: Option<f64>,
    /// Phase B apparent power measurement value, [VA]
    b_aprt_power: Option<f64>,
    /// Phase B power factor measurement value
    b_pf: Option<f64>,
    /// Phase B network frequency measurement value
    b_freq: Option<f64>,
    /// Phase B error conditions occurred. May contain `out_of_range:active_power`, `out_of_range:apparent_power`, `out_of_range:voltage`, `out_of_range:current`, (shown if at least one error is present)
    #[serde(default)]
    b_errors: Vec<String>,

    /// Phase C current measurement value, [A]
    c_current: Option<f64>,
    /// Phase C voltage measurement value, [V]
    c_voltage: Option<f64>,
    /// Phase C active power measurement value, [W]
    c_act_power: Option<f64>,
    /// Phase C apparent power measurement value, [VA]
    c_aprt_power: Option<f64>,
    /// Phase C power factor measurement value
    c_pf: Option<f64>,
    /// Phase C network frequency measurement value
    c_freq: Option<f64>,
    /// Phase C error conditions occurred. May contain `out_of_range:active_power`, `out_of_range:apparent_power`, `out_of_range:voltage`, `out_of_range:current`, (shown if at least one error is present)
    #[serde(default)]
    c_errors: Vec<String>,

    /// Neutral current measurement value, [A] (if supported)
    n_current: Option<f64>,
    /// Neutral error conditions occurred. May contain `out_of_range:current`,(shown if error is present)
    #[serde(default)]
    n_errors: Vec<String>,

    /// Sum of the current on all phases(excluding neutral readings if available)
    total_current: Option<f64>,
    /// Sum of the active power on all phases
    total_act_power: Option<f64>,
    /// Sum of the apparent power on all phases
    total_aprt_power: Option<f64>,

    /// Indicates which phase was user calibrated
    #[serde(default)]
    user_calibrated_phase: Vec<String>,

    /// EM component error conditions. May contain `power_meter_failure`, `phase_sequence` or `ct_type_not_set`. Present in status only if not empty.
    #[serde(default)]
    errors: Vec<String>,
}

impl ShellyEMStatus {
    /// Return `true` if the EM measure threee phase installation, `false` otherwise
    fn is_three_phase(&self) -> bool {
        self.a_voltage.is_some_and(|v| v > 0.0)
            && self.b_voltage.is_some_and(|v| v > 0.0)
            && self.c_voltage.is_some_and(|v| v > 0.0)
    }

    fn get_voltage(&self) -> Option<(f64, Option<(f64, f64)>)> {
        if let Some(a_voltage) = self.a_voltage
            && a_voltage > 0.0
        {
            if let Some(b_voltage) = self.b_voltage
                && b_voltage > 0.0
                && let Some(c_voltage) = self.c_voltage
                && c_voltage > 0.0
            {
                Some((a_voltage, Some((b_voltage, c_voltage))))
            } else {
                Some((a_voltage, None))
            }
        } else if let Some(b_voltage) = self.b_voltage
            && b_voltage > 50.0
        {
            Some((b_voltage, None))
        } else if let Some(c_voltage) = self.c_voltage
            && c_voltage > 50.0
        {
            Some((c_voltage, None))
        } else {
            None
        }
    }

    fn get_current(&self) -> Option<(f64, Option<(f64, f64)>)> {
        if self.is_three_phase() {
            if let Some(a_current) = self.a_current
                && let Some(b_current) = self.b_current
                && let Some(c_current) = self.c_current
            {
                Some((a_current, Some((b_current, c_current))))
            } else {
                None
            }
        } else {
            self.total_current.map(|t| (t, None))
        }
    }

    fn get_active_power(&self) -> Option<(f64, Option<(f64, f64)>)> {
        if self.is_three_phase() {
            if let Some(a_act_power) = self.a_act_power
                && let Some(b_act_power) = self.b_act_power
                && let Some(c_act_power) = self.c_act_power
            {
                Some((a_act_power, Some((b_act_power, c_act_power))))
            } else {
                None
            }
        } else {
            self.total_act_power.map(|t| (t, None))
        }
    }

    fn get_apparent_power(&self) -> Option<(f64, Option<(f64, f64)>)> {
        if self.is_three_phase() {
            if let Some(a_aprt_power) = self.a_aprt_power
                && let Some(b_aprt_power) = self.b_aprt_power
                && let Some(c_aprt_power) = self.c_aprt_power
            {
                Some((a_aprt_power, Some((b_aprt_power, c_aprt_power))))
            } else {
                None
            }
        } else {
            self.total_aprt_power.map(|t| (t, None))
        }
    }
}

#[derive(Debug, Default, Deserialize)]
struct ShellyEMData {
    /// Id of the EMData component instance
    id: u8,

    /// Total active energy on phase A, Wh
    a_total_act_energy: f64,
    /// Total active returned energy on phase A, Wh
    a_total_act_ret_energy: f64,

    /// Total active energy on phase B, Wh
    b_total_act_energy: f64,
    /// Total active returned energy on phase B, Wh
    b_total_act_ret_energy: f64,

    /// Total active energy on phase C, Wh
    c_total_act_energy: f64,
    /// Total active returned energy on phase C, Wh
    c_total_act_ret_energy: f64,

    /// Total active energy on all phases, Wh
    total_act: f64,
    /// Total active returned energy on all phases, Wh
    total_act_ret: f64,

    /// Error condition occurred. May contain database_error or ct_type_not_set, (shown if the error is present).
    #[serde(default)]
    errors: Vec<String>,
}

impl ShellyEMData {
    fn get_power(&self) -> (f64, Option<(f64, f64)>) {
        if self.a_total_act_energy > 0.0 {
            if self.b_total_act_energy > 0.0 && self.c_total_act_energy > 0.0 {
                (
                    self.a_total_act_energy,
                    Some((self.b_total_act_energy, self.c_total_act_energy)),
                )
            } else {
                (self.a_total_act_energy, None)
            }
        } else if self.b_total_act_energy > 0.0 {
            (self.b_total_act_energy, None)
        } else if self.c_total_act_energy > 0.0 {
            (self.c_total_act_energy, None)
        } else {
            (self.total_act, None)
        }
    }

    fn get_returned_power(&self) -> (f64, Option<(f64, f64)>) {
        if self.a_total_act_ret_energy > 0.0 {
            if self.b_total_act_ret_energy > 0.0 && self.c_total_act_ret_energy > 0.0 {
                (
                    self.a_total_act_ret_energy,
                    Some((self.b_total_act_ret_energy, self.c_total_act_ret_energy)),
                )
            } else {
                (self.a_total_act_ret_energy, None)
            }
        } else if self.b_total_act_ret_energy > 0.0 {
            (self.b_total_act_ret_energy, None)
        } else if self.c_total_act_ret_energy > 0.0 {
            (self.c_total_act_ret_energy, None)
        } else {
            (self.total_act_ret, None)
        }
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct ShellyStatus {
    /// Identifier of the device
    #[serde(skip)]
    id: String,

    // EM Shelly
    #[serde(rename = "em:0")]
    em: Option<ShellyEMStatus>,
    #[serde(rename = "emdata:0")]
    em_data: Option<ShellyEMData>,

    #[serde(rename = "temperature:0")]
    temperature: Option<HashMap<String, serde_json::Value>>,
    wifi: Option<HashMap<String, serde_json::Value>>,
}

impl ShellyStatus {
    fn get_error(&self) -> Option<String> {
        self.em
            .as_ref()
            .map(|s| format!("Shelly[{}] EM error: {}", self.id, s.errors.join(", ")))
            .or(self
                .em_data
                .as_ref()
                .map(|s| format!("Shelly[{}] EM Data error: {}", self.id, s.errors.join(", "))))
    }

    fn get_celsius_temp(&self) -> Option<f64> {
        self.temperature
            .as_ref()
            .and_then(|t| t.get("tC").and_then(|v| v.as_f64()))
    }

    fn get_wifi(&self) -> Option<(String, i64)> {
        if let Some(wifi) = &self.wifi
            && let Some(ssid) = wifi.get("ssid").and_then(|s| s.as_str())
            && let Some(rssi) = wifi.get("rssi").and_then(|r| r.as_i64())
        {
            Some((ssid.to_string(), rssi))
        } else {
            None
        }
    }
}

/// Adaptor for [Shelly](https://shelly-api-docs.shelly.cloud/) components
#[derive(Adaptor)]
pub struct FetcherShellyAdaptor {
    uri_status: Uri,

    /// Identifier of the device
    id: Option<String>,

    // Observability
    shelly_status: watch::Sender<ShellyStatus>,
}

// Macro to observe instantaneous
macro_rules! observe_instantaneous {
    ($getter:expr, $observer:expr, $id:expr, $name:expr, $type:expr) => {
        match $getter {
            Some((value, None)) => {
                $observer.observe(
                    value,
                    &[
                        KeyValue::new("name", $name),
                        KeyValue::new("id", $id),
                        KeyValue::new("type", $type),
                    ],
                );
            }
            Some((value_1, Some((value_2, value_3)))) => {
                $observer.observe(
                    value_1,
                    &[
                        KeyValue::new("name", $name),
                        KeyValue::new("id", $id),
                        KeyValue::new("type", $type),
                        KeyValue::new("phase", 1i64),
                    ],
                );
                $observer.observe(
                    value_2,
                    &[
                        KeyValue::new("name", $name),
                        KeyValue::new("id", $id),
                        KeyValue::new("type", $type),
                        KeyValue::new("phase", 2i64),
                    ],
                );
                $observer.observe(
                    value_3,
                    &[
                        KeyValue::new("name", $name),
                        KeyValue::new("id", $id),
                        KeyValue::new("type", $type),
                        KeyValue::new("phase", 3i64),
                    ],
                );
            }
            _ => {}
        }
    };
}

// Macro to observe power
macro_rules! observe_power {
    ($getter:expr, $observer:expr, $id:expr, $name:expr, $type:expr) => {
        match $getter {
            (value, None) => {
                $observer.observe(
                    value,
                    &[
                        KeyValue::new("name", $name),
                        KeyValue::new("id", $id),
                        KeyValue::new("type", $type),
                    ],
                );
            }
            (value_1, Some((value_2, value_3))) => {
                $observer.observe(
                    value_1,
                    &[
                        KeyValue::new("name", $name),
                        KeyValue::new("id", $id),
                        KeyValue::new("type", $type),
                        KeyValue::new("phase", 1i64),
                    ],
                );
                $observer.observe(
                    value_2,
                    &[
                        KeyValue::new("name", $name),
                        KeyValue::new("id", $id),
                        KeyValue::new("type", $type),
                        KeyValue::new("phase", 2i64),
                    ],
                );
                $observer.observe(
                    value_3,
                    &[
                        KeyValue::new("name", $name),
                        KeyValue::new("id", $id),
                        KeyValue::new("type", $type),
                        KeyValue::new("phase", 3i64),
                    ],
                );
            }
        }
    };
}

impl<M> FetcherAdaptor<M> for FetcherShellyAdaptor
where
    M: 'static
        + std::marker::Send
        + std::marker::Sync
        + std::marker::Sized
        + std::clone::Clone
        + std::fmt::Debug
        + prosa::core::msg::Tvf
        + std::default::Default,
{
    fn new(proc: &FetcherProc<M>) -> Result<Self, FetcherError<M>> {
        let (shelly_status, watch_shelly_status) = watch::channel(ShellyStatus::default());

        let watch_instantaneous = watch_shelly_status.clone();
        let _observable_instantaneous = proc
            .get_proc_param()
            .meter("shelly")
            .f64_observable_gauge("prosa_shelly_instantaneous")
            .with_description("Instantaneous of the Shelly")
            .with_callback(move |observer| {
                let shelly_status = watch_instantaneous.borrow();
                if let Some(em_status) = &shelly_status.em {
                    observe_instantaneous!(
                        em_status.get_voltage(),
                        observer,
                        em_status.id as i64,
                        shelly_status.id.clone(),
                        "voltage"
                    );

                    observe_instantaneous!(
                        em_status.get_current(),
                        observer,
                        em_status.id as i64,
                        shelly_status.id.clone(),
                        "current"
                    );

                    observe_instantaneous!(
                        em_status.get_active_power(),
                        observer,
                        em_status.id as i64,
                        shelly_status.id.clone(),
                        "active_power"
                    );

                    observe_instantaneous!(
                        em_status.get_apparent_power(),
                        observer,
                        em_status.id as i64,
                        shelly_status.id.clone(),
                        "power"
                    );
                }
            })
            .build();

        let watch_power = watch_shelly_status.clone();
        let _observable_power = proc
            .get_proc_param()
            .meter("shelly")
            .f64_observable_counter("prosa_shelly_power")
            .with_description("Power information of the Shelly")
            .with_callback(move |observer| {
                let shelly_status = watch_power.borrow();
                if let Some(em_data) = &shelly_status.em_data {
                    observe_power!(
                        em_data.get_power(),
                        observer,
                        em_data.id as i64,
                        shelly_status.id.clone(),
                        "power"
                    );

                    observe_power!(
                        em_data.get_returned_power(),
                        observer,
                        em_data.id as i64,
                        shelly_status.id.clone(),
                        "ret_power"
                    );
                }
            })
            .build();

        let watch_temp = watch_shelly_status.clone();
        let _observable_temp = proc
            .get_proc_param()
            .meter("shelly")
            .f64_observable_gauge("prosa_shelly_temp")
            .with_description("Temperature information of the Shelly")
            .with_callback(move |observer| {
                let shelly_status = watch_temp.borrow();
                if let Some(temp) = shelly_status.get_celsius_temp() {
                    observer.observe(temp, &[KeyValue::new("id", shelly_status.id.clone())]);
                }
            })
            .build();

        let _observable_wireless = proc
            .get_proc_param()
            .meter("shelly")
            .i64_observable_gauge("prosa_shelly_wireless")
            .with_description("Wireless information of the Shelly")
            .with_callback(move |observer| {
                let shelly_status = watch_shelly_status.borrow();
                if let Some((ssid, rssi)) = shelly_status.get_wifi() {
                    observer.observe(
                        rssi,
                        &[
                            KeyValue::new("id", shelly_status.id.clone()),
                            KeyValue::new("ssid", ssid),
                        ],
                    );
                }
            })
            .build();

        Ok(FetcherShellyAdaptor {
            uri_status: "/rpc/Shelly.GetStatus"
                .parse::<hyper::Uri>()
                .expect("RPC Shelly Get status URI for Shelly Adaptor should be parsed"),
            id: None,
            shelly_status,
        })
    }

    fn fetch(&mut self) -> Result<FetchAction<M>, FetcherError<M>> {
        // Call HTTP to retrieve consumption
        Ok(FetchAction::Http)
    }

    fn create_http_request(
        &self,
        mut request_builder: request::Builder,
    ) -> Result<Request<BoxBody<hyper::body::Bytes, Infallible>>, FetcherError<M>> {
        if self.id.is_some() {
            request_builder = request_builder
                .method(Method::GET)
                .uri(self.uri_status.clone())
                .header(hyper::header::ACCEPT, "application/json");
            let request = request_builder.body(BoxBody::default())?;
            debug!("Send request: {:?}", request);
            Ok(request)
        } else {
            request_builder =
                request_builder
                    .method(Method::GET)
                    .uri("/rpc/Shelly.GetDeviceInfo".parse::<hyper::Uri>().expect(
                        "RPC Shelly Get device info URI for Shelly Adaptor should be parsed",
                    ))
                    .header(hyper::header::ACCEPT, "application/json");
            let request = request_builder.body(BoxBody::default())?;
            debug!("Send device info request: {:?}", request);
            Ok(request)
        }
    }

    async fn process_http_response(
        &mut self,
        response: Result<Response<Incoming>, FetcherError<M>>,
    ) -> Result<FetchAction<M>, FetcherError<M>> {
        match response {
            Ok(response) => {
                debug!("Receive response: {:?}", response);
                match response.status() {
                    StatusCode::OK => {
                        if let Some(id) = self.id.as_ref() {
                            let server = response
                                .headers()
                                .get(http::header::SERVER)
                                .and_then(|s| s.to_str().ok().map(|h| h.to_string()));
                            let body = response
                                .collect()
                                .await
                                .map_err(|e| FetcherError::Hyper(e, server.unwrap_or_default()))?
                                .aggregate();

                            // Parse the API response return to get the data
                            let mut shelly_status: ShellyStatus =
                                serde_json::from_reader(body.reader())
                                    .map_err(|e| FetcherError::Io(e.into()))?;
                            shelly_status.id = id.clone();

                            if let Some(shelly_err) = shelly_status.get_error() {
                                warn!(name = id, "{shelly_err}");
                            } else {
                                debug!("shelly status: {shelly_status:?}");
                            }

                            let _ = self.shelly_status.send(shelly_status);
                        } else {
                            let server = response
                                .headers()
                                .get(http::header::SERVER)
                                .and_then(|s| s.to_str().ok().map(|h| h.to_string()));
                            let body = response
                                .collect()
                                .await
                                .map_err(|e| FetcherError::Hyper(e, server.unwrap_or_default()))?
                                .aggregate();

                            // Parse the API response return to get the data
                            let device_info_resp: HashMap<String, serde_json::Value> =
                                serde_json::from_reader(body.reader())
                                    .map_err(|e| FetcherError::Io(e.into()))?;
                            debug!("Device info {device_info_resp:?}");

                            self.id = device_info_resp
                                .get("name")
                                .and_then(|v| v.as_str().map(|s| s.to_string()))
                                .or(device_info_resp
                                    .get("id")
                                    .and_then(|v| v.as_str().map(|s| s.to_string())));
                        }

                        Ok(FetchAction::None)
                    }
                    StatusCode::UNAUTHORIZED => {
                        if response
                            .headers()
                            .contains_key(hyper::header::WWW_AUTHENTICATE)
                        {
                            // Recall with the credential
                            unimplemented!("Baerer auth need to be implemented");
                            //Ok(FetchAction::Http)
                        } else {
                            warn!("Unauthorized from HTTP remote");
                            Err(FetcherError::Other(
                                "Unauthorized from HTTP remote".to_string(),
                            ))
                        }
                    }
                    code => {
                        warn!("Receive wrong response: {:?}", response);
                        Err(FetcherError::Other(format!(
                            "Receive error from HTTP remote: {code}"
                        )))
                    }
                }
            }
            Err(FetcherError::Hyper(he, addr)) => {
                warn!(addr = addr, "HTTP error {:?}", he);
                Err(FetcherError::Hyper(he, addr))
            }
            Err(e) => Err(e),
        }
    }

    fn end_active_period(&mut self) {
        // Reset ID at the end of the period
        self.id = None;
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_em_status() {
        let shelly_em_status: ShellyEMStatus = serde_json::from_str(
            "{
            \"id\": 0,
            \"a_current\": 4.029,
            \"a_voltage\": 236.1,
            \"a_act_power\": 951.2,
            \"a_aprt_power\": 951.9,
            \"a_pf\": 1,
            \"a_freq\": 50,
            \"b_current\": 4.027,
            \"b_voltage\": 236.201,
            \"b_act_power\": -951.1,
            \"b_aprt_power\": 951.8,
            \"b_pf\": 1,
            \"b_freq\": 50,
            \"c_current\": 3.03,
            \"c_voltage\": 236.402,
            \"c_active_power\": 715.4,
            \"c_aprt_power\": 716.2,
            \"c_pf\": 1,
            \"c_freq\": 50,
            \"n_current\": 11.029,
            \"total_current\": 11.083,
            \"total_act_power\": 2484.782,
            \"total_aprt_power\": 2486.7,
            \"user_calibrated_phase\": [],
            \"errors\": [
                \"phase_sequence\"
            ]
        }",
        )
        .expect("ShellyEMStatus should be parsed");
        assert_eq!(0, shelly_em_status.id);
        assert!(shelly_em_status.a_errors.is_empty());
        assert!(shelly_em_status.b_errors.is_empty());
        assert!(shelly_em_status.c_errors.is_empty());
        assert!(shelly_em_status.n_errors.is_empty());
        assert_eq!(
            Some("phase_sequence"),
            shelly_em_status.errors.first().map(|e| e.as_str())
        );
    }

    #[test]
    fn test_em_data() {
        let shelly_em_data: ShellyEMData = serde_json::from_str(
            "{
            \"id\": 0,
            \"a_total_act_energy\": 0,
            \"a_total_act_ret_energy\": 0,
            \"b_total_act_energy\": 0,
            \"b_total_act_ret_energy\": 0,
            \"c_total_act_energy\": 0,
            \"c_total_act_ret_energy\": 0,
            \"total_act\": 0,
            \"total_act_ret\": 0
        }",
        )
        .expect("ShellyEMStatus should be parsed");
        assert_eq!(0, shelly_em_data.id);
        assert_eq!(0.0, shelly_em_data.a_total_act_energy);
        assert_eq!(0.0, shelly_em_data.a_total_act_ret_energy);
        assert_eq!(0.0, shelly_em_data.b_total_act_energy);
        assert_eq!(0.0, shelly_em_data.b_total_act_ret_energy);
        assert_eq!(0.0, shelly_em_data.c_total_act_energy);
        assert_eq!(0.0, shelly_em_data.c_total_act_ret_energy);
        assert_eq!(0.0, shelly_em_data.total_act);
        assert_eq!(0.0, shelly_em_data.total_act_ret);
        assert!(shelly_em_data.errors.is_empty());
    }
}
