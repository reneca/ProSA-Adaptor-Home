//! Fetcher adaptor for [Shelly](https://shelly-api-docs.shelly.cloud/gen2/General/RPCChannels) appliances
//! Use to read electricity power values out of it

use std::{collections::HashMap, convert::Infallible, io};

use bytes::Bytes;
use http::{HeaderValue, Method, Request, Response, StatusCode, request};
use http_body_util::{BodyExt as _, Limited, combinators::BoxBody};
use hyper::body::Incoming;
use prosa::{
    core::{adaptor::Adaptor, proc::ProcConfig as _},
    otel::KeyValue,
    tracing::{debug, trace, warn},
};
use prosa_fetcher::{
    adaptor::FetcherAdaptor,
    proc::{FetchAction, FetcherError, FetcherProc},
};
use serde::{Deserialize, Deserializer, de::Error as _};
use sha2::{Digest as _, Sha256};
use tokio::sync::watch;

const MAX_RESPONSE_BYTES: usize = 256 * 1024;
const DEVICE_INFO_URI: &str = "/rpc/Shelly.GetDeviceInfo";
const CONFIG_URI: &str = "/rpc/Shelly.GetConfig";
const STATUS_URI: &str = "/rpc/Shelly.GetStatus";

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
        self.a_voltage.is_some_and(|v| v > 50.0)
            && self.b_voltage.is_some_and(|v| v > 50.0)
            && self.c_voltage.is_some_and(|v| v > 50.0)
    }

    fn get_voltage(&self) -> Option<(f64, Option<(f64, f64)>)> {
        if let Some(a_voltage) = self.a_voltage
            && a_voltage > 50.0
        {
            if let Some(b_voltage) = self.b_voltage
                && b_voltage > 50.0
                && let Some(c_voltage) = self.c_voltage
                && c_voltage > 50.0
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
        if self.a_total_act_energy > 10.0 {
            if self.b_total_act_energy > 10.0 && self.c_total_act_energy > 10.0 {
                (
                    self.a_total_act_energy,
                    Some((self.b_total_act_energy, self.c_total_act_energy)),
                )
            } else {
                (self.a_total_act_energy, None)
            }
        } else if self.b_total_act_energy > 10.0 {
            (self.b_total_act_energy, None)
        } else if self.c_total_act_energy > 10.0 {
            (self.c_total_act_energy, None)
        } else {
            (self.total_act, None)
        }
    }

    fn get_returned_power(&self) -> (f64, Option<(f64, f64)>) {
        if self.a_total_act_ret_energy > 10.0 {
            if self.b_total_act_ret_energy > 10.0 && self.c_total_act_ret_energy > 10.0 {
                (
                    self.a_total_act_ret_energy,
                    Some((self.b_total_act_ret_energy, self.c_total_act_ret_energy)),
                )
            } else {
                (self.a_total_act_ret_energy, None)
            }
        } else if self.b_total_act_ret_energy > 10.0 {
            (self.b_total_act_ret_energy, None)
        } else if self.c_total_act_ret_energy > 10.0 {
            (self.c_total_act_ret_energy, None)
        } else {
            (self.total_act_ret, None)
        }
    }
}

#[derive(Debug, Default, Deserialize)]
struct ShellyPMEnergy {
    /// Total of energy
    total: f64,
}

#[derive(Debug, Default, Deserialize)]
struct ShellyPMStatus {
    /// Id of the Switch component instance.
    id: u8,
    /// Configured name of the Switch component instance.
    #[serde(default)]
    name: String,
    /// Instantaneous active power in W.
    apower: Option<f64>,
    /// Total consumed active energy.
    aenergy: Option<ShellyPMEnergy>,
    /// Total returned active energy.
    ret_aenergy: Option<ShellyPMEnergy>,
}

fn deserialize_pm<'de, D>(deserializer: D) -> Result<Vec<ShellyPMStatus>, D::Error>
where
    D: Deserializer<'de>,
{
    let components = HashMap::<String, serde_json::Value>::deserialize(deserializer)?;
    let mut pm = components
        .into_iter()
        .filter(|(component, _)| component.starts_with("switch:"))
        .map(|(component, value)| {
            serde_json::from_value::<ShellyPMStatus>(value)
                .map_err(|error| D::Error::custom(format!("Invalid {component} status: {error}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    pm.sort_unstable_by_key(|status| status.id);
    Ok(pm)
}

#[derive(Debug, Deserialize)]
struct ShellySwitchConfig {
    /// Channel ID
    id: u8,
    /// Channel name
    name: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ShellyConfig {
    #[serde(flatten)]
    components: HashMap<String, serde_json::Value>,
}

impl ShellyConfig {
    fn configured_switch_names(self) -> Result<HashMap<u8, String>, String> {
        let mut names = HashMap::new();
        for (component, value) in self.components {
            if !component.starts_with("switch:") {
                continue;
            }
            let config: ShellySwitchConfig = serde_json::from_value(value)
                .map_err(|error| format!("Invalid {component} configuration: {error}"))?;
            if let Some(name) = config.name.filter(|name| !name.trim().is_empty()) {
                names.insert(config.id, name);
            }
        }
        Ok(names)
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

    /// Switch components with power metering support.
    #[serde(flatten, deserialize_with = "deserialize_pm")]
    pm: Vec<ShellyPMStatus>,

    #[serde(rename = "temperature:0")]
    temperature: Option<HashMap<String, serde_json::Value>>,
    wifi: Option<HashMap<String, serde_json::Value>>,
}

impl ShellyStatus {
    fn apply_pm_names(&mut self, names: &HashMap<u8, String>) {
        self.pm.retain_mut(|status| {
            if let Some(name) = names.get(&status.id) {
                status.name.clone_from(name);
                true
            } else {
                false
            }
        });
    }

    fn get_error(&self) -> Option<String> {
        self.em
            .as_ref()
            .and_then(|s| {
                if !s.errors.is_empty() {
                    Some(format!(
                        "Shelly[{}] EM error: {}",
                        self.id,
                        s.errors.join(", ")
                    ))
                } else {
                    None
                }
            })
            .or(self.em_data.as_ref().and_then(|s| {
                if !s.errors.is_empty() {
                    Some(format!(
                        "Shelly[{}] EM Data error: {}",
                        self.id,
                        s.errors.join(", ")
                    ))
                } else {
                    None
                }
            }))
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

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq)]
enum ShellyRequest {
    #[default]
    DeviceInfo,
    Config,
    Status,
}

impl ShellyRequest {
    fn uri(self) -> &'static str {
        match self {
            Self::DeviceInfo => DEVICE_INFO_URI,
            Self::Config => CONFIG_URI,
            Self::Status => STATUS_URI,
        }
    }
}

struct DigestChallenge {
    realm: String,
    nonce: String,
}

impl DigestChallenge {
    fn parse(header: &str) -> Result<Self, String> {
        if digest_parameter(header, "algorithm") != Some("SHA-256")
            || digest_parameter(header, "qop") != Some("auth")
        {
            return Err("Shelly Digest challenge must use SHA-256 and qop=auth".into());
        }
        let realm =
            digest_parameter(header, "realm").ok_or("Shelly Digest challenge is missing realm")?;
        let nonce =
            digest_parameter(header, "nonce").ok_or("Shelly Digest challenge is missing nonce")?;
        if realm.contains(['"', '\\']) || nonce.contains(['"', '\\']) {
            return Err("Shelly Digest challenge contains invalid characters".into());
        }
        Ok(Self {
            realm: realm.to_string(),
            nonce: nonce.to_string(),
        })
    }

    fn authorization(&self, password: &[u8], uri: &str) -> Result<HeaderValue, String> {
        let cnonce = format!("{:016x}", rand::random::<u64>());
        self.authorization_with_cnonce(password, uri, &cnonce)
    }

    fn authorization_with_cnonce(
        &self,
        password: &[u8],
        uri: &str,
        cnonce: &str,
    ) -> Result<HeaderValue, String> {
        const NC: &str = "00000001";
        let ha1 = sha256_hex(&[b"admin:", self.realm.as_bytes(), b":", password]);
        let ha2 = sha256_hex(&[b"GET:", uri.as_bytes()]);
        let response = sha256_hex(&[
            ha1.as_bytes(),
            b":",
            self.nonce.as_bytes(),
            b":",
            NC.as_bytes(),
            b":",
            cnonce.as_bytes(),
            b":auth:",
            ha2.as_bytes(),
        ]);

        let value = format!(
            "Digest username=\"admin\", realm=\"{}\", nonce=\"{}\", uri=\"{}\", algorithm=SHA-256, response=\"{}\", qop=auth, nc={NC}, cnonce=\"{}\"",
            self.realm, self.nonce, uri, response, cnonce,
        );
        let mut value = HeaderValue::from_str(&value)
            .map_err(|error| format!("Invalid digest authorization header: {error}"))?;
        value.set_sensitive(true);
        Ok(value)
    }
}

fn digest_parameter<'a>(header: &'a str, name: &str) -> Option<&'a str> {
    header
        .strip_prefix("Digest ")?
        .split(',')
        .find_map(|parameter| {
            let (parameter_name, value) = parameter.trim().split_once('=')?;
            (parameter_name == name).then(|| value.trim_matches('"'))
        })
}

fn sha256_hex(parts: &[&[u8]]) -> String {
    let mut digest = Sha256::new();
    for part in parts {
        digest.update(part);
    }
    format!("{:x}", digest.finalize())
}

/// Adaptor for [Shelly](https://shelly-api-docs.shelly.cloud/) components
#[derive(Adaptor)]
pub struct FetcherShellyAdaptor {
    request: ShellyRequest,

    /// Identifier of the device
    id: Option<String>,

    switch_names: HashMap<u8, String>,
    password: Option<Vec<u8>>,
    digest_challenge: Option<DigestChallenge>,

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

macro_rules! observe_pm_instantaneous {
    ($value:expr, $observer:expr, $status:expr, $type:expr) => {
        if let Some(value) = $value {
            $observer.observe(
                value,
                &[
                    KeyValue::new("name", $status.name.clone()),
                    KeyValue::new("id", $status.id as i64),
                    KeyValue::new("type", $type),
                ],
            );
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
        let password = proc.settings.password()?;

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
                for pm_status in &shelly_status.pm {
                    observe_pm_instantaneous!(
                        pm_status.apower,
                        observer,
                        pm_status,
                        "active_power"
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
                for pm_status in &shelly_status.pm {
                    if let Some(energy) = &pm_status.aenergy {
                        observer.observe(
                            energy.total,
                            &[
                                KeyValue::new("name", pm_status.name.clone()),
                                KeyValue::new("id", pm_status.id as i64),
                                KeyValue::new("type", "power"),
                            ],
                        );
                    }
                    if let Some(energy) = &pm_status.ret_aenergy {
                        observer.observe(
                            energy.total,
                            &[
                                KeyValue::new("name", pm_status.name.clone()),
                                KeyValue::new("id", pm_status.id as i64),
                                KeyValue::new("type", "ret_power"),
                            ],
                        );
                    }
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
            request: ShellyRequest::default(),
            id: None,
            switch_names: HashMap::new(),
            password,
            digest_challenge: None,
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
    ) -> Result<Request<BoxBody<Bytes, Infallible>>, FetcherError<M>> {
        let uri = self.request.uri();
        request_builder = request_builder
            .method(Method::GET)
            .uri(uri)
            .header(hyper::header::ACCEPT, "application/json");
        if let (Some(password), Some(challenge)) = (&self.password, &self.digest_challenge) {
            let authorization = challenge
                .authorization(password, uri)
                .map_err(FetcherError::Other)?;
            request_builder
                .headers_mut()
                .ok_or_else(|| FetcherError::Other("Invalid Shelly request builder".into()))?
                .insert(hyper::header::AUTHORIZATION, authorization);
        }
        let request = request_builder.body(BoxBody::default())?;
        trace!("Send Shelly request: {request:?}");
        Ok(request)
    }

    async fn process_http_response(
        &mut self,
        response: Result<Response<Incoming>, FetcherError<M>>,
    ) -> Result<FetchAction<M>, FetcherError<M>> {
        let response = response?;
        trace!("Receive Shelly response: {response:?}");
        if response.status() == StatusCode::UNAUTHORIZED {
            if self.password.is_none() {
                return Err(FetcherError::Other(
                    "Shelly authentication is required; configure a base64-url encoded password in the target URL"
                        .into(),
                ));
            }
            if self.digest_challenge.is_some() {
                return Err(FetcherError::Other(
                    "Shelly digest authentication failed".into(),
                ));
            }
            let challenge = response
                .headers()
                .get(hyper::header::WWW_AUTHENTICATE)
                .ok_or_else(|| {
                    FetcherError::Other(
                        "Shelly returned 401 without a WWW-Authenticate header".into(),
                    )
                })?
                .to_str()
                .map_err(|error| {
                    FetcherError::Other(format!("Invalid Shelly WWW-Authenticate header: {error}"))
                })?;
            self.digest_challenge =
                Some(DigestChallenge::parse(challenge).map_err(FetcherError::Other)?);
            return Ok(FetchAction::Http);
        }
        if response.status() != StatusCode::OK {
            return Err(FetcherError::Other(format!(
                "Shelly API returned HTTP {}",
                response.status()
            )));
        }

        self.digest_challenge = None;
        let body = Limited::new(response.into_body(), MAX_RESPONSE_BYTES)
            .collect()
            .await
            .map_err(|error| {
                FetcherError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid Shelly response body: {error}"),
                ))
            })?
            .to_bytes();

        match self.request {
            ShellyRequest::DeviceInfo => {
                let device_info: HashMap<String, serde_json::Value> = serde_json::from_slice(&body)
                    .map_err(|error| {
                        FetcherError::Io(io::Error::new(io::ErrorKind::InvalidData, error))
                    })?;
                debug!("Shelly device info: {device_info:?}");
                self.id = device_info
                    .get("name")
                    .and_then(|value| value.as_str())
                    .filter(|name| !name.trim().is_empty())
                    .or_else(|| device_info.get("id").and_then(|value| value.as_str()))
                    .map(str::to_string);
                if self.id.is_none() {
                    return Err(FetcherError::Other(
                        "Shelly device info is missing name and id".into(),
                    ));
                }
                self.request = ShellyRequest::Config;
                Ok(FetchAction::Http)
            }
            ShellyRequest::Config => {
                let config: ShellyConfig = serde_json::from_slice(&body).map_err(|error| {
                    FetcherError::Io(io::Error::new(io::ErrorKind::InvalidData, error))
                })?;
                self.switch_names = config
                    .configured_switch_names()
                    .map_err(FetcherError::Other)?;
                self.request = ShellyRequest::Status;
                Ok(FetchAction::Http)
            }
            ShellyRequest::Status => {
                let mut shelly_status: ShellyStatus =
                    serde_json::from_slice(&body).map_err(|error| {
                        FetcherError::Io(io::Error::new(io::ErrorKind::InvalidData, error))
                    })?;
                shelly_status.id = self
                    .id
                    .clone()
                    .ok_or_else(|| FetcherError::Other("Shelly device id is unavailable".into()))?;
                shelly_status.apply_pm_names(&self.switch_names);

                if let Some(shelly_error) = shelly_status.get_error() {
                    warn!(name = shelly_status.id, "{shelly_error}");
                } else {
                    debug!("Shelly status: {shelly_status:?}");
                }
                let _ = self.shelly_status.send(shelly_status);
                Ok(FetchAction::None)
            }
        }
    }

    fn end_active_period(&mut self) {
        self.request = ShellyRequest::default();
        self.id = None;
        self.switch_names.clear();
        self.digest_challenge = None;
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

    #[test]
    fn discovers_only_named_pm_channels() {
        let config: ShellyConfig = serde_json::from_str(
            r#"{
                "sys": {"device": {"name": null}},
                "switch:0": {"id": 0, "name": "Chauffe-eau"},
                "switch:1": {"id": 1, "name": null},
                "switch:2": {"id": 2, "name": "Prise Terrasse"},
                "switch:3": {"id": 3, "name": "Lum. Terrasse"}
            }"#,
        )
        .unwrap();

        let names = config.configured_switch_names().unwrap();
        assert_eq!(names.len(), 3);
        assert_eq!(names.get(&0).map(String::as_str), Some("Chauffe-eau"));
        assert_eq!(names.get(&2).map(String::as_str), Some("Prise Terrasse"));
        assert_eq!(names.get(&3).map(String::as_str), Some("Lum. Terrasse"));
        assert!(!names.contains_key(&1));
    }

    #[test]
    fn parses_pm_power_and_energy_for_named_channels() {
        let names = HashMap::from([
            (0, "Chauffe-eau".to_string()),
            (2, "Prise Terrasse".to_string()),
            (3, "Lum. Terrasse".to_string()),
        ]);
        let mut status: ShellyStatus = serde_json::from_str(
            r#"{
                "sys": {"mac": "C8F09E844AF8"},
                "wifi": {"rssi": -55},
                "switch:0": {
                    "id": 0, "apower": 1250.5, "voltage": 234.7, "current": 5.328,
                    "aenergy": {"total": 1569460.0}, "ret_aenergy": {"total": 0.0}
                },
                "switch:1": {
                    "id": 1, "apower": 42.0,
                    "aenergy": {"total": 100.0}, "ret_aenergy": {"total": 0.0}
                },
                "switch:2": {
                    "id": 2, "apower": 10.25, "voltage": 234.8, "current": 0.044,
                    "aenergy": {"total": 11551.0}, "ret_aenergy": {"total": 54.0}
                },
                "switch:3": {
                    "id": 3, "apower": 3.5, "errors": ["overpower"],
                    "aenergy": {"total": 5.0}, "ret_aenergy": {"total": 0.0}
                }
            }"#,
        )
        .unwrap();

        assert_eq!(status.pm.len(), 4);
        status.apply_pm_names(&names);
        assert_eq!(status.pm.len(), 3);
        assert_eq!(status.pm[0].name, "Chauffe-eau");
        assert_eq!(status.pm[0].apower, Some(1250.5));
        assert_eq!(
            status.pm[0].aenergy.as_ref().map(|energy| energy.total),
            Some(1569460.0)
        );
        assert_eq!(status.pm[1].id, 2);
        assert_eq!(
            status.pm[1].ret_aenergy.as_ref().map(|energy| energy.total),
            Some(54.0)
        );
    }

    #[test]
    fn builds_shelly_digest_authorization() {
        let challenge = DigestChallenge::parse(
            r#"Digest qop="auth", realm="shellypro4pm-test", nonce="abc", algorithm=SHA-256"#,
        )
        .unwrap();

        let authorization = challenge
            .authorization_with_cnonce(b"password", STATUS_URI, "0123456789abcdef")
            .unwrap();
        let authorization = authorization.to_str().unwrap();
        assert!(authorization.contains("username=\"admin\""));
        assert!(authorization.contains("nc=00000001"));
        assert!(
            authorization.contains(
                "response=\"ef4a1e6b16e41e6635282d6a0c214426a9c0057bec3a164ae56a843e9b4493e8\""
            ),
            "{authorization}"
        );
    }

    #[test]
    fn rejects_unsupported_digest_challenges() {
        assert!(
            DigestChallenge::parse(r#"Basic realm="shellypro4pm-c8f09e844af8", algorithm=SHA-256"#)
                .is_err()
        );
        assert!(
            DigestChallenge::parse(
                r#"Digest realm="shellypro4pm-c8f09e844af8", nonce="abc", qop="auth", algorithm=MD5"#
            )
            .is_err()
        );
    }
}
