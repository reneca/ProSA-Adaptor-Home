//! Fetcher adaptor for [Frebbox](https://dev.freebox.fr/sdk/os/#) french internet provider box

use std::{collections::HashMap, convert::Infallible};

use bytes::{Buf as _, Bytes};
use hmac::Hmac;
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt as _, Full, combinators::BoxBody};
use hyper::body::Incoming;
use opentelemetry::KeyValue;
use prosa::core::{adaptor::Adaptor, proc::ProcConfig};
use prosa_fetcher::{
    adaptor::FetcherAdaptor,
    proc::{FetchAction, FetcherError, FetcherProc, FetcherSettings},
};
use serde::Deserialize;
use serde_json::{Map, Value};
use tokio::sync::watch;
use tracing::{debug, warn};

#[derive(Debug, Default, Copy, Clone, PartialEq)]
pub enum FreeboxFetchState {
    #[default]
    Connection,
    System,
    SwitchStatus,
    SwitchPort(u8),
    End,
}

impl FreeboxFetchState {
    /// Getter of the URI for the current Freebox call to do
    pub fn call(&self) -> Option<(Method, hyper::Uri)> {
        match self {
            FreeboxFetchState::Connection => Some((
                Method::GET,
                "/api/v4/connection/".parse::<hyper::Uri>().unwrap(),
            )),
            FreeboxFetchState::System => Some((
                Method::GET,
                "/api/v4/system/".parse::<hyper::Uri>().unwrap(),
            )),
            FreeboxFetchState::SwitchStatus => Some((
                Method::GET,
                "/api/v4/switch/status/".parse::<hyper::Uri>().unwrap(),
            )),
            FreeboxFetchState::SwitchPort(port) => Some((
                Method::GET,
                format!("/api/v4/switch/port/{port}/stats")
                    .parse::<hyper::Uri>()
                    .unwrap(),
            )),
            _ => None,
        }
    }

    pub fn next_state(&self, number_ports: u8) -> FreeboxFetchState {
        match self {
            FreeboxFetchState::Connection => FreeboxFetchState::System,
            FreeboxFetchState::System => FreeboxFetchState::SwitchStatus,
            FreeboxFetchState::SwitchStatus => FreeboxFetchState::SwitchPort(number_ports),
            FreeboxFetchState::SwitchPort(port) => {
                // Port ID start at 1
                if *port > 1 {
                    FreeboxFetchState::SwitchPort(port - 1)
                } else {
                    FreeboxFetchState::End
                }
            }
            _ => FreeboxFetchState::End,
        }
    }
}

#[derive(Default, Debug, Clone, Deserialize)]
pub struct FreeboxApiResponse {
    success: bool,
    result: Option<HashMap<String, Value>>,
}

impl FreeboxApiResponse {
    pub fn get_string(&self, key: &str) -> Option<String> {
        self.result
            .as_ref()
            .and_then(|m| m.get(key).and_then(|v| v.as_str().map(|s| s.to_owned())))
    }

    pub fn get_u64(&self, key: &str) -> Option<u64> {
        self.result.as_ref().and_then(|m| {
            m.get(key).and_then(|v| match v {
                Value::Number(n) => n.as_u64(),
                Value::String(s) => s.parse().ok(),
                _ => None,
            })
        })
    }
}

/// Adaptor for [Freebox](https://dev.freebox.fr/sdk/os/#) french internet provider box
#[derive(Adaptor)]
pub struct FetcherFreeboxAdaptor {
    settings: FetcherSettings,
    challenge_freebox: Option<String>,
    session_token: Option<String>,
    state: FreeboxFetchState,
    number_ports: u8,

    // Observability
    meter_conn: watch::Sender<FreeboxApiResponse>,
    meter_system: watch::Sender<FreeboxApiResponse>,
    meter_switch: watch::Sender<Vec<Map<String, Value>>>,
    meter_eth: watch::Sender<Vec<FreeboxApiResponse>>,
}

impl<M> FetcherAdaptor<M> for FetcherFreeboxAdaptor
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
    fn new(proc: &FetcherProc<M>) -> Result<Self, FetcherError<M>>
    where
        Self: std::marker::Sized,
    {
        let (meter_conn, watch_conn) = watch::channel(FreeboxApiResponse::default());

        let watch_conn_byte = watch_conn.clone();
        let _observable_byte = proc
            .get_proc_param()
            .meter("freebox")
            .u64_observable_counter("prosa_freebox_byte")
            .with_description("Freebox byte counter")
            .with_callback(move |observer| {
                let conn = watch_conn_byte.borrow();
                if let (Some(conn_type), Some(up)) =
                    (conn.get_string("type"), conn.get_u64("bytes_up"))
                {
                    observer.observe(
                        up,
                        &[
                            KeyValue::new("conn", conn_type),
                            KeyValue::new("flow", "send"),
                        ],
                    );
                }

                if let (Some(conn_type), Some(down)) =
                    (conn.get_string("type"), conn.get_u64("bytes_down"))
                {
                    observer.observe(
                        down,
                        &[
                            KeyValue::new("conn", conn_type),
                            KeyValue::new("flow", "recv"),
                        ],
                    );
                }
            })
            .build();

        let watch_conn_rate = watch_conn.clone();
        let _observable_rate = proc
            .get_proc_param()
            .meter("freebox")
            .u64_observable_gauge("prosa_freebox_rate")
            .with_description("Freebox rate gauge")
            .with_callback(move |observer| {
                let conn = watch_conn_rate.borrow();
                if let (Some(conn_type), Some(up)) =
                    (conn.get_string("type"), conn.get_u64("rate_up"))
                {
                    observer.observe(
                        up,
                        &[
                            KeyValue::new("conn", conn_type),
                            KeyValue::new("type", "rate"),
                            KeyValue::new("flow", "send"),
                        ],
                    );
                }

                if let (Some(conn_type), Some(down)) =
                    (conn.get_string("type"), conn.get_u64("rate_down"))
                {
                    observer.observe(
                        down,
                        &[
                            KeyValue::new("conn", conn_type),
                            KeyValue::new("type", "rate"),
                            KeyValue::new("flow", "recv"),
                        ],
                    );
                }

                if let (Some(conn_type), Some(up)) =
                    (conn.get_string("type"), conn.get_u64("bandwidth_up"))
                {
                    observer.observe(
                        up,
                        &[
                            KeyValue::new("conn", conn_type),
                            KeyValue::new("type", "bandwidth"),
                            KeyValue::new("flow", "send"),
                        ],
                    );
                }

                if let (Some(conn_type), Some(down)) =
                    (conn.get_string("type"), conn.get_u64("bandwidth_down"))
                {
                    observer.observe(
                        down,
                        &[
                            KeyValue::new("conn", conn_type),
                            KeyValue::new("type", "bandwidth"),
                            KeyValue::new("flow", "recv"),
                        ],
                    );
                }
            })
            .build();

        let _observable_state = proc
            .get_proc_param()
            .meter("freebox")
            .u64_observable_gauge("prosa_freebox_state")
            .with_description(
                "State of the Freebox (0 for down, 1 for going down, 2 for going up, 3 for up)",
            )
            .with_callback(move |observer| {
                let conn = watch_conn.borrow();
                if let (Some(conn_type), Some(state)) =
                    (conn.get_string("type"), conn.get_string("state"))
                {
                    let state_enum = match state.as_str() {
                        "up" => 3,
                        "going_up" => 2,
                        "going_down" => 1,
                        _ => 0,
                    };
                    observer.observe(state_enum, &[KeyValue::new("conn", conn_type)]);
                }
            })
            .build();

        let (meter_system, watch_system) = watch::channel(FreeboxApiResponse::default());
        let _observable_temp = proc
            .get_proc_param()
            .meter("freebox")
            .u64_observable_gauge("prosa_freebox_temp")
            .with_description("Temperatures of the Freebox")
            .with_callback(move |observer| {
                let conn = watch_system.borrow();
                if let (Some(board_name), Some(temp)) =
                    (conn.get_string("board_name"), conn.get_u64("temp_cpum"))
                {
                    observer.observe(
                        temp,
                        &[
                            KeyValue::new("board", board_name),
                            KeyValue::new("type", "cpum"),
                        ],
                    );
                }

                if let (Some(board_name), Some(temp)) =
                    (conn.get_string("board_name"), conn.get_u64("temp_sw"))
                {
                    observer.observe(
                        temp,
                        &[
                            KeyValue::new("board", board_name),
                            KeyValue::new("type", "sw"),
                        ],
                    );
                }

                if let (Some(board_name), Some(temp)) =
                    (conn.get_string("board_name"), conn.get_u64("temp_cpub"))
                {
                    observer.observe(
                        temp,
                        &[
                            KeyValue::new("board", board_name),
                            KeyValue::new("type", "cpub"),
                        ],
                    );
                }

                if let (Some(board_name), Some(temp)) =
                    (conn.get_string("board_name"), conn.get_u64("temp_t1"))
                {
                    observer.observe(
                        temp,
                        &[
                            KeyValue::new("board", board_name),
                            KeyValue::new("type", "t1"),
                        ],
                    );
                }

                if let (Some(board_name), Some(temp)) =
                    (conn.get_string("board_name"), conn.get_u64("temp_t2"))
                {
                    observer.observe(
                        temp,
                        &[
                            KeyValue::new("board", board_name),
                            KeyValue::new("type", "t2"),
                        ],
                    );
                }

                if let (Some(board_name), Some(rpm)) =
                    (conn.get_string("board_name"), conn.get_u64("fan_rpm"))
                {
                    observer.observe(
                        rpm,
                        &[
                            KeyValue::new("board", board_name),
                            KeyValue::new("type", "fan"),
                        ],
                    );
                }
            })
            .build();

        let (meter_switch, watch_switch) = watch::channel(Vec::<Map<String, Value>>::new());
        let watch_switch_status = watch_switch.clone();
        let _observable_switch_status =
            proc.get_proc_param()
                .meter("freebox")
                .u64_observable_gauge("prosa_freebox_switch_status")
                .with_description("Status of the Freebox switch port")
                .with_callback(move |observer| {
                    let switch = watch_switch_status.borrow();
                    for port in switch.iter() {
                        if let Some(port_id) = port.get("id").and_then(|i| i.as_u64()) {
                            match port.get("link").and_then(|s| s.as_str()) {
                                Some("up") => observer
                                    .observe(1, &[KeyValue::new("port", port_id.to_string())]),
                                Some("down") => observer
                                    .observe(0, &[KeyValue::new("port", port_id.to_string())]),
                                _ => {}
                            }
                        }
                    }
                })
                .build();

        let _observable_switch_speed = proc
            .get_proc_param()
            .meter("freebox")
            .u64_observable_gauge("prosa_freebox_switch_speed")
            .with_description("Negociate speed of the Freebox switch port")
            .with_callback(move |observer| {
                let switch = watch_switch.borrow();
                for port in switch.iter() {
                    if let (Some(port_id), Some(speed)) = (
                        port.get("id").and_then(|i| i.as_u64()),
                        port.get("speed")
                            .and_then(|i| i.as_str().and_then(|s| s.parse().ok())),
                    ) {
                        observer.observe(speed, &[KeyValue::new("port", port_id.to_string())]);
                    }
                }
            })
            .build();

        let (meter_eth, watch_eth) = watch::channel(Vec::<FreeboxApiResponse>::new());
        let watch_eth_byte = watch_eth.clone();
        let _observable_eth_byte = proc
            .get_proc_param()
            .meter("freebox")
            .u64_observable_counter("prosa_freebox_switch_bytes")
            .with_description("Bytes process by the Freebox switch port")
            .with_callback(move |observer| {
                let eth = watch_eth_byte.borrow();
                for (port_id, port) in eth.iter().enumerate() {
                    if let Some(rx_bytes) = port.get_u64("rx_good_bytes") {
                        observer.observe(
                            rx_bytes,
                            &[
                                KeyValue::new("port", port_id.to_string()),
                                KeyValue::new("flow", "recv"),
                            ],
                        );
                    }

                    if let Some(tx_bytes) = port.get_u64("tx_bytes") {
                        observer.observe(
                            tx_bytes,
                            &[
                                KeyValue::new("port", port_id.to_string()),
                                KeyValue::new("flow", "send"),
                            ],
                        );
                    }
                }
            })
            .build();

        let _observable_eth_packet = proc
            .get_proc_param()
            .meter("freebox")
            .u64_observable_counter("prosa_freebox_switch_packets")
            .with_description("Packets process by the Freebox switch port")
            .with_callback(move |observer| {
                let eth = watch_eth.borrow();
                for (port_id, port) in eth.iter().enumerate() {
                    if let Some(rx_packets) = port.get_u64("rx_good_packets") {
                        observer.observe(
                            rx_packets,
                            &[
                                KeyValue::new("port", port_id.to_string()),
                                KeyValue::new("flow", "recv"),
                            ],
                        );
                    }

                    if let Some(tx_packets) = port.get_u64("tx_packets") {
                        observer.observe(
                            tx_packets,
                            &[
                                KeyValue::new("port", port_id.to_string()),
                                KeyValue::new("flow", "send"),
                            ],
                        );
                    }

                    if let Some(rx_packets) = port.get_u64("rx_err_packets") {
                        observer.observe(
                            rx_packets,
                            &[
                                KeyValue::new("port", port_id.to_string()),
                                KeyValue::new("flow", "recv_err"),
                            ],
                        );
                    }

                    if let Some(tx_packets) = port.get_u64("tx_collisions") {
                        observer.observe(
                            tx_packets,
                            &[
                                KeyValue::new("port", port_id.to_string()),
                                KeyValue::new("flow", "send_err"),
                            ],
                        );
                    }
                }
            })
            .build();

        Ok(Self {
            settings: proc.settings.clone(),
            challenge_freebox: None,
            session_token: None,
            state: FreeboxFetchState::End,
            number_ports: 0,
            meter_conn,
            meter_system,
            meter_switch,
            meter_eth,
        })
    }

    fn fetch(&mut self) -> Result<FetchAction<M>, FetcherError<M>> {
        // Call HTTP to retrieve statistics with first state
        self.state = FreeboxFetchState::default();
        Ok(FetchAction::Http)
    }

    fn create_http_request(
        &self,
        mut request_builder: http::request::Builder,
    ) -> Result<Request<BoxBody<hyper::body::Bytes, Infallible>>, FetcherError<M>> {
        if self.challenge_freebox.is_none() {
            // Get a challenge to login after
            request_builder = request_builder
                .method(Method::GET)
                .uri("/api/v4/login/".parse::<hyper::Uri>().unwrap())
                .header(hyper::header::CONNECTION, "keep-alive")
                .header(hyper::header::ACCEPT, "application/json");
            let request = request_builder.body(BoxBody::default())?;
            Ok(request)
        } else if let Some(challenge_freebox) = &self.challenge_freebox
            && self.session_token.is_none()
        {
            // Get a session token to login
            if let (Some(username), Some(challenge)) = (
                self.settings.username(),
                self.settings
                    .challenge_password::<Hmac<sha1::Sha1>, M>(challenge_freebox.as_bytes())?,
            ) {
                let json_data =
                    format!("{{\"app_id\":\"{username}\",\"password\":\"{challenge:02x}\"}}");
                request_builder = request_builder
                    .method(Method::POST)
                    .uri("/api/v4/login/session/".parse::<hyper::Uri>().unwrap())
                    .header(hyper::header::CONNECTION, "keep-alive")
                    .header(hyper::header::ACCEPT, "application/json")
                    .header(hyper::header::CONTENT_TYPE, "application/json")
                    .header(hyper::header::CONTENT_LENGTH, json_data.len().to_string());
                let request =
                    request_builder.body(BoxBody::new(Full::new(Bytes::from(json_data))))?;
                Ok(request)
            } else {
                Err(FetcherError::Other(
                    "Can't retrieve `challenge` from remote".to_string(),
                ))
            }
        } else if let Some((method, uri)) = self.state.call() {
            // Send request depending of the state
            request_builder = request_builder
                .method(method)
                .uri(uri)
                .header(hyper::header::CONNECTION, "keep-alive")
                .header(hyper::header::ACCEPT, "application/json")
                .header("X-Fbx-App-Auth", self.session_token.as_ref().unwrap());
            let request = request_builder.body(BoxBody::default())?;
            Ok(request)
        } else {
            Err(FetcherError::Other(
                "Can't get URI for remote API call".to_string(),
            ))
        }
    }

    async fn process_http_response(
        &mut self,
        response: Result<Response<Incoming>, FetcherError<M>>,
    ) -> Result<FetchAction<M>, FetcherError<M>> {
        match response {
            Ok(response) => {
                if self.challenge_freebox.is_none() {
                    match response.status() {
                        StatusCode::OK => {
                            let server = response
                                .headers()
                                .get(http::header::SERVER)
                                .and_then(|s| s.to_str().ok().map(|h| h.to_string()));
                            let body = response
                                .collect()
                                .await
                                .map_err(|e| FetcherError::Hyper(e, server.unwrap_or_default()))?
                                .aggregate();

                            // Parse the login return to get the challenge value
                            let login_json: FreeboxApiResponse =
                                serde_json::from_reader(body.reader())
                                    .map_err(|e| FetcherError::Io(e.into()))?;
                            if let Some(challenge) = login_json.get_string("challenge") {
                                self.challenge_freebox = Some(challenge.to_string());

                                // Go for next call to retrieve `session_token`
                                Ok(FetchAction::Http)
                            } else {
                                Err(FetcherError::Other(
                                    "Can't retrieve `challenge` from remote".to_string(),
                                ))
                            }
                        }
                        code => Err(FetcherError::Other(format!(
                            "Receive error from HTTP remote for challenge: {code}"
                        ))),
                    }
                } else if self.session_token.is_none() {
                    match response.status() {
                        StatusCode::OK => {
                            let server = response
                                .headers()
                                .get(http::header::SERVER)
                                .and_then(|s| s.to_str().ok().map(|h| h.to_string()));
                            let body = response
                                .collect()
                                .await
                                .map_err(|e| FetcherError::Hyper(e, server.unwrap_or_default()))?
                                .aggregate();

                            // Parse the login return to get the challenge value
                            let login_json: FreeboxApiResponse =
                                serde_json::from_reader(body.reader())
                                    .map_err(|e| FetcherError::Io(e.into()))?;
                            if let Some(token) = login_json.get_string("session_token") {
                                self.session_token = Some(token.to_string());

                                // Go for next call to get all statistics
                                Ok(FetchAction::Http)
                            } else {
                                Err(FetcherError::Other(
                                    "Can't retrieve `session_token` from remote for session"
                                        .to_string(),
                                ))
                            }
                        }
                        code => Err(FetcherError::Other(format!(
                            "Receive error from HTTP remote: {code}"
                        ))),
                    }
                } else {
                    match response.status() {
                        StatusCode::OK => {
                            let server = response
                                .headers()
                                .get(http::header::SERVER)
                                .and_then(|s| s.to_str().ok().map(|h| h.to_string()));
                            let body = response
                                .collect()
                                .await
                                .map_err(|e| FetcherError::Hyper(e, server.unwrap_or_default()))?
                                .aggregate();

                            if self.state == FreeboxFetchState::SwitchStatus {
                                let switch_status_resp: Value =
                                    serde_json::from_reader(body.reader())
                                        .map_err(|e| FetcherError::Io(e.into()))?;
                                if let Some(switch_status_array) =
                                    switch_status_resp.get("result").and_then(|r| r.as_array())
                                {
                                    self.number_ports = switch_status_array.len() as u8;
                                    let _ = self.meter_switch.send(
                                        switch_status_array
                                            .iter()
                                            .map(|v| {
                                                if let Some(value) = v.as_object() {
                                                    value.clone()
                                                } else {
                                                    Map::new()
                                                }
                                            })
                                            .collect(),
                                    );
                                    self.state = self.state.next_state(self.number_ports);
                                    Ok(FetchAction::Http)
                                } else {
                                    Ok(FetchAction::None)
                                }
                            } else {
                                // Parse the API response return to get the data
                                let api_resp: FreeboxApiResponse =
                                    serde_json::from_reader(body.reader())
                                        .map_err(|e| FetcherError::Io(e.into()))?;
                                if api_resp.success {
                                    match self.state {
                                        FreeboxFetchState::Connection => {
                                            let _ = self.meter_conn.send(api_resp);
                                        }
                                        FreeboxFetchState::System => {
                                            let _ = self.meter_system.send(api_resp);
                                        }
                                        FreeboxFetchState::SwitchPort(port_id) => {
                                            let mut eth = self.meter_eth.borrow().clone();
                                            if !eth.is_empty() {
                                                eth[(port_id - 1) as usize] = api_resp;
                                            } else {
                                                eth =
                                                    Vec::with_capacity(self.number_ports as usize);
                                                for _ in 0..self.number_ports {
                                                    eth.push(FreeboxApiResponse::default());
                                                }
                                                eth[(port_id - 1) as usize] = api_resp;
                                            }
                                            let _ = self.meter_eth.send(eth);
                                        }
                                        _ => {}
                                    }
                                } else {
                                    warn!(
                                        "API[{:?}] respond with an error: {api_resp:?}",
                                        self.state
                                    );
                                    return Ok(FetchAction::None);
                                }

                                self.state = self.state.next_state(self.number_ports);
                                if self.state != FreeboxFetchState::End {
                                    // Call for next state
                                    Ok(FetchAction::Http)
                                } else {
                                    // Every call have been made
                                    Ok(FetchAction::None)
                                }
                            }
                        }
                        code => Err(FetcherError::Other(format!(
                            "Receive error from HTTP remote: {code}"
                        ))),
                    }
                }
            }
            Err(FetcherError::Hyper(he, addr)) => {
                if he.is_canceled() {
                    debug!(addr = addr, "HTTP error {:?}", he);
                    Ok(FetchAction::None)
                } else {
                    warn!(addr = addr, "HTTP error {:?}", he);
                    Err(FetcherError::Hyper(he, addr))
                }
            }
            Err(e) => Err(e),
        }
    }
}
