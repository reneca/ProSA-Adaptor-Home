//! Fetcher adaptor for [Frebbox](https://dev.freebox.fr/sdk/os/#) french internet provider box

use std::{collections::HashMap, convert::Infallible};

use bytes::{Buf as _, Bytes};
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
pub enum BBoxFetchState {
    #[default]
    Cpu,
    Mem,
    Wan,
    Lan,
    Wifi(bool),
    End,
}

impl BBoxFetchState {
    /// Getter of the URI for the current Freebox call to do
    pub fn call(&self) -> Option<(Method, hyper::Uri)> {
        match self {
            BBoxFetchState::Cpu => Some((
                Method::GET,
                "/api/v1/device/cpu".parse::<hyper::Uri>().unwrap(),
            )),
            BBoxFetchState::Mem => Some((
                Method::GET,
                "/api/v1/device/mem".parse::<hyper::Uri>().unwrap(),
            )),
            BBoxFetchState::Wan => Some((
                Method::GET,
                "/api/v1/wan/ip/stats".parse::<hyper::Uri>().unwrap(),
            )),
            BBoxFetchState::Lan => Some((
                Method::GET,
                "/api/v1/lan/stats".parse::<hyper::Uri>().unwrap(),
            )),
            BBoxFetchState::Wifi(high) => {
                if *high {
                    Some((
                        Method::GET,
                        "/api/v1/wireless/5/stats".parse::<hyper::Uri>().unwrap(),
                    ))
                } else {
                    Some((
                        Method::GET,
                        "/api/v1/wireless/24/stats".parse::<hyper::Uri>().unwrap(),
                    ))
                }
            }
            _ => None,
        }
    }

    pub fn next_state(&self) -> BBoxFetchState {
        match self {
            BBoxFetchState::Cpu => BBoxFetchState::Mem,
            BBoxFetchState::Mem => BBoxFetchState::Wan,
            BBoxFetchState::Wan => BBoxFetchState::Lan,
            BBoxFetchState::Lan => BBoxFetchState::Wifi(false),
            BBoxFetchState::Wifi(false) => BBoxFetchState::Wifi(true),
            BBoxFetchState::Wifi(true) => BBoxFetchState::End,
            _ => BBoxFetchState::End,
        }
    }
}

#[derive(Default, Debug, Clone, Deserialize)]
pub struct BBoxApiResponse {
    device: Option<HashMap<String, Value>>,
    wan: Option<HashMap<String, Value>>,
    lan: Option<HashMap<String, Value>>,
    wireless: Option<HashMap<String, Value>>,
}

impl BBoxApiResponse {
    pub fn merge(&mut self, other: Self) {
        if let Some(device_other) = other.device {
            if let Some(device) = self.device.as_mut() {
                device.extend(device_other);
            } else {
                self.device = Some(device_other);
            }
        }

        if let Some(wan_other) = other.wan {
            if let Some(wan) = self.wan.as_mut() {
                wan.extend(wan_other);
            } else {
                self.wan = Some(wan_other);
            }
        }

        if let Some(lan_other) = other.lan {
            if let Some(lan) = self.lan.as_mut() {
                lan.extend(lan_other);
            } else {
                self.lan = Some(lan_other);
            }
        }

        // Merge 2.4 and 5 GHz each other on different key
        if let Some(wireless_other) = other.wireless {
            if let Some(wireless) = self.wireless.as_mut()
                && let Some(ssid) = wireless_other.get("ssid").and_then(|s| s.as_object())
                && let Some(id) = ssid.get("id").and_then(|i| i.as_number())
            {
                wireless.insert(id.to_string(), Value::Object(ssid.clone()));
            } else if let Some(ssid) = wireless_other.get("ssid").and_then(|s| s.as_object())
                && let Some(id) = ssid.get("id").and_then(|i| i.as_number())
            {
                let mut wireless_map = HashMap::with_capacity(2);
                wireless_map.insert(id.to_string(), Value::Object(ssid.clone()));
                self.wireless = Some(wireless_map);
            }
        }
    }

    pub fn take(&mut self) -> Self {
        Self {
            device: self.device.take(),
            wan: self.wan.take(),
            lan: self.lan.take(),
            wireless: self.wireless.take(),
        }
    }

    pub fn get_wan_stats(&self) -> Option<&Map<String, Value>> {
        self.wan.as_ref().and_then(|w| {
            w.get("ip")
                .and_then(|ip| ip.get("stats").and_then(|s| s.as_object()))
        })
    }

    pub fn get_lan_stats(&self) -> Option<Vec<&Map<String, Value>>> {
        self.lan.as_ref().and_then(|l| {
            l.get("stats").and_then(|s| {
                s.get("port").and_then(|p| {
                    p.as_array()
                        .and_then(|a| a.iter().map(|l| l.as_object()).collect())
                })
            })
        })
    }

    pub fn get_wifi_stats(&self) -> Option<Vec<&Map<String, Value>>> {
        self.wireless
            .as_ref()
            .and_then(|w| w.values().map(|s| s.as_object()).collect())
    }

    pub fn parse_u64(v: &Value) -> Option<u64> {
        match v {
            Value::Number(n) => n.as_u64(),
            Value::String(s) => s.parse().ok(),
            _ => None,
        }
    }
}

/// Adaptor for [Freebox](https://dev.freebox.fr/sdk/os/#) french internet provider box
#[derive(Adaptor)]
pub struct FetcherBBoxAdaptor {
    settings: FetcherSettings,
    bbox_id: Option<String>,
    state: BBoxFetchState,
    stats: BBoxApiResponse,

    // Observability
    meter_bbox: watch::Sender<BBoxApiResponse>,
}

impl<M> FetcherAdaptor<M> for FetcherBBoxAdaptor
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
        let (meter_bbox, watch_bbox) = watch::channel(BBoxApiResponse::default());

        let watch_system = watch_bbox.clone();
        let _observable_temp = proc
            .get_proc_param()
            .meter("bbox")
            .u64_observable_gauge("prosa_bbox_system")
            .with_description("System information of the BBox")
            .with_callback(move |observer| {
                if let Some(device) = &watch_system.borrow().device {
                    if let Some(cpu) = device.get("cpu").and_then(|c| c.as_object()) {
                        if let Some(time) = cpu.get("time") {
                            if let Some(total) =
                                time.get("total").and_then(BBoxApiResponse::parse_u64)
                            {
                                observer.observe(
                                    total,
                                    &[KeyValue::new("type", "cpu"), KeyValue::new("time", "total")],
                                );
                            }

                            if let Some(idle) =
                                time.get("idle").and_then(BBoxApiResponse::parse_u64)
                            {
                                observer.observe(
                                    idle,
                                    &[KeyValue::new("type", "cpu"), KeyValue::new("time", "idle")],
                                );
                            }
                        }

                        if let Some(process) = cpu.get("process") {
                            if let Some(created) =
                                process.get("created").and_then(BBoxApiResponse::parse_u64)
                            {
                                observer.observe(
                                    created,
                                    &[
                                        KeyValue::new("type", "process"),
                                        KeyValue::new("process", "created"),
                                    ],
                                );
                            }

                            if let Some(running) =
                                process.get("running").and_then(BBoxApiResponse::parse_u64)
                            {
                                observer.observe(
                                    running,
                                    &[
                                        KeyValue::new("type", "process"),
                                        KeyValue::new("process", "running"),
                                    ],
                                );
                            }

                            if let Some(blocked) =
                                process.get("blocked").and_then(BBoxApiResponse::parse_u64)
                            {
                                observer.observe(
                                    blocked,
                                    &[
                                        KeyValue::new("type", "process"),
                                        KeyValue::new("process", "blocked"),
                                    ],
                                );
                            }
                        }

                        if let Some(temp) = cpu.get("temperature")
                            && let Some(main) =
                                temp.get("main").and_then(BBoxApiResponse::parse_u64)
                        {
                            observer.observe(
                                main,
                                &[KeyValue::new("type", "temp"), KeyValue::new("temp", "main")],
                            );
                        }
                    }

                    if let Some(mem) = device.get("mem").and_then(|c| c.as_object()) {
                        if let Some(total) = mem.get("total").and_then(BBoxApiResponse::parse_u64) {
                            observer.observe(
                                total,
                                &[KeyValue::new("type", "mem"), KeyValue::new("mem", "total")],
                            );
                        }

                        if let Some(free) = mem.get("free").and_then(BBoxApiResponse::parse_u64) {
                            observer.observe(
                                free,
                                &[KeyValue::new("type", "mem"), KeyValue::new("mem", "free")],
                            );
                        }

                        if let Some(cached) = mem.get("cached").and_then(BBoxApiResponse::parse_u64)
                        {
                            observer.observe(
                                cached,
                                &[KeyValue::new("type", "mem"), KeyValue::new("mem", "cached")],
                            );
                        }
                    }
                }
            })
            .build();

        let watch_bytes = watch_bbox.clone();
        let _observable_rate =
            proc.get_proc_param()
                .meter("bbox")
                .u64_observable_counter("prosa_bbox_bytes")
                .with_description("BBox bytes counter")
                .with_callback(move |observer| {
                    let bbox_api = watch_bytes.borrow();
                    if let Some(wan_stats) = bbox_api.get_wan_stats() {
                        if let Some(rx) = wan_stats
                            .get("rx")
                            .and_then(|r| r.get("bytes").and_then(BBoxApiResponse::parse_u64))
                        {
                            observer.observe(
                                rx,
                                &[KeyValue::new("type", "wan"), KeyValue::new("flow", "recv")],
                            );
                        }

                        if let Some(tx) = wan_stats
                            .get("tx")
                            .and_then(|t| t.get("bytes").and_then(BBoxApiResponse::parse_u64))
                        {
                            observer.observe(
                                tx,
                                &[KeyValue::new("type", "wan"), KeyValue::new("flow", "send")],
                            );
                        }
                    }

                    if let Some(lan_stats) = bbox_api.get_lan_stats() {
                        for (lan_id, lan) in lan_stats.iter().enumerate() {
                            if let Some(rx) = lan
                                .get("rx")
                                .and_then(|r| r.get("bytes").and_then(BBoxApiResponse::parse_u64))
                            {
                                observer.observe(
                                    rx,
                                    &[
                                        KeyValue::new("port", lan_id.to_string()),
                                        KeyValue::new("type", "lan"),
                                        KeyValue::new("flow", "recv"),
                                    ],
                                );
                            }

                            if let Some(tx) = lan
                                .get("tx")
                                .and_then(|t| t.get("bytes").and_then(BBoxApiResponse::parse_u64))
                            {
                                observer.observe(
                                    tx,
                                    &[
                                        KeyValue::new("port", lan_id.to_string()),
                                        KeyValue::new("type", "lan"),
                                        KeyValue::new("flow", "send"),
                                    ],
                                );
                            }
                        }
                    }

                    if let Some(wifi_stats) = bbox_api.get_wifi_stats() {
                        for wifi in wifi_stats {
                            if let (Some(wifi_id), Some(wifi_stat)) = (
                                wifi.get("id").map(|i| i.to_string()),
                                wifi.get("stats").and_then(|s| s.as_object()),
                            ) {
                                if let Some(rx) = wifi_stat.get("rx").and_then(|r| {
                                    r.get("bytes").and_then(BBoxApiResponse::parse_u64)
                                }) {
                                    observer.observe(
                                        rx,
                                        &[
                                            KeyValue::new("band", wifi_id.clone()),
                                            KeyValue::new("type", "wifi"),
                                            KeyValue::new("flow", "recv"),
                                        ],
                                    );
                                }

                                if let Some(tx) = wifi_stat.get("tx").and_then(|t| {
                                    t.get("bytes").and_then(BBoxApiResponse::parse_u64)
                                }) {
                                    observer.observe(
                                        tx,
                                        &[
                                            KeyValue::new("band", wifi_id),
                                            KeyValue::new("type", "wifi"),
                                            KeyValue::new("flow", "send"),
                                        ],
                                    );
                                }
                            }
                        }
                    }
                })
                .build();

        let watch_packets = watch_bbox.clone();
        let _observable_rate =
            proc.get_proc_param()
                .meter("bbox")
                .u64_observable_counter("prosa_bbox_packets")
                .with_description("BBox packets counter")
                .with_callback(move |observer| {
                    let bbox_api = watch_packets.borrow();
                    if let Some(wan_stats) = bbox_api.get_wan_stats() {
                        if let Some(rx) = wan_stats
                            .get("rx")
                            .and_then(|r| r.get("packets").and_then(BBoxApiResponse::parse_u64))
                        {
                            observer.observe(
                                rx,
                                &[KeyValue::new("type", "wan"), KeyValue::new("flow", "recv")],
                            );
                        }

                        if let Some(rx) = wan_stats.get("rx").and_then(|r| {
                            r.get("packetserrors").and_then(BBoxApiResponse::parse_u64)
                        }) {
                            observer.observe(
                                rx,
                                &[
                                    KeyValue::new("type", "wan"),
                                    KeyValue::new("flow", "recv_err"),
                                ],
                            );
                        }

                        if let Some(rx) = wan_stats.get("rx").and_then(|r| {
                            r.get("packetsdiscards")
                                .and_then(BBoxApiResponse::parse_u64)
                        }) {
                            observer.observe(
                                rx,
                                &[
                                    KeyValue::new("type", "wan"),
                                    KeyValue::new("flow", "recv_discard"),
                                ],
                            );
                        }

                        if let Some(tx) = wan_stats
                            .get("tx")
                            .and_then(|t| t.get("packets").and_then(BBoxApiResponse::parse_u64))
                        {
                            observer.observe(
                                tx,
                                &[KeyValue::new("type", "wan"), KeyValue::new("flow", "send")],
                            );
                        }

                        if let Some(tx) = wan_stats.get("tx").and_then(|t| {
                            t.get("packetserrors").and_then(BBoxApiResponse::parse_u64)
                        }) {
                            observer.observe(
                                tx,
                                &[
                                    KeyValue::new("type", "wan"),
                                    KeyValue::new("flow", "send_err"),
                                ],
                            );
                        }

                        if let Some(tx) = wan_stats.get("tx").and_then(|t| {
                            t.get("packetsdiscards")
                                .and_then(BBoxApiResponse::parse_u64)
                        }) {
                            observer.observe(
                                tx,
                                &[
                                    KeyValue::new("type", "wan"),
                                    KeyValue::new("flow", "send_discard"),
                                ],
                            );
                        }
                    }

                    if let Some(lan_stats) = bbox_api.get_lan_stats() {
                        for (lan_id, lan) in lan_stats.iter().enumerate() {
                            if let Some(rx) = lan
                                .get("rx")
                                .and_then(|r| r.get("packets").and_then(BBoxApiResponse::parse_u64))
                            {
                                observer.observe(
                                    rx,
                                    &[
                                        KeyValue::new("port", lan_id.to_string()),
                                        KeyValue::new("type", "lan"),
                                        KeyValue::new("flow", "recv"),
                                    ],
                                );
                            }

                            if let Some(tx) = lan
                                .get("tx")
                                .and_then(|t| t.get("packets").and_then(BBoxApiResponse::parse_u64))
                            {
                                observer.observe(
                                    tx,
                                    &[
                                        KeyValue::new("port", lan_id.to_string()),
                                        KeyValue::new("type", "lan"),
                                        KeyValue::new("flow", "send"),
                                    ],
                                );
                            }
                        }
                    }

                    if let Some(wifi_stats) = bbox_api.get_wifi_stats() {
                        for wifi in wifi_stats {
                            if let (Some(wifi_id), Some(wifi_stat)) = (
                                wifi.get("id").map(|i| i.to_string()),
                                wifi.get("stats").and_then(|s| s.as_object()),
                            ) {
                                if let Some(rx) = wifi_stat.get("rx").and_then(|r| {
                                    r.get("packets").and_then(BBoxApiResponse::parse_u64)
                                }) {
                                    observer.observe(
                                        rx,
                                        &[
                                            KeyValue::new("band", wifi_id.clone()),
                                            KeyValue::new("type", "wifi"),
                                            KeyValue::new("flow", "recv"),
                                        ],
                                    );
                                }

                                if let Some(rx) = wifi_stat.get("rx").and_then(|r| {
                                    r.get("packetserrors").and_then(BBoxApiResponse::parse_u64)
                                }) {
                                    observer.observe(
                                        rx,
                                        &[
                                            KeyValue::new("band", wifi_id.clone()),
                                            KeyValue::new("type", "wifi"),
                                            KeyValue::new("flow", "recv_err"),
                                        ],
                                    );
                                }

                                if let Some(rx) = wifi_stat.get("rx").and_then(|r| {
                                    r.get("packetsdiscards")
                                        .and_then(BBoxApiResponse::parse_u64)
                                }) {
                                    observer.observe(
                                        rx,
                                        &[
                                            KeyValue::new("band", wifi_id.clone()),
                                            KeyValue::new("type", "wifi"),
                                            KeyValue::new("flow", "recv_discard"),
                                        ],
                                    );
                                }

                                if let Some(tx) = wifi_stat.get("tx").and_then(|t| {
                                    t.get("packets").and_then(BBoxApiResponse::parse_u64)
                                }) {
                                    observer.observe(
                                        tx,
                                        &[
                                            KeyValue::new("band", wifi_id.clone()),
                                            KeyValue::new("type", "wifi"),
                                            KeyValue::new("flow", "send"),
                                        ],
                                    );
                                }

                                if let Some(tx) = wifi_stat.get("tx").and_then(|t| {
                                    t.get("packetserrors").and_then(BBoxApiResponse::parse_u64)
                                }) {
                                    observer.observe(
                                        tx,
                                        &[
                                            KeyValue::new("band", wifi_id.clone()),
                                            KeyValue::new("type", "wifi"),
                                            KeyValue::new("flow", "send_err"),
                                        ],
                                    );
                                }

                                if let Some(tx) = wifi_stat.get("tx").and_then(|t| {
                                    t.get("packetsdiscards")
                                        .and_then(BBoxApiResponse::parse_u64)
                                }) {
                                    observer.observe(
                                        tx,
                                        &[
                                            KeyValue::new("band", wifi_id),
                                            KeyValue::new("type", "wifi"),
                                            KeyValue::new("flow", "send_discard"),
                                        ],
                                    );
                                }
                            }
                        }
                    }
                })
                .build();

        let watch_bandwidth = watch_bbox.clone();
        let _observable_rate =
            proc.get_proc_param()
                .meter("bbox")
                .u64_observable_gauge("prosa_bbox_bandwidth")
                .with_description("BBox bandwidth gauge")
                .with_callback(move |observer| {
                    let bbox_api = watch_bandwidth.borrow();
                    if let Some(wan_stats) = bbox_api.get_wan_stats() {
                        if let Some(rx) = wan_stats
                            .get("rx")
                            .and_then(|r| r.get("bandwidth").and_then(BBoxApiResponse::parse_u64))
                        {
                            observer.observe(
                                rx,
                                &[KeyValue::new("type", "wan"), KeyValue::new("flow", "recv")],
                            );
                        }

                        if let Some(tx) = wan_stats
                            .get("tx")
                            .and_then(|t| t.get("bandwidth").and_then(BBoxApiResponse::parse_u64))
                        {
                            observer.observe(
                                tx,
                                &[KeyValue::new("type", "wan"), KeyValue::new("flow", "send")],
                            );
                        }
                    }

                    if let Some(lan_stats) = bbox_api.get_lan_stats() {
                        for (lan_id, lan) in lan_stats.iter().enumerate() {
                            if let Some(rx) = lan.get("rx").and_then(|r| {
                                r.get("bandwidth").and_then(BBoxApiResponse::parse_u64)
                            }) {
                                observer.observe(
                                    rx,
                                    &[
                                        KeyValue::new("port", lan_id.to_string()),
                                        KeyValue::new("type", "lan"),
                                        KeyValue::new("flow", "recv"),
                                    ],
                                );
                            }

                            if let Some(tx) = lan.get("tx").and_then(|t| {
                                t.get("bandwidth").and_then(BBoxApiResponse::parse_u64)
                            }) {
                                observer.observe(
                                    tx,
                                    &[
                                        KeyValue::new("port", lan_id.to_string()),
                                        KeyValue::new("type", "lan"),
                                        KeyValue::new("flow", "send"),
                                    ],
                                );
                            }
                        }
                    }
                })
                .build();

        Ok(Self {
            settings: proc.settings.clone(),
            bbox_id: None,
            state: BBoxFetchState::End,
            stats: BBoxApiResponse::default(),
            meter_bbox,
        })
    }

    fn fetch(&mut self) -> Result<FetchAction<M>, FetcherError<M>> {
        // Call HTTP to retrieve statistics with first state
        self.state = BBoxFetchState::default();
        Ok(FetchAction::Http)
    }

    fn create_http_request(
        &self,
        mut request_builder: http::request::Builder,
    ) -> Result<Request<BoxBody<hyper::body::Bytes, Infallible>>, FetcherError<M>> {
        if self.bbox_id.is_none() {
            if let Some(Ok(password)) = self.settings.password()?.map(String::from_utf8) {
                // Get a challenge to login after
                request_builder = request_builder
                    .method(Method::POST)
                    .uri("/api/v1/login".parse::<hyper::Uri>().unwrap())
                    .header(hyper::header::CONNECTION, "keep-alive");
                let request = request_builder.body(BoxBody::new(Full::new(Bytes::from(
                    format!("password={password}"),
                ))))?;
                Ok(request)
            } else {
                Err(FetcherError::Other(
                    "Can't get password for remote API call".to_string(),
                ))
            }
        } else if let Some((method, uri)) = self.state.call() {
            // Send request depending of the state
            request_builder = request_builder
                .method(method)
                .uri(uri)
                .header(hyper::header::CONNECTION, "keep-alive")
                .header(hyper::header::ACCEPT, "application/json")
                .header(
                    hyper::header::COOKIE,
                    format!("BBOX_ID={}", self.bbox_id.as_ref().unwrap()),
                );
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
                if self.bbox_id.is_none() {
                    match response.status() {
                        StatusCode::OK => {
                            for cookie in
                                response.headers().get_all(hyper::header::SET_COOKIE).iter()
                            {
                                if let Ok(Some(bbox_id)) =
                                    cookie.to_str().map(|c| c.strip_prefix("BBOX_ID="))
                                {
                                    self.bbox_id = Some(
                                        bbox_id.split(';').next().unwrap_or(bbox_id).to_string(),
                                    );
                                }
                            }

                            if self.bbox_id.is_some() {
                                // Go for next call
                                Ok(FetchAction::Http)
                            } else {
                                Err(FetcherError::Other(
                                    "Can't retrieve `BBOX_ID` from remote".to_string(),
                                ))
                            }
                        }
                        code => Err(FetcherError::Other(format!(
                            "Receive error from HTTP remote for login: {code}"
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

                            // Parse the API response return to get the data
                            let api_resp: Vec<BBoxApiResponse> =
                                serde_json::from_reader(body.reader())
                                    .map_err(|e| FetcherError::Io(e.into()))?;
                            for bbox_api in api_resp {
                                self.stats.merge(bbox_api);
                            }

                            self.state = self.state.next_state();
                            if self.state != BBoxFetchState::End {
                                // Call for next state
                                Ok(FetchAction::Http)
                            } else {
                                // Every call have been made
                                let _ = self.meter_bbox.send(self.stats.take());
                                Ok(FetchAction::None)
                            }
                        }
                        StatusCode::UNAUTHORIZED => {
                            self.bbox_id = None;
                            // Ask for a new token (it may expired)
                            Ok(FetchAction::Http)
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
