//! Fetcher adaptor for [Deye](https://deye.com/fr/product/sun-m60-80-100g4-eu-q0/) solar inverter

use std::convert::Infallible;

use http::{Method, Request, Response, StatusCode, Uri, request};
use http_body_util::{BodyExt as _, combinators::BoxBody};
use hyper::body::Incoming;
use opentelemetry::KeyValue;
use prosa::core::{adaptor::Adaptor, proc::ProcConfig as _};
use prosa_fetcher::{
    adaptor::FetcherAdaptor,
    proc::{FetchAction, FetcherError, FetcherProc},
};
use tokio::sync::watch;
use tracing::{debug, warn};

#[derive(Debug, Default)]
struct DeyeSolarData {
    serial_number: String,
    current_power: u64,
    yield_today: f64,
    total_yield: f64,
    wireless_router_ssid: String,
    wireless_signal_quality: u8,
}

impl TryFrom<String> for DeyeSolarData {
    type Error = &'static str;

    fn try_from(data: String) -> Result<Self, Self::Error> {
        let mut serial_number = None;
        let mut current_power = None;
        let mut yield_today = None;
        let mut total_yield = None;
        let mut wireless_router_ssid = None;
        let mut wireless_signal_quality = None;
        for line in data.lines() {
            if line.starts_with("var ") {
                if let Some(sn) = line.strip_prefix("var webdata_sn = \"") {
                    serial_number = sn.strip_suffix("\";").map(|s| s.trim_end());
                } else if let Some(now_p) = line.strip_prefix("var webdata_now_p = \"") {
                    current_power = now_p
                        .strip_suffix("\";")
                        .map(|p| p.parse::<u64>().unwrap_or_default());
                } else if let Some(today_e) = line.strip_prefix("var webdata_today_e = \"") {
                    yield_today = today_e
                        .strip_suffix("\";")
                        .map(|p| p.parse::<f64>().unwrap_or_default());
                } else if let Some(total_e) = line.strip_prefix("var webdata_total_e = \"") {
                    total_yield = total_e
                        .strip_suffix("\";")
                        .map(|p| p.parse::<f64>().unwrap_or_default());
                } else if let Some(sta_ssid) = line.strip_prefix("var cover_sta_ssid = \"") {
                    wireless_router_ssid = sta_ssid.strip_suffix("\";");
                } else if let Some(sta_rssi) = line.strip_prefix("var cover_sta_rssi = \"") {
                    wireless_signal_quality = sta_rssi
                        .strip_suffix("%\";")
                        .map(|p| p.parse::<u8>().unwrap_or_default());
                }
            }
        }

        Ok(DeyeSolarData {
            serial_number: serial_number
                .ok_or("Missing serial number [webdata_sn]")?
                .to_string(),
            current_power: current_power.ok_or("Missing current power [webdata_now_p]")?,
            yield_today: yield_today.ok_or("Missing yield power today [webdata_today_e]")?,
            total_yield: total_yield.ok_or("Missing total yield power [webdata_total_e]")?,
            wireless_router_ssid: wireless_router_ssid
                .ok_or("Missing wireless SSID [cover_sta_ssid]")?
                .to_string(),
            wireless_signal_quality: wireless_signal_quality
                .ok_or("Missing wireless signal quality [cover_sta_rssi]")?,
        })
    }
}

/// Adaptor for [Deye](https://deye.com/fr/product/sun-m60-80-100g4-eu-q0/) solar inverter
#[derive(Adaptor)]
pub struct FetcherDeyeSolarAdaptor {
    uri_fetch: Uri,

    // Observability
    meter_solar: watch::Sender<DeyeSolarData>,
}

impl<M> FetcherAdaptor<M> for FetcherDeyeSolarAdaptor
where
    M: 'static
        + std::marker::Send
        + std::marker::Sync
        + std::marker::Sized
        + std::clone::Clone
        + std::fmt::Debug
        + prosa_utils::msg::tvf::Tvf
        + std::default::Default,
{
    fn new(proc: &FetcherProc<M>) -> Result<Self, FetcherError<M>> {
        let (meter_solar, watch_solar) = watch::channel(DeyeSolarData::default());

        let watch_power = watch_solar.clone();
        let _observable_power = proc
            .get_proc_param()
            .meter("deye_solar")
            .f64_observable_gauge("prosa_deye_solar_live_power")
            .with_description("Live power information of the Deye inverter")
            .with_callback(move |observer| {
                let solar_data = watch_power.borrow();
                if !solar_data.serial_number.is_empty() {
                    observer.observe(
                        solar_data.current_power as f64,
                        &[
                            KeyValue::new("sn", solar_data.serial_number.clone()),
                            KeyValue::new("type", "instantaneous"),
                        ],
                    );
                }
            })
            .init();

        let watch_power = watch_solar.clone();
        let _observable_power = proc
            .get_proc_param()
            .meter("deye_solar")
            .f64_observable_counter("prosa_deye_solar_power")
            .with_description("Power information of the Deye inverter")
            .with_callback(move |observer| {
                let solar_data = watch_power.borrow();
                if !solar_data.serial_number.is_empty() {
                    if solar_data.yield_today > 0f64 {
                        observer.observe(
                            solar_data.yield_today,
                            &[
                                KeyValue::new("sn", solar_data.serial_number.clone()),
                                KeyValue::new("type", "daily"),
                            ],
                        );
                    }

                    if solar_data.total_yield > 0f64 {
                        observer.observe(
                            solar_data.total_yield,
                            &[
                                KeyValue::new("sn", solar_data.serial_number.clone()),
                                KeyValue::new("type", "total"),
                            ],
                        );
                    }
                }
            })
            .init();

        let _observable_wireless = proc
            .get_proc_param()
            .meter("deye_solar")
            .u64_observable_gauge("prosa_deye_solar_wireless")
            .with_description("Wireless information of the Deye inverter")
            .with_callback(move |observer| {
                let solar_data = watch_solar.borrow();
                if !solar_data.serial_number.is_empty() {
                    observer.observe(
                        solar_data.wireless_signal_quality as u64,
                        &[
                            KeyValue::new("sn", solar_data.serial_number.clone()),
                            KeyValue::new("ssid", solar_data.wireless_router_ssid.clone()),
                        ],
                    );
                }
            })
            .init();

        Ok(FetcherDeyeSolarAdaptor {
            uri_fetch: "/status.html".parse::<hyper::Uri>().unwrap(),
            meter_solar,
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
        request_builder = request_builder
            .method(Method::GET)
            .uri(self.uri_fetch.clone())
            .header(hyper::header::CONNECTION, "keep-alive")
            .header(hyper::header::ACCEPT, "text/html");
        let request = request_builder.body(BoxBody::default())?;
        debug!("Send request: {:?}", request);
        Ok(request)
    }

    async fn process_http_response(
        &mut self,
        mut response: Response<Incoming>,
    ) -> Result<FetchAction<M>, FetcherError<M>> {
        debug!("Receive response: {:?}", response);
        match response.status() {
            StatusCode::OK => {
                let mut data = String::with_capacity(4096);
                while let Some(next) = response.frame().await {
                    if let Some(chunk) = next?.data_ref() {
                        data.push_str(
                            String::from_utf8(chunk.to_vec())
                                .map_err(|e| {
                                    FetcherError::Other(format!("UTF8 HTML format error `{e}`"))
                                })?
                                .as_str(),
                        );

                        // Wait until information variables are received
                        if data.contains("var status_c = ") {
                            break;
                        }
                    }
                }

                let solar_data =
                    DeyeSolarData::try_from(data).map_err(|e| FetcherError::Other(e.into()))?;
                debug!("solar_data: {solar_data:?}");
                let _ = self.meter_solar.send(solar_data);
                Ok(FetchAction::None)
            }
            StatusCode::UNAUTHORIZED => {
                if response
                    .headers()
                    .contains_key(hyper::header::WWW_AUTHENTICATE)
                {
                    // Recall with the credential
                    Ok(FetchAction::Http)
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
}
