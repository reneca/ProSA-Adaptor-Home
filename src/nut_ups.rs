//! Read-only NUT UPS metrics over TCP.

use bytes::Bytes;
use prosa::{
    core::{adaptor::Adaptor, proc::ProcConfig as _},
    io::stream::{Stream, TargetSetting},
    otel::KeyValue,
    tracing,
};
use prosa_fetcher::{
    adaptor::FetcherAdaptor,
    proc::{FetchAction, FetcherError, FetcherProc},
};
use tokio::{io::AsyncReadExt as _, sync::watch};
use tracing::warn;

const MAX_RESPONSE_BYTES: usize = 64 * 1024;

#[derive(Debug, Default, Clone, PartialEq)]
struct NutSnapshot {
    ups: String,
    realpower: Option<f64>,
    battery_charge: Option<f64>,
    ups_load: Option<f64>,
    battery_runtime: Option<f64>,
    status: Option<String>,
}

fn ups_name<M: Send>(target: &TargetSetting) -> Result<&str, FetcherError<M>> {
    let name = target.url.path().strip_prefix('/').unwrap_or_default();
    if target.url.scheme() != "tcp"
        || target.url.host_str().is_none()
        || target.url.port().is_none()
        || !target.url.username().is_empty()
        || target.url.password().is_some()
        || target.url.query().is_some()
        || target.url.fragment().is_some()
        || name.is_empty()
        || name.len() > 64
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
    {
        return Err(FetcherError::Other(
            "NUT target must be tcp://host:port/<UPS name> with a simple UPS name".into(),
        ));
    }
    Ok(name)
}

fn parse_value(value: &str) -> Result<String, &'static str> {
    let inner = value
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .ok_or("NUT VAR value must be quoted")?;
    let mut decoded = String::with_capacity(inner.len());
    let mut chars = inner.chars();
    while let Some(ch) = chars.next() {
        let ch = if ch == '\\' {
            chars.next().ok_or("trailing NUT escape")?
        } else {
            if ch == '"' {
                return Err("unescaped quote in NUT value");
            }
            ch
        };
        if ch.is_control() {
            return Err("control character in NUT value");
        }
        decoded.push(ch);
    }
    Ok(decoded)
}

fn parse_number(value: &str) -> Result<f64, &'static str> {
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit() || b == b'.') {
        return Err("invalid NUT numeric value");
    }
    let number = value
        .parse::<f64>()
        .map_err(|_| "invalid NUT numeric value")?;
    if !number.is_finite() {
        return Err("non-finite NUT numeric value");
    }
    Ok(number)
}

fn split_token(input: &str) -> Option<(&str, &str)> {
    let input = input.trim_start_matches([' ', '\t']);
    if input.is_empty() {
        return None;
    }
    let end = input.find([' ', '\t']).unwrap_or(input.len());
    Some((&input[..end], &input[end..]))
}

fn parse_var(line: &str, ups: &str, snapshot: &mut NutSnapshot) -> Result<(), &'static str> {
    let (command, rest) = split_token(line).ok_or("missing NUT VAR command")?;
    let (line_ups, rest) = split_token(rest).ok_or("missing NUT UPS name")?;
    if command != "VAR" || line_ups != ups {
        return Err("unexpected NUT VAR framing or UPS name");
    }
    let (name, rest) = split_token(rest).ok_or("missing NUT variable name")?;
    if name.is_empty()
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        return Err("invalid NUT variable name");
    }
    let value = parse_value(rest.trim_matches([' ', '\t']))?;
    match name {
        "ups.realpower" => snapshot.realpower = Some(parse_number(&value)?),
        "battery.charge" => snapshot.battery_charge = Some(parse_number(&value)?),
        "ups.load" => snapshot.ups_load = Some(parse_number(&value)?),
        "battery.runtime" => snapshot.battery_runtime = Some(parse_number(&value)?),
        "ups.status" => snapshot.status = Some(value),
        _ => {}
    }
    Ok(())
}

async fn read_snapshot(mut stream: Stream, ups: &str) -> Result<NutSnapshot, &'static str> {
    let mut snapshot = NutSnapshot {
        ups: ups.into(),
        ..Default::default()
    };
    let mut buffer = Vec::new();
    let mut received = 0usize;
    let mut began = false;
    let mut chunk = [0u8; 4096];
    loop {
        let size = stream
            .read(&mut chunk)
            .await
            .map_err(|_| "NUT read failed")?;
        if size == 0 {
            return Err("NUT response ended before END LIST VAR");
        }
        received += size;
        if received > MAX_RESPONSE_BYTES {
            return Err("NUT response exceeds 64 KiB");
        }
        buffer.extend_from_slice(&chunk[..size]);
        while let Some(pos) = buffer.iter().position(|&b| b == b'\n') {
            let mut line = buffer.drain(..=pos).collect::<Vec<_>>();
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            let line = std::str::from_utf8(&line).map_err(|_| "NUT response is not UTF-8")?;
            if line.split_ascii_whitespace().next() == Some("ERR") {
                return Err("NUT server returned ERR");
            }
            if !began {
                if !line
                    .split_ascii_whitespace()
                    .eq(["BEGIN", "LIST", "VAR", ups])
                {
                    return Err("missing BEGIN LIST VAR");
                }
                began = true;
            } else if line
                .split_ascii_whitespace()
                .eq(["END", "LIST", "VAR", ups])
            {
                return if buffer.is_empty() {
                    Ok(snapshot)
                } else {
                    Err("data after END LIST VAR")
                };
            } else {
                parse_var(line, ups, &mut snapshot)?;
            }
        }
    }
}

/// Fetches variables from a NUT `upsd` server using `LIST VAR`.
#[derive(Adaptor)]
pub struct FetcherNutUpsAdaptor {
    snapshot: watch::Sender<Option<NutSnapshot>>,
    success: watch::Sender<(String, u64)>,
}

impl<M> FetcherAdaptor<M> for FetcherNutUpsAdaptor
where
    M: 'static + Send + Sync + Sized + Clone + std::fmt::Debug + prosa::core::msg::Tvf + Default,
{
    fn new(proc: &FetcherProc<M>) -> Result<Self, FetcherError<M>> {
        let meter = proc.get_proc_param().meter("nut_ups");
        let (snapshot, watch_snapshot) = watch::channel(None::<NutSnapshot>);
        let (success, watch_success) = watch::channel((String::new(), 0u64));

        let data = watch_snapshot.clone();
        let _realpower = meter
            .f64_observable_gauge("prosa_nut_ups_realpower")
            .with_description("UPS real power in watts")
            .with_unit("W")
            .with_callback(move |observer| {
                if let Some(data) = data.borrow().as_ref()
                    && let Some(value) = data.realpower
                {
                    observer.observe(value, &[KeyValue::new("ups", data.ups.clone())]);
                }
            })
            .build();

        let data = watch_snapshot.clone();
        let _charge = meter
            .f64_observable_gauge("prosa_nut_battery_charge")
            .with_description("UPS battery charge in percent")
            .with_unit("%")
            .with_callback(move |observer| {
                if let Some(data) = data.borrow().as_ref()
                    && let Some(value) = data.battery_charge
                {
                    observer.observe(value, &[KeyValue::new("ups", data.ups.clone())]);
                }
            })
            .build();

        let data = watch_snapshot.clone();
        let _load = meter
            .f64_observable_gauge("prosa_nut_ups_load")
            .with_description("UPS load in percent")
            .with_unit("%")
            .with_callback(move |observer| {
                if let Some(data) = data.borrow().as_ref()
                    && let Some(value) = data.ups_load
                {
                    observer.observe(value, &[KeyValue::new("ups", data.ups.clone())]);
                }
            })
            .build();

        let data = watch_snapshot.clone();
        let _runtime = meter
            .f64_observable_gauge("prosa_nut_battery_runtime")
            .with_description("UPS battery runtime in seconds")
            .with_unit("s")
            .with_callback(move |observer| {
                if let Some(data) = data.borrow().as_ref()
                    && let Some(value) = data.battery_runtime
                {
                    observer.observe(value, &[KeyValue::new("ups", data.ups.clone())]);
                }
            })
            .build();

        let data = watch_snapshot;
        let _status = meter
            .u64_observable_gauge("prosa_nut_ups_status")
            .with_description("Current NUT UPS status, identified by the status label")
            .with_callback(move |observer| {
                if let Some(data) = data.borrow().as_ref()
                    && let Some(status) = data.status.as_ref()
                {
                    observer.observe(
                        1,
                        &[
                            KeyValue::new("ups", data.ups.clone()),
                            KeyValue::new("status", status.clone()),
                        ],
                    );
                }
            })
            .build();

        let _success = meter
            .u64_observable_gauge("prosa_nut_fetch_success")
            .with_description("Whether the most recent NUT fetch succeeded")
            .with_callback(move |observer| {
                let (ups, value) = &*watch_success.borrow();
                if !ups.is_empty() {
                    observer.observe(*value, &[KeyValue::new("ups", ups.clone())]);
                }
            })
            .build();

        Ok(Self { snapshot, success })
    }

    fn fetch(&mut self) -> Result<FetchAction<M>, FetcherError<M>> {
        Ok(FetchAction::Tcp)
    }

    fn create_tcp_request(&self, target: &TargetSetting) -> Result<Bytes, FetcherError<M>> {
        Ok(Bytes::from(format!("LIST VAR {}\n", ups_name(target)?)))
    }

    async fn process_tcp_response(
        &mut self,
        target: &TargetSetting,
        response: Result<Stream, FetcherError<M>>,
    ) -> Result<FetchAction<M>, FetcherError<M>> {
        let ups = ups_name(target)?.to_string();
        match response {
            Ok(stream) => match read_snapshot(stream, &ups).await {
                Ok(data) => {
                    let _ = self.snapshot.send(Some(data));
                    let _ = self.success.send((ups, 1));
                }
                Err(error) => {
                    warn!(%error, "NUT fetch failed");
                    let _ = self.snapshot.send(None);
                    let _ = self.success.send((ups, 0));
                }
            },
            Err(error) => {
                warn!(%error, "NUT fetch failed");
                let _ = self.snapshot.send(None);
                let _ = self.success.send((ups, 0));
            }
        }
        Ok(FetchAction::None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prosa_utils::msg::simple_string_tvf::SimpleStringTvf;
    use std::time::Duration;
    use tokio::{
        io::AsyncWriteExt as _,
        net::{TcpListener, TcpStream},
        time,
    };
    use url::Url;

    async fn from_server(
        chunks: Vec<Vec<u8>>,
        keep_open: bool,
    ) -> Result<NutSnapshot, &'static str> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            for chunk in chunks {
                socket.write_all(&chunk).await.unwrap();
                tokio::task::yield_now().await;
            }
            if keep_open {
                time::sleep(Duration::from_secs(1)).await;
            }
        });
        let socket = TcpStream::connect(addr).await.unwrap();
        let result = read_snapshot(Stream::Tcp(socket), "eaton").await;
        server.abort();
        result
    }

    #[tokio::test]
    async fn reads_split_response_without_waiting_for_eof() {
        time::timeout(Duration::from_secs(1), async {
            let data = from_server(vec![
                b"BEGIN LIST VAR eaton\nVAR eaton ups.realpower \"245\"\nVAR eaton battery.".to_vec(),
                b"charge \"91\"\nVAR eaton ups.load \"42.5\"\nVAR eaton battery.runtime \"1200\"\n".to_vec(),
                b"VAR eaton ups.status \"OL CHRG\"\nEND LIST VAR eaton\n".to_vec(),
            ], true).await.unwrap();
            assert_eq!(data.realpower, Some(245.0));
            assert_eq!(data.battery_charge, Some(91.0));
            assert_eq!(data.ups_load, Some(42.5));
            assert_eq!(data.battery_runtime, Some(1200.0));
            assert_eq!(data.status.as_deref(), Some("OL CHRG"));
        }).await.expect("NUT parser should stop at END LIST VAR");
    }

    #[tokio::test]
    async fn omits_missing_variables_and_does_not_substitute_apparent_power() {
        time::timeout(Duration::from_secs(1), async {
            let data = from_server(
                vec![
                    b"BEGIN LIST VAR eaton\nVAR eaton ups.power \"500\"\nEND LIST VAR eaton\n"
                        .to_vec(),
                ],
                false,
            )
            .await
            .unwrap();
            assert_eq!(data.realpower, None);
            assert_eq!(data.battery_charge, None);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn accepts_protocol_whitespace_and_escaped_values() {
        time::timeout(Duration::from_secs(1), async {
            let data = from_server(
                vec![b"BEGIN\tLIST  VAR eaton\nVAR\t eaton\tups.status  \"OL \\\"CHRG\\\"\"\nEND\tLIST VAR eaton\n".to_vec()],
                false,
            ).await.unwrap();
            assert_eq!(data.status.as_deref(), Some("OL \"CHRG\""));
        }).await.unwrap();
    }

    #[tokio::test]
    async fn rejects_nut_errors_bad_framing_and_oversized_responses() {
        time::timeout(Duration::from_secs(1), async {
            for payload in [
                b"ERR DATA-STALE\n".to_vec(),
                b"BEGIN LIST VAR other\nEND LIST VAR other\n".to_vec(),
                b"BEGIN LIST VAR eaton\nVAR other ups.status \"OL\"\nEND LIST VAR eaton\n".to_vec(),
                b"BEGIN LIST VAR eaton\nVAR eaton ups.load \"bad\"\nEND LIST VAR eaton\n".to_vec(),
                b"BEGIN LIST VAR eaton\nVAR eaton ups.status \"OL\"\n".to_vec(),
            ] {
                assert!(from_server(vec![payload], false).await.is_err());
            }
            assert!(
                from_server(vec![vec![b'x'; MAX_RESPONSE_BYTES + 1]], false)
                    .await
                    .is_err()
            );
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn validates_ups_name_before_building_command() {
        time::timeout(Duration::from_secs(1), async {
            let target = TargetSetting::from(Url::parse("tcp://127.0.0.1:3493/eaton").unwrap());
            assert_eq!(ups_name::<()>(&target).unwrap(), "eaton");
            for url in [
                "tcp://127.0.0.1:3493/",
                "tcp://127.0.0.1:3493/a%0Ab",
                "tcp://127.0.0.1:3493/a/b",
                "http://127.0.0.1:3493/eaton",
            ] {
                let target = TargetSetting::from(Url::parse(url).unwrap());
                assert!(ups_name::<()>(&target).is_err());
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn publishes_only_complete_snapshots_and_clears_failed_fetches() {
        time::timeout(Duration::from_secs(1), async {
            let (snapshot, watch_snapshot) = watch::channel(None);
            let (success, watch_success) = watch::channel((String::new(), 0));
            let mut adaptor = FetcherNutUpsAdaptor { snapshot, success };

            for (response, expected_power, expected_success) in [
                (
                    b"BEGIN LIST VAR eaton\nVAR eaton ups.realpower \"250\"\n".as_slice(),
                    None,
                    0,
                ),
                (
                    b"BEGIN LIST VAR eaton\nVAR eaton ups.realpower \"250\"\nEND LIST VAR eaton\n"
                        .as_slice(),
                    Some(250.0),
                    1,
                ),
                (b"ERR DATA-STALE\n".as_slice(), None, 0),
            ] {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let response = response.to_vec();
                let server = tokio::spawn(async move {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    socket.write_all(&response).await.unwrap();
                });
                let socket = TcpStream::connect(addr).await.unwrap();
                let target =
                    TargetSetting::from(Url::parse(&format!("tcp://{addr}/eaton")).unwrap());
                <FetcherNutUpsAdaptor as FetcherAdaptor<SimpleStringTvf>>::process_tcp_response(
                    &mut adaptor,
                    &target,
                    Ok(Stream::Tcp(socket)),
                )
                .await
                .unwrap();
                assert_eq!(
                    watch_snapshot
                        .borrow()
                        .as_ref()
                        .and_then(|data| data.realpower),
                    expected_power
                );
                assert_eq!(watch_success.borrow().1, expected_success);
                server.await.unwrap();
            }
        })
        .await
        .unwrap();
    }
}
