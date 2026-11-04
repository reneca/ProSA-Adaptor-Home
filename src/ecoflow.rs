//! Read-only EcoFlow STREAM metrics through the IoT Open HTTP and MQTT APIs.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    convert::Infallible,
    fmt::Write as _,
    io,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use hmac::{Hmac, Mac as _};
use http::{Method, Request, Response, StatusCode, request};
use http_body_util::{BodyExt as _, Limited, combinators::BoxBody};
use hyper::body::Incoming;
use prosa::{
    core::{adaptor::Adaptor, proc::ProcConfig as _},
    io::stream::TargetSetting,
    otel::{KeyValue, metrics::AsyncInstrument},
    tracing::{debug, warn},
};
use prosa_fetcher::{
    adaptor::FetcherAdaptor,
    mqtt::{MqttEvent, MqttSession, QoS},
    proc::{FetchAction, FetcherError, FetcherProc},
};
use serde::Deserialize;
use serde_json::{Map, Value};
use sha2::Sha256;
use tokio::sync::watch;
use url::Url;

const MAX_RESPONSE_BYTES: usize = 256 * 1024;
/// Delay without quota update after which the MQTT session is renewed and the metrics are dropped.
/// The device publishes changes every few seconds and a full system snapshot every 2 minutes.
const QUOTA_STALE_AFTER: Duration = Duration::from_secs(5 * 60);
/// Serial number prefixes of STREAM models that report system telemetry through the Developer API:
/// Ultra, Pro, AC Pro, Max, AC and Ultra X.
const STREAM_SERIAL_PREFIXES: [&str; 6] = ["BK11", "BK12", "BK31", "BK41", "BK51", "BK61"];
/// Maximum number of battery packs
const MAX_PACKS: usize = 8;
/// Maximum length of a stored string, array or object
const MAX_STORED_LEN: usize = 64;
/// Key identifying the battery pack of a BMS report
const PACK_KEY: &str = "packSn";
const BATTERY_CHARGE_KEY: &str = "cmsBattSoc";

/// Measurement read from a quota key
struct Measure {
    /// Quota key, or `object.field` for a field of an object value
    key: &'static str,
    /// Label value of the measurement within its metric
    label: &'static str,
    /// Factor and divisor converting the quota value to the metric unit
    scale: f64,
    divisor: f64,
    /// Key that must be strictly positive for the measurement to be meaningful (e.g. a sensor count)
    requires: Option<&'static str>,
}

const fn measure(key: &'static str, label: &'static str) -> Measure {
    scaled(key, label, 1.0)
}

const fn scaled(key: &'static str, label: &'static str, scale: f64) -> Measure {
    Measure {
        key,
        label,
        scale,
        divisor: 1.0,
        requires: None,
    }
}

const fn divided(key: &'static str, label: &'static str, divisor: f64) -> Measure {
    Measure {
        divisor,
        ..scaled(key, label, 1.0)
    }
}

const fn requiring(measure: Measure, requires: &'static str) -> Measure {
    Measure {
        requires: Some(requires),
        ..measure
    }
}

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
const POWERS: [Measure; 19] = [
    measure("powGetPvSum", "solar"),
    measure("powGetBpCms", "battery"),
    measure("powGetSysLoad", "system_load"),
    measure("powGetSysLoadFromPv", "load_from_solar"),
    measure("powGetSysLoadFromBp", "load_from_battery"),
    measure("powGetSysLoadFromGrid", "load_from_grid"),
    measure("powGetSysGrid", "system_grid"),
    measure("gridConnectionPower", "grid_connection"),
    measure("sysGridConnectionPower", "system_grid_connection"),
    measure("acTotalActivePower", "ac_output"),
    measure("powGetSchuko1", "ac_outlet_1"),
    measure("powGetSchuko2", "ac_outlet_2"),
    measure("socketMeasurePower", "socket_measure"),
    measure("cloudMetter.phaseAPower", "meter_phase_1"),
    measure("cloudMetter.phaseBPower", "meter_phase_2"),
    measure("cloudMetter.phaseCPower", "meter_phase_3"),
    measure("offgrid1ActivePower", "offgrid_1"),
    measure("offgrid2ActivePower", "offgrid_2"),
    measure("ongridInActivePower", "inverter_grid"),
];
const APPARENT_POWERS: [Measure; 2] = [
    measure("offgrid1ApperentPower", "offgrid_1"),
    measure("offgrid2ApperentPower", "offgrid_2"),
];
const REACTIVE_POWERS: [Measure; 1] = [measure("ongridInReactivePower", "inverter_grid")];
const VOLTAGES: [Measure; 5] = [
    measure("gridConnectionVol", "grid"),
    divided("offgridVout1Rms", "offgrid_1", 10.0),
    divided("offgridVout2Rms", "offgrid_2", 10.0),
    divided("ongridVinRms", "inverter_grid_input", 10.0),
    divided("ongridVoutRms", "inverter_grid_output", 10.0),
];
const FREQUENCIES: [Measure; 2] = [
    measure("gridConnectionFreq", "grid"),
    divided("voltFreqfilter", "inverter", 100.0),
];
const BATTERY_CHARGES: [Measure; 1] = [measure(BATTERY_CHARGE_KEY, "")];
const BATTERY_HEALTHS: [Measure; 2] = [
    measure("cmsBattSoh", "system"),
    measure("bmsBattSoh", "main"),
];
const BATTERY_ENERGIES: [Measure; 1] = [measure("cmsBattFullEnergy", "full")];
const BATTERY_REMAINING_TIMES: [Measure; 2] = [
    scaled("cmsChgRemTime", "charge", 60.0),
    scaled("cmsDsgRemTime", "discharge", 60.0),
];
const SOC_LIMITS: [Measure; 3] = [
    measure("cmsMaxChgSoc", "max_charge"),
    measure("cmsMinDsgSoc", "min_discharge"),
    measure("backupReverseSoc", "backup_reserve"),
];
const POWER_LIMITS: [Measure; 12] = [
    measure("cmsBattPowInMax", "battery_input"),
    measure("cmsBattPowOutMax", "battery_output"),
    measure("powSysAcInMax", "ac_input"),
    measure("powSysAcOutMax", "ac_output"),
    measure("busbarPowLimit", "busbar"),
    measure("maxInvInput", "inverter_input"),
    measure("maxInvOutput", "inverter_output"),
    measure("maxBpInput", "pack_input"),
    measure("maxBpOutput", "pack_output"),
    measure("feedGridModePowLimit", "feed_grid"),
    measure("feedGridModePowMax", "feed_grid_max"),
    measure("sysGridInPwrLimit", "grid_input"),
];
const TEMPERATURES: [Measure; 10] = [
    measure("bmsMaxCellTemp", "battery_cell_max"),
    measure("bmsMinCellTemp", "battery_cell_min"),
    measure("bmsMaxMosTemp", "battery_mos_max"),
    measure("bmsMinMosTemp", "battery_mos_min"),
    measure("invTempNtc", "inverter"),
    measure("dcTemp1Ntc", "dc_1"),
    measure("dcTemp2Ntc", "dc_2"),
    measure("dcTemp3Ntc", "dc_3"),
    measure("dabHighTempNtc", "dab_high"),
    measure("dabLvTempNtc", "dab_low"),
];
const SWITCHES: [Measure; 15] = [
    measure("relay1Onoff", "relay_1"),
    measure("relay2Onoff", "relay_2"),
    measure("relay3Onoff", "relay_3"),
    measure("relay4Onoff", "relay_4"),
    measure("bmsBattHeating", "battery_heating"),
    measure("cmsBmsRunState", "bms_running"),
    measure("sysOffgrid", "offgrid"),
    measure("plugInInfoPvFlag", "solar_1_connected"),
    measure("plugInInfoPv2Flag", "solar_2_connected"),
    measure("plugInInfoPv3Flag", "solar_3_connected"),
    measure("plugInInfoPv4Flag", "solar_4_connected"),
    measure(
        "energyStrategyOperateMode.operateSelfPoweredOpen",
        "self_powered_mode",
    ),
    measure(
        "energyStrategyOperateMode.operateScheduledOpen",
        "scheduled_mode",
    ),
    measure("energyStrategyOperateMode.operateTouModeOpen", "tou_mode"),
    measure(
        "energyStrategyOperateMode.operateIntelligentScheduleModeOpen",
        "intelligent_schedule_mode",
    ),
];
const WIRELESS: [Measure; 1] = [measure("moduleWifiRssi", "")];
const FAN_LEVELS: [Measure; 1] = [measure("v1p0.fanLevel", "")];
const FAULTS: [Measure; 12] = [
    measure("invFault", "inverter"),
    measure("invFaultLock", "inverter_lock"),
    measure("invCommFault", "inverter_communication"),
    measure("dabFault", "dab"),
    measure("dabFaultLock", "dab_lock"),
    measure("pfcFault", "pfc"),
    measure("gridFault", "grid"),
    measure("mpptFaultComm", "mppt_communication"),
    measure("mpptPv1Fault", "mppt_1"),
    measure("mpptPv2Fault", "mppt_2"),
    measure("mpptPv3Fault", "mppt_3"),
    measure("mpptPv4Fault", "mppt_4"),
];
/// Names of numeric state values
type StateNames = &'static [(i64, &'static str)];
const CHARGE_STATES: StateNames = &[(0, "idle"), (1, "discharging"), (2, "charging")];
/// State name, quota key and names of its numeric values
const STATES: [(&str, &str, StateNames); 7] = [
    ("grid_connection", "gridConnectionSta", &[]),
    ("distributed_device", "distributedDeviceStatus", &[]),
    ("series_connection", "seriesConnectDeviceStatus", &[]),
    ("grid_code", "gridCodeSelection", &[]),
    ("feed_grid_mode", "feedGridMode", &[]),
    ("charge_discharge", "cmsChgDsgState", CHARGE_STATES),
    ("main_charge_discharge", "bmsChgDsgState", CHARGE_STATES),
];
const ERRORS_KEY: &str = "devErrcodeList.devErrcode";
const TIMEZONE_KEY: &str = "utcTimezoneId";
const IOT_VERSION_KEY: &str = "iotVersion";
/// Undocumented device values exported unscaled
const RAW_DEVICE_KEYS: [&str; 19] = [
    // Inverter AC measurements of unknown scale
    "invVcapRms",
    "ongridIinRms",
    "invIl1Rms",
    // Inverter power derating and capability limits, in W
    "actPwrRated",
    "actPwrByFreqDroop",
    "actPwrByOfdp",
    "actPwrByOvdp",
    "actPwrByPerc",
    "actPwrByRampUp",
    "actPwrByUfip",
    "addPwrActLimit",
    "acTotalInvDeratePower",
    "ongridInvDeratePower",
    "offgridInvDeratePower",
    "invInPowerAbilityMax",
    "invOutPowerAbilityMax",
    "ongridOutPowerAbilityMax",
    "bypassOutPowerAbilityMax",
    "chgOverCurOffCnt",
];
/// Undocumented battery pack values exported unscaled
const RAW_PACK_KEYS: [&str; 17] = [
    // Cell balancing
    "remainBalanceTime",
    "balanceCmd",
    "balanceState",
    "maxVolDiff",
    "diffSoc",
    // Status codes
    "sysState",
    "mosState",
    "chgDsgState",
    "afeSysStatus",
    "bqSysStatReg",
    "bsmSysEvent",
    "sysPwrFlag",
    "openBmsFlag",
    "waterInFlag",
    "mcuPinInStatus",
    "mcuPinOutStatus",
    "pwr12v",
];

const PACK_CHARGES: [Measure; 4] = [
    measure("soc", "reported"),
    measure("actSoc", "actual"),
    measure("targetSoc", "target"),
    measure("f32ShowSoc", "precise"),
];
const PACK_HEALTHS: [Measure; 4] = [
    measure("soh", "reported"),
    measure("realSoh", "real"),
    measure("cycleSoh", "cycle"),
    measure("calendarSoh", "calendar"),
];
const PACK_VOLTAGES: [Measure; 3] = [
    divided("vol", "pack", 1000.0),
    divided("maxCellVol", "cell_max", 1000.0),
    divided("minCellVol", "cell_min", 1000.0),
];
const PACK_CURRENTS: [Measure; 2] = [
    divided("amp", "measured", 1000.0),
    divided("tagChgAmp", "charge_target", 1000.0),
];
const PACK_CAPACITIES: [Measure; 3] = [
    divided("designCap", "design", 1000.0),
    divided("fullCap", "full", 1000.0),
    divided("remainCap", "remaining", 1000.0),
];
const PACK_CYCLES: [Measure; 1] = [measure("cycles", "")];
const PACK_ENERGIES: [Measure; 2] = [
    measure("accuChgEnergy", "charged"),
    measure("accuDsgEnergy", "discharged"),
];
const PACK_CHARGES_TRANSFERRED: [Measure; 2] = [
    divided("accuChgCap", "charged", 1000.0),
    divided("accuDsgCap", "discharged", 1000.0),
];
const PACK_POWERS: [Measure; 2] = [
    measure("inputWatts", "input"),
    measure("outputWatts", "output"),
];
const PACK_REMAINING_TIMES: [Measure; 1] = [scaled("remainTime", "", 60.0)];
const PACK_TEMPERATURES: [Measure; 11] = [
    measure("temp", "pack"),
    measure("maxCellTemp", "cell_max"),
    measure("minCellTemp", "cell_min"),
    measure("maxMosTemp", "mos_max"),
    measure("minMosTemp", "mos_min"),
    requiring(measure("maxEnvTemp", "env_max"), "envNtcNum"),
    requiring(measure("minEnvTemp", "env_min"), "envNtcNum"),
    requiring(
        measure("maxCurSensorTemp", "current_sensor_max"),
        "curSensorNtcNum",
    ),
    requiring(
        measure("minCurSensorTemp", "current_sensor_min"),
        "curSensorNtcNum",
    ),
    requiring(measure("maxHeatfilmTemp", "heatfilm_max"), "heatfilmNtcNum"),
    requiring(measure("minHeatfilmTemp", "heatfilm_min"), "heatfilmNtcNum"),
];
/// Quota key and sensor label prefix of the per-sensor temperature arrays
const PACK_TEMPERATURE_ARRAYS: [(&str, &str); 6] = [
    ("cellTemp", "cell"),
    ("mosTemp", "mos"),
    ("envTemp", "env"),
    ("curSensorTemp", "current_sensor"),
    ("heatfilmTemp", "heatfilm"),
    ("powerportTemp", "powerport"),
];
const PACK_CELL_VOLTAGES_KEY: &str = "cellVol";
const PACK_FAULTS: [Measure; 9] = [
    measure("bmsFault", "bms_fault"),
    measure("allBmsFault", "all_bms_fault"),
    measure("allErrCode", "all_error"),
    measure("errCode", "error"),
    measure("bmsAlarmState1", "alarm_1"),
    measure("bmsAlarmState2", "alarm_2"),
    measure("bmsProtectState1", "protect_1"),
    measure("bmsProtectState2", "protect_2"),
    measure("bmsFaultState", "fault_state"),
];
const PACK_VERSION_KEY: &str = "sysVer";
const PACK_HARDWARE_KEY: &str = "hwVer";

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum Scope {
    Device,
    Pack,
}

type Emit<'a> = dyn FnMut(f64, &[KeyValue]) + 'a;

/// Metric built from the EcoFlow state
struct Family {
    name: &'static str,
    description: &'static str,
    unit: &'static str,
    /// Monotonic total instead of a gauge
    counter: bool,
    scope: Scope,
    /// Label name of the measurements, empty for a single measurement
    label: &'static str,
    measures: &'static [Measure],
    /// Additional observations that don't map to a single quota key
    extra: Option<fn(&EcoFlowState, &mut Emit<'_>)>,
}

impl Family {
    const fn new(
        name: &'static str,
        description: &'static str,
        unit: &'static str,
        scope: Scope,
        label: &'static str,
        measures: &'static [Measure],
    ) -> Self {
        Self {
            name,
            description,
            unit,
            counter: false,
            scope,
            label,
            measures,
            extra: None,
        }
    }

    const fn counter(self) -> Self {
        Self {
            counter: true,
            ..self
        }
    }

    const fn extra(self, extra: fn(&EcoFlowState, &mut Emit<'_>)) -> Self {
        Self {
            extra: Some(extra),
            ..self
        }
    }

    fn observe(&self, state: &EcoFlowState, emit: &mut Emit<'_>) {
        match self.scope {
            Scope::Device => observe_measures(
                &state.device,
                self.measures,
                self.label,
                &state.device_attributes(),
                emit,
            ),
            Scope::Pack => {
                for (pack, data) in &state.packs {
                    observe_measures(
                        data,
                        self.measures,
                        self.label,
                        &state.pack_attributes(pack),
                        emit,
                    );
                }
            }
        }
        if let Some(extra) = self.extra {
            extra(state, emit);
        }
    }
}

static FAMILIES: [Family; 34] = [
    Family::new(
        "prosa_ecoflow_battery_charge",
        "EcoFlow STREAM system battery charge",
        "%",
        Scope::Device,
        "",
        &BATTERY_CHARGES,
    ),
    Family::new(
        "prosa_ecoflow_power",
        "EcoFlow STREAM instantaneous power",
        "W",
        Scope::Device,
        "type",
        &POWERS,
    )
    .extra(observe_solar_powers),
    Family::new(
        "prosa_ecoflow_voltage",
        "EcoFlow STREAM input voltage",
        "V",
        Scope::Device,
        "type",
        &VOLTAGES,
    )
    .extra(observe_solar_voltages),
    Family::new(
        "prosa_ecoflow_current",
        "EcoFlow STREAM input current",
        "A",
        Scope::Device,
        "type",
        &[],
    )
    .extra(observe_solar_currents),
    Family::new(
        "prosa_ecoflow_frequency",
        "EcoFlow STREAM grid frequency",
        "Hz",
        Scope::Device,
        "type",
        &FREQUENCIES,
    ),
    Family::new(
        "prosa_ecoflow_battery_health",
        "EcoFlow STREAM battery state of health",
        "%",
        Scope::Device,
        "type",
        &BATTERY_HEALTHS,
    ),
    Family::new(
        "prosa_ecoflow_battery_energy",
        "EcoFlow STREAM battery energy",
        "Wh",
        Scope::Device,
        "type",
        &BATTERY_ENERGIES,
    ),
    Family::new(
        "prosa_ecoflow_battery_remaining_time",
        "EcoFlow STREAM remaining time to fully charge or discharge the battery",
        "s",
        Scope::Device,
        "type",
        &BATTERY_REMAINING_TIMES,
    ),
    Family::new(
        "prosa_ecoflow_soc_limit",
        "EcoFlow STREAM configured battery charge limits",
        "%",
        Scope::Device,
        "type",
        &SOC_LIMITS,
    ),
    Family::new(
        "prosa_ecoflow_power_limit",
        "EcoFlow STREAM power limits",
        "W",
        Scope::Device,
        "type",
        &POWER_LIMITS,
    ),
    Family::new(
        "prosa_ecoflow_temperature",
        "EcoFlow STREAM temperatures",
        "Cel",
        Scope::Device,
        "sensor",
        &TEMPERATURES,
    ),
    Family::new(
        "prosa_ecoflow_switch",
        "EcoFlow STREAM switch and mode states, 1 when enabled",
        "",
        Scope::Device,
        "name",
        &SWITCHES,
    ),
    Family::new(
        "prosa_ecoflow_wireless",
        "EcoFlow STREAM Wi-Fi signal strength",
        "dBm",
        Scope::Device,
        "",
        &WIRELESS,
    ),
    Family::new(
        "prosa_ecoflow_apparent_power",
        "EcoFlow STREAM inverter apparent power",
        "VA",
        Scope::Device,
        "type",
        &APPARENT_POWERS,
    ),
    Family::new(
        "prosa_ecoflow_reactive_power",
        "EcoFlow STREAM inverter reactive power",
        "var",
        Scope::Device,
        "type",
        &REACTIVE_POWERS,
    ),
    Family::new(
        "prosa_ecoflow_fault",
        "EcoFlow STREAM inverter fault codes, 0 when none",
        "",
        Scope::Device,
        "type",
        &FAULTS,
    ),
    Family::new(
        "prosa_ecoflow_fan_level",
        "EcoFlow STREAM fan level",
        "",
        Scope::Device,
        "",
        &FAN_LEVELS,
    ),
    Family::new(
        "prosa_ecoflow_state",
        "EcoFlow STREAM current states, 1 for the current value",
        "",
        Scope::Device,
        "",
        &[],
    )
    .extra(observe_states),
    Family::new(
        "prosa_ecoflow_errors",
        "EcoFlow STREAM number of active device error codes",
        "",
        Scope::Device,
        "",
        &[],
    )
    .extra(observe_errors),
    Family::new(
        "prosa_ecoflow_info",
        "EcoFlow STREAM device information",
        "",
        Scope::Device,
        "",
        &[],
    )
    .extra(observe_info),
    Family::new(
        "prosa_ecoflow_pack_charge",
        "EcoFlow STREAM battery pack charge",
        "%",
        Scope::Pack,
        "type",
        &PACK_CHARGES,
    ),
    Family::new(
        "prosa_ecoflow_pack_health",
        "EcoFlow STREAM battery pack state of health",
        "%",
        Scope::Pack,
        "type",
        &PACK_HEALTHS,
    ),
    Family::new(
        "prosa_ecoflow_pack_voltage",
        "EcoFlow STREAM battery pack and cell voltages",
        "V",
        Scope::Pack,
        "type",
        &PACK_VOLTAGES,
    )
    .extra(observe_pack_cell_voltages),
    Family::new(
        "prosa_ecoflow_pack_current",
        "EcoFlow STREAM battery pack current, negative when discharging",
        "A",
        Scope::Pack,
        "type",
        &PACK_CURRENTS,
    ),
    Family::new(
        "prosa_ecoflow_pack_capacity",
        "EcoFlow STREAM battery pack capacity",
        "Ah",
        Scope::Pack,
        "type",
        &PACK_CAPACITIES,
    ),
    Family::new(
        "prosa_ecoflow_pack_cycles",
        "EcoFlow STREAM battery pack charge cycles",
        "",
        Scope::Pack,
        "",
        &PACK_CYCLES,
    ),
    Family::new(
        "prosa_ecoflow_pack_energy",
        "EcoFlow STREAM battery pack lifetime charged and discharged energy",
        "Wh",
        Scope::Pack,
        "type",
        &PACK_ENERGIES,
    )
    .counter(),
    Family::new(
        "prosa_ecoflow_pack_charge_transferred",
        "EcoFlow STREAM battery pack lifetime charged and discharged capacity",
        "Ah",
        Scope::Pack,
        "type",
        &PACK_CHARGES_TRANSFERRED,
    )
    .counter(),
    Family::new(
        "prosa_ecoflow_pack_power",
        "EcoFlow STREAM battery pack input and output power",
        "W",
        Scope::Pack,
        "type",
        &PACK_POWERS,
    ),
    Family::new(
        "prosa_ecoflow_pack_remaining_time",
        "EcoFlow STREAM battery pack remaining time",
        "s",
        Scope::Pack,
        "",
        &PACK_REMAINING_TIMES,
    ),
    Family::new(
        "prosa_ecoflow_pack_temperature",
        "EcoFlow STREAM battery pack temperatures",
        "Cel",
        Scope::Pack,
        "sensor",
        &PACK_TEMPERATURES,
    )
    .extra(observe_pack_temperature_arrays),
    Family::new(
        "prosa_ecoflow_pack_fault",
        "EcoFlow STREAM battery pack fault, alarm and protection codes, 0 when none",
        "",
        Scope::Pack,
        "type",
        &PACK_FAULTS,
    ),
    Family::new(
        "prosa_ecoflow_pack_info",
        "EcoFlow STREAM battery pack information",
        "",
        Scope::Pack,
        "",
        &[],
    )
    .extra(observe_pack_info),
    Family::new(
        "prosa_ecoflow_raw",
        "EcoFlow STREAM undocumented quota values, unscaled",
        "",
        Scope::Device,
        "",
        &[],
    )
    .extra(observe_raw),
];

/// Top-level quota key of a path
fn root(path: &str) -> &str {
    path.split_once('.').map_or(path, |(object, _)| object)
}

/// Returns `true` if a metric uses the top-level quota key
fn is_used(scope: Scope, key: &str) -> bool {
    FAMILIES
        .iter()
        .filter(|family| family.scope == scope)
        .any(|family| {
            family
                .measures
                .iter()
                .any(|measure| root(measure.key) == key || measure.requires == Some(key))
        })
        || match scope {
            Scope::Device => {
                SOLAR_INPUT_KEYS
                    .iter()
                    .any(|(_, power_key, voltage_key, current_key)| {
                        [power_key, voltage_key, current_key].contains(&&key)
                    })
                    || STATES.iter().any(|(_, state_key, _)| *state_key == key)
                    || [root(ERRORS_KEY), TIMEZONE_KEY, IOT_VERSION_KEY].contains(&key)
                    || RAW_DEVICE_KEYS.contains(&key)
            }
            Scope::Pack => {
                PACK_TEMPERATURE_ARRAYS
                    .iter()
                    .any(|(array_key, _)| *array_key == key)
                    || [PACK_CELL_VOLTAGES_KEY, PACK_VERSION_KEY, PACK_HARDWARE_KEY].contains(&key)
                    || RAW_PACK_KEYS.contains(&key)
            }
        }
}

/// Value at a quota key, or at `object.field` for a field of an object value
fn lookup<'a>(data: &'a Map<String, Value>, path: &str) -> Option<&'a Value> {
    data.get(path).or_else(|| {
        let (object, field) = path.split_once('.')?;
        data.get(object)?.as_object()?.get(field)
    })
}

/// Numeric value of a quota value; booleans are 0 or 1, and null or empty strings have no value
fn number(value: &Value) -> Result<Option<f64>, ()> {
    let value = match value {
        Value::Null => return Ok(None),
        Value::Bool(value) => Some(f64::from(u8::from(*value))),
        Value::Number(value) => value.as_f64(),
        Value::String(value) if value.is_empty() => return Ok(None),
        Value::String(value) => value.parse().ok(),
        _ => None,
    };
    value
        .filter(|value: &f64| value.is_finite())
        .map(Some)
        .ok_or(())
}

fn quantity(data: &Map<String, Value>, path: &str) -> Option<f64> {
    lookup(data, path).and_then(|value| number(value).ok().flatten())
}

fn numbers(value: &Value) -> impl Iterator<Item = f64> + '_ {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|value| number(value).ok().flatten())
}

/// Decodes a version packed as four bytes, e.g. `16777747` for `1.0.2.19`
fn version(value: f64) -> Option<String> {
    let value = u32::try_from(value as u64).ok().filter(|_| value >= 0.0)?;
    let [major, minor, patch, build] = value.to_be_bytes();
    Some(format!("{major}.{minor}.{patch}.{build}"))
}

fn with_label(
    base: &[KeyValue],
    name: &'static str,
    value: impl Into<prosa::otel::Value>,
) -> Vec<KeyValue> {
    let mut attributes = base.to_vec();
    attributes.push(KeyValue::new(name, value));
    attributes
}

fn observe_measures(
    data: &Map<String, Value>,
    measures: &[Measure],
    label: &'static str,
    base: &[KeyValue],
    emit: &mut Emit<'_>,
) {
    for measure in measures {
        if measure
            .requires
            .is_some_and(|key| !quantity(data, key).is_some_and(|count| count > 0.0))
        {
            continue;
        }
        if let Some(value) = quantity(data, measure.key) {
            if label.is_empty() {
                emit(value * measure.scale / measure.divisor, base);
            } else {
                emit(
                    value * measure.scale / measure.divisor,
                    &with_label(base, label, measure.label),
                );
            }
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
    ) -> Self {
        let voltage = quantity(data, voltage_key);
        let current = quantity(data, current_key);
        // Firmware 1.0.1.41 and later reports 0 for per-input power and only exposes voltage and current.
        let power = match quantity(data, power_key) {
            Some(power) if power != 0.0 => Some(power),
            power => voltage
                .zip(current)
                .map(|(voltage, current)| voltage * current)
                .or(power),
        };
        Self {
            power,
            voltage,
            current,
        }
    }
}

fn observe_solar(state: &EcoFlowState, value: fn(&SolarInput) -> Option<f64>, emit: &mut Emit<'_>) {
    let base = state.device_attributes();
    for keys in SOLAR_INPUT_KEYS {
        if let Some(value) = value(&SolarInput::from_data(&state.device, keys)) {
            emit(value, &with_label(&base, "type", keys.0));
        }
    }
}

fn observe_solar_powers(state: &EcoFlowState, emit: &mut Emit<'_>) {
    observe_solar(state, |input| input.power, emit);
}

fn observe_solar_voltages(state: &EcoFlowState, emit: &mut Emit<'_>) {
    observe_solar(state, |input| input.voltage, emit);
}

fn observe_solar_currents(state: &EcoFlowState, emit: &mut Emit<'_>) {
    observe_solar(state, |input| input.current, emit);
}

fn observe_states(state: &EcoFlowState, emit: &mut Emit<'_>) {
    let base = state.device_attributes();
    for (name, key, names) in STATES {
        let value = match state.device.get(key) {
            Some(Value::String(value)) if !value.is_empty() => value.clone(),
            Some(Value::Number(value)) => match value.as_i64() {
                Some(value) => names
                    .iter()
                    .find(|(code, _)| *code == value)
                    .map_or_else(|| value.to_string(), |(_, name)| name.to_string()),
                None => continue,
            },
            _ => continue,
        };
        let mut attributes = with_label(&base, "name", name);
        attributes.push(KeyValue::new("value", value));
        emit(1.0, &attributes);
    }
}

fn observe_errors(state: &EcoFlowState, emit: &mut Emit<'_>) {
    if let Some(Value::Array(errors)) = lookup(&state.device, ERRORS_KEY) {
        emit(errors.len() as f64, &state.device_attributes());
    }
}

fn observe_info(state: &EcoFlowState, emit: &mut Emit<'_>) {
    let mut attributes = state.device_attributes();
    if let Some(timezone) = state.device.get(TIMEZONE_KEY).and_then(Value::as_str) {
        attributes.push(KeyValue::new("timezone", timezone.to_string()));
    }
    if let Some(version) = quantity(&state.device, IOT_VERSION_KEY).and_then(version) {
        attributes.push(KeyValue::new("iot_version", version));
    }
    if attributes.len() > 1 {
        emit(1.0, &attributes);
    }
}

fn observe_pack_cell_voltages(state: &EcoFlowState, emit: &mut Emit<'_>) {
    for (pack, data) in &state.packs {
        let base = state.pack_attributes(pack);
        if let Some(cells) = data.get(PACK_CELL_VOLTAGES_KEY) {
            for (index, value) in numbers(cells).enumerate() {
                emit(
                    value / 1000.0,
                    &with_label(&base, "type", format!("cell_{}", index + 1)),
                );
            }
        }
    }
}

fn observe_pack_temperature_arrays(state: &EcoFlowState, emit: &mut Emit<'_>) {
    for (pack, data) in &state.packs {
        let base = state.pack_attributes(pack);
        for (key, sensor) in PACK_TEMPERATURE_ARRAYS {
            if let Some(temperatures) = data.get(key) {
                for (index, value) in numbers(temperatures).enumerate() {
                    emit(
                        value,
                        &with_label(&base, "sensor", format!("{sensor}_{}", index + 1)),
                    );
                }
            }
        }
    }
}

fn observe_pack_info(state: &EcoFlowState, emit: &mut Emit<'_>) {
    for (pack, data) in &state.packs {
        let mut attributes = state.pack_attributes(pack);
        if let Some(version) = quantity(data, PACK_VERSION_KEY).and_then(version) {
            attributes.push(KeyValue::new("version", version));
        }
        if let Some(hardware) = data.get(PACK_HARDWARE_KEY).and_then(Value::as_str) {
            attributes.push(KeyValue::new("hardware", hardware.to_string()));
        }
        emit(1.0, &attributes);
    }
}

fn observe_raw_keys(
    data: &Map<String, Value>,
    keys: &[&'static str],
    base: &[KeyValue],
    emit: &mut Emit<'_>,
) {
    for key in keys {
        match data.get(*key) {
            Some(value @ Value::Array(_)) => {
                let attributes = with_label(base, "key", *key);
                for (index, value) in numbers(value).enumerate() {
                    emit(value, &with_label(&attributes, "index", index as i64));
                }
            }
            Some(value) => {
                if let Ok(Some(value)) = number(value) {
                    emit(value, &with_label(base, "key", *key));
                }
            }
            None => {}
        }
    }
}

fn observe_raw(state: &EcoFlowState, emit: &mut Emit<'_>) {
    observe_raw_keys(
        &state.device,
        &RAW_DEVICE_KEYS,
        &state.device_attributes(),
        emit,
    );
    for (pack, data) in &state.packs {
        observe_raw_keys(data, &RAW_PACK_KEYS, &state.pack_attributes(pack), emit);
    }
}

/// Values that can be stored: scalars, short strings, and short arrays or objects of them
fn storable(value: &Value) -> bool {
    fn scalar(value: &Value) -> bool {
        match value {
            Value::Bool(_) | Value::Number(_) => true,
            Value::String(value) => value.len() <= MAX_STORED_LEN,
            _ => false,
        }
    }
    fn array(value: &Value) -> bool {
        matches!(value, Value::Array(items) if items.len() <= MAX_STORED_LEN && items.iter().all(scalar))
    }
    match value {
        Value::Object(fields) => {
            fields.len() <= MAX_STORED_LEN
                && fields.values().all(|value| scalar(value) || array(value))
        }
        value => scalar(value) || array(value),
    }
}

/// Invalid value of a quota path, with the expected value
type InvalidValue = (&'static str, &'static str);

/// Finds the values that don't match their dedicated metric
fn invalid_values(message: &Map<String, Value>, scope: Scope) -> Vec<InvalidValue> {
    const NUMBER: &str = "a finite number";
    let is_number = |value: &Value| number(value).is_ok();
    let mut invalid = Vec::new();
    for family in FAMILIES.iter().filter(|family| family.scope == scope) {
        for measure in family.measures {
            if lookup(message, measure.key).is_some_and(|value| !is_number(value)) {
                invalid.push((measure.key, NUMBER));
            }
        }
    }
    match scope {
        Scope::Device => {
            for key in
                SOLAR_INPUT_KEYS
                    .iter()
                    .flat_map(|(_, power_key, voltage_key, current_key)| {
                        [*power_key, *voltage_key, *current_key]
                    })
            {
                if message.get(key).is_some_and(|value| !is_number(value)) {
                    invalid.push((key, NUMBER));
                }
            }
            if let Some(Ok(Some(battery_charge))) = message.get(BATTERY_CHARGE_KEY).map(number)
                && !(0.0..=100.0).contains(&battery_charge)
            {
                invalid.push((BATTERY_CHARGE_KEY, "between 0 and 100"));
            }
            for (_, key, _) in STATES {
                if message.get(key).is_some_and(|value| {
                    !matches!(value, Value::Null | Value::String(_) | Value::Number(_))
                }) {
                    invalid.push((key, "a string or a number"));
                }
            }
        }
        Scope::Pack => {
            for key in PACK_TEMPERATURE_ARRAYS
                .iter()
                .map(|(key, _)| *key)
                .chain([PACK_CELL_VOLTAGES_KEY])
            {
                if message.get(key).is_some_and(
                    |value| !matches!(value, Value::Array(items) if items.iter().all(is_number)),
                ) {
                    invalid.push((key, "an array of numbers"));
                }
            }
        }
    }
    invalid
}

/// Removes the value at a quota key, or at `object.field` for a field of an object value
fn remove(data: &mut Map<String, Value>, path: &str) {
    if data.remove(path).is_none()
        && let Some((object, field)) = path.split_once('.')
        && let Some(Value::Object(fields)) = data.get_mut(object)
    {
        fields.remove(field);
    }
}

/// Outcome of a merged quota message
#[derive(Debug, Default, PartialEq)]
struct Merge {
    /// A value was stored
    stored: bool,
    /// Ignored values that don't match their metric
    invalid: Vec<InvalidValue>,
}

/// Quota values merged from the MQTT messages
#[derive(Debug, Default, Clone, PartialEq)]
struct EcoFlowState {
    serial_number: String,
    /// System and inverter values
    device: Map<String, Value>,
    /// BMS values of each battery pack, by pack serial number
    packs: BTreeMap<String, Map<String, Value>>,
}

impl EcoFlowState {
    fn device_attributes(&self) -> Vec<KeyValue> {
        vec![KeyValue::new("sn", self.serial_number.clone())]
    }

    fn pack_attributes(&self, pack: &str) -> Vec<KeyValue> {
        vec![
            KeyValue::new("sn", self.serial_number.clone()),
            KeyValue::new("pack", pack.to_string()),
        ]
    }

    fn clear(&mut self) {
        self.device.clear();
        self.packs.clear();
    }

    /// Merges a quota message, a JSON object carrying only the changed keys.
    /// A BMS report, identified by its pack serial number, goes to its battery pack.
    /// Only the keys used by a metric are stored; a value that doesn't match its metric is ignored and reported.
    /// A message that can't be attributed leaves the state unchanged.
    fn merge(&mut self, payload: &[u8]) -> Result<Merge, String> {
        let mut message: Map<String, Value> = serde_json::from_slice(payload)
            .map_err(|error| format!("Invalid EcoFlow quota message: {error}"))?;
        let (scope, pack) = match message.get(PACK_KEY) {
            None => (Scope::Device, None),
            Some(Value::String(pack)) if valid_serial_number(pack) => {
                (Scope::Pack, Some(pack.clone()))
            }
            Some(_) => return Err("EcoFlow returned an invalid battery pack serial number".into()),
        };
        let invalid = invalid_values(&message, scope);
        for (path, _) in &invalid {
            remove(&mut message, path);
        }
        let data = match pack {
            None => &mut self.device,
            Some(pack) => {
                if !self.packs.contains_key(&pack) && self.packs.len() >= MAX_PACKS {
                    return Err(format!(
                        "EcoFlow reported more than {MAX_PACKS} battery packs"
                    ));
                }
                self.packs.entry(pack).or_default()
            }
        };

        let mut stored = false;
        for (key, value) in message {
            if !is_used(scope, &key) {
                continue;
            }
            if value.is_null() {
                stored |= data.remove(&key).is_some();
            } else if storable(&value) {
                data.insert(key, value);
                stored = true;
            }
        }
        Ok(Merge { stored, invalid })
    }
}

#[derive(Debug, Default, Copy, Clone, PartialEq)]
enum EcoFlowFetchState {
    #[default]
    DeviceList,
    MainDevice,
    Certification,
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

/// MQTT broker credentials returned by the certification API
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EcoFlowCertification {
    certificate_account: String,
    certificate_password: String,
    url: String,
    port: Value,
    protocol: String,
}

impl EcoFlowCertification {
    fn mqtt_session(self, serial_number: &str, client_suffix: &str) -> Result<MqttSession, String> {
        if self.protocol != "mqtts" {
            return Err(format!(
                "EcoFlow MQTT protocol `{}` is not supported",
                self.protocol
            ));
        }
        if !valid_host(&self.url) {
            return Err("EcoFlow returned an invalid MQTT broker host".into());
        }
        let port = match &self.port {
            Value::Number(port) => port.as_u64(),
            Value::String(port) => port.parse().ok(),
            _ => None,
        }
        .and_then(|port| u16::try_from(port).ok())
        .filter(|port| *port != 0)
        .ok_or("EcoFlow returned an invalid MQTT broker port")?;
        if self.certificate_account.is_empty()
            || !self
                .certificate_account
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        {
            return Err("EcoFlow returned an invalid MQTT certificate account".into());
        }
        let url = Url::parse(&format!("mqtts://{}:{port}", self.url))
            .map_err(|error| format!("Invalid EcoFlow MQTT broker URL: {error}"))?;

        Ok(MqttSession {
            target: TargetSetting::from(url),
            client_id: format!("{}_{client_suffix}", self.certificate_account),
            subscriptions: vec![(
                format!("/open/{}/{serial_number}/quota", self.certificate_account),
                QoS::AtLeastOnce,
            )],
            username: Some(self.certificate_account),
            password: Some(self.certificate_password),
        })
    }
}

fn valid_host(host: &str) -> bool {
    (1..=253).contains(&host.len())
        && host
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'-')
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
///
/// The system is discovered over HTTP, then its telemetry is pushed over MQTT.
#[derive(Adaptor)]
pub struct FetcherEcoFlowAdaptor {
    access_key: String,
    secret_key: Vec<u8>,
    state: EcoFlowFetchState,
    pending_serial_numbers: VecDeque<String>,
    main_serial_numbers: BTreeSet<String>,
    main_serial_number: Option<String>,
    /// MQTT client identifier suffix, kept for the lifetime of the adaptor
    client_suffix: String,
    /// Quota topic of the open MQTT session
    quota_topic: Option<String>,
    /// Opening of the MQTT session or last quota update
    last_update: Option<Instant>,
    /// Quota paths already reported as invalid
    invalid_paths: BTreeSet<&'static str>,
    quota: watch::Sender<EcoFlowState>,
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
                    self.state = EcoFlowFetchState::Certification;
                }
                Ok(FetchAction::Http)
            }
            EcoFlowFetchState::Certification => {
                let serial_number = self.main_serial_number.clone().ok_or_else(|| {
                    FetcherError::Other("EcoFlow main serial number is unavailable".into())
                })?;
                // The error doesn't carry serde details, which could expose the certificate password.
                let certification: EcoFlowCertification =
                    serde_json::from_value(data).map_err(|_| {
                        FetcherError::Other("Invalid EcoFlow MQTT certification".into())
                    })?;
                let session = certification
                    .mqtt_session(&serial_number, &self.client_suffix)
                    .map_err(FetcherError::Other)?;
                self.quota_topic = session
                    .subscriptions
                    .first()
                    .map(|(topic, _)| topic.clone());
                self.last_update = Some(Instant::now());
                self.quota
                    .send_modify(|state| state.serial_number = serial_number);
                Ok(FetchAction::Mqtt(Box::new(session)))
            }
        }
    }

    /// Forgets the MQTT session state and drops the metrics
    fn close_session(&mut self) {
        self.quota_topic = None;
        self.last_update = None;
        self.quota.send_modify(EcoFlowState::clear);
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
        let (quota, watch_quota) = watch::channel(EcoFlowState::default());

        let meter = proc.get_proc_param().meter("ecoflow");
        for family in &FAMILIES {
            let state = watch_quota.clone();
            let callback = move |observer: &dyn AsyncInstrument<f64>| {
                family.observe(&state.borrow(), &mut |value, attributes| {
                    observer.observe(value, attributes)
                });
            };
            if family.counter {
                let builder = meter
                    .f64_observable_counter(family.name)
                    .with_description(family.description);
                let builder = if family.unit.is_empty() {
                    builder
                } else {
                    builder.with_unit(family.unit)
                };
                builder.with_callback(callback).build();
            } else {
                let builder = meter
                    .f64_observable_gauge(family.name)
                    .with_description(family.description);
                let builder = if family.unit.is_empty() {
                    builder
                } else {
                    builder.with_unit(family.unit)
                };
                builder.with_callback(callback).build();
            }
        }

        Ok(Self {
            access_key,
            secret_key,
            state: EcoFlowFetchState::default(),
            pending_serial_numbers: VecDeque::new(),
            main_serial_numbers: BTreeSet::new(),
            main_serial_number: None,
            client_suffix: format!("{:012x}", rand::random::<u64>() >> 16),
            quota_topic: None,
            last_update: None,
            invalid_paths: BTreeSet::new(),
            quota,
        })
    }

    fn fetch(&mut self) -> Result<FetchAction<M>, FetcherError<M>> {
        if self.quota_topic.is_some() {
            if !self
                .last_update
                .is_some_and(|last_update| last_update.elapsed() >= QUOTA_STALE_AFTER)
            {
                return Ok(FetchAction::None);
            }
            warn!("EcoFlow quota is stale, renewing the MQTT session");
            self.close_session();
        }
        self.state = if self.main_serial_number.is_some() {
            EcoFlowFetchState::Certification
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
            EcoFlowFetchState::Certification => {
                self.signed_request(request_builder, "/iot-open/sign/certification".into(), "")
            }
        }
    }

    async fn process_http_response(
        &mut self,
        response: Result<Response<Incoming>, FetcherError<M>>,
    ) -> Result<FetchAction<M>, FetcherError<M>> {
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
        let response: EcoFlowApiResponse = serde_json::from_slice(&body)
            .map_err(|error| FetcherError::Io(io::Error::new(io::ErrorKind::InvalidData, error)))?;
        self.process_data(response.into_data().map_err(FetcherError::Other)?)
    }

    fn process_mqtt_message(
        &mut self,
        topic: &str,
        payload: Bytes,
    ) -> Result<FetchAction<M>, FetcherError<M>> {
        if self.quota_topic.as_deref() != Some(topic) {
            debug!(topic, "Ignore EcoFlow MQTT message");
            return Ok(FetchAction::None);
        }
        let mut result = Ok(Merge::default());
        self.quota.send_if_modified(|state| {
            result = state.merge(&payload);
            result.as_ref().is_ok_and(|merge| merge.stored)
        });
        let merge = result.map_err(FetcherError::Other)?;
        for (path, expected) in merge.invalid {
            // Warn once per key, a device model may always report it differently
            if self.invalid_paths.insert(path) {
                warn!("Ignore EcoFlow quota {path}: it must be {expected}");
            } else {
                debug!("Ignore EcoFlow quota {path}: it must be {expected}");
            }
        }
        if merge.stored {
            self.last_update = Some(Instant::now());
        }
        Ok(FetchAction::None)
    }

    fn process_mqtt_event(
        &mut self,
        event: MqttEvent<M>,
    ) -> Result<FetchAction<M>, FetcherError<M>> {
        match event {
            MqttEvent::Connected {
                rejected_subscriptions,
            } if !rejected_subscriptions.is_empty() => {
                // The topic depends on the certificate account and the serial number: discover them again
                self.close_session();
                self.main_serial_number = None;
                Err(FetcherError::Other(format!(
                    "EcoFlow MQTT broker rejected the subscription to {}",
                    rejected_subscriptions.join(", ")
                )))
            }
            MqttEvent::Connected { .. } => Ok(FetchAction::None),
            MqttEvent::Disconnected(error) => Err(error),
            MqttEvent::Refused(code) => {
                warn!(
                    "EcoFlow MQTT broker refused the connection ({code:?}), renewing the certification"
                );
                self.close_session();
                self.state = EcoFlowFetchState::Certification;
                Ok(FetchAction::Http)
            }
        }
    }

    fn end_active_period(&mut self) {
        self.close_session();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prosa_fetcher::mqtt::ConnectReturnCode;
    use prosa_utils::msg::simple_string_tvf::SimpleStringTvf;
    use serde_json::json;

    const SERIAL_NUMBER: &str = "BK11ZEBB2H350011";
    const PACK_SERIAL_NUMBER: &str = "BK11PEBB2H350012";
    const QUOTA_TOPIC: &str = "/open/account/BK11ZEBB2H350011/quota";

    /// System and inverter values, as published by a STREAM Ultra
    fn device_message() -> Value {
        serde_json::from_str(
            r#"{
                "cmsBattSoc": 23.0,
                "cmsBattSoh": 100.0,
                "bmsBattSoh": 99.0,
                "cmsBattFullEnergy": 1920,
                "cmsChgRemTime": 5939,
                "cmsDsgRemTime": 4552,
                "cmsMaxChgSoc": 95,
                "backupReverseSoc": 23,
                "cmsBattPowInMax": 1050,
                "feedGridModePowLimit": 900,
                "bmsMaxCellTemp": 27,
                "invTempNtc": 25,
                "relay2Onoff": true,
                "cmsBmsRunState": 1,
                "plugInInfoPvFlag": false,
                "energyStrategyOperateMode": {
                    "operateScheduledOpen": true,
                    "operateSelfPoweredOpen": false
                },
                "moduleWifiRssi": -31.0,
                "gridConnectionSta": "PANEL_GRID_IN",
                "cmsChgDsgState": 2,
                "feedGridMode": 7,
                "devErrcodeList": {"devErrcode": []},
                "utcTimezoneId": "Europe/Paris",
                "iotVersion": 16777747,
                "gridConnectionVol": 231.5,
                "gridConnectionFreq": 49.99,
                "powGetPvSum": 850.0,
                "powGetBpCms": -225.5,
                "powGetSysLoad": 454.0,
                "powGetPv": 0.0,
                "plugInInfoPvVol": 40.0,
                "plugInInfoPvAmp": 10.0,
                "plugInInfoPv2Vol": "32.5",
                "cloudMetter": {
                    "phaseAPower": 0,
                    "phaseCPower": 459,
                    "model": "CT_SHELLY_PRO_3EM",
                    "hasMeter": true
                },
                "localMeter": {"phaseACur": 1.5},
                "dabStateMonitor1": 1527260564,
                "offgridVout1Rms": 2311,
                "offgrid1ActivePower": 6,
                "offgrid1ApperentPower": 16,
                "ongridInReactivePower": 151,
                "voltFreqfilter": 4996,
                "invFault": 0,
                "mpptPv1Fault": 16,
                "v1p0": {"fanLevel": 0, "chgAmp": 90000},
                "actPwrRated": 900,
                "aiTouValid": false,
                "iotIpAddress": 1057073344,
                "systemGroupId": 335409153,
                "iotWifiBssid": "00:00:00:00:00:00",
                "allTimerTask": {"timeTask": [{"taskIndex": 1}]}
            }"#,
        )
        .unwrap()
    }

    /// BMS report of a battery pack
    fn pack_message() -> Value {
        json!({
            "packSn": PACK_SERIAL_NUMBER,
            "soc": 23,
            "realSoh": 99.97,
            "calendarSoh": 88.0,
            "vol": 19738,
            "amp": -154,
            "cellVol": [3290, 3289],
            "designCap": 100000,
            "cycles": 129,
            "accuChgEnergy": 260917,
            "accuDsgCap": 12966644,
            "inputWatts": 0,
            "remainTime": 4552,
            "temp": 27,
            "cellTemp": [27, 26],
            "envNtcNum": 0,
            "maxEnvTemp": 0,
            "heatfilmNtcNum": 1,
            "maxHeatfilmTemp": 26,
            "bmsProtectState1": 0,
            "sysVer": 33620049,
            "hwVer": "V0.0.0",
            "remainBalanceTime": [0, 5],
            "mosState": 3
        })
    }

    fn state() -> EcoFlowState {
        let mut state = EcoFlowState {
            serial_number: SERIAL_NUMBER.into(),
            ..Default::default()
        };
        assert_eq!(
            state
                .merge(device_message().to_string().as_bytes())
                .map(|merge| merge.stored),
            Ok(true)
        );
        assert_eq!(
            state
                .merge(pack_message().to_string().as_bytes())
                .map(|merge| merge.stored),
            Ok(true)
        );
        state
    }

    /// Observations of a family as values with their labels other than `sn`
    fn observe(name: &str, state: &EcoFlowState) -> Vec<(f64, Vec<(String, String)>)> {
        let family = FAMILIES.iter().find(|family| family.name == name).unwrap();
        let mut observations = Vec::new();
        family.observe(state, &mut |value, attributes| {
            assert_eq!(attributes[0].key.as_str(), "sn");
            observations.push((
                value,
                attributes[1..]
                    .iter()
                    .map(|attribute| (attribute.key.to_string(), attribute.value.to_string()))
                    .collect(),
            ));
        });
        observations
    }

    /// Value of the observation labelled `label=value`
    fn observed(name: &str, state: &EcoFlowState, label: &str, value: &str) -> Option<f64> {
        observe(name, state)
            .into_iter()
            .find(|(_, labels)| {
                labels
                    .iter()
                    .any(|(key, label_value)| key == label && label_value == value)
            })
            .map(|(value, _)| value)
    }

    fn adaptor() -> (FetcherEcoFlowAdaptor, watch::Receiver<EcoFlowState>) {
        let (quota, watch_quota) = watch::channel(EcoFlowState {
            serial_number: SERIAL_NUMBER.into(),
            ..Default::default()
        });
        let adaptor = FetcherEcoFlowAdaptor {
            access_key: "access".into(),
            secret_key: b"secret".to_vec(),
            state: EcoFlowFetchState::Certification,
            pending_serial_numbers: VecDeque::new(),
            main_serial_numbers: BTreeSet::new(),
            main_serial_number: Some(SERIAL_NUMBER.into()),
            client_suffix: "0123456789ab".into(),
            quota_topic: Some(QUOTA_TOPIC.into()),
            last_update: Some(Instant::now()),
            invalid_paths: BTreeSet::new(),
            quota,
        };
        (adaptor, watch_quota)
    }

    fn fetch(adaptor: &mut FetcherEcoFlowAdaptor) -> FetchAction<SimpleStringTvf> {
        FetcherAdaptor::<SimpleStringTvf>::fetch(adaptor).unwrap()
    }

    fn push(
        adaptor: &mut FetcherEcoFlowAdaptor,
        topic: &str,
        payload: Value,
    ) -> Result<FetchAction<SimpleStringTvf>, FetcherError<SimpleStringTvf>> {
        FetcherAdaptor::<SimpleStringTvf>::process_mqtt_message(
            adaptor,
            topic,
            Bytes::from(payload.to_string()),
        )
    }

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

    #[test]
    fn builds_mqtt_session_from_certification() {
        for port in [json!("8883"), json!(8883)] {
            let certification: EcoFlowCertification = serde_json::from_value(json!({
                "certificateAccount": "open-0123456789",
                "certificatePassword": "password",
                "url": "mqtt-e.ecoflow.com",
                "port": port,
                "protocol": "mqtts"
            }))
            .unwrap();
            let session = certification
                .mqtt_session(SERIAL_NUMBER, "0123456789ab")
                .unwrap();
            assert_eq!(
                session.target.url.as_str(),
                "mqtts://mqtt-e.ecoflow.com:8883"
            );
            assert_eq!(session.client_id, "open-0123456789_0123456789ab");
            assert_eq!(session.username.as_deref(), Some("open-0123456789"));
            assert_eq!(session.password.as_deref(), Some("password"));
            assert_eq!(
                session.subscriptions,
                [(
                    "/open/open-0123456789/BK11ZEBB2H350011/quota".to_string(),
                    QoS::AtLeastOnce
                )]
            );
        }
    }

    #[test]
    fn rejects_invalid_certification() {
        let valid = json!({
            "certificateAccount": "open-0123456789",
            "certificatePassword": "password",
            "url": "mqtt-e.ecoflow.com",
            "port": "8883",
            "protocol": "mqtts"
        });
        for (key, value) in [
            ("protocol", json!("mqtt")),
            ("url", json!("evil.com/path@host")),
            ("url", json!("")),
            ("port", json!("0")),
            ("port", json!(70000)),
            ("port", json!(null)),
            ("certificateAccount", json!("open/+/#")),
            ("certificateAccount", json!("")),
        ] {
            let mut data = valid.clone();
            data[key] = value;
            let certification: EcoFlowCertification = serde_json::from_value(data).unwrap();
            assert!(
                certification
                    .mqtt_session(SERIAL_NUMBER, "0123456789ab")
                    .is_err(),
                "{key} should be rejected"
            );
        }
    }

    #[test]
    fn routes_bms_reports_to_their_battery_pack() {
        let state = state();
        assert!(state.device.contains_key("cmsBattSoc"));
        assert!(!state.device.contains_key("soc"));
        let pack = &state.packs[PACK_SERIAL_NUMBER];
        assert_eq!(pack.get("cycles"), Some(&json!(129)));
        assert!(!pack.contains_key("cmsBattSoc"));
    }

    #[test]
    fn merges_deltas_and_removes_null_values() {
        let mut state = state();
        let delta = json!({"powGetBpCms": -475.4, "powGetSysLoad": null}).to_string();
        assert_eq!(
            state.merge(delta.as_bytes()).map(|merge| merge.stored),
            Ok(true)
        );
        assert_eq!(
            observed("prosa_ecoflow_power", &state, "type", "battery"),
            Some(-475.4)
        );
        assert_eq!(
            observed("prosa_ecoflow_power", &state, "type", "system_load"),
            None
        );
        assert_eq!(
            observed("prosa_ecoflow_power", &state, "type", "solar"),
            Some(850.0)
        );
    }

    #[test]
    fn stores_only_values_used_by_a_metric() {
        let mut state = state();
        for unused in [
            "allTimerTask",
            "dabStateMonitor1",
            "aiTouValid",
            "iotIpAddress",
            "systemGroupId",
            "iotWifiBssid",
            "localMeter",
        ] {
            assert!(!state.device.contains_key(unused), "{unused}");
        }
        assert!(state.packs[PACK_SERIAL_NUMBER].contains_key("envNtcNum"));
        let unused = json!({"iotMemHeapSize": 54876, "dev1PvRemain": 0}).to_string();
        assert_eq!(
            state.merge(unused.as_bytes()).map(|merge| merge.stored),
            Ok(false)
        );
        let long = json!({"utcTimezoneId": "x".repeat(MAX_STORED_LEN + 1)}).to_string();
        assert_eq!(
            state.merge(long.as_bytes()).map(|merge| merge.stored),
            Ok(false)
        );
        assert_eq!(
            state.device.get("utcTimezoneId"),
            Some(&json!("Europe/Paris"))
        );
    }

    #[test]
    fn ignores_invalid_values_and_keeps_the_rest() {
        let mut state = state();
        let merge = |state: &mut EcoFlowState, payload: Value| {
            state.merge(payload.to_string().as_bytes()).unwrap()
        };

        let result = merge(
            &mut state,
            json!({"powGetBpCms": "invalid", "powGetSysLoad": 1.0, "cmsBattSoc": 120}),
        );
        assert_eq!(
            result,
            Merge {
                stored: true,
                invalid: vec![
                    ("powGetBpCms", "a finite number"),
                    ("cmsBattSoc", "between 0 and 100")
                ],
            }
        );
        let power =
            |state: &EcoFlowState, kind| observed("prosa_ecoflow_power", state, "type", kind);
        assert_eq!(power(&state, "battery"), Some(-225.5));
        assert_eq!(power(&state, "system_load"), Some(1.0));
        assert_eq!(state.device.get("cmsBattSoc"), Some(&json!(23.0)));

        let result = merge(
            &mut state,
            json!({"cloudMetter": {"phaseAPower": 5, "phaseBPower": "x"}}),
        );
        assert_eq!(
            result.invalid,
            [("cloudMetter.phaseBPower", "a finite number")]
        );
        assert_eq!(power(&state, "meter_phase_1"), Some(5.0));
        assert_eq!(power(&state, "meter_phase_2"), None);

        let result = merge(
            &mut state,
            json!({"gridConnectionSta": {"code": 1}, "plugInInfoPv4Amp": "x", "gridConnectionVol": 230.0}),
        );
        assert_eq!(
            result.invalid,
            [
                ("plugInInfoPv4Amp", "a finite number"),
                ("gridConnectionSta", "a string or a number")
            ]
        );
        assert_eq!(
            state.device.get("gridConnectionSta"),
            Some(&json!("PANEL_GRID_IN"))
        );
        assert_eq!(
            observed("prosa_ecoflow_voltage", &state, "type", "grid"),
            Some(230.0)
        );

        let result = merge(
            &mut state,
            json!({"packSn": PACK_SERIAL_NUMBER, "cellVol": [3290, "x"], "vol": "x", "cycles": 130}),
        );
        assert_eq!(
            result.invalid,
            [
                ("vol", "a finite number"),
                ("cellVol", "an array of numbers")
            ]
        );
        let pack = &state.packs[PACK_SERIAL_NUMBER];
        assert_eq!(pack.get("cellVol"), Some(&json!([3290, 3289])));
        assert_eq!(pack.get("vol"), Some(&json!(19738)));
        assert_eq!(pack.get("cycles"), Some(&json!(130)));
    }

    #[test]
    fn rejects_messages_that_cannot_be_attributed() {
        let mut state = state();
        let before = state.clone();
        for payload in [
            json!({"packSn": "bad/sn", "soc": 20}),
            json!({"packSn": 12, "soc": 20}),
            json!([1, 2]),
        ] {
            assert!(
                state.merge(payload.to_string().as_bytes()).is_err(),
                "{payload}"
            );
        }
        assert!(state.merge(b"not json").is_err());
        assert_eq!(state, before);
    }

    #[test]
    fn bounds_battery_packs() {
        let mut state = EcoFlowState::default();
        for pack in 0..MAX_PACKS {
            let report = json!({"packSn": format!("BK11P{pack:011}"), "soc": 20}).to_string();
            assert_eq!(
                state.merge(report.as_bytes()).map(|merge| merge.stored),
                Ok(true)
            );
        }
        let report = json!({"packSn": "BK11P99999999999", "soc": 20}).to_string();
        assert!(state.merge(report.as_bytes()).is_err());
        assert_eq!(state.packs.len(), MAX_PACKS);
    }

    #[test]
    fn observes_system_measurements_in_metric_units() {
        let state = state();
        assert_eq!(
            observe("prosa_ecoflow_battery_charge", &state),
            [(23.0, vec![])]
        );
        let remaining =
            |kind| observed("prosa_ecoflow_battery_remaining_time", &state, "type", kind);
        assert_eq!(remaining("charge"), Some(5939.0 * 60.0));
        assert_eq!(remaining("discharge"), Some(4552.0 * 60.0));
        assert_eq!(
            observed("prosa_ecoflow_battery_health", &state, "type", "main"),
            Some(99.0)
        );
        assert_eq!(
            observed("prosa_ecoflow_soc_limit", &state, "type", "backup_reserve"),
            Some(23.0)
        );
        assert_eq!(
            observed("prosa_ecoflow_power_limit", &state, "type", "feed_grid"),
            Some(900.0)
        );
        assert_eq!(
            observed("prosa_ecoflow_temperature", &state, "sensor", "inverter"),
            Some(25.0)
        );
        assert_eq!(
            observed("prosa_ecoflow_voltage", &state, "type", "grid"),
            Some(231.5)
        );
        assert_eq!(
            observed("prosa_ecoflow_frequency", &state, "type", "grid"),
            Some(49.99)
        );
        assert_eq!(observe("prosa_ecoflow_wireless", &state), [(-31.0, vec![])]);
    }

    #[test]
    fn observes_solar_inputs_and_meter_phases_as_powers() {
        let state = state();
        let power = |kind| observed("prosa_ecoflow_power", &state, "type", kind);
        assert_eq!(power("solar_1"), Some(400.0));
        assert_eq!(power("solar_2"), None);
        assert_eq!(power("meter_phase_1"), Some(0.0));
        assert_eq!(power("meter_phase_3"), Some(459.0));
        assert_eq!(power("meter_phase_2"), None);
        assert_eq!(
            observed("prosa_ecoflow_voltage", &state, "type", "solar_2"),
            Some(32.5)
        );
        assert_eq!(
            observed("prosa_ecoflow_current", &state, "type", "solar_1"),
            Some(10.0)
        );
    }

    #[test]
    fn keeps_reported_solar_input_power_on_legacy_firmware() {
        let data = json!({"powGetPv": 400.0, "plugInInfoPvVol": 40.0, "plugInInfoPvAmp": 0.0});
        let input = SolarInput::from_data(data.as_object().unwrap(), SOLAR_INPUT_KEYS[0]);
        assert_eq!(input.power, Some(400.0));
    }

    #[test]
    fn observes_switches_states_errors_and_info() {
        let state = state();
        let switch = |name| observed("prosa_ecoflow_switch", &state, "name", name);
        assert_eq!(switch("relay_2"), Some(1.0));
        assert_eq!(switch("bms_running"), Some(1.0));
        assert_eq!(switch("solar_1_connected"), Some(0.0));
        assert_eq!(switch("scheduled_mode"), Some(1.0));
        assert_eq!(switch("self_powered_mode"), Some(0.0));
        assert_eq!(switch("tou_mode"), None);

        let states = observe("prosa_ecoflow_state", &state);
        let state_value = |name: &str| {
            states
                .iter()
                .find(|(_, labels)| labels[0].1 == name)
                .map(|(_, labels)| labels[1].1.clone())
        };
        assert_eq!(
            state_value("grid_connection").as_deref(),
            Some("PANEL_GRID_IN")
        );
        assert_eq!(state_value("charge_discharge").as_deref(), Some("charging"));
        assert_eq!(state_value("feed_grid_mode").as_deref(), Some("7"));
        assert!(states.iter().all(|(value, _)| *value == 1.0));

        assert_eq!(observe("prosa_ecoflow_errors", &state), [(0.0, vec![])]);
        assert_eq!(
            observe("prosa_ecoflow_info", &state),
            [(
                1.0,
                vec![
                    ("timezone".to_string(), "Europe/Paris".to_string()),
                    ("iot_version".to_string(), "1.0.2.19".to_string())
                ]
            )]
        );
    }

    #[test]
    fn observes_battery_pack_measurements_in_metric_units() {
        let state = state();
        let pack = |name| {
            observe(name, &state)
                .into_iter()
                .map(|(value, labels)| {
                    assert_eq!(
                        labels[0],
                        ("pack".to_string(), PACK_SERIAL_NUMBER.to_string())
                    );
                    (
                        value,
                        labels[1..]
                            .iter()
                            .map(|(_, value)| value.clone())
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            pack("prosa_ecoflow_pack_voltage"),
            [
                (19.738, vec!["pack".to_string()]),
                (3.29, vec!["cell_1".to_string()]),
                (3.289, vec!["cell_2".to_string()]),
            ]
        );
        assert_eq!(
            pack("prosa_ecoflow_pack_current"),
            [(-0.154, vec!["measured".to_string()])]
        );
        assert_eq!(
            pack("prosa_ecoflow_pack_capacity"),
            [(100.0, vec!["design".to_string()])]
        );
        assert_eq!(pack("prosa_ecoflow_pack_cycles"), [(129.0, vec![])]);
        assert_eq!(
            pack("prosa_ecoflow_pack_energy"),
            [(260917.0, vec!["charged".to_string()])]
        );
        assert_eq!(
            pack("prosa_ecoflow_pack_charge_transferred"),
            [(12966.644, vec!["discharged".to_string()])]
        );
        assert_eq!(
            pack("prosa_ecoflow_pack_remaining_time"),
            [(4552.0 * 60.0, vec![])]
        );
        assert_eq!(
            pack("prosa_ecoflow_pack_health"),
            [
                (99.97, vec!["real".to_string()]),
                (88.0, vec!["calendar".to_string()])
            ]
        );
        // Environment sensors are absent, the heat film sensor is present
        assert_eq!(
            pack("prosa_ecoflow_pack_temperature"),
            [
                (27.0, vec!["pack".to_string()]),
                (26.0, vec!["heatfilm_max".to_string()]),
                (27.0, vec!["cell_1".to_string()]),
                (26.0, vec!["cell_2".to_string()]),
            ]
        );
        assert_eq!(
            pack("prosa_ecoflow_pack_fault"),
            [(0.0, vec!["protect_1".to_string()])]
        );
        assert_eq!(
            pack("prosa_ecoflow_pack_info"),
            [(1.0, vec!["2.1.0.81".to_string(), "V0.0.0".to_string()])]
        );
    }

    #[test]
    fn observes_inverter_measurements_and_faults() {
        let state = state();
        assert_eq!(
            observed("prosa_ecoflow_power", &state, "type", "offgrid_1"),
            Some(6.0)
        );
        assert_eq!(
            observed("prosa_ecoflow_apparent_power", &state, "type", "offgrid_1"),
            Some(16.0)
        );
        assert_eq!(
            observed(
                "prosa_ecoflow_reactive_power",
                &state,
                "type",
                "inverter_grid"
            ),
            Some(151.0)
        );
        assert_eq!(
            observed("prosa_ecoflow_voltage", &state, "type", "offgrid_1"),
            Some(231.1)
        );
        assert_eq!(
            observed("prosa_ecoflow_frequency", &state, "type", "inverter"),
            Some(49.96)
        );
        assert_eq!(
            observe("prosa_ecoflow_fault", &state)
                .into_iter()
                .map(|(value, labels)| (value, labels[0].1.clone()))
                .collect::<Vec<_>>(),
            [(0.0, "inverter".to_string()), (16.0, "mppt_1".to_string())]
        );
        assert_eq!(observe("prosa_ecoflow_fan_level", &state), [(0.0, vec![])]);
    }

    #[test]
    fn observes_allowed_undocumented_values_as_raw() {
        let state = state();
        assert_eq!(
            observe("prosa_ecoflow_raw", &state),
            [
                (900.0, vec![("key".to_string(), "actPwrRated".to_string())]),
                (
                    0.0,
                    vec![
                        ("pack".to_string(), PACK_SERIAL_NUMBER.to_string()),
                        ("key".to_string(), "remainBalanceTime".to_string()),
                        ("index".to_string(), "0".to_string())
                    ]
                ),
                (
                    5.0,
                    vec![
                        ("pack".to_string(), PACK_SERIAL_NUMBER.to_string()),
                        ("key".to_string(), "remainBalanceTime".to_string()),
                        ("index".to_string(), "1".to_string())
                    ]
                ),
                (
                    3.0,
                    vec![
                        ("pack".to_string(), PACK_SERIAL_NUMBER.to_string()),
                        ("key".to_string(), "mosState".to_string())
                    ]
                ),
            ]
        );
    }

    #[test]
    fn declares_unique_metric_families() {
        let names = FAMILIES
            .iter()
            .map(|family| family.name)
            .collect::<BTreeSet<_>>();
        assert_eq!(names.len(), FAMILIES.len());
        let counters = FAMILIES
            .iter()
            .filter(|family| family.counter)
            .map(|family| family.name)
            .collect::<Vec<_>>();
        assert_eq!(
            counters,
            [
                "prosa_ecoflow_pack_energy",
                "prosa_ecoflow_pack_charge_transferred"
            ]
        );
    }

    #[test]
    fn publishes_pushed_quota_from_the_session_topic_only() {
        let (mut adaptor, quota) = adaptor();
        push(
            &mut adaptor,
            "/open/account/other/quota",
            json!({"cmsBattSoc": 64.0}),
        )
        .unwrap();
        assert!(quota.borrow().device.is_empty());

        push(&mut adaptor, QUOTA_TOPIC, json!({"cmsBattSoc": 64.0})).unwrap();
        assert_eq!(quota.borrow().device.get("cmsBattSoc"), Some(&json!(64.0)));
        push(&mut adaptor, QUOTA_TOPIC, json!({"cmsBattSoc": "x"})).unwrap();
        push(&mut adaptor, QUOTA_TOPIC, json!({"cmsBattSoc": "y"})).unwrap();
        assert_eq!(adaptor.invalid_paths, BTreeSet::from(["cmsBattSoc"]));
        assert_eq!(quota.borrow().device.get("cmsBattSoc"), Some(&json!(64.0)));
    }

    #[test]
    fn keeps_session_while_quota_is_fresh() {
        let (mut adaptor, _quota) = adaptor();
        assert!(matches!(fetch(&mut adaptor), FetchAction::None));
    }

    #[test]
    fn renews_stale_session() {
        let (mut adaptor, quota) = adaptor();
        push(&mut adaptor, QUOTA_TOPIC, json!({"cmsBattSoc": 64.0})).unwrap();
        adaptor.last_update = Instant::now().checked_sub(QUOTA_STALE_AFTER);

        assert!(matches!(fetch(&mut adaptor), FetchAction::Http));
        assert_eq!(adaptor.state, EcoFlowFetchState::Certification);
        assert!(adaptor.quota_topic.is_none());
        assert!(quota.borrow().device.is_empty());
    }

    #[test]
    fn renews_certification_when_broker_refuses_connection() {
        let (mut adaptor, quota) = adaptor();
        push(&mut adaptor, QUOTA_TOPIC, json!({"cmsBattSoc": 64.0})).unwrap();

        let action = FetcherAdaptor::<SimpleStringTvf>::process_mqtt_event(
            &mut adaptor,
            MqttEvent::Refused(ConnectReturnCode::BadUserNamePassword),
        )
        .unwrap();
        assert!(matches!(action, FetchAction::Http));
        assert_eq!(adaptor.state, EcoFlowFetchState::Certification);
        assert!(quota.borrow().device.is_empty());

        // A dropped deferred request is retried on the next tick
        assert!(matches!(fetch(&mut adaptor), FetchAction::Http));
        assert_eq!(adaptor.state, EcoFlowFetchState::Certification);
    }

    #[test]
    fn rediscovers_device_when_subscription_is_rejected() {
        let (mut adaptor, _quota) = adaptor();
        assert!(
            FetcherAdaptor::<SimpleStringTvf>::process_mqtt_event(
                &mut adaptor,
                MqttEvent::Connected {
                    rejected_subscriptions: vec![QUOTA_TOPIC.into()],
                },
            )
            .is_err()
        );
        assert!(matches!(fetch(&mut adaptor), FetchAction::Http));
        assert_eq!(adaptor.state, EcoFlowFetchState::DeviceList);
    }

    #[test]
    fn closes_session_at_end_of_active_period() {
        let (mut adaptor, quota) = adaptor();
        push(
            &mut adaptor,
            QUOTA_TOPIC,
            json!({"packSn": PACK_SERIAL_NUMBER, "soc": 20}),
        )
        .unwrap();
        FetcherAdaptor::<SimpleStringTvf>::end_active_period(&mut adaptor);
        assert!(adaptor.quota_topic.is_none());
        assert!(quota.borrow().packs.is_empty());
        assert!(matches!(fetch(&mut adaptor), FetchAction::Http));
    }
}
