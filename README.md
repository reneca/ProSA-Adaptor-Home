# Adaptor Home

ProSA home adaptor for automation.
This needs to be used with the corresponding processor packaged with [cargo-prosa](https://github.com/worldline/ProSA/tree/main/cargo-prosa).

## BBox

The BBox adaptor is designed to fetch metrics from your home router using the [bbox API](https://api.bbox.fr/doc/apirouter/index.html).
It exposes hardware metrics and internet consumption data for your router.

> The password in the URL needs to be encoded in [base64-url](https://en.wikipedia.org/wiki/Base64#URL_applications).

```yaml
bbox:
  target:
    url: https://:password@mabbox.bytel.fr
  authorization: false
  ssl:
    store:
      certs:
      - |
        -----BEGIN CERTIFICATE-----
        MIIDrzCCApegAwIBAgIQCDvgVpBCRrGhdWrJWZHHSjANBgkqhkiG9w0BAQUFADBh
        MQswCQYDVQQGEwJVUzEVMBMGA1UEChMMRGlnaUNlcnQgSW5jMRkwFwYDVQQLExB3
        d3cuZGlnaWNlcnQuY29tMSAwHgYDVQQDExdEaWdpQ2VydCBHbG9iYWwgUm9vdCBD
        QTAeFw0wNjExMTAwMDAwMDBaFw0zMTExMTAwMDAwMDBaMGExCzAJBgNVBAYTAlVT
        MRUwEwYDVQQKEwxEaWdpQ2VydCBJbmMxGTAXBgNVBAsTEHd3dy5kaWdpY2VydC5j
        b20xIDAeBgNVBAMTF0RpZ2lDZXJ0IEdsb2JhbCBSb290IENBMIIBIjANBgkqhkiG
        9w0BAQEFAAOCAQ8AMIIBCgKCAQEA4jvhEXLeqKTTo1eqUKKPC3eQyaKl7hLOllsB
        CSDMAZOnTjC3U/dDxGkAV53ijSLdhwZAAIEJzs4bg7/fzTtxRuLWZscFs3YnFo97
        nh6Vfe63SKMI2tavegw5BmV/Sl0fvBf4q77uKNd0f3p4mVmFaG5cIzJLv07A6Fpt
        43C/dxC//AH2hdmoRBBYMql1GNXRor5H4idq9Joz+EkIYIvUX7Q6hL+hqkpMfT7P
        T19sdl6gSzeRntwi5m3OFBqOasv+zbMUZBfHWymeMr/y7vrTC0LUq7dBMtoM1O/4
        gdW7jVg/tRvoSSiicNoxBN33shbyTApOB6jtSj1etX+jkMOvJwIDAQABo2MwYTAO
        BgNVHQ8BAf8EBAMCAYYwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUA95QNVbR
        TLtm8KPiGxvDl7I90VUwHwYDVR0jBBgwFoAUA95QNVbRTLtm8KPiGxvDl7I90VUw
        DQYJKoZIhvcNAQEFBQADggEBAMucN6pIExIK+t1EnE9SsPTfrgT1eXkIoyQY/Esr
        hMAtudXH/vTBH1jLuG2cenTnmCmrEbXjcKChzUyImZOMkXDiqw8cvpOp/2PV5Adg
        06O/nVsJ8dWO41P0jmP6P6fbtGbfYmbW0W5BjfIttep3Sp+dWOIrWcBAI+0tKIJF
        PnlUkiaY4IBIqDfv8NZ5YBberOgOzW6sRBc4L0na4UU+Krk2U886UAb3LujEV0ls
        YSEY1QSteDwsOoBrp+uvFRTp2InBuThs4pFsiv9kuXclVzDAGySj4dzp30d8tbQk
        CAUw7C29C79Fv1C5qfPrmAESrciIxpg0X40KPMbp1ZWVbd4=
        -----END CERTIFICATE-----
```

## Freebox

The Freebox adaptor is designed to fetch metrics from your home router using the Freebox API.
It exposes hardware metrics and internet consumption data for your router.

> The password in the URL needs to be encoded in [base64-url](https://en.wikipedia.org/wiki/Base64#URL_applications).

```yaml
freebox:
  target:
    url: https://app_id:app_password@freebox_url
  authorization: false
  ssl:
    store:
      certs:
      - |
        -----BEGIN CERTIFICATE-----
        MIICWTCCAd+gAwIBAgIJAMaRcLnIgyukMAoGCCqGSM49BAMCMGExCzAJBgNVBAYT
        AkZSMQ8wDQYDVQQIDAZGcmFuY2UxDjAMBgNVBAcMBVBhcmlzMRMwEQYDVQQKDApG
        cmVlYm94IFNBMRwwGgYDVQQDDBNGcmVlYm94IEVDQyBSb290IENBMB4XDTE1MDkw
        MTE4MDIwN1oXDTM1MDgyNzE4MDIwN1owYTELMAkGA1UEBhMCRlIxDzANBgNVBAgM
        BkZyYW5jZTEOMAwGA1UEBwwFUGFyaXMxEzARBgNVBAoMCkZyZWVib3ggU0ExHDAa
        BgNVBAMME0ZyZWVib3ggRUNDIFJvb3QgQ0EwdjAQBgcqhkjOPQIBBgUrgQQAIgNi
        AASCjD6ZKn5ko6cU5Vxh8GA1KqRi6p2GQzndxHtuUmwY8RvBbhZ0GIL7bQ4f08ae
        JOv0ycWjEW0fyOnAw6AYdsN6y1eNvH2DVfoXQyGoCSvXQNAUxla+sJuLGICRYiZz
        mnijYzBhMB0GA1UdDgQWBBTIB3c2GlbV6EIh2ErEMJvFxMz/QTAfBgNVHSMEGDAW
        gBTIB3c2GlbV6EIh2ErEMJvFxMz/QTAPBgNVHRMBAf8EBTADAQH/MA4GA1UdDwEB
        /wQEAwIBhjAKBggqhkjOPQQDAgNoADBlAjA8tzEMRVX8vrFuOGDhvZr7OSJjbBr8
        gl2I70LeVNGEXZsAThUkqj5Rg9bV8xw3aSMCMQCDjB5CgsLH8EdZmiksdBRRKM2r
        vxo6c0dSSNrr7dDN+m2/dRvgoIpGL2GauOGqDFY=
        -----END CERTIFICATE-----
      - |
        -----BEGIN CERTIFICATE-----
        MIIFmjCCA4KgAwIBAgIJAKLyz15lYOrYMA0GCSqGSIb3DQEBCwUAMFoxCzAJBgNV
        BAYTAkZSMQ8wDQYDVQQIDAZGcmFuY2UxDjAMBgNVBAcMBVBhcmlzMRAwDgYDVQQK
        DAdGcmVlYm94MRgwFgYDVQQDDA9GcmVlYm94IFJvb3QgQ0EwHhcNMTUwNzMwMTUw
        OTIwWhcNMzUwNzI1MTUwOTIwWjBaMQswCQYDVQQGEwJGUjEPMA0GA1UECAwGRnJh
        bmNlMQ4wDAYDVQQHDAVQYXJpczEQMA4GA1UECgwHRnJlZWJveDEYMBYGA1UEAwwP
        RnJlZWJveCBSb290IENBMIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEA
        xqYIvq8538SH6BJ99jDlOPoyDBrlwKEp879oYplicTC2/p0X66R/ft0en1uSQadC
        sL/JTyfgyJAgI1Dq2Y5EYVT/7G6GBtVH6Bxa713mM+I/v0JlTGFalgMqamMuIRDQ
        tdyvqEIs8DcfGB/1l2A8UhKOFbHQsMcigxOe9ZodMhtVNn0mUyG+9Zgu1e/YMhsS
        iG4Kqap6TGtk80yruS1mMWVSgLOq9F5BGD4rlNlWLo0C3R10mFCpqvsFU+g4kYoA
        dTxaIpi1pgng3CGLE0FXgwstJz8RBaZObYEslEYKDzmer5zrU1pVHiwkjsgwbnuy
        WtM1Xry3Jxc7N/i1rxFmN/4l/Tcb1F7x4yVZmrzbQVptKSmyTEvPvpzqzdxVWuYi
        qIFSe/njl8dX9v5hjbMo4CeLuXIRE4nSq2A7GBm4j9Zb6/l2WIBpnCKtwUVlroKw
        NBgB6zHg5WI9nWGuy3ozpP4zyxqXhaTgrQcDDIG/SQS1GOXKGdkCcSa+VkJ0jTf5
        od7PxBn9/TuN0yYdgQK3YDjD9F9+CLp8QZK1bnPdVGywPfL1iztngF9J6JohTyL/
        VMvpWfS/X6R4Y3p8/eSio4BNuPvm9r0xp6IMpW92V8SYL0N6TQQxzZYgkLV7TbQI
        Hw6v64yMbbF0YS9VjS0sFpZcFERVQiodRu7nYNC1jy8CAwEAAaNjMGEwHQYDVR0O
        BBYEFD2erMkECujilR0BuER09FdsYIebMB8GA1UdIwQYMBaAFD2erMkECujilR0B
        uER09FdsYIebMA8GA1UdEwEB/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgGGMA0GCSqG
        SIb3DQEBCwUAA4ICAQAZ2Nx8mWIWckNY8X2t/ymmCbcKxGw8Hn3BfTDcUWQ7GLRf
        MGzTqxGSLBQ5tENaclbtTpNrqPv2k6LY0VjfrKoTSS8JfXkm6+FUtyXpsGK8MrLL
        hZ/YdADTfbbWOjjD0VaPUoglvo2N4n7rOuRxVYIij11fL/wl3OUZ7GHLgL3qXSz0
        +RGW+1oZo8HQ7pb6RwLfv42Gf+2gyNBckM7VVh9R19UkLCsHFqhFBbUmqwJgNA2/
        3twgV6Y26qlyHXXODUfV3arLCwFoNB+IIrde1E/JoOry9oKvF8DZTo/Qm6o2KsdZ
        dxs/YcIUsCvKX8WCKtH6la/kFCUcXIb8f1u+Y4pjj3PBmKI/1+Rs9GqB0kt1otyx
        Q6bqxqBSgsrkuhCfRxwjbfBgmXjIZ/a4muY5uMI0gbl9zbMFEJHDojhH6TUB5qd0
        JJlI61gldaT5Ci1aLbvVcJtdeGhElf7pOE9JrXINpP3NOJJaUSueAvxyj/WWoo0v
        4KO7njox8F6jCHALNDLdTsX0FTGmUZ/s/QfJry3VNwyjCyWDy1ra4KWoqt6U7SzM
        d5jENIZChM8TnDXJzqc+mu00cI3icn9bV9flYCXLTIsprB21wVSMh0XeBGylKxeB
        S27oDfFq04XSox7JM9HdTt2hLK96x1T7FpFrBTnALzb7vHv9MhXqAT90fPR/8A==
        -----END CERTIFICATE-----
```

## Deye

This adaptor is used to retrieve electricity production metrics from [DEYE micro inverters](https://deye.com/fr/product-category/inverter/microinverter/).
It provides technical metrics from the micro-inverter and all production metrics.

To configure this, you need to set the URL of the micro-inverter exposed on your network along with the user credentials.
An active time range is also provided to not try to get information during the night.
```yaml
deye:
  period:
    secs: 150
    nanos: 0
  target:
    url: http://user:password@192.168.1.x
  title_case_headers: true
  active_time_range:
    start: "06:00:00"
    end: "23:00:00"
```

## Shelly

[Shelly](https://shelly-api-docs.shelly.cloud/gen2/General/RPCChannels) devices from which power metrics are exposed.
It provides metrics for:
 - [EM](https://shelly-api-docs.shelly.cloud/gen2/ComponentsAndServices/EM)
 - [EMData](https://shelly-api-docs.shelly.cloud/gen2/ComponentsAndServices/EMData)
 - Switch channels with [power metering](https://shelly-api-docs.shelly.cloud/gen2/ComponentsAndServices/Switch)

```yaml
shelly:
  target:
    url: http://192.168.1.x
  authorization: false
```

For a password-protected Gen2+ device, use the fixed `admin` username and encode the password as base64-url in the target URL:

```yaml
shelly:
  target:
    url: http://admin:BASE64URL_PASSWORD@192.168.1.x
  authorization: false
```

Named power-metering switch channels are discovered from the Shelly configuration; unnamed channels are ignored. Their configured name is preserved in the `name` label. `prosa_shelly_instantaneous` reports active power and `prosa_shelly_power` reports consumed and returned active energy in Wh with `type` values `power` and `ret_power`.

## EcoFlow STREAM

`FetcherEcoFlowAdaptor` uses EcoFlow's signed IoT Open HTTP API to discover one directly bound STREAM system and obtain MQTT credentials, then receives the telemetry pushed by the device over EcoFlow's MQTT broker. Supported models are STREAM Ultra (`BK11`), Pro (`BK12`), AC Pro (`BK31`), Max (`BK41`), AC (`BK51`), and Ultra X (`BK61`); other EcoFlow devices on the account are ignored. Create an application in the [EcoFlow Developer Portal](https://developer-eu.ecoflow.com/) first. The secret key in the URL must be encoded in [base64-url](https://en.wikipedia.org/wiki/Base64#URL_applications); keep the resulting configuration outside version control.

```yaml
ecoflow:
  period:
    secs: 60
    nanos: 0
  target:
    url: https://ACCESS_KEY:BASE64URL_SECRET@api-e.ecoflow.com
  authorization: false
  mqtt:
    keep_alive:
      secs: 60
      nanos: 0
    max_packet_size: 65536
    reconnect_min:
      secs: 1
      nanos: 0
    reconnect_max:
      secs: 60
      nanos: 0
```

Use `api.ecoflow.com` instead for a Global/US developer account; the MQTT broker is the one returned by the API. The `mqtt` section is optional and shows the default values. Device discovery only includes equipment bound directly to the developer account, not shared devices. Multiple batteries in one cascaded STREAM system are supported; multiple independent STREAM systems require separate selection support and are rejected.

The device pushes changed values every few seconds, a full system snapshot every two minutes, a report of each battery pack every minute, and an inverter report every five minutes. Each `period` tick checks the session: without any update for five minutes, the metrics are dropped and a new MQTT session is opened. A broker refusing the credentials triggers a new certification request.

All metrics have an `sn` label; battery pack metrics also have a `pack` label with the pack serial number. A measurement is emitted only when the device reports it; a value that doesn't match its metric, e.g. text instead of a number, is ignored with a warning logged once per key. Signed values retain EcoFlow's convention; notably, positive battery power means charging and negative means discharging.

| Metric | Unit | Labels and values |
|---|---|---|
| `prosa_ecoflow_battery_charge` | % | System battery charge |
| `prosa_ecoflow_power` | W | `type`: `solar`, `solar_1`–`solar_4`, `battery`, `system_load`, `load_from_solar`, `load_from_battery`, `load_from_grid`, `system_grid`, `grid_connection`, `system_grid_connection`, `ac_output`, `ac_outlet_1`, `ac_outlet_2`, `socket_measure`, `meter_phase_1`–`meter_phase_3`, `offgrid_1`, `offgrid_2`, `inverter_grid` |
| `prosa_ecoflow_apparent_power` | VA | `type`: `offgrid_1`, `offgrid_2` |
| `prosa_ecoflow_reactive_power` | var | `type`: `inverter_grid` |
| `prosa_ecoflow_voltage` | V | `type`: `solar_1`–`solar_4`, `grid`, `offgrid_1`, `offgrid_2`, `inverter_grid_input`, `inverter_grid_output` |
| `prosa_ecoflow_current` | A | `type`: `solar_1`–`solar_4` |
| `prosa_ecoflow_frequency` | Hz | `type`: `grid`, `inverter` |
| `prosa_ecoflow_battery_health` | % | `type`: `system`, `main` |
| `prosa_ecoflow_battery_energy` | Wh | `type`: `full` |
| `prosa_ecoflow_battery_remaining_time` | s | `type`: `charge`, `discharge` |
| `prosa_ecoflow_soc_limit` | % | `type`: `max_charge`, `min_discharge`, `backup_reserve` |
| `prosa_ecoflow_power_limit` | W | `type`: `battery_input`, `battery_output`, `ac_input`, `ac_output`, `busbar`, `inverter_input`, `inverter_output`, `pack_input`, `pack_output`, `feed_grid`, `feed_grid_max`, `grid_input` |
| `prosa_ecoflow_temperature` | °C | `sensor`: `battery_cell_max`, `battery_cell_min`, `battery_mos_max`, `battery_mos_min`, `inverter`, `dc_1`–`dc_3`, `dab_high`, `dab_low` |
| `prosa_ecoflow_switch` | 0/1 | `name`: `relay_1`–`relay_4`, `battery_heating`, `bms_running`, `offgrid`, `solar_1_connected`–`solar_4_connected`, `self_powered_mode`, `scheduled_mode`, `tou_mode`, `intelligent_schedule_mode` |
| `prosa_ecoflow_wireless` | dBm | Wi-Fi signal strength |
| `prosa_ecoflow_fan_level` | level | Fan level |
| `prosa_ecoflow_fault` | code | `type`: `inverter`, `inverter_lock`, `inverter_communication`, `dab`, `dab_lock`, `pfc`, `grid`, `mppt_communication`, `mppt_1`–`mppt_4`; 0 when none |
| `prosa_ecoflow_state` | 1 | `name` and current `value`: `grid_connection`, `distributed_device`, `series_connection`, `grid_code`, `feed_grid_mode`, `charge_discharge` and `main_charge_discharge` (`idle`, `discharging`, `charging`) |
| `prosa_ecoflow_errors` | count | Active device error codes |
| `prosa_ecoflow_info` | 1 | `timezone`, `iot_version` |
| `prosa_ecoflow_pack_charge` | % | `type`: `reported`, `actual`, `target`, `precise` |
| `prosa_ecoflow_pack_health` | % | `type`: `reported`, `real`, `cycle`, `calendar` |
| `prosa_ecoflow_pack_voltage` | V | `type`: `pack`, `cell_max`, `cell_min`, `cell_1`… |
| `prosa_ecoflow_pack_current` | A | `type`: `measured`, `charge_target` |
| `prosa_ecoflow_pack_capacity` | Ah | `type`: `design`, `full`, `remaining` |
| `prosa_ecoflow_pack_cycles` | count | Charge cycles |
| `prosa_ecoflow_pack_energy` | Wh, counter | `type`: `charged`, `discharged` over the pack lifetime |
| `prosa_ecoflow_pack_charge_transferred` | Ah, counter | `type`: `charged`, `discharged` over the pack lifetime |
| `prosa_ecoflow_pack_power` | W | `type`: `input`, `output` |
| `prosa_ecoflow_pack_remaining_time` | s | Remaining time at the current power |
| `prosa_ecoflow_pack_temperature` | °C | `sensor`: `pack`, `cell_max`, `cell_min`, `mos_max`, `mos_min`, `env_max`, `env_min`, `current_sensor_max`, `current_sensor_min`, `heatfilm_max`, `heatfilm_min`, and per sensor `cell_1`…, `mos_1`…, `env_1`…, `current_sensor_1`…, `heatfilm_1`…, `powerport_1`… |
| `prosa_ecoflow_pack_fault` | code | `type`: `bms_fault`, `all_bms_fault`, `all_error`, `error`, `alarm_1`, `alarm_2`, `protect_1`, `protect_2`, `fault_state`; 0 when none |
| `prosa_ecoflow_pack_info` | 1 | `version`, `hardware` |
| `prosa_ecoflow_raw` | unscaled | `key`, and `index` for arrays: undocumented inverter power limits (`actPwrRated`, `actPwrByFreqDroop`, `actPwrByOfdp`, `actPwrByOvdp`, `actPwrByPerc`, `actPwrByRampUp`, `actPwrByUfip`, `addPwrActLimit`, `acTotalInvDeratePower`, `ongridInvDeratePower`, `offgridInvDeratePower`, `invInPowerAbilityMax`, `invOutPowerAbilityMax`, `ongridOutPowerAbilityMax`, `bypassOutPowerAbilityMax`, in W), inverter currents and capacitor voltage of unknown scale (`ongridIinRms`, `invIl1Rms`, `invVcapRms`), `chgOverCurOffCnt`, and battery pack balancing (`remainBalanceTime`, `balanceCmd`, `balanceState`, `maxVolDiff`, `diffSoc`) and status codes (`sysState`, `mosState`, `chgDsgState`, `afeSysStatus`, `bqSysStatReg`, `bsmSysEvent`, `sysPwrFlag`, `openBmsFlag`, `waterInFlag`, `mcuPinInStatus`, `mcuPinOutStatus`, `pwr12v`) |

`solar_1` through `solar_4` power is computed from each input's voltage and current. `relay_2` and `relay_3` drive the two AC outlets. Minimum and maximum temperatures of a battery pack sensor group are omitted when the pack reports no such sensor. Versions are decoded from their four-byte encoding, e.g. `1.0.2.19`. Other values published by the device, such as multi-unit cascade data, connectivity internals, and controller state words, are not exported.

## NUT UPS

`FetcherNutUpsAdaptor` reads a UPS from a [NUT `upsd` server](https://networkupstools.org/docs/developer-guide.chunked/net-protocol.html). The URL path is the UPS name; the adaptor sends only `LIST VAR <name>` and waits for `END LIST VAR <name>` without requiring the server to close the connection.

```yaml
nut_ups:
  target:
    url: tcp://127.0.0.1:3493/eaton
  period:
    secs: 60
    nanos: 0
  timeout:
    secs: 10
    nanos: 0
```

The adaptor publishes `prosa_nut_ups_realpower` (W), `prosa_nut_battery_charge` (%), `prosa_nut_ups_load` (%), `prosa_nut_battery_runtime` (s), and `prosa_nut_ups_status` (value 1 with a `status` label). All have an `ups` label. Missing variables are omitted. `prosa_nut_fetch_success` is 1 after a complete valid response and 0 after a failed fetch; failed fetches clear the prior measurement snapshot.
