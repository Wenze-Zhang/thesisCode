import json
import random
import sys
import time

from tb_client import ensure_device, mqtt_client

TOPIC = "v1/devices/me/telemetry"

MAX_EV_POWER_KW = 22.0
EV_BATTERY_CAPACITY_KWH = 60.0


def generate_energy_meter_payload(state, interval):
    if "energy_kwh" not in state:
        state["energy_kwh"] = round(random.uniform(1000, 5000), 2)

    power_w = round(random.uniform(1100, 1300), 1)
    state["energy_kwh"] = round(
        state["energy_kwh"] + power_w * interval / 3600 / 1000,
        4,
    )
    return {
        "power_w": power_w,
        "voltage_v": round(random.uniform(228.0, 232.0), 2),
        "current_a": round(power_w / 230.0, 3),
        "energy_kwh": state["energy_kwh"],
        "phase": "3P",
    }


def generate_climate_payload(state, interval):
    return {
        "temperature_c": round(random.uniform(21.0, 26.0), 2),
        "humidity_pct": round(random.uniform(40.0, 60.0), 1),
        "co2_ppm": random.randint(420, 900),
        "hvac_state": random.choice(["cooling", "idle", "heating"]),
    }


def generate_water_payload(state, interval):
    if "total_m3" not in state:
        state["total_m3"] = round(random.uniform(100.0, 500.0), 3)

    flow_lpm = round(random.uniform(5.0, 40.0), 2)
    state["total_m3"] = round(
        state["total_m3"] + flow_lpm * interval / 60.0 / 1000.0,
        4,
    )
    return {
        "flow_lpm": flow_lpm,
        "total_m3": state["total_m3"],
        "pressure_bar": round(random.uniform(2.5, 4.0), 2),
        "leak_detected": random.random() < 0.02,
    }


def aqi_from_pm25(pm25):
    if pm25 <= 12:
        return int(pm25 / 12 * 50)
    if pm25 <= 35.4:
        return int(51 + (pm25 - 12) / (35.4 - 12) * 49)
    if pm25 <= 55.4:
        return int(101 + (pm25 - 35.4) / (55.4 - 35.4) * 49)
    return int(151 + min((pm25 - 55.4) / 100 * 100, 199))


def generate_air_quality_payload(state, interval):
    pm25 = round(random.uniform(5.0, 80.0), 1)
    pm10 = round(pm25 + random.uniform(2.0, 20.0), 1)
    return {
        "pm2_5_ugm3": pm25,
        "pm10_ugm3": pm10,
        "voc_ppb": random.randint(50, 600),
        "aqi": aqi_from_pm25(pm25),
    }


def generate_ev_charger_payload(state, interval):
    if "state" not in state:
        state["state"] = "idle"
        state["soc_pct"] = round(random.uniform(20.0, 60.0), 1)
        state["session_kwh"] = 0.0

    if state["state"] == "idle" and random.random() < 0.25:
        state["state"] = "charging"
        state["session_kwh"] = 0.0
    elif state["state"] == "charging" and (
        state["soc_pct"] >= 95.0 or random.random() < 0.05
    ):
        state["state"] = "complete"
    elif state["state"] == "complete" and random.random() < 0.4:
        state["state"] = "idle"

    if state["state"] == "charging":
        power_kw = round(random.uniform(6.0, MAX_EV_POWER_KW), 2)
        delta_kwh = power_kw * interval / 3600.0
        state["session_kwh"] = round(state["session_kwh"] + delta_kwh, 4)
        state["soc_pct"] = round(
            min(
                100.0,
                state["soc_pct"] + delta_kwh / EV_BATTERY_CAPACITY_KWH * 100.0,
            ),
            2,
        )
    else:
        power_kw = 0.0

    return {
        "state": state["state"],
        "power_kw": power_kw,
        "soc_pct": state["soc_pct"],
        "session_kwh": state["session_kwh"],
        "voltage_v": round(random.uniform(395.0, 410.0), 2),
        "current_a": round(power_kw * 1000.0 / 400.0, 2) if power_kw else 0.0,
    }


DEVICE_TYPES = {
    "energy": {
        "label": "Energy meter",
        "default_name": "energy-meter-001",
        "default_interval": 5.0,
        "generator": generate_energy_meter_payload,
    },
    "climate": {
        "label": "Climate sensor",
        "default_name": "sim-climate-sensor",
        "default_interval": 5.0,
        "generator": generate_climate_payload,
    },
    "water": {
        "label": "Water meter",
        "default_name": "sim-water-meter",
        "default_interval": 10.0,
        "generator": generate_water_payload,
    },
    "air": {
        "label": "Air quality",
        "default_name": "sim-air-quality-01",
        "default_interval": 15.0,
        "generator": generate_air_quality_payload,
    },
    "ev": {
        "label": "EV charger",
        "default_name": "sim-ev-charger-01",
        "default_interval": 10.0,
        "generator": generate_ev_charger_payload,
    },
}


def usage():
    types = ", ".join(DEVICE_TYPES)
    print(f"Usage: python -u simulator.py <type> [device_name] [interval]")
    print(f"Available types: {types}")


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in DEVICE_TYPES:
        usage()
        sys.exit(2)

    device_type = sys.argv[1]
    config = DEVICE_TYPES[device_type]
    device_name = sys.argv[2] if len(sys.argv) > 2 else config["default_name"]
    interval = float(sys.argv[3]) if len(sys.argv) > 3 else config["default_interval"]

    _, token = ensure_device(device_name, label=config["label"])
    client = mqtt_client(token)
    state = {}

    print(f"[{device_type}] sending every {interval}s via MQTT -> {device_name}")

    try:
        while True:

            payload = config["generator"](state, interval)

            # Transmit from Python dict to JSON string 
            # ThingsBoard receives operational telemetry in JSON format.
            result = client.publish(TOPIC, json.dumps(payload))
            
            # examine the result code of message delivery
            status = "OK" if result.rc == 0 else f"FAIL rc={result.rc}"
            
            print(f"[{status}] {payload}")
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print(f"\n[{device_type}] stopped.")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
