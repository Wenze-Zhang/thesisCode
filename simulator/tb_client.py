import os
import sys
from typing import Tuple
import paho.mqtt.client as mqtt
import requests

TB_HOST = os.environ.get("TB_HOST", "http://localhost:8080")
TENANT_USER = os.environ.get("TB_USER", "tenant@thingsboard.org")
TENANT_PASS = os.environ.get("TB_PASS", "tenant")
MQTT_HOST = os.environ.get("TB_MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("TB_MQTT_PORT", "1883"))


def login() -> str:
    r = requests.post(
        f"{TB_HOST}/api/auth/login",
        json={"username": TENANT_USER, "password": TENANT_PASS},
        timeout=10,
    )
    r.raise_for_status()  
    return r.json()["token"]


def ensure_device(name: str, label: str = "") -> Tuple[str, str]:
    jwt = login()
    headers = {"X-Authorization": f"Bearer {jwt}"}

    r = requests.get(
        f"{TB_HOST}/api/tenant/devices",
        params={"deviceName": name},
        headers=headers,
        timeout=10,
    )
    # http return status code
    if r.status_code == 200:
        device_id = r.json()["id"]["id"]
        created = False
    elif r.status_code == 404:
        r = requests.post(
            f"{TB_HOST}/api/device",
            json={"name": name, "label": label or name, "type": "default"},
            headers={**headers, "Content-Type": "application/json"},
            timeout=10,
        )
        r.raise_for_status()
        device_id = r.json()["id"]["id"]
        created = True
    else:
        r.raise_for_status()
        sys.exit(1)

    r = requests.get(
        f"{TB_HOST}/api/device/{device_id}/credentials",
        headers=headers,
        timeout=10,
    )
    r.raise_for_status()
    
    token = r.json()["credentialsId"]
    print(
        f"[tb_client] device '{name}' {'created' if created else 'reused'} "
        f"id={device_id} token={token}"
    )
    return device_id, token


def mqtt_client(access_token: str) -> mqtt.Client:

    client = mqtt.Client()
    client.username_pw_set(access_token)
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()
    return client
