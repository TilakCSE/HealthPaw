import json
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import paho.mqtt.client as mqtt


# ============================================================
# MQTT CONFIGURATION
# ============================================================

BROKER = "broker.hivemq.com"
PORT = 1883

DEVICE_ID = "cts_01"

TELEMETRY_TOPIC = f"device/{DEVICE_ID}/telemetry"
COMMAND_TOPIC = f"device/{DEVICE_ID}/command"


# ============================================================
# MQTT CALLBACKS
# ============================================================

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("✅ Backend connected to MQTT Broker")

        client.subscribe(TELEMETRY_TOPIC)

        print(f"🎧 Subscribed to telemetry: {TELEMETRY_TOPIC}")
    else:
        print(f"❌ Failed to connect to MQTT Broker, return code: {rc}")


def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())

        weight = payload.get("current_weight_g")
        status = payload.get("gate_status")

        print(
            f"📥 INGESTED -> "
            f"Weight: {weight}g | "
            f"Gate: {status}"
        )

    except json.JSONDecodeError:
        print("⚠️ Received malformed JSON payload")


# ============================================================
# MQTT CLIENT
# ============================================================

mqtt_client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id="healthpaw_fastapi_backend"
)

mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message


# ============================================================
# DISPENSE REQUEST MODEL
# ============================================================

class DispenseRequest(BaseModel):
    target_mass_g: float = Field(
        default=150.0,
        gt=0,
        le=1000
    )


# ============================================================
# FASTAPI LIFESPAN
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):

    print("🚀 Starting HealthPaw backend...")

    try:
        mqtt_client.connect(BROKER, PORT, 60)
        mqtt_client.loop_start()

    except Exception as e:
        print(f"❌ MQTT connection failed: {e}")

    yield

    print("🛑 Shutting down HealthPaw backend...")

    mqtt_client.loop_stop()

    if mqtt_client.is_connected():
        mqtt_client.disconnect()

    print("🔌 Backend disconnected from broker.")


# ============================================================
# FASTAPI APP
# ============================================================

app = FastAPI(
    lifespan=lifespan,
    title="HealthPaw API",
    description="HealthPaw smart pet feeder backend API"
)


# ============================================================
# ROOT
# ============================================================

@app.get("/")
def read_root():

    return {
        "status": "HealthPaw Backend is running",
        "mqtt_connected": mqtt_client.is_connected(),
        "device_id": DEVICE_ID
    }


# ============================================================
# DISPENSE COMMAND
# ============================================================

@app.post("/dispense")
def dispense(request: DispenseRequest):

    # Make sure backend is connected to MQTT
    if not mqtt_client.is_connected():
        raise HTTPException(
            status_code=503,
            detail="MQTT broker is not connected"
        )

    # Generate unique transaction ID
    transaction_id = f"tx_{uuid.uuid4().hex[:8]}"

    # Build MQTT command
    payload = {
        "command": "DISPENSE",
        "target_mass_g": request.target_mass_g,
        "transaction_id": transaction_id
    }

    payload_json = json.dumps(payload)

    print()
    print("======================================")
    print("       DISPENSE COMMAND")
    print("======================================")
    print(f"Device: {DEVICE_ID}")
    print(f"Target mass: {request.target_mass_g} g")
    print(f"Transaction: {transaction_id}")
    print(f"Topic: {COMMAND_TOPIC}")
    print(f"Payload: {payload_json}")
    print("======================================")
    print()

    # Publish command to MQTT broker
    result = mqtt_client.publish(
        COMMAND_TOPIC,
        payload_json
    )

    # Check whether Paho accepted the publish
    if result.rc != mqtt.MQTT_ERR_SUCCESS:

        print(
            f"❌ Failed to publish command. "
            f"MQTT error code: {result.rc}"
        )

        raise HTTPException(
            status_code=500,
            detail="Failed to publish MQTT dispense command"
        )

    print("✅ DISPENSE command published successfully")

    return {
        "status": "command_sent",
        "device_id": DEVICE_ID,
        "command": "DISPENSE",
        "target_mass_g": request.target_mass_g,
        "transaction_id": transaction_id,
        "mqtt_topic": COMMAND_TOPIC
    }