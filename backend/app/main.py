import json
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import paho.mqtt.client as mqtt

from datetime import datetime, timezone

from sqlalchemy.orm import Session
from sqlalchemy import select

from .database import engine
from .models import Device, Telemetry
from .session_pipeline import process_device_telemetry

import queue
import threading


# ============================================================
# MQTT CONFIGURATION
# ============================================================

BROKER = "broker.hivemq.com"
PORT = 1883

DEVICE_ID = "cts_01"

TELEMETRY_TOPIC = f"device/{DEVICE_ID}/telemetry"
COMMAND_TOPIC = f"device/{DEVICE_ID}/command"

# ============================================================
# TELEMETRY QUEUE
# ============================================================

telemetry_queue = queue.Queue()


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

# ============================================================
# TELEMETRY DATABASE WORKER
# ============================================================

def telemetry_database_worker():
    print("🗄️ Telemetry database worker started")

    BATCH_SIZE = 20
    MAX_WAIT_SECONDS = 0.5

    while True:
        batch = []

        # Wait for the first telemetry record
        try:
            first_payload = telemetry_queue.get(
                timeout=MAX_WAIT_SECONDS
            )
        except queue.Empty:
            continue

        if first_payload is None:
            telemetry_queue.task_done()
            break

        batch.append(first_payload)

        # Collect additional records without blocking
        while len(batch) < BATCH_SIZE:
            try:
                payload = telemetry_queue.get_nowait()

                if payload is None:
                    telemetry_queue.task_done()
                    break

                batch.append(payload)

            except queue.Empty:
                break

        try:
            # --------------------------------------------------
            # Find all devices needed by this batch
            # --------------------------------------------------

            device_ids = {
                payload["device_id"]
                for payload in batch
            }

            with Session(engine) as session:

                devices = session.scalars(
                    select(Device).where(
                        Device.device_id.in_(device_ids)
                    )
                ).all()

                valid_device_ids = {
                    device.device_id
                    for device in devices
                }

                telemetry_rows = []

                for payload in batch:

                    device_id = payload["device_id"]

                    if device_id not in valid_device_ids:
                        print(
                            f"⚠️ Rejected telemetry: "
                            f"unknown device '{device_id}'"
                        )
                        continue

                    # Device observation timestamp
                    observation_time = datetime.fromtimestamp(
                        payload["timestamp"] / 1000,
                        tz=timezone.utc,
                    ).replace(tzinfo=None)

                    # Backend MQTT receipt timestamp
                    received_at = payload["received_at"]

                    telemetry_rows.append(
                        Telemetry(
                            device_id=device_id,
                            timestamp=observation_time,
                            current_weight_g=float(
                                payload["current_weight_g"]
                            ),
                            gate_status=payload["gate_status"],
                            received_at=received_at,
                        )
                    )

                # --------------------------------------------------
                # One database transaction for the whole batch
                # --------------------------------------------------

                if telemetry_rows:
                    session.add_all(telemetry_rows)
                    session.commit()

                    print(
                        f"💾 STORED BATCH → "
                        f"{len(telemetry_rows)} rows | "
                        f"Queue: {telemetry_queue.qsize()}"
                    )

                    for device_id in device_ids & valid_device_ids:
                        process_device_telemetry(session, device_id)

        except Exception as e:
            print(f"❌ Database batch error: {e}")

        finally:
            # Mark every item retrieved from the queue as processed
            for _ in batch:
                telemetry_queue.task_done()

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())

        # --------------------------------------------------
        # Validate required fields
        # --------------------------------------------------

        required_fields = [
            "device_id",
            "timestamp",
            "current_weight_g",
            "gate_status",
        ]

        missing_fields = [
            field
            for field in required_fields
            if field not in payload
        ]

        if missing_fields:
            print(
                f"⚠️ Rejected telemetry. "
                f"Missing fields: {missing_fields}"
            )
            return

        device_id = payload["device_id"]
        timestamp = payload["timestamp"]
        current_weight_g = payload["current_weight_g"]
        gate_status = payload["gate_status"]

        # --------------------------------------------------
        # Validate values
        # --------------------------------------------------

        if not isinstance(device_id, str):
            print("⚠️ Rejected telemetry: invalid device_id")
            return

        if not isinstance(timestamp, int):
            print(
                "⚠️ Rejected telemetry: "
                "timestamp must be Unix milliseconds"
            )
            return

        if timestamp <= 0:
            print(
                "⚠️ Rejected telemetry: "
                "invalid timestamp"
            )
            return

        if not isinstance(
            current_weight_g,
            (int, float)
        ):
            print(
                "⚠️ Rejected telemetry: "
                "invalid current_weight_g"
            )
            return

        if current_weight_g < 0:
            print(
                "⚠️ Rejected telemetry: "
                "negative current_weight_g"
            )
            return

        if gate_status not in ["OPEN", "CLOSED"]:
            print(
                "⚠️ Rejected telemetry: "
                "invalid gate_status"
            )
            return

        # --------------------------------------------------
        # Queue validated telemetry
        # --------------------------------------------------

        received_at = datetime.now(
            timezone.utc
        ).replace(tzinfo=None)

        telemetry_queue.put({
            "device_id": device_id,
            "timestamp": timestamp,
            "current_weight_g": float(current_weight_g),
            "gate_status": gate_status,
            "received_at": received_at,
        })

    except json.JSONDecodeError:
        print("⚠️ Received malformed JSON payload")

    except Exception as e:
        print(f"❌ Telemetry processing error: {e}")


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

    db_worker = threading.Thread(
        target=telemetry_database_worker,
        daemon=True,
    )

    db_worker.start()

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
