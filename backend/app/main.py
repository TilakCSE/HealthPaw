import json
from contextlib import asynccontextmanager
from fastapi import FastAPI
import paho.mqtt.client as mqtt

# --- Configuration ---
BROKER = "broker.hivemq.com"
PORT = 1883
TOPIC = "device/cts_01/telemetry"

# --- MQTT Callbacks ---
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Backend connected to MQTT Broker")
        # Subscribe to the telemetry topic as soon as we connect
        client.subscribe(TOPIC)
        print(f"🎧 Subscribed to topic: {TOPIC}")
    else:
        print(f"❌ Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
    # This triggers every time a new message hits the subscribed topic
    try:
        payload = json.loads(msg.payload.decode())
        weight = payload.get("current_weight_g")
        status = payload.get("gate_status")
        print(f"📥 INGESTED -> Weight: {weight}g | Gate: {status}")
    except json.JSONDecodeError:
        print("⚠️ Received malformed JSON payload")

# --- Initialize MQTT Client ---
mqtt_client = mqtt.Client(client_id="healthpaw_fastapi_backend")
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# --- FastAPI Lifespan ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Startup: Connect to MQTT and start the background listening loop
    mqtt_client.connect(BROKER, PORT, 60)
    mqtt_client.loop_start()
    yield
    # 2. Shutdown: Stop the loop and disconnect gracefully
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    print("🔌 Backend disconnected from broker.")

# --- FastAPI App ---
app = FastAPI(lifespan=lifespan, title="HealthPaw API")

@app.get("/")
def read_root():
    return {"status": "HealthPaw Backend is running", "mqtt_connected": mqtt_client.is_connected()}