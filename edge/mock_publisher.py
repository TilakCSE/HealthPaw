import time
import json
import random
import paho.mqtt.client as mqtt

# --- Configuration ---
BROKER = "broker.hivemq.com" # HiveMQ's public broker
PORT = 1883
TOPIC = "device/cts_01/telemetry"
DEVICE_ID = "cts_01"

TELEMETRY_HZ = 2
SAMPLE_INTERVAL = 1 / TELEMETRY_HZ

# --- Callbacks ---
def on_connect(client, userdata, flags, rc):
    """Callback for when the client receives a CONNACK response from the server."""
    if rc == 0:
        print(f"✅ Connected to MQTT Broker: {BROKER}")
    else:
        print(f"❌ Failed to connect, return code {rc}")

# --- Initialize Client ---
# Using the standard Paho client initialization
client = mqtt.Client(client_id="healthpaw_mock_edge")
client.on_connect = on_connect

# Connect to the broker
client.connect(BROKER, PORT, 60)

# Start the background thread to handle network traffic
client.loop_start()

print(f"📡 Starting telemetry simulation on topic: {TOPIC}")
print("Press Ctrl+C to stop.")

try:
    # --- Simulation Loop ---
    current_weight = 150.0  # Starting weight in grams
    
    while True:
        # Simulate slight sensor noise and occasional eating (weight drops)
        is_eating = random.choice([True, False, False, False]) # 25% chance pet is eating
        
        if is_eating and current_weight > 10:
            # Pet eats between 2g and 8g
            consumption = random.uniform(2.0, 8.0)
            current_weight -= consumption
            gate_status = "OPEN"
        else:
            # Just sensor noise (+/- 0.2g)
            current_weight += random.uniform(-0.2, 0.2)
            gate_status = "CLOSED"
            
        # Ensure weight doesn't drop below 0
        current_weight = max(0.0, current_weight)

        # Construct the payload matching your specification
        payload = {
            "device_id": DEVICE_ID,
            "timestamp": int(time.time() * 1000),
            "current_weight_g": round(current_weight, 2),
            "gate_status": gate_status
        }
        
        # Publish the JSON payload
        client.publish(TOPIC, json.dumps(payload))
        print(f"Published: {payload}")
        
        # Simulate a 10 Hz sampling rate (0.1 second delay)
        time.sleep(SAMPLE_INTERVAL)

except KeyboardInterrupt:
    print("\n🛑 Simulation stopped by user.")
    
finally:
    # Clean up the connection
    client.loop_stop()
    client.disconnect()
    print("🔌 Disconnected from broker.")