#pragma once

// ============================================================
// HealthPaw MQTT Configuration
// ============================================================

#define MQTT_BROKER "broker.hivemq.com"
#define MQTT_PORT 1883

#define DEVICE_ID "cts_01"

#define MQTT_TELEMETRY_TOPIC "device/cts_01/telemetry"
#define MQTT_COMMAND_TOPIC   "device/cts_01/command"

// ============================================================
// Telemetry
// ============================================================

#define PUBLISH_INTERVAL_MS 500