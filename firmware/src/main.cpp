#include <Arduino.h>
#include <WiFi.h>
#include <PubSubClient.h>

#include "config.h"
#include "secrets.h"

// ============================================================
// MQTT Client
// ============================================================

WiFiClient espClient;
PubSubClient mqttClient(espClient);

// ============================================================
// Timing
// ============================================================

unsigned long lastPublishTime = 0;

const unsigned long PUBLISH_INTERVAL = 1000;

// ============================================================
// Wi-Fi Connection
// ============================================================

void connectWiFi()
{
    Serial.println();
    Serial.print("Connecting to Wi-Fi: ");
    Serial.println(WIFI_SSID);

    WiFi.mode(WIFI_STA);
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);

    unsigned long startAttemptTime = millis();

    while (WiFi.status() != WL_CONNECTED &&
           millis() - startAttemptTime < 15000)
    {
        delay(500);
        Serial.print(".");
    }

    Serial.println();

    if (WiFi.status() == WL_CONNECTED)
    {
        Serial.println("✅ Wi-Fi connected");

        Serial.print("ESP32 IP address: ");
        Serial.println(WiFi.localIP());
    }
    else
    {
        Serial.print("❌ Wi-Fi connection failed. Status: ");
        Serial.println(WiFi.status());
    }
}

// ============================================================
// MQTT Connection
// ============================================================

void connectMQTT()
{
    while (!mqttClient.connected())
    {
        Serial.print("Connecting to MQTT broker... ");

        String clientId = "healthpaw_";
        clientId += DEVICE_ID;
        clientId += "_";
        clientId += String(random(0xffff), HEX);

        if (mqttClient.connect(clientId.c_str()))
        {
            Serial.println("✅ connected");
        }
        else
        {
            Serial.print("❌ failed, rc=");
            Serial.print(mqttClient.state());
            Serial.println(" | retrying in 2 seconds");

            delay(2000);
        }
    }
}

// ============================================================
// Publish Telemetry
// ============================================================

void publishTelemetry()
{
    // Temporary simulated weight.
    // This will be replaced by HX711 reading later.
    float simulatedWeight = 150.0;

    String payload = "{";
    payload += "\"device_id\":\"";
    payload += DEVICE_ID;
    payload += "\",";
    payload += "\"timestamp\":";
    payload += String(time(nullptr));
    payload += ",";
    payload += "\"current_weight_g\":";
    payload += String(simulatedWeight, 2);
    payload += ",";
    payload += "\"gate_status\":\"CLOSED\"";
    payload += "}";

    bool success = mqttClient.publish(
        MQTT_TOPIC,
        payload.c_str()
    );

    if (success)
    {
        Serial.print("📡 Published: ");
        Serial.println(payload);
    }
    else
    {
        Serial.println("❌ MQTT publish failed");
    }
}

// ============================================================
// Setup
// ============================================================

void setup()
{
    Serial.begin(115200);

    delay(1000);

    Serial.println();
    Serial.println("======================================");
    Serial.println("        HEALTHPAW ESP32 EDGE");
    Serial.println("======================================");

    connectWiFi();

    mqttClient.setServer(
        MQTT_BROKER,
        MQTT_PORT
    );

    connectMQTT();

    Serial.println();
    Serial.println("🚀 HealthPaw ESP32 is ready");
}

// ============================================================
// Main Loop
// ============================================================

void loop()
{
    if (WiFi.status() != WL_CONNECTED)
    {
        Serial.println("⚠️ Wi-Fi disconnected");
        connectWiFi();
    }

    if (!mqttClient.connected())
    {
        Serial.println("⚠️ MQTT disconnected");
        connectMQTT();
    }

    mqttClient.loop();

    unsigned long currentMillis = millis();

    if (currentMillis - lastPublishTime >= PUBLISH_INTERVAL)
    {
        lastPublishTime = currentMillis;

        publishTelemetry();
    }
}