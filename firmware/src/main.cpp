#include <Arduino.h>
#include <WiFi.h>
#include <PubSubClient.h>
#include <ESP32Servo.h>
#include <HX711_ADC.h>

#include "config.h"
#include "secrets.h"

// ============================================================
// FUNCTION PROTOTYPES
// ============================================================

void connectWiFi();
void connectMQTT();

void stopServo();
void openGate();
void closeGate();

float readLoadCell();

void publishTelemetry();

void mqttCallback(
    char* topic,
    byte* payload,
    unsigned int length
);

void dispenseFood(float targetWeight);
void realWeightDispensing(float targetWeight);

// ============================================================
// HEALTHPAW HARDWARE CONFIGURATION
// ============================================================

// HX711
constexpr int HX711_DOUT_PIN = 25;
constexpr int HX711_SCK_PIN  = 14;

// Servo
constexpr int SERVO_PIN = 32;

// ============================================================
// CALIBRATION
// ============================================================

// Calibration factor obtained using approximately 170 g
// reference mass.
constexpr float CALIBRATION_FACTOR = 28.029411;

// Default target if backend does not provide one.
constexpr float DEFAULT_TARGET_WEIGHT = 150.0;

// Safety timeout.
constexpr unsigned long DISPENSE_TIMEOUT = 30000;

// Telemetry interval during dispensing.
constexpr unsigned long TELEMETRY_INTERVAL = PUBLISH_INTERVAL_MS;

// ============================================================
// SERVO CONFIGURATION
// ============================================================
//
// This servo is being operated as a continuous-rotation servo.
//
// 90  = STOP
// 180 = OPEN direction
// 0   = CLOSE direction
//
// The gate moves only for the specified time and then stops.
//
// If your physical gate moves in the opposite direction,
// swap SERVO_OPEN and SERVO_CLOSE.
//

constexpr int SERVO_STOP  = 90;
constexpr int SERVO_OPEN  = 180;
constexpr int SERVO_CLOSE = 0;

// Adjust these if the physical gate needs more/less movement.
constexpr unsigned long GATE_OPEN_TIME  = 700;
constexpr unsigned long GATE_CLOSE_TIME = 700;

// ============================================================
// HX711
// ============================================================

HX711_ADC LoadCell(
    HX711_DOUT_PIN,
    HX711_SCK_PIN
);

// ============================================================
// SERVO
// ============================================================

Servo gateServo;

// ============================================================
// WIFI / MQTT
// ============================================================

WiFiClient espClient;
PubSubClient mqttClient(espClient);

// ============================================================
// STATE
// ============================================================

float currentWeight = 0.0;

bool gateIsOpen = false;
bool dispensing = false;

// ============================================================
// SERVO FUNCTIONS
// ============================================================

void stopServo()
{
    gateServo.write(SERVO_STOP);
}

// ------------------------------------------------------------

void openGate()
{
    Serial.println();
    Serial.println("--------------------------------------");
    Serial.println("Opening gate...");
    Serial.println("--------------------------------------");

    gateServo.write(SERVO_OPEN);

    delay(GATE_OPEN_TIME);

    stopServo();

    gateIsOpen = true;

    Serial.println("Gate -> OPEN");
}

// ------------------------------------------------------------

void closeGate()
{
    Serial.println();
    Serial.println("--------------------------------------");
    Serial.println("Closing gate...");
    Serial.println("--------------------------------------");

    gateServo.write(SERVO_CLOSE);

    delay(GATE_CLOSE_TIME);

    stopServo();

    gateIsOpen = false;

    Serial.println("Gate -> CLOSED");
}

// ============================================================
// HX711 READING
// ============================================================

float readLoadCell()
{
    if (LoadCell.update())
    {
        return LoadCell.getData();
    }

    return currentWeight;
}

// ============================================================
// TELEMETRY
// ============================================================

void publishTelemetry()
{
    if (!mqttClient.connected())
    {
        return;
    }

    String payload = "{";

    payload += "\"device_id\":\"";
    payload += DEVICE_ID;
    payload += "\",";

    payload += "\"timestamp\":";
    payload += String(millis() / 1000);
    payload += ",";

    payload += "\"current_weight_g\":";
    payload += String(currentWeight, 2);
    payload += ",";

    payload += "\"gate_status\":\"";
    payload += gateIsOpen ? "OPEN" : "CLOSED";
    payload += "\"";

    payload += "}";

    bool success = mqttClient.publish(
        MQTT_TELEMETRY_TOPIC,
        payload.c_str()
    );

    if (success)
    {
        Serial.print("Telemetry published: ");
        Serial.println(payload);
    }
    else
    {
        Serial.println("Telemetry publish failed");
    }
}

// ============================================================
// MQTT CALLBACK
// ============================================================

void mqttCallback(
    char* topic,
    byte* payload,
    unsigned int length
)
{
    Serial.println();
    Serial.println("======================================");
    Serial.println("        MQTT MESSAGE RECEIVED");
    Serial.println("======================================");

    Serial.print("Topic: ");
    Serial.println(topic);

    // --------------------------------------------------------
    // Convert MQTT payload into String
    // --------------------------------------------------------

    String message;

    for (unsigned int i = 0; i < length; i++)
    {
        message += (char)payload[i];
    }

    Serial.print("Payload: ");
    Serial.println(message);

    // --------------------------------------------------------
    // Verify topic
    // --------------------------------------------------------

    if (String(topic) != MQTT_COMMAND_TOPIC)
    {
        Serial.println("Message ignored: wrong topic.");
        return;
    }

    // --------------------------------------------------------
    // Remove whitespace
    // --------------------------------------------------------

    String compactMessage = message;

    compactMessage.replace(" ", "");
    compactMessage.replace("\n", "");
    compactMessage.replace("\r", "");
    compactMessage.replace("\t", "");

    // --------------------------------------------------------
    // Check DISPENSE command
    // --------------------------------------------------------

    if (
        compactMessage.indexOf(
            "\"command\":\"DISPENSE\""
        ) < 0
    )
    {
        Serial.println("Unknown MQTT command");
        return;
    }

    Serial.println("Parsed command: DISPENSE");

    // --------------------------------------------------------
    // Extract target_mass_g
    // --------------------------------------------------------

    float targetWeight = DEFAULT_TARGET_WEIGHT;

    int targetIndex =
        compactMessage.indexOf("\"target_mass_g\"");

    if (targetIndex >= 0)
    {
        int colonIndex =
            compactMessage.indexOf(":", targetIndex);

        if (colonIndex >= 0)
        {
            String value =
                compactMessage.substring(colonIndex + 1);

            int commaIndex =
                value.indexOf(",");

            if (commaIndex >= 0)
            {
                value =
                    value.substring(0, commaIndex);
            }

            int braceIndex =
                value.indexOf("}");

            if (braceIndex >= 0)
            {
                value =
                    value.substring(0, braceIndex);
            }

            value.trim();

            float parsedWeight =
                value.toFloat();

            if (parsedWeight > 0)
            {
                targetWeight = parsedWeight;
            }
        }
    }

    // --------------------------------------------------------
    // Accept command
    // --------------------------------------------------------

    Serial.println("DISPENSE command accepted");

    Serial.print("Target weight: ");
    Serial.print(targetWeight, 2);
    Serial.println(" g");

    // --------------------------------------------------------
    // Start dispensing
    // --------------------------------------------------------

    dispenseFood(targetWeight);
}

// ============================================================
// REAL HX711 DISPENSING
// ============================================================

void realWeightDispensing(float targetWeight)
{
    Serial.println();
    Serial.println("======================================");
    Serial.println("       REAL LOAD CELL DISPENSING");
    Serial.println("======================================");

    Serial.print("Target: ");
    Serial.print(targetWeight, 2);
    Serial.println(" g");

    Serial.println();
    Serial.println("Reading HX711...");
    Serial.println();

    unsigned long startTime = millis();
    unsigned long lastPrint = 0;
    unsigned long lastTelemetry = 0;

    while (true)
    {
        // ----------------------------------------------------
        // Keep HX711 updated
        // ----------------------------------------------------

        if (LoadCell.update())
        {
            currentWeight = LoadCell.getData();
        }

        // ----------------------------------------------------
        // Display weight
        // ----------------------------------------------------

        if (millis() - lastPrint >= 250)
        {
            Serial.print("Weight -> ");
            Serial.print(currentWeight, 2);
            Serial.println(" g");

            lastPrint = millis();
        }

        // ----------------------------------------------------
        // Publish telemetry
        // ----------------------------------------------------

        if (millis() - lastTelemetry >= TELEMETRY_INTERVAL)
        {
            publishTelemetry();

            lastTelemetry = millis();
        }

        // ----------------------------------------------------
        // TARGET REACHED
        // ----------------------------------------------------

        if (currentWeight >= targetWeight)
        {
            Serial.println();
            Serial.println("======================================");
            Serial.println("       TARGET WEIGHT REACHED");
            Serial.println("======================================");

            Serial.print("Measured weight: ");
            Serial.print(currentWeight, 2);
            Serial.println(" g");

            break;
        }

        // ----------------------------------------------------
        // SAFETY TIMEOUT
        // ----------------------------------------------------

        if (
            millis() - startTime >=
            DISPENSE_TIMEOUT
        )
        {
            Serial.println();
            Serial.println("======================================");
            Serial.println("        DISPENSE TIMEOUT");
            Serial.println("======================================");

            Serial.print("Weight reached: ");
            Serial.print(currentWeight, 2);
            Serial.println(" g");

            Serial.println(
                "Stopping dispensing for safety."
            );

            break;
        }

        // ----------------------------------------------------
        // Keep MQTT connection alive
        // ----------------------------------------------------

        mqttClient.loop();

        delay(10);
    }
}

// ============================================================
// COMPLETE DISPENSE PROCESS
// ============================================================

void dispenseFood(float targetWeight)
{
    // Prevent another dispense command from
    // starting while one is already running.

    if (dispensing)
    {
        Serial.println(
            "Dispense already in progress."
        );

        return;
    }

    dispensing = true;

    Serial.println();
    Serial.println("======================================");
    Serial.println("       DISPENSE REQUEST RECEIVED");
    Serial.println("======================================");

    Serial.print("Target weight: ");
    Serial.print(targetWeight, 2);
    Serial.println(" g");

    Serial.println(
        "Mode: REAL HX711 LOAD CELL"
    );

    // --------------------------------------------------------
    // Get stable starting weight
    // --------------------------------------------------------

    Serial.println();
    Serial.println("Checking starting weight...");

    for (int i = 0; i < 30; i++)
    {
        LoadCell.update();
        delay(10);
    }

    currentWeight =
        LoadCell.getData();

    Serial.print("Starting weight: ");
    Serial.print(currentWeight, 2);
    Serial.println(" g");

    // --------------------------------------------------------
    // If already at target, don't open gate
    // --------------------------------------------------------

    if (currentWeight >= targetWeight)
    {
        Serial.println();
        Serial.println(
            "Target already reached."
        );

        Serial.println(
            "No dispensing required."
        );

        publishTelemetry();

        dispensing = false;

        return;
    }

    // --------------------------------------------------------
    // OPEN GATE
    // --------------------------------------------------------

    openGate();

    // Tell backend that gate is open.
    publishTelemetry();

    Serial.println();
    Serial.println(
        "Starting real load-cell monitoring..."
    );

    // --------------------------------------------------------
    // Monitor HX711 until target reached
    // --------------------------------------------------------

    realWeightDispensing(targetWeight);

    // --------------------------------------------------------
    // CLOSE GATE
    // --------------------------------------------------------

    closeGate();

    // --------------------------------------------------------
    // Get final reading
    // --------------------------------------------------------

    Serial.println();
    Serial.println(
        "Taking final HX711 reading..."
    );

    for (int i = 0; i < 20; i++)
    {
        LoadCell.update();
        delay(10);
    }

    currentWeight =
        LoadCell.getData();

    // --------------------------------------------------------
    // Final telemetry
    // --------------------------------------------------------

    publishTelemetry();

    // --------------------------------------------------------
    // COMPLETE
    // --------------------------------------------------------

    Serial.println();
    Serial.println("======================================");
    Serial.println("       DISPENSE COMPLETE");
    Serial.println("======================================");

    Serial.print("Final weight: ");
    Serial.print(currentWeight, 2);
    Serial.println(" g");

    Serial.println("Gate status: CLOSED");

    dispensing = false;
}

// ============================================================
// WIFI
// ============================================================

void connectWiFi()
{
    Serial.println();
    Serial.print("Connecting to Wi-Fi: ");
    Serial.println(WIFI_SSID);

    WiFi.mode(WIFI_STA);

    WiFi.begin(
        WIFI_SSID,
        WIFI_PASSWORD
    );

    unsigned long startAttempt =
        millis();

    while (
        WiFi.status() != WL_CONNECTED &&
        millis() - startAttempt < 15000
    )
    {
        delay(500);
        Serial.print(".");
    }

    Serial.println();

    if (WiFi.status() == WL_CONNECTED)
    {
        Serial.println("Wi-Fi connected");

        Serial.print("ESP32 IP address: ");
        Serial.println(WiFi.localIP());
    }
    else
    {
        Serial.println(
            "Wi-Fi connection failed"
        );
    }
}

// ============================================================
// MQTT
// ============================================================

void connectMQTT()
{
    while (!mqttClient.connected())
    {
        Serial.print(
            "Connecting to MQTT broker... "
        );

        String clientId =
            "healthpaw_";

        clientId += DEVICE_ID;
        clientId += "_";
        clientId += String(
            random(0xffff),
            HEX
        );

        if (
            mqttClient.connect(
                clientId.c_str()
            )
        )
        {
            Serial.println(
                "MQTT connected"
            );

            bool subscribed =
                mqttClient.subscribe(
                    MQTT_COMMAND_TOPIC
                );

            if (subscribed)
            {
                Serial.print(
                    "Subscribed to: "
                );

                Serial.println(
                    MQTT_COMMAND_TOPIC
                );
            }
            else
            {
                Serial.println(
                    "MQTT subscription failed"
                );
            }

            // Send initial telemetry.
            publishTelemetry();
        }
        else
        {
            Serial.print(
                "MQTT failed, rc="
            );

            Serial.println(
                mqttClient.state()
            );

            delay(2000);
        }
    }
}

// ============================================================
// SETUP
// ============================================================

void setup()
{
    Serial.begin(115200);

    delay(1000);

    Serial.println();
    Serial.println("======================================");
    Serial.println("        HEALTHPAW ESP32 EDGE");
    Serial.println("======================================");

    // ========================================================
    // HARDWARE MODE
    // ========================================================

    Serial.println();
    Serial.println("MODE = REAL HARDWARE");
    Serial.println("Using HX711 load cell.");

    // ========================================================
    // SERVO
    // ========================================================

    Serial.println();
    Serial.println("Initializing servo...");

    gateServo.setPeriodHertz(50);

    gateServo.attach(
        SERVO_PIN,
        500,
        2400
    );

    // IMPORTANT:
    // Stop the continuous-rotation servo immediately.
    stopServo();

    delay(500);

    gateIsOpen = false;

    Serial.println("Servo initialized.");
    Serial.println("Gate -> CLOSED");

    // ========================================================
    // HX711
    // ========================================================

    Serial.println();
    Serial.println("Initializing HX711...");

    LoadCell.begin();

    Serial.println(
        "Stabilizing HX711 and taring..."
    );

    // 3 seconds stabilization + tare.
    LoadCell.start(
        3000,
        true
    );

    if (
        LoadCell.getTareTimeoutFlag() ||
        LoadCell.getSignalTimeoutFlag()
    )
    {
        Serial.println();
        Serial.println("HX711 ERROR");

        Serial.println();
        Serial.println("Check wiring:");

        Serial.println(
            "HX711 VCC  -> ESP32 5V/VIN"
        );

        Serial.println(
            "HX711 GND  -> ESP32 GND"
        );

        Serial.println(
            "HX711 DOUT -> ESP32 GPIO 25"
        );

        Serial.println(
            "HX711 SCK  -> ESP32 GPIO 14"
        );

        while (true)
        {
            stopServo();
            delay(1000);
        }
    }

    // --------------------------------------------------------
    // Apply calibration factor
    // --------------------------------------------------------

    LoadCell.setCalFactor(
        CALIBRATION_FACTOR
    );

    // --------------------------------------------------------
    // Wait for first valid reading
    // --------------------------------------------------------

    while (!LoadCell.update())
    {
        delay(10);
    }

    currentWeight =
        LoadCell.getData();

    Serial.println();
    Serial.println("HX711 READY");

    Serial.print(
        "Calibration factor: "
    );

    Serial.println(
        CALIBRATION_FACTOR,
        6
    );

    Serial.print(
        "Initial weight: "
    );

    Serial.print(
        currentWeight,
        2
    );

    Serial.println(" g");

    // ========================================================
    // WIFI
    // ========================================================

    connectWiFi();

    // ========================================================
    // MQTT
    // ========================================================

    mqttClient.setServer(
        MQTT_BROKER,
        MQTT_PORT
    );

    mqttClient.setCallback(
        mqttCallback
    );

    connectMQTT();

    // ========================================================
    // SYSTEM READY
    // ========================================================

    Serial.println();
    Serial.println("======================================");
    Serial.println("          SYSTEM READY");
    Serial.println("======================================");

    Serial.println();

    Serial.print(
        "Current weight: "
    );

    Serial.print(
        currentWeight,
        2
    );

    Serial.println(" g");

    Serial.println(
        "Gate status: CLOSED"
    );

    Serial.println();

    Serial.println(
        "Waiting for MQTT DISPENSE command..."
    );

    Serial.println();
}

// ============================================================
// LOOP
// ============================================================

void loop()
{
    // --------------------------------------------------------
    // Keep HX711 updated when not dispensing
    // --------------------------------------------------------

    if (!dispensing)
    {
        if (LoadCell.update())
        {
            currentWeight =
                LoadCell.getData();
        }
    }

    // --------------------------------------------------------
    // Wi-Fi recovery
    // --------------------------------------------------------

    if (WiFi.status() != WL_CONNECTED)
    {
        Serial.println(
            "Wi-Fi disconnected"
        );

        connectWiFi();
    }

    // --------------------------------------------------------
    // MQTT recovery
    // --------------------------------------------------------

    if (!mqttClient.connected())
    {
        Serial.println(
            "MQTT disconnected"
        );

        connectMQTT();
    }

    // --------------------------------------------------------
    // MQTT processing
    // --------------------------------------------------------

    mqttClient.loop();

    delay(10);
}