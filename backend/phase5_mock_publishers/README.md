# Phase 5 accelerated MQTT demonstrations

These five scripts publish synthetic weight telemetry to the same backend MQTT
ingestion path used by the feeder. The backend must be running and subscribed
before starting a publisher.

## Start the backend for the classroom demo

In PowerShell terminal 1:

```powershell
cd D:\Development\Projects\HealthPaw\backend
$env:HEALTHPAW_INACTIVITY_TIMEOUT_SECONDS = '150'
$env:HEALTHPAW_TELEMETRY_BATCH_SIZE = '1000'
.\.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

The 150-second timeout is for the accelerated demonstration only. The app's
default remains 600 seconds when that environment variable is not set. The
backend subscribes to the hardware topic and the isolated demo topic
`device/phase5_demo_v2/telemetry`.
The larger batch size only affects the synthetic rehearsal and reduces repeat
sessionization work; the normal default remains 20 telemetry rows.

## Run one scenario per terminal command

In PowerShell terminal 2, from the project root:

```powershell
cd D:\Development\Projects\HealthPaw
$env:HEALTHPAW_INACTIVITY_TIMEOUT_SECONDS = '150'
backend\.venv\Scripts\python.exe backend\phase5_mock_publishers\normal.py
```

Run these one at a time, waiting for the backend's `SYNTHETIC DEMO PREDICTION`
message before starting the next:

```powershell
backend\.venv\Scripts\python.exe backend\phase5_mock_publishers\rapid_eating.py
backend\.venv\Scripts\python.exe backend\phase5_mock_publishers\reduced_intake.py
backend\.venv\Scripts\python.exe backend\phase5_mock_publishers\no_consumption.py
backend\.venv\Scripts\python.exe backend\phase5_mock_publishers\prolonged_irregular.py
```

Use `--offline` to check the generated trace and model output without publishing
to MQTT or writing to Neon. The default `--delay 0.01` accelerates the wall-clock
playback; telemetry timestamps retain a 0.5-second (2 Hz) simulated cadence.

## Important scope note

This is synthetic accelerated telemetry, not a real dog's live feeding. The
current event pipeline cannot persist a true zero-event feeding opportunity.
To exercise the end-to-end persistence path for the `No Consumption` model
scenario, that publisher emits one 0.75 g threshold-edge change. Call it a
near-zero, threshold-edge synthetic example. The direct `/predict` example in
`data/output/v4.3_synthetic_baseline/prediction_examples.json` still uses an
exact zero-intake feature vector.

All printed classes come from the v4.3 synthetic demonstration model and are
not validated on real dogs. The scripts leave rows on the isolated
`phase5_demo_v2` device in Neon for review; do not point the demo device at live
hardware.
