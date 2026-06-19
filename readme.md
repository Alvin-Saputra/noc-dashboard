# Project Watchtower - NOC Dashboard

Watchtower is a high-throughput, concurrent streaming log ingestion and analysis system built with Go. It features a real-time Server-Sent Events (SSE) dashboard, a machine learning worker for anomaly detection and forecasting, and a stateless-to-stateful recovery architecture using MinIO.

## Prerequisites

Before running this project, ensure you have the following installed on your machine:

1. **Go** (v1.21 or higher)
2. **Docker** and **Docker Compose** (for running MinIO Object Storage)
3. **Git**

---

## Getting Started: 10-Minute Cold Start Guide

Follow these exact steps in order to start the system from a fresh clone.

### Step 1: Clone the Repository

Open your terminal and clone this repository, then navigate into the project directory:

```bash
git clone <URL_GITHUB_REPOSITORY_ANDA>
cd noc-dashboard
```

---

### Step 2: Start MinIO Storage

We use Docker Compose to spin up a local MinIO instance (acting as our S3-compatible object storage).

Run the following command:

```bash
docker-compose up -d
```

**Expected Output:**

Docker will pull the MinIO image and start the container in the background. You should see the `minio` service in a **Started** or **Running** state.

---

### Step 3: Download Dependencies

Ensure all Go modules and dependencies are correctly downloaded:

```bash
go mod tidy
```

**Expected Output:**

Go will download the required packages, including:

```text
github.com/minio/minio-go/v7
```

No errors should be displayed.

---

### Step 4: Run the Application

Start the Go backend server and the streaming pipeline:

```bash
go run main.go
```

**Expected Output:**

```text
[OK] Config successfully Loaded.
[OK] Channels Successfully Created.
[System] ℹ️ Tidak ada state sebelumnya (Mulai Drop Counter dari 0).
[Server] 🚀 Web Server API at port 8081...
```

---

## Verifying System Features

Once the server is running, verify the three core features below.

---

### 1. Verify the Real-Time Dashboard

Open your web browser and navigate to:

```text
http://localhost:8081
```

**Expected Output:**

- The **NOC Watchtower** dashboard loads successfully.
- In the **Live Event Feed** panel, new logs appear continuously without requiring a page refresh.
- In the **Ingestion Rates** panel, counters for:
  - Prometheus
  - Dynatrace
  - Splunk
  - Riverbed

  will increment rapidly.

- The **5 Minutes AI Forecast** panel will eventually display numerical predictions after approximately **50–100 data points** have been collected.

---

### 2. Trigger Policy Hot-Reload (Backpressure Test)

This test validates the dynamic policy watcher and the backpressure drop counter.

#### Steps

1. Locate the **Policy Configuration (JSON)** panel on the left side of the dashboard.
2. Change:

```json
"default_priority": "P4"
```

to:

```json
"default_priority": "P99"
```

3. Click the **Update Policy** button.

#### Expected Output

- The UI displays:

```text
Policy berhasil diupdate!
```

- Because of the stricter policy, the Noise Filter drops almost all incoming traffic.
- The **Dropped Events** counter starts increasing rapidly (e.g. 500, 1000, 2000, and so on).
- The terminal displays logs similar to:

```text
[Backpressure] Total event terbuang: 1500
```

---

### 3. Test Graceful Shutdown & State Recovery

This test verifies that the system can gracefully shut down and persist dashboard state to MinIO.

#### Shutdown Procedure

Go back to the terminal where the application is running and press:

```text
Ctrl + C
```

#### Expected Output

```text
[System] Stop signal received! Shutting down the application gracefully...
[Storage] ✅ Dashboard state saved to /state/dashboard.json
[System] Application Shutdown Successfully
```

---

#### Recovery Verification

Start the application again:

```bash
go run main.go
```

Refresh the browser:

```text
http://localhost:8081
```

#### Expected Output

The **Dropped Events** counter will **not reset to 0**. Instead, it will continue from the exact value recorded before shutdown, proving that the stateless-to-stateful recovery mechanism is functioning correctly.

---

## Success Criteria

The system is considered fully operational when:

- ✅ Real-time logs stream continuously to the dashboard.
- ✅ Ingestion metrics update dynamically.
- ✅ AI Forecast generates predictions after sufficient data collection.
- ✅ Policy hot-reload updates filtering behavior without restarting the application.
- ✅ Backpressure metrics accurately track dropped events.
- ✅ Graceful shutdown persists dashboard state.
- ✅ Application restart successfully restores the previous state from storage.