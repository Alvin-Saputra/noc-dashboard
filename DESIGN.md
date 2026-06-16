# Project Watchtower - Architecture & Design Document

## 1. Data Structures: EventEnvelope

Semua data yang dihasilkan oleh empat mock source (**Dynatrace**, **Splunk**, **Riverbed**, dan **Prometheus**) akan dibungkus dalam struktur standar `EventEnvelope` sebelum dikirim ke channel ingestion. Hal ini memastikan pipeline pemrosesan yang seragam.

```go
type EventEnvelope struct {
    Version   string                 `json:"version"`   // Required version field
    ID        string                 `json:"id"`        // Unique identifier (UUID)
    Source    string                 `json:"source"`    // e.g., "dynatrace", "splunk", "riverbed", "prometheus"
    Timestamp int64                  `json:"timestamp"` // Unix timestamp
    Payload   map[string]interface{} `json:"payload"`   // Flexible payload varying by source
}
```

---

## 2. Object Storage Key Schema

Karena penggunaan database tidak diperbolehkan, seluruh state dan data akan disimpan ke **MinIO (S3-compatible)** menggunakan path berikut:

### Raw Events

```text
/events/raw/YYYY/MM/DD/HH/<uuid>.json
```

### Screened Events

```text
/events/screened/YYYY/MM/DD/HH/<uuid>.json
```

### Anomaly Outputs

```text
/ml/anomalies/YYYY/MM/DD/<uuid>.json
```

### Forecasts

```text
/ml/forecasts/YYYY/MM/DD/<metric>.json
```

> File forecast akan ditimpa (overwrite) setiap kali prediksi baru tersedia.

### Active Policy

```text
/policy/screening.json
```

### Dashboard Snapshot

```text
/state/dashboard.json
```

### Dedup TTL Buckets

```text
/dedup/window/<bucket>.json
```

---

## 3. Channel Buffer Strategy & Backpressure

Sistem menggunakan satu buffered channel terpusat:

```go
chan EventEnvelope
```

### Buffer Size

Kapasitas channel tidak di-hardcode, tetapi diinjeksi secara dinamis melalui:

```json
ingestion.channel_buffer_size
```

yang didefinisikan pada `config.json`.

### Backpressure Handling

Untuk mencegah aplikasi crash saat terjadi beban tinggi atau *burst storm*, mock producer menggunakan `select` dengan `default`:

```go
select {
case ingestionChan <- event:
    // sent
default:
    // drop event
}
```

Jika channel penuh, event akan masuk ke blok `default` dan dibuang (*dropped*).

### Monitoring

Setiap event yang dibuang harus:

* Dicatat ke log.
* Menambah nilai metrik **drop counter**.
* Diekspos ke dashboard untuk pemantauan.

---

## 4. Goroutine Ownership Map

Untuk mencegah *data race* dan memastikan batas tanggung jawab yang jelas, goroutine dipetakan sebagai berikut:

### Producers (4 Goroutines)

Satu goroutine independen untuk masing-masing source:

* Dynatrace
* Splunk
* Riverbed
* Prometheus

Tanggung jawab:

* Menghasilkan mock data.
* Menjadi satu-satunya penulis (*writer*) ke ingestion channel.

### Consumers / Worker Pool (N Goroutines)

Jumlah worker ditentukan oleh:

```json
screening.worker_count
```

pada `config.json`.

Tanggung jawab:

* Membaca event dari ingestion channel.
* Menjalankan pipeline screening:

  1. Deduplication
  2. Classification
  3. Noise Filtering

### Background Policy Poller (1 Goroutine)

Goroutine khusus yang secara periodik melakukan polling:

```text
/policy/screening.json
```

menggunakan **ETag** untuk melakukan *hot reload* aturan screening tanpa memblokir pipeline utama.

### Storage Archiver (1 Goroutine)

Mendengarkan secondary channel dari ingestor untuk melakukan penulisan raw event ke MinIO secara asinkron.

Tujuan:

* Menghindari latency storage menghambat proses ingestion.

---

## 5. Graceful Shutdown Sequence

Aplikasi harus dapat melakukan *clean shutdown* dan keluar dalam waktu maksimal **5 detik** setelah menerima sinyal interupsi.

### 1. Signal Capture

Dengarkan sinyal berikut menggunakan package `os/signal`:

* `SIGINT`
* `SIGTERM`

### 2. Context Cancellation

Panggil global `context.CancelFunc` untuk memberi tahu seluruh producer agar berhenti menghasilkan mock data.

### 3. Channel Closure

Setelah seluruh producer mengonfirmasi penghentian:

```go
close(ingestionChannel)
```

### 4. Drain & Process

Worker pool consumer akan secara otomatis menghabiskan seluruh event yang masih berada di buffer channel.

Gunakan:

```go
sync.WaitGroup
```

untuk menunggu seluruh worker selesai.

### 5. State Snapshot

Sebelum aplikasi berhenti, serialisasikan state terakhir dan simpan ke:

#### Dashboard State

```text
/state/dashboard.json
```

#### Dedup State

```text
/dedup/window/<bucket>.json
```

### 6. Exit

Terminasi proses secara bersih (*clean exit*).
