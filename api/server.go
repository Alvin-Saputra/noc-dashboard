package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"sync/atomic"
	"watchtower/screening"

	"github.com/minio/minio-go/v7"
)

type APIServer struct {
	PolicyManager *screening.PolicyManager
	MinioClient   *minio.Client
	BucketName    string
	SSEBroker     *Broker
	DropCounter   *atomic.Uint64 
	CountProm     *atomic.Uint64 // <--- TAMBAHKAN INI
	CountDyna     *atomic.Uint64
	CountSplunk   *atomic.Uint64
	CountRiver    *atomic.Uint64
	WorkerCount   int
}

func (s *APIServer) Start(port int) error {
	mime.AddExtensionType(".css", "text/css")
	// -------------------------------------------------------------

	mux := http.NewServeMux()

	fs := http.FileServer(http.Dir("web"))
	mux.Handle("/", fs)

	mux.HandleFunc("/api/state", s.handleState)
	mux.HandleFunc("/api/policy", s.handlePolicy)
	mux.HandleFunc("/stream", s.handleStream)

	fmt.Printf("[Server] 🚀 Web Server API at port %d...\n", port)
	return http.ListenAndServe(fmt.Sprintf(":%d", port), mux)
}

func (s *APIServer) handleState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	actualState := map[string]interface{}{
		"status":         "Live",
		"active_workers": s.WorkerCount,
		"total_dropped":  s.DropCounter.Load(),
		"ingestion_totals": map[string]interface{}{
			"prometheus":    s.CountProm.Load(),
			"dynatrace":     s.CountDyna.Load(),
			"splunktrace":   s.CountSplunk.Load(),
			"riverbedtrace": s.CountRiver.Load(),
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(actualState)
}

func (s *APIServer) handlePolicy(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodGet:
		s.PolicyManager.RLock()
		currentPolicy := s.PolicyManager.GetPolicy()
		s.PolicyManager.RUnlock()

		json.NewEncoder(w).Encode(currentPolicy)

	case http.MethodPut:
		var newPolicy screening.Policy
		if err := json.NewDecoder(r.Body).Decode(&newPolicy); err != nil {
			http.Error(w, `{"error": "Format JSON tidak valid"}`, http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		policyBytes, err := json.MarshalIndent(newPolicy, "", "  ")
		if err != nil {
			http.Error(w, `{"error": "Gagal memproses policy"}`, http.StatusInternalServerError)
			return
		}

		ctx := r.Context()
		objectName := "policy/screening.json"
		reader := bytes.NewReader(policyBytes)

		_, err = s.MinioClient.PutObject(ctx, s.BucketName, objectName, reader, int64(len(policyBytes)), minio.PutObjectOptions{
			ContentType: "application/json",
		})

		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error": "Gagal menyimpan ke MinIO: %v"}`, err), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"message": "Buku Panduan berhasil diperbarui ke MinIO!"}`))

	default:
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	}
}

func (s *APIServer) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming tidak didukung!", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	messageChan := make(chan []byte)
	s.SSEBroker.newClients <- messageChan

	defer func() {
		s.SSEBroker.closingClients <- messageChan
	}()

	fmt.Fprintf(w, "event: connection\ndata: {\"message\": \"Terhubung ke Watchtower\"}\n\n")
	flusher.Flush()

	for {
		select {
		case msg := <-messageChan:
			fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()

		case <-r.Context().Done():
			return
		}
	}
}
