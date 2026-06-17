package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"watchtower/screening"

	"github.com/minio/minio-go/v7"
)

type APIServer struct {
	PolicyManager *screening.PolicyManager
	MinioClient   *minio.Client
	BucketName    string
}

func (s *APIServer) Start(port int) error {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/state", s.handleState)
	mux.HandleFunc("/api/policy", s.handlePolicy)

	fmt.Printf("[Server] 🚀 Web Server API berjalan di port %d...\n", port)
	return http.ListenAndServe(fmt.Sprintf(":%d", port), mux)
}

func (s *APIServer) handleState(w http.ResponseWriter, r *http.Request) {
	// Pastikan hanya menerima metode GET
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	dummyState := map[string]interface{}{
		"status":         "Aplikasi berjalan dengan baik",
		"active_workers": 4,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(dummyState) 
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
		
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"message": "Fitur PUT Policy segera hadir!"}`))

	default:
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	}
}
