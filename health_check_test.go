package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// healthCheckHandler responds with 200 OK for health checks
func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func TestHealthCheckHandler(t *testing.T) {
	tests := []struct {
		name           string
		path           string
		expectedStatus int
		expectedBody   string
	}{
		{"Health endpoint", "/health", http.StatusOK, "OK"},
		{"Healthz endpoint", "/healthz", http.StatusOK, "OK"},
		{"Root endpoint", "/", http.StatusOK, "OK"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", tt.path, nil)
			if err != nil {
				t.Fatal(err)
			}

			rr := httptest.NewRecorder()
			handler := http.HandlerFunc(healthCheckHandler)
			handler.ServeHTTP(rr, req)

			if status := rr.Code; status != tt.expectedStatus {
				t.Errorf("handler returned wrong status code: got %v want %v",
					status, tt.expectedStatus)
			}

			if rr.Body.String() != tt.expectedBody {
				t.Errorf("handler returned unexpected body: got %v want %v",
					rr.Body.String(), tt.expectedBody)
			}
		})
	}
}
