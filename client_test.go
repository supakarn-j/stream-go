package stream

import (
	"testing"
)

func TestClientConnect(t *testing.T) {
	tests := []struct {
		name           string
		addr, password string
		wantPanic      bool
	}{
		{"valid connection", "localhost:6379", "", false},
		{"invalid connection", "localhost:1234", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantPanic {
				defer func() {
					if r := recover(); r == nil {
						t.Errorf("Expected panic but did not occur")
					}
				}()
			}
			got := NewRedisClient(
				RedisConfig{
					Addr:     tt.addr,
					Password: tt.password,
				})
			if !tt.wantPanic && got == nil {
				t.Errorf("Expected valid client but got nil")
			}
		})
	}
}
