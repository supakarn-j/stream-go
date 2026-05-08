package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrAckUnavailable       = errors.New("message ack function is unavailable")
	ErrMessageFieldNotFound = errors.New("message field not found")
)

type Message struct {
	Stream    string
	ID        string
	Type      string
	Source    string
	Version   int64
	Timestamp time.Time
	Values    map[string]interface{}
	ackFunc   func(ctx context.Context, id string) error
}

func (m *Message) Ack(ctx context.Context) error {
	if m.ackFunc == nil {
		return ErrAckUnavailable
	}

	return m.ackFunc(ctx, m.ID)
}

func (m *Message) Decode(dest interface{}) error {
	values, err := m.decodedValues()
	if err != nil {
		return err
	}

	data, err := json.Marshal(values)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, dest)
}

func (m *Message) DecodeValue(key string, dest interface{}) error {
	value, ok := m.Values[key]
	if !ok {
		return fmt.Errorf("%w: %s", ErrMessageFieldNotFound, key)
	}

	decoded, err := decodeStreamValue(value)
	if err != nil {
		return fmt.Errorf("message field %q: %w", key, err)
	}

	data, err := json.Marshal(decoded)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, dest)
}

func (m *Message) decodedValues() (map[string]interface{}, error) {
	values := make(map[string]interface{}, len(m.Values))
	for key, value := range m.Values {
		decoded, err := decodeStreamValue(value)
		if err != nil {
			return nil, fmt.Errorf("message field %q: %w", key, err)
		}
		values[key] = decoded
	}

	return values, nil
}

func decodeStreamValue(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case string:
		return decodeJSONString(v)
	case []byte:
		return decodeJSONString(string(v))
	default:
		return v, nil
	}
}

func decodeJSONString(value string) (interface{}, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return value, nil
	}

	var decoded interface{}
	if err := json.Unmarshal([]byte(trimmed), &decoded); err == nil {
		return decoded, nil
	}

	return value, nil
}
