package protocol

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestEnvelope_MarshalUnmarshal(t *testing.T) {
	env := NewEnvelope("client-a", "parent", TypeStatusReport, MustEncode(StatusPayload{
		State:      "working",
		Message:    "extracting auth module",
		ReportedAt: time.Date(2026, 6, 9, 10, 42, 31, 0, time.UTC),
	}))
	env.TaskID = "auth-001"

	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got Envelope
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.ID != env.ID || got.From != env.From || got.To != env.To ||
		got.Type != env.Type || got.TaskID != env.TaskID || got.Version != Version {
		t.Errorf("round trip mismatch:\n got %+v\nwant %+v", got, *env)
	}

	var status StatusPayload
	if err := got.DecodePayload(&status); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if status.State != "working" || status.Message != "extracting auth module" {
		t.Errorf("payload mismatch: %+v", status)
	}
}

func TestEnvelope_CorrelationIDOmittedWhenEmpty(t *testing.T) {
	data, err := json.Marshal(NewEnvelope("a", "b", TypePing, ""))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(data), "correlation_id") {
		t.Errorf("empty correlation_id must be omitted from JSON: %s", data)
	}
}

func TestEnvelope_Validate(t *testing.T) {
	tests := []struct {
		name    string
		env     *Envelope
		wantErr bool
	}{
		{"valid", NewEnvelope("a", "b", TypePing, ""), false},
		{"empty from", &Envelope{To: "b", Type: TypePing}, true},
		{"empty type", &Envelope{From: "a", To: "b"}, true},
		{"nil", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.env.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && !errors.Is(err, ErrMalformedEnvelope) {
				t.Errorf("error must wrap ErrMalformedEnvelope, got %v", err)
			}
		})
	}
}

func TestFraming_WriteRead(t *testing.T) {
	var buf bytes.Buffer
	want := NewEnvelope("sender", "receiver", TypeDirectMessage, "hello there")
	if err := WriteMessage(&buf, want, 0); err != nil {
		t.Fatalf("write: %v", err)
	}

	// First 4 bytes are the big-endian payload length.
	header := buf.Bytes()[:4]
	if int(binary.BigEndian.Uint32(header)) != buf.Len()-4 {
		t.Errorf("length prefix %d != body length %d", binary.BigEndian.Uint32(header), buf.Len()-4)
	}

	got, err := ReadMessage(&buf, 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got.ID != want.ID || got.Payload != want.Payload || got.Type != want.Type {
		t.Errorf("round trip mismatch: got %+v want %+v", got, want)
	}
}

func TestFraming_RoundTrip_LargeMessage(t *testing.T) {
	var buf bytes.Buffer
	payload := strings.Repeat("x", 512*1024) // 512KB, under the 1MB default
	want := NewEnvelope("a", "b", TypeBroadcast, payload)
	if err := WriteMessage(&buf, want, 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := ReadMessage(&buf, 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got.Payload != payload {
		t.Error("large payload corrupted in round trip")
	}
}

func TestFraming_WriteOverLimit(t *testing.T) {
	var buf bytes.Buffer
	env := NewEnvelope("a", "b", TypeBroadcast, strings.Repeat("x", 2048))
	err := WriteMessage(&buf, env, 1024)
	if !errors.Is(err, ErrMessageTooLarge) {
		t.Errorf("want ErrMessageTooLarge, got %v", err)
	}
	if buf.Len() != 0 {
		t.Error("nothing must be written when the message is too large")
	}
}

func TestFraming_ReadOverLimit(t *testing.T) {
	var buf bytes.Buffer
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], 10*1024*1024)
	buf.Write(header[:])
	_, err := ReadMessage(&buf, 1024)
	if !errors.Is(err, ErrMessageTooLarge) {
		t.Errorf("want ErrMessageTooLarge, got %v", err)
	}
}

func TestFraming_TruncatedFrame(t *testing.T) {
	var buf bytes.Buffer
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], 100)
	buf.Write(header[:])
	buf.WriteString("short") // 5 bytes instead of the promised 100
	_, err := ReadMessage(&buf, 0)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("want io.ErrUnexpectedEOF, got %v", err)
	}
}

func TestFraming_MalformedJSON(t *testing.T) {
	var buf bytes.Buffer
	body := []byte{0xde, 0xad, 0xbe, 0xef}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(body)))
	buf.Write(header[:])
	buf.Write(body)
	if _, err := ReadMessage(&buf, 0); err == nil {
		t.Error("binary garbage must fail to decode")
	}
}

func TestFraming_UnicodePayload(t *testing.T) {
	var buf bytes.Buffer
	payload := "auth 模块完成 ✅ 絵文字テスト 🚀🎉"
	want := NewEnvelope("a", "b", TypeDirectMessage, payload)
	if err := WriteMessage(&buf, want, 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := ReadMessage(&buf, 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got.Payload != payload {
		t.Errorf("unicode payload corrupted: got %q want %q", got.Payload, payload)
	}
}

func TestIsValidState(t *testing.T) {
	for _, s := range ValidStates {
		if !IsValidState(s) {
			t.Errorf("%q must be valid", s)
		}
	}
	if IsValidState("napping") {
		t.Error("unknown state must be invalid")
	}
}
