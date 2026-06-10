package protocol

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

// DefaultMaxMessageBytes is the framing size limit used when the caller does
// not supply one (config.limits.max_message_bytes overrides it).
const DefaultMaxMessageBytes = 1 << 20 // 1MB

// All messages are length-prefixed JSON: a 4-byte big-endian unsigned length
// followed by the UTF-8 JSON payload.

// WriteMessage writes a length-prefixed JSON message to a writer.
func WriteMessage(w io.Writer, env *Envelope, maxBytes int) error {
	data, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxMessageBytes
	}
	if len(data) > maxBytes {
		return ErrMessageTooLarge
	}
	// Write length and body as a single buffer so concurrent writers guarded
	// by a mutex never interleave a header with another message's body.
	buf := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(data)))
	copy(buf[4:], data)
	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("write frame: %w", err)
	}
	return nil
}

// ReadMessage reads a length-prefixed JSON message from a reader.
func ReadMessage(r io.Reader, maxBytes int) (*Envelope, error) {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxMessageBytes
	}
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		// Propagate EOF unwrapped so callers can distinguish a clean
		// disconnect from a mid-frame truncation (io.ErrUnexpectedEOF).
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("read length: %w", err)
	}
	length := binary.BigEndian.Uint32(header[:])
	if length > uint32(maxBytes) {
		// Drain the oversized body so the stream stays framed and the caller
		// can answer MESSAGE_TOO_LARGE and keep the connection. A length
		// beyond any plausible frame is treated as a corrupt stream instead.
		const maxDiscard = 64 << 20
		if length <= maxDiscard {
			if _, err := io.CopyN(io.Discard, r, int64(length)); err != nil {
				return nil, fmt.Errorf("%w (discard failed: %v)", ErrMessageTooLarge, err)
			}
		}
		return nil, ErrMessageTooLarge
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	var env Envelope
	if err := json.Unmarshal(buf, &env); err != nil {
		// Wrap with ErrMalformedEnvelope: the frame itself was fully consumed,
		// so the stream is still in sync and the caller may keep reading after
		// responding with INVALID_MESSAGE.
		return nil, fmt.Errorf("%w: unmarshal: %v", ErrMalformedEnvelope, err)
	}
	return &env, nil
}
