package messaging

import (
	"bytes"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestDeadLetterTopologyFor(t *testing.T) {
	namespace, err := NewNamespace("im:test")
	if err != nil {
		t.Fatalf("NewNamespace() error = %v", err)
	}

	direct := DeadLetterTopologyFor(namespace, DeadLetterDirect)
	wantDirect := DeadLetterTopology{
		Kind:             DeadLetterDirect,
		Exchange:         "im.test.messages.dead",
		Queue:            "im.test.messages.dead",
		RoutingKey:       "direct.dead",
		SourceExchange:   "im.test.messages",
		SourceRoutingKey: "direct.persist",
	}
	if direct != wantDirect {
		t.Fatalf("direct topology = %#v, want %#v", direct, wantDirect)
	}

	group := DeadLetterTopologyFor(namespace, DeadLetterGroup)
	wantGroup := DeadLetterTopology{
		Kind:             DeadLetterGroup,
		Exchange:         "im.test.group.messages.dead",
		Queue:            "im.test.group.messages.dead",
		RoutingKey:       "group.dead",
		SourceExchange:   "im.test.group.messages",
		SourceRoutingKey: "group.persist",
	}
	if group != wantGroup {
		t.Fatalf("group topology = %#v, want %#v", group, wantGroup)
	}

	if unknown := DeadLetterTopologyFor(namespace, DeadLetterKind("other")); unknown.Exchange != "" || unknown.Queue != "" {
		t.Fatalf("unknown topology = %#v, want empty names", unknown)
	}
}

func TestDeclareDeadLetterTopologyRejectsIncompleteTopology(t *testing.T) {
	if err := DeclareDeadLetterTopology(nil, DeadLetterTopology{Kind: DeadLetterDirect}); err == nil {
		t.Fatal("DeclareDeadLetterTopology() error = nil, want incomplete topology error")
	}
	if err := DeclareDeadLetterTopology(nil, DeadLetterTopologyFor(DefaultNamespace(), DeadLetterDirect)); err == nil {
		t.Fatal("DeclareDeadLetterTopology(nil channel) error = nil, want nil channel error")
	}
}

func TestDLQMessageHeaders(t *testing.T) {
	failedAt := time.Date(2026, 10, 29, 12, 0, 0, 0, time.FixedZone("CST", 8*3600))
	message := dlqMessage{
		Kind:          DeadLetterGroup,
		Body:          []byte(`{"server_message_id":7}`),
		MessageID:     "7",
		Reason:        dlqReasonRetriesExhausted,
		Error:         "persist group message: foreign key constraint fails",
		Attempts:      6,
		FirstFailedAt: failedAt,
	}
	headers := message.headers()
	if len(headers) != 5 {
		t.Fatalf("headers = %#v, want five entries", headers)
	}
	if got := headers[dlqHeaderReason]; got != dlqReasonRetriesExhausted {
		t.Fatalf("reason = %v", got)
	}
	if got := headers[dlqHeaderError]; got != message.Error {
		t.Fatalf("error = %v", got)
	}
	if got, ok := headers[dlqHeaderAttempts].(int); !ok || got != 6 {
		t.Fatalf("attempts = %#v, want int 6", headers[dlqHeaderAttempts])
	}
	if got := headers[dlqHeaderFirstFailedAt]; got != "2026-10-29T04:00:00Z" {
		t.Fatalf("first failed at = %v, want UTC RFC3339", got)
	}
	if got := headers[dlqHeaderSource]; got != "group" {
		t.Fatalf("source = %v, want group", got)
	}
	for key, value := range headers {
		if text, ok := value.(string); ok && strings.Contains(text, string(message.Body)) {
			t.Fatalf("header %s contains the message body", key)
		}
	}
	if !bytes.Equal(message.Body, []byte(`{"server_message_id":7}`)) {
		t.Fatalf("body = %q", message.Body)
	}
}

func TestDLQTruncateErrorKeepsValidUTF8(t *testing.T) {
	ascii := strings.Repeat("x", 600)
	if got := dlqTruncateError(ascii); len(got) != dlqErrorLimit {
		t.Fatalf("truncated length = %d, want %d", len(got), dlqErrorLimit)
	}

	multibyte := strings.Repeat("世", 300)
	got := dlqTruncateError(multibyte)
	if len(got) > dlqErrorLimit {
		t.Fatalf("truncated length = %d, want at most %d", len(got), dlqErrorLimit)
	}
	if !utf8.ValidString(got) {
		t.Fatal("truncated error is not valid UTF-8")
	}
	if !strings.HasPrefix(multibyte, got) {
		t.Fatal("truncated error is not a prefix of the original")
	}

	short := "short error"
	if got := dlqTruncateError(short); got != short {
		t.Fatalf("dlqTruncateError(short) = %q", got)
	}
}
