package main

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/Johnny-Cheung/IM/backend/internal/config"
	"github.com/Johnny-Cheung/IM/backend/internal/messaging"
	"github.com/rabbitmq/amqp091-go"
)

func dlqTestOptions(mutate func(*serverOptions)) serverOptions {
	options := serverOptions{dlqKind: "all", dlqLimit: dlqDefaultLimit}
	if mutate != nil {
		mutate(&options)
	}
	return options
}

func TestDLQParseOptionsRequiresExactlyOneMode(t *testing.T) {
	if _, err := dlqParseOptions(dlqTestOptions(nil)); err == nil {
		t.Fatal("no mode: error = nil")
	}
	_, err := dlqParseOptions(dlqTestOptions(func(options *serverOptions) {
		options.dlqList = true
		options.dlqReplay = true
	}))
	if err == nil {
		t.Fatal("two modes: error = nil")
	}

	if _, err := dlqParseOptions(dlqTestOptions(func(options *serverOptions) {
		options.dlqList = true
		options.backfillSeq = true
	})); err == nil {
		t.Fatal("dlq plus -backfill-seq: error = nil")
	}

	command, err := dlqParseOptions(dlqTestOptions(func(options *serverOptions) {
		options.dlqList = true
	}))
	if err != nil || command.mode != dlqModeList {
		t.Fatalf("list: command=%#v err=%v", command, err)
	}
	command, err = dlqParseOptions(dlqTestOptions(func(options *serverOptions) {
		options.dlqReplay = true
		options.dlqID = 42
	}))
	if err != nil || command.mode != dlqModeReplay || command.messageID != 42 {
		t.Fatalf("replay: command=%#v err=%v", command, err)
	}
	command, err = dlqParseOptions(dlqTestOptions(func(options *serverOptions) {
		options.dlqDiscard = true
		options.dlqYes = true
	}))
	if err != nil || command.mode != dlqModeDiscard {
		t.Fatalf("discard: command=%#v err=%v", command, err)
	}
}

func TestDLQParseOptionsConfirmationRules(t *testing.T) {
	discard := func(options *serverOptions) {
		options.dlqDiscard = true
	}
	if _, err := dlqParseOptions(dlqTestOptions(discard)); err == nil {
		t.Fatal("discard without -dlq-yes: error = nil")
	}
	discardYes := func(options *serverOptions) {
		options.dlqDiscard = true
		options.dlqYes = true
	}
	if _, err := dlqParseOptions(dlqTestOptions(discardYes)); err != nil {
		t.Fatalf("discard with -dlq-yes: error = %v", err)
	}

	replayAll := func(options *serverOptions) {
		options.dlqReplay = true
	}
	if _, err := dlqParseOptions(dlqTestOptions(replayAll)); err == nil {
		t.Fatal("replay of all without -dlq-yes: error = nil")
	}
	if _, err := dlqParseOptions(dlqTestOptions(func(options *serverOptions) {
		options.dlqReplay = true
		options.dlqYes = true
	})); err != nil {
		t.Fatalf("replay of all with -dlq-yes: error = %v", err)
	}
	if _, err := dlqParseOptions(dlqTestOptions(func(options *serverOptions) {
		options.dlqReplay = true
		options.dlqID = 7
	})); err != nil {
		t.Fatalf("single replay without -dlq-yes: error = %v", err)
	}
	if _, err := dlqParseOptions(dlqTestOptions(func(options *serverOptions) {
		options.dlqList = true
	})); err != nil {
		t.Fatalf("list without -dlq-yes: error = %v", err)
	}
}

func TestDLQParseOptionsKinds(t *testing.T) {
	cases := []struct {
		kind string
		want []messaging.DeadLetterKind
		ok   bool
	}{
		{"", []messaging.DeadLetterKind{messaging.DeadLetterDirect, messaging.DeadLetterGroup}, true},
		{"all", []messaging.DeadLetterKind{messaging.DeadLetterDirect, messaging.DeadLetterGroup}, true},
		{"direct", []messaging.DeadLetterKind{messaging.DeadLetterDirect}, true},
		{" GROUP ", []messaging.DeadLetterKind{messaging.DeadLetterGroup}, true},
		{"both", nil, false},
	}
	for _, testCase := range cases {
		command, err := dlqParseOptions(dlqTestOptions(func(options *serverOptions) {
			options.dlqList = true
			options.dlqKind = testCase.kind
		}))
		if testCase.ok {
			if err != nil {
				t.Fatalf("kind %q: error = %v", testCase.kind, err)
			}
			if fmt.Sprint(command.kinds) != fmt.Sprint(testCase.want) {
				t.Fatalf("kind %q: kinds = %v, want %v", testCase.kind, command.kinds, testCase.want)
			}
			continue
		}
		if err == nil {
			t.Fatalf("kind %q: error = nil", testCase.kind)
		}
	}
}

func TestDLQParseOptionsLimitBounds(t *testing.T) {
	for _, limit := range []int{1, 100, dlqMaxLimit} {
		if _, err := dlqParseOptions(dlqTestOptions(func(options *serverOptions) {
			options.dlqList = true
			options.dlqLimit = limit
		})); err != nil {
			t.Fatalf("limit %d: error = %v", limit, err)
		}
	}
	for _, limit := range []int{0, -1, dlqMaxLimit + 1} {
		if _, err := dlqParseOptions(dlqTestOptions(func(options *serverOptions) {
			options.dlqList = true
			options.dlqLimit = limit
		})); err == nil {
			t.Fatalf("limit %d: error = nil", limit)
		}
	}
}

func TestDLQShortErrorCollapsesWhitespaceAndTruncates(t *testing.T) {
	if got := dlqShortError(""); got != "-" {
		t.Fatalf("empty = %q, want -", got)
	}
	if got := dlqShortError("first\nsecond   third"); got != "first second third" {
		t.Fatalf("whitespace = %q", got)
	}
	long := strings.Repeat("中", 200)
	got := dlqShortError(long)
	if !utf8.ValidString(got) {
		t.Fatalf("truncated error is not valid UTF-8: %q", got)
	}
	if !strings.HasSuffix(got, "...") || len(got) > dlqShortErrorSize+3 {
		t.Fatalf("truncated error = %q (len %d)", got, len(got))
	}
}

func TestDLQEntryLineUsesStableFields(t *testing.T) {
	line := dlqEntryLine(messaging.DeadLetterEntry{
		ID:            12,
		Kind:          messaging.DeadLetterDirect,
		Seq:           3,
		Reason:        "retries_exhausted",
		Attempts:      6,
		FirstFailedAt: time.Date(2026, 10, 29, 10, 0, 0, 0, time.UTC),
		Error:         "insert failed",
	})
	for _, fragment := range []string{
		"id=12", "kind=direct", "seq=3", "reason=retries_exhausted",
		"attempts=6", "first_failed=2026-10-29T10:00:00Z", "error=insert failed",
	} {
		if !strings.Contains(line, fragment) {
			t.Fatalf("line %q is missing %q", line, fragment)
		}
	}
	if !strings.Contains(dlqEntryLine(messaging.DeadLetterEntry{}), "first_failed=-") {
		t.Fatal("zero time is not rendered as -")
	}
}

func TestDLQSanitizeErrorRemovesCredentials(t *testing.T) {
	rabbitConfig := config.RabbitMQConfig{
		URL:      "amqp://im_v11@127.0.0.1:25676/im_v11",
		Password: "s3cret",
	}
	err := dlqSanitizeError(
		fmt.Errorf("dial amqp://im_v11@127.0.0.1:25676/im_v11 with s3cret: refused"),
		rabbitConfig,
	)
	message := err.Error()
	if strings.Contains(message, "s3cret") || strings.Contains(message, "amqp://") {
		t.Fatalf("error leaked credentials: %s", message)
	}
	if !strings.Contains(message, "refused") {
		t.Fatalf("error lost the cause: %s", message)
	}
}

func TestDLQQueueMissing(t *testing.T) {
	if !dlqQueueMissing(&amqp091.Error{Code: amqp091.NotFound}) {
		t.Fatal("amqp NotFound not recognized")
	}
	if !dlqQueueMissing(fmt.Errorf("inspect queue: %w", &amqp091.Error{Code: amqp091.NotFound})) {
		t.Fatal("wrapped amqp NotFound not recognized")
	}
	if dlqQueueMissing(errors.New("connection refused")) {
		t.Fatal("unrelated error recognized as missing queue")
	}
}
