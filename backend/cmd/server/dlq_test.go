package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
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

// dlqTestChannel is a broker-free messaging.DeadLetterChannel. Deliveries are
// generated on demand and carry unique delivery tags.
type dlqTestChannel struct {
	queued     int
	total      int
	next       int
	idFor      func(index int) string
	published  []amqp091.Publishing
	acked      []uint64
	nacked     []uint64
	publishErr error
}

func (f *dlqTestChannel) Get(queue string, autoAck bool) (amqp091.Delivery, bool, error) {
	if f.next >= f.total {
		return amqp091.Delivery{}, false, nil
	}
	index := f.next
	f.next++
	messageID := ""
	if f.idFor != nil {
		messageID = f.idFor(index)
	}
	return amqp091.Delivery{
		DeliveryTag: uint64(index + 1),
		MessageId:   messageID,
		Body:        []byte(`{"server_message_id":1}`),
	}, true, nil
}

func (f *dlqTestChannel) PublishConfirmed(
	ctx context.Context,
	exchange, routingKey string,
	message amqp091.Publishing,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.publishErr != nil {
		return f.publishErr
	}
	f.published = append(f.published, message)
	return nil
}

func (f *dlqTestChannel) Ack(deliveryTag uint64) error {
	f.acked = append(f.acked, deliveryTag)
	return nil
}

func (f *dlqTestChannel) Nack(deliveryTag uint64, multiple, requeue bool) error {
	f.nacked = append(f.nacked, deliveryTag)
	return nil
}

func (f *dlqTestChannel) QueueDeclarePassive(name string) (amqp091.Queue, error) {
	return amqp091.Queue{Name: name, Messages: f.queued}, nil
}

func dlqTestTopology() messaging.DeadLetterTopology {
	return messaging.DeadLetterTopology{
		Kind:             messaging.DeadLetterDirect,
		Exchange:         "im.messages.dead",
		Queue:            "im.messages.dead",
		RoutingKey:       "direct.dead",
		SourceExchange:   "im.messages",
		SourceRoutingKey: "direct.persist",
	}
}

func TestDLQScanLimitWidensOnlyForSpecificID(t *testing.T) {
	cases := []struct {
		name    string
		command dlqCommand
		queued  int
		want    int
	}{
		{"all messages keeps limit", dlqCommand{limit: 100}, 500, 100},
		{"specific id widens to depth", dlqCommand{messageID: 7, limit: 100}, 500, 500},
		{"specific id beyond max is capped", dlqCommand{messageID: 7, limit: 100}, 20000, dlqMaxLimit},
		{"specific id within limit unchanged", dlqCommand{messageID: 7, limit: 500}, 500, 500},
	}
	for _, testCase := range cases {
		if got := dlqScanLimit(testCase.command, testCase.queued); got != testCase.want {
			t.Fatalf("%s: limit = %d, want %d", testCase.name, got, testCase.want)
		}
	}
}

func TestDLQRunKindReplayFindsIDBeyondDefaultLimit(t *testing.T) {
	fake := &dlqTestChannel{
		queued: 500,
		total:  500,
		idFor: func(index int) string {
			if index == 299 {
				return "77"
			}
			return strconv.Itoa(1000 + index)
		},
	}
	var out strings.Builder
	command := dlqCommand{mode: dlqModeReplay, messageID: 77, limit: 100}
	var totals dlqTotals
	matched, err := dlqRunKindChannel(
		context.Background(), fake, command, dlqTestTopology(), &out, &totals,
	)
	if err != nil {
		t.Fatalf("dlqRunKindChannel() error = %v\n%s", err, out.String())
	}
	if matched != 1 {
		t.Fatalf("matched = %d, want 1", matched)
	}
	if len(fake.published) != 1 || fake.published[0].MessageId != "77" {
		t.Fatalf("published = %#v, want only id 77", fake.published)
	}
	if len(fake.acked) != 1 || fake.acked[0] != 300 {
		t.Fatalf("acked = %v, want tag 300", fake.acked)
	}
	if totals.replayed != 1 || totals.scanned != 500 {
		t.Fatalf("totals = %#v", totals)
	}
	if !strings.Contains(out.String(), "replayed=1") {
		t.Fatalf("output = %q, want replayed=1", out.String())
	}
}

func TestDLQRunKindReplayFailsWhenIDNotInWindow(t *testing.T) {
	fake := &dlqTestChannel{
		queued: 20000,
		total:  dlqMaxLimit,
		idFor:  func(index int) string { return strconv.Itoa(1000 + index) },
	}
	var out strings.Builder
	command := dlqCommand{mode: dlqModeReplay, messageID: 999999, limit: 100}
	var totals dlqTotals
	matched, err := dlqRunKindChannel(context.Background(), fake, command, dlqTestTopology(), &out, &totals)
	if err == nil {
		t.Fatal("dlqRunKindChannel() error = nil, want not-found error")
	}
	var miss *dlqIDMiss
	if !errors.As(err, &miss) {
		t.Fatalf("error = %v, want *dlqIDMiss", err)
	}
	if matched != 0 {
		t.Fatalf("matched = %d, want 0", matched)
	}
	if !miss.capped() {
		t.Fatal("miss = not capped, want the absolute window cap")
	}
	if !strings.Contains(err.Error(), "not found in the first 10000 of 20000") {
		t.Fatalf("error = %v, want window detail", err)
	}
	if !strings.Contains(err.Error(), "drain the head first") {
		t.Fatalf("error = %v, want the drain-head remedy", err)
	}
	if strings.Contains(err.Error(), "raise") {
		t.Fatalf("error = %v, must not suggest raising a limit that is already capped", err)
	}
	if strings.Contains(out.String(), "replayed=") {
		t.Fatalf("output = %q, must not report a successful replay", out.String())
	}
	if len(fake.published) != 0 || len(fake.acked) != 0 {
		t.Fatalf("published=%d acked=%v, want nothing", len(fake.published), fake.acked)
	}
	if len(fake.nacked) != dlqMaxLimit {
		t.Fatalf("nacked = %d, want every scanned message requeued", len(fake.nacked))
	}
}

func TestDLQRunKindDiscardFailsWhenIDNotInWindow(t *testing.T) {
	fake := &dlqTestChannel{
		queued: 20000,
		total:  dlqMaxLimit,
		idFor:  func(index int) string { return strconv.Itoa(1000 + index) },
	}
	var out strings.Builder
	command := dlqCommand{mode: dlqModeDiscard, messageID: 999999, limit: 100}
	var totals dlqTotals
	if _, err := dlqRunKindChannel(
		context.Background(), fake, command, dlqTestTopology(), &out, &totals,
	); err == nil {
		t.Fatal("dlqRunKindChannel() error = nil, want not-found error")
	}
	if len(fake.acked) != 0 {
		t.Fatalf("acked = %v, want nothing", fake.acked)
	}
}

func TestDLQCheckIDFoundFullScanIsDefinitive(t *testing.T) {
	command := dlqCommand{mode: dlqModeReplay, messageID: 42, limit: 100}
	err := dlqCheckIDFound(command, dlqTestTopology(), 5, 100, 0)
	var miss *dlqIDMiss
	if !errors.As(err, &miss) {
		t.Fatalf("error = %v, want *dlqIDMiss", err)
	}
	if miss.capped() {
		t.Fatal("a fully scanned queue was reported as capped")
	}
	message := err.Error()
	if !strings.Contains(message, "not found in 5 queued direct messages") {
		t.Fatalf("error = %q, want the definitive not-found wording", message)
	}
	if strings.Contains(message, "first 5 of 5") || strings.Contains(message, "capped") {
		t.Fatalf("error = %q, a full scan must not be described as a window", message)
	}
	if err := dlqCheckIDFound(command, dlqTestTopology(), 5, 100, 1); err != nil {
		t.Fatalf("matched id: error = %v", err)
	}
	if err := dlqCheckIDFound(
		dlqCommand{mode: dlqModeReplay, limit: 100}, dlqTestTopology(), 5, 100, 0,
	); err != nil {
		t.Fatalf("replay of every message: error = %v, want nil", err)
	}
}

func TestDLQRunKindsTriesBothChainsBeforeNotFound(t *testing.T) {
	namespace, err := messaging.NewNamespace("im:test")
	if err != nil {
		t.Fatalf("namespace: %v", err)
	}
	command := dlqCommand{
		mode:      dlqModeReplay,
		kinds:     []messaging.DeadLetterKind{messaging.DeadLetterDirect, messaging.DeadLetterGroup},
		messageID: 42,
		limit:     100,
	}
	direct := &dlqTestChannel{
		queued: 20000,
		total:  dlqMaxLimit,
		idFor:  func(index int) string { return strconv.Itoa(1000 + index) },
	}
	group := &dlqTestChannel{
		queued: 20000,
		total:  6000,
		idFor: func(index int) string {
			if index == 5999 {
				return "42"
			}
			return strconv.Itoa(1000 + index)
		},
	}
	channels := map[messaging.DeadLetterKind]*dlqTestChannel{
		messaging.DeadLetterDirect: direct,
		messaging.DeadLetterGroup:  group,
	}
	var out, errOut strings.Builder
	attempts := make([]messaging.DeadLetterKind, 0, 2)
	run := func(
		ctx context.Context,
		topology messaging.DeadLetterTopology,
		out io.Writer,
		totals *dlqTotals,
	) (int, error) {
		attempts = append(attempts, topology.Kind)
		return dlqRunKindChannel(ctx, channels[topology.Kind], command, topology, out, totals)
	}

	if err := dlqRunKinds(context.Background(), command, namespace, &out, &errOut, run); err != nil {
		t.Fatalf("dlqRunKinds() error = %v\nout=%s\nerr=%s", err, out.String(), errOut.String())
	}
	if len(attempts) != 2 ||
		attempts[0] != messaging.DeadLetterDirect ||
		attempts[1] != messaging.DeadLetterGroup {
		t.Fatalf("attempts = %v, want both chains in order", attempts)
	}
	if len(group.published) != 1 || group.published[0].MessageId != "42" {
		t.Fatalf("group published = %#v, want only id 42", group.published)
	}
	if len(direct.published) != 0 {
		t.Fatalf("direct published = %d, want none", len(direct.published))
	}
	if !strings.Contains(out.String(), "total scanned=") ||
		!strings.Contains(out.String(), "replayed=1") {
		t.Fatalf("output = %q, want the combined totals with the replay", out.String())
	}
	if !strings.Contains(errOut.String(), "kind=direct") ||
		!strings.Contains(errOut.String(), "first 10000 of 20000") {
		t.Fatalf("stderr = %q, want the direct window miss reported", errOut.String())
	}
}

func TestDLQRunKindsReportsNotFoundOnlyAfterEveryChain(t *testing.T) {
	namespace, err := messaging.NewNamespace("im:test")
	if err != nil {
		t.Fatalf("namespace: %v", err)
	}
	command := dlqCommand{
		mode:      dlqModeReplay,
		kinds:     []messaging.DeadLetterKind{messaging.DeadLetterDirect, messaging.DeadLetterGroup},
		messageID: 42,
		limit:     100,
	}
	newFake := func() *dlqTestChannel {
		return &dlqTestChannel{
			queued: 20000,
			total:  dlqMaxLimit,
			idFor:  func(index int) string { return strconv.Itoa(1000 + index) },
		}
	}
	channels := map[messaging.DeadLetterKind]*dlqTestChannel{
		messaging.DeadLetterDirect: newFake(),
		messaging.DeadLetterGroup:  newFake(),
	}
	var out, errOut strings.Builder
	attempts := make([]messaging.DeadLetterKind, 0, 2)
	run := func(
		ctx context.Context,
		topology messaging.DeadLetterTopology,
		out io.Writer,
		totals *dlqTotals,
	) (int, error) {
		attempts = append(attempts, topology.Kind)
		return dlqRunKindChannel(ctx, channels[topology.Kind], command, topology, out, totals)
	}

	err = dlqRunKinds(context.Background(), command, namespace, &out, &errOut, run)
	if err == nil {
		t.Fatal("dlqRunKinds() error = nil, want a not-found error")
	}
	var notFound *dlqIDNotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("error = %v, want *dlqIDNotFoundError", err)
	}
	if len(attempts) != 2 {
		t.Fatalf("attempts = %v, want both chains", attempts)
	}
	message := err.Error()
	for _, fragment := range []string{"direct", "group", "first 10000 of 20000"} {
		if !strings.Contains(message, fragment) {
			t.Fatalf("error = %q, missing %q", message, fragment)
		}
	}
	if strings.Contains(out.String(), "kind=direct scanned=") ||
		strings.Contains(out.String(), "kind=group scanned=") {
		t.Fatalf("output = %q, must not report a successful replay", out.String())
	}
}

func TestDLQRunKindsStopsOnBrokerError(t *testing.T) {
	namespace, err := messaging.NewNamespace("im:test")
	if err != nil {
		t.Fatalf("namespace: %v", err)
	}
	command := dlqCommand{
		mode:      dlqModeReplay,
		kinds:     []messaging.DeadLetterKind{messaging.DeadLetterDirect, messaging.DeadLetterGroup},
		messageID: 42,
		limit:     100,
	}
	attempts := 0
	run := func(
		context.Context,
		messaging.DeadLetterTopology,
		io.Writer,
		*dlqTotals,
	) (int, error) {
		attempts++
		return 0, errors.New("broker broke")
	}
	if err := dlqRunKinds(
		context.Background(), command, namespace, io.Discard, io.Discard, run,
	); err == nil || !strings.Contains(err.Error(), "broker broke") {
		t.Fatalf("error = %v, want the broker failure", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want the run to stop at the first broker error", attempts)
	}
}

func TestDLQRunKindsPrintsTotalsBeforeBrokerError(t *testing.T) {
	namespace, err := messaging.NewNamespace("im:test")
	if err != nil {
		t.Fatalf("namespace: %v", err)
	}
	command := dlqCommand{
		mode:      dlqModeReplay,
		kinds:     []messaging.DeadLetterKind{messaging.DeadLetterDirect, messaging.DeadLetterGroup},
		messageID: 42,
		limit:     100,
	}
	var out strings.Builder
	attempts := make([]messaging.DeadLetterKind, 0, 2)
	run := func(
		_ context.Context,
		topology messaging.DeadLetterTopology,
		_ io.Writer,
		totals *dlqTotals,
	) (int, error) {
		attempts = append(attempts, topology.Kind)
		if topology.Kind == messaging.DeadLetterDirect {
			totals.scanned++
			totals.replayed++
			return 1, nil
		}
		return 0, errors.New("broker broke")
	}

	err = dlqRunKinds(context.Background(), command, namespace, &out, io.Discard, run)
	if err == nil || !strings.Contains(err.Error(), "broker broke") {
		t.Fatalf("error = %v, want the broker failure", err)
	}
	if len(attempts) != 2 {
		t.Fatalf("attempts = %v, want both chains", attempts)
	}
	if !strings.Contains(out.String(), "replayed=1") ||
		!strings.Contains(out.String(), "scanned=1") {
		t.Fatalf("output = %q, want the successful replay reported before the error", out.String())
	}
}

func TestDLQRunKindsSkipsMissingQueueInAllMode(t *testing.T) {
	namespace, err := messaging.NewNamespace("im:test")
	if err != nil {
		t.Fatalf("namespace: %v", err)
	}
	command := dlqCommand{
		mode:      dlqModeReplay,
		kinds:     []messaging.DeadLetterKind{messaging.DeadLetterDirect, messaging.DeadLetterGroup},
		messageID: 42,
		limit:     100,
	}
	group := &dlqTestChannel{
		queued: 5,
		total:  1,
		idFor:  func(int) string { return "42" },
	}
	attempts := make([]messaging.DeadLetterKind, 0, 2)
	run := func(
		ctx context.Context,
		topology messaging.DeadLetterTopology,
		out io.Writer,
		totals *dlqTotals,
	) (int, error) {
		attempts = append(attempts, topology.Kind)
		if topology.Kind == messaging.DeadLetterDirect {
			return 0, &amqp091.Error{Code: amqp091.NotFound}
		}
		return dlqRunKindChannel(ctx, group, command, topology, out, totals)
	}
	if err := dlqRunKinds(
		context.Background(), command, namespace, io.Discard, io.Discard, run,
	); err != nil {
		t.Fatalf("dlqRunKinds() error = %v, want the missing queue skipped", err)
	}
	if len(attempts) != 2 || len(group.published) != 1 {
		t.Fatalf("attempts = %v published = %d, want both chains and one replay", attempts, len(group.published))
	}
}

func TestDLQRunKindsMissingQueuesDoNotHideMissingID(t *testing.T) {
	for _, id := range []uint64{0, 42} {
		command := dlqCommand{
			mode:      dlqModeReplay,
			kinds:     []messaging.DeadLetterKind{messaging.DeadLetterDirect, messaging.DeadLetterGroup},
			messageID: id,
			limit:     100,
		}
		attempts := 0
		err := dlqRunKinds(context.Background(), command, messaging.DefaultNamespace(), io.Discard, io.Discard,
			func(context.Context, messaging.DeadLetterTopology, io.Writer, *dlqTotals) (int, error) {
				attempts++
				return 0, &amqp091.Error{Code: amqp091.NotFound}
			})
		if attempts != 2 {
			t.Fatalf("id=%d attempts=%d, want both chains", id, attempts)
		}
		if id == 0 && err != nil {
			t.Fatalf("empty all-queues inspection: %v", err)
		}
		var notFound *dlqIDNotFoundError
		if id != 0 && !errors.As(err, &notFound) {
			t.Fatalf("specific id error = %v, want not found", err)
		}
	}
}

func TestDLQRunKindReplayAllKeepsLimitWindow(t *testing.T) {
	fake := &dlqTestChannel{
		queued: 500,
		total:  500,
		idFor:  func(index int) string { return strconv.Itoa(1000 + index) },
	}
	var out strings.Builder
	command := dlqCommand{mode: dlqModeReplay, messageID: 0, limit: 100}
	var totals dlqTotals
	if _, err := dlqRunKindChannel(
		context.Background(), fake, command, dlqTestTopology(), &out, &totals,
	); err != nil {
		t.Fatalf("dlqRunKindChannel() error = %v", err)
	}
	if totals.scanned != 100 || totals.replayed != 100 {
		t.Fatalf("totals = %#v, want the 100-message window", totals)
	}
	if len(fake.nacked) != 0 {
		t.Fatalf("nacked = %v, want every replayed message acked", fake.nacked)
	}
}
