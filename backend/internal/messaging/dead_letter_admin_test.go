package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

var (
	dlqAdminDirectTopology = DeadLetterTopology{
		Kind:             DeadLetterDirect,
		Exchange:         "im.messages.dead",
		Queue:            "im.messages.dead",
		RoutingKey:       "direct.dead",
		SourceExchange:   "im.messages",
		SourceRoutingKey: "direct.persist",
	}
	dlqAdminGroupTopology = DeadLetterTopology{
		Kind:             DeadLetterGroup,
		Exchange:         "im.group.messages.dead",
		Queue:            "im.group.messages.dead",
		RoutingKey:       "group.dead",
		SourceExchange:   "im.group.messages",
		SourceRoutingKey: "group.persist",
	}
)

type dlqAdminNack struct {
	tag     uint64
	requeue bool
}

type dlqAdminFakeChannel struct {
	deliveries []amqp091.Delivery
	next       int
	calls      int
	events     []string
	acked      []uint64
	nacked     []dlqAdminNack
	published  []amqp091.Publishing
	publishErr error
	getErrAt   int
	queue      amqp091.Queue
	declareErr error
}

func (f *dlqAdminFakeChannel) Get(queue string, autoAck bool) (amqp091.Delivery, bool, error) {
	f.calls++
	if f.getErrAt > 0 && f.calls == f.getErrAt {
		return amqp091.Delivery{}, false, errors.New("broker unavailable")
	}
	if f.next >= len(f.deliveries) {
		return amqp091.Delivery{}, false, nil
	}
	delivery := f.deliveries[f.next]
	f.next++
	f.events = append(f.events, "get:"+delivery.MessageId)
	return delivery, true, nil
}

func (f *dlqAdminFakeChannel) PublishConfirmed(
	ctx context.Context,
	exchange, routingKey string,
	message amqp091.Publishing,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.publishErr != nil {
		f.events = append(f.events, "publish-error:"+message.MessageId)
		return f.publishErr
	}
	f.published = append(f.published, message)
	f.events = append(f.events, "publish:"+message.MessageId)
	return nil
}

func (f *dlqAdminFakeChannel) Ack(deliveryTag uint64) error {
	f.acked = append(f.acked, deliveryTag)
	f.events = append(f.events, "ack:"+uintString(deliveryTag))
	return nil
}

func (f *dlqAdminFakeChannel) Nack(deliveryTag uint64, multiple, requeue bool) error {
	f.nacked = append(f.nacked, dlqAdminNack{tag: deliveryTag, requeue: requeue})
	f.events = append(f.events, "nack:"+uintString(deliveryTag))
	return nil
}

func (f *dlqAdminFakeChannel) QueueDeclarePassive(name string) (amqp091.Queue, error) {
	if f.declareErr != nil {
		return amqp091.Queue{}, f.declareErr
	}
	return f.queue, nil
}

func uintString(value uint64) string {
	return strconv.FormatUint(value, 10)
}

func dlqAdminTestDirect(tag uint64, messageID string, seq uint64, headers amqp091.Table) amqp091.Delivery {
	body, _ := json.Marshal(directMessageEnvelope{
		ServerMessageID: 10,
		Seq:             seq,
		SenderID:        7,
		RecipientID:     8,
		ClientMessageID: "client-1",
		Body:            "secret body text",
		CreatedAt:       "2026-10-29T10:00:00Z",
	})
	return amqp091.Delivery{
		DeliveryTag: tag,
		MessageId:   messageID,
		Headers:     headers,
		ContentType: "application/json",
		Body:        body,
	}
}

func dlqAdminTestGroup(tag uint64, messageID string, seq uint64) amqp091.Delivery {
	body, _ := json.Marshal(groupMessageEnvelope{
		ServerMessageID: 20,
		Seq:             seq,
		GroupID:         9,
		SenderID:        7,
		ClientMessageID: "client-2",
		Body:            "secret group text",
		CreatedAt:       "2026-10-29T10:00:00Z",
	})
	return amqp091.Delivery{DeliveryTag: tag, MessageId: messageID, Body: body}
}

func dlqAdminTestHeaders(reason string, attempts interface{}) amqp091.Table {
	return amqp091.Table{
		dlqHeaderReason:        reason,
		dlqHeaderError:         "insert failed",
		dlqHeaderAttempts:      attempts,
		dlqHeaderFirstFailedAt: "2026-10-29T09:55:00Z",
		dlqHeaderSource:        "direct",
	}
}

func TestListDeadLettersRequeuesEverything(t *testing.T) {
	fake := &dlqAdminFakeChannel{deliveries: []amqp091.Delivery{
		dlqAdminTestDirect(1, "101", 3, dlqAdminTestHeaders(dlqReasonRetriesExhausted, int32(6))),
		dlqAdminTestDirect(2, "102", 4, dlqAdminTestHeaders(dlqReasonConflict, int64(1))),
	}}

	entries, err := ListDeadLetters(context.Background(), fake, dlqAdminDirectTopology, 10)
	if err != nil {
		t.Fatalf("ListDeadLetters() error = %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(entries))
	}
	first := entries[0]
	if first.ID != 101 || first.Kind != DeadLetterDirect || first.Seq != 3 ||
		first.SenderID != 7 || first.RecipientID != 8 {
		t.Fatalf("entry = %#v", first)
	}
	if first.Reason != dlqReasonRetriesExhausted || first.Attempts != 6 {
		t.Fatalf("reason/attempts = %q/%d", first.Reason, first.Attempts)
	}
	if first.Source != "direct" || first.Error != "insert failed" {
		t.Fatalf("source/error = %q/%q", first.Source, first.Error)
	}
	if !first.FirstFailedAt.Equal(time.Date(2026, 10, 29, 9, 55, 0, 0, time.UTC)) {
		t.Fatalf("first failed = %v", first.FirstFailedAt)
	}
	if first.BodySize == 0 {
		t.Fatalf("body size = 0")
	}
	if entries[1].Attempts != 1 || entries[1].Reason != dlqReasonConflict {
		t.Fatalf("second entry = %#v", entries[1])
	}
	encoded, _ := json.Marshal(entries)
	if strings.Contains(string(encoded), "secret body text") {
		t.Fatalf("entry leaked the body text: %s", encoded)
	}
	if len(fake.acked) != 0 {
		t.Fatalf("acked = %v, want none", fake.acked)
	}
	if len(fake.nacked) != 2 || !fake.nacked[0].requeue || !fake.nacked[1].requeue {
		t.Fatalf("nacked = %#v, want both requeued", fake.nacked)
	}
	if want := []string{"get:101", "get:102", "nack:1", "nack:2"}; !equalStrings(fake.events, want) {
		t.Fatalf("events = %v, want %v", fake.events, want)
	}
}

func TestListDeadLettersHonorsLimitAndEmptyQueue(t *testing.T) {
	fake := &dlqAdminFakeChannel{deliveries: []amqp091.Delivery{
		dlqAdminTestDirect(1, "1", 1, nil),
		dlqAdminTestDirect(2, "2", 2, nil),
		dlqAdminTestDirect(3, "3", 3, nil),
	}}
	entries, err := ListDeadLetters(context.Background(), fake, dlqAdminDirectTopology, 2)
	if err != nil {
		t.Fatalf("ListDeadLetters() error = %v", err)
	}
	if len(entries) != 2 || fake.calls != 2 {
		t.Fatalf("entries/calls = %d/%d, want 2/2", len(entries), fake.calls)
	}
	if len(fake.nacked) != 2 {
		t.Fatalf("nacked = %#v", fake.nacked)
	}

	empty := &dlqAdminFakeChannel{}
	entries, err = ListDeadLetters(context.Background(), empty, dlqAdminDirectTopology, 5)
	if err != nil || len(entries) != 0 || len(empty.nacked) != 0 {
		t.Fatalf("empty queue: entries=%d err=%v nacked=%d", len(entries), err, len(empty.nacked))
	}
}

func TestListDeadLettersRequeuesOnError(t *testing.T) {
	fake := &dlqAdminFakeChannel{
		deliveries: []amqp091.Delivery{
			dlqAdminTestDirect(1, "1", 1, nil),
			dlqAdminTestDirect(2, "2", 2, nil),
		},
		getErrAt: 2,
	}
	entries, err := ListDeadLetters(context.Background(), fake, dlqAdminDirectTopology, 5)
	if err == nil {
		t.Fatal("ListDeadLetters() error = nil, want error")
	}
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1 partial entry", len(entries))
	}
	if len(fake.nacked) != 1 || fake.nacked[0].tag != 1 || !fake.nacked[0].requeue {
		t.Fatalf("nacked = %#v, want tag 1 requeued", fake.nacked)
	}
}

func TestListDeadLettersFailsOnRepeatedDeliveryTag(t *testing.T) {
	delivery := dlqAdminTestDirect(1, "1", 1, nil)
	fake := &dlqAdminFakeChannel{deliveries: []amqp091.Delivery{delivery, delivery}}
	entries, err := ListDeadLetters(context.Background(), fake, dlqAdminDirectTopology, 5)
	if err == nil {
		t.Fatal("ListDeadLetters() error = nil, want repeated delivery tag error")
	}
	if !strings.Contains(err.Error(), "delivery tag 1 repeated") {
		t.Fatalf("error = %v, want repeated delivery tag", err)
	}
	if len(entries) != 1 || fake.calls != 2 {
		t.Fatalf("entries/calls = %d/%d, want 1/2", len(entries), fake.calls)
	}
	if len(fake.nacked) != 2 {
		t.Fatalf("nacked = %#v, want the entry and the repeated delivery", fake.nacked)
	}
	for _, nack := range fake.nacked {
		if !nack.requeue {
			t.Fatalf("nacked without requeue: %#v", nack)
		}
	}
}

func TestListDeadLettersProcessesDuplicateMessageIDs(t *testing.T) {
	fake := &dlqAdminFakeChannel{deliveries: []amqp091.Delivery{
		dlqAdminTestDirect(1, "101", 3, nil),
		dlqAdminTestDirect(2, "101", 3, nil),
		dlqAdminTestDirect(3, "102", 4, nil),
	}}
	entries, err := ListDeadLetters(context.Background(), fake, dlqAdminDirectTopology, 5)
	if err != nil {
		t.Fatalf("ListDeadLetters() error = %v", err)
	}
	if len(entries) != 3 || fake.calls != 4 {
		t.Fatalf("entries/calls = %d/%d, want 3 entries and the empty probe", len(entries), fake.calls)
	}
	if entries[0].ID != 101 || entries[1].ID != 101 || entries[2].ID != 102 {
		t.Fatalf("entries = %#v", entries)
	}
}

func TestListDeadLettersSummarizesGroupAndMalformedBodies(t *testing.T) {
	group := dlqAdminTestGroup(1, "201", 12)
	malformed := amqp091.Delivery{DeliveryTag: 2, MessageId: "202", Body: []byte("not-json")}
	fake := &dlqAdminFakeChannel{deliveries: []amqp091.Delivery{group, malformed}}

	entries, err := ListDeadLetters(context.Background(), fake, dlqAdminGroupTopology, 5)
	if err != nil {
		t.Fatalf("ListDeadLetters() error = %v", err)
	}
	if entries[0].GroupID != 9 || entries[0].RecipientID != 0 || entries[0].Seq != 12 {
		t.Fatalf("group entry = %#v", entries[0])
	}
	if entries[1].SummaryError == "" {
		t.Fatalf("malformed entry = %#v, want summary error", entries[1])
	}
}

func TestReplayDeadLettersPublishesThenAcks(t *testing.T) {
	fake := &dlqAdminFakeChannel{deliveries: []amqp091.Delivery{
		dlqAdminTestDirect(1, "101", 3, nil),
		dlqAdminTestDirect(2, "102", 4, nil),
	}}
	result, err := ReplayDeadLetters(context.Background(), fake, dlqAdminDirectTopology, 0, 10)
	if err != nil {
		t.Fatalf("ReplayDeadLetters() error = %v", err)
	}
	if result.Scanned != 2 || result.Replayed != 2 || result.Skipped != 0 {
		t.Fatalf("result = %#v", result)
	}
	if len(fake.acked) != 2 || len(fake.nacked) != 0 {
		t.Fatalf("acked=%v nacked=%#v", fake.acked, fake.nacked)
	}
	if len(fake.published) != 2 {
		t.Fatalf("published = %d, want 2", len(fake.published))
	}
	if fake.published[0].MessageId != "101" ||
		fake.published[0].DeliveryMode != amqp091.Persistent ||
		string(fake.published[0].Body) != string(fake.deliveries[0].Body) {
		t.Fatalf("published = %#v", fake.published[0])
	}
	want := []string{"get:101", "publish:101", "ack:1", "get:102", "publish:102", "ack:2"}
	if !equalStrings(fake.events, want) {
		t.Fatalf("events = %v, want %v", fake.events, want)
	}
}

func TestReplayDeadLettersFailedPublishRequeuesAndStops(t *testing.T) {
	fake := &dlqAdminFakeChannel{
		deliveries: []amqp091.Delivery{
			dlqAdminTestDirect(1, "101", 3, nil),
			dlqAdminTestDirect(2, "102", 4, nil),
		},
		publishErr: errors.New("confirm rejected"),
	}
	result, err := ReplayDeadLetters(context.Background(), fake, dlqAdminDirectTopology, 0, 10)
	if err == nil {
		t.Fatal("ReplayDeadLetters() error = nil, want error")
	}
	if result.Scanned != 1 || result.Replayed != 0 {
		t.Fatalf("result = %#v", result)
	}
	if len(fake.acked) != 0 || fake.calls != 1 {
		t.Fatalf("acked=%v calls=%d, want no ack and one get", fake.acked, fake.calls)
	}
	if len(fake.nacked) != 1 || fake.nacked[0].tag != 1 || !fake.nacked[0].requeue {
		t.Fatalf("nacked = %#v, want tag 1 requeued", fake.nacked)
	}
}

func TestReplayDeadLettersSkipsNonMatching(t *testing.T) {
	fake := &dlqAdminFakeChannel{deliveries: []amqp091.Delivery{
		dlqAdminTestDirect(1, "101", 3, nil),
		dlqAdminTestDirect(2, "102", 4, nil),
		dlqAdminTestDirect(3, "103", 5, nil),
	}}
	result, err := ReplayDeadLetters(context.Background(), fake, dlqAdminDirectTopology, 102, 10)
	if err != nil {
		t.Fatalf("ReplayDeadLetters() error = %v", err)
	}
	if result.Scanned != 3 || result.Replayed != 1 || result.Skipped != 2 {
		t.Fatalf("result = %#v", result)
	}
	if len(fake.published) != 1 || fake.published[0].MessageId != "102" {
		t.Fatalf("published = %#v", fake.published)
	}
	if len(fake.acked) != 1 || fake.acked[0] != 2 {
		t.Fatalf("acked = %v, want tag 2", fake.acked)
	}
	if len(fake.nacked) != 2 || fake.nacked[0].tag != 1 || fake.nacked[1].tag != 3 {
		t.Fatalf("nacked = %#v", fake.nacked)
	}
	for _, nack := range fake.nacked {
		if !nack.requeue {
			t.Fatalf("nacked without requeue: %#v", nack)
		}
	}
}

func TestReplayDeadLettersReplaysDuplicateMessageIDs(t *testing.T) {
	fake := &dlqAdminFakeChannel{deliveries: []amqp091.Delivery{
		dlqAdminTestDirect(1, "101", 3, nil),
		dlqAdminTestDirect(2, "101", 3, nil),
	}}
	result, err := ReplayDeadLetters(context.Background(), fake, dlqAdminDirectTopology, 0, 10)
	if err != nil {
		t.Fatalf("ReplayDeadLetters() error = %v", err)
	}
	if result.Scanned != 2 || result.Replayed != 2 || result.Skipped != 0 {
		t.Fatalf("result = %#v", result)
	}
	if len(fake.published) != 2 || len(fake.acked) != 2 || len(fake.nacked) != 0 {
		t.Fatalf("published=%d acked=%v nacked=%#v", len(fake.published), fake.acked, fake.nacked)
	}
}

func TestReplayDeadLettersFailsOnRepeatedDeliveryTag(t *testing.T) {
	delivery := dlqAdminTestDirect(1, "101", 3, nil)
	fake := &dlqAdminFakeChannel{deliveries: []amqp091.Delivery{delivery, delivery}}
	result, err := ReplayDeadLetters(context.Background(), fake, dlqAdminDirectTopology, 0, 10)
	if err == nil {
		t.Fatal("ReplayDeadLetters() error = nil, want repeated delivery tag error")
	}
	if result.Scanned != 1 || result.Replayed != 1 {
		t.Fatalf("result = %#v", result)
	}
	if len(fake.nacked) != 1 || fake.nacked[0].tag != 1 || !fake.nacked[0].requeue {
		t.Fatalf("nacked = %#v, want the repeated tag requeued", fake.nacked)
	}
}

func TestReplayDeadLettersKeepsNonNumericMessageID(t *testing.T) {
	delivery := dlqAdminTestDirect(1, "", 3, nil)
	fake := &dlqAdminFakeChannel{deliveries: []amqp091.Delivery{delivery}}
	result, err := ReplayDeadLetters(context.Background(), fake, dlqAdminDirectTopology, 0, 10)
	if err != nil {
		t.Fatalf("ReplayDeadLetters() error = %v", err)
	}
	if result.Replayed != 1 || len(fake.published) != 1 || fake.published[0].MessageId != "" {
		t.Fatalf("result=%#v published=%#v", result, fake.published)
	}
}

// dlqReplayAMQPChannel is a broker-free dlqAdminAMQPChannel.
type dlqReplayAMQPChannel struct {
	confirms  chan amqp091.Confirmation
	returns   chan amqp091.Return
	nextSeq   uint64
	delivered []amqp091.Delivery
	next      int
	mandatory []bool
	returned  *amqp091.Return
	acked     []uint64
	nacked    []dlqAdminNack
}

func newDLQReplayAMQPChannel(deliveries ...amqp091.Delivery) *dlqReplayAMQPChannel {
	return &dlqReplayAMQPChannel{
		confirms:  make(chan amqp091.Confirmation, 1),
		returns:   make(chan amqp091.Return, 1),
		nextSeq:   1,
		delivered: deliveries,
	}
}

func (f *dlqReplayAMQPChannel) Confirm(bool) error {
	return nil
}

func (f *dlqReplayAMQPChannel) NotifyPublish(c chan amqp091.Confirmation) chan amqp091.Confirmation {
	f.confirms = c
	return c
}

func (f *dlqReplayAMQPChannel) NotifyReturn(c chan amqp091.Return) chan amqp091.Return {
	f.returns = c
	return c
}

func (f *dlqReplayAMQPChannel) GetNextPublishSeqNo() uint64 {
	return f.nextSeq
}

func (f *dlqReplayAMQPChannel) PublishWithContext(
	ctx context.Context,
	exchange, key string,
	mandatory, immediate bool,
	msg amqp091.Publishing,
) error {
	f.mandatory = append(f.mandatory, mandatory)
	if f.returned != nil {
		f.returns <- *f.returned
	}
	f.confirms <- amqp091.Confirmation{DeliveryTag: f.nextSeq, Ack: true}
	return nil
}

func (f *dlqReplayAMQPChannel) Get(queue string, autoAck bool) (amqp091.Delivery, bool, error) {
	if f.next >= len(f.delivered) {
		return amqp091.Delivery{}, false, nil
	}
	delivery := f.delivered[f.next]
	f.next++
	return delivery, true, nil
}

func (f *dlqReplayAMQPChannel) Ack(deliveryTag uint64, multiple bool) error {
	f.acked = append(f.acked, deliveryTag)
	return nil
}

func (f *dlqReplayAMQPChannel) Nack(deliveryTag uint64, multiple, requeue bool) error {
	f.nacked = append(f.nacked, dlqAdminNack{tag: deliveryTag, requeue: requeue})
	return nil
}

func (f *dlqReplayAMQPChannel) QueueDeclarePassive(
	name string,
	durable, autoDelete, exclusive, noWait bool,
	args amqp091.Table,
) (amqp091.Queue, error) {
	return amqp091.Queue{Name: name}, nil
}

func TestReplayDeadLettersFailsWhenReplayUnroutable(t *testing.T) {
	fake := newDLQReplayAMQPChannel(dlqAdminTestDirect(1, "101", 3, nil))
	fake.returned = &amqp091.Return{
		ReplyCode:  312,
		ReplyText:  "NO_ROUTE",
		Exchange:   dlqAdminDirectTopology.SourceExchange,
		RoutingKey: dlqAdminDirectTopology.SourceRoutingKey,
	}
	channel := &AMQPDeadLetterChannel{
		channel:  fake,
		confirms: fake.confirms,
		returns:  fake.returns,
		timeout:  10 * time.Millisecond,
	}
	result, err := ReplayDeadLetters(context.Background(), channel, dlqAdminDirectTopology, 0, 10)
	if err == nil || !strings.Contains(err.Error(), "unroutable") {
		t.Fatalf("ReplayDeadLetters() error = %v, want unroutable error", err)
	}
	if result.Replayed != 0 || len(fake.mandatory) != 1 || !fake.mandatory[0] {
		t.Fatalf("result=%#v mandatory=%v, want mandatory publish and no replay", result, fake.mandatory)
	}
	if len(fake.acked) != 0 {
		t.Fatalf("acked = %v, an unroutable replay must keep the dead letter", fake.acked)
	}
	if len(fake.nacked) != 1 || fake.nacked[0].tag != 1 || !fake.nacked[0].requeue {
		t.Fatalf("nacked = %#v, want tag 1 requeued", fake.nacked)
	}
}

func TestDiscardDeadLettersAcksOnlyMatching(t *testing.T) {
	fake := &dlqAdminFakeChannel{deliveries: []amqp091.Delivery{
		dlqAdminTestDirect(1, "101", 3, nil),
		dlqAdminTestDirect(2, "102", 4, nil),
		dlqAdminTestDirect(3, "103", 5, nil),
	}}
	discarded, err := DiscardDeadLetters(context.Background(), fake, dlqAdminDirectTopology, 102, 10)
	if err != nil {
		t.Fatalf("DiscardDeadLetters() error = %v", err)
	}
	if discarded != 1 {
		t.Fatalf("discarded = %d, want 1", discarded)
	}
	if len(fake.acked) != 1 || fake.acked[0] != 2 {
		t.Fatalf("acked = %v, want tag 2", fake.acked)
	}
	if len(fake.nacked) != 2 || len(fake.published) != 0 {
		t.Fatalf("nacked=%#v published=%#v", fake.nacked, fake.published)
	}
}

func TestDiscardDeadLettersDiscardsDuplicateMessageIDs(t *testing.T) {
	fake := &dlqAdminFakeChannel{deliveries: []amqp091.Delivery{
		dlqAdminTestDirect(1, "101", 3, nil),
		dlqAdminTestDirect(2, "101", 3, nil),
	}}
	discarded, err := DiscardDeadLetters(context.Background(), fake, dlqAdminDirectTopology, 0, 10)
	if err != nil {
		t.Fatalf("DiscardDeadLetters() error = %v", err)
	}
	if discarded != 2 || len(fake.acked) != 2 || len(fake.nacked) != 0 {
		t.Fatalf("discarded=%d acked=%v nacked=%#v", discarded, fake.acked, fake.nacked)
	}
}

func TestDiscardDeadLettersHonorsLimit(t *testing.T) {
	fake := &dlqAdminFakeChannel{deliveries: []amqp091.Delivery{
		dlqAdminTestDirect(1, "101", 3, nil),
		dlqAdminTestDirect(2, "102", 4, nil),
		dlqAdminTestDirect(3, "103", 5, nil),
	}}
	discarded, err := DiscardDeadLetters(context.Background(), fake, dlqAdminDirectTopology, 0, 2)
	if err != nil {
		t.Fatalf("DiscardDeadLetters() error = %v", err)
	}
	if discarded != 2 || fake.calls != 2 || len(fake.acked) != 2 {
		t.Fatalf("discarded=%d calls=%d acked=%v", discarded, fake.calls, fake.acked)
	}
}

func TestDeadLetterCountUsesPassiveDeclare(t *testing.T) {
	fake := &dlqAdminFakeChannel{queue: amqp091.Queue{Name: "q", Messages: 7}}
	count, err := DeadLetterCount(context.Background(), fake, dlqAdminDirectTopology)
	if err != nil {
		t.Fatalf("DeadLetterCount() error = %v", err)
	}
	if count != 7 {
		t.Fatalf("count = %d, want 7", count)
	}

	fake.declareErr = errors.New("NOT_FOUND")
	if _, err := DeadLetterCount(context.Background(), fake, dlqAdminDirectTopology); err == nil {
		t.Fatal("DeadLetterCount() error = nil, want error")
	}
}

func TestDeadLetterAdminRejectsInvalidArguments(t *testing.T) {
	fake := &dlqAdminFakeChannel{}
	for _, limit := range []int{0, -1, dlqAdminMaxLimit + 1} {
		if _, err := ListDeadLetters(context.Background(), fake, dlqAdminDirectTopology, limit); err == nil {
			t.Fatalf("limit %d: error = nil", limit)
		}
	}
	if _, err := ListDeadLetters(context.Background(), nil, dlqAdminDirectTopology, 10); err == nil {
		t.Fatal("nil channel: error = nil")
	}
	if _, err := ListDeadLetters(context.Background(), fake, DeadLetterTopology{Kind: DeadLetterDirect}, 10); err == nil {
		t.Fatal("incomplete topology: error = nil")
	}
}

func TestDeadLetterHeaderParsing(t *testing.T) {
	delivery := dlqAdminTestDirect(1, "101", 3, amqp091.Table{
		dlqHeaderAttempts:      float64(4),
		dlqHeaderFirstFailedAt: "not-a-time",
		dlqHeaderReason:        []byte("conflict"),
	})
	entry := dlqAdminEntry(DeadLetterDirect, delivery)
	if entry.Attempts != 4 || entry.Reason != "conflict" {
		t.Fatalf("entry = %#v", entry)
	}
	if !entry.FirstFailedAt.IsZero() {
		t.Fatalf("first failed = %v, want zero", entry.FirstFailedAt)
	}

	stringAttempts := dlqAdminEntry(DeadLetterDirect, dlqAdminTestDirect(1, "1", 1, amqp091.Table{
		dlqHeaderAttempts: "6",
	}))
	if stringAttempts.Attempts != 6 {
		t.Fatalf("string attempts = %d, want 6", stringAttempts.Attempts)
	}
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
