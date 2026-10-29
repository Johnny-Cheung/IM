package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Johnny-Cheung/IM/backend/internal/config"
	"github.com/Johnny-Cheung/IM/backend/internal/infra"
	"github.com/Johnny-Cheung/IM/backend/internal/messaging"
	"github.com/rabbitmq/amqp091-go"
)

const (
	dlqDefaultLimit   = 100
	dlqMaxLimit       = 10000
	dlqCommandTimeout = 5 * time.Minute
	dlqShortErrorSize = 120
)

type dlqMode int

const (
	dlqModeList dlqMode = iota
	dlqModeReplay
	dlqModeDiscard
)

type dlqCommand struct {
	mode      dlqMode
	kinds     []messaging.DeadLetterKind
	messageID uint64
	limit     int
}

// runDeadLetterAdmin implements -dlq-list, -dlq-replay and -dlq-discard. It
// only needs RabbitMQ configuration and does not touch MySQL or Redis.
func runDeadLetterAdmin(options serverOptions) error {
	command, err := dlqParseOptions(options)
	if err != nil {
		return err
	}
	infraConfig, err := config.LoadInfra()
	if err != nil {
		return fmt.Errorf("load infrastructure configuration: %w", err)
	}
	if !infraConfig.RabbitMQ.Enabled() {
		return fmt.Errorf("RabbitMQ configuration is required for dead letter commands")
	}
	namespace, err := messaging.NewNamespace(infraConfig.MessagingNamespace)
	if err != nil {
		return fmt.Errorf("configure messaging namespace: %w", err)
	}
	connection, err := infra.DialRabbit(infraConfig.RabbitMQ)
	if err != nil {
		return fmt.Errorf("connect RabbitMQ: %w", dlqSanitizeError(err, infraConfig.RabbitMQ))
	}
	defer func() {
		if err := connection.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close RabbitMQ connection: %v\n", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), dlqCommandTimeout)
	defer cancel()

	return dlqRunKinds(
		ctx,
		command,
		namespace,
		os.Stdout,
		os.Stderr,
		func(ctx context.Context, topology messaging.DeadLetterTopology, out io.Writer, totals *dlqTotals) (int, error) {
			return dlqRunKind(ctx, connection, command, topology, out, totals)
		},
	)
}

// dlqRunKinds runs the command against every selected chain. A not-found
// result in one chain is not fatal: -dlq-kind all must try the other chain
// before reporting that the id is missing, and a hit in either chain is
// success. Real broker errors still abort the run.
func dlqRunKinds(
	ctx context.Context,
	command dlqCommand,
	namespace messaging.Namespace,
	out, errOut io.Writer,
	runKind func(context.Context, messaging.DeadLetterTopology, io.Writer, *dlqTotals) (int, error),
) error {
	var totals dlqTotals
	// Report what was done even when a later chain fails: a successful replay
	// in the first chain must not look like nothing happened.
	defer func() { dlqPrintTotals(out, command.mode, totals) }()
	missing := make([]dlqIDMiss, 0, len(command.kinds))
	matched := 0
	for _, kind := range command.kinds {
		topology := messaging.DeadLetterTopologyFor(namespace, kind)
		kindMatched, err := runKind(ctx, topology, out, &totals)
		if err == nil {
			matched += kindMatched
			continue
		}
		var miss *dlqIDMiss
		if errors.As(err, &miss) {
			missing = append(missing, *miss)
			fmt.Fprintf(errOut, "kind=%s: %v\n", kind, err)
			continue
		}
		if dlqQueueMissing(err) && len(command.kinds) > 1 {
			fmt.Fprintf(errOut, "kind=%s skipped: %v\n", kind, err)
			if command.messageID != 0 {
				missing = append(missing, dlqIDMiss{kind: kind, id: command.messageID})
			}
			continue
		}
		return err
	}
	if command.messageID == 0 || matched > 0 || len(missing) == 0 {
		return nil
	}
	return &dlqIDNotFoundError{misses: missing}
}

type dlqTotals struct {
	scanned   int
	listed    int
	replayed  int
	skipped   int
	discarded int
}

func dlqRunKind(
	ctx context.Context,
	connection *amqp091.Connection,
	command dlqCommand,
	topology messaging.DeadLetterTopology,
	out io.Writer,
	totals *dlqTotals,
) (int, error) {
	channel, err := connection.Channel()
	if err != nil {
		return 0, fmt.Errorf("open RabbitMQ channel: %w", err)
	}
	defer func() { _ = channel.Close() }()

	adminChannel, err := messaging.NewAMQPDeadLetterChannel(channel)
	if err != nil {
		return 0, err
	}
	return dlqRunKindChannel(ctx, adminChannel, command, topology, out, totals)
}

// dlqRunKindChannel returns how many messages matched the requested id. A
// specific id that was not seen yields a *dlqIDMiss so -dlq-kind all can try
// the other chain before reporting failure.
func dlqRunKindChannel(
	ctx context.Context,
	adminChannel messaging.DeadLetterChannel,
	command dlqCommand,
	topology messaging.DeadLetterTopology,
	out io.Writer,
	totals *dlqTotals,
) (int, error) {
	queued, err := messaging.DeadLetterCount(ctx, adminChannel, topology)
	if err != nil {
		return 0, err
	}
	fmt.Fprintf(out, "kind=%s queued=%d\n", topology.Kind, queued)

	matched := 0
	switch command.mode {
	case dlqModeList:
		entries, err := messaging.ListDeadLetters(ctx, adminChannel, topology, command.limit)
		for _, entry := range entries {
			fmt.Fprintln(out, dlqEntryLine(entry))
		}
		fmt.Fprintf(out, "kind=%s listed=%d\n", topology.Kind, len(entries))
		totals.listed += len(entries)
		if err != nil {
			return 0, err
		}
	case dlqModeReplay:
		limit := dlqScanLimit(command, queued)
		result, err := messaging.ReplayDeadLetters(
			ctx, adminChannel, topology, command.messageID, limit,
		)
		totals.scanned += result.Scanned
		totals.replayed += result.Replayed
		totals.skipped += result.Skipped
		matched = result.Replayed
		if err == nil {
			err = dlqCheckIDFound(command, topology, queued, limit, matched)
		}
		if err != nil {
			return matched, err
		}
		fmt.Fprintf(
			out, "kind=%s scanned=%d replayed=%d skipped=%d\n",
			topology.Kind, result.Scanned, result.Replayed, result.Skipped,
		)
	case dlqModeDiscard:
		limit := dlqScanLimit(command, queued)
		discarded, err := messaging.DiscardDeadLetters(
			ctx, adminChannel, topology, command.messageID, limit,
		)
		totals.discarded += discarded
		matched = discarded
		if err == nil {
			err = dlqCheckIDFound(command, topology, queued, limit, matched)
		}
		if err != nil {
			return matched, err
		}
		fmt.Fprintf(out, "kind=%s discarded=%d\n", topology.Kind, discarded)
	}
	return matched, nil
}

// dlqScanLimit widens a single-id search to the whole queue when the default
// window is smaller. basic.get requeues ahead, so a fixed head window can
// never reach a message that sits behind it.
func dlqScanLimit(command dlqCommand, queued int) int {
	if command.messageID == 0 || queued <= command.limit {
		return command.limit
	}
	if queued > dlqMaxLimit {
		return dlqMaxLimit
	}
	return queued
}

// dlqCheckIDFound refuses to report success when a specific id was not seen.
// The error distinguishes a fully scanned queue from a window cut short by the
// absolute cap so the operator knows whether draining the head can help.
func dlqCheckIDFound(
	command dlqCommand,
	topology messaging.DeadLetterTopology,
	queued, limit, matched int,
) error {
	if command.messageID == 0 || matched > 0 {
		return nil
	}
	return &dlqIDMiss{
		kind:   topology.Kind,
		id:     command.messageID,
		limit:  limit,
		queued: queued,
	}
}

// dlqIDMiss records one chain that did not contain the requested message id.
type dlqIDMiss struct {
	kind   messaging.DeadLetterKind
	id     uint64
	limit  int
	queued int
}

func (miss *dlqIDMiss) Error() string {
	if miss.capped() {
		return fmt.Sprintf(
			"dead letter id %d not found in the first %d of %d queued %s messages; the scan window is capped at %d, drain the head first",
			miss.id, miss.limit, miss.queued, miss.kind, dlqMaxLimit,
		)
	}
	return fmt.Sprintf(
		"dead letter id %d not found in %d queued %s messages",
		miss.id, miss.queued, miss.kind,
	)
}

// capped reports whether the scan window was cut short by the absolute limit
// instead of covering the whole queue.
func (miss *dlqIDMiss) capped() bool {
	return miss.queued > miss.limit
}

// dlqIDNotFoundError reports that no selected chain contained the id.
type dlqIDNotFoundError struct {
	misses []dlqIDMiss
}

func (err *dlqIDNotFoundError) Error() string {
	parts := make([]string, len(err.misses))
	for index := range err.misses {
		parts[index] = err.misses[index].Error()
	}
	return strings.Join(parts, "; ")
}

func dlqPrintTotals(out io.Writer, mode dlqMode, totals dlqTotals) {
	switch mode {
	case dlqModeList:
		fmt.Fprintf(out, "total listed=%d\n", totals.listed)
	case dlqModeReplay:
		fmt.Fprintf(
			out, "total scanned=%d replayed=%d skipped=%d\n",
			totals.scanned, totals.replayed, totals.skipped,
		)
	case dlqModeDiscard:
		fmt.Fprintf(out, "total discarded=%d\n", totals.discarded)
	}
}

// dlqParseOptions validates the flags before any connection is opened.
func dlqParseOptions(options serverOptions) (dlqCommand, error) {
	var command dlqCommand
	selected := 0
	if options.dlqList {
		selected++
		command.mode = dlqModeList
	}
	if options.dlqReplay {
		selected++
		command.mode = dlqModeReplay
	}
	if options.dlqDiscard {
		selected++
		command.mode = dlqModeDiscard
	}
	if selected == 0 {
		return command, fmt.Errorf("no dead letter command selected")
	}
	if selected > 1 {
		return command, fmt.Errorf("-dlq-list, -dlq-replay and -dlq-discard are mutually exclusive")
	}
	if options.backfillSeq {
		return command, fmt.Errorf("dead letter commands cannot be combined with -backfill-seq")
	}

	kinds, err := dlqParseKinds(options.dlqKind)
	if err != nil {
		return command, err
	}
	if options.dlqLimit < 1 || options.dlqLimit > dlqMaxLimit {
		return command, fmt.Errorf("-dlq-limit must be between 1 and %d", dlqMaxLimit)
	}
	if command.mode == dlqModeDiscard && !options.dlqYes {
		return command, fmt.Errorf("-dlq-discard requires -dlq-yes")
	}
	if command.mode == dlqModeReplay && options.dlqID == 0 && !options.dlqYes {
		return command, fmt.Errorf("-dlq-replay of every dead letter requires -dlq-id or -dlq-yes")
	}

	command.kinds = kinds
	command.messageID = options.dlqID
	command.limit = options.dlqLimit
	return command, nil
}

func dlqParseKinds(value string) ([]messaging.DeadLetterKind, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "all":
		return []messaging.DeadLetterKind{
			messaging.DeadLetterDirect,
			messaging.DeadLetterGroup,
		}, nil
	case string(messaging.DeadLetterDirect):
		return []messaging.DeadLetterKind{messaging.DeadLetterDirect}, nil
	case string(messaging.DeadLetterGroup):
		return []messaging.DeadLetterKind{messaging.DeadLetterGroup}, nil
	default:
		return nil, fmt.Errorf("-dlq-kind must be direct, group or all")
	}
}

func dlqEntryLine(entry messaging.DeadLetterEntry) string {
	return fmt.Sprintf(
		"id=%d kind=%s seq=%d reason=%s attempts=%d first_failed=%s error=%s",
		entry.ID,
		entry.Kind,
		entry.Seq,
		dlqOrDash(entry.Reason),
		entry.Attempts,
		dlqFormatTime(entry.FirstFailedAt),
		dlqShortError(entry.Error),
	)
}

func dlqFormatTime(value time.Time) string {
	if value.IsZero() {
		return "-"
	}
	return value.UTC().Format(time.RFC3339)
}

func dlqOrDash(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return value
}

func dlqShortError(text string) string {
	text = strings.Join(strings.Fields(text), " ")
	if text == "" {
		return "-"
	}
	if len(text) <= dlqShortErrorSize {
		return text
	}
	cut := dlqShortErrorSize
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut] + "..."
}

func dlqQueueMissing(err error) bool {
	var amqpErr *amqp091.Error
	return errors.As(err, &amqpErr) && amqpErr.Code == amqp091.NotFound
}

// dlqSanitizeError keeps the RabbitMQ URL and password out of CLI errors.
func dlqSanitizeError(err error, rabbitConfig config.RabbitMQConfig) error {
	if err == nil {
		return nil
	}
	message := err.Error()
	if rabbitConfig.URL != "" {
		message = strings.ReplaceAll(message, rabbitConfig.URL, "<redacted-url>")
	}
	if rabbitConfig.Password != "" {
		message = strings.ReplaceAll(message, rabbitConfig.Password, "<credential>")
	}
	return errors.New(message)
}
