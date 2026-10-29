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

	var totals dlqTotals
	for _, kind := range command.kinds {
		topology := messaging.DeadLetterTopologyFor(namespace, kind)
		if err := dlqRunKind(ctx, connection, command, topology, os.Stdout, &totals); err != nil {
			if dlqQueueMissing(err) && len(command.kinds) > 1 {
				fmt.Fprintf(os.Stderr, "kind=%s skipped: %v\n", kind, err)
				continue
			}
			return err
		}
	}
	dlqPrintTotals(os.Stdout, command.mode, totals)
	return nil
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
) error {
	channel, err := connection.Channel()
	if err != nil {
		return fmt.Errorf("open RabbitMQ channel: %w", err)
	}
	defer func() { _ = channel.Close() }()

	adminChannel, err := messaging.NewAMQPDeadLetterChannel(channel)
	if err != nil {
		return err
	}
	queued, err := messaging.DeadLetterCount(ctx, adminChannel, topology)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "kind=%s queued=%d\n", topology.Kind, queued)

	switch command.mode {
	case dlqModeList:
		entries, err := messaging.ListDeadLetters(ctx, adminChannel, topology, command.limit)
		for _, entry := range entries {
			fmt.Fprintln(out, dlqEntryLine(entry))
		}
		fmt.Fprintf(out, "kind=%s listed=%d\n", topology.Kind, len(entries))
		totals.listed += len(entries)
		if err != nil {
			return err
		}
	case dlqModeReplay:
		result, err := messaging.ReplayDeadLetters(
			ctx, adminChannel, topology, command.messageID, command.limit,
		)
		fmt.Fprintf(
			out, "kind=%s scanned=%d replayed=%d skipped=%d\n",
			topology.Kind, result.Scanned, result.Replayed, result.Skipped,
		)
		totals.scanned += result.Scanned
		totals.replayed += result.Replayed
		totals.skipped += result.Skipped
		if err != nil {
			return err
		}
	case dlqModeDiscard:
		discarded, err := messaging.DiscardDeadLetters(
			ctx, adminChannel, topology, command.messageID, command.limit,
		)
		fmt.Fprintf(out, "kind=%s discarded=%d\n", topology.Kind, discarded)
		totals.discarded += discarded
		if err != nil {
			return err
		}
	}
	return nil
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
