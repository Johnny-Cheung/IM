import type { GroupReadStats } from "./api";

export const GROUP_RECEIPT_RECENT_MS = 10_000;
export const GROUP_RECEIPT_RECENT_INTERVAL_MS = 1000;
export const GROUP_RECEIPT_MID_MS = 60_000;
export const GROUP_RECEIPT_MID_INTERVAL_MS = 3000;
export const GROUP_RECEIPT_SLOW_MS = 5 * 60_000;
export const GROUP_RECEIPT_SLOW_INTERVAL_MS = 10_000;
export const GROUP_RECEIPT_MAX_INTERVAL_MS = 30_000;
export const GROUP_RECEIPT_STOP_AFTER_MS = 10 * 60_000;
export const GROUP_RECEIPT_FAST_POLLS = 5;

export type GroupReceiptTarget = {
  groupID: number;
  seq: number;
  sentAtMs: number;
};

// A recent receipt is worth checking often; an old one backs off. After the
// message is old enough, only a visible target that is still changing keeps
// polling. The caller stops the poller when the message leaves the viewport.
export function groupReceiptDelay(
  ageMs: number,
  fastPolls: number,
  unchangedPolls: number,
): number | null {
  const age = Number.isFinite(ageMs) ? Math.max(0, ageMs) : 0;
  if (age >= GROUP_RECEIPT_STOP_AFTER_MS && fastPolls <= 0) {
    return null;
  }

  let base: number;
  if (age < GROUP_RECEIPT_RECENT_MS) {
    base = GROUP_RECEIPT_RECENT_INTERVAL_MS;
  } else if (age < GROUP_RECEIPT_MID_MS) {
    base = GROUP_RECEIPT_MID_INTERVAL_MS;
  } else if (age < GROUP_RECEIPT_SLOW_MS) {
    base = GROUP_RECEIPT_SLOW_INTERVAL_MS;
  } else {
    base = GROUP_RECEIPT_MAX_INTERVAL_MS;
  }

  if (unchangedPolls >= 3) {
    const factor = Math.min(2 ** Math.min(unchangedPolls - 2, 3), 8);
    base = Math.min(base * factor, GROUP_RECEIPT_MAX_INTERVAL_MS);
  }
  if (fastPolls > 0) {
    base = GROUP_RECEIPT_RECENT_INTERVAL_MS;
  }
  return base;
}

export function withJitter(
  delayMs: number,
  random: () => number = Math.random,
): number {
  return Math.round(delayMs * (0.85 + random() * 0.3));
}

export type GroupReceiptPollerOptions = {
  isEligible: () => boolean;
  getAgeMs: () => number;
  fetch: () => Promise<GroupReadStats | null>;
  onStats?: (stats: GroupReadStats) => void;
  onError?: (error: unknown) => void;
  random?: () => number;
};

// GroupReceiptPoller is a visibility-aware, adaptive poller for one sender's
// last group message. It never runs while the receipt is off screen, stops
// once every other member has read, and speeds up briefly after a change.
export class GroupReceiptPoller {
  private timer: ReturnType<typeof setTimeout> | null = null;
  private started = false;
  private completed = false;
  private inFlight = false;
  private fastPolls = 0;
  private unchangedPolls = 0;
  private lastSignature: string | null = null;

  constructor(private readonly options: GroupReceiptPollerOptions) {}

  start(immediate = true, reset = false): void {
    if (reset) {
      this.fastPolls = 0;
      this.unchangedPolls = 0;
      this.lastSignature = null;
      this.completed = false;
    }
    if (this.completed) {
      return;
    }
    this.started = true;
    this.clearTimer();
    if (!this.options.isEligible()) {
      this.started = false;
      return;
    }
    this.schedule(immediate ? 0 : this.nextDelay());
  }

  stop(): void {
    this.started = false;
    this.clearTimer();
  }

  // sync starts polling when the receipt becomes visible and stops when it
  // leaves the viewport. It intentionally does nothing while a timer is
  // already pending.
  sync(): void {
    if (this.completed) {
      return;
    }
    if (!this.options.isEligible()) {
      this.stop();
      return;
    }
    if (!this.started) {
      this.start(true, false);
    }
  }

  // refreshNow is used after a new message, a membership change, or a
  // reconnect. It also reopens a completed receipt because the denominator may
  // have changed.
  refreshNow(): void {
    this.completed = false;
    if (!this.options.isEligible()) {
      this.stop();
      return;
    }
    this.started = true;
    this.clearTimer();
    this.schedule(0);
  }

  private schedule(delayMs: number | null): void {
    if (delayMs === null) {
      this.complete();
      return;
    }
    this.clearTimer();
    this.timer = setTimeout(() => {
      this.timer = null;
      void this.tick();
    }, withJitter(delayMs, this.options.random));
  }

  private async tick(): Promise<void> {
    if (!this.started || this.completed || this.inFlight) {
      return;
    }
    if (!this.options.isEligible()) {
      this.stop();
      return;
    }

    this.inFlight = true;
    let stats: GroupReadStats | null = null;
    try {
      stats = await this.options.fetch();
    } catch (error) {
      this.options.onError?.(error);
    } finally {
      this.inFlight = false;
    }
    if (!this.started || this.completed) {
      return;
    }
    if (stats !== null) {
      this.observeStats(stats);
      if (stats.member_count <= 0 || stats.all_read) {
        this.complete();
        return;
      }
    }
    this.schedule(this.nextDelay());
  }

  private observeStats(stats: GroupReadStats): void {
    const signature = `${stats.read_count}/${stats.member_count}/${stats.all_read}`;
    if (this.lastSignature === null) {
      this.lastSignature = signature;
      return;
    }
    if (signature !== this.lastSignature) {
      this.fastPolls = GROUP_RECEIPT_FAST_POLLS;
      this.unchangedPolls = 0;
    } else {
      this.unchangedPolls += 1;
      if (this.fastPolls > 0) {
        this.fastPolls -= 1;
      }
    }
    this.lastSignature = signature;
    this.options.onStats?.(stats);
  }

  private nextDelay(): number | null {
    return groupReceiptDelay(
      this.options.getAgeMs(),
      this.fastPolls,
      this.unchangedPolls,
    );
  }

  private complete(): void {
    this.completed = true;
    this.started = false;
    this.clearTimer();
  }

  private clearTimer(): void {
    if (this.timer !== null) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  }
}
