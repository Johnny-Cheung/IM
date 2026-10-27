export type DirectPendingRead = {
  kind: "direct";
  key: string;
  username: string;
  throughID: number;
};

export type GroupPendingRead = {
  kind: "group";
  key: string;
  groupID: number;
  throughID: number;
};

export type PendingReadTarget =
  | { kind: "direct"; key: string; username: string }
  | { kind: "group"; key: string; groupID: number };

export type PendingRead = DirectPendingRead | GroupPendingRead;

export type SerializedPendingReads = Record<string, PendingRead>;

export function enqueueRead(
  queue: Map<string, PendingRead>,
  target: PendingReadTarget,
  throughID: number,
): boolean {
  if (!Number.isSafeInteger(throughID) || throughID <= 0) {
    return false;
  }
  if (target.key === "") {
    return false;
  }
  if (target.kind === "direct" && target.username === "") {
    return false;
  }
  if (
    target.kind === "group"
    && (!Number.isSafeInteger(target.groupID) || target.groupID <= 0)
  ) {
    return false;
  }
  const existing = queue.get(target.key);
  if (existing !== undefined && existing.throughID >= throughID) {
    return false;
  }
  queue.set(target.key, { ...target, throughID });
  return true;
}

export function serializePendingReads(
  queue: Map<string, PendingRead>,
): SerializedPendingReads {
  const result: SerializedPendingReads = {};
  for (const [key, item] of queue) {
    result[key] = item;
  }
  return result;
}

export function deserializePendingReads(
  value: unknown,
): Map<string, PendingRead> {
  const result = new Map<string, PendingRead>();
  if (typeof value !== "object" || value === null) {
    return result;
  }
  for (const [key, raw] of Object.entries(value as Record<string, unknown>)) {
    if (typeof raw !== "object" || raw === null) {
      continue;
    }
    const record = raw as Record<string, unknown>;
    const throughID = record.throughID;
    if (
      typeof throughID !== "number"
      || !Number.isSafeInteger(throughID)
      || throughID <= 0
      || record.key !== key
    ) {
      continue;
    }
    if (
      record.kind === "direct"
      && typeof record.username === "string"
      && record.username !== ""
    ) {
      result.set(key, {
        kind: "direct",
        key,
        username: record.username,
        throughID,
      });
      continue;
    }
    if (
      record.kind === "group"
      && typeof record.groupID === "number"
      && Number.isSafeInteger(record.groupID)
      && record.groupID > 0
    ) {
      result.set(key, {
        kind: "group",
        key,
        groupID: record.groupID,
        throughID,
      });
    }
  }
  return result;
}

export function splitPendingReads(items: Iterable<PendingRead>): {
  direct: DirectPendingRead[];
  groups: GroupPendingRead[];
} {
  const direct: DirectPendingRead[] = [];
  const groups: GroupPendingRead[] = [];
  for (const item of items) {
    if (item.kind === "direct") {
      direct.push(item);
    } else {
      groups.push(item);
    }
  }
  return { direct, groups };
}
