import type { MomentComment } from "./api";

export const MOMENT_BODY_LIMIT = 1000;
export const COMMENT_LIMIT = 500;

const dateTimeFormatter = new Intl.DateTimeFormat("zh-CN", {
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
});

export function formatMomentTime(value: string, now = Date.now()): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  const diff = now - date.getTime();
  if (diff >= 0 && diff < 60_000) {
    return "刚刚";
  }
  if (diff >= 0 && diff < 3_600_000) {
    return `${Math.floor(diff / 60_000)} 分钟前`;
  }
  if (diff >= 0 && diff < 86_400_000) {
    return `${Math.floor(diff / 3_600_000)} 小时前`;
  }
  return dateTimeFormatter.format(date);
}

export function runeCount(value: string): number {
  return [...value].length;
}

export function initialOf(name: string): string {
  return [...name][0] ?? "?";
}

export function mergeMomentComments(
  current: readonly MomentComment[],
  incoming: readonly MomentComment[],
): MomentComment[] {
  const seen = new Set(current.map((comment) => comment.id));
  const merged = [...current];
  for (const comment of incoming) {
    if (!seen.has(comment.id)) {
      seen.add(comment.id);
      merged.push(comment);
    }
  }
  return merged.sort((first, second) => first.id - second.id);
}
