import { describe, expect, it } from "vitest";
import {
  formatMomentTime,
  mergeMomentComments,
  runeCount,
} from "./moments";
import type { MomentComment } from "./api";

function comment(id: number, body = `comment-${id}`): MomentComment {
  return {
    id,
    author: { username: "alice", nickname: "Alice" },
    body,
    created_at: "2026-10-30T10:00:00Z",
  };
}

describe("moments helpers", () => {
  it("counts characters instead of UTF-16 units", () => {
    expect(runeCount("你好")).toBe(2);
    expect(runeCount("👍🏽")).toBe(2);
    expect(runeCount("")).toBe(0);
  });

  it("formats relative times", () => {
    const now = Date.parse("2026-10-30T10:00:00Z");
    expect(formatMomentTime("2026-10-30T09:59:30Z", now)).toBe("刚刚");
    expect(formatMomentTime("2026-10-30T09:55:00Z", now)).toBe("5 分钟前");
    expect(formatMomentTime("2026-10-30T07:00:00Z", now)).toBe("3 小时前");
  });

  it("merges comment pages without duplicates and keeps ascending order", () => {
    const merged = mergeMomentComments(
      [comment(4), comment(5)],
      [comment(2), comment(4), comment(6)],
    );
    expect(merged.map((item) => item.id)).toEqual([2, 4, 5, 6]);
    expect(merged[0].body).toBe("comment-2");
  });
});
