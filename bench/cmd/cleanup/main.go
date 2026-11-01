// cleanup 按用户名前缀或编号区间精确清理压测账号及其关联数据。
//
// 用法:
//
//	go run ./cmd/cleanup -dsn 'im_app:...@tcp(127.0.0.1:13308)/im' -prefix bench_
//	go run ./cmd/cleanup -dsn '...' -prefix bench -start 300000 -count 1460
//
// 直接连 MySQL 删除，不经 docker exec。删除顺序按外键依赖从叶子到根：
// 先删引用 users 的行，最后删 users 自己。
//
// 优先用 -start/-count 按编号区间清理：-prefix bench 会匹配到 bench_3、
// bench_30、bench_300…… 任何以 bench_ 开头的账号，若数据集里混有不同编号
// 段（例如 bench_0..199999 的正式数据 + 试跑留下的 bench_300000+），
// 前缀清理会把不该删的一起删掉。编号区间与 register/warmup 的 -start/-count
// 语义一致，清理时最精确。
//
// 注意不要用 group_concat 收集 id —— 它有 group_concat_max_len(默认 1024)
// 限制，上万条 id 会被静默截断。这里全部用子查询在 SQL 层完成。
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// selector 是"选哪些用户"的条件，以及用于展示的说明。
type selector struct {
	desc string // 展示用
	cond string // 不含 WHERE 的 SQL 条件
}

// byPrefix 选出用户名以 prefix 开头的账号。
func byPrefix(prefix string) selector {
	return selector{
		desc: fmt.Sprintf("前缀 %s", prefix),
		// LIKE 的特殊字符要转义，避免前缀里的 % 或 _ 扩大匹配范围。
		cond: fmt.Sprintf("username LIKE '%s%%'", escapeLike(prefix, '\\')),
	}
}

// byIndexRange 选出名为 prefix_<n> 且 n 落在 [start, start+count) 的账号。
// 编号从用户名尾部取下划线后的数字部分，因此 bench_1000000 不会被误选入
// bench_0..199999 区间（数值比较而非字符串比较）。
func byIndexRange(prefix string, start, count int) selector {
	idx := fmt.Sprintf("CAST(SUBSTRING_INDEX(username, '_', -1) AS UNSIGNED)")
	return selector{
		desc: fmt.Sprintf("前缀 %s，编号 %d..%d", prefix, start, start+count-1),
		cond: fmt.Sprintf("username LIKE '%s\\_%%' AND %s >= %d AND %s < %d",
			escapeLike(prefix, '\\'), idx, start, idx, start+count),
	}
}

// deleteStep 是一步删除：说明 + 用子查询限定范围的 SQL。
// %[1]s 会替换为 "username LIKE '<prefix>%'" 形式的用户筛选条件。
type deleteStep struct {
	desc string
	sql  string
}

// 删除顺序：先清引用 users 的行，最后删 users。遗漏任何一张都会留下悬挂数据。
var deleteSteps = []deleteStep{
	{"sessions", `DELETE FROM sessions WHERE user_id IN (SELECT id FROM users WHERE %[1]s)`},
	{"friendships", `DELETE FROM friendships
		WHERE user_id IN (SELECT id FROM users WHERE %[1]s)
		   OR friend_user_id IN (SELECT id FROM users WHERE %[1]s)`},
	{"friend_requests", `DELETE FROM friend_requests
		WHERE from_user_id IN (SELECT id FROM users WHERE %[1]s)
		   OR to_user_id IN (SELECT id FROM users WHERE %[1]s)`},
	{"conversation_reads", `DELETE FROM conversation_reads
		WHERE user_id IN (SELECT id FROM users WHERE %[1]s)
		   OR peer_user_id IN (SELECT id FROM users WHERE %[1]s)`},
	{"messages", `DELETE FROM messages WHERE sender_id IN (SELECT id FROM users WHERE %[1]s)`},
	{"group_members", `DELETE FROM group_members WHERE user_id IN (SELECT id FROM users WHERE %[1]s)`},
	{"group_messages", `DELETE FROM group_messages WHERE sender_id IN (SELECT id FROM users WHERE %[1]s)`},
	{"moments", `DELETE FROM moments WHERE author_id IN (SELECT id FROM users WHERE %[1]s)`},
	{"moment_likes", `DELETE FROM moment_likes WHERE user_id IN (SELECT id FROM users WHERE %[1]s)`},
	{"moment_comments", `DELETE FROM moment_comments WHERE author_id IN (SELECT id FROM users WHERE %[1]s)`},
	{"moment_seen", `DELETE FROM moment_seen WHERE user_id IN (SELECT id FROM users WHERE %[1]s)`},
	{"users", `DELETE FROM users WHERE %[1]s`},
}

func main() {
	var (
		dsn    = flag.String("dsn", "", "MySQL DSN（必填）")
		prefix = flag.String("prefix", "bench_", "用户名前缀，逗号分隔可多个")
		start  = flag.Int("start", 0, "起始编号；与 -count 一起启用编号区间选择")
		count  = flag.Int("count", 0, "编号个数；>0 时按 [start, start+count) 选择")
		dryRun = flag.Bool("dry-run", false, "只统计不删除")
	)
	flag.Parse()

	if strings.TrimSpace(*dsn) == "" {
		log.Fatalf("cleanup: -dsn 必填")
	}
	if *count < 0 || *start < 0 {
		log.Fatalf("cleanup: -start/-count 不能为负")
	}
	prefixes := []string{}
	for _, p := range strings.Split(*prefix, ",") {
		if p = strings.TrimSpace(p); p != "" {
			prefixes = append(prefixes, p)
		}
	}
	if len(prefixes) == 0 {
		log.Fatalf("cleanup: -prefix 不能为空")
	}

	// -count>0 时按编号区间选，否则按整前缀选。
	sels := make([]selector, 0, len(prefixes))
	for _, p := range prefixes {
		if *count > 0 {
			sels = append(sels, byIndexRange(p, *start, *count))
		} else {
			sels = append(sels, byPrefix(p))
		}
	}

	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		log.Fatalf("cleanup: 连接 MySQL: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		log.Fatalf("cleanup: ping MySQL: %v", err)
	}

	failed := false
	for _, sel := range sels {
		cond := sel.cond

		var matched int
		if err := db.QueryRow(
			"SELECT COUNT(*) FROM users WHERE "+cond,
		).Scan(&matched); err != nil {
			log.Fatalf("cleanup: 统计 %s: %v", sel.desc, err)
		}
		fmt.Printf("=== %s: 匹配 %d 个账号 ===\n", sel.desc, matched)
		if matched == 0 {
			continue
		}
		if *dryRun {
			continue
		}

		for _, step := range deleteSteps {
			q := fmt.Sprintf(step.sql, cond)
			res, err := db.Exec(q)
			if err != nil {
				fmt.Fprintf(os.Stderr, "  删除 %s 失败: %v\n", step.desc, err)
				failed = true
				break
			}
			if n, _ := res.RowsAffected(); n > 0 {
				fmt.Printf("  %-20s 删除 %d 行\n", step.desc, n)
			}
		}

		// 自校验：删不干净必须显式失败，否则会留下悬挂数据。
		var left int
		if err := db.QueryRow(
			"SELECT COUNT(*) FROM users WHERE "+cond,
		).Scan(&left); err != nil {
			log.Fatalf("cleanup: 复核 %s: %v", sel.desc, err)
		}
		if left != 0 {
			fmt.Fprintf(os.Stderr, "  警告: %s 仍有 %d 个账号未删除\n", sel.desc, left)
			failed = true
		} else {
			fmt.Printf("  剩余 0 ✅\n")
		}
	}

	var total int
	if err := db.QueryRow("SELECT COUNT(*) FROM users").Scan(&total); err != nil {
		log.Fatalf("cleanup: 统计总数: %v", err)
	}
	fmt.Printf("\n最终 users 总数: %d\n", total)
	if failed {
		os.Exit(1)
	}
}

// escapeLike 转义 LIKE 模式里的通配符，使前缀按字面匹配。
func escapeLike(s string, escape rune) string {
	r := strings.NewReplacer(
		string(escape), string(escape)+string(escape),
		"%", string(escape)+"%",
		"_", string(escape)+"_",
	)
	return r.Replace(s)
}