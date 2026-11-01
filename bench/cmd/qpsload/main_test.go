package main

import (
	"os"
	"path/filepath"
	"testing"
)

// loadPairs 必须跳过空行、注释与残缺行，只留下合法的"发送方<TAB>接收方"。
func TestLoadPairsSkipsNoise(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pairs.txt")
	content := "0\t1\n" +
		"2\t3\n" +
		"\n" +
		"   \n" +
		"4 5\n" + // 空格分隔也算合法
		"6\n" + // 只有一列：跳过
		"x\ty\n" + // 非数字：跳过
		"8\t9\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	pairs, err := loadPairs(path)
	if err != nil {
		t.Fatalf("loadPairs: %v", err)
	}
	want := []pair{{0, 1}, {2, 3}, {4, 5}, {8, 9}}
	if len(pairs) != len(want) {
		t.Fatalf("得到 %d 对，想要 %d 对: %+v", len(pairs), len(want), pairs)
	}
	for i := range want {
		if pairs[i] != want[i] {
			t.Errorf("第 %d 对 = %+v，想要 %+v", i, pairs[i], want[i])
		}
	}
}

// 文件不存在必须显式报错，而不是静默返回空表（否则会"跑了个寂寞"）。
func TestLoadPairsMissingFile(t *testing.T) {
	if _, err := loadPairs(filepath.Join(t.TempDir(), "nope.txt")); err == nil {
		t.Fatal("文件不存在时应返回错误")
	}
}