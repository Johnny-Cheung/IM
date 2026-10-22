package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/go-sql-driver/mysql"
)

func TestMySQLDSNFromPasswordFile(t *testing.T) {
	passwordFile := filepath.Join(t.TempDir(), "mysql-password")
	if err := os.WriteFile(passwordFile, []byte("test-password\n"), 0o600); err != nil {
		t.Fatalf("write password file: %v", err)
	}

	t.Setenv("IM_MYSQL_DSN", "")
	t.Setenv("IM_MYSQL_PASSWORD_FILE", passwordFile)
	t.Setenv("IM_MYSQL_HOST", "127.0.0.1")
	t.Setenv("IM_MYSQL_PORT", "13307")
	t.Setenv("IM_MYSQL_DB", "im")
	t.Setenv("IM_MYSQL_USER", "im_app")

	dsn, err := MySQLDSN()
	if err != nil {
		t.Fatalf("MySQLDSN() error = %v", err)
	}

	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	if cfg.User != "im_app" || cfg.Passwd != "test-password" {
		t.Fatalf("credentials = %q/%q, want im_app/test-password", cfg.User, cfg.Passwd)
	}
	if cfg.Addr != "127.0.0.1:13307" || cfg.DBName != "im" {
		t.Fatalf("address/database = %q/%q, want 127.0.0.1:13307/im", cfg.Addr, cfg.DBName)
	}
	if !cfg.ParseTime {
		t.Fatal("ParseTime = false, want true")
	}
	if cfg.Collation != "utf8mb4_0900_ai_ci" {
		t.Fatalf("Collation = %q, want utf8mb4_0900_ai_ci", cfg.Collation)
	}
}

func TestMySQLDSNPrefersExplicitDSN(t *testing.T) {
	const explicit = "user:pass@tcp(127.0.0.1:3306)/example?parseTime=true"
	t.Setenv("IM_MYSQL_DSN", explicit)
	t.Setenv("IM_MYSQL_PASSWORD_FILE", "/path/that/does/not/exist")

	dsn, err := MySQLDSN()
	if err != nil {
		t.Fatalf("MySQLDSN() error = %v", err)
	}
	if dsn != explicit {
		t.Fatalf("DSN = %q, want %q", dsn, explicit)
	}
}

func TestMySQLDSNRequiresPasswordFile(t *testing.T) {
	t.Setenv("IM_MYSQL_DSN", "")
	t.Setenv("IM_MYSQL_PASSWORD_FILE", "")

	if _, err := MySQLDSN(); err == nil {
		t.Fatal("MySQLDSN() error = nil, want password file error")
	}
}
