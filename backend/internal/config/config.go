package config

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

const (
	defaultMySQLHost = "127.0.0.1"
	defaultMySQLPort = "13307"
	defaultMySQLDB   = "im"
	defaultMySQLUser = "im_app"
)

// MySQLDSN returns the DSN used by the backend. IM_MYSQL_DSN takes precedence;
// otherwise connection parts are read from environment variables and the
// password is read from IM_MYSQL_PASSWORD_FILE.
func MySQLDSN() (string, error) {
	if dsn := strings.TrimSpace(os.Getenv("IM_MYSQL_DSN")); dsn != "" {
		return dsn, nil
	}

	passwordFile := strings.TrimSpace(os.Getenv("IM_MYSQL_PASSWORD_FILE"))
	if passwordFile == "" {
		return "", fmt.Errorf("IM_MYSQL_PASSWORD_FILE is required when IM_MYSQL_DSN is not set")
	}
	password, err := os.ReadFile(passwordFile)
	if err != nil {
		return "", fmt.Errorf("read MySQL password file: %w", err)
	}
	if strings.TrimSpace(string(password)) == "" {
		return "", fmt.Errorf("MySQL password file %s is empty", passwordFile)
	}

	cfg := mysql.NewConfig()
	cfg.User = envOrDefault("IM_MYSQL_USER", defaultMySQLUser)
	cfg.Passwd = strings.TrimSpace(string(password))
	cfg.Net = "tcp"
	cfg.Addr = net.JoinHostPort(
		envOrDefault("IM_MYSQL_HOST", defaultMySQLHost),
		envOrDefault("IM_MYSQL_PORT", defaultMySQLPort),
	)
	cfg.DBName = envOrDefault("IM_MYSQL_DB", defaultMySQLDB)
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	cfg.Timeout = 5 * time.Second
	cfg.ReadTimeout = 5 * time.Second
	cfg.WriteTimeout = 5 * time.Second
	cfg.Collation = "utf8mb4_0900_ai_ci"
	cfg.Params = map[string]string{"charset": "utf8mb4"}

	return cfg.FormatDSN(), nil
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
