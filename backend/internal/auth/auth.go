package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"golang.org/x/crypto/bcrypt"
)

const (
	MinUsernameLength = 3
	MaxUsernameLength = 20
	MaxNicknameRunes  = 20
	MinPasswordRunes  = 8
	MaxPasswordBytes  = 72
	SessionTokenBytes = 32
	PasswordHashCost  = 10

	SessionTTL = 7 * 24 * time.Hour
)

var (
	ErrInvalidUsername = errors.New("invalid username")
	ErrInvalidNickname = errors.New("invalid nickname")
	ErrInvalidPassword = errors.New("invalid password")
)

var usernamePattern = regexp.MustCompile(`^[A-Za-z0-9_]{3,20}$`)

// CleanUsername trims surrounding space and validates the username format.
func CleanUsername(username string) (string, error) {
	username = strings.TrimSpace(username)
	if !usernamePattern.MatchString(username) {
		return "", ErrInvalidUsername
	}
	return username, nil
}

// UsernameKey returns the case-insensitive key used for uniqueness and login.
func UsernameKey(username string) string {
	return strings.ToLower(strings.TrimSpace(username))
}

// CleanNickname trims surrounding space, rejects control characters, and
// limits the nickname to 20 Unicode characters.
func CleanNickname(nickname string) (string, error) {
	nickname = strings.TrimSpace(nickname)
	if nickname == "" || utf8.RuneCountInString(nickname) > MaxNicknameRunes {
		return "", ErrInvalidNickname
	}
	for _, r := range nickname {
		if unicode.IsControl(r) {
			return "", ErrInvalidNickname
		}
	}
	return nickname, nil
}

// ValidatePassword requires at least 8 Unicode characters and at most 72
// UTF-8 bytes, matching bcrypt's input limit.
func ValidatePassword(password string) error {
	if utf8.RuneCountInString(password) < MinPasswordRunes || len(password) > MaxPasswordBytes {
		return ErrInvalidPassword
	}
	return nil
}

// HashPassword hashes a plaintext password with bcrypt.
func HashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), PasswordHashCost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

// CheckPassword reports whether password matches the stored bcrypt hash.
func CheckPassword(hash, password string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
}

// NewSessionToken creates a random session token and its SHA-256 hash. The
// token is returned to the browser; only the hash is stored.
func NewSessionToken() (token string, hash []byte, err error) {
	raw := make([]byte, SessionTokenBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", nil, err
	}
	token = base64.RawURLEncoding.EncodeToString(raw)
	return token, HashSessionToken(token), nil
}

// HashSessionToken returns the SHA-256 hash stored for a session token.
func HashSessionToken(token string) []byte {
	sum := sha256.Sum256([]byte(token))
	return sum[:]
}

var dummyPasswordHash = func() []byte {
	hash, err := bcrypt.GenerateFromPassword([]byte("im-dummy-password"), PasswordHashCost)
	if err != nil {
		return nil
	}
	return hash
}()

// SpendDummyPasswordCheck performs a bcrypt comparison against a fixed hash
// so unknown usernames take roughly the same time as wrong passwords.
func SpendDummyPasswordCheck(password string) {
	if len(dummyPasswordHash) == 0 {
		return
	}
	_ = bcrypt.CompareHashAndPassword(dummyPasswordHash, []byte(password))
}
