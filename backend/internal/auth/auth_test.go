package auth

import (
	"strings"
	"testing"
)

func TestCleanUsername(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "valid", input: "alice_1", want: "alice_1"},
		{name: "surrounding spaces", input: "  Alice_1  ", want: "Alice_1"},
		{name: "too short", input: "ab", wantErr: true},
		{name: "too long", input: strings.Repeat("a", 21), wantErr: true},
		{name: "hyphen", input: "alice-1", wantErr: true},
		{name: "chinese", input: "爱丽丝", wantErr: true},
		{name: "empty", input: "   ", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CleanUsername(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("CleanUsername(%q) error = nil, want error", tt.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("CleanUsername(%q) error = %v", tt.input, err)
			}
			if got != tt.want {
				t.Fatalf("CleanUsername(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestUsernameKeyIsCaseInsensitive(t *testing.T) {
	if got, want := UsernameKey("  Alice_1 "), "alice_1"; got != want {
		t.Fatalf("UsernameKey() = %q, want %q", got, want)
	}
}

func TestCleanNickname(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "valid", input: "Alice", want: "Alice"},
		{name: "trim spaces", input: "  Alice  ", want: "Alice"},
		{name: "internal spaces", input: "Alice Smith", want: "Alice Smith"},
		{name: "emoji", input: "小明😀", want: "小明😀"},
		{name: "empty", input: "   ", wantErr: true},
		{name: "control character", input: "Alice\nBob", wantErr: true},
		{name: "too long", input: strings.Repeat("好", 21), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CleanNickname(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("CleanNickname(%q) error = nil, want error", tt.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("CleanNickname(%q) error = %v", tt.input, err)
			}
			if got != tt.want {
				t.Fatalf("CleanNickname(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestValidatePassword(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "minimum ascii", input: "12345678"},
		{name: "minimum unicode", input: "密码密码密码密码"},
		{name: "too short", input: "1234567", wantErr: true},
		{name: "too many bytes", input: strings.Repeat("密", 25), wantErr: true},
		{name: "exactly 72 bytes", input: strings.Repeat("a", 72)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidatePassword(tt.input); (err != nil) != tt.wantErr {
				t.Fatalf("ValidatePassword() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPasswordHashAndCheck(t *testing.T) {
	hash, err := HashPassword("correct horse battery staple")
	if err != nil {
		t.Fatalf("HashPassword() error = %v", err)
	}
	if !CheckPassword(hash, "correct horse battery staple") {
		t.Fatal("CheckPassword() = false for correct password")
	}
	if CheckPassword(hash, "wrong password") {
		t.Fatal("CheckPassword() = true for wrong password")
	}
}

func TestSessionTokensAreRandomAndHashed(t *testing.T) {
	tokenOne, hashOne, err := NewSessionToken()
	if err != nil {
		t.Fatalf("NewSessionToken() error = %v", err)
	}
	tokenTwo, hashTwo, err := NewSessionToken()
	if err != nil {
		t.Fatalf("NewSessionToken() error = %v", err)
	}
	if tokenOne == tokenTwo {
		t.Fatal("session tokens are identical")
	}
	if len(hashOne) != sha256Size || len(hashTwo) != sha256Size {
		t.Fatalf("hash lengths = %d/%d, want %d", len(hashOne), len(hashTwo), sha256Size)
	}
	if string(hashOne) != string(HashSessionToken(tokenOne)) {
		t.Fatal("stored hash does not match token hash")
	}
}

const sha256Size = 32
