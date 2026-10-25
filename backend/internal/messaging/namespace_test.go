package messaging

import "testing"

func TestNamespaceDerivesIsolatedNames(t *testing.T) {
	namespace, err := NewNamespace("im:v13-verify")
	if err != nil {
		t.Fatalf("NewNamespace() error = %v", err)
	}
	if namespace.RedisPrefix() != "im:v13-verify:" {
		t.Fatalf("RedisPrefix() = %q", namespace.RedisPrefix())
	}
	if namespace.RabbitPrefix() != "im.v13-verify" {
		t.Fatalf("RabbitPrefix() = %q", namespace.RabbitPrefix())
	}
	if namespace.ConsumerPrefix() != "im-v13-verify" {
		t.Fatalf("ConsumerPrefix() = %q", namespace.ConsumerPrefix())
	}
	keyspace := NewKeyspace(namespace)
	if got := keyspace.groupMessageStreamKey(); got != "im:v13-verify:group-messages:stream" {
		t.Fatalf("group stream key = %q", got)
	}
}

func TestNamespaceRejectsUnsafeValues(t *testing.T) {
	for _, value := range []string{"", "im:v13 verify", "im:v13/*", ":"} {
		if _, err := NewNamespace(value); err == nil {
			t.Fatalf("NewNamespace(%q) error = nil", value)
		}
	}
}
