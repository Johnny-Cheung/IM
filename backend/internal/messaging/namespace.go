package messaging

import (
	"fmt"
	"strings"
)

const DefaultMessagingNamespace = "im:v12"

// Namespace derives the Redis and RabbitMQ names for one isolated messaging
// instance. The namespace is non-sensitive configuration.
type Namespace struct {
	base string
}

func NewNamespace(base string) (Namespace, error) {
	base = strings.TrimSuffix(strings.TrimSpace(base), ":")
	if base == "" {
		return Namespace{}, fmt.Errorf("messaging namespace is empty")
	}
	for _, character := range base {
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == ':' || character == '-' || character == '_' {
			continue
		}
		return Namespace{}, fmt.Errorf("messaging namespace contains invalid character")
	}
	return Namespace{base: base}, nil
}

func DefaultNamespace() Namespace {
	return Namespace{base: DefaultMessagingNamespace}
}

func (n Namespace) Base() string {
	if n.base == "" {
		return DefaultMessagingNamespace
	}
	return n.base
}

func (n Namespace) RedisPrefix() string {
	return n.Base() + ":"
}

func (n Namespace) RabbitPrefix() string {
	return strings.ReplaceAll(n.Base(), ":", ".")
}

func (n Namespace) ConsumerPrefix() string {
	return strings.ReplaceAll(n.Base(), ":", "-")
}

// Keyspace contains all Redis key construction for one messaging namespace.
type Keyspace struct {
	prefix string
}

func NewKeyspace(namespace Namespace) Keyspace {
	return Keyspace{prefix: namespace.RedisPrefix()}
}

func (k Keyspace) prefixOrDefault() string {
	if k.prefix == "" {
		return DefaultNamespace().RedisPrefix()
	}
	return k.prefix
}
