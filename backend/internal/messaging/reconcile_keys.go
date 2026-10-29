package messaging

import (
	"strconv"
	"strings"
)

// reconcileDirectCounterScanPattern matches every private conversation counter
// in the keyspace. It feeds the reconciler's bounded SCAN for conversations
// that exist only in Redis.
func (k Keyspace) reconcileDirectCounterScanPattern() string {
	return k.prefixOrDefault() + "dm:seq:*"
}

// reconcileGroupCounterScanPattern matches every group conversation counter.
func (k Keyspace) reconcileGroupCounterScanPattern() string {
	return k.prefixOrDefault() + "group:*:seq"
}

// reconcileParseDirectCounterKey extracts the conversation pair from a
// dm:seq:{low}:{high} key.
func (k Keyspace) reconcileParseDirectCounterKey(key string) (uint64, uint64, bool) {
	rest, ok := strings.CutPrefix(key, k.prefixOrDefault()+"dm:seq:")
	if !ok {
		return 0, 0, false
	}
	lowRaw, highRaw, ok := strings.Cut(rest, ":")
	if !ok {
		return 0, 0, false
	}
	low, err := strconv.ParseUint(lowRaw, 10, 64)
	if err != nil || low == 0 {
		return 0, 0, false
	}
	high, err := strconv.ParseUint(highRaw, 10, 64)
	if err != nil || high == 0 {
		return 0, 0, false
	}
	return low, high, true
}

// reconcileParseGroupCounterKey extracts the group ID from a group:{G}:seq key.
func (k Keyspace) reconcileParseGroupCounterKey(key string) (uint64, bool) {
	rest, ok := strings.CutPrefix(key, k.prefixOrDefault()+"group:")
	if !ok {
		return 0, false
	}
	groupRaw, ok := strings.CutSuffix(rest, ":seq")
	if !ok || strings.Contains(groupRaw, ":") {
		return 0, false
	}
	groupID, err := strconv.ParseUint(groupRaw, 10, 64)
	if err != nil || groupID == 0 {
		return 0, false
	}
	return groupID, true
}
