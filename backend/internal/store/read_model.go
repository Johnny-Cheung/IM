package store

import (
	"context"
	"fmt"
)

// DirectPairCount is the number of persisted messages for one direction.
type DirectPairCount struct {
	RecipientID uint64
	SenderID    uint64
	Count       uint64
}

// CountDirectMessagesByPair returns persisted message counts per direction.
func (s *Store) CountDirectMessagesByPair(ctx context.Context) ([]DirectPairCount, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT recipient_id, sender_id, COUNT(*)
		 FROM messages
		 GROUP BY recipient_id, sender_id`,
	)
	if err != nil {
		return nil, fmt.Errorf("count direct messages by pair: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make([]DirectPairCount, 0)
	for rows.Next() {
		var item DirectPairCount
		if err := rows.Scan(&item.RecipientID, &item.SenderID, &item.Count); err != nil {
			return nil, fmt.Errorf("scan direct message count: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate direct message counts: %w", err)
	}
	return result, nil
}
