package log

import (
	"context"
	"encoding/json"
)

type Entry struct {
	Valid   bool
	Offset  int
	Message int
}

func (e *Entry) UnmarshalJSON(bytes []byte) error {
	var row []int
	if err := json.Unmarshal(bytes, &row); err != nil {
		return err
	}
	e.Offset, e.Message = row[0], row[1]
	e.Valid = true
	return nil
}

func (e *Entry) MarshalJSON() ([]byte, error) {
	return json.Marshal([]int{e.Offset, e.Message})
}

type Storage interface {
	Send(ctx context.Context, key string, msg int) (int, error)
	Poll(ctx context.Context, key string, offset int) ([]Entry, error)
	Commit(ctx context.Context, key string, offset int) error
	GetCommittedOffset(ctx context.Context, key string) (int, error)
}
