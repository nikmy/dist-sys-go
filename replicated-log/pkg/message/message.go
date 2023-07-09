package kafka

type SendRequest struct {
	Type string `json:"type"`
	Key  string `json:"key"`
	Msg  int64  `json:"msg"`
}

type SendOk struct {
	Type   string `json:"type"`
	Offset int    `json:"offset"`
}

type PollRequest struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type PollOk struct {
	Type string                `json:"type"`
	Msgs map[string][]LogEntry `json:"msgs"`
}

type CommitOffsetsRequest struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type CommitOffsetsOk struct {
	Type string `json:"type"`
}

type ListCommittedOffsetsRequest struct {
	Type string   `json:"type"`
	Keys []string `json:"keys"`
}

type ListCommittedOffsetsOk struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

func NewSendOk(offset int) SendOk {
	return SendOk{Type: "send_ok", Offset: offset}
}

func NewPollOk(msgs map[string][]LogEntry) *PollOk {
	return &PollOk{Type: "poll_ok", Msgs: msgs}
}

func NewCommitOffsetsOk() CommitOffsetsOk {
	return CommitOffsetsOk{Type: "commit_offsets_ok"}
}

func NewListCommittedOffsetsOk(offsets map[string]int) ListCommittedOffsetsOk {
	return ListCommittedOffsetsOk{Type: "list_committed_offsets_ok", Offsets: offsets}
}
