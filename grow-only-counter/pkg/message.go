package pkg

type addMsgBody struct {
	Type  string `json:"type,omitempty"`
	Delta int    `json:"delta,omitempty"`
}

type readMsgBody struct {
	Type  string `json:"type,omitempty"`
	Value int    `json:"value"`
}
