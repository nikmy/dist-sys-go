package pkg

type addMsgBody struct {
	Type  string `json:"type,omitempty"`
	Delta int32  `json:"delta,omitempty"`
}

type readMsgBody struct {
	Type  string `json:"type,omitempty"`
	Value int32  `json:"value"`
}
