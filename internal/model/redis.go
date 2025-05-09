package model

type ScrapperPayload struct {
	Url  string `json:"url"`
	Date string `json:"date"`
	Text string `json:"text"`
}

type DonePayload struct {
	Date     string `json:"date"`
	Count    int    `json:"count"`
	Duration string `json:"duration"`
}
