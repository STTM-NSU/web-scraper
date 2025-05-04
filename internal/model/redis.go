package model

const (
	RedisChanelName = "scrapper"
	PartitionsCount = 5
)

type ScrapperPayload struct {
	Date string `json:"date"`
	Text string `json:"text"`
}
