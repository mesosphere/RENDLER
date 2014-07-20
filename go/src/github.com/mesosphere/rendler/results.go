package rendler

import (
	"encoding/json"
	"fmt"
)

// The type of crawl results
type CrawlResult struct {
	TaskID string
	URL    string
	Links  []string
}

func (target CrawlResult) FromJson(data []byte) CrawlResult {
	err := json.Unmarshal(data, &target)
	if err != nil {
		fmt.Printf("Error deserializing CrawlResult from JSON")
	}
	return target
}

func (source CrawlResult) ToJson() []byte {
	b, err := json.Marshal(source)
	if err != nil {
		fmt.Printf("Error serializing CrawlResult to JSON")
	}
	return b
}

// The type of render results
type RenderResult struct {
	TaskID   string
	URL      string
	ImageURL string
}

func (target RenderResult) FromJson(data []byte) RenderResult {
	err := json.Unmarshal(data, &target)
	if err != nil {
		fmt.Printf("Error deserializing RenderResult from JSON")
	}
	return target
}

func (source RenderResult) ToJson() []byte {
	b, err := json.Marshal(source)
	if err != nil {
		fmt.Printf("Error serializing RenderResult to JSON")
	}
	return b
}
