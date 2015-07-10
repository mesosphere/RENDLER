package rendler

import (
	"encoding/json"
	"fmt"
)

// CrawlResult is the result of a crawl operation
type CrawlResult struct {
	TaskID string
	URL    string
	Links  []string
}

// FromJSON converts a JSON blob to a CrawlResult
func (c *CrawlResult) FromJSON(data []byte) error {
	err := json.Unmarshal(data, c)
	if err != nil {
		return fmt.Errorf("Error deserializing CrawlResult from JSON")
	}
	return nil
}

// ToJSON converts a CrawlResult to a JSON blob
func (c *CrawlResult) ToJSON() ([]byte, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("Error serializing CrawlResult to JSON")
	}
	return b, nil
}

// RenderResult is the result of a render operation
type RenderResult struct {
	TaskID   string
	URL      string
	ImageURL string
}

// FromJSON converts a JSON blob to a RenderResult
func (r *RenderResult) FromJSON(data []byte) error {
	err := json.Unmarshal(data, r)
	if err != nil {
		return fmt.Errorf("Error deserializing RenderResult from JSON")
	}
	return nil
}

// ToJSON converts a RenderResult to a JSON blob
func (r *RenderResult) ToJSON() ([]byte, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("Error serializing RenderResult to JSON")
	}
	return b, nil
}
