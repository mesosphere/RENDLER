package rendler

// CrawlResult is the result of a crawl operation
type CrawlResult struct {
	TaskID string
	URL    string
	Links  []string
}

// RenderResult is the result of a render operation
type RenderResult struct {
	TaskID   string
	URL      string
	ImageURL string
}
