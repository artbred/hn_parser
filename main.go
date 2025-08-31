package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/time/rate"
)

const (
	BaseURL              = "https://hacker-news.firebaseio.com/v0"
	DefaultMaxConcurrent = 100
	DefaultRPS           = 200
	DefaultTargetStories = 10000
	DefaultBatchSize     = 10
	BufferSize           = 20000
	CheckpointInterval   = 200
	ShutdownTimeout      = 30 * time.Second
	WriterBufferSize     = 256 * 1024 // 256KB buffer for writer
	MaxRetries           = 2
	RetryDelay           = 100 * time.Millisecond
)

// Buffer pools for memory optimization
var (
	bufferPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4096))
		},
	}
	storyPool = sync.Pool{
		New: func() interface{} {
			return &Story{}
		},
	}
)

// Story represents a HackerNews story
type Story struct {
	ID          int      `json:"id"`
	Title       string   `json:"title"`
	URL         string   `json:"url,omitempty"`
	Text        string   `json:"text,omitempty"`
	Type        string   `json:"type"`
	By          string   `json:"by"`
	Time        int64    `json:"time"`
	Score       int      `json:"score"`
	Descendants int      `json:"descendants"`
	Kids        []int    `json:"kids,omitempty"`
	Dead        bool     `json:"dead,omitempty"`
	Deleted     bool     `json:"deleted,omitempty"`
}

// Reset resets the story for reuse
func (s *Story) Reset() {
	s.ID = 0
	s.Title = ""
	s.URL = ""
	s.Text = ""
	s.Type = ""
	s.By = ""
	s.Time = 0
	s.Score = 0
	s.Descendants = 0
	s.Kids = s.Kids[:0]
	s.Dead = false
	s.Deleted = false
}

// Checkpoint represents the state for recovery with enhanced metadata
type Checkpoint struct {
	LastProcessedID    int       `json:"last_processed_id"`
	LastSuccessfulID   int       `json:"last_successful_id"`
	StoriesCount       int       `json:"stories_count"`
	ItemsChecked       int64     `json:"items_checked"`
	FailedRequests     int64     `json:"failed_requests"`
	Timestamp          time.Time `json:"timestamp"`
	OutputFile         string    `json:"output_file"`
	InProgress         bool      `json:"in_progress"`
	GracefulShutdown   bool      `json:"graceful_shutdown"`
	Checksum           string    `json:"checksum"`
}

// Statistics tracks download progress
type Statistics struct {
	StoriesDownloaded  int64
	ItemsChecked       int64
	FailedRequests     int64
	LastSuccessfulID   int32
	StartTime          time.Time
	CacheHits          int64
	CacheMisses        int64
}

func (s *Statistics) IncrementStories() {
	atomic.AddInt64(&s.StoriesDownloaded, 1)
}

func (s *Statistics) IncrementChecked() {
	atomic.AddInt64(&s.ItemsChecked, 1)
}

func (s *Statistics) IncrementFailed() {
	atomic.AddInt64(&s.FailedRequests, 1)
}

func (s *Statistics) IncrementCacheHit() {
	atomic.AddInt64(&s.CacheHits, 1)
}

func (s *Statistics) IncrementCacheMiss() {
	atomic.AddInt64(&s.CacheMisses, 1)
}

func (s *Statistics) SetLastSuccessfulID(id int) {
	atomic.StoreInt32(&s.LastSuccessfulID, int32(id))
}

func (s *Statistics) GetStats() (stories, checked, failed int64, lastID int32, elapsed time.Duration, cacheHits, cacheMisses int64) {
	stories = atomic.LoadInt64(&s.StoriesDownloaded)
	checked = atomic.LoadInt64(&s.ItemsChecked)
	failed = atomic.LoadInt64(&s.FailedRequests)
	lastID = atomic.LoadInt32(&s.LastSuccessfulID)
	elapsed = time.Since(s.StartTime)
	cacheHits = atomic.LoadInt64(&s.CacheHits)
	cacheMisses = atomic.LoadInt64(&s.CacheMisses)
	return
}

// InvalidItemCache for tracking invalid items
type InvalidItemCache struct {
	items map[int]time.Time
	mu    sync.RWMutex
	ttl   time.Duration
}

func NewInvalidItemCache(ttl time.Duration) *InvalidItemCache {
	cache := &InvalidItemCache{
		items: make(map[int]time.Time),
		ttl:   ttl,
	}
	go cache.cleanup()
	return cache
}

func (c *InvalidItemCache) Add(id int) {
	c.mu.Lock()
	c.items[id] = time.Now().Add(c.ttl)
	c.mu.Unlock()
}

func (c *InvalidItemCache) Contains(id int) bool {
	c.mu.RLock()
	expiry, exists := c.items[id]
	c.mu.RUnlock()
	
	if exists && time.Now().Before(expiry) {
		return true
	}
	return false
}

func (c *InvalidItemCache) cleanup() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	
	for range ticker.C {
		now := time.Now()
		c.mu.Lock()
		for id, expiry := range c.items {
			if now.After(expiry) {
				delete(c.items, id)
			}
		}
		c.mu.Unlock()
	}
}

// BatchProcessor for batch processing
type BatchProcessor struct {
	batchSize int
	items     []int
	mu        sync.Mutex
}

func NewBatchProcessor(size int) *BatchProcessor {
	return &BatchProcessor{
		batchSize: size,
		items:     make([]int, 0, size),
	}
}

func (b *BatchProcessor) Add(id int) []int {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	b.items = append(b.items, id)
	if len(b.items) >= b.batchSize {
		batch := make([]int, len(b.items))
		copy(batch, b.items)
		b.items = b.items[:0]
		return batch
	}
	return nil
}

func (b *BatchProcessor) Flush() []int {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	if len(b.items) > 0 {
		batch := make([]int, len(b.items))
		copy(batch, b.items)
		b.items = b.items[:0]
		return batch
	}
	return nil
}

// HNParser is the main parser struct
type HNParser struct {
	client             *http.Client
	limiter            *rate.Limiter
	maxConcurrent      int
	targetStories      int
	minScore           int
	minDescendants     int
	outputFile         string
	checkpointFile     string
	stats              *Statistics
	storyChan          chan *Story
	ctx                context.Context
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
	writerWg           sync.WaitGroup
	shutdownChan       chan struct{}
	isShuttingDown     int32
	lastCheckpointID   int32
	checkpointMutex    sync.Mutex
	invalidCache       *InvalidItemCache
	batchProcessor     *BatchProcessor
	adaptiveCheckpoint *AdaptiveCheckpoint
	stopAtID           int  // ID to stop at for incremental updates
	appendMode         bool // Whether to append to existing file
}

// AdaptiveCheckpoint adjusts checkpoint frequency based on throughput
type AdaptiveCheckpoint struct {
	baseInterval   int
	currentInterval int
	lastCheckpoint time.Time
	mu             sync.Mutex
}

func NewAdaptiveCheckpoint(baseInterval int) *AdaptiveCheckpoint {
	return &AdaptiveCheckpoint{
		baseInterval:    baseInterval,
		currentInterval: baseInterval,
		lastCheckpoint:  time.Now(),
	}
}

func (a *AdaptiveCheckpoint) ShouldCheckpoint(itemsProcessed int) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	
	if itemsProcessed >= a.currentInterval {
		elapsed := time.Since(a.lastCheckpoint)
		
		// Adjust interval based on throughput
		if elapsed < 10*time.Second {
			// High throughput, increase interval
			a.currentInterval = min(a.currentInterval*2, a.baseInterval*4)
		} else if elapsed > 30*time.Second {
			// Low throughput, decrease interval
			a.currentInterval = max(a.currentInterval/2, a.baseInterval/2)
		}
		
		a.lastCheckpoint = time.Now()
		return true
	}
	return false
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// GetLastIDFromJSONL reads the last story ID from a JSONL file
func GetLastIDFromJSONL(filePath string) (int, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var lastID int
	scanner := bufio.NewScanner(file)
	
	// Read through all lines to get the last valid story
	for scanner.Scan() {
		var story Story
		if err := json.Unmarshal(scanner.Bytes(), &story); err == nil {
			if story.ID > 0 {
				lastID = story.ID
			}
		}
	}
	
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	
	if lastID == 0 {
		return 0, fmt.Errorf("no valid stories found in file")
	}
	
	return lastID, nil
}

// GetLastIDFromURL fetches a JSONL file from URL and gets the last story ID
func GetLastIDFromURL(url string) (int, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	
	resp, err := client.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HTTP error: %d", resp.StatusCode)
	}
	
	var lastID int
	scanner := bufio.NewScanner(resp.Body)
	
	// Read through all lines to get the last valid story
	for scanner.Scan() {
		var story Story
		if err := json.Unmarshal(scanner.Bytes(), &story); err == nil {
			if story.ID > 0 {
				lastID = story.ID
			}
		}
	}
	
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	
	if lastID == 0 {
		return 0, fmt.Errorf("no valid stories found in URL response")
	}
	
	return lastID, nil
}

// NewHNParser creates a new parser instance
func NewHNParser(maxConcurrent, rps, targetStories, minScore, minDescendants int, outputFile string, stopAtID int, appendMode bool) *HNParser {
	ctx, cancel := context.WithCancel(context.Background())
	
	// Optimized transport configuration
	transport := &http.Transport{
		MaxIdleConns:        maxConcurrent * 4, // Increased for better reuse
		MaxIdleConnsPerHost: maxConcurrent * 2,
		MaxConnsPerHost:     maxConcurrent * 2,
		IdleConnTimeout:     120 * time.Second,
		DisableKeepAlives:   false,
		ForceAttemptHTTP2:   true,
		TLSHandshakeTimeout: 10 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    true, // Disable compression for small JSON responses
	}
	
	return &HNParser{
		client: &http.Client{
			Timeout:   30 * time.Second,
			Transport: transport,
		},
		limiter:            rate.NewLimiter(rate.Limit(rps), maxConcurrent),
		maxConcurrent:      maxConcurrent,
		targetStories:      targetStories,
		minScore:           minScore,
		minDescendants:     minDescendants,
		outputFile:         outputFile,
		checkpointFile:     outputFile + ".checkpoint",
		stats:              &Statistics{StartTime: time.Now()},
		storyChan:          make(chan *Story, BufferSize),
		ctx:                ctx,
		cancel:             cancel,
		shutdownChan:       make(chan struct{}),
		invalidCache:       NewInvalidItemCache(5 * time.Minute),
		batchProcessor:     NewBatchProcessor(DefaultBatchSize),
		adaptiveCheckpoint: NewAdaptiveCheckpoint(CheckpointInterval),
		stopAtID:           stopAtID,
		appendMode:         appendMode,
	}
}

// GetMaxItemID fetches the latest item ID from HN with context
func (p *HNParser) GetMaxItemID() (int, error) {
	if err := p.limiter.Wait(p.ctx); err != nil {
		return 0, err
	}

	req, err := http.NewRequestWithContext(p.ctx, "GET", fmt.Sprintf("%s/maxitem.json", BaseURL), nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Connection", "keep-alive")

	resp, err := p.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() {
		io.Copy(io.Discard, resp.Body) // Ensure body is fully read
		resp.Body.Close()
	}()

	// Use buffer pool for reading response
	buf := bufferPool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		bufferPool.Put(buf)
	}()

	_, err = io.Copy(buf, resp.Body)
	if err != nil {
		return 0, err
	}

	var maxID int
	if err := json.Unmarshal(buf.Bytes(), &maxID); err != nil {
		return 0, err
	}

	return maxID, nil
}

// FetchItem fetches a single item from HN API with optimizations
func (p *HNParser) FetchItem(itemID int) (*Story, error) {
	// Check if we're shutting down
	if atomic.LoadInt32(&p.isShuttingDown) == 1 {
		return nil, fmt.Errorf("shutting down")
	}

	// Check invalid cache first
	if p.invalidCache.Contains(itemID) {
		p.stats.IncrementCacheHit()
		return nil, nil
	}
	p.stats.IncrementCacheMiss()

	if err := p.limiter.Wait(p.ctx); err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(p.ctx, "GET", fmt.Sprintf("%s/item/%d.json", BaseURL, itemID), nil)
	if err != nil {
		p.stats.IncrementFailed()
		return nil, err
	}
	req.Header.Set("Connection", "keep-alive")

	resp, err := p.client.Do(req)
	if err != nil {
		p.stats.IncrementFailed()
		return nil, err
	}
	defer func() {
		io.Copy(io.Discard, resp.Body) // Ensure body is fully read for connection reuse
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		p.stats.IncrementFailed()
		return nil, fmt.Errorf("status code: %d", resp.StatusCode)
	}

	// Use buffer pool for reading response
	buf := bufferPool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		bufferPool.Put(buf)
	}()

	_, err = io.Copy(buf, resp.Body)
	if err != nil {
		p.stats.IncrementFailed()
		return nil, err
	}

	// Get story from pool
	story := storyPool.Get().(*Story)
	story.Reset()

	if err := json.Unmarshal(buf.Bytes(), story); err != nil {
		storyPool.Put(story)
		p.stats.IncrementFailed()
		return nil, err
	}

	p.stats.IncrementChecked()

	// Filter for valid stories
	if story.Type != "story" || story.Deleted || story.Dead {
		p.invalidCache.Add(itemID)
		storyPool.Put(story)
		return nil, nil
	}

	// Apply filters
	if p.minScore > 0 && story.Score < p.minScore {
		p.invalidCache.Add(itemID)
		storyPool.Put(story)
		return nil, nil
	}

	if p.minDescendants > 0 && story.Descendants < p.minDescendants {
		p.invalidCache.Add(itemID)
		storyPool.Put(story)
		return nil, nil
	}

	// Update last successful ID
	p.stats.SetLastSuccessfulID(story.ID)

	return story, nil
}

// BatchWorker processes batches of item IDs
func (p *HNParser) BatchWorker(batchChan <-chan []int) {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			log.Printf("Batch worker shutting down due to context cancellation")
			return
		case batch, ok := <-batchChan:
			if !ok {
				return
			}

			for _, itemID := range batch {
				if atomic.LoadInt32(&p.isShuttingDown) == 1 {
					return
				}

				story, err := p.FetchItem(itemID)
				if err != nil {
					if err.Error() != "shutting down" {
						// Retry logic for transient failures
						time.Sleep(RetryDelay)
						story, err = p.FetchItem(itemID)
						if err != nil {
							log.Printf("Error fetching item %d after retry: %v", itemID, err)
						}
					}
					continue
				}

				if story != nil {
					p.stats.IncrementStories()
					select {
					case p.storyChan <- story:
					case <-p.ctx.Done():
						storyPool.Put(story)
						return
					}
				}
			}
		}
	}
}

// JSONLWriter writes stories to JSONL file with optimized buffering
func (p *HNParser) JSONLWriter() {
	defer p.writerWg.Done()

	flags := os.O_CREATE | os.O_WRONLY
	if p.appendMode {
		flags |= os.O_APPEND
		log.Printf("Opening file in append mode: %s", p.outputFile)
	} else {
		flags |= os.O_TRUNC
		log.Printf("Opening file in truncate mode: %s", p.outputFile)
	}
	
	file, err := os.OpenFile(p.outputFile, flags, 0644)
	if err != nil {
		log.Fatalf("Failed to open output file: %v", err)
	}
	defer file.Close()

	writer := bufio.NewWriterSize(file, WriterBufferSize)
	defer func() {
		writer.Flush()
		log.Println("Writer buffer flushed")
	}()

	encoder := json.NewEncoder(writer)
	checkpointCounter := 0
	ticker := time.NewTicker(15 * time.Second) // Less frequent periodic flush
	defer ticker.Stop()

	storyBuffer := make([]*Story, 0, 100)

	for {
		select {
		case <-p.ctx.Done():
			// Flush remaining stories before exiting
			for _, s := range storyBuffer {
				encoder.Encode(s)
				storyPool.Put(s)
			}
			for {
				select {
				case story := <-p.storyChan:
					encoder.Encode(story)
					storyPool.Put(story)
				default:
					writer.Flush()
					return
				}
			}
		case <-ticker.C:
			if len(storyBuffer) > 0 {
				for _, s := range storyBuffer {
					encoder.Encode(s)
					storyPool.Put(s)
				}
				storyBuffer = storyBuffer[:0]
				writer.Flush()
			}
		case story, ok := <-p.storyChan:
			if !ok {
				for _, s := range storyBuffer {
					encoder.Encode(s)
					storyPool.Put(s)
				}
				return
			}

			storyBuffer = append(storyBuffer, story)
			
			// Write batch when buffer is full
			if len(storyBuffer) >= 50 {
				for _, s := range storyBuffer {
					if err := encoder.Encode(s); err != nil {
						log.Printf("Failed to write story %d: %v", s.ID, err)
					}
					storyPool.Put(s)
				}
				storyBuffer = storyBuffer[:0]
				
				checkpointCounter += 50
				if p.adaptiveCheckpoint.ShouldCheckpoint(checkpointCounter) {
					writer.Flush()
					if err := p.SaveCheckpoint(story.ID, false); err != nil {
						log.Printf("Failed to save checkpoint: %v", err)
					}
					checkpointCounter = 0
				}
			}
		}
	}
}

// SaveCheckpoint saves current progress for recovery with atomic write
func (p *HNParser) SaveCheckpoint(lastID int, gracefulShutdown bool) error {
	p.checkpointMutex.Lock()
	defer p.checkpointMutex.Unlock()

	stories, checked, failed, lastSuccessID, _, cacheHits, cacheMisses := p.stats.GetStats()
	
	checkpoint := Checkpoint{
		LastProcessedID:    lastID,
		LastSuccessfulID:   int(lastSuccessID),
		StoriesCount:       int(stories),
		ItemsChecked:       checked,
		FailedRequests:     failed,
		Timestamp:          time.Now(),
		OutputFile:         p.outputFile,
		InProgress:         !gracefulShutdown,
		GracefulShutdown:   gracefulShutdown,
	}

	// Calculate checksum
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	checkpoint.Checksum = fmt.Sprintf("%x", md5.Sum(data))
	
	// Marshal with checksum
	data, err = json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return err
	}

	// Atomic write: write to temp file then rename
	tempFile := p.checkpointFile + ".tmp"
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return err
	}

	// Atomic rename
	if err := os.Rename(tempFile, p.checkpointFile); err != nil {
		os.Remove(tempFile) // Clean up on error
		return err
	}

	atomic.StoreInt32(&p.lastCheckpointID, int32(lastID))
	
	// Log cache statistics
	if cacheHits+cacheMisses > 0 {
		cacheHitRate := float64(cacheHits) / float64(cacheHits+cacheMisses) * 100
		log.Printf("Cache hit rate: %.2f%% (hits: %d, misses: %d)", cacheHitRate, cacheHits, cacheMisses)
	}
	
	return nil
}

// LoadCheckpoint loads previous progress for recovery with validation
func (p *HNParser) LoadCheckpoint() (*Checkpoint, error) {
	data, err := os.ReadFile(p.checkpointFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var checkpoint Checkpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil, fmt.Errorf("invalid checkpoint format: %v", err)
	}

	// Validate checksum
	checksumCopy := checkpoint.Checksum
	checkpoint.Checksum = ""
	validateData, _ := json.Marshal(checkpoint)
	expectedChecksum := fmt.Sprintf("%x", md5.Sum(validateData))
	
	if checksumCopy != expectedChecksum && checksumCopy != "" {
		log.Printf("Warning: Checkpoint checksum mismatch, may be corrupted")
	}

	// Check for incomplete shutdown
	if checkpoint.InProgress && !checkpoint.GracefulShutdown {
		log.Printf("Warning: Previous session did not shut down gracefully")
		// Adjust to last successful ID if available
		if checkpoint.LastSuccessfulID > 0 {
			checkpoint.LastProcessedID = checkpoint.LastSuccessfulID
			log.Printf("Adjusting to last successful ID: %d", checkpoint.LastSuccessfulID)
		}
	}

	return &checkpoint, nil
}

// GracefulShutdown performs a graceful shutdown with timeout
func (p *HNParser) GracefulShutdown() {
	atomic.StoreInt32(&p.isShuttingDown, 1)
	log.Println("Initiating graceful shutdown...")

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), ShutdownTimeout)
	defer shutdownCancel()

	// Signal all workers to stop
	p.cancel()

	// Wait for workers with timeout
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All workers stopped gracefully")
	case <-shutdownCtx.Done():
		log.Println("Shutdown timeout exceeded, forcing exit")
	}

	// Save final checkpoint
	lastID := atomic.LoadInt32(&p.lastCheckpointID)
	if err := p.SaveCheckpoint(int(lastID), true); err != nil {
		log.Printf("Failed to save final checkpoint: %v", err)
	} else {
		log.Printf("Final checkpoint saved at ID: %d", lastID)
	}

	close(p.shutdownChan)
}

// PrintProgress prints current download progress
func (p *HNParser) PrintProgress() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			stories, checked, failed, lastID, elapsed, cacheHits, cacheMisses := p.stats.GetStats()
			storiesPerSec := float64(stories) / elapsed.Seconds()
			cacheHitRate := float64(0)
			if cacheHits+cacheMisses > 0 {
				cacheHitRate = float64(cacheHits) / float64(cacheHits+cacheMisses) * 100
			}
			
			progressMsg := fmt.Sprintf("Progress: %d", stories)
			if p.targetStories > 0 {
				progressMsg = fmt.Sprintf("Progress: %d/%d stories", stories, p.targetStories)
			}
			
			log.Printf("%s | Checked: %d | Failed: %d | Last ID: %d | Speed: %.2f stories/sec | Cache: %.1f%%",
				progressMsg, checked, failed, lastID, storiesPerSec, cacheHitRate)
				
			if p.stopAtID > 0 {
				log.Printf("Will stop at ID: %d", p.stopAtID)
			}
		case <-p.ctx.Done():
			return
		}
	}
}

// Run starts the parser with enhanced recovery and batch processing
func (p *HNParser) Run() error {
	// Setup signal handler for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("Received signal: %v", sig)
		p.GracefulShutdown()
	}()

	// Check for checkpoint
	checkpoint, err := p.LoadCheckpoint()
	if err != nil {
		log.Printf("Warning: Failed to load checkpoint: %v", err)
	}

	startID := 0
	if checkpoint != nil {
		startID = checkpoint.LastProcessedID
		atomic.StoreInt64(&p.stats.StoriesDownloaded, int64(checkpoint.StoriesCount))
		atomic.StoreInt64(&p.stats.ItemsChecked, checkpoint.ItemsChecked)
		atomic.StoreInt64(&p.stats.FailedRequests, checkpoint.FailedRequests)
		log.Printf("Resuming from checkpoint: ID=%d, Stories=%d, Graceful=%v", 
			startID, checkpoint.StoriesCount, checkpoint.GracefulShutdown)
	}

	// Get max item ID if starting fresh
	if startID == 0 {
		maxID, err := p.GetMaxItemID()
		if err != nil {
			return fmt.Errorf("failed to get max item ID: %v", err)
		}
		startID = maxID
		log.Printf("Starting from max item ID: %d", maxID)
	}

	// Mark session as in progress
	if err := p.SaveCheckpoint(startID, false); err != nil {
		log.Printf("Warning: Failed to save initial checkpoint: %v", err)
	}

	// Start JSONL writer
	p.writerWg.Add(1)
	go p.JSONLWriter()

	// Start progress printer
	go p.PrintProgress()

	// Create batch channel
	batchChan := make(chan []int, p.maxConcurrent)

	// Start batch workers
	for i := 0; i < p.maxConcurrent; i++ {
		p.wg.Add(1)
		go p.BatchWorker(batchChan)
	}

	// Feed work to workers in batches
	currentID := startID
	feedLoop:
	for (p.targetStories <= 0 || atomic.LoadInt64(&p.stats.StoriesDownloaded) < int64(p.targetStories)) && currentID > 0 {
		// Check if we've reached the stop ID for incremental updates
		if p.stopAtID > 0 && currentID <= p.stopAtID {
			log.Printf("Reached stop ID: %d, finishing incremental update", p.stopAtID)
			break feedLoop
		}
		
		select {
		case <-p.shutdownChan:
			log.Println("Stopping work distribution due to shutdown")
			break feedLoop
		default:
			// Check if this ID is in invalid cache
			if p.invalidCache.Contains(currentID) {
				currentID--
				continue
			}
			
			// Add to batch
			if batch := p.batchProcessor.Add(currentID); batch != nil {
				select {
				case batchChan <- batch:
				case <-p.shutdownChan:
					break feedLoop
				}
			}
			
			currentID--
			atomic.StoreInt32(&p.lastCheckpointID, int32(currentID))
		}
	}

	// Flush remaining batch
	if batch := p.batchProcessor.Flush(); batch != nil {
		select {
		case batchChan <- batch:
		case <-time.After(5 * time.Second):
			log.Println("Timeout sending final batch")
		}
	}

	// Cleanup
	close(batchChan)
	p.wg.Wait()
	close(p.storyChan)
	p.writerWg.Wait()

	// Check if shutdown was requested
	if atomic.LoadInt32(&p.isShuttingDown) == 1 {
		log.Println("Shutdown completed")
		return nil
	}

	// Final checkpoint
	if err := p.SaveCheckpoint(currentID, true); err != nil {
		log.Printf("Warning: Failed to save final checkpoint: %v", err)
	}

	// Print final stats
	stories, checked, failed, lastID, elapsed, cacheHits, cacheMisses := p.stats.GetStats()
	cacheHitRate := float64(0)
	if cacheHits+cacheMisses > 0 {
		cacheHitRate = float64(cacheHits) / float64(cacheHits+cacheMisses) * 100
	}
	
	log.Printf("\nDownload completed!")
	log.Printf("Stories downloaded: %d", stories)
	log.Printf("Items checked: %d", checked)
	log.Printf("Failed requests: %d", failed)
	log.Printf("Last successful ID: %d", lastID)
	log.Printf("Total time: %v", elapsed)
	log.Printf("Average speed: %.2f stories/second", float64(stories)/elapsed.Seconds())
	log.Printf("Cache hit rate: %.2f%% (hits: %d, misses: %d)", cacheHitRate, cacheHits, cacheMisses)

	// Clean checkpoint on successful completion
	if p.targetStories > 0 && stories >= int64(p.targetStories) {
		if err := os.Remove(p.checkpointFile); err != nil {
			log.Printf("Warning: Failed to remove checkpoint file: %v", err)
		}
	}

	return nil
}

func main() {
	var (
		// Primary parameters
		outputFile     = flag.String("output", "", "Output JSONL file path")
		targetStories  = flag.Int("stories", 0, "Number of stories to download (0 = incremental update)")
		updateFromFile = flag.String("update-from-file", "", "Update incrementally from existing JSONL file")
		updateFromURL  = flag.String("update-from-url", "", "Update incrementally from JSONL file at URL")
		resumeID       = flag.Int("resume-id", 0, "Resume from specific ID (overrides checkpoint)")
		
		// Settings (at the end)
		maxConcurrent  = flag.Int("concurrent", DefaultMaxConcurrent, "Max concurrent requests")
		rps            = flag.Int("rps", DefaultRPS, "Requests per second limit")
		minScore       = flag.Int("min-score", 0, "Minimum score filter (0 = no filter)")
		minDescendants = flag.Int("min-descendants", 0, "Minimum descendants filter (0 = no filter)")
	)

	flag.Parse()

	// Determine mode and stop ID for incremental updates
	var stopAtID int
	var appendMode bool
	
	if *updateFromFile != "" {
		// Incremental update from local file
		lastID, err := GetLastIDFromJSONL(*updateFromFile)
		if err != nil {
			log.Fatalf("Failed to read last ID from file: %v", err)
		}
		stopAtID = lastID
		appendMode = true
		if *outputFile == "" {
			*outputFile = *updateFromFile
		}
		log.Printf("Incremental update mode: updating from ID %d", lastID)
	} else if *updateFromURL != "" {
		// Incremental update from URL
		lastID, err := GetLastIDFromURL(*updateFromURL)
		if err != nil {
			log.Fatalf("Failed to read last ID from URL: %v", err)
		}
		stopAtID = lastID
		appendMode = true
		log.Printf("Incremental update mode: updating from ID %d (from URL)", lastID)
	}

	// Generate output filename if not provided
	if *outputFile == "" {
		timestamp := time.Now().Format("20060102_150405")
		if *targetStories > 0 {
			*outputFile = filepath.Join("data", fmt.Sprintf("hn_stories_%d_%s.jsonl", *targetStories, timestamp))
		} else {
			*outputFile = filepath.Join("data", fmt.Sprintf("hn_stories_update_%s.jsonl", timestamp))
		}
	}

	// Ensure output directory exists
	outputDir := filepath.Dir(*outputFile)
	if outputDir != "" && outputDir != "." {
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			log.Fatalf("Failed to create output directory: %v", err)
		}
	}

	// Log configuration
	log.Printf("=== HackerNews Parser (Optimized Version) ===")
	
	if stopAtID > 0 {
		log.Printf("Mode: Incremental Update")
		log.Printf("Will fetch new stories until ID: %d", stopAtID)
		if appendMode {
			log.Printf("Appending to: %s", *outputFile)
		}
	} else if *targetStories > 0 {
		log.Printf("Mode: Fetch Fixed Number")
		log.Printf("Target: %d stories", *targetStories)
	} else {
		log.Printf("Mode: Continuous")
		log.Printf("Will fetch all available stories")
	}
	
	log.Printf("Output: %s", *outputFile)
	log.Printf("Checkpoint: %s.checkpoint", *outputFile)
	log.Printf("Concurrency: %d workers", *maxConcurrent)
	log.Printf("Rate limit: %d req/s", *rps)
	log.Printf("Batch size: %d", DefaultBatchSize)
	log.Printf("Writer buffer: %d KB", WriterBufferSize/1024)
	
	if *minScore > 0 {
		log.Printf("Min score filter: %d", *minScore)
	}
	if *minDescendants > 0 {
		log.Printf("Min descendants filter: %d", *minDescendants)
	}
	if *resumeID > 0 {
		log.Printf("Manual resume from ID: %d", *resumeID)
	}

	parser := NewHNParser(*maxConcurrent, *rps, *targetStories, *minScore, *minDescendants, *outputFile, stopAtID, appendMode)
	
	// Override with manual resume ID if provided
	if *resumeID > 0 {
		parser.SaveCheckpoint(*resumeID, false)
	}

	if err := parser.Run(); err != nil {
		log.Fatalf("Parser error: %v", err)
	}
}