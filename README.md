# HackerNews Story Parser

A high-performance, concurrent HackerNews story scraper written in Go. This tool efficiently downloads story data from the HackerNews API with support for incremental updates, checkpointing, and various filtering options.

# Dataset with 450k+ HN stories

https://huggingface.co/datasets/artbred/hn_stories

## Installation

### Prerequisites
- Go 1.21 or higher

### Build from source
```bash
git clone https://github.com/artbred/hn_parser.git
cd hn_parser
go mod download
go build -o hn_parser main.go
```

## Usage

### Basic Commands

#### Download a specific number of stories
```bash
./hn_parser -stories 10000 -output stories.jsonl
```

#### Incremental update from existing file
```bash
# This will append new stories to the existing file
./hn_parser -update-from-file stories.jsonl
```

#### Incremental update from URL
```bash
# Useful for cloud deployments
./hn_parser -update-from-url https://example.com/stories.jsonl -output updated_stories.jsonl
```

#### Apply filters
```bash
# Only fetch stories with score >= 100 and >= 50 comments
./hn_parser -stories 5000 -min-score 100 -min-descendants 50 -output filtered_stories.jsonl
```

### Command-Line Options

#### Primary Parameters
- `-output string`: Output JSONL file path
- `-stories int`: Number of stories to download (0 = incremental update mode)
- `-update-from-file string`: Update incrementally from existing local JSONL file
- `-update-from-url string`: Update incrementally from JSONL file at URL
- `-resume-id int`: Resume from specific ID (overrides checkpoint)

#### Configuration Settings
- `-concurrent int`: Max concurrent requests (default: 100)
- `-rps int`: Requests per second limit (default: 200)
- `-min-score int`: Minimum score filter (0 = no filter)
- `-min-descendants int`: Minimum descendants/comments filter (0 = no filter)

## Operation Modes

### 1. Full Download Mode
Downloads a specified number of stories from the most recent backwards:
```bash
./hn_parser -stories 10000 -output full_dataset.jsonl
```

### 2. Incremental Update Mode
Perfect for keeping your dataset up-to-date without re-downloading everything:
```bash
# From local file
./hn_parser -update-from-file existing_dataset.jsonl

# From remote URL
./hn_parser -update-from-url gs://my-bucket/dataset.jsonl -output updated_dataset.jsonl
```

### 3. Continuous Mode
Downloads all available stories (use with caution):
```bash
./hn_parser -output all_stories.jsonl
```

## Output Format

Stories are saved in JSONL (JSON Lines) format, with one story per line:

```json
{"id":12345,"title":"Example Story","url":"https://example.com","type":"story","by":"username","time":1634567890,"score":150,"descendants":75,"kids":[12346,12347]}
{"id":12348,"title":"Another Story","text":"Story text here","type":"story","by":"author","time":1634567900,"score":200,"descendants":100}
```

## Checkpointing and Recovery

The parser automatically saves checkpoints during operation:
- Checkpoint file: `<output-file>.checkpoint`
- Automatically resumes from last checkpoint if interrupted
- Graceful shutdown on SIGINT/SIGTERM with state preservation

To manually resume from a specific ID:
```bash
./hn_parser -resume-id 12345 -stories 5000 -output stories.jsonl
```

## Performance Optimization

The optimized version includes several performance enhancements:

- **Memory Pooling**: Reuses buffers and story objects to reduce allocations
- **Batch Processing**: Processes items in batches of 10 for efficiency
- **Connection Reuse**: HTTP/2 with connection pooling
- **Smart Caching**: LRU cache for invalid items (dead/deleted stories)
- **Adaptive Checkpointing**: Adjusts checkpoint frequency based on throughput
- **Large Write Buffers**: 256KB buffer for efficient disk I/O

### Performance Tuning

Adjust concurrency and rate limit based on your needs:
```bash
# Conservative settings
./hn_parser -concurrent 50 -rps 100 -stories 10000

# Aggressive settings (default)
./hn_parser -concurrent 100 -rps 200 -stories 10000

# Maximum performance (be respectful to the API)
./hn_parser -concurrent 200 -rps 500 -stories 10000
```

