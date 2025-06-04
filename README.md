# Leaf - Log Error & Anomaly Finder

# WARNING: Experimental tool

A high-performance Rust application that clusters Kubernetes logs using Sørensen-Dice similarity with memory-mapped storage and work-stealing parallelism to help identify patterns and reduce noise in large log datasets.

## Overview

Leaf fetches logs from Kubernetes pods and groups similar log entries together using Sørensen-Dice similarity. This helps DevOps engineers and developers quickly identify patterns, recurring issues, and anomalies in their application logs with advanced memory efficiency and parallel processing.

## Features

- **Fast Sørensen-Dice Similarity**: Uses textdistance library's Sørensen-Dice algorithm for accurate log clustering
- **Memory-Mapped Storage**: Uses memmap2 for efficient string storage (256MB memory-mapped pool)
- **Work-Stealing Parallelism**: 4-worker parallel clustering with tokio async tasks
- **Kubernetes Native**: Direct integration with Kubernetes API using the `kube` crate
- **Source Tracking**: Tracks namespace, pod, and container for each log cluster
- **Performance Optimized**: Multiple optimizations for handling large log volumes
  - Memory-mapped string interning with O(1) deduplication
  - Work-stealing parallelism with 1000-item batches
  - HashSet-based O(1) member and source deduplication
  - Cross-worker cluster merging with similarity-based consolidation
  - Smart cluster limits (max 100 clusters per worker)
  - Early exit on good similarity matches
  - Fallback mechanism when memory-mapping fails
- **Flexible Filtering**: Filter by namespace, pod labels, time ranges, and content keywords
- **Multiple Output Formats**: Human-readable text or JSON output
- **Word-based Normalization**: Intelligent text preprocessing for better clustering

## How It Works

### 1. Log Filtering (Optional)

When filter strings are provided via the `--filter` flag, logs are pre-filtered before normalization:

1. **Case-insensitive matching**: Converts both log content and filter strings to lowercase
2. **OR condition**: A log line matches if it contains ANY of the specified filter strings
3. **Performance boost**: Reduces dataset size before expensive clustering operations
4. **Statistics**: Shows filtering results (original count → filtered count → percentage retained)

```bash
# Example: Filter for error-related logs
./leaf --filter error,exception,failed
# Matches: "ERROR: Connection failed", "Exception in thread", "Authentication failed"
```

### 2. Log Normalization

The `LogNormalizer` processes each log line through several steps:

1. **Word Extraction**: Extracts alphabetic words (minimum 3 characters)
2. **Text Normalization**: Creates a normalized string with lowercase letters and spaces
3. **Caching**: Stores processed results to avoid recomputation

```rust
// Example normalization
Input:  "2024-06-04T10:15:30 ERROR [user-service] Authentication failed for user ID 12345"
Words:  ["error", "user", "service", "authentication", "failed", "for", "user"]
Normalized: "error user service authentication failed for user"
```

### 2. Similarity Calculation

Uses Sørensen-Dice similarity to compare normalized text strings:

- **Sørensen-Dice Coefficient**: Measures similarity between two sets as 2|A∩B| / (|A| + |B|)
- **String-based**: Works directly on normalized text using character overlap
- **Range**: Returns values between 0.0 (completely different) and 1.0 (identical)
- **Better for text**: More sensitive to overlapping content than Jaccard similarity

### 3. Parallel Clustering Algorithm

The clustering process uses work-stealing parallelism:

1. **Memory-Mapped Storage**: 256MB memory-mapped string pool for efficient string storage and deduplication
2. **Work Distribution**: Logs are split into 1000-item batches and distributed to 4 workers
3. **Work-Stealing**: Workers use tokio mpsc channels to dynamically claim work batches
4. **Parallel Processing**: Each worker processes batches independently with local cluster sets
5. **Cross-Worker Merging**: Similar clusters from different workers are merged using similarity comparison
6. **O(1) Deduplication**: HashSet-based deduplication for both members and sources
7. **Performance Limits**: 
   - Max 100 clusters per worker
   - Checks only top 50 clusters for similarity matching
8. **Fallback Mode**: Automatically falls back to single-threaded mode if memory-mapping fails
9. **Result Sorting**: Orders clusters by frequency (most common first)

### 4. Performance Optimizations

- **Memory-Mapped Storage**: 256MB memory-mapped string pool with automatic fallback
- **Work-Stealing Parallelism**: 4 workers processing 1000-item batches concurrently
- **O(1) Deduplication**: HashSet-based member and source deduplication
- **String Interning**: Memory-mapped string pool reduces memory usage and enables fast comparison
- **Pre-filtering**: Reduces dataset size before clustering when using `--filter`
- **Caching**: Avoids reprocessing identical log lines (5000 entry limit)
- **Early Exit**: Stops searching when a good match is found
- **Limited Search**: Workers check only top 50 clusters, max 100 clusters per worker
- **Cross-Worker Merging**: Intelligent cluster consolidation after parallel processing
- **Word Filtering**: Only processes meaningful words (3+ characters)
- **Batch Processing**: 1000-item batches for optimal work distribution

## Installation

### Prerequisites

- Rust 1.70+ (2024 edition)
- Kubernetes cluster access
- Valid kubeconfig file

### Build from Source

```bash
git clone git@github.com:adsanz/leaf.git
cd leaf
cargo build --release
```

The compiled binary will be available at `target/release/leaf`.

## Usage

### Basic Usage

```bash
# Cluster logs from all pods in all namespaces
./leaf

# Cluster logs from specific namespace
./leaf --namespace my-app

# Filter by pod labels
./leaf --label app=web-server

# Use custom similarity threshold
./leaf --threshold 0.8

# Output as JSON
./leaf --json

# Filter logs since specific time
./leaf --since "2024-06-04T10:00:00Z"

# Filter logs containing error messages
./leaf --filter error

# Filter logs containing multiple keywords (OR condition)
./leaf --filter error,warn,exception

# Limit JSON output to 5 members per cluster for large datasets
./leaf --json --member-limit 5

# Combine filters for focused analysis
./leaf --namespace production --filter error,timeout --threshold 0.8

# Control concurrent log fetching for better resource management
./leaf --fetch-limit 5
```

### Command Line Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--label` | `-l` | Filter pods by label selector | None |
| `--namespace` | `-n` | Target specific namespace | All namespaces |
| `--threshold` | `-t` | Similarity threshold (0.0-1.0) | 0.9 |
| `--json` | `-j` | Output results as JSON | false |
| `--since` | `-s` | Filter logs since RFC3339 timestamp | None |
| `--filter` | `-f` | Filter logs containing specific strings (comma-separated, case-insensitive) | None |
| `--member-limit` | `-m` | Limit number of log members in JSON output (0 = unlimited) | 0 |
| `--fetch-limit` | | Maximum number of concurrent pod log fetches | 10 |

### Examples

```bash
# Monitor web application logs in production
./leaf -n production -l app=webapp -t 0.8

# Focus on error logs only
./leaf --filter error --threshold 0.9

# Get recent error and warning logs as JSON for processing
./leaf --filter error,warn --since "2024-06-04T09:00:00Z" --json

# Limit output size for large clusters
./leaf --json --member-limit 10 --filter error

# High-sensitivity clustering for debugging specific issues
./leaf -n staging --filter timeout,connection --threshold 0.9

# Reduce concurrent fetches for resource-constrained environments
./leaf --fetch-limit 3
```

## Output Format

### Human-Readable Output

```
Found 5 pods
Fetching logs from pod: web-app-1, container: app
  Fetched 150 lines
Starting parallel memory-mapped clustering with 150 log lines...
Split into 1 batches with 4 workers
Worker 0 processing batch of 150 items
Collected 8 clusters from all workers
Merging complete, final clusters: 6
Parallel clustering complete! Created 6 clusters

--- Log Clusters ---
Cluster 1: (Count: 45)
  Representative: INFO [web-app] Request processed successfully
  Key words: ["info", "web", "app", "request", "processed", "successfully"]
  Sources:
    - production/web-app-1/app
    - production/web-app-2/app

Cluster 2: (Count: 23)
  Representative: ERROR [database] Connection timeout after 30 seconds
  Key words: ["error", "database", "connection", "timeout", "after", "seconds"]
  Sources:
    - production/database-1/postgres
```

### JSON Output

```json
[
  {
    "representative": "INFO [web-app] Request processed successfully",
    "normalized_text": "info web app request processed successfully",
    "words": ["info", "web", "app", "request", "processed", "successfully"],
    "members": ["INFO [web-app] Request processed successfully", "..."],
    "count": 45,
    "sources": [
      {
        "namespace": "production",
        "pod": "web-app-1",
        "container": "app"
      },
      {
        "namespace": "production", 
        "pod": "web-app-2",
        "container": "app"
      }
    ]
  }
]
```

## Configuration

### Kubernetes Access

Leaf uses the standard Kubernetes configuration:

1. **In-cluster**: Automatically detects when running inside a pod
2. **Kubeconfig**: Uses `~/.kube/config` or `KUBECONFIG` environment variable
3. **Service Account**: Uses mounted service account when available

### Required Permissions

Your Kubernetes user or service account needs these permissions:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: leaf-log-reader
rules:
- apiGroups: [""]
  resources: ["pods", "pods/log"]
  verbs: ["get", "list"]
```

## Performance Characteristics

### Throughput

- **Small datasets** (< 1,000 logs): Near real-time processing with 4-worker parallelism
- **Medium datasets** (1,000 - 10,000 logs): ~0.5-1 seconds with memory-mapped storage
- **Large datasets** (10,000+ logs): Scales efficiently with work-stealing parallelism

### Memory Usage

- **Base memory**: ~20-30 MB (including 256MB memory-mapped pool)
- **Memory-mapped pool**: 256MB pre-allocated for string storage
- **Per log line**: ~100 bytes average (with string interning)
- **Peak usage**: Significantly reduced due to string deduplication and memory mapping

### Parallelism Performance

- **4 workers**: Optimal for most workloads
- **1000-item batches**: Balanced work distribution
- **Work-stealing**: Dynamic load balancing across workers
- **Cross-worker merging**: Intelligent similarity-based cluster consolidation

### Accuracy vs Performance Tradeoffs

- **High threshold** (0.9+): More precise clusters, potentially more clusters
- **Medium threshold** (0.7-0.8): Balanced clustering for most use cases
- **Low threshold** (< 0.7): Aggressive grouping, fewer but larger clusters
- **Sørensen-Dice**: Better text similarity detection than Jaccard for log content

## Troubleshooting

### Common Issues

1. **"No permissions to access pods"**
   - Verify RBAC permissions
   - Check kubeconfig is valid: `kubectl get pods`

2. **"No clusters generated"**
   - Lower the similarity threshold
   - Check if logs contain meaningful text
   - Verify pods have recent logs

3. **Performance issues with large datasets**
   - Use namespace filtering
   - Apply label selectors
   - Use time-based filtering with `--since`
   - Monitor memory usage (256MB memory-mapped pool)

4. **"Failed to create memory-mapped pool"**
   - Check available disk space for temporary files
   - System automatically falls back to regular clustering
   - Consider reducing dataset size with filters

### Debug Mode

For debugging, use verbose output without JSON mode to see processing statistics.

## Dependencies

- `kube`: Kubernetes API client
- `k8s-openapi`: Kubernetes API types
- `textdistance`: Similarity algorithms (Sørensen-Dice)
- `memmap2`: Memory-mapped file I/O for efficient string storage
- `tempfile`: Temporary file creation for memory mapping
- `clap`: Command-line parsing
- `tokio`: Async runtime and parallelism
- `serde`: Serialization/deserialization
- `chrono`: Date/time handling
- `futures`: Stream processing utilities

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure `cargo test` passes
5. Submit a pull request

## Changelog

### Current Version
- **Implemented Sørensen-Dice similarity algorithm** for better text similarity detection
- **Added memory-mapped storage** with memmap2 for efficient string storage and deduplication
- **Implemented work-stealing parallelism** with 4 workers and tokio async tasks
- **Added cross-worker cluster merging** with similarity-based consolidation
- **Optimized performance** with O(1) HashSet deduplication for members and sources
- **Enhanced string interning** with 256MB memory-mapped pool and fallback mechanism
- **Improved batch processing** with 1000-item batches for optimal work distribution
- **Added performance monitoring** with detailed processing statistics
- **Enhanced source tracking** (namespace/pod/container) for log clusters
- **Implemented content-based filtering** with `--filter` flag
- **Added automatic fallback** to single-threaded mode when memory-mapping fails