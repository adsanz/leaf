# Leaf - Log Error & Anomaly Finder

# WARNING: Experimental tool

A high-performance Rust application that clusters Kubernetes logs using Jaccard similarity to help identify patterns and reduce noise in large log datasets.

## Overview

Leaf fetches logs from Kubernetes pods and groups similar log entries together using Jaccard similarity. This helps DevOps engineers and developers quickly identify patterns, recurring issues, and anomalies in their application logs.

## Features

- **Fast Jaccard Similarity**: Uses textdistance library's Jaccard algorithm for accurate log clustering
- **Kubernetes Native**: Direct integration with Kubernetes API using the `kube` crate
- **Source Tracking**: Tracks namespace, pod, and container for each log cluster
- **Performance Optimized**: Multiple optimizations for handling large log volumes
  - Caching system (5000 entry limit)
  - Cluster limits (max 1000 clusters)
  - Smart cluster checking (max 100 when > 500 exist)
  - Early exit on good similarity matches
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

Uses Jaccard similarity to compare normalized text strings:

- **Jaccard Index**: Measures similarity between two sets as |A∩B| / |A∪B|
- **String-based**: Works directly on normalized text rather than word sets
- **Range**: Returns values between 0.0 (completely different) and 1.0 (identical)

### 3. Clustering Algorithm

The clustering process follows these steps:

1. **Sequential Processing**: Processes each log line one by one
2. **Similarity Search**: Compares against existing clusters
3. **Threshold Matching**: Groups logs that exceed the similarity threshold (default: 0.75)
4. **Performance Limits**: 
   - Max 1000 total clusters
   - Checks only top 100 clusters when more than 500 exist
5. **Result Sorting**: Orders clusters by frequency (most common first)

### 4. Performance Optimizations

- **Pre-filtering**: Reduces dataset size before clustering when using `--filter`
- **Caching**: Avoids reprocessing identical log lines
- **Early Exit**: Stops searching when a good match is found
- **Limited Search**: Reduces comparison overhead for large cluster sets
- **Word Filtering**: Only processes meaningful words (3+ characters)

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
```

### Command Line Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--label` | `-l` | Filter pods by label selector | None |
| `--namespace` | `-n` | Target specific namespace | All namespaces |
| `--threshold` | `-t` | Similarity threshold (0.0-1.0) | 0.75 |
| `--json` | `-j` | Output results as JSON | false |
| `--since` | `-s` | Filter logs since RFC3339 timestamp | None |
| `--filter` | `-f` | Filter logs containing specific strings (comma-separated, case-insensitive) | None |
| `--member-limit` | `-m` | Limit number of log members in JSON output (0 = unlimited) | 0 |

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
```

## Output Format

### Human-Readable Output

```
Found 5 pods
Fetching logs from pod: web-app-1, container: app
  Fetched 150 lines
Starting fast jaccard-based clustering with 150 log lines...
Clustering complete! Created 8 clusters from 150 log lines

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

- **Small datasets** (< 1,000 logs): Near real-time processing
- **Medium datasets** (1,000 - 10,000 logs): ~1-2 seconds
- **Large datasets** (10,000+ logs): Scales linearly with optimizations

### Memory Usage

- **Base memory**: ~10-20 MB
- **Per log line**: ~200 bytes (including caching)
- **Peak usage**: Proportional to number of unique log patterns

### Accuracy vs Performance Tradeoffs

- **High threshold** (0.9+): More precise clusters, potentially more clusters
- **Medium threshold** (0.7-0.8): Balanced clustering for most use cases
- **Low threshold** (< 0.7): Aggressive grouping, fewer but larger clusters

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

### Debug Mode

For debugging, use verbose output without JSON mode to see processing statistics.

## Dependencies

- `kube`: Kubernetes API client
- `k8s-openapi`: Kubernetes API types
- `textdistance`: Similarity algorithms (Jaccard)
- `clap`: Command-line parsing
- `tokio`: Async runtime
- `serde`: Serialization/deserialization
- `chrono`: Date/time handling

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure `cargo test` passes
5. Submit a pull request

## Changelog

### Current Version
- Implemented Jaccard similarity algorithm
- Added performance optimizations for large datasets
- Improved caching system
- Enhanced CLI with flexible filtering options
- Added source tracking (namespace/pod/container) for log clusters
- Implemented content-based filtering with `--filter` flag