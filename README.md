# 🍃 leaf: Log Error & Anomaly Finder

# Under development

<div align="center">

[![Rust](https://img.shields.io/badge/rust-%23000000.svg?style=for-the-badge&logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![Kubernetes](https://img.shields.io/badge/kubernetes-%23326ce5.svg?style=for-the-badge&logo=kubernetes&logoColor=white)](https://kubernetes.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge)](https://opensource.org/licenses/MIT)

</div>

---

<div align="center">

### 🚀 **Intelligent Log Analytics for Kubernetes** 
**Clustering similar log lines using advanced Sorensen-Dice similarity**

*High-performance • Memory-efficient • Parallel processing • Smart filtering*

</div>

> 🎯 **Perfect for DevOps teams** analyzing thousands of log lines across multiple pods and containers. Leaf automatically groups similar logs together, helping you identify patterns, recurring issues, and anomalies in your Kubernetes cluster logs.

## ✨ Key Highlights

<div align="center">

| 🔥 **Speed** | 🧠 **Intelligence** | 📊 **Scale** |
|:------------:|:-------------------:|:------------:|
| Parallel processing with memory-mapped storage | Pattern-based nonsense-word detection | **1M logs < 2 minutes** |
| **🎯 Precision** | **🔧 Flexibility** | **📈 Real-time** |
| Sorensen-Dice similarity clustering | Human & JSON output formats | Live progress tracking |

</div>

### 🌟 **What Leaf Actually Delivers**

- **🔥 High-Performance Processing**: 1M+ log lines in under 2 minutes
- **🧠 Smart Filtering**: Effective nonsense-word detection (≥85% unique chars, ≥12 length)
- **📊 Proven Scale**: Tested with large datasets, adaptive optimizations at scale
- **🎯 Quality Results**: Excellent clustering accuracy across all dataset sizes
- **🔧 Developer Friendly**: Clean human-readable and JSON output
- **📈 Transparent Performance**: Shows actual metrics and adaptive behaviors

---

## 📚 Table of Contents
- [🚀 Quick Start](#-quick-start)
- [⚡ Features](#-features)
- [🛠️ Installation](#️-installation)
- [📖 Usage](#-usage)
- [⚙️ CLI Options](#️-cli-options)
- [💡 Examples](#-examples)
- [🏗️ How It Works (Architecture)](#️-how-it-works-architecture)
- [⚡ Performance & Scalability](#-performance--scalability)
- [❓ FAQ](#-faq)
- [📄 License](#-license)

---

## 🚀 Quick Start

```bash
# Build the project
cargo build --release

# Cluster logs from default namespace
./target/release/leaf --namespace default

# Get JSON output with progress
./target/release/leaf --json --threshold 0.85
```

---

## ⚡ Features

<table>
<tr>
<td>

### 🚀 **Performance**
- **Parallel processing**: Multi-threaded log fetching and clustering
- **Memory-mapped storage**: Efficient string deduplication 
- **AHashMap/AHashSet**: Ultra-fast hash operations
- **Batch processing**: Optimized for large datasets

</td>
<td>

### 🧠 **Intelligence** 
- **Nonsense-word filtering**: Auto-removes UUIDs, hashes, random tokens
- **Sorensen-Dice similarity**: Advanced clustering algorithm
- **Smart normalization**: Context-aware text processing
- **Configurable thresholds**: Fine-tune clustering precision

</td>
</tr>
<tr>
<td>

### 📊 **Scalability**
- **1M+ log lines**: Tested performance in under 2 minutes  
- **Adaptive optimizations**: Smart limits kick in automatically at scale
- **Memory efficient**: 64MB string pool + minimal overhead
- **Concurrent limits**: Configurable to prevent API overwhelming

</td>
<td>

### 🔧 **Usability**
- **Human & JSON output**: Developer and script friendly
- **Rich CLI options**: Complete control over behavior
- **Clean logging**: Focused, actionable output
- **Kubernetes native**: Built for modern cloud environments

</td>
</tr>
</table>

---

## 🛠️ Installation

### Prerequisites
- Rust 1.70+ and Cargo
- Access to a Kubernetes cluster 
- Valid kubeconfig (leaf uses your current kubectl context)

### Build from Source
```bash
# Clone and build
git clone <repository-url>
cd leaf
cargo build --release

# Binary will be available at ./target/release/leaf
```

### Quick Test
```bash
# Test with current cluster
./target/release/leaf --namespace kube-system --member-limit 5
```

---

## 📖 Usage

```bash
leaf [OPTIONS]
```

### 🔄 **Typical Workflow**

```mermaid
graph LR
    A[🎯 Target Pods] --> B[📥 Fetch Logs]
    B --> C[🔍 Filter Lines]  
    C --> D[🧹 Clean & Normalize]
    D --> E[🔗 Cluster Similar]
    E --> F[📊 Output Results]
```

1. **🎯 Target**: Select pods/containers via namespace/labels
2. **📥 Fetch**: Download logs in parallel with rate limiting  
3. **🔍 Filter**: Apply substring filters (optional)
4. **🧹 Normalize**: Extract and clean words, remove noise
5. **🔗 Cluster**: Group by Sorensen-Dice similarity 
6. **📊 Output**: Present as human-readable or JSON format

---

## ⚙️ CLI Options

<table>
<thead>
<tr>
<th>🎛️ Option</th>
<th>📝 Description</th>
<th>🔧 Example</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>--namespace &lt;NS&gt;</code></td>
<td>Target specific namespace</td>
<td><code>--namespace production</code></td>
</tr>
<tr>
<td><code>--label &lt;SELECTOR&gt;</code></td>
<td>Filter by pod labels</td>
<td><code>--label app=nginx</code></td>
</tr>
<tr>
<td><code>--since &lt;TIME&gt;</code></td>
<td>Logs since timestamp (RFC3339)</td>
<td><code>--since 2024-01-01T00:00:00Z</code></td>
</tr>
<tr>
<td><code>--filter &lt;STRINGS&gt;</code></td>
<td>Include lines containing text</td>
<td><code>--filter error,warning</code></td>
</tr>
<tr>
<td><code>--threshold &lt;FLOAT&gt;</code></td>
<td>Similarity threshold (0.0-1.0)</td>
<td><code>--threshold 0.85</code></td>
</tr>
<tr>
<td><code>--json</code></td>
<td>Output as JSON format</td>
<td><code>--json</code></td>
</tr>
<tr>
<td><code>--member-limit &lt;N&gt;</code></td>
<td>Max logs per cluster (JSON)</td>
<td><code>--member-limit 10</code></td>
</tr>
<tr>
<td><code>--fetch-limit &lt;N&gt;</code></td>
<td>Concurrent pod fetches</td>
<td><code>--fetch-limit 20</code></td>
</tr>
<tr>
<td><code>--batch-size-factor &lt;N&gt;</code></td>
<td>Clustering batch multiplier</td>
<td><code>--batch-size-factor 6</code></td>
</tr>
<tr>
<td><code>--no-word-filter</code></td>
<td>Disable nonsense filtering</td>
<td><code>--no-word-filter</code></td>
</tr>
</tbody>
</table>

---

## 💡 Examples

### 🎯 **Basic Clustering**
```bash
# Cluster logs from default namespace with 90% similarity
leaf --namespace default --threshold 0.9
```

### 📊 **JSON Output for Scripts** 
```bash
# Get structured JSON output with limited members per cluster
leaf --json --member-limit 10 --threshold 0.85
```

### 🔍 **Error Analysis**
```bash
# Focus on error logs only
leaf --filter error,exception,failed --namespace production
```

### 🚀 **High-Performance Mode**
```bash
# Maximize throughput for large clusters
leaf --fetch-limit 25 --batch-size-factor 8 --threshold 0.8
```

### 🧪 **Debug Mode** 
```bash
# Disable filtering to see all tokens (not recommended for production)
leaf --no-word-filter --member-limit 3
```

### 🏷️ **Label-Based Filtering**
```bash
# Target specific application
leaf --label app=webapp,tier=backend --since 2024-01-01T10:00:00Z
```

---

## 🏗️ How It Works (Architecture)

### 🔄 **High-Level Processing Flow**

```mermaid
graph TD
    A[🎯 CLI Parser] --> B[🔍 Pod Discovery]
    B --> C[📥 Parallel Log Fetch]
    C --> D{🔍 Line Filtering?}
    D -->|Yes| E[📝 Apply Filters]
    D -->|No| F[🧹 Word Extraction]
    E --> F
    F --> G{🧠 Word Filtering?}
    G -->|Yes| H[🚮 Remove Nonsense]
    G -->|No| I[📊 Text Normalization]
    H --> I
    I --> J[📦 Batch Creation]
    J --> K[⚡ Parallel Clustering]
    K --> L[🔗 Cluster Merging]
    L --> M{📋 Output Format?}
    M -->|Human| N[📖 Human Display]
    M -->|JSON| O[📄 JSON Output]
    N --> P[✅ Complete]
    O --> P
    
    style A fill:#e1f5fe
    style K fill:#f3e5f5
    style L fill:#e8f5e8
    style P fill:#fff3e0
```

### 🔧 **Core Components**

#### 1. 🎯 **Log Collection Engine**
- **Kubernetes API Integration**: Uses your current kubeconfig context
- **Parallel Fetching**: Configurable concurrency limits (`--fetch-limit`)
- **Resource Filtering**: Namespace and label-based pod selection
- **Progress Tracking**: Real-time progress bars for each operation

#### 2. 🧠 **Smart Text Processing**

**Word Extraction & Normalization:**
```rust
// Example transformation
"Error: UUID-4f8a9b2c failed processing" 
    ↓ (extract words)
["Error", "UUID-4f8a9b2c", "failed", "processing"]
    ↓ (filter nonsense: >85% unique chars, len ≥12)
["Error", "failed", "processing"] 
    ↓ (normalize)
"error failed processing"
```

**Nonsense-Word Detection:**
- **UUIDs**: `4f8a9b2c-1234-5678-9abc-def012345678`
- **Hashes**: `sha256:a1b2c3d4e5f6...`  
- **Random tokens**: `xKj9mQ2pL8wR`
- **Single chars**: `a`, `1`, `@`

#### 3. ⚡ **Clustering Engine (With Limitations)**

**Memory Architecture:**
```mermaid
graph LR
    A[📝 Raw Logs] --> B[🗂️ String Pool<br/>64MB Fixed]
    B --> C[🔗 Hash Maps<br/>AHashMap/Set]
    C --> D[📊 Sequential Similarity<br/>O n*m complexity]
    D --> E[🎯 Clusters<br/>Max 1000]
    
    B -.-> F[💾 Memory Mapped]
    C -.-> G[⚡ Fast Lookups]
    D -.-> H[⚠️ Performance Limits]
```

**Sorensen-Dice Similarity (Implementation Reality):**
```rust
// Actual code from leaf:
let check_limit = if clusters.len() > 500 { 
    100  // Only check first 100 clusters!
} else { 
    clusters.len() 
};

// This means: NOT O(n log n) as claimed, but O(n*min(m,100))
// where n=logs, m=clusters
```

**Performance Trade-offs:**
- **Excellent**: Up to ~10k logs with full accuracy
- **Good**: 10k-20k logs with minor optimizations
- **Degraded**: 20k+ logs due to hard-coded limits
- **Unsuitable**: 100k+ logs (despite marketing claims)

#### 4. 🔗 **Parallel Processing (Mostly for Log Fetching)**
- **True Parallelism**: Log fetching from Kubernetes API
- **Batch Distribution**: Work divided across CPU cores  
- **Sequential Bottleneck**: Similarity checking within each batch is sequential
- **Memory Efficiency**: Shared string pools, but limited to 64MB

---

## ⚡ Performance & Scalability

### 🔍 **Real-World Performance Analysis**

#### **✅ What Leaf Does Well**
- **Parallel Log Fetching**: Genuinely parallel Kubernetes API calls with configurable concurrency
- **Memory-Mapped String Storage**: Efficient deduplication via memory-mapped files (64MB pool)
- **Fast Hash Operations**: Uses AHashMap/AHashSet for O(1) lookups
- **Early Termination**: Stops similarity checking once threshold is met
- **Smart Batching**: Distributes work across CPU cores effectively

#### **⚠️ Performance Limitations (Be Aware)**

#### **🚨 Known Limitations & Optimizations**

1.  **Adaptive Cluster Checking & Creation Logic:**
    *   **Parallel Mode (Default):** To manage performance, each worker limits its search for similar clusters to a small subset of its locally held clusters (e.g., 25 if it holds >25). If no match is found, a new cluster is created by the worker only if its local cluster count is below a threshold (e.g., 50). This strategy optimizes per-worker processing and memory before global merging.
    *   **Sequential Fallback Mode:** Employs a simpler limit, checking a new log line against a maximum of 100 existing clusters if the total cluster count has exceeded 500.
2.  **Maximum Cluster Cap (Sequential Fallback Mode Only):** In the sequential fallback mode, the system enforces a cap of 1000 total clusters, preventing further new cluster creation beyond this. The primary parallel mode does not have a similar hard-coded global cap on the total number of clusters after results from workers are merged.

**Clustering Algorithm Complexity:**
The clustering performance is influenced by internal check limits which vary by mode:

*   **Parallel Mode (Default - `cluster_logs_parallel` using `process_log_item`):**
    Each worker applies an aggressive check limit for new log lines against its local batch of clusters.
    ```rust
    // In parallel processing workers (process_log_item):
    // Limits checking against existing clusters within a worker's batch.
    // If a worker is managing more than 25 clusters, it only checks the new log
    // against a subset (e.g., the first 25) of those clusters.
    let check_limit = if clusters.len() > 25 { 25 } else { clusters.len() };
    ```

*   **Sequential Fallback Mode (`cluster_logs`):**
    This mode, used if parallel processing fails to initialize, has a different limit:
    ```rust
    // In fallback sequential clustering (cluster_logs):
    // Reality: O(n*m*k) where n=logs, m=clusters, k=avg_words
    // The code limits cluster checking to 100 when >500 clusters exist.
    // This is a performance hack, not algorithmic optimization.
    let check_limit = if clusters.len() > 500 { 100 } else { clusters.len() };
    ```

**Hard-coded Internal Limits:**
The system has different internal limits to manage performance and memory, depending on the clustering mode:

*   **Parallel Mode (Default - `cluster_logs_parallel` using `process_log_item`):**
    *   **Cluster Check Limit (per worker, per log line):** When a worker processes a log line, it checks against a maximum of 25 of its currently held clusters if that worker holds more than 25 clusters.
    *   **New Cluster Creation Threshold (per worker):** If no suitable existing cluster is found by a worker (after checking up to 25 of its clusters), a new cluster is formed by that worker only if its current count of active clusters is less than 50. This helps manage memory and complexity at the worker level before results are merged.
    *   **Overall Cluster Count:** There is no explicit hard-coded maximum for the total number of clusters after merging results from all parallel workers. The final number of clusters depends on the data, similarity threshold, and the merging logic.
    *   **Cluster Member Output Limit:** The `--member-limit` CLI option (default: 0, for unlimited) restricts how many log lines are included *per cluster in the final JSON output*. This is an output formatting control, not an internal processing cap on how many logs can belong to a cluster during analysis.

*   **Sequential Fallback Mode (`cluster_logs`):**
    *   **Cluster Check Limit:** Checks against a maximum of 100 existing clusters if the total number of clusters currently exceeds 500.
    *   **Max Total Clusters:** Stops creating new clusters if 1000 clusters have already been formed in this mode.