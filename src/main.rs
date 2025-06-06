// Kubernetes log clustering with sorensen_dice similarity
use ahash::{AHashMap, AHashSet};
use chrono::{DateTime, Utc};
use clap::Parser;
use futures::stream::{self, StreamExt};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use k8s_openapi::api::core::v1::Pod;
use kube::api::{ListParams, LogParams};
use kube::{Api, Client};
use memmap2::{MmapMut, MmapOptions};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::io::{Seek, SeekFrom};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use textdistance::str::sorensen_dice;
use tokio::task;

#[derive(Serialize, Debug)]
struct ErrorEntry {
    error: String,
    location: String,
    timestamp: String,
}

#[derive(Parser)]
#[clap(
    name = "leaf",
    about = "Kubernetes log clustering with fast word-based similarity",
    version
)]
struct Cli {
    #[clap(short, long)]
    label: Option<String>,
    #[clap(short, long)]
    namespace: Option<String>,
    #[clap(short = 't', long, default_value = "0.9")]
    threshold: f64,
    #[clap(short, long)]
    json: bool,
    #[clap(short, long)]
    since: Option<String>,
    #[clap(
        short = 'f',
        long,
        help = "Filter logs containing specific strings (comma-separated, case-insensitive)",
        value_delimiter = ','
    )]
    filter: Vec<String>,
    #[clap(
        short = 'm',
        long,
        help = "Limit the number of log members included in JSON output (default: unlimited)",
        default_value = "0"
    )]
    member_limit: usize,
    #[clap(
        long,
        help = "Maximum number of concurrent pod log fetches (default: 10)",
        default_value = "10"
    )]
    fetch_limit: usize,
    #[clap(
        long,
        help = "Batch size multiplier for clustering (default: 4) - increases parallelism for large datasets",
        default_value = "4"
    )]
    batch_size_factor: usize,
    #[clap(
        long,
        help = "Disable nonsense-word filtering for clustering (not recommended)",
        action,
        default_value_t = true
    )]
    no_word_filter: bool,
}

// Memory-mapped string pool for efficient string storage
#[derive(Debug)]
struct MmapStringPool {
    #[allow(dead_code)] // Keeps the file alive for the memory mapping
    file: std::fs::File,
    mmap: Option<MmapMut>,
    string_to_id: AHashMap<String, usize>,
    id_to_offset: Vec<(usize, usize)>, // (offset, length) pairs
    current_offset: usize,
    capacity: usize,
    #[allow(dead_code)]
    lock_count: Arc<AtomicUsize>, // For lock contention measurement
}

impl MmapStringPool {
    fn new(capacity_mb: usize) -> std::io::Result<Self> {
        let capacity = capacity_mb * 1024 * 1024; // Convert MB to bytes
        let temp_file = tempfile::NamedTempFile::new()?;
        let mut file = temp_file.into_file();

        // Pre-allocate file space
        file.set_len(capacity as u64)?;
        file.seek(SeekFrom::Start(0))?;

        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };

        Ok(MmapStringPool {
            file,
            mmap: Some(mmap),
            string_to_id: AHashMap::new(),
            id_to_offset: Vec::new(),
            current_offset: 0,
            capacity,
            lock_count: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn intern_string(&mut self, s: &str) -> usize {
        if let Some(&id) = self.string_to_id.get(s) {
            return id;
        }

        let string_bytes = s.as_bytes();
        let needed_space = string_bytes.len() + 4; // 4 bytes for length prefix

        if self.current_offset + needed_space > self.capacity {
            // Pool is full, return a fallback ID
            return usize::MAX;
        }

        if let Some(ref mut mmap) = self.mmap {
            // Write length prefix (little-endian u32)
            let len_bytes = (string_bytes.len() as u32).to_le_bytes();
            mmap[self.current_offset..self.current_offset + 4].copy_from_slice(&len_bytes);

            // Write string data
            let string_start = self.current_offset + 4;
            mmap[string_start..string_start + string_bytes.len()].copy_from_slice(string_bytes);

            let id = self.id_to_offset.len();
            self.id_to_offset.push((self.current_offset, needed_space));

            // Only intern short strings (e.g., <256 bytes) to reduce memory usage
            if s.len() < 256 {
                self.string_to_id.insert(s.to_string(), id);
            }

            self.current_offset += needed_space;

            id
        } else {
            usize::MAX
        }
    }

    fn get_string(&self, id: usize) -> Option<String> {
        if id == usize::MAX || id >= self.id_to_offset.len() {
            return None;
        }

        let (offset, _size) = self.id_to_offset[id];
        if let Some(ref mmap) = self.mmap {
            // Read length prefix
            let len_bytes = &mmap[offset..offset + 4];
            let string_len =
                u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]])
                    as usize;

            // Read string data
            let string_start = offset + 4;
            if string_start + string_len <= mmap.len() {
                let string_bytes = &mmap[string_start..string_start + string_len];
                return String::from_utf8(string_bytes.to_vec()).ok();
            }
        }
        None
    }
}

// Thread-safe string pool wrapper
// type SharedStringPool = Arc<Mutex<MmapStringPool>>;
type SharedStringPool = Arc<Mutex<MmapStringPool>>;

// Optimized LogCluster using string pool IDs
#[derive(Debug, Clone)]
struct OptimizedLogCluster {
    representative_id: usize,
    normalized_text_id: usize,
    word_ids: Vec<usize>,
    member_ids: Vec<usize>,
    member_ids_set: AHashSet<usize>, // O(1) deduplication lookup
    count: usize,
    sources: Vec<PackedLogMetadata>,
    sources_set: AHashSet<(usize, usize, usize)>, // O(1) source deduplication lookup
}

// Packed metadata for memory efficiency
#[derive(Debug, Clone)]
struct PackedLogMetadata {
    namespace_id: usize,
    pod_id: usize,
    container_id: usize,
}

impl OptimizedLogCluster {
    fn new(
        log_line: &str, // Added log_line parameter based on usage in original code
        words: Vec<String>,
        normalized_text: String,
        metadata: LogMetadata,
        string_pool: &mut MmapStringPool,
    ) -> Self {
        let representative_id = string_pool.intern_string(log_line);
        let normalized_text_id = string_pool.intern_string(&normalized_text);
        let word_ids: Vec<usize> = words.iter().map(|w| string_pool.intern_string(w)).collect();
        let member_ids = vec![representative_id];
        let mut member_ids_set = AHashSet::new();
        member_ids_set.insert(representative_id);

        let packed_metadata = PackedLogMetadata {
            namespace_id: string_pool.intern_string(&metadata.namespace),
            pod_id: string_pool.intern_string(&metadata.pod),
            container_id: string_pool.intern_string(&metadata.container),
        };

        let mut sources_set = AHashSet::new();
        sources_set.insert((
            packed_metadata.namespace_id,
            packed_metadata.pod_id,
            packed_metadata.container_id,
        ));

        OptimizedLogCluster {
            representative_id,
            normalized_text_id,
            word_ids,
            member_ids,
            member_ids_set,
            count: 1,
            sources: vec![packed_metadata],
            sources_set,
        }
    }

    fn add_member(
        &mut self,
        log_line: &str,
        metadata: LogMetadata,
        string_pool: &mut MmapStringPool,
    ) {
        let log_id = string_pool.intern_string(log_line);

        // Only add if not already present (O(1) lookup)
        if !self.member_ids_set.contains(&log_id) {
            self.member_ids.push(log_id);
            self.member_ids_set.insert(log_id);
            self.count += 1;
        }

        let packed_metadata = PackedLogMetadata {
            namespace_id: string_pool.intern_string(&metadata.namespace),
            pod_id: string_pool.intern_string(&metadata.pod),
            container_id: string_pool.intern_string(&metadata.container),
        };

        let source_key = (
            packed_metadata.namespace_id,
            packed_metadata.pod_id,
            packed_metadata.container_id,
        );

        // Add source metadata if not already present (O(1) lookup)
        if !self.sources_set.contains(&source_key) {
            self.sources.push(packed_metadata);
            self.sources_set.insert(source_key);
        }
    }

    fn similarity_to(&self, normalized_text: &str, string_pool: &MmapStringPool) -> f64 {
        if let Some(stored_text) = string_pool.get_string(self.normalized_text_id) {
            sorensen_dice_similarity(&stored_text, normalized_text)
        } else {
            0.0
        }
    }

    fn to_log_cluster(&self, string_pool: &MmapStringPool, member_limit: usize) -> LogCluster {
        let representative = string_pool
            .get_string(self.representative_id)
            .unwrap_or_default();
        let normalized_text = string_pool
            .get_string(self.normalized_text_id)
            .unwrap_or_default();
        let words: Vec<String> = self
            .word_ids
            .iter()
            .filter_map(|&id| string_pool.get_string(id))
            .collect();

        let num_members_to_take = if member_limit == 0 {
            self.member_ids.len() // Take all if limit is 0
        } else {
            member_limit.min(self.member_ids.len()) // Otherwise, take up to the limit, but not more than available
        };

        let members: Vec<String> = self
            .member_ids
            .iter()
            .take(num_members_to_take) // Apply the limit
            .filter_map(|&id| string_pool.get_string(id))
            .collect();

        let sources: Vec<LogMetadata> = self
            .sources
            .iter()
            .filter_map(|packed| {
                let namespace = string_pool.get_string(packed.namespace_id)?;
                let pod = string_pool.get_string(packed.pod_id)?;
                let container = string_pool.get_string(packed.container_id)?;
                Some(LogMetadata {
                    namespace,
                    pod,
                    container,
                })
            })
            .collect();

        // OPTIMIZATION: These sets are not used after conversion from OptimizedLogCluster
        // in the current workflow (JSON output or console printing).
        // Initializing them as empty saves computation and memory, especially when
        // member_limit is 0 and 'members'/'sources' vectors can be large.
        let members_set = AHashSet::new();
        let sources_set = AHashSet::new();

        LogCluster {
            representative,
            normalized_text,
            words,
            members,           // Members vector is now potentially smaller
            members_set,       // Now an empty set
            count: self.count, // Count still reflects the total number of original members
            sources,
            sources_set, // Now an empty set
        }
    }
}

// Work item for parallel processing
#[derive(Debug, Clone)]
struct WorkItem {
    log_line: String,
    metadata: LogMetadata,
}

// Ultra-fast log normalizer using word extraction and string normalization
struct LogNormalizer {
    cache: AHashMap<String, (Vec<String>, String)>,
    no_word_filter: bool,
}

impl LogNormalizer {
    fn new_with_filter(no_word_filter: bool) -> Self {
        LogNormalizer {
            cache: AHashMap::new(),
            no_word_filter,
        }
    }

    fn is_nonsense_word(word: &str) -> bool {
        let len = word.len();
        if len > 20 {
            return true;
        }
        if len < 3 {
            return true;
        }
        let digit_count = word.chars().filter(|c| c.is_ascii_digit()).count();
        if digit_count > len / 2 {
            return true;
        }
        if len > 8 && word.chars().all(|c| c.is_ascii_hexdigit()) {
            return true;
        }
        // Exclude words with low alphabetic ratio (e.g., v1beta1, k8s, etc.)
        let alpha_count = word.chars().filter(|c| c.is_ascii_alphabetic()).count();
        if alpha_count < len.div_ceil(2) {
            return true;
        }
        // Relaxed: Only exclude very high-entropy words (all unique, len >= 12)
        let unique_chars = word.chars().collect::<std::collections::HashSet<_>>().len();
        if len >= 12 && unique_chars as f32 / len as f32 > 0.85 {
            return true;
        }
        false
    }

    fn extract_words_and_normalized(&mut self, log_line: &str) -> (Vec<String>, String) {
        // Check cache first
        if let Some(cached) = self.cache.get(log_line) {
            return cached.clone();
        }

        let (words, normalized) = if self.no_word_filter {
            self.fast_extract_words_and_normalized(log_line)
        } else {
            self.fast_extract_words_and_normalized_no_filter(log_line)
        };

        // Cache the result (limit cache size)
        if self.cache.len() < 5000 {
            self.cache
                .insert(log_line.to_string(), (words.clone(), normalized.clone()));
        }

        (words, normalized)
    }

    fn fast_extract_words_and_normalized(&self, log_line: &str) -> (Vec<String>, String) {
        let mut words = Vec::new();
        let mut current_word = String::new();

        for ch in log_line.chars() {
            if ch.is_alphabetic() {
                let lower_ch = ch.to_ascii_lowercase();
                current_word.push(lower_ch);
            } else {
                if !current_word.is_empty()
                    && current_word.len() >= 3
                    && !Self::is_nonsense_word(&current_word)
                {
                    words.push(current_word.clone());
                }
                current_word.clear();
            }
        }

        // Add the last word if it exists
        if !current_word.is_empty()
            && current_word.len() >= 3
            && !Self::is_nonsense_word(&current_word)
        {
            words.push(current_word);
        }

        // Normalized string is now just the filtered words joined by space
        let normalized_string = words.join(" ");
        (words, normalized_string)
    }

    fn fast_extract_words_and_normalized_no_filter(&self, log_line: &str) -> (Vec<String>, String) {
        let mut words = Vec::new();
        let mut current_word = String::new();
        for ch in log_line.chars() {
            if ch.is_alphabetic() {
                let lower_ch = ch.to_ascii_lowercase();
                current_word.push(lower_ch);
            } else {
                if !current_word.is_empty() && current_word.len() >= 3 {
                    words.push(current_word.clone());
                }
                current_word.clear();
            }
        }
        if !current_word.is_empty() && current_word.len() >= 3 {
            words.push(current_word);
        }
        let normalized_string = words.join(" ");
        (words, normalized_string)
    }
}

// sorensen_dice-based similarity calculation
fn sorensen_dice_similarity(text1: &str, text2: &str) -> f64 {
    if text1.is_empty() && text2.is_empty() {
        return 1.0;
    }

    sorensen_dice(text1, text2)
}

// Metadata for tracking log source
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LogMetadata {
    namespace: String,
    pod: String,
    container: String,
}

// Simplified LogCluster with sorensen_dice-based clustering
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LogCluster {
    representative: String,
    normalized_text: String,
    words: Vec<String>,
    members: Vec<String>,
    #[serde(skip)] // Skip serialization for performance sets
    members_set: AHashSet<String>, // O(1) deduplication lookup
    count: usize,
    sources: Vec<LogMetadata>,
    #[serde(skip)] // Skip serialization for performance sets
    sources_set: AHashSet<(String, String, String)>, // O(1) source deduplication lookup
}

#[derive(Serialize)]
struct JsonOutput {
    clusters: Vec<LogCluster>,
    errors: Vec<ErrorEntry>,
}

impl LogCluster {
    fn new(log_line: String, normalizer: &mut LogNormalizer, metadata: LogMetadata) -> Self {
        let (words, normalized_text) = normalizer.extract_words_and_normalized(&log_line);

        let mut members_set = AHashSet::new();
        members_set.insert(log_line.clone());

        let mut sources_set = AHashSet::new();
        sources_set.insert((
            metadata.namespace.clone(),
            metadata.pod.clone(),
            metadata.container.clone(),
        ));

        LogCluster {
            representative: log_line.clone(),
            normalized_text,
            words,
            members: vec![log_line],
            members_set,
            count: 1,
            sources: vec![metadata],
            sources_set,
        }
    }

    fn add_member(&mut self, log_line: String, metadata: LogMetadata) {
        // Only add if not already present (O(1) lookup)
        if !self.members_set.contains(&log_line) {
            self.members.push(log_line.clone());
            self.members_set.insert(log_line);
            self.count += 1;
        }

        let source_key = (
            metadata.namespace.clone(),
            metadata.pod.clone(),
            metadata.container.clone(),
        );

        // Add source metadata if not already present (O(1) lookup)
        if !self.sources_set.contains(&source_key) {
            self.sources.push(metadata);
            self.sources_set.insert(source_key);
        }
    }

    fn similarity_to(&self, normalized_text: &str) -> f64 {
        sorensen_dice_similarity(&self.normalized_text, normalized_text)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let client = Client::try_default().await?;

    // Get pod list - use all namespaces if none specified
    let pods_api: Api<Pod> = if let Some(ref namespace) = cli.namespace {
        Api::namespaced(client.clone(), namespace)
    } else {
        Api::all(client.clone())
    };

    let mut list_params = ListParams::default();
    if let Some(ref label) = cli.label {
        list_params = list_params.labels(label);
    }

    let pod_list = pods_api.list(&list_params).await?;

    // Remove all println! except for progress bar and final stats in human mode
    if !cli.json {
        println!("Found {} pods", pod_list.items.len());
    }

    let mut all_log_lines = Vec::new();
    let mut error_list = Vec::new();

    // Create tasks for concurrent log fetching
    let mut tasks = Vec::new();

    for pod in pod_list.items {
        let pod_name = pod.metadata.name.unwrap_or_default();
        let namespace = pod.metadata.namespace.unwrap_or_default();

        // Process all containers in the pod
        if let Some(containers) = pod.spec.as_ref().map(|s| &s.containers) {
            for container in containers {
                let container_name = container.name.clone();
                let client_clone = client.clone();
                let pod_name_clone = pod_name.clone();
                let namespace_clone = namespace.clone();
                let since = cli.since.clone();

                let task = task::spawn(async move {
                    // Create namespace-specific client for log fetching
                    let namespace_pods_api: Api<Pod> =
                        Api::namespaced(client_clone, &namespace_clone);

                    let mut log_params = LogParams {
                        container: Some(container_name.clone()),
                        ..Default::default()
                    };

                    if let Some(ref since_str) = since {
                        if let Ok(since_time) = DateTime::parse_from_rfc3339(since_str) {
                            log_params.since_time = Some(since_time.with_timezone(&Utc));
                        }
                    }

                    match namespace_pods_api.logs(&pod_name_clone, &log_params).await {
                        Ok(logs) => {
                            let lines: Vec<String> = logs.lines().map(|s| s.to_string()).collect();
                            // Create metadata for each log line
                            let log_entries: Vec<(String, LogMetadata)> = lines
                                .into_iter()
                                .map(|line| {
                                    (
                                        line,
                                        LogMetadata {
                                            namespace: namespace_clone.clone(),
                                            pod: pod_name_clone.clone(),
                                            container: container_name.clone(),
                                        },
                                    )
                                })
                                .collect();

                            Ok(log_entries)
                        }
                        Err(e) => {
                            let error_entry = ErrorEntry {
                                error: e.to_string(),
                                location: format!(
                                    "pod: {}, container: {}",
                                    pod_name_clone, container_name
                                ),
                                timestamp: Utc::now().to_rfc3339(),
                            };
                            Err(error_entry)
                        }
                    }
                });

                tasks.push(task);
            }
        }
    }

    // Create progress bar for log fetching
    let fetch_progress = {
        let pb = ProgressBar::new(tasks.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}",
                )
                .unwrap()
                .progress_chars("#>-"),
        );
        pb.set_message("Fetching logs from pods...");
        if cli.json {
            pb.set_draw_target(ProgressDrawTarget::stderr());
        }
        Some(pb)
    };

    // Execute tasks with concurrency limit and update progress bar in real-time
    let concurrent_stream = stream::iter(tasks).buffer_unordered(cli.fetch_limit);

    let mut results_stream = concurrent_stream;

    // Process results as they come in to show real-time progress
    while let Some(result) = results_stream.next().await {
        match result {
            Ok(Ok(log_entries)) => {
                all_log_lines.extend(log_entries);
            }
            Ok(Err(error_entry)) => {
                error_list.push(error_entry);
            }
            Err(e) => {
                if !cli.json {
                    eprintln!("Task error: {}", e);
                }
                error_list.push(ErrorEntry {
                    error: e.to_string(),
                    location: "Task execution".to_string(),
                    timestamp: Utc::now().to_rfc3339(),
                });
            }
        }

        // Update progress bar after each completed task
        if let Some(ref pb) = fetch_progress {
            pb.inc(1);
        }
    }

    if let Some(pb) = fetch_progress {
        pb.finish_with_message("Log fetching complete!");
    }
    if !cli.json {
        println!(
            "Finished fetching all logs. Total lines: {}",
            all_log_lines.len()
        );
    }

    // Apply filtering if filter strings are provided
    let filtered_logs = if !cli.filter.is_empty() {
        let original_count = all_log_lines.len();

        // Create progress bar for filtering
        let filter_progress = if !cli.json {
            let pb = ProgressBar::new(original_count as u64);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("{spinner:.green} [{elapsed_precise}] [{bar:40.yellow/blue}] {pos}/{len} {msg}")
                    .unwrap()
                    .progress_chars("#>-"),
            );
            pb.set_message("Filtering logs...");
            Some(pb)
        } else {
            let pb = ProgressBar::new(original_count as u64);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("{spinner:.green} [{elapsed_precise}] [{bar:40.yellow/blue}] {pos}/{len} {msg}")
                    .unwrap()
                    .progress_chars("#>-"),
            );
            pb.set_message("Filtering logs...");
            pb.set_draw_target(ProgressDrawTarget::stderr());
            Some(pb)
        };

        let filtered: Vec<(String, LogMetadata)> = all_log_lines
            .into_iter()
            .enumerate()
            .filter_map(|(i, (line, metadata))| {
                if let Some(ref pb) = filter_progress {
                    if i % 100 == 0 {
                        // Update every 100 items to avoid too frequent updates
                        pb.set_position(i as u64);
                    }
                }

                let line_lower = line.to_lowercase();
                let matches = cli
                    .filter
                    .iter()
                    .any(|filter| line_lower.contains(&filter.to_lowercase()));

                if matches {
                    Some((line, metadata))
                } else {
                    None
                }
            })
            .collect();

        if let Some(pb) = filter_progress {
            pb.finish_with_message("Filtering complete!");
        }
        if !cli.json {
            println!(
                "Applied filter {:?}: {} lines -> {} lines ({:.1}% retained)",
                cli.filter,
                original_count,
                filtered.len(),
                if original_count > 0 {
                    (filtered.len() as f64 / original_count as f64) * 100.0
                } else {
                    0.0
                }
            );
        }
        filtered
    } else {
        all_log_lines
    };

    let clusters = cluster_logs_parallel(
        filtered_logs,
        cli.threshold,
        cli.json,
        cli.batch_size_factor,
        cli.no_word_filter,
        cli.member_limit, // Pass member_limit here
    )
    .await;

    if cli.json {
        // The `clusters` vector now already respects `cli.member_limit`
        // if cli.member_limit > 0, so direct truncation here is no longer needed.
        let output = JsonOutput {
            errors: error_list,
            clusters,
        };
        let json_output = serde_json::to_string_pretty(&output)?;
        println!("{}", json_output);
    } else {
        println!("\n--- Log Clusters ---");
        for (i, cluster) in clusters.iter().enumerate() {
            println!("Cluster {}: (Count: {})", i + 1, cluster.count);
            println!("  Representative: {}", cluster.representative);
            println!("  Key words: {:?}", cluster.words);
            println!("  Sources:");
            for source in &cluster.sources {
                println!(
                    "    - {}/{}/{}",
                    source.namespace, source.pod, source.container
                );
            }
        }
    }

    Ok(())
}

// Parallel clustering using work-stealing with memory-mapped storage
async fn cluster_logs_parallel(
    logs: Vec<(String, LogMetadata)>,
    similarity_threshold: f64,
    json_mode: bool,
    batch_size_factor: usize,
    no_word_filter: bool,
    member_limit: usize, // Added member_limit parameter
) -> Vec<LogCluster> {
    if logs.is_empty() {
        return Vec::new();
    }

    if !json_mode {
        println!(
            "Starting parallel memory-mapped clustering with {} log lines...",
            logs.len()
        );
    }

    // Dynamically calculate MmapStringPool capacity
    let estimated_total_string_size_bytes: usize = logs.iter().map(|(line, _)| line.len()).sum();
    // Apply a multiplier (e.g., 2.5x) to account for normalized strings, words, and overhead.
    // Ensure a minimum capacity (e.g., 64MB) and a maximum (e.g., 2048MB).
    let calculated_capacity_mb =
        ((estimated_total_string_size_bytes as f64 * 2.5) / (1024.0 * 1024.0)).ceil() as usize;
    let capacity_mb = calculated_capacity_mb.clamp(4, 2048);

    if !json_mode {
        println!(
            "Estimated total string data size: {:.2} MB",
            estimated_total_string_size_bytes as f64 / (1024.0 * 1024.0)
        );
        println!(
            "Calculated MmapStringPool capacity: {} MB (Min: 4MB, Max: 2048MB)",
            capacity_mb
        );
    }

    // Create memory-mapped string pool
    let string_pool = match MmapStringPool::new(capacity_mb) {
        Ok(pool) => Arc::new(Mutex::new(pool)),
        Err(e) => {
            if !json_mode {
                eprintln!(
                    "Failed to create memory-mapped pool, falling back to regular clustering: {}",
                    e
                );
            }
            return cluster_logs_fallback(logs, similarity_threshold, json_mode, no_word_filter);
        }
    };

    let start = Instant::now();

    // Optimized batch size and worker count for better parallelism with large datasets
    let base_batch_size = if logs.len() > 100_000 {
        (logs.len() / 1000).max(500)
    } else {
        1000
    };
    let batch_size = base_batch_size * batch_size_factor;
    let num_workers = std::thread::available_parallelism()
        .map(|n| n.get().min(16))
        .unwrap_or(8);

    let batches: Vec<Vec<WorkItem>> = logs
        .into_iter()
        .map(|(log_line, metadata)| WorkItem { log_line, metadata })
        .collect::<Vec<_>>()
        .chunks(batch_size)
        .map(|chunk| chunk.to_vec())
        .collect();

    if !json_mode {
        println!(
            "Split into {} batches with {} workers (batch size: {})",
            batches.len(),
            num_workers,
            batch_size
        );
    }

    // Create progress bar for batch processing
    let batch_progress = {
        let pb = ProgressBar::new(batches.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.magenta/blue}] {pos}/{len} {msg}",
                )
                .unwrap()
                .progress_chars("#>-"),
        );
        pb.set_message("Clustering batches...");
        if json_mode {
            pb.set_draw_target(ProgressDrawTarget::stderr());
        }
        Some(Arc::new(Mutex::new(pb)))
    };

    // Use crossbeam_channel for work distribution
    let (sender, receiver) = crossbeam_channel::unbounded();
    for batch in batches {
        sender.send(batch).unwrap();
    }
    drop(sender); // Close the channel

    // Spawn worker tasks
    let mut worker_handles = Vec::new();
    for _ in 0..num_workers {
        let receiver = receiver.clone();
        let string_pool = Arc::clone(&string_pool);
        let batch_progress = batch_progress.as_ref().map(Arc::clone);

        let handle = task::spawn(async move {
            let mut normalizer = LogNormalizer::new_with_filter(no_word_filter);
            let mut worker_clusters: Vec<OptimizedLogCluster> = Vec::new();
            let _batches_processed = 0; // Renamed to suppress warning

            while let Ok(batch) = receiver.recv() {
                for work_item in batch {
                    process_log_item(
                        work_item,
                        &mut worker_clusters,
                        &mut normalizer,
                        similarity_threshold,
                        &string_pool,
                    )
                    .await;
                }

                // Update progress bar directly after each batch
                if let Some(ref pb_mutex) = batch_progress {
                    let pb = pb_mutex.lock();
                    pb.inc(1);
                }
            }

            worker_clusters
        });

        worker_handles.push(handle);
    }

    // Collect results from all workers
    let mut all_clusters: Vec<OptimizedLogCluster> = Vec::new();
    for handle in worker_handles {
        if let Ok(worker_clusters) = handle.await {
            all_clusters.extend(worker_clusters);
        }
    }

    let clustering_duration = start.elapsed();

    // Finish batch processing progress bar
    if let Some(pb_mutex) = batch_progress {
        let pb = pb_mutex.lock();
        pb.finish_with_message("Batch processing complete!");
    }

    // Merge similar clusters across workers using the original merge function
    let merge_start = Instant::now();
    let merged_clusters = merge_clusters(
        all_clusters,
        similarity_threshold,
        &string_pool,
        None, // No progress bar for merging in human mode
    )
    .await;
    let merge_duration = merge_start.elapsed();

    if !json_mode {
        println!(
            "Merging complete, final clusters: {}",
            merged_clusters.len()
        );
    }

    // Convert optimized clusters back to regular clusters
    let string_pool_guard = string_pool.lock();
    let result_clusters: Vec<LogCluster> = merged_clusters
        .iter()
        .map(|opt_cluster| opt_cluster.to_log_cluster(&string_pool_guard, member_limit)) // Pass member_limit
        .collect();
    drop(string_pool_guard);

    // Sort by count, descending
    let mut sorted_clusters = result_clusters;
    sorted_clusters.sort_by(|a, b| b.count.cmp(&a.count));

    if !json_mode {
        println!(
            "Parallel clustering complete! Created {} clusters",
            sorted_clusters.len()
        );
        let pool = string_pool.lock();
        println!(
            "DEBUG_PERFORMANCE: Parallel clustering timing: clustering phase = {:?}, merging phase = {:?}, string pool lock count = {}",
            clustering_duration,
            merge_duration,
            pool.lock_count.load(std::sync::atomic::Ordering::Relaxed)
        );
    }

    sorted_clusters
}

// Merge clusters from different workers
async fn merge_clusters(
    mut clusters: Vec<OptimizedLogCluster>,
    similarity_threshold: f64,
    string_pool: &SharedStringPool,
    progress_bar: Option<&ProgressBar>,
) -> Vec<OptimizedLogCluster> {
    if clusters.len() <= 1 {
        return clusters;
    }

    // Sort by count to prioritize larger clusters
    clusters.sort_by(|a, b| b.count.cmp(&a.count));

    let mut merged: Vec<OptimizedLogCluster> = Vec::new();

    for (i, cluster) in clusters.into_iter().enumerate() {
        let mut found_merge = false;
        let string_pool_guard = lock_and_count(string_pool);

        if let Some(cluster_text) = string_pool_guard.get_string(cluster.normalized_text_id) {
            // Check if this cluster can be merged with any existing one
            for merged_cluster in &mut merged {
                if let Some(merged_text) =
                    string_pool_guard.get_string(merged_cluster.normalized_text_id)
                {
                    let similarity = sorensen_dice_similarity(&cluster_text, &merged_text);
                    if similarity >= similarity_threshold {
                        // Merge clusters with O(1) deduplication

                        // Deduplicate member_ids using HashSet
                        for &member_id in &cluster.member_ids {
                            if !merged_cluster.member_ids_set.contains(&member_id) {
                                merged_cluster.member_ids.push(member_id);
                                merged_cluster.member_ids_set.insert(member_id);
                            }
                        }

                        // Deduplicate sources using HashSet
                        for source in &cluster.sources {
                            let source_key =
                                (source.namespace_id, source.pod_id, source.container_id);
                            if !merged_cluster.sources_set.contains(&source_key) {
                                merged_cluster.sources.push(source.clone());
                                merged_cluster.sources_set.insert(source_key);
                            }
                        }

                        // Recalculate count based on actual unique members
                        merged_cluster.count = merged_cluster.member_ids.len();
                        found_merge = true;
                        break;
                    }
                }
            }
        }
        drop(string_pool_guard);

        if !found_merge {
            merged.push(cluster);
        }

        // Update progress bar
        if let Some(pb) = progress_bar {
            pb.set_position((i + 1) as u64);
        }
    }

    merged
}

// Helper to increment lock count
fn lock_and_count<'a>(pool: &'a SharedStringPool) -> parking_lot::MutexGuard<'a, MmapStringPool> {
    let guard = pool.lock();
    guard.lock_count.fetch_add(1, Ordering::Relaxed);
    guard
}

// Process a single log item with the shared string pool
async fn process_log_item(
    work_item: WorkItem,
    clusters: &mut Vec<OptimizedLogCluster>,
    normalizer: &mut LogNormalizer,
    similarity_threshold: f64,
    string_pool: &SharedStringPool,
) {
    let log_line = work_item.log_line;
    let metadata = work_item.metadata;

    if log_line.trim().is_empty() {
        return;
    }

    // Extract words and normalized text without holding the lock
    let (words, normalized_text) = normalizer.extract_words_and_normalized(&log_line);

    // Skip lines with no meaningful words
    if words.is_empty() {
        return;
    }

    let mut best_match_index: Option<usize> = None;
    let mut max_similarity = 0.0;

    // Limit cluster checking for performance
    let check_limit = if clusters.len() > 25 {
        25 // More aggressive limiting for memory efficiency
    } else {
        clusters.len()
    };

    // Only lock the string pool for the similarity checks
    {
        let string_pool_guard = lock_and_count(string_pool);
        for (i, cluster) in clusters.iter().enumerate().take(check_limit) {
            let similarity = cluster.similarity_to(&normalized_text, &string_pool_guard);
            if similarity > max_similarity {
                max_similarity = similarity;
                if similarity >= similarity_threshold {
                    best_match_index = Some(i);
                    break; // Early exit on good match
                }
            }
        }
    }

    if let Some(index) = best_match_index {
        // Only lock for the minimal time needed to add a member
        let mut string_pool_guard = lock_and_count(string_pool);
        clusters[index].add_member(&log_line, metadata, &mut string_pool_guard);
    } else {
        // Limit total clusters to prevent memory issues
        if clusters.len() < 50 {
            // Only lock for the minimal time needed to create a new cluster
            let mut string_pool_guard = lock_and_count(string_pool);
            let new_cluster = OptimizedLogCluster::new(
                &log_line,
                words,
                normalized_text,
                metadata,
                &mut string_pool_guard,
            );
            clusters.push(new_cluster);
        }
    }
}

// Fallback clustering function (original implementation)
fn cluster_logs_fallback(
    logs: Vec<(String, LogMetadata)>,
    similarity_threshold: f64,
    json_mode: bool,
    no_word_filter: bool,
) -> Vec<LogCluster> {
    let mut normalizer = LogNormalizer::new_with_filter(no_word_filter);
    cluster_logs(logs, similarity_threshold, &mut normalizer, json_mode)
}

// Ultra-fast clustering using sorensen_dice similarity
fn cluster_logs(
    logs: Vec<(String, LogMetadata)>,
    similarity_threshold: f64,
    normalizer: &mut LogNormalizer,
    json_mode: bool,
) -> Vec<LogCluster> {
    let mut clusters: Vec<LogCluster> = Vec::new();

    if logs.is_empty() {
        return clusters;
    }

    if !json_mode {
        println!(
            "Starting fast sorensen_dice-based clustering with {} log lines...",
            logs.len()
        );
    }

    // Create progress bar for fallback clustering
    let clustering_progress = if !json_mode {
        let pb = ProgressBar::new(logs.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}",
                )
                .unwrap()
                .progress_chars("#>-"),
        );
        pb.set_message("Clustering logs (fallback mode)...");
        Some(pb)
    } else {
        None
    };

    let mut processed = 0;

    for (log_line, metadata) in logs {
        if log_line.trim().is_empty() {
            continue;
        }

        processed += 1;
        if !json_mode && processed % 500 == 0 {
            println!(
                "Processed {} lines, {} clusters so far",
                processed,
                clusters.len()
            );
        }

        // Update progress bar
        if let Some(ref pb) = clustering_progress {
            if processed % 10 == 0 {
                // Update every 10 items to avoid too frequent updates
                pb.set_position(processed as u64);
            }
        }

        let (words, normalized_text) = normalizer.extract_words_and_normalized(&log_line);

        // Skip lines with no meaningful words
        if words.is_empty() {
            continue;
        }

        let mut best_match_index: Option<usize> = None;
        let mut max_similarity = 0.0;

        // Limit cluster checking for performance
        let check_limit = if clusters.len() > 500 {
            100
        } else {
            clusters.len()
        };

        for (i, _a) in clusters.iter().enumerate().take(check_limit) {
            let similarity = clusters[i].similarity_to(&normalized_text);
            if similarity > max_similarity {
                max_similarity = similarity;
                if similarity >= similarity_threshold {
                    best_match_index = Some(i);
                    break; // Early exit on good match
                }
            }
        }

        if let Some(index) = best_match_index {
            clusters[index].add_member(log_line, metadata);
        } else {
            // Limit total clusters to prevent performance degradation
            if clusters.len() < 1000 {
                clusters.push(LogCluster::new(log_line, normalizer, metadata));
            }
        }
    }

    // Finish clustering progress bar
    if let Some(pb) = clustering_progress {
        pb.finish_with_message("Clustering complete!");
    }

    // Sort by count, descending
    clusters.sort_by(|a, b| b.count.cmp(&a.count));

    if !json_mode {
        println!(
            "Clustering complete! Created {} clusters from {} log lines",
            clusters.len(),
            processed
        );
    }

    clusters
}
