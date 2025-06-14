// Kubernetes log clustering with sorensen_dice similarity
use ahash::{AHashMap, AHashSet};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use parking_lot::Mutex;
use serde::{Deserialize, Deserializer, Serialize, Serializer}; // Added Deserializer, Serializer
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use textdistance::str::sorensen_dice;
use tokio::task;

// Helper module for Arc<String> serialization/deserialization
mod arc_string_serde {
    use super::*;
    use std::sync::Arc;

    pub fn serialize<S>(arc_str: &Arc<String>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(arc_str.as_str())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Arc<String>, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer).map(Arc::new)
    }
}

// Bigram cache for optimized string similarity calculations
#[derive(Debug)]
pub struct BigramCache {
    cache: AHashMap<String, Vec<String>>,
    max_size: usize,
}

impl BigramCache {
    pub fn new(max_size: usize) -> Self {
        BigramCache {
            cache: AHashMap::new(),
            max_size,
        }
    }

    pub fn get_bigrams(&mut self, text: &str) -> Vec<String> {
        if let Some(bigrams) = self.cache.get(text) {
            return bigrams.clone();
        }

        let bigrams = self.generate_bigrams(text);

        if self.cache.len() >= self.max_size {
            // Simple eviction: remove a quarter of the cache if full.
            // A more sophisticated LRU or LFU policy could be used here.
            let to_remove_count = self.max_size / 4;
            let keys_to_remove: Vec<String> =
                self.cache.keys().take(to_remove_count).cloned().collect();
            for key in keys_to_remove {
                self.cache.remove(&key);
            }
        }

        self.cache.insert(text.to_string(), bigrams.clone());
        bigrams
    }

    fn generate_bigrams(&self, text: &str) -> Vec<String> {
        if text.len() < 2 {
            // For single character strings or empty strings, consider the string itself as a "bigram"
            // or return an empty vec. Returning the string itself might be more useful for similarity.
            return vec![text.to_string()];
        }
        text.chars()
            .collect::<Vec<char>>()
            .windows(2)
            .map(|window| window.iter().collect::<String>())
            .collect()
    }
}

// Simplified Optimized Similarity Calculator
#[derive(Debug)]
pub struct SimpleOptimizedSimilarityCalculator {
    bigram_cache: BigramCache,
    similarity_threshold: f64, // Store the threshold it was created with
}

impl SimpleOptimizedSimilarityCalculator {
    pub fn new(similarity_threshold: f64, cache_size: usize) -> Self {
        SimpleOptimizedSimilarityCalculator {
            bigram_cache: BigramCache::new(cache_size),
            similarity_threshold,
        }
    }

    fn passes_char_frequency_check(&self, text1: &str, text2: &str) -> bool {
        let mut chars1 = AHashMap::new();
        let mut chars2 = AHashMap::new();

        for ch in text1.chars() {
            *chars1.entry(ch).or_insert(0) += 1;
        }
        for ch in text2.chars() {
            *chars2.entry(ch).or_insert(0) += 1;
        }

        let mut common_chars = 0;
        let total_chars1: usize = chars1.values().sum();
        let total_chars2: usize = chars2.values().sum();

        if total_chars1 == 0 && total_chars2 == 0 {
            return true; // Both empty, considered similar in this context
        }
        if total_chars1 == 0 || total_chars2 == 0 {
            return false; // One empty, one not
        }

        for (ch, &count1) in &chars1 {
            if let Some(&count2) = chars2.get(ch) {
                common_chars += count1.min(count2);
            }
        }
        let char_similarity = (2.0 * common_chars as f64) / (total_chars1 + total_chars2) as f64;
        // Use a slightly relaxed factor for the char frequency check compared to the main threshold
        char_similarity >= (self.similarity_threshold * 0.75)
    }

    fn calculate_cached_sorensen_dice(&mut self, text1: &str, text2: &str) -> f64 {
        let bigrams1 = self.bigram_cache.get_bigrams(text1);
        let bigrams2 = self.bigram_cache.get_bigrams(text2);

        if bigrams1.is_empty() && bigrams2.is_empty() {
            return 1.0;
        }
        if bigrams1.is_empty() || bigrams2.is_empty() {
            return 0.0;
        }

        let set1: AHashSet<&String> = bigrams1.iter().collect();
        let set2: AHashSet<&String> = bigrams2.iter().collect();
        let intersection_size = set1.intersection(&set2).count();

        (2.0 * intersection_size as f64) / (bigrams1.len() + bigrams2.len()) as f64
    }

    pub fn calculate_similarity(&mut self, text1: &str, text2: &str) -> f64 {
        // Early exit #1: Identical strings
        if text1 == text2 {
            return 1.0;
        }

        // Early exit #2: Empty strings
        if text1.is_empty() && text2.is_empty() {
            return 1.0; // Both empty
        }
        if text1.is_empty() || text2.is_empty() {
            return 0.0; // One empty
        }

        // Early exit #3: Length difference too large
        // (From performance.md: "Add quick rejection for obviously dissimilar strings based on length difference")
        let len1 = text1.len();
        let len2 = text2.len();
        let min_len = len1.min(len2) as f64;
        let max_len = len1.max(len2) as f64;

        if max_len == 0.0 {
            return 1.0;
        } // Should be caught by empty string check, but good for safety
        let len_ratio = min_len / max_len;

        // If one string is more than 3x longer (ratio < 0.33), or if the length ratio implies
        // that the Sorensen-Dice score cannot possibly meet the threshold.
        // Max possible Sorensen-Dice is 2 * min_len / (min_len + max_len).
        // If 2 * min_len / (min_len + max_len) < threshold, then they can\'t match.
        // This simplifies to len_ratio < threshold / (2.0 - threshold)
        if len_ratio < self.similarity_threshold / (2.0 - self.similarity_threshold) {
            // A more aggressive check than just 0.33, tied to the threshold.
            // For threshold 0.75, this is len_ratio < 0.75 / 1.25 = 0.6
            // For threshold 0.5, this is len_ratio < 0.5 / 1.5 = 0.33
            return 0.0;
        }

        // Early exit #4: Character frequency pre-filtering
        // (From performance.md: "Implement character frequency pre-filtering")
        // Only run this check for strings of a certain minimum length to avoid overhead on tiny strings.
        const MIN_LEN_FOR_CHAR_FREQ_CHECK: usize = 15; // Adjusted from 20
        if len1 > MIN_LEN_FOR_CHAR_FREQ_CHECK
            && len2 > MIN_LEN_FOR_CHAR_FREQ_CHECK
            && !self.passes_char_frequency_check(text1, text2)
        {
            return 0.0;
        }

        // Full Sorensen-Dice calculation using cached bigrams
        self.calculate_cached_sorensen_dice(text1, text2)
    }
}

#[derive(Serialize, Debug)]
pub struct ErrorEntry {
    pub error: String,
    pub location: String,
    pub timestamp: String,
}

// Memory-mapped string pool for efficient string storage
#[derive(Debug)]
pub struct MmapStringPool {
    buffer: Vec<u8>, // Safe byte buffer instead of memory mapping
    string_to_id: AHashMap<String, usize>,
    id_to_offset: Vec<(usize, usize)>, // (offset, length) pairs
    current_offset: usize,
    capacity: usize,
    #[allow(dead_code)]
    pub lock_count: Arc<AtomicUsize>, // For lock contention measurement
}

impl MmapStringPool {
    pub fn new(capacity_mb: usize) -> std::io::Result<Self> {
        let capacity = capacity_mb * 1024 * 1024; // Convert MB to bytes

        // Pre-allocate buffer with the required capacity and initialize with zeros
        let buffer = vec![0; capacity];

        Ok(MmapStringPool {
            buffer,
            string_to_id: AHashMap::new(),
            id_to_offset: Vec::new(),
            current_offset: 0,
            capacity,
            lock_count: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn intern_string(&mut self, s: &str) -> usize {
        if let Some(&id) = self.string_to_id.get(s) {
            return id;
        }

        let string_bytes = s.as_bytes();
        let needed_space = string_bytes.len() + 4; // 4 bytes for length prefix

        if self.current_offset + needed_space > self.capacity {
            // Pool is full, return a fallback ID
            return usize::MAX;
        }

        // Write length prefix (little-endian u32)
        let len_bytes = (string_bytes.len() as u32).to_le_bytes();
        self.buffer[self.current_offset..self.current_offset + 4].copy_from_slice(&len_bytes);

        // Write string data
        let string_start = self.current_offset + 4;
        self.buffer[string_start..string_start + string_bytes.len()].copy_from_slice(string_bytes);

        let id = self.id_to_offset.len();
        self.id_to_offset.push((self.current_offset, needed_space));

        // Only intern short strings (e.g., <256 bytes) to reduce memory usage
        if s.len() < 256 {
            self.string_to_id.insert(s.to_string(), id);
        }

        self.current_offset += needed_space;

        id
    }

    pub fn get_string(&self, id: usize) -> Option<String> {
        if id == usize::MAX || id >= self.id_to_offset.len() {
            return None;
        }

        let (offset, _size) = self.id_to_offset[id];
        // Read length prefix
        let len_bytes = &self.buffer[offset..offset + 4];
        let string_len =
            u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;

        // Read string data
        let string_start = offset + 4;
        if string_start + string_len <= self.buffer.len() {
            let string_bytes = &self.buffer[string_start..string_start + string_len];
            return String::from_utf8(string_bytes.to_vec()).ok();
        }
        None
    }
}

// Thread-safe string pool wrapper
pub type SharedStringPool = Arc<Mutex<MmapStringPool>>;

// Optimized LogCluster using string pool IDs
#[derive(Debug, Clone)]
pub struct OptimizedLogCluster {
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
pub struct PackedLogMetadata {
    namespace_id: usize,
    pod_id: usize,
    container_id: usize,
}

impl OptimizedLogCluster {
    pub fn new(
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
            namespace_id: string_pool.intern_string(&metadata.namespace), // Dereference Arc
            pod_id: string_pool.intern_string(&metadata.pod),             // Dereference Arc
            container_id: string_pool.intern_string(&metadata.container), // Dereference Arc
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

    pub fn add_member(
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
            namespace_id: string_pool.intern_string(&metadata.namespace), // Dereference Arc
            pod_id: string_pool.intern_string(&metadata.pod),             // Dereference Arc
            container_id: string_pool.intern_string(&metadata.container), // Dereference Arc
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

    pub fn similarity_to(&self, normalized_text: &str, string_pool: &MmapStringPool) -> f64 {
        if let Some(stored_text) = string_pool.get_string(self.normalized_text_id) {
            sorensen_dice_similarity(&stored_text, normalized_text)
        } else {
            0.0
        }
    }

    pub fn to_log_cluster(&self, string_pool: &MmapStringPool, member_limit: usize) -> LogCluster {
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
                let namespace = string_pool.get_string(packed.namespace_id).map(Arc::new)?; // Wrap in Arc
                let pod = string_pool.get_string(packed.pod_id).map(Arc::new)?; // Wrap in Arc
                let container = string_pool.get_string(packed.container_id).map(Arc::new)?; // Wrap in Arc
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
        // This sources_set is for the final LogCluster, which will be constructed with Arcs.
        // The original LogCluster::new and add_member methods will correctly populate this if needed.
        // For now, it's initialized empty as per the optimization comment.
        let sources_set: AHashSet<(Arc<String>, Arc<String>, Arc<String>)> = AHashSet::new();

        LogCluster {
            representative,
            normalized_text,
            words,
            members,           // Members vector is now potentially smaller
            members_set,       // Now an empty set
            count: self.count, // Count still reflects the total number of original members
            sources,
            sources_set, // Now an empty set, correctly typed
        }
    }
}

// Work item for parallel processing
#[derive(Debug, Clone)]
pub struct WorkItem {
    pub log_line: String,
    pub metadata: LogMetadata,
}

// Ultra-fast log normalizer using word extraction and string normalization
pub struct LogNormalizer {
    cache: AHashMap<String, (Vec<String>, String)>,
    no_word_filter: bool,
}

impl LogNormalizer {
    pub fn new_with_filter(no_word_filter: bool) -> Self {
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

    pub fn extract_words_and_normalized(&mut self, log_line: &str) -> (Vec<String>, String) {
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
pub fn sorensen_dice_similarity(text1: &str, text2: &str) -> f64 {
    if text1.is_empty() && text2.is_empty() {
        return 1.0;
    }

    sorensen_dice(text1, text2)
}

// Metadata for tracking log source
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)] // Added PartialEq, Eq, Hash
pub struct LogMetadata {
    #[serde(with = "arc_string_serde")]
    pub namespace: Arc<String>,
    #[serde(with = "arc_string_serde")]
    pub pod: Arc<String>,
    #[serde(with = "arc_string_serde")]
    pub container: Arc<String>,
}

// Simplified LogCluster with sorensen_dice-based clustering
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogCluster {
    pub representative: String,
    pub normalized_text: String,
    pub words: Vec<String>,
    pub members: Vec<String>,
    #[serde(skip)] // Skip serialization for performance sets
    pub members_set: AHashSet<String>, // O(1) deduplication lookup
    pub count: usize,
    pub sources: Vec<LogMetadata>, // This now contains LogMetadata with Arc<String>
    #[serde(skip)] // Skip serialization for performance sets
    pub sources_set: AHashSet<(Arc<String>, Arc<String>, Arc<String>)>, // O(1) source deduplication lookup, type updated
}

#[derive(Serialize)]
pub struct JsonOutput {
    pub clusters: Vec<LogCluster>,
    pub errors: Vec<ErrorEntry>,
}

impl LogCluster {
    pub fn new(log_line: String, normalizer: &mut LogNormalizer, metadata: LogMetadata) -> Self {
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

    pub fn add_member(&mut self, log_line: String, metadata: LogMetadata) {
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

    pub fn similarity_to(&self, normalized_text: &str) -> f64 {
        sorensen_dice_similarity(&self.normalized_text, normalized_text)
    }
}

// Parallel clustering using work-stealing with memory-mapped storage
pub async fn cluster_logs_parallel(
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
            // Initialize the new similarity calculator for each worker
            let mut similarity_calculator =
                SimpleOptimizedSimilarityCalculator::new(similarity_threshold, 100); // Cache size 100 for workers

            let _batches_processed = 0; // Renamed to suppress warning

            while let Ok(batch) = receiver.recv() {
                for work_item in batch {
                    process_log_item(
                        work_item,
                        &mut worker_clusters,
                        &mut normalizer,
                        similarity_threshold, // This threshold is now primarily for the calculator's internal logic if needed, or could be removed if calculator always uses its own
                        &string_pool,
                        &mut similarity_calculator, // Pass the calculator
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
pub async fn merge_clusters(
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
    // Initialize the new similarity calculator for merging
    let mut similarity_calculator =
        SimpleOptimizedSimilarityCalculator::new(similarity_threshold, 100); // Cache size 100 for merging

    for (i, cluster) in clusters.into_iter().enumerate() {
        let mut found_merge = false;
        let string_pool_guard = lock_and_count(string_pool);

        if let Some(cluster_text) = string_pool_guard.get_string(cluster.normalized_text_id) {
            // Check if this cluster can be merged with any existing one
            for merged_cluster in &mut merged {
                if let Some(merged_text) =
                    string_pool_guard.get_string(merged_cluster.normalized_text_id)
                {
                    // Use the new calculator
                    let similarity =
                        similarity_calculator.calculate_similarity(&cluster_text, &merged_text);
                    if similarity >= similarity_threshold {
                        // Still check against the original threshold for merging decision
                        // Merge cluster into merged_cluster
                        for member_id in cluster.member_ids.iter() {
                            if !merged_cluster.member_ids_set.contains(member_id) {
                                merged_cluster.member_ids.push(*member_id);
                                merged_cluster.member_ids_set.insert(*member_id);
                            }
                        }

                        for source in cluster.sources.iter() {
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
pub fn lock_and_count<'a>(
    pool: &'a SharedStringPool,
) -> parking_lot::MutexGuard<'a, MmapStringPool> {
    let guard = pool.lock();
    guard.lock_count.fetch_add(1, Ordering::Relaxed);
    guard
}

// Process a single log item with the shared string pool and optimized similarity calculator
pub async fn process_log_item(
    work_item: WorkItem,
    clusters: &mut Vec<OptimizedLogCluster>,
    normalizer: &mut LogNormalizer,
    similarity_threshold: f64, // This might become redundant if calculator manages its own threshold strictly
    string_pool: &SharedStringPool,
    similarity_calculator: &mut SimpleOptimizedSimilarityCalculator, // Added calculator
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
            if let Some(cluster_text) = string_pool_guard.get_string(cluster.normalized_text_id) {
                // Use the new calculator
                let similarity =
                    similarity_calculator.calculate_similarity(&normalized_text, &cluster_text);
                if similarity > max_similarity {
                    max_similarity = similarity;
                    best_match_index = Some(i);
                }
            }
        }
    }

    if let Some(index) = best_match_index {
        if max_similarity >= similarity_threshold {
            // Check against the original threshold for adding to cluster
            // Only lock for the minimal time needed to add a member
            let mut string_pool_guard = lock_and_count(string_pool);
            clusters[index].add_member(&log_line, metadata, &mut string_pool_guard);
        }
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
pub fn cluster_logs_fallback(
    logs: Vec<(String, LogMetadata)>,
    similarity_threshold: f64,
    json_mode: bool,
    no_word_filter: bool,
) -> Vec<LogCluster> {
    let mut normalizer = LogNormalizer::new_with_filter(no_word_filter);
    cluster_logs(logs, similarity_threshold, &mut normalizer, json_mode)
}

// Ultra-fast clustering using sorensen_dice similarity
pub fn cluster_logs(
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
