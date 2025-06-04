// Kubernetes log clustering with jaccard similarity
use chrono::{DateTime, Utc};
use clap::Parser;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{ListParams, LogParams};
use kube::{Api, Client};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use textdistance::str::jaccard;

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
    #[clap(short = 't', long, default_value = "0.75")]
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
}

// Ultra-fast log normalizer using word extraction and string normalization
struct LogNormalizer {
    cache: HashMap<String, (Vec<String>, String)>,
}

impl LogNormalizer {
    fn new() -> Self {
        LogNormalizer {
            cache: HashMap::new(),
        }
    }

    fn extract_words_and_normalized(&mut self, log_line: &str) -> (Vec<String>, String) {
        // Check cache first
        if let Some(cached) = self.cache.get(log_line) {
            return cached.clone();
        }

        let (words, normalized) = self.fast_extract_words_and_normalized(log_line);

        // Cache the result (limit cache size)
        if self.cache.len() < 5000 {
            self.cache
                .insert(log_line.to_string(), (words.clone(), normalized.clone()));
        }

        (words, normalized)
    }

    fn fast_extract_words_and_normalized(&self, log_line: &str) -> (Vec<String>, String) {
        let mut words = Vec::new();
        let mut normalized_chars = Vec::new();
        let mut current_word = String::new();

        for ch in log_line.chars() {
            if ch.is_alphabetic() {
                let lower_ch = ch.to_ascii_lowercase();
                current_word.push(lower_ch);
                normalized_chars.push(lower_ch);
            } else {
                if !current_word.is_empty() && current_word.len() >= 3 {
                    // Only keep meaningful words (3+ chars)
                    words.push(current_word.clone());
                }
                current_word.clear();
                // Add space to normalized string for word separation
                if !normalized_chars.is_empty() && normalized_chars.last() != Some(&' ') {
                    normalized_chars.push(' ');
                }
            }
        }

        // Add the last word if it exists
        if !current_word.is_empty() && current_word.len() >= 3 {
            words.push(current_word);
        }

        let normalized_string: String = normalized_chars.into_iter().collect();
        (words, normalized_string.trim().to_string())
    }
}

// jaccard-based similarity calculation
fn jaccard_similarity(text1: &str, text2: &str) -> f64 {
    if text1.is_empty() && text2.is_empty() {
        return 1.0;
    }

    jaccard(text1, text2)
}

// Metadata for tracking log source
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LogMetadata {
    namespace: String,
    pod: String,
    container: String,
}

// Simplified LogCluster with jaccard-based clustering
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LogCluster {
    representative: String,
    normalized_text: String,
    words: Vec<String>,
    members: Vec<String>,
    count: usize,
    sources: Vec<LogMetadata>,
}

impl LogCluster {
    fn new(log_line: String, normalizer: &mut LogNormalizer, metadata: LogMetadata) -> Self {
        let (words, normalized_text) = normalizer.extract_words_and_normalized(&log_line);

        LogCluster {
            representative: log_line.clone(),
            normalized_text,
            words,
            members: vec![log_line],
            count: 1,
            sources: vec![metadata],
        }
    }

    fn add_member(&mut self, log_line: String, metadata: LogMetadata) {
        self.members.push(log_line);
        self.count += 1;

        // Add source metadata if not already present
        if !self.sources.iter().any(|s| {
            s.namespace == metadata.namespace
                && s.pod == metadata.pod
                && s.container == metadata.container
        }) {
            self.sources.push(metadata);
        }
    }

    fn similarity_to(&self, normalized_text: &str) -> f64 {
        jaccard_similarity(&self.normalized_text, normalized_text)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let client = Client::try_default().await?;
    let pods: Api<Pod> = if let Some(ref namespace) = cli.namespace {
        Api::namespaced(client, namespace)
    } else {
        Api::all(client)
    };

    let mut list_params = ListParams::default();
    if let Some(ref label) = cli.label {
        list_params = list_params.labels(label);
    }

    let pod_list = pods.list(&list_params).await?;

    if !cli.json {
        println!("Found {} pods", pod_list.items.len());
    }

    let mut all_log_lines = Vec::new();
    let mut normalizer = LogNormalizer::new();

    for pod in pod_list.items {
        let pod_name = pod.metadata.name.unwrap_or_default();
        let namespace = pod.metadata.namespace.unwrap_or_default();

        if let Some(ref containers) = pod.spec.and_then(|s| s.containers.into_iter().next()) {
            let container_name = &containers.name;

            if !cli.json {
                println!(
                    "Fetching logs from pod: {}, container: {}",
                    pod_name, container_name
                );
            }

            let mut log_params = LogParams::default();
            if let Some(ref since_str) = cli.since {
                if let Ok(since_time) = DateTime::parse_from_rfc3339(since_str) {
                    log_params.since_time = Some(since_time.with_timezone(&Utc));
                }
            }

            match pods.logs(&pod_name, &log_params).await {
                Ok(logs) => {
                    let lines: Vec<String> = logs.lines().map(|s| s.to_string()).collect();
                    if !cli.json {
                        println!("  Fetched {} lines", lines.len());
                    }

                    // Add lines with metadata
                    for line in lines {
                        let metadata = LogMetadata {
                            namespace: namespace.clone(),
                            pod: pod_name.clone(),
                            container: container_name.clone(),
                        };
                        all_log_lines.push((line, metadata));
                    }
                }
                Err(e) => {
                    if !cli.json {
                        eprintln!("Error fetching logs for pod {}: {}", pod_name, e);
                    }
                }
            }
        }
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
        let filtered: Vec<(String, LogMetadata)> = all_log_lines
            .into_iter()
            .filter(|(line, _metadata)| {
                let line_lower = line.to_lowercase();
                cli.filter
                    .iter()
                    .any(|filter| line_lower.contains(&filter.to_lowercase()))
            })
            .collect();

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

    let clusters = cluster_logs(filtered_logs, cli.threshold, &mut normalizer, cli.json);

    if cli.json {
        // Apply member limit if specified for JSON output
        let limited_clusters: Vec<LogCluster> = if cli.member_limit > 0 {
            clusters
                .into_iter()
                .map(|mut cluster| {
                    if cluster.members.len() > cli.member_limit {
                        cluster.members.truncate(cli.member_limit);
                    }
                    cluster
                })
                .collect()
        } else {
            clusters
        };

        let json_output = serde_json::to_string_pretty(&limited_clusters)?;
        println!("{}", json_output);
    } else {
        println!("\n--- Log Clusters ---");
        for (i, cluster) in clusters.iter().enumerate() {
            println!("Cluster {}: (Count: {})", i + 1, cluster.count);
            println!("  Representative: {}", cluster.representative);
            println!("  Key words: {:?}", cluster.words);

            // Show sources (namespace/pod/container)
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

// Ultra-fast clustering using jaccard similarity
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
            "Starting fast jaccard-based clustering with {} log lines...",
            logs.len()
        );
    }

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
