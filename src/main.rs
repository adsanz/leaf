use chrono::{DateTime, Utc};
use clap::Parser;
use futures::stream::{self, StreamExt};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use k8s_openapi::api::core::v1::Pod;
use kube::api::{ListParams, LogParams};
use kube::{Api, Client};
use tokio::task;

use leaf::{ErrorEntry, JsonOutput, LogMetadata, cluster_logs_parallel};

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
            clusters,
            errors: error_list,
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
