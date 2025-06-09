// This file is part of the Leaf project.
use chrono;
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use leaf::{LogMetadata, cluster_logs_parallel};
use rand::Rng;
use rand::prelude::*;
use rand::rngs::StdRng;
use std::time::Duration;
use tokio::runtime::Runtime;

// Template log patterns for realistic clustering test data
const LOG_PATTERNS: &[&str] = &[
    "ERROR: Connection timeout to database server {host}:{port} after {duration}ms",
    "INFO: User {user_id} successfully authenticated from IP {ip_address}",
    "WARN: High memory usage detected: {memory_percent}% of {total_memory}MB used",
    "ERROR: Failed to process payment for order {order_id}: {error_message}",
    "INFO: Starting HTTP server on port {port} with {worker_count} workers",
    "DEBUG: Cache hit for key '{cache_key}' in {cache_time}ms",
    "ERROR: Invalid JSON payload received: {json_error}",
    "INFO: Scheduled job '{job_name}' completed successfully in {duration}s",
    "WARN: Rate limit exceeded for API key {api_key}: {request_count} requests",
    "ERROR: Database query failed: {sql_error} (query: {query})",
    "INFO: File '{filename}' uploaded successfully ({file_size} bytes)",
    "WARN: SSL certificate expires in {days} days for domain {domain}",
    "ERROR: Service '{service}' health check failed: {status_code}",
    "INFO: New user registration: {email} from {country}",
    "DEBUG: Processing webhook payload from {source} (size: {size} bytes)",
    "ERROR: Failed to send email to {recipient}: {smtp_error}",
    "INFO: Background task '{task}' queued with priority {priority}",
    "WARN: Slow query detected: {query_time}ms for table '{table}'",
    "ERROR: Authorization failed for user {user} accessing resource {resource}",
    "INFO: Cache entry '{key}' expired after {ttl} seconds",
];

// Generate semi-random realistic log data for clustering tests
fn generate_realistic_logs(num_logs: usize, seed: u64) -> Vec<(String, LogMetadata)> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut logs = Vec::with_capacity(num_logs);

    // Create clusters of similar logs with some variation
    let cluster_sizes = [
        (0.3, 1000), // 30% of logs in large clusters (common errors)
        (0.2, 500),  // 20% in medium clusters
        (0.25, 100), // 25% in small clusters
        (0.15, 50),  // 15% in very small clusters
        (0.1, 1),    // 10% unique logs
    ];

    let namespaces = [
        "default",
        "kube-system",
        "monitoring",
        "app-prod",
        "app-staging",
    ];
    let services = [
        "web-server",
        "api-gateway",
        "database",
        "cache",
        "worker",
        "scheduler",
    ];

    for i in 0..num_logs {
        // Select cluster size based on distribution
        let r: f64 = rng.random();
        let mut cumulative = 0.0;
        let mut target_cluster_size = 1;

        for (prob, size) in cluster_sizes.iter() {
            cumulative += prob;
            if r <= cumulative {
                target_cluster_size = *size;
                break;
            }
        }

        // Generate cluster ID based on target cluster size
        let cluster_id = i / target_cluster_size;
        let pattern = &LOG_PATTERNS[cluster_id % LOG_PATTERNS.len()];

        // Fill in template variables with semi-random data
        let log_line = pattern
            .replace("{host}", &format!("db-{}", rng.random_range(1..=5)))
            .replace("{port}", &format!("{}", rng.random_range(3000..=9000)))
            .replace("{duration}", &format!("{}", rng.random_range(100..=5000)))
            .replace(
                "{user_id}",
                &format!("user_{}", rng.random_range(1000..=9999)),
            )
            .replace(
                "{ip_address}",
                &format!(
                    "{}.{}.{}.{}",
                    rng.random_range(1..=255),
                    rng.random_range(1..=255),
                    rng.random_range(1..=255),
                    rng.random_range(1..=255)
                ),
            )
            .replace(
                "{memory_percent}",
                &format!("{}", rng.random_range(60..=95)),
            )
            .replace(
                "{total_memory}",
                &format!("{}", rng.random_range(1024..=16384)),
            )
            .replace(
                "{order_id}",
                &format!("ORD{}", rng.random_range(100000..=999999)),
            )
            .replace(
                "{error_message}",
                &[
                    "Insufficient funds",
                    "Card declined",
                    "Network error",
                    "Timeout",
                ][rng.random_range(0..4)],
            )
            .replace("{worker_count}", &format!("{}", rng.random_range(1..=8)))
            .replace(
                "{cache_key}",
                &format!("cache_{}", rng.random_range(1..=1000)),
            )
            .replace("{cache_time}", &format!("{}", rng.random_range(1..=50)))
            .replace(
                "{json_error}",
                &["Unexpected token", "Missing field", "Invalid format"][rng.random_range(0..3)],
            )
            .replace(
                "{job_name}",
                &["cleanup", "backup", "report", "sync"][rng.random_range(0..4)],
            )
            .replace(
                "{api_key}",
                &format!("api_{}", rng.random_range(1000..=9999)),
            )
            .replace(
                "{request_count}",
                &format!("{}", rng.random_range(100..=1000)),
            )
            .replace(
                "{sql_error}",
                &["Connection lost", "Syntax error", "Table not found"][rng.random_range(0..3)],
            )
            .replace(
                "{query}",
                &format!("SELECT * FROM table_{}", rng.random_range(1..=10)),
            )
            .replace(
                "{filename}",
                &format!(
                    "file_{}.{}",
                    rng.random_range(1..=1000),
                    ["jpg", "pdf", "txt", "csv"][rng.random_range(0..4)]
                ),
            )
            .replace(
                "{file_size}",
                &format!("{}", rng.random_range(1024..=1048576)),
            )
            .replace("{days}", &format!("{}", rng.random_range(1..=30)))
            .replace(
                "{domain}",
                &format!(
                    "{}.com",
                    ["example", "test", "demo", "app"][rng.random_range(0..4)]
                ),
            )
            .replace("{service}", &services[rng.random_range(0..services.len())])
            .replace(
                "{status_code}",
                &format!("{}", [200, 404, 500, 503][rng.random_range(0..4)]),
            )
            .replace(
                "{email}",
                &format!("user{}@example.com", rng.random_range(1..=1000)),
            )
            .replace(
                "{country}",
                &["US", "UK", "CA", "DE", "FR"][rng.random_range(0..5)],
            )
            .replace(
                "{source}",
                &["github", "stripe", "slack"][rng.random_range(0..3)],
            )
            .replace("{size}", &format!("{}", rng.random_range(100..=10000)))
            .replace(
                "{recipient}",
                &format!("user{}@domain.com", rng.random_range(1..=100)),
            )
            .replace(
                "{smtp_error}",
                &[
                    "Connection refused",
                    "Authentication failed",
                    "Rate limited",
                ][rng.random_range(0..3)],
            )
            .replace(
                "{task}",
                &["email", "report", "cleanup", "backup"][rng.random_range(0..4)],
            )
            .replace("{priority}", &format!("{}", rng.random_range(1..=5)))
            .replace(
                "{query_time}",
                &format!("{}", rng.random_range(1000..=10000)),
            )
            .replace("{table}", &format!("table_{}", rng.random_range(1..=20)))
            .replace("{user}", &format!("user_{}", rng.random_range(1..=100)))
            .replace(
                "{resource}",
                &format!(
                    "/api/v1/{}",
                    ["users", "orders", "products", "reports"][rng.random_range(0..4)]
                ),
            )
            .replace("{key}", &format!("session_{}", rng.random_range(1..=1000)))
            .replace("{ttl}", &format!("{}", rng.random_range(300..=3600)));

        let metadata = LogMetadata {
            namespace: namespaces[rng.random_range(0..namespaces.len())].to_string(),
            pod: format!(
                "{}-{}",
                services[rng.random_range(0..services.len())],
                rng.random_range(1..=3)
            ),
            container: services[rng.random_range(0..services.len())].to_string(),
        };

        logs.push((log_line, metadata));
    }

    // Shuffle to make clustering more realistic
    logs.shuffle(&mut rng);
    logs
}

fn benchmark_clustering(c: &mut Criterion) {
    let mut group = c.benchmark_group("Log Clustering");

    // Reasonable benchmark settings to prevent infinite loops
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(5));

    let rt = Runtime::new().unwrap();

    // Small dataset (1K logs) - fast baseline
    let small_logs = generate_realistic_logs(1_000, 42);
    group.bench_function("cluster_1k_logs", |b| {
        b.to_async(&rt).iter(|| {
            cluster_logs_parallel(
                black_box(small_logs.clone()),
                black_box(0.75),  // similarity_threshold
                black_box(true),  // json_mode (quiet)
                black_box(4),     // batch_size_factor
                black_box(false), // no_word_filter
                black_box(0),     // member_limit (unlimited)
            )
        })
    });

    // Medium dataset (10K logs) - realistic workload
    let medium_logs = generate_realistic_logs(10_000, 123);
    group.bench_function("cluster_10k_logs", |b| {
        b.to_async(&rt).iter(|| {
            cluster_logs_parallel(
                black_box(medium_logs.clone()),
                black_box(0.75),
                black_box(true), // json_mode (quiet)
                black_box(6),    // higher batch factor for better parallelism
                black_box(false),
                black_box(0),
            )
        })
    });

    // Large dataset (50K logs) - stress test
    let large_logs = generate_realistic_logs(50_000, 456);
    group.bench_function("cluster_50k_logs", |b| {
        b.to_async(&rt).iter(|| {
            cluster_logs_parallel(
                black_box(large_logs.clone()),
                black_box(0.75),
                black_box(true), // json_mode (quiet)
                black_box(8),    // higher batch factor for large dataset
                black_box(false),
                black_box(0),
            )
        })
    });

    // Very large dataset (100K logs) - performance limit test
    let xl_logs = generate_realistic_logs(100_000, 789);
    group.bench_function("cluster_100k_logs", |b| {
        b.to_async(&rt).iter(|| {
            cluster_logs_parallel(
                black_box(xl_logs.clone()),
                black_box(0.8),  // higher threshold for faster clustering
                black_box(true), // json_mode (quiet)
                black_box(10),   // maximum batch factor
                black_box(false),
                black_box(100), // limit members per cluster to prevent memory issues
            )
        })
    });

    // Extreme dataset (500K logs) - scalability test
    let xxl_logs = generate_realistic_logs(500_000, 999);
    group.bench_function("cluster_500k_logs", |b| {
        b.to_async(&rt).iter(|| {
            cluster_logs_parallel(
                black_box(xxl_logs.clone()),
                black_box(0.85), // even higher threshold for performance
                black_box(true), // json_mode (quiet)
                black_box(12),   // maximum batch factor
                black_box(false),
                black_box(50), // limit members per cluster
            )
        })
    });

    // Ultimate stress test (1M logs) - maximum scale
    if std::env::var("ENABLE_1M_BENCHMARK").is_ok() {
        let ultimate_logs = generate_realistic_logs(1_000_000, 1337);
        group.bench_function("cluster_1m_logs", |b| {
            b.to_async(&rt).iter(|| {
                cluster_logs_parallel(
                    black_box(ultimate_logs.clone()),
                    black_box(0.9),  // very high threshold for maximum performance
                    black_box(true), // json_mode (quiet)
                    black_box(16),   // maximum batch factor
                    black_box(false),
                    black_box(25), // strict limit on members per cluster
                )
            })
        });
    }

    // Extreme stress test (5M logs) - theoretical maximum
    // Note: This will use a lot of memory and take a very long time
    // Only enable if you have sufficient resources
    if std::env::var("ENABLE_5M_BENCHMARK").is_ok() {
        let extreme_logs = generate_realistic_logs(5_000_000, 31337);
        group.bench_function("cluster_5m_logs", |b| {
            b.to_async(&rt).iter(|| {
                cluster_logs_parallel(
                    black_box(extreme_logs.clone()),
                    black_box(0.95), // maximum threshold for fastest clustering
                    black_box(true), // json_mode (quiet)
                    black_box(20),   // maximum batch factor
                    black_box(false),
                    black_box(10), // very strict member limit
                )
            })
        });
    }

    group.finish();
}

// Benchmark different similarity thresholds to see performance impact
fn benchmark_similarity_thresholds(c: &mut Criterion) {
    let mut group = c.benchmark_group("Similarity Thresholds");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    let rt = Runtime::new().unwrap();
    let test_logs = generate_realistic_logs(100_000, 555);

    for threshold in [0.5, 0.7, 0.8, 0.9, 0.95] {
        group.bench_function(&format!("threshold_{}", (threshold * 100.0) as u32), |b| {
            b.to_async(&rt).iter(|| {
                cluster_logs_parallel(
                    black_box(test_logs.clone()),
                    black_box(threshold),
                    black_box(true),
                    black_box(4),
                    black_box(false),
                    black_box(0),
                )
            })
        });
    }

    group.finish();
}

// Benchmark different batch size factors to optimize parallelism
fn benchmark_batch_factors(c: &mut Criterion) {
    let mut group = c.benchmark_group("Batch Size Factors");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    let rt = Runtime::new().unwrap();
    let test_logs = generate_realistic_logs(100_000, 777);

    for batch_factor in [1, 2, 4, 8, 12, 16] {
        group.bench_function(&format!("batch_factor_{}", batch_factor), |b| {
            b.to_async(&rt).iter(|| {
                cluster_logs_parallel(
                    black_box(test_logs.clone()),
                    black_box(0.75),
                    black_box(true),
                    black_box(batch_factor),
                    black_box(false),
                    black_box(0),
                )
            })
        });
    }

    group.finish();
}

// Benchmark word filtering impact on performance
fn benchmark_word_filtering(c: &mut Criterion) {
    let mut group = c.benchmark_group("Word Filtering");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    let rt = Runtime::new().unwrap();
    let test_logs = generate_realistic_logs(100_000, 888);

    group.bench_function("with_word_filter", |b| {
        b.to_async(&rt).iter(|| {
            cluster_logs_parallel(
                black_box(test_logs.clone()),
                black_box(0.75),
                black_box(true),
                black_box(6),
                black_box(false), // Word filtering enabled
                black_box(0),
            )
        })
    });

    group.bench_function("without_word_filter", |b| {
        b.to_async(&rt).iter(|| {
            cluster_logs_parallel(
                black_box(test_logs.clone()),
                black_box(0.75),
                black_box(true),
                black_box(6),
                black_box(true), // Word filtering disabled
                black_box(0),
            )
        })
    });

    group.finish();
}

// Benchmark memory usage and performance with different log patterns
fn benchmark_log_diversity(c: &mut Criterion) {
    let mut group = c.benchmark_group("Log Diversity");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(50));

    let rt = Runtime::new().unwrap();

    // High diversity - many unique patterns (worst case for clustering)
    let high_diversity_logs = generate_high_diversity_logs(100_000, 999);
    group.bench_function("high_diversity", |b| {
        b.to_async(&rt).iter(|| {
            cluster_logs_parallel(
                black_box(high_diversity_logs.clone()),
                black_box(0.75),
                black_box(true),
                black_box(6),
                black_box(false),
                black_box(0),
            )
        })
    });

    // Low diversity - few patterns repeated (best case for clustering)
    let low_diversity_logs = generate_low_diversity_logs(100_000, 111);
    group.bench_function("low_diversity", |b| {
        b.to_async(&rt).iter(|| {
            cluster_logs_parallel(
                black_box(low_diversity_logs.clone()),
                black_box(0.75),
                black_box(true),
                black_box(6),
                black_box(false),
                black_box(0),
            )
        })
    });

    // Medium diversity - realistic mix (normal case)
    let medium_diversity_logs = generate_realistic_logs(100_000, 333);
    group.bench_function("medium_diversity", |b| {
        b.to_async(&rt).iter(|| {
            cluster_logs_parallel(
                black_box(medium_diversity_logs.clone()),
                black_box(0.75),
                black_box(true),
                black_box(6),
                black_box(false),
                black_box(0),
            )
        })
    });

    group.finish();
}

// Benchmark member limit impact on performance and memory
fn benchmark_member_limits(c: &mut Criterion) {
    let mut group = c.benchmark_group("Member Limits");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    let rt = Runtime::new().unwrap();
    let test_logs = generate_realistic_logs(100_000, 1234);

    for limit in [0, 10, 50, 100, 500] {
        let limit_name = if limit == 0 {
            "unlimited".to_string()
        } else {
            limit.to_string()
        };
        group.bench_function(&format!("member_limit_{}", limit_name), |b| {
            b.to_async(&rt).iter(|| {
                cluster_logs_parallel(
                    black_box(test_logs.clone()),
                    black_box(0.75),
                    black_box(true),
                    black_box(6),
                    black_box(false),
                    black_box(limit),
                )
            })
        });
    }

    group.finish();
}

// Benchmark string pool capacity impact on performance
fn benchmark_string_pool_capacity(c: &mut Criterion) {
    let mut group = c.benchmark_group("String Pool Capacity");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));

    let rt = Runtime::new().unwrap();
    let test_logs = generate_realistic_logs(100_000, 1111);

    // Test with different batch size factors to see how they affect string pool usage
    for batch_factor in [2, 8, 16] {
        group.bench_function(&format!("batch_factor_{}", batch_factor), |b| {
            b.to_async(&rt).iter(|| {
                cluster_logs_parallel(
                    black_box(test_logs.clone()),
                    black_box(0.75),
                    black_box(true),
                    black_box(batch_factor),
                    black_box(false),
                    black_box(0),
                )
            })
        });
    }

    group.finish();
}

// Benchmark performance with different log line lengths
fn benchmark_log_line_lengths(c: &mut Criterion) {
    let mut group = c.benchmark_group("Log Line Lengths");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    let rt = Runtime::new().unwrap();

    // Short log lines (avg ~50 chars)
    let short_logs = generate_variable_length_logs(100_000, 2222, 20, 80);
    group.bench_function("short_lines", |b| {
        b.to_async(&rt).iter(|| {
            cluster_logs_parallel(
                black_box(short_logs.clone()),
                black_box(0.75),
                black_box(true),
                black_box(6),
                black_box(false),
                black_box(0),
            )
        })
    });

    // Medium log lines (avg ~200 chars)
    let medium_logs = generate_variable_length_logs(100_000, 3333, 100, 300);
    group.bench_function("medium_lines", |b| {
        b.to_async(&rt).iter(|| {
            cluster_logs_parallel(
                black_box(medium_logs.clone()),
                black_box(0.75),
                black_box(true),
                black_box(6),
                black_box(false),
                black_box(0),
            )
        })
    });

    // Long log lines (avg ~500 chars)
    let long_logs = generate_variable_length_logs(100_000, 4444, 300, 700);
    group.bench_function("long_lines", |b| {
        b.to_async(&rt).iter(|| {
            cluster_logs_parallel(
                black_box(long_logs.clone()),
                black_box(0.75),
                black_box(true),
                black_box(6),
                black_box(false),
                black_box(0),
            )
        })
    });

    group.finish();
}

// Benchmark clustering accuracy vs performance trade-offs
fn benchmark_accuracy_vs_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("Accuracy vs Performance");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    let rt = Runtime::new().unwrap();
    let test_logs = generate_realistic_logs(100_000, 5555);

    // Conservative settings (high accuracy, slower)
    group.bench_function("high_accuracy", |b| {
        b.to_async(&rt).iter(|| {
            cluster_logs_parallel(
                black_box(test_logs.clone()),
                black_box(0.6), // Low threshold for high accuracy
                black_box(true),
                black_box(2),     // Small batches for thoroughness
                black_box(false), // Word filtering enabled
                black_box(0),     // No member limit
            )
        })
    });

    // Balanced settings (medium accuracy, medium speed)
    group.bench_function("balanced", |b| {
        b.to_async(&rt).iter(|| {
            cluster_logs_parallel(
                black_box(test_logs.clone()),
                black_box(0.75), // Standard threshold
                black_box(true),
                black_box(6), // Medium batch size
                black_box(false),
                black_box(100), // Some member limiting
            )
        })
    });

    // Performance settings (lower accuracy, faster)
    group.bench_function("high_performance", |b| {
        b.to_async(&rt).iter(|| {
            cluster_logs_parallel(
                black_box(test_logs.clone()),
                black_box(0.9), // High threshold for speed
                black_box(true),
                black_box(12),   // Large batches
                black_box(true), // No word filtering
                black_box(25),   // Strict member limit
            )
        })
    });

    group.finish();
}

// Helper function to generate high diversity logs (many unique patterns)
fn generate_high_diversity_logs(num_logs: usize, seed: u64) -> Vec<(String, LogMetadata)> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut logs = Vec::with_capacity(num_logs);

    let namespaces = [
        "default",
        "kube-system",
        "monitoring",
        "app-prod",
        "app-staging",
    ];
    let services = [
        "web-server",
        "api-gateway",
        "database",
        "cache",
        "worker",
        "scheduler",
    ];

    for i in 0..num_logs {
        // Generate mostly unique log patterns
        let log_line = format!(
            "{} [{}] {}: Operation {} completed with status {} in {}ms for resource {} (correlation_id: {})",
            ["ERROR", "WARN", "INFO", "DEBUG"][rng.random_range(0..4)],
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S"),
            services[rng.random_range(0..services.len())],
            format!("op_{}", i), // Unique operation ID
            rng.random_range(200..600),
            rng.random_range(1..5000),
            format!("resource_{}", rng.random_range(1..1000)),
            format!("corr_{}", i) // Unique correlation ID
        );

        let metadata = LogMetadata {
            namespace: namespaces[rng.random_range(0..namespaces.len())].to_string(),
            pod: format!(
                "{}-{}",
                services[rng.random_range(0..services.len())],
                rng.random_range(1..100)
            ),
            container: services[rng.random_range(0..services.len())].to_string(),
        };

        logs.push((log_line, metadata));
    }

    logs
}

// Helper function to generate low diversity logs (few patterns repeated)
fn generate_low_diversity_logs(num_logs: usize, seed: u64) -> Vec<(String, LogMetadata)> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut logs = Vec::with_capacity(num_logs);

    // Only 5 basic patterns that will cluster well
    let patterns = [
        "ERROR: Database connection failed",
        "INFO: Request processed successfully",
        "WARN: High memory usage detected",
        "ERROR: Authentication failed",
        "INFO: Service started successfully",
    ];

    let namespaces = ["default", "app-prod"];
    let services = ["web-server", "database"];

    for _ in 0..num_logs {
        let pattern = patterns[rng.random_range(0..patterns.len())];
        let log_line = format!("{} at {}", pattern, chrono::Utc::now().format("%H:%M:%S"));

        let metadata = LogMetadata {
            namespace: namespaces[rng.random_range(0..namespaces.len())].to_string(),
            pod: format!("{}-1", services[rng.random_range(0..services.len())]),
            container: services[rng.random_range(0..services.len())].to_string(),
        };

        logs.push((log_line, metadata));
    }

    logs
}

// Helper function to generate logs with specific line lengths
fn generate_variable_length_logs(
    num_logs: usize,
    seed: u64,
    min_len: usize,
    max_len: usize,
) -> Vec<(String, LogMetadata)> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut logs = Vec::with_capacity(num_logs);

    let namespaces = ["default", "kube-system", "monitoring"];
    let services = ["web-server", "api-gateway", "database"];
    let levels = ["ERROR", "WARN", "INFO", "DEBUG"];

    for i in 0..num_logs {
        let target_len = rng.random_range(min_len..=max_len);
        let level = levels[rng.random_range(0..levels.len())];
        let service = services[rng.random_range(0..services.len())];

        // Build log line to approximately target length
        let mut log_line = format!("{} [{}] ", level, service);

        // Add filler content to reach target length
        let remaining_len = target_len.saturating_sub(log_line.len());
        if remaining_len > 20 {
            let filler = match i % 4 {
                0 => format!(
                    "Processing request {} with parameters: {}",
                    rng.random_range(1000..9999),
                    "x".repeat(remaining_len.saturating_sub(50))
                ),
                1 => format!(
                    "Database query executed in {}ms: {}",
                    rng.random_range(1..1000),
                    "SELECT * FROM table WHERE ".to_string()
                        + &"field = value AND ".repeat(remaining_len / 20)
                ),
                2 => format!(
                    "Network request to {}:{} completed with response: {}",
                    "api.example.com",
                    rng.random_range(80..9000),
                    "data".repeat(remaining_len / 4)
                ),
                _ => format!(
                    "Operation completed successfully with details: {}",
                    "detail ".repeat(remaining_len / 7)
                ),
            };
            log_line.push_str(&filler);
        }

        // Truncate to exact target length if needed
        if log_line.len() > target_len {
            log_line.truncate(target_len);
        }

        let metadata = LogMetadata {
            namespace: namespaces[rng.random_range(0..namespaces.len())].to_string(),
            pod: format!("{}-{}", service, rng.random_range(1..5)),
            container: service.to_string(),
        };

        logs.push((log_line, metadata));
    }

    logs
}

criterion_group!(
    benches,
    benchmark_clustering,
    benchmark_similarity_thresholds,
    benchmark_batch_factors,
    benchmark_word_filtering,
    benchmark_log_diversity,
    benchmark_member_limits,
    benchmark_string_pool_capacity,
    benchmark_log_line_lengths,
    benchmark_accuracy_vs_performance
);
criterion_main!(benches);
