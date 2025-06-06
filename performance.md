# Performance Optimizations Plan

## Phase 1: Memory Optimization for Log Fetching

-   [x] **Implement Pre-fetch Phase:**
    -   [x] Execute a preliminary log fetching step to count lines and total byte size without storing logs in memory.
    -   [x] Calculate the required `MmapStringPool` capacity (total log size * 2).
-   [x] **Initialize `MmapStringPool` Dynamically:**
    -   [x] Create the `MmapStringPool` with the calculated capacity before the main log fetching.
-   [x] **Integrate `MmapStringPool` into Log Fetching:**
    -   [x] Modify log fetching tasks to store log lines directly into the `MmapStringPool`, returning string IDs instead of full strings.
    -   [x] Update `all_log_lines` to store `(StringId, LogMetadata)`.
-   [x] **Adapt Downstream Processing:**
    -   [x] Modify filtering logic to retrieve strings from `MmapStringPool` for comparison.
    -   [x] Ensure `cluster_logs_parallel` and `cluster_logs_fallback` can handle `(StringId, LogMetadata)` and use the `MmapStringPool`.
-   [x] **Update `WorkItem` Structure:**
    -   [x] Change `WorkItem` to hold `StringId` instead of `String` for `log_line` if logs are pre-inserted into the pool before batching for clustering. Alternatively, if insertion happens during `process_log_item`, this might not be needed, but `process_log_item` will need access to the pool to store the initial `log_line`.
-   [x] **Refactor `OptimizedLogCluster` and `LogCluster`:**
    -   [x] Ensure `OptimizedLogCluster::new` and `OptimizedLogCluster::add_member` correctly handle string IDs and the pool.
    -   [x] Ensure `OptimizedLogCluster::to_log_cluster` correctly retrieves strings from the pool.
-   [ ] **Verify Memory Reduction:**
    -   [ ] Test and confirm that memory usage during log fetching is reduced.
-   [x] **Update `DEBUG_PERFORMANCE` metrics:**
    -   [x] Include metrics related to the pre-fetch phase and string pool initialization if relevant.
