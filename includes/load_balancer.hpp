#pragma once
#include <logger.hpp>
#include <string>
#include <vector>
#include <queue>

// Records information of clusters to be assigned. Used to estimate cost and determine priority, etc.
struct ClusterInfo {
    int cluster_id;
    int node_count;     // number of nodes
    int edge_count;     // number of edges
};

class LoadBalancer {
private:
    Logger logger;
    std::string work_dir;
    std::string output_file;
    bool use_rank_0_worker;
    std::vector<ClusterInfo> unprocessed_clusters;  // Vector of unprocessed clusters

    /**
     * Partition clustering into separate cluster files
     * Returns vector of created cluster IDs
     */
    std::vector<ClusterInfo> partition_clustering(const std::string& edgelist,
                                          const std::string& cluster_file,
                                          const std::string& output_dir);

    /**
     * Load cluster info from pre-partitioned directory
     * Returns vector of ClusterInfo loaded from existing files
     * Note: this method assumes the directory contains a completed partioning and doesn't check if all clusters in the given clustering is included
     */
    std::vector<ClusterInfo> load_partitioned_clusters(const std::string& partitioned_dir);

    /**
     * Initialize job queue from created clusters
     */
    void initialize_job_queue(const std::vector<ClusterInfo>& created_clusters);

public:
    /**
     * Constructor: Initialize load balancer
     * - Partitions the clustering into separate cluster files (or loads from pre-partitioned dir)
     * - Initializes the job queue
     * This runs synchronously on rank 0 before any workers start
     */
    LoadBalancer(const std::string& edgelist,
                const std::string& cluster_file,
                const std::string& work_dir,
                const std::string& output_file,
                int log_level,
                bool use_rank_0_worker,
                const std::string& partitioned_clusters_dir = "");

    /**
     * Runtime phase: Distribute jobs to workers
     * This runs in a separate thread on rank 0
     */
    void run();

    /**
     * Estimate the cost of processing a cluster
     */
    float getCost(int node_count, int edge_count);
};