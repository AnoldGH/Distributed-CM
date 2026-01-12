#include <mpi.h>
#include <worker.hpp>
#include <constants.hpp>

#include <fstream>

// Constructor
Worker::Worker(Logger& logger, const std::string& work_dir,
               const std::string& connectedness_criterion,
               const std::string& mincut_type, bool prune)
    : logger(logger), work_dir(work_dir),
      connectedness_criterion(connectedness_criterion),
      mincut_type(mincut_type), prune(prune) {}

// Main run function
void Worker::run() {
    logger.info("Worker runtime phase started");

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // Worker main loop
    while (true) {
        // Send work request to load balancer (rank 0)
        logger.info("Requesting cluster from the load balancer");
        int request_msg = to_int(MessageType::WORK_REQUEST);
        MPI_Send(&request_msg, 1, MPI_INT, 0, to_int(MessageType::WORK_REQUEST), MPI_COMM_WORLD);

        // Receive cluster ID from load balancer
        int assigned_cluster;
        MPI_Recv(&assigned_cluster, 1, MPI_INT, 0, to_int(MessageType::DISTRIBUTE_WORK), MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        // Check for termination signal
        if (assigned_cluster == NO_MORE_JOBS) {
            logger.info("No more jobs available, terminating worker");
            break;
        }

        logger.info("Received cluster " + std::to_string(assigned_cluster));

        // Process the cluster
        bool success = process_cluster(assigned_cluster);

        // Send completion status
        MessageType status_type = success ? MessageType::WORK_DONE : MessageType::WORK_ABORTED;
        MPI_Send(&assigned_cluster, 1, MPI_INT, 0, to_int(status_type), MPI_COMM_WORLD);    // inform load balancer success / failure

        if (success) {
            logger.info("Completed cluster " + std::to_string(assigned_cluster));
        } else {
            logger.info("Aborted cluster " + std::to_string(assigned_cluster));
        }
    }

    logger.info("Worker runtime phase ended");
}

// Process a single cluster
bool Worker::process_cluster(int cluster_id) {
    // TODO: implement actual cluster processing
    // For now, this is a placeholder that simulates work

    std::string cluster_file = work_dir + "/clusters/" + std::to_string(cluster_id) + ".edgelist";
    logger.debug("Processing cluster file: " + cluster_file);

    // TODO: spawn a thread and call CM

    // TODO: dummy output logics
    std::string output_file = work_dir + "/output/" + std::to_string(cluster_id) + ".output";
    std::ofstream out(output_file);

    out << "Dummy output" << std::endl;

    out.close();

    return true;
}