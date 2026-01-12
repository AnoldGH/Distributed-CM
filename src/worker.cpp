#include <mpi.h>
#include <worker.hpp>
#include <constants.hpp>
#include <cm.h>
#include <constrained.h>

#include <fstream>
#include <unistd.h>
#include <sys/wait.h>

// Constructor
Worker::Worker(Logger& logger, const std::string& work_dir,
               const std::string& algorithm, double clustering_parameter,
               int log_level, const std::string& connectedness_criterion,
               const std::string& mincut_type, bool prune)
    : logger(logger), work_dir(work_dir),
      algorithm(algorithm), clustering_parameter(clustering_parameter),
      log_level(log_level), connectedness_criterion(connectedness_criterion),
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

    std::string cluster_edgelist = work_dir + "/clusters/" + std::to_string(cluster_id) + ".edgelist";
    std::string cluster_clustering_file = work_dir + "/clusters/" + std::to_string(cluster_id) + ".cluster";
    logger.debug("Processing cluster file: " + cluster_edgelist);

    logger.flush(); // to avoid duplicate logs after fork()

    // TODO: spawn a child process and call CM processing logic on it
    // This is to gracefully handle OOM kills

    int pid = fork();
    if (pid == 0) {  // child process
        logger.info("Child process starts on cluster " + std::to_string(cluster_id));

        // TODO: CM logic
        std::string output_file = work_dir + "/output/" + std::to_string(cluster_id) + ".output";
        std::string log_file = work_dir + "/logs/clusters/" + std::to_string(cluster_id) + ".log"; // TODO: since CC was built as a standalone app with its own logging system, we have to use a different file. In the future we should try to integrate the two systems into one unified logging system.

        ConstrainedClustering* connectivity_modifier = new CM(cluster_edgelist, this->algorithm, this->clustering_parameter, cluster_clustering_file, 1, output_file, log_file, this->log_level, this->connectedness_criterion, this->prune, this->mincut_type);

        connectivity_modifier->main();  // run CM

        logger.info("Child process finishes cluster " + std::to_string(cluster_id));
        exit(0);    // exits successfully
    } else if (pid > 0) {    // control process
        int status;
        waitpid(pid, &status, 0);

        // Check how child terminates
        if (WIFEXITED(status)) {
            logger.log("Child exited with code: " + std::to_string(WEXITSTATUS(status)));
        } else {
            logger.log("Child killed by signal: " + std::to_string(WTERMSIG(status)));
        }
    } else {
        logger.log("Fork failed");  // TODO: this is serious. Need explicit handling
    }

    return true;
}