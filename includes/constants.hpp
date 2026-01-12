#pragma once
#include <mpi.h>

enum class MessageType: int {
    // Worker to LB
    WORK_REQUEST = 0,   // requesting a cluster to be processed
    WORK_DONE = 1,      // the processing of the assigned cluster is completed successfully
    WORK_ABORTED = 2,   // the processing of the assigned cluster is aborted

    // LB to Worker
    DISTRIBUTE_WORK = 3,    // distribute a cluster to be processed
};

constexpr int to_int(MessageType messageType) {
    return static_cast<int>(messageType);
}

// Special cluster ID value to signal no more jobs available
constexpr int NO_MORE_JOBS = -1;