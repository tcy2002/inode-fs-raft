#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

template <typename state_machine, typename command>
class raft {
    static_assert(std::is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
    static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");

    friend class thread_pool;
/*
#define RAFT_LOG(fmt, args...) \
    do {                       \
    } while (0);
*/
#define RAFT_LOG(fmt, args...)                                                                                   \
    do {                                                                                                         \
        auto now =                                                                                               \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                std::chrono::system_clock::now().time_since_epoch())                                             \
                .count();                                                                                        \
        printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
    } while (0);


public:
    raft(
        rpcs *rpc_server,
        std::vector<rpcc *> rpc_clients,
        int idx,
        raft_storage<command> *storage,
        state_machine *state);
    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node.
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped().
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false.
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx; // A big lock to protect the whole data structure
    ThrPool *thread_pool;
    raft_storage<command> *storage; // To persist the raft log
    state_machine *state;           // The state machine that applies the raft log, e.g. a kv store

    rpcs *rpc_server;                // RPC server to recieve and handle the RPC requests
    std::vector<rpcc *> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                       // The index of this node in rpc_clients, start from 0
    int last_log_term;
    int last_log_index;
    int commit_index;
    int last_applied;

    int granted_num;
    int rejected_num;
    std::vector<bool> vote_replied{};
    bool update;

    int size_nodes;

    std::vector<log_entry<command>> log{};
    std::vector<int> next_index{};
    std::vector<int> match_index{};

    std::vector<bool> heartbeat_replied{};

    std::atomic_bool stopped;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int current_term;
    int leader_id;

    std::thread *background_election;
    std::thread *background_ping;
    std::thread *background_commit;
    std::thread *background_apply;

    // Your code here:

    /* ----Persistent state on all server----  */

    /* ---- Volatile state on all server----  */

    /* ---- Volatile state on leader----  */

private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply &reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply &reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply &reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command> &arg, const append_entries_reply &reply);

    void send_install_snapshot(int target, install_snapshot_args arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args &arg, const install_snapshot_reply &reply);

private:
    bool is_stopped();
    int num_nodes() {
        return size_nodes;
    }

    // background workers
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:
};

template <typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs *server, std::vector<rpcc *> clients, int idx, raft_storage<command> *storage, state_machine *state) :
    storage(storage),
    state(state),
    rpc_server(server),
    rpc_clients(clients),
    my_id(idx),
    last_log_term(0),
    last_log_index(0),
    commit_index(0),
    last_applied(0),
    granted_num(0),
    rejected_num(0),
    update(false),
    stopped(false),
    role(follower),
    current_term(0),
    leader_id(-1),
    background_election(nullptr),
    background_ping(nullptr),
    background_commit(nullptr),
    background_apply(nullptr) {
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here:
    // Do the initialization
    size_nodes = rpc_clients.size();
    vote_replied.reserve(size_nodes);
    next_index.reserve(size_nodes);
    match_index.reserve(size_nodes);
    heartbeat_replied.reserve(size_nodes);
}

template <typename state_machine, typename command>
raft<state_machine, command>::~raft() {
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    delete thread_pool;
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    std::unique_lock<std::mutex> _(mtx);
    term = current_term;
    return role == leader;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::start() {
    // Lab3: Your code here

    RAFT_LOG("start");
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> _(mtx);

    term = current_term;
    if (role != leader)
        return false;

    RAFT_LOG("leader receives new command")

    log.push_back({cmd, current_term});
    last_log_index = log.size();
    last_log_term = current_term;
    next_index[my_id] = last_log_index + 1;
    match_index[my_id] = last_log_index;
    index = last_log_index;

    RAFT_LOG("leader last log index: %d", last_log_index)
    RAFT_LOG("leader commit index: %d", commit_index)
    return true;
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Lab3: Your code here
    return true;
}

/******************************************************************

                         RPC Related

*******************************************************************/
template <typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply &reply) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> _(mtx);

    RAFT_LOG("receive vote request from node %d", args.candidate_id)

    if (args.term > current_term) {
        current_term = args.term;
        if (args.last_log_term > last_log_term || (args.last_log_term == last_log_term && args.last_log_index >= last_log_index)) {
            reply.vote_granted = true;
            leader_id = args.candidate_id;
            update = true;
            RAFT_LOG("grant vote for node %d", args.candidate_id)
        } else {
            reply.vote_granted = false;
            RAFT_LOG("reject vote for node %d", args.candidate_id)
        }
    } else {
        reply.vote_granted = false;
        RAFT_LOG("reject vote for node %d: smaller term", args.candidate_id)
    }

    reply.term = current_term;
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> _(mtx);

    vote_replied[target] = true;
    RAFT_LOG("node %d votes: %d", target, reply.vote_granted)

    if (reply.vote_granted) {
        granted_num++;
        // majority granted
        if (role == candidate && granted_num >= size_nodes / 2 + 1) {
            role = leader;
            next_index.assign(size_nodes, last_log_index + 1);
            match_index.assign(size_nodes, 0);
            match_index[my_id] = last_log_index;
            RAFT_LOG("has been accepted by majority, become leader")
        }
    } else {
        // majority rejected
        rejected_num++;
        if (role == candidate && rejected_num >= size_nodes / 2 + 1) {
            role = follower;
            RAFT_LOG("has been rejected by majority, revert to follower")
        }
        if (reply.term > current_term) {
            current_term = reply.term;
            role = follower;
            update = true;
            RAFT_LOG("seeing bigger term, revert to follower")
        }
    }
}

template <typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply &reply) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> _(mtx);

    RAFT_LOG("receive entries from leader %d, prev is %d", arg.leader_id, arg.prev_log_index)
    RAFT_LOG("old size is %d, received size is %ld", last_log_index, arg.entries.size())
    if (arg.term >= current_term) {
        current_term = arg.term;
        update = true;
        reply.success = true;
        role = follower;
        leader_id = arg.leader_id;

        // 0 means heartbeat or no previous entries
        if (arg.prev_log_index) {
            if (last_log_index < arg.prev_log_index) {
                reply.success = false;
                RAFT_LOG("reject: no prev entry")
            } else if (log[arg.prev_log_index - 1].term != arg.prev_log_term) {
                log.erase(log.begin() + arg.prev_log_index - 1, log.end());
                reply.success = false;
                RAFT_LOG("reject: prev entry doesn't match")
            }
        }

        if (reply.success) {
            // add entries
            if (!arg.entries.empty()) {
                log.erase(log.begin() + arg.prev_log_index, log.end());
                for (auto &ent: arg.entries)
                    log.push_back(ent);
                last_log_index = log.size();
                last_log_term = log.back().term;
            }

            // update commit index
            if (arg.leader_commit > commit_index && last_log_term == current_term)
                commit_index = last_log_index < arg.leader_commit ? last_log_index : arg.leader_commit;

            RAFT_LOG("leader commit: %d", arg.leader_commit)
            RAFT_LOG("last log index: %d", last_log_index)
            RAFT_LOG("commit index: %d", commit_index)
            RAFT_LOG("last applied: %d", last_applied)
        }
    } else {
        reply.success = false;
        RAFT_LOG("reject: smaller term")
    }

    reply.term = current_term;
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int node, const append_entries_args<command> &arg, const append_entries_reply &reply) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> _(mtx);

    heartbeat_replied[node] = true;
    RAFT_LOG("receive feedback from node %d", node)

    if (reply.term > current_term) {
        current_term = reply.term;
        role = follower;
        update = true;
        RAFT_LOG("seeing bigger term from node %d", reply.term)
    } else if (!arg.entries.empty() || arg.prev_log_index > 0) {
        if (reply.success) {
            next_index[node] = last_log_index + 1;
            match_index[node] = last_log_index;

            // check if exists new log entry which can be committed
            RAFT_LOG("last log index: %d, commit index: %d", last_log_index, commit_index)
            int sum;
            for (int idx = last_log_index; idx > commit_index; idx--) {
                sum = 0;
                for (int id = 0; id < size_nodes; id++)
                    if (match_index[id] >= idx)
                        sum++;
                if ((sum >= size_nodes / 2 + 1) && log[idx - 1].term == current_term) {
                    commit_index = idx;
                    RAFT_LOG("new command is committed")
                    break;
                }
            }
        } else {
            next_index[node]--;
        }
    }
}

template <typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply &reply) {
    // Lab3: Your code here
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int node, const install_snapshot_args &arg, const install_snapshot_reply &reply) {
    // Lab3: Your code here
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

#define RAND(n) rand() % n + n

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    // Periodly check the liveness of the leader.

    // Work for followers and candidates.

    srand(time(NULL));
    int min_timeout = 150;
    int max_timestamp = 120;
    int timeout = RAND(min_timeout);
    int timestamp = 0;

    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        std::unique_lock<std::mutex> _(mtx);

        if (role == leader)
            continue;

        // update: triggered by heartbeat or append entry
        if (update) {
            timeout = RAND(min_timeout);
            update = false;
            continue;
        }

        timeout -= 10;

        if (role == follower) {
            // begin election
            if (timeout > 0)
                continue;
            timeout = RAND(min_timeout);
            current_term++;
            granted_num = 1;
            rejected_num = 0;
            vote_replied.assign(size_nodes, false);
            vote_replied[my_id] = true;
            role = candidate;
        } else {
            // timestamp is initially 50 to leave other nodes some time to vote
            timestamp -= 10;
            if (timeout <= 0) {
                role = follower;
                continue;
            } else if (timestamp > 0)
                continue;
        }

        request_vote_args args = {current_term, my_id, last_log_index, last_log_term};
        for (int id = 0; id < size_nodes; id++) {
            if (vote_replied[id])
                continue;
            thread_pool->addObjJob(this, &raft::send_request_vote, id, args);
            RAFT_LOG("send vote request to node %d", id)
        }

        timestamp = max_timestamp;
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Periodly send logs to the follower.

    // Only work for the leader.

    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        std::unique_lock<std::mutex> _(mtx);

        if (role != leader)
            continue;

        append_entries_args<command> args = {current_term, my_id, 0, 0, commit_index, {}};
        for (int id = 0; id < size_nodes; id++) {
            // commit only when a node has replied the heartbeat
            if (id == my_id || !heartbeat_replied[id])
                continue;
            heartbeat_replied[id] = false;
            if (match_index[id] < last_log_index) {
                args.prev_log_index = next_index[id] - 1;
                args.prev_log_term = args.prev_log_index ? log[args.prev_log_index - 1].term : 0;
                args.entries.clear();
                // commit all necessary entries at a time
                args.entries.assign(log.begin() + args.prev_log_index, log.end());
                thread_pool->addObjJob(this, &raft::send_append_entries, id, args);
                RAFT_LOG("leader require node %d to append log", id)
            }
        }
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Periodly apply committed logs the state machine

    // Work for all the nodes.

    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here:
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        std::unique_lock<std::mutex> _(mtx);

        if (commit_index > last_applied) {
            for (int idx = last_applied + 1; idx <= commit_index; idx++) {
                state->apply_log(log[idx - 1].cmd);
                RAFT_LOG("apply command, index: %d", idx)
            }
            last_applied = commit_index;
        }
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.
    int timeout = 120;

    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here:
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        std::unique_lock<std::mutex> _(mtx);

        if (role != leader)
            continue;

        if ((timeout -= 10) > 0)
            continue;

        heartbeat_replied.assign(size_nodes, false);

        append_entries_args<command> args = {current_term, my_id, 0, 0, commit_index, {}};
        for (int id = 0; id < size_nodes; id++) {
            if (id == my_id)
                continue;
            thread_pool->addObjJob(this, &raft::send_append_entries, id, args);
            RAFT_LOG("send heartbeat to node %d", id)
        }

        timeout = 120;
    }
}

/******************************************************************

                        Other functions

*******************************************************************/

#endif // raft_h