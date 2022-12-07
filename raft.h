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

#define RAFT_LOG(fmt, args...) \
    do {                       \
    } while (0);
/*
#define RAFT_LOG(fmt, args...)                                                                                   \
    do {                                                                                                         \
        auto now =                                                                                               \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                std::chrono::system_clock::now().time_since_epoch())                                             \
                .count();                                                                                        \
        printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
    } while (0);
*/
#define IDX(index) ((index) - last_snapshot_index - 1)
#define LOG(index) log[IDX(index)]
#define LEN(size) ((size) + last_snapshot_index)

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
    int last_applied_index;
    int last_applied_term;

    int last_snapshot_index;
    int last_snapshot_term;

    int granted_num;
    int rejected_num;
    bool update;

    int size_nodes;

    std::vector<log_entry<command>> log{};
    std::vector<int> next_index{};
    std::vector<int> match_index{};

    std::vector<bool> vote_replied{};
    std::vector<bool> heartbeat_replied{};

    std::atomic_bool stopped;

    enum raft_role {
        follower=1,
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
        last_applied_index(0),
        last_applied_term(0),
        last_snapshot_index(0),
        last_snapshot_term(0),
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
    RAFT_LOG("constructor")
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    std::unique_lock<std::mutex> _(mtx);
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

    // restore
    storage->load("last_log_index", last_log_index);
    storage->load("last_log_term", last_log_term);
    storage->load("commit_index", commit_index);
    storage->load("last_snapshot_index", last_snapshot_index);
    storage->load("last_snapshot_term", last_snapshot_term);
    storage->load("current_term", current_term);
    storage->load("log", log);
    std::vector<char> ss{};
    storage->load("snapshot", ss);
    if (!ss.empty()) {
        state->apply_snapshot(ss);
        last_applied_index = last_snapshot_index;
        last_applied_term = last_snapshot_term;
    }
    RAFT_LOG("construct finished")
}

template <typename state_machine, typename command>
raft<state_machine, command>::~raft() {
    RAFT_LOG("destructor")
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
    RAFT_LOG("end")
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
    RAFT_LOG("join finished")
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

    RAFT_LOG("start")
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
    RAFT_LOG("background threading is running")
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> _(mtx);

    term = current_term;
    if (role != leader)
        return false;

    RAFT_LOG("leader receives new command")

    int size = cmd.size();
    char buf[size];
    cmd.serialize(buf, size);

    log.push_back({cmd, current_term});
    storage->store("log", log.back());
    last_log_index = LEN(log.size());
    storage->store("last_log_index", last_log_index);
    last_log_term = current_term;
    storage->store("last_log_term", last_log_term);

    next_index[my_id] = last_log_index + 1;
    match_index[my_id] = last_log_index;
    index = last_log_index;

    RAFT_LOG("leader last log index: %d", last_log_index)
    RAFT_LOG("leader commit index: %d", commit_index)
    RAFT_LOG("leader last applied index: %d", last_applied_index)
    return true;
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Lab3: Your code here
    RAFT_LOG("receives snapshot request")
    install_snapshot_args args{current_term, leader_id, last_applied_index, last_applied_term, state->snapshot()};
    install_snapshot_reply reply{};
    install_snapshot(args, reply);
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
        role = follower;
        update = true;
        current_term = args.term;
        storage->store("current_term", current_term);

        if (args.last_log_term > last_log_term || (args.last_log_term == last_log_term && args.last_log_index >= last_log_index)) {
            reply.vote_granted = true;
            leader_id = args.candidate_id;
            RAFT_LOG("grant vote for node %d", args.candidate_id)
        } else {
            reply.vote_granted = false;
            RAFT_LOG("reject vote for node %d", args.candidate_id)
        }
    } else {
        reply.vote_granted = false;
        RAFT_LOG("reject vote for node %d: no bigger term", args.candidate_id)
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

    if (reply.term > current_term) {
        current_term = reply.term;
        storage->store("current_term", current_term);
        role = follower;
        update = true;
        RAFT_LOG("seeing bigger term, revert to follower")
        return;
    }

    if (role != candidate)
        return;
    if (reply.vote_granted) {
        granted_num++;
        // majority granted
        if (granted_num >= size_nodes / 2 + 1) {
            role = leader;
            next_index.assign(size_nodes, last_log_index + 1);
            match_index.assign(size_nodes, 0);
            match_index[my_id] = last_log_index;
            RAFT_LOG("has been accepted by majority, become leader")
        }
    } else {
        // majority rejected
        rejected_num++;
        if (rejected_num >= size_nodes / 2 + 1) {
            role = follower;
            RAFT_LOG("has been rejected by majority, revert to follower")
        }
    }
}

template <typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply &reply) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> _(mtx);

    RAFT_LOG("append: start, receive entries from leader %d, prev is %d", arg.leader_id, arg.prev_log_index)
    RAFT_LOG("old size is %d, current log size is %ld, received size is %ld", last_log_index, log.size(), arg.entries.size())

    if (arg.term >= current_term) {
        if (arg.term > current_term) {
            current_term = arg.term;
            storage->store("current_term", current_term);
        }
        update = true;
        reply.success = true;
        role = follower;
        leader_id = arg.leader_id;

        int index = IDX(arg.prev_log_index);

        if (!arg.entries.empty() || arg.prev_log_index > 0) {
            // maybe no previous entries
            if (index >= 0) {
                if (last_log_index < arg.prev_log_index) {
                    reply.success = false;
                    RAFT_LOG("reject: no prev entry")
                } else if (LOG(arg.prev_log_index).term != arg.prev_log_term) {
                    log.erase(log.begin() + index, log.end());
                    storage->remove_back("log", index);

                    last_log_index = LEN(log.size());
                    last_log_term = (log.size() ? log.back().term : 0);
                    storage->store("last_log_index", last_log_index);
                    storage->store("last_log_term", last_log_term);

                    reply.success = false;
                    RAFT_LOG("reject: prev entry doesn't match")
                }
            }

            // add entries
            if (reply.success && !arg.entries.empty()) {
                log.erase(log.begin() + (index + 1), log.end());
                storage->remove_back("log", index + 1);
                for (auto &ent: arg.entries) {
                    storage->store("log", ent);
                    log.push_back(ent);
                }

                last_log_index = LEN(log.size());
                last_log_term = log.back().term;
                storage->store("last_log_index", last_log_index);
                storage->store("last_log_term", last_log_term);
            }
        }

        // update commit index
        if (reply.success && arg.leader_commit > commit_index && last_log_term == current_term) {
            commit_index = (arg.leader_commit <= last_log_index ? arg.leader_commit : last_log_index);
            storage->store("commit_index", commit_index);
        }

        RAFT_LOG("leader commit: %d, last log index: %d, commit index: %d, last applied: %d",
                 arg.leader_commit, last_log_index, commit_index, last_applied_index)
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
        storage->store("current_term", current_term);
        role = follower;
        update = true;
        RAFT_LOG("seeing bigger term from node %d", node)
    }

    if (role != leader)
        return;
    if (!arg.entries.empty() || arg.prev_log_index > 0) {
        if (reply.success) {
            match_index[node] = arg.prev_log_index + (int)arg.entries.size();
            next_index[node] = match_index[node] + 1;

            // check if exists new log entry which can be committed
            int sum;
            for (int idx = last_log_index; idx > commit_index; idx--) {
                sum = 0;
                for (int id = 0; id < size_nodes; id++)
                    if (match_index[id] >= idx)
                        sum++;
                if ((sum >= size_nodes / 2 + 1) && LOG(idx).term == current_term) {
                    commit_index = idx;
                    storage->store("commit_index", commit_index);
                    RAFT_LOG("new command is committed")
                    break;
                }
            }

            RAFT_LOG("last log index: %d, commit index: %d, last applied: %d",
                     last_log_index, commit_index, last_applied_index)
        } else {
            next_index[node]--;
        }
    }
}

template <typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply &reply) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> _(mtx);

    RAFT_LOG("snapshot: index %d, term %d", args.last_included_index, args.last_included_term)

    if (args.term >= current_term) {
        if (args.term > current_term) {
            current_term = args.term;
            storage->store("current_term", current_term);
        }

        // apply the snapshot immediately
        storage->store("snapshot", args.data);
        state->apply_snapshot(args.data);
        last_applied_index = args.last_included_index;
        last_applied_term = args.last_included_term;

        // and then modify the log entries according to snapshot
        if (last_applied_index <= last_log_index && LOG(last_applied_index).term == last_applied_term) {
            int index = IDX(last_applied_index);
            log.erase(log.begin(), log.begin() + (index + 1));
            storage->remove_front("log", index + 1);
            RAFT_LOG("snapshot: partially remove log entries")
        } else {
            log.clear();
            storage->remove_all("log");
            last_log_index = last_applied_index;
            last_log_term = last_applied_term;
            storage->store("last_log_index", last_log_index);
            storage->store("last_log_term", last_log_term);
            RAFT_LOG("snapshot: remove all log entries")
        }

        last_snapshot_index = last_applied_index;
        last_snapshot_term = last_applied_term;
        storage->store("last_snapshot_index", last_snapshot_index);
        storage->store("last_snapshot_term", last_snapshot_term);

        // maybe this node has higher commit, then the committed entries
        // that are removed unexpectedly will be reapplied
        if (last_snapshot_index > commit_index) {
            commit_index = last_snapshot_index;
            storage->store("commit_index", commit_index);
        }

        RAFT_LOG("last index: %d, last term: %d, commit index: %d, last applied: %d", last_log_index, last_log_term, commit_index, last_applied_index)
    } else {
        RAFT_LOG("snapshot rejects: smaller term")
    }

    reply.term = current_term;
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int node, const install_snapshot_args &arg, const install_snapshot_reply &reply) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> _(mtx);
    RAFT_LOG("leader receives snapshot feedback from node %d", node)

    if (reply.term > current_term) {
        role = follower;
        update = true;
        current_term = reply.term;
        storage->store("current_term", current_term);
        RAFT_LOG("seeing bigger term from node %d", node)
        return;
    }

    if (role != leader)
        return;
    next_index[node] = arg.last_included_index + 1;
    match_index[node] = arg.last_included_index;
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
            storage->store("current_term", current_term);
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
                int prev_index = next_index[id] - 1;
                if (last_snapshot_index > prev_index) {
                    // this occassion means follower id is far left behind
                    std::vector<char> ss{};
                    storage->load("snapshot", ss);
                    install_snapshot_args args{current_term, my_id, last_snapshot_index, last_snapshot_term, ss};
                    thread_pool->addObjJob(this, &raft::send_install_snapshot, id, args);
                    RAFT_LOG("leader require node %d to apply snapshot", id)
                } else {
                    append_entries_args<command> args{current_term, my_id, prev_index,
                                                      (IDX(prev_index) >= 0 ? LOG(prev_index).term : 0),
                                                      commit_index,
                                                      {log.begin() + IDX(prev_index + 1), log.end()}};
                    // commit all necessary entries at a time
                    thread_pool->addObjJob(this, &raft::send_append_entries, id, args);
                    RAFT_LOG("leader require node %d to append log", id)
                }
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
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        std::unique_lock<std::mutex> _(mtx);

        if (commit_index > last_applied_index) {
            for (int idx = last_applied_index + 1; idx <= commit_index; idx++) {
                state->apply_log(LOG(idx).cmd);
                RAFT_LOG("apply command, index: %d", idx)
            }
            last_applied_index = commit_index;
            last_applied_term = LOG(commit_index).term;
        }
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.

    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here:
        std::this_thread::sleep_for(std::chrono::milliseconds(120));
        std::unique_lock<std::mutex> _(mtx);

        if (role != leader)
            continue;

        heartbeat_replied.assign(size_nodes, false);

        append_entries_args<command> args = {current_term, my_id, 0, 0, commit_index, {}};
        for (int id = 0; id < size_nodes; id++) {
            if (id == my_id)
                continue;
            thread_pool->addObjJob(this, &raft::send_append_entries, id, args);
            RAFT_LOG("send heartbeat to node %d", id)
        }
    }
}

/******************************************************************

                        Other functions

*******************************************************************/

#endif // raft_h