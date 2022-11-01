#ifndef persister_h
#define persister_h

#include <fcntl.h>
#include <mutex>
#include <iostream>
#include <fstream>
#include "rpc.h"

#define MAX_LOG_SZ 1024

#define PRINT_MSG(func, msg) printf("%s: %s\n", (func), (msg));
#define MAX(a, b) (a) > (b) ? (a) : (b)

/*
 * chmod -R o+w cse-lab
 * sudo docker run -it --rm --privileged --cap-add=ALL -v `pwd`/cse-lab:/home/stu/cse-lab shenjiahuan/cselab_env:1.0 /bin/bash
 */

/*
 * Your code here for Lab2A:
 * Implement class chfs_command, you may need to add command types such as
 * 'create', 'put' here to represent different commands a transaction requires. 
 * 
 * Here are some tips:
 * 1. each transaction in ChFS consists of several chfs_commands.
 * 2. each transaction in ChFS MUST contain a BEGIN command and a COMMIT command.
 * 3. each chfs_commands contains transaction ID, command type, and other information.
 * 4. you can treat a chfs_command as a log entry.
 */
class chfs_command {
public:
    typedef unsigned long long inum_t;
    typedef unsigned long long txid_t;
    typedef long long actid_t;
    enum cmd_type {
        CMD_BEGIN = 0,
        CMD_COMMIT,
        CMD_CREATE,
        CMD_PUT,
        CMD_REMOVE
    };

    static const uint64_t head_size = sizeof(cmd_type) + sizeof(txid_t) + 2 * sizeof(actid_t) +
                                      sizeof(inum_t) + sizeof(int) + 2 * sizeof(uint64_t);
    cmd_type type;
    txid_t tid;
    actid_t aid;
    actid_t pre;

    inum_t name;
    std::string old_val;
    std::string new_val;

    // for checkpoint
    int checked = 0;
    int state = 0;

    // constructor
    chfs_command(cmd_type type, txid_t tid, actid_t aid, actid_t pre, inum_t name, const std::string &old_val, const std::string &new_val):
    type(type), tid(tid), aid(aid), pre(pre), name(name), old_val(old_val), new_val(new_val) {}

    chfs_command(const char *buf) {
        from_string(buf);
    }

    uint64_t size() const {
        return head_size + old_val.size() + new_val.size();
    }

    std::string to_string() {
        int64_t len = size(), len_str;
        char buf[len]{};
        char *p = buf;

        memcpy(p, (char *)&type, sizeof(cmd_type));
        p += sizeof(cmd_type);
        memcpy(p, (char *)&tid, sizeof(txid_t));
        p += sizeof(txid_t);
        memcpy(p, (char *)&aid, sizeof(actid_t));
        p += sizeof(actid_t);
        memcpy(p, (char *)&pre, sizeof(actid_t));
        p += sizeof(actid_t);
        memcpy(p, (char *)&name, sizeof(inum_t));
        p += sizeof(inum_t);
        memcpy(p, (char *)&checked, sizeof(int));
        p += sizeof(int);

        len_str = old_val.size();
        memcpy(p, (char *)&len_str, sizeof(uint64_t));
        p += sizeof(uint64_t);
        memcpy(p, old_val.c_str(), len_str);
        p += len_str;

        len_str = new_val.size();
        memcpy(p, (char *)&len_str, sizeof(uint64_t));
        p += sizeof(uint64_t);
        memcpy(p, new_val.c_str(), len_str);

        return std::string(buf, len);
    }

    void print_data() {
        printf("type: %d, tid: %lld, aid: %lld, pre: %lld, state: %d, name: %lld, old: %ld, new: %ld\n",
               type, tid, aid, pre, state, name, old_val.size(), new_val.size());
    }

private:
    void from_string(const char *src) {
        char *p = (char *)src;
        uint64_t len_str;

        memcpy((char *)&type, p, sizeof(cmd_type));
        p += sizeof(cmd_type);
        memcpy((char *)&tid, p, sizeof(txid_t));
        p += sizeof(txid_t);
        memcpy((char *)&aid, p, sizeof(actid_t));
        p += sizeof(actid_t);
        memcpy((char *)&pre, p, sizeof(actid_t));
        p += sizeof(actid_t);
        memcpy((char *)&name, p, sizeof(inum_t));
        p += sizeof(inum_t);
        memcpy((char *)&checked, p, sizeof(int));
        p += sizeof(int);

        memcpy((char *)&len_str, p, sizeof(uint64_t));
        p += sizeof(uint64_t);
        old_val.assign(p, len_str);
        p += len_str;

        memcpy((char *)&len_str, p, sizeof(uint64_t));
        p += sizeof(uint64_t);
        new_val.assign(p, len_str);
    }
};

/*
 * Your code here for Lab2A:
 * Implement class persister. A persister directly interacts with log files.
 * Remember it should not contain any transaction logic, its only job is to 
 * persist and recover data.
 * 
 * P.S. When and how to do checkpoint is up to you. Just keep your logfile size
 *      under MAX_LOG_SZ and checkpoint file size under DISK_SIZE.
 */
template<typename command>
class persister {

public:
    persister(const std::string& file_dir, inode_manager *im);
    ~persister();

    // persist data into solid binary file
    // You may modify parameters in these functions
    chfs_command::txid_t append_begin();
    void append_log(chfs_command::txid_t ctid, chfs_command::cmd_type type, chfs_command::inum_t name, const std::string &old_val, const std::string &new_val);
    void append_commit(chfs_command::txid_t ctid);
    void checkpoint();

    // restore data from solid binary file
    // You may modify parameters in these functions
    void restore_logdata();

private:
    std::mutex mtx;
    std::string file_dir;
    std::string file_path_checkpoint;
    std::string file_path_logfile;

    // restored log data
    std::vector<command> log_entries;
    chfs_command::txid_t tid;
    chfs_command::actid_t aid;
    uint64_t log_size;
    inode_manager *im;

    void load_logs(const char *filename);
    void save_logs(const char *filename);
};

template<typename command>
persister<command>::persister(const std::string& dir, inode_manager *im): tid(1), aid(0), log_size(0), im(im) {
    // DO NOT change the file names here
    file_dir = dir;
    file_path_checkpoint = file_dir + "/checkpoint.bin";
    file_path_logfile = file_dir + "/logdata.bin";
    PRINT_MSG("persister", "init");
}

template<typename command>
persister<command>::~persister() {
    // Your code here for lab2A

}

template<typename command>
chfs_command::txid_t persister<command>::append_begin() {
    std::unique_lock<std::mutex> lock(mtx);
    PRINT_MSG("append_begin", "pre");

    if (chfs_command::head_size + log_size > MAX_LOG_SZ)
        checkpoint();

    command log(chfs_command::CMD_BEGIN, tid, aid++, -1, 0, "", "");
    log_entries.push_back(log);
    log_size += log.size();

    FILE *log_file = fopen(file_path_logfile.c_str(), "a");
    fwrite(log.to_string().c_str(), log.size(), 1, log_file);
    fclose(log_file);
    PRINT_MSG("append_begin", "finished");

    return tid++;
}

template<typename command>
void persister<command>::append_log(chfs_command::txid_t ctid, chfs_command::cmd_type type, chfs_command::inum_t name, const std::string &old_val, const std::string &new_val) {
    // Your code here for lab2A
    std::unique_lock<std::mutex> lock(mtx);
    PRINT_MSG("append_log", "pre");

    if (chfs_command::head_size + old_val.size() + new_val.size() + log_size > MAX_LOG_SZ)
        checkpoint();

    chfs_command::actid_t pre = -1;
    for (auto i = log_entries.rbegin(); i != log_entries.rend(); i++)
        if (i->tid == ctid) {
            pre = i->aid;
            break;
        }

    command log(type, ctid, aid++, pre, name, old_val, new_val);
    log_entries.push_back(log);
    log_size += log.size();

    FILE *log_file = fopen(file_path_logfile.c_str(), "a");
    fwrite(log.to_string().c_str(), log.size(), 1, log_file);
    fclose(log_file);
    PRINT_MSG("append_log", "finished");
}

template<typename command>
void persister<command>::append_commit(chfs_command::txid_t ctid) {
    std::unique_lock<std::mutex> lock(mtx);
    PRINT_MSG("append_commit", "pre");

    if (chfs_command::head_size + log_size > MAX_LOG_SZ)
        checkpoint();

    chfs_command::actid_t pre = -1;
    for (auto i = log_entries.rbegin(); i != log_entries.rend(); i++)
        if (i->tid == ctid) {
            pre = i->aid;
            break;
        }

    command log(chfs_command::CMD_COMMIT, ctid, aid++, pre, 0, "", "");
    log_entries.push_back(log);
    log_size += log.size();

    FILE *log_file = fopen(file_path_logfile.c_str(), "a");
    fwrite(log.to_string().c_str(), log.size(), 1, log_file);
    fclose(log_file);
    PRINT_MSG("append_commit", "finished");
}

template<typename command>
void persister<command>::checkpoint() {
    // Your code here for lab2A
    PRINT_MSG("checkpoint", "pre")
    FILE *log_file = fopen(file_path_logfile.c_str(), "r");
    FILE *checkpoint_file = fopen(file_path_checkpoint.c_str(), "a");

    int ch;
    while ((ch = fgetc(log_file)) != EOF)
        fputc(ch, checkpoint_file);
    log_size = 0;

    fclose(log_file);
    fclose(checkpoint_file);
    remove(file_path_logfile.c_str());
    PRINT_MSG("checkpoint", "all finished")
}

template<typename command>
void persister<command>::restore_logdata() {
    // Your code here for lab2A
    PRINT_MSG("restore_logdata", "pre")
    load_logs(file_path_checkpoint.c_str());
    load_logs(file_path_logfile.c_str());

    // mark committed logs
    chfs_command *cmd;
    for (auto i = log_entries.rbegin(); i < log_entries.rend(); i++) {
        if (i->type == chfs_command::CMD_COMMIT) {
            cmd = &log_entries[i->aid];
            while (cmd->pre != -1) {
                cmd->state = 1;
                cmd = &log_entries[cmd->pre];
            }
            cmd->state = 1;
        }
    }

    // redo
    for (auto i = log_entries.begin(); i < log_entries.end(); i++) {
        if (i->state == 1 && i->checked == 0) {
            switch (i->type) {
                case chfs_command::CMD_CREATE:
                    im->alloc_inode(i->new_val[0] - 48, i->name);
                    break;
                case chfs_command::CMD_PUT:
                    im->write_file(i->name, i->new_val.c_str(), i->new_val.size());
                    break;
                case chfs_command::CMD_REMOVE:
                    im->remove_file(i->name);
                    break;
                default:
                    break;
            }
        }
    }

    PRINT_MSG("restore_checkpoint", "redo finished")

    // remove not committed logs
    int64_t p_size = (int64_t)log_entries.size();
    int64_t c_size = (int64_t)log_entries.size();
    for (int64_t i = 0; i < c_size; i++) {
        if (log_entries[i].state == 0) {
            log_size -= log_entries[i].size();
            log_entries.erase(log_entries.begin() + i);
            c_size--;
            i--;
        } else if (log_entries[i].aid != i) {
            for (int64_t j = i + 1; j < c_size; j++)
                if (log_entries[j].pre == log_entries[i].aid)
                    log_entries[j].pre = i;
            log_entries[i].aid = i;
        }
    }

    PRINT_MSG("restore_checkpoint", "remove finished")

    if (c_size > 0)
        tid = MAX(tid, log_entries.back().tid + 1);
    aid = log_entries.size();

    if (c_size == p_size)
        return;

    // rewrite to file
    save_logs(file_path_checkpoint.c_str());
    remove(file_path_logfile.c_str());
}

template<typename command>
void persister<command>::load_logs(const char *filename) {
    FILE *file = fopen(filename, "r");
    if (file == NULL)
        return;

    fseek(file, 0, SEEK_END);
    uint64_t size = ftell(file);
    fseek(file, 0, SEEK_SET);

    char *buf = new char[size];
    fread(buf, size, 1, file);
    fclose(file);
    char *p = buf;

    log_size = 0;
    while (size > 0) {
        chfs_command ent(p);
        p += ent.size();
        log_size += ent.size();
        size -= ent.size();
        log_entries.push_back(ent);
    }

    delete[] buf;
}

template<typename command>
void persister<command>::save_logs(const char *filename) {
    FILE *file = fopen(filename, "w");
    for (auto i = log_entries.begin(); i < log_entries.end(); i++)
        fwrite(i->to_string().c_str(), i->size(), 1, file);
    fclose(file);
}

using chfs_persister = persister<chfs_command>;

#endif // persister_h