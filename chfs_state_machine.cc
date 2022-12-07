#include "chfs_state_machine.h"
#include <cstring>

chfs_command_raft::chfs_command_raft() {
    // Lab3: Your code here
    res = std::make_shared<result>();
    res->done = false;
}

chfs_command_raft::chfs_command_raft(const chfs_command_raft &cmd) :
    cmd_tp(cmd.cmd_tp), type(cmd.type),  id(cmd.id), buf(cmd.buf), res(cmd.res) {
    // Lab3: Your code here
}
chfs_command_raft::~chfs_command_raft() {
    // Lab3: Your code here
}

int chfs_command_raft::size() const{ 
    // Lab3: Your code here
    int id_size = sizeof(extent_protocol::extentid_t);
    int int_size = sizeof(int);
    int u32_size = sizeof(uint32_t);
    switch (cmd_tp) {
        case CMD_CRT:
            return int_size + u32_size;
        case CMD_PUT:
            return 2 * int_size + id_size + buf.size();
        case CMD_GET:
        case CMD_GETA:
        case CMD_RMV:
            return int_size + id_size;
        default:
            return 0;
    }
}

void chfs_command_raft::serialize(char *buf_out, int size) const {
    // Lab3: Your code here
    int id_size = sizeof(extent_protocol::extentid_t);
    int int_size = sizeof(int);
    int u32_size = sizeof(uint32_t);

    int len = buf.size();
    int tp = cmd_tp;

    memcpy(buf_out, (char *)&tp, int_size);

    switch (cmd_tp) {
        case CMD_CRT:
            memcpy(buf_out + int_size, (char *)&type, u32_size);
            break;
        case CMD_PUT:
            memcpy(buf_out + (int_size + id_size), (char *)&len, int_size);
            memcpy(buf_out + (id_size + 2 * int_size), buf.c_str(), len);
        case CMD_GET:
        case CMD_GETA:
        case CMD_RMV:
            memcpy(buf_out + int_size, (char *)&id, id_size);
            break;
        default:
            break;
    }
}

void chfs_command_raft::deserialize(const char *buf_in, int size) {
    // Lab3: Your code here
    int id_size = sizeof(extent_protocol::extentid_t);
    int int_size = sizeof(int);
    int u32_size = sizeof(uint32_t);

    int tp = 0, len = 0;
    char *str;

    memcpy((char *)&tp, buf_in, int_size);
    cmd_tp = (chfs_command_raft::command_type)tp;

    switch (cmd_tp) {
        case CMD_CRT:
            memcpy((char *)&type, buf_in + int_size, u32_size);
            break;
        case CMD_PUT:
            memcpy((char *)&len, buf_in + (int_size + id_size), int_size);
            str = new char[len];
            memcpy(str, buf_in + (2 * int_size + id_size), len);
            buf.assign(str, len);
            delete[] str;
        case CMD_GET:
        case CMD_GETA:
        case CMD_RMV:
            memcpy((char *)&id, buf_in + int_size, id_size);
            break;
        default:
            break;
    }
}

marshall &operator<<(marshall &m, const chfs_command_raft &cmd) {
    // Lab3: Your code here
    int t = cmd.cmd_tp, len = cmd.buf.size();
    m << t;
    switch (cmd.cmd_tp) {
        case chfs_command_raft::CMD_CRT:
            m << cmd.type;
            break;
        case chfs_command_raft::CMD_PUT:
            m << len;
            for (auto c : cmd.buf)
                m << c;
        case chfs_command_raft::CMD_GET:
        case chfs_command_raft::CMD_GETA:
        case chfs_command_raft::CMD_RMV:
            m << cmd.id;
            break;
        default:
            break;
    }
    return m;
}

unmarshall &operator>>(unmarshall &u, chfs_command_raft &cmd) {
    // Lab3: Your code here
    int t = 0, len = 0;
    char *str;
    u >> t;
    cmd.cmd_tp = (chfs_command_raft::command_type)t;
    switch (cmd.cmd_tp) {
        case chfs_command_raft::CMD_CRT:
            u >> cmd.type;
            break;
        case chfs_command_raft::CMD_PUT:
            u >> len;
            str = new char[len];
            for (int i = 0; i < len; i++)
            u >> str[i];
            cmd.buf.assign(str, len);
            delete[] str;
        case chfs_command_raft::CMD_GET:
        case chfs_command_raft::CMD_GETA:
        case chfs_command_raft::CMD_RMV:
            u >> cmd.id;
            break;
        default:
            break;
    }
    return u;
}

void chfs_state_machine::apply_log(raft_command &cmd) {
    chfs_command_raft &chfs_cmd = dynamic_cast<chfs_command_raft &>(cmd);
    // Lab3: Your code here
    std::unique_lock<std::mutex> _mtx;
    std::cout << "apply_log" << std::endl;
    int tmp;
    switch (chfs_cmd.cmd_tp) {
        case chfs_command_raft::CMD_CRT:
            es.create(chfs_cmd.type, chfs_cmd.res->id);
            break;
        case chfs_command_raft::CMD_PUT:
            es.put(chfs_cmd.id, chfs_cmd.buf, tmp);
            break;
        case chfs_command_raft::CMD_GET:
            es.get(chfs_cmd.id, chfs_cmd.res->buf);
            break;
        case chfs_command_raft::CMD_GETA:
            es.getattr(chfs_cmd.id, chfs_cmd.res->attr);
            break;
        case chfs_command_raft::CMD_RMV:
            es.remove(chfs_cmd.id, tmp);
            break;
        default:
            break;
    }
    chfs_cmd.res->done = true;
    chfs_cmd.res->cv.notify_all();
    return;
}


