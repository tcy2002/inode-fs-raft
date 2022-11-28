#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <fstream>
#include <unistd.h>

template <typename command>
class raft_storage {
public:
    raft_storage(const std::string &file_dir);
    ~raft_storage();
    // Lab3: Your code here
    void store(const std::string &name, int value);
    void load(const std::string &name, int &value);

    void store(const std::string &name, log_entry<command> &value);
    void load(const std::string &name, std::vector<log_entry<command>> &value);

    void store(const std::string &name, std::vector<char> &value);
    void load(const std::string &name, std::vector<char> &value);

    void remove_back(const std::string &name, int index);
    void remove_front(const std::string &name, int index);
    void remove_all(const std::string &name);

private:
    // Lab3: Your code here
    std::mutex mtx;
    std::string dir;
    std::vector<int> log_line_size{};

    int size_of_entry(log_entry<command> &value);
};

template <typename command>
raft_storage<command>::raft_storage(const std::string &file_dir): dir(file_dir) {
    // Lab3: Your code here
}

template <typename command>
raft_storage<command>::~raft_storage() {
    // Lab3: Your code here
}

template <typename command>
void raft_storage<command>::store(const std::string &name, int value) {
    std::unique_lock<std::mutex> _(mtx);
    std::ofstream file(dir + "/" + name);
    file << value << "\n";
    file.close();
}

template <typename command>
void raft_storage<command>::load(const std::string &name, int &value) {
    std::unique_lock<std::mutex> _(mtx);
    std::ifstream file(dir + "/" + name);
    if (!file.is_open())
        return;

    file >> value;
    file.close();
}

template <typename command>
void raft_storage<command>::store(const std::string &name, log_entry<command> &value) {
    std::unique_lock<std::mutex> _(mtx);
    std::ofstream file(dir + "/" + name, std::ios::app);
    int size = value.cmd.size();
    file << size << " ";
    auto buf = new char[size];
    value.cmd.serialize(buf, size);
    file.write(buf, size);
    file << " " << value.term << "\n";

    // record the size of each line, for the sake of removing
    log_line_size.push_back(size_of_entry(value));

    file.close();
    delete[] buf;
}

template <typename command>
void raft_storage<command>::load(const std::string &name, std::vector<log_entry<command>> &value) {
    std::unique_lock<std::mutex> _(mtx);
    std::ifstream file(dir + "/" + name);
    if (!file.is_open())
        return;

    value.clear();
    log_line_size.clear();

    while (true) {
        log_entry<command> ent{};
        int size{};
        file >> size;
        if (size == 0)
            break;

        auto buf = new char[size];
        file.ignore();
        file.read(buf, size);
        ent.cmd.deserialize(buf, size);
        file >> ent.term;
        value.push_back(ent);

        // record the size of each line, for the sake of removing
        log_line_size.push_back(size_of_entry(ent));
        delete[] buf;
    }

    file.close();
}

template <typename command>
void raft_storage<command>::store(const std::string &name, std::vector<char> &value) {
    std::unique_lock<std::mutex> _(mtx);
    std::ofstream file(dir + "/" + name);
    file << (int)value.size() << " ";
    for (auto c : value)
        file.put(c);
    file.close();
}

template <typename command>
void raft_storage<command>::load(const std::string &name, std::vector<char> &value) {
    std::unique_lock<std::mutex> _(mtx);
    std::ifstream file(dir + "/" + name);
    if (!file.is_open())
        return;
    int size = 0;
    file >> size;
    file.ignore();
    value.assign(size, 0);
    for (int i = 0; i < size; i++)
        file.get(value[i]);
    file.close();
}

template <typename command>
void raft_storage<command>::remove_back(const std::string &name, int index) {
    std::unique_lock<std::mutex> _(mtx);
    if (index >= (int)log_line_size.size())
        return;
    std::string file_path = dir + "/" + name;
    int size = 0;
    for (int i = 0; i < index; i++)
        size += log_line_size[i];
    truncate(file_path.c_str(), size);
    log_line_size.erase(log_line_size.begin() + index, log_line_size.end());
    std::cout << "remove log: " << index << " current num: " << log_line_size.size() << std::endl;
}

template <typename command>
void raft_storage<command>::remove_front(const std::string &name, int index) {
    std::unique_lock<std::mutex> _(mtx);
    if (index <= 0)
        return;
    std::string file_path = dir + "/" + name;

    std::ifstream old_file(file_path);
    if (!old_file.is_open())
        return;
    old_file.seekg(0, std::ifstream::end);
    int length = old_file.tellg(), size = 0, remainder;
    for (int i = 0; i < index; i++)
        size += log_line_size[i];
    remainder = length - size;
    auto buf = new char[remainder];
    old_file.seekg(size);
    old_file.read(buf, remainder);
    old_file.close();

    std::ofstream new_file(file_path);
    new_file.write(buf, remainder);
    new_file.close();
    log_line_size.erase(log_line_size.begin(), log_line_size.begin() + index);
    std::cout << "remove front: " << index << " current num: " << log_line_size.size() << std::endl;
}

template <typename command>
void raft_storage<command>::remove_all(const std::string &name) {
    remove_back(name, 0);
}

template <typename command>
int raft_storage<command>::size_of_entry(log_entry<command> &value) {
    int log_size = value.cmd.size(), size = log_size + 5, term_size = value.term;
    while ((log_size /= 10))
        size++;
    while ((term_size /= 10))
        size++;
    return size;
}

#endif // raft_storage_h