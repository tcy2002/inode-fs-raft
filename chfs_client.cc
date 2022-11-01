// chfs client.  implements FS operations using extent and lock server
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "chfs_client.h"
#include "extent_client.h"

/* 
 * Your code here for Lab2A:
 * Here we treat each ChFS operation(especially write operation such as 'create', 
 * 'write' and 'symlink') as a transaction, your job is to use write ahead log 
 * to achive all-or-nothing for these transactions.
 */

#define MIN(a, b)   (a) < (b) ? (a) : (b)
#define MAX(a, b)   (a) > (b) ? (a) : (b)

#define MAX_NAME_LENGTH 68

#define PRINT_ERROR(func, msg, data) printf("%s: %s, %lld\n", (func), (msg), (data));

/*
 * directory format (1 disk block):
 * size    60      4     64
 * 1    |name...|inum|
 * 2    |name...|inum|
 * ...
 * 8    |name...|inum|  512
 *
 * tricks encountered:
 * 1. take care when using strcmp or std::string.compare
 * 2. the fuse.cc must be completed after implementing corresponding functions
 * 3. using windows docker desktop would cause unexpected errors
 * 4. every word in tutorial should be carefully considered
 */

struct entry {
    char name[MAX_NAME_LENGTH];
    uint32_t inum;
};

chfs_client::chfs_client()
{
    ec = new extent_client();

}

chfs_client::chfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "", 0) != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is not a file, %d\n", inum, a.type);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
chfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isfile: %lld is a dir\n", inum);
        return true;
    }
    printf("isfile: %lld is not a dir, %d\n", inum, a.type);
    return false;
}

bool
chfs_client::issymlink(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_SYMLINK) {
        printf("isfile: %lld is a symlink\n", inum);
        return true;
    }
    printf("isfile: %lld is not a symlink, %d\n", inum, a.type);
    return false;
}

int
chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    std::string buf_old, buf_new;
    char buf[size];
    chfs_command::txid_t tid;

    EXT_RPC(ec->tx_begin(tid));
    EXT_RPC(ec->get(ino, buf_old));

    memset(buf, '\0', size);
    memcpy(buf, buf_old.c_str(), MIN(size, buf_old.size()));
    buf_new.assign(buf, size);

    EXT_RPC(ec->put(ino, buf_new, tid));
    EXT_RPC(ec->tx_commit(tid));

release:
    return r;
}

/* add one entry to a directory */
int 
chfs_client::addentry(inum parent, inum ino, const char *name, chfs_command::txid_t tid)
{
    int r = OK;
    int entry_num;
    entry *entries;
    chfs_client::dirent de;
    std::list<dirent> list;
    std::string buf;

    if (!isdir(parent)) {
        PRINT_ERROR("my addentry", "not dir error", parent)
        return IOERR;
    }

    EXT_RPC(readdir(parent, list));

    entry_num = list.size();
    entries = new entry[entry_num + 1];
    for (int i = 0; i < entry_num; i++) {
        de = list.front();
        memcpy(entries[i].name, de.name.c_str(), MAX_NAME_LENGTH);
        entries[i].inum = (uint32_t)de.inum;
        list.pop_front();
    }
    memset(entries[entry_num].name, '\0', MAX_NAME_LENGTH);
    strncpy(entries[entry_num].name, name, MAX_NAME_LENGTH);
    entries[entry_num].inum = ino;
    buf.assign((char *)entries, (entry_num + 1) * sizeof(entry));

    EXT_RPC(ec->put(parent, buf, tid));

    delete[] entries;

release: 
    return r;
}

/* remove one entry from a directory */
int 
chfs_client::unlinkentry(inum parent, const char *name, chfs_command::txid_t tid)
{
    int r = OK;
    int entry_num;
    entry *entries;
    chfs_client::dirent de;
    std::list<dirent> list;
    std::string buf;

    if (!isdir(parent)) {
        PRINT_ERROR("my unlinkentry", "not dir error", parent)
        return IOERR;
    }

    EXT_RPC(readdir(parent, list));

    entry_num = list.size();
    if (entry_num <= 1) {
        EXT_RPC(ec->put(parent, "", tid));
        return r;
    }

    entries = new entry[entry_num - 1];
    for (int i = 0; i < entry_num - 1; i++) {
        de = list.front();
        if (strncmp(name, de.name.c_str(), MAX_NAME_LENGTH) != 0) {
            memcpy(entries[i].name, de.name.c_str(), MAX_NAME_LENGTH);
            entries[i].inum = de.inum;
        } else
            i--;
        list.pop_front();
    }
    buf.assign((char *)entries, (entry_num - 1) * sizeof(entry));

    EXT_RPC(ec->put(parent, buf, tid));

    delete[] entries;

release:
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    bool found;
    inum ino;
    chfs_command::txid_t tid;

    PRINT_MSG("chfs_client::create", name)

    EXT_RPC(ec->tx_begin(tid));

    if (!isdir(parent)) {
        PRINT_ERROR("my create", "not dir error", parent)
        return IOERR;
    }

    EXT_RPC(lookup(parent, name, found, ino));

    if (found) {
        PRINT_ERROR("my create", "exist error", parent)
        return EXIST;
    }

    EXT_RPC(ec->create(extent_protocol::T_FILE, ino, tid));
    EXT_RPC(addentry(parent, ino, name, tid));

    ino_out = ino;

    EXT_RPC(ec->tx_commit(tid));

release:
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    bool found;
    inum ino;
    chfs_command::txid_t tid;

    EXT_RPC(ec->tx_begin(tid));

    if (!isdir(parent)) {
        PRINT_ERROR("my mkdir", "not dir error", parent)
        return IOERR;
    }

    EXT_RPC(lookup(parent, name, found, ino));

    if (found) {
        PRINT_ERROR("my mkdir", "duplicate error", parent)
        return EXIST;
    }

    EXT_RPC(ec->create(extent_protocol::T_DIR, ino, tid));
    EXT_RPC(addentry(parent, ino, name, tid));
    EXT_RPC(ec->tx_commit(tid));

    ino_out = ino;

release:
    return r;
}

int
chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    found = false;
    std::list<dirent> list;

    if (!isdir(parent)) {
        PRINT_ERROR("my lookup", "not dir error", parent)
        return IOERR;
    }

    EXT_RPC(readdir(parent, list));

    for (const dirent &de : list) {
        if (strncmp(name, de.name.c_str(), MAX_NAME_LENGTH) == 0) {
            found = true;
            ino_out = de.inum;
            goto release;
        }
    }

release:
    return r;
}

int
chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    std::string buf;
    int entry_num;
    dirent de;
    entry *entries;

    if (!isdir(dir)) {
        PRINT_ERROR("my readdir", "not dir error", dir)
        return IOERR;
    }

    EXT_RPC(ec->get(dir, buf));

    entry_num = buf.size() / sizeof(entry);
    entries = new entry[entry_num];
    memcpy((char *)entries, buf.c_str(), buf.size());

    list.clear();
    for (int i = 0; i < entry_num; i++) {
        de = {{entries[i].name, MAX_NAME_LENGTH}, entries[i].inum};
        list.push_back(de);
    }

    delete[] entries;

release:
    return r;
}

int
chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */
    std::string buf;
    char *buf_out;
    int size_out;

    if (!isfile(ino)) {
        PRINT_ERROR("my read", "not file error", ino)
        return IOERR;
    }

    EXT_RPC(ec->get(ino, buf));

    size_out = MIN(buf.size() - off, size);
    buf_out = new char[size_out];
    memcpy(buf_out, buf.c_str() + off, size_out);
    data.assign(buf_out, size_out);

    delete[] buf_out;

release:
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    std::string buf, str_new;
    char *buf_new;
    int size_new;
    chfs_command::txid_t tid;

    EXT_RPC(ec->tx_begin(tid));

    if (!isfile(ino)) {
        PRINT_ERROR("my write", "not file error", ino)
        return IOERR;
    }

    EXT_RPC(ec->get(ino, buf));

    size_new = MAX(buf.size(), off + size);
    buf_new = new char[size_new];
    memset(buf_new, '\0', size_new);
    memcpy(buf_new, buf.c_str(), buf.size());
    memcpy(buf_new + off, data, size);
    str_new.assign(buf_new, size_new);

    EXT_RPC(ec->put(ino, str_new, tid));

    bytes_written = size;
    delete[] buf_new;

    EXT_RPC(ec->tx_commit(tid));

release:
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int chfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    bool found;
    inum ino;
    chfs_command::txid_t tid;

    EXT_RPC(ec->tx_begin(tid));

    if (!isdir(parent)) {
        PRINT_ERROR("my unlink", "not dir error", parent)
        return IOERR;
    }

    EXT_RPC(lookup(parent, name, found, ino));

    if (!found) {
        PRINT_ERROR("my unlink", "not found error", parent)
        return NOENT;
    }
    if (!isfile(ino)) {
        PRINT_ERROR("my unlink", "not file error", parent)
        return IOERR;
    }

    EXT_RPC(ec->remove(ino, tid));
    EXT_RPC(unlinkentry(parent, name, tid));
    EXT_RPC(ec->tx_commit(tid));

release:
    return r;
}

int
chfs_client::symlink(const char *link, inum parent, const char *name, inum &ino_out)
{
    int r = OK;
    inum ino;
    std::string buf;
    chfs_command::txid_t tid;

    EXT_RPC(ec->tx_begin(tid));

    if (!isdir(parent)) {
        PRINT_ERROR("my symlink", "not dir error", parent)
        return IOERR;
    }
    buf.assign(link);

    EXT_RPC(ec->create(extent_protocol::T_SYMLINK, ino, tid));
    EXT_RPC(ec->put(ino, buf, tid));
    EXT_RPC(addentry(parent, ino, name, tid));

    ino_out = ino;

    EXT_RPC(ec->tx_commit(tid));

release:
    return r;
}

int
chfs_client::readlink(inum ino, std::string &buf_out)
{
    int r = OK;
    std::string buf;

    if (!issymlink(ino)) {
        PRINT_ERROR("my readlink", "not symlink error", ino)
        return IOERR;
    }

    EXT_RPC(ec->get(ino, buf));

    buf_out.assign(buf);

release:
    return r;
}
