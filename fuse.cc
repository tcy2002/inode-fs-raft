/*
 * receive request from fuse and call methods of chfs_client
 *
 * started life as low-level example in the fuse distribution.  we
 * have to use low-level interface in order to get i-numbers.  the
 * high-level interface only gives us complete paths.
 */

#include <fuse_lowlevel.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "chfs_client.h"

int myid;
chfs_client *chfs;

int id() { 
    return myid;
}

//
// A file/directory's attributes are a set of information
// including owner, permissions, size, &c. The information is
// much the same as that returned by the stat() system call.
// The kernel needs attributes in many situations, and some
// fuse functions (such as lookup) need to return attributes
// as well as other information, so getattr() gets called a lot.
//
// ChFS fakes most of the attributes. It does provide more or
// less correct values for the access/modify/change times
// (atime, mtime, and ctime), and correct values for file sizes.
//
chfs_client::status
getattr(chfs_client::inum inum, struct stat &st) {
    chfs_client::status ret;

    bzero(&st, sizeof(st));

    st.st_ino = inum;
    printf("getattr %016llx %d\n", inum, chfs->isfile(inum));
    if (chfs->isfile(inum)) {
        chfs_client::fileinfo info;
        ret = chfs->getfile(inum, info);
        if (ret != chfs_client::OK)
            return ret;
        st.st_mode = S_IFREG | 0666;
        st.st_nlink = 1;
        st.st_atime = info.atime;
        st.st_mtime = info.mtime;
        st.st_ctime = info.ctime;
        st.st_size = info.size;
        printf("   getattr file -> %llu\n", info.size);
    } else if (chfs->issymlink(inum)) {
        chfs_client::fileinfo info;
        ret = chfs->getfile(inum, info);
        if (ret != chfs_client::OK)
            return ret;
        st.st_mode = S_IFLNK | 0666;
        st.st_nlink = 7;
        st.st_atime = info.atime;
        st.st_mtime = info.mtime;
        st.st_ctime = info.ctime;
        st.st_size = info.size;
        printf("   getattr symlink -> %llu\n", info.size);
    } else {
        chfs_client::dirinfo info;
        ret = chfs->getdir(inum, info);
        if(ret != chfs_client::OK)
            return ret;
        st.st_mode = S_IFDIR | 0777;
        st.st_nlink = 2;
        st.st_atime = info.atime;
        st.st_mtime = info.mtime;
        st.st_ctime = info.ctime;
        printf("   getattr dir -> %lu %lu %lu\n", info.atime, info.mtime, info.ctime);
    }
    return chfs_client::OK;
}

//
// This is a typical fuse operation handler; you'll be writing
// a bunch of handlers like it.
//
// A handler takes some arguments
// and supplies either a success or failure response. It provides
// an error response by calling either fuse_reply_err(req, errno), and
// a normal response by calling ruse_reply_xxx(req, ...). The req
// argument serves to link up this response with the original
// request; just pass the same @req that was passed into the handler.
// 
// The @ino argument indicates the file or directory FUSE wants
// you to operate on. It's a 32-bit FUSE identifier; just assign
// it to a chfs_client::inum to get a 64-bit ChFS inum.
//
void
fuseserver_getattr(fuse_req_t req, fuse_ino_t ino,
        struct fuse_file_info *fi)
{
    struct stat st;
    chfs_client::inum inum = ino; // req->in.h.nodeid;
    chfs_client::status ret;

    ret = getattr(inum, st);
    if(ret != chfs_client::OK){
        fuse_reply_err(req, ENOENT);
        return;
    }
    fuse_reply_attr(req, &st, 0);
}

//
// Set the attributes of a file. Often used as part of overwriting
// a file, to set the file length to zero.
//
// to_set is a bitmap indicating which attributes to set. You only
// have to implement the FUSE_SET_ATTR_SIZE bit, which indicates
// that the size of the file should be changed. The new size is
// in attr->st_size. If the new size is bigger than the current
// file size, fill the new bytes with '\0'.
//
// On success, call fuse_reply_attr, passing the file's new
// attributes (from a call to getattr()).
//
void
fuseserver_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
        int to_set, struct fuse_file_info *fi)
{
    printf("fuseserver_setattr 0x%x\n", to_set);
    if (FUSE_SET_ATTR_SIZE & to_set) {
        printf("   fuseserver_setattr set size to %zu\n", attr->st_size);

#if 1
    struct stat st;

    printf("fuseserver_setattr\n");
    // Change the above line to "#if 1", and your code goes here
    // Note: fill st using getattr before fuse_reply_attr
    if (getattr(ino, st) != chfs_client::OK) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    st.st_size = attr->st_size;
    if (chfs->setattr(ino, st.st_size) != chfs_client::OK) {
        fuse_reply_err(req, ENOSYS);
        return;
    }

    fuse_reply_attr(req, &st, 0);

#else
    fuse_reply_err(req, ENOSYS);
#endif

    } else {
        fuse_reply_err(req, ENOSYS);
    }
}

//
// Read up to @size bytes starting at byte offset @off in file @ino.
//
// Pass the number of bytes actually read to fuse_reply_buf.
// If there are fewer than @size bytes to read between @off and the
// end of the file, read just that many bytes. If @off is greater
// than or equal to the size of the file, read zero bytes.
//
// Ignore @fi. 
// @req identifies this request, and is used only to send a 
// response back to fuse with fuse_reply_buf or fuse_reply_err.
//
void
fuseserver_read(fuse_req_t req, fuse_ino_t ino, size_t size,
        off_t off, struct fuse_file_info *fi)
{
#if 1
    // Change the above "#if 0" to "#if 1", and your code goes here
    std::string data;

    printf("fuseserver_read\n");

    if (chfs->read(ino, size, off, data) != chfs_client::OK) {
        fuse_reply_err(req, ENOSYS);
        return;
    }
    fuse_reply_buf(req, data.c_str(), data.size());

#else
    fuse_reply_err(req, ENOSYS);
#endif


}

//
// Write @size bytes from @buf to file @ino, starting
// at byte offset @off in the file.
//
// If @off + @size is greater than the current size of the
// file, the write should cause the file to grow. If @off is
// beyond the end of the file, fill the gap with null bytes.
//
// Set the file's mtime to the current time.
//
// Ignore @fi.
//
// @req identifies this request, and is used only to send a 
// response back to fuse with fuse_reply_write or fuse_reply_err.
//
void
fuseserver_write(fuse_req_t req, fuse_ino_t ino,
        const char *buf, size_t size, off_t off,
        struct fuse_file_info *fi)
{
#if 1
    // Change the above line to "#if 1", and your code goes here
    size_t bytes_written;

    printf("fuseserver_write\n");

    if (chfs->write(ino, size, off, buf, bytes_written) != chfs_client::OK) {
        fuse_reply_err(req, ENOSYS);
        return;
    }
    fuse_reply_write(req, bytes_written);

#else
    fuse_reply_err(req, ENOSYS);
#endif
}

//
// Create file @name in directory @parent. 
//
// - @mode specifies the create mode of the file. Ignore it - you do not
//   have to implement file mode.
// - If a file named @name already exists in @parent, return EXIST.
// - Pick an ino (with type of chfs_client::inum) for file @name. 
//   Make sure ino indicates a file, not a directory!
// - Create an empty extent for ino.
// - Add a <name, ino> entry into @parent.
// - Change the parent's mtime and ctime to the current time/date
//   (this may fall naturally out of your extent server code).
// - On success, store the inum of newly created file into @e->ino, 
//   and the new file's attribute into @e->attr. Get the file's
//   attributes with getattr().
//
// @return chfs_client::OK on success, and EXIST if @name already exists.
//
chfs_client::status
fuseserver_createhelper(fuse_ino_t parent, const char *name,
        mode_t mode, struct fuse_entry_param *e, int type)
{
    int ret;
    // In chfs, timeouts are always set to 0.0, and generations are always set to 0
    e->attr_timeout = 0.0;
    e->entry_timeout = 0.0;
    e->generation = 0;

    chfs_client::inum inum;
    if ( type == extent_protocol::T_FILE )
		ret = chfs->create(parent, name, mode, inum);
	else 
		ret = chfs->mkdir(parent,name,mode,inum);
    if (ret != chfs_client::OK)
        return ret;
    e->ino = inum;
    ret = getattr(inum, e->attr);
    return ret;
}

void
fuseserver_create(fuse_req_t req, fuse_ino_t parent, const char *name,
        mode_t mode, struct fuse_file_info *fi)
{
    struct fuse_entry_param e;
    chfs_client::status ret;

    printf("fuseserver_create\n");

    if( (ret = fuseserver_createhelper( parent, name, mode, &e, extent_protocol::T_FILE)) == chfs_client::OK ) {
        fuse_reply_create(req, &e, fi);
        printf("OK: create returns.\n");
    } else {
        if (ret == chfs_client::EXIST) {
            fuse_reply_err(req, EEXIST);
        }else{
            fuse_reply_err(req, ENOENT);
        }
    }
}

void fuseserver_mknod( fuse_req_t req, fuse_ino_t parent, 
        const char *name, mode_t mode, dev_t rdev ) {
    struct fuse_entry_param e;
    chfs_client::status ret;

    printf("fuseserver_mknod\n");

    if( (ret = fuseserver_createhelper( parent, name, mode, &e, extent_protocol::T_FILE)) == chfs_client::OK ) {
        fuse_reply_entry(req, &e);
    } else {
        if (ret == chfs_client::EXIST) {
            fuse_reply_err(req, EEXIST);
        }else{
            fuse_reply_err(req, ENOENT);
        }
    }
}

//
// Look up file or directory @name in the directory @parent. If @name is
// found, set e.attr (using getattr) and e.ino to the attribute and inum of
// the file.
//
void
fuseserver_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
    struct fuse_entry_param e;
    // In chfs, timeouts are always set to 0.0, and generations are always set to 0
    e.attr_timeout = 0.0;
    e.entry_timeout = 0.0;
    e.generation = 0;
    bool found = false;
    chfs_client::inum ino;

    printf("fuseserver_lookup\n");

    chfs->lookup(parent, name, found, ino);

    if (found) {
        e.ino = ino;
        getattr(ino, e.attr);
        fuse_reply_entry(req, &e);
    } else {
        fuse_reply_err(req, ENOENT);
    }

}


struct dirbuf {
    char *p;
    size_t size;
};

void dirbuf_add(struct dirbuf *b, const char *name, fuse_ino_t ino)
{
    struct stat stbuf;
    size_t oldsize = b->size;
    b->size += fuse_dirent_size(strlen(name));
    b->p = (char *) realloc(b->p, b->size);
    memset(&stbuf, 0, sizeof(stbuf));
    stbuf.st_ino = ino;
    fuse_add_dirent(b->p + oldsize, name, &stbuf, b->size);
}

#define min(x, y) ((x) < (y) ? (x) : (y))

int reply_buf_limited(fuse_req_t req, const char *buf, size_t bufsize,
        off_t off, size_t maxsize)
{
    if ((size_t)off < bufsize)
        return fuse_reply_buf(req, buf + off, min(bufsize - off, maxsize));
    else
        return fuse_reply_buf(req, NULL, 0);
}

//
// Retrieve all the file names / i-numbers pairs
// in directory @ino. Send the reply using reply_buf_limited.
//
// You can ignore @size and @off (except that you must pass
// them to reply_buf_limited).
//
// Call dirbuf_add(&b, name, inum) for each entry in the directory.
//
void
fuseserver_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
        off_t off, struct fuse_file_info *fi)
{
    chfs_client::inum inum = ino; // req->in.h.nodeid;
    struct dirbuf b;

    printf("fuseserver_readdir\n");

    if(!chfs->isdir(inum)){
        fuse_reply_err(req, ENOTDIR);
        return;
    }

    memset(&b, 0, sizeof(b));

    std::list<chfs_client::dirent> entries;
    chfs->readdir(inum, entries);
    for (std::list<chfs_client::dirent>::iterator it = entries.begin(); it != entries.end(); ++it) {
        dirbuf_add(&b, it->name.c_str(), (fuse_ino_t) it->inum);
    }

    reply_buf_limited(req, b.p, b.size, off, size);
    free(b.p);
}


void
fuseserver_open(fuse_req_t req, fuse_ino_t ino,
        struct fuse_file_info *fi)
{
    fuse_reply_open(req, fi);
}

//
// Create a new directory with name @name in parent directory @parent.
// Leave new directory's inum in e.ino and attributes in e.attr.
//
// The new directory should be empty (no . or ..).
// 
// If a file/directory named @name already exists, indicate error EEXIST.
//
// Ignore mode.
//
void
fuseserver_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
        mode_t mode)
{
    struct fuse_entry_param e;
    // In chfs, timeouts are always set to 0.0, and generations are always set to 0
    e.attr_timeout = 0.0;
    e.entry_timeout = 0.0;
    e.generation = 0;
    // Suppress compiler warning of unused e.
    //(void) e;

#if 1
    // Change the above line to "#if 1", and your code goes here
    int ret;
    chfs_client::inum ino;

    printf("my symlink\n");

    if ((ret = chfs->mkdir(parent, name, mode, ino)) != chfs_client::OK) {
        if (ret == chfs_client::EXIST)
            fuse_reply_err(req, EEXIST);
        else
            fuse_reply_err(req, ENOSYS);
        return;
    }
    e.ino = ino;
    if (getattr(ino, e.attr) != chfs_client::OK) {
        fuse_reply_err(req, ENOENT);
        return;
    }

    fuse_reply_entry(req, &e);

#else
    fuse_reply_err(req, ENOSYS);
#endif

}

//
// Remove the file named @name from directory @parent.
// Free the file's extent.
// If the file doesn't exist, indicate error ENOENT.
//
// Do *not* allow unlinking of a directory.
//
void
fuseserver_unlink(fuse_req_t req, fuse_ino_t parent, const char *name)
{
    int r;

    printf("fuseserver_unlink\n");

    if ((r = chfs->unlink(parent, name)) == chfs_client::OK) {
        fuse_reply_err(req, 0);
    } else {
        if (r == chfs_client::NOENT) {
            fuse_reply_err(req, ENOENT);
        } else {
            fuse_reply_err(req, ENOTEMPTY);
        }
    }
}

void
fuseserver_symlink(fuse_req_t req, const char *link, fuse_ino_t parent, const char *name)
{
    chfs_client::inum ino;
    struct fuse_entry_param e;
    chfs_client::status ret;

    printf("fuseserver_symlink\n");

    e.attr_timeout = 0.0;
    e.entry_timeout = 0.0;
    e.generation = 0;

    if ((ret = chfs->symlink(link, parent, name, ino)) != chfs_client::OK) {
        if (ret == chfs_client::NOENT)
            fuse_reply_err(req, ENOENT);
        else
            fuse_reply_err(req, ENOSYS);
        return;
    }
    e.ino = ino;
    if (getattr(ino, e.attr) != chfs_client::OK) {
        fuse_reply_err(req, ENOENT);
        return;
    }

    fuse_reply_entry(req, &e);
}

void
fuseserver_readlink(fuse_req_t req, fuse_ino_t ino)
{
    std::string data;

    printf("fuseserver_readlink\n");

    if (chfs->readlink(ino, data) != chfs_client::OK) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    fuse_reply_buf(req, data.c_str(), data.size());
}

void
fuseserver_statfs(fuse_req_t req)
{
    struct statvfs buf;

    printf("fuseserver_statfs\n");

    memset(&buf, 0, sizeof(buf));

    buf.f_namemax = 255;
    buf.f_bsize = 512;

    fuse_reply_statfs(req, &buf);
}

struct fuse_lowlevel_ops fuseserver_oper;

int
main(int argc, char *argv[])
{
    char *mountpoint = 0;
    int err = -1;
    int fd;

    setvbuf(stdout, NULL, _IONBF, 0);

#if 0
    if(argc != 4){
        fprintf(stderr, "Usage: chfs_client <mountpoint> <port-extent-server> <port-lock-server>\n");
        exit(1);
    }
#endif
    if(argc != 2){
        fprintf(stderr, "Usage: chfs_client <mountpoint>\n");
        exit(1);
    }
    mountpoint = argv[1];

    srandom(getpid());

    myid = random();

    // chfs = new chfs_client(argv[2], argv[3]);
    chfs = new chfs_client();

    fuseserver_oper.getattr    = fuseserver_getattr;
    fuseserver_oper.statfs     = fuseserver_statfs;
    fuseserver_oper.readdir    = fuseserver_readdir;
    fuseserver_oper.lookup     = fuseserver_lookup;
    fuseserver_oper.create     = fuseserver_create;
    fuseserver_oper.mknod      = fuseserver_mknod;
    fuseserver_oper.open       = fuseserver_open;
    fuseserver_oper.read       = fuseserver_read;
    fuseserver_oper.write      = fuseserver_write;
    fuseserver_oper.setattr    = fuseserver_setattr;
    fuseserver_oper.unlink     = fuseserver_unlink;
    fuseserver_oper.mkdir      = fuseserver_mkdir;
    fuseserver_oper.symlink    = fuseserver_symlink;
    fuseserver_oper.readlink   = fuseserver_readlink;
    /** Your code here for Lab.
     * you may want to add
     * routines here to implement symbolic link,
     * rmdir, etc.
     * */

    const char *fuse_argv[20];
    int fuse_argc = 0;
    fuse_argv[fuse_argc++] = argv[0];
#ifdef __APPLE__
    fuse_argv[fuse_argc++] = "-o";
    fuse_argv[fuse_argc++] = "nolocalcaches"; // no dir entry caching
    fuse_argv[fuse_argc++] = "-o";
    fuse_argv[fuse_argc++] = "daemon_timeout=86400";
#endif

    // everyone can play, why not?
    //fuse_argv[fuse_argc++] = "-o";
    //fuse_argv[fuse_argc++] = "allow_other";

    fuse_argv[fuse_argc++] = mountpoint;
    fuse_argv[fuse_argc++] = "-d";

    fuse_args args = FUSE_ARGS_INIT( fuse_argc, (char **) fuse_argv );
    int foreground;
    int res = fuse_parse_cmdline( &args, &mountpoint, 0 /*multithreaded*/, 
            &foreground );
    if( res == -1 ) {
        fprintf(stderr, "fuse_parse_cmdline failed\n");
        return 0;
    }

    args.allocated = 0;

    fd = fuse_mount(mountpoint, &args);
    if(fd == -1){
        fprintf(stderr, "fuse_mount failed\n");
        exit(1);
    }

    struct fuse_session *se;

    se = fuse_lowlevel_new(&args, &fuseserver_oper, sizeof(fuseserver_oper),
            NULL);
    if(se == 0){
        fprintf(stderr, "fuse_lowlevel_new failed\n");
        exit(1);
    }

    struct fuse_chan *ch = fuse_kern_chan_new(fd);
    if (ch == NULL) {
        fprintf(stderr, "fuse_kern_chan_new failed\n");
        exit(1);
    }

    fuse_session_add_chan(se, ch);
    // err = fuse_session_loop_mt(se);   // FK: wheelfs does this; why?
    err = fuse_session_loop(se);

    fuse_session_destroy(se);
    close(fd);
    fuse_unmount(mountpoint);

    return err ? 1 : 0;
}
