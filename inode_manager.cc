#include <time.h>
#include "inode_manager.h"

#define PRINT_ERROR(func, msg, data) printf("%s: %s, %d\n", (func), (msg), (data));

#define CHECK_INUM(func, inum) \
if ((inum) < 1 || (inum) > INODE_NUM) { \
    PRINT_ERROR((func), "invalid inode number", inum) \
    exit(EXIT_FAILURE); \
}
#define CHECK_BNUM(func, bnum) \
if ((bnum) < 0 || (bnum) >= BLOCK_NUM) { \
    PRINT_ERROR((func), "invalid block number", bnum) \
    exit(EXIT_FAILURE); \
}
#define CHECK_TYPE(func, type) \
if ((type) != 0                \
    && (type) != extent_protocol::T_DIR \
    && (type) != extent_protocol::T_FILE\
    && (type) != extent_protocol::T_SYMLINK) { \
    PRINT_ERROR((func), "invalid file type", type) \
    exit(EXIT_FAILURE); \
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

typedef struct indirect {
    blockid_t blocks[NINDIRECT];
} indirect_t;

/*
 * layout of disk
 * |--|sb|bm|bm|bm|bm|bm|bm|bm|bm|it|it|it|it|......|da|da|da|...|
 *  0  1  2  3  4  5  6  7  8  9  10 11 12 13 14 15 ......1036
 *    1 sb       8 bitmap block      1024 inode block
 *
 * tricks encountered:
 * 1. difference between unsigned char and char
 * 2. should block indices start from 0 or 1
 * 3. consistency of alloc_block and free_block
 * 5. when overwriting a file, all occasions of old size and
 *    new size should be considered
 *
 */

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
    CHECK_BNUM("read_block", id)

    memcpy(buf, (char *)blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
    CHECK_BNUM("write_block", id)

    memcpy((char *)blocks[id], (char *)buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

blockid_t free_bnum = 2 + BLOCK_NUM / BPB + INODE_NUM / IPB;

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
    blockid_t bnum = free_bnum;
    blockid_t blockid = BBLOCK(bnum);
    int byteid = ((bnum % BPB) / 8);
    char mask = 0x01 << ((bnum % BPB) % 8);
    char *buf = new char[BLOCK_SIZE];

    read_block(blockid, buf);
    buf[byteid] |= mask;
    write_block(blockid, buf);

    while (1) {
        while (mask & buf[byteid])
            ++free_bnum && (mask <<= 1);
        if (mask) {
            delete[] buf;
            return bnum;
        }
        mask = 0x01;
        byteid++;

        while (byteid < BLOCK_SIZE && (unsigned char)buf[byteid] == 0xff)
            ++byteid && (free_bnum += 8);

        if (byteid == BLOCK_SIZE) {
            blockid++;
            if (blockid > BLOCK_NUM / BPB + 1)
                break;
            byteid = 0;
            read_block(blockid, buf);
        }
    }

    delete[] buf;
    PRINT_ERROR("alloc_block", "no free block", 0)
    return 0;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
    CHECK_BNUM("free_block", id)

    blockid_t blockid = BBLOCK(id);
    int byteid = (id % BPB) / 8;
    char mask = 0x01 << ((id % BPB) % 8);
    char *buf = new char[BLOCK_SIZE];

    read_block(blockid, buf);
    buf[byteid] &= ~mask;
    write_block(blockid, buf);

    if (id < free_bnum)
        free_bnum = id;

    delete[] buf;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

/* read the data of a file from multiple blocks */
int
inode_manager::read_blocks(blockid_t *blocks, char *buf_out, int size, int max_num) {
    char *buf_p = buf_out;
    char *block_buf = new char[BLOCK_SIZE];
    int id = 0, r_size = size;

    while (id < max_num && blocks[id] > 0) {
        bm->read_block(blocks[id], block_buf);
        memcpy(buf_p, block_buf, MIN(BLOCK_SIZE, r_size));

        buf_p += BLOCK_SIZE;
        r_size -= BLOCK_SIZE;
        id++;
    }

    delete[] block_buf;
    return r_size;
}

/* write the data of a file to multiple blocks */
int
inode_manager::write_blocks(blockid_t *blocks, const char *buf, int size, int max_num) {
    int id = 0, r_size = size;
    char *buf_p = (char *)buf;
    char *block_buf = new char[BLOCK_SIZE];

    while (id < max_num && (blocks[id] > 0 || r_size > 0)) {
        if (r_size > 0) {
            if (blocks[id] == 0)
                blocks[id] = bm->alloc_block();

            if (r_size < BLOCK_SIZE)
                bm->read_block(blocks[id], block_buf);
            memcpy(block_buf, buf_p, MIN(BLOCK_SIZE, r_size));
            bm->write_block(blocks[id], block_buf);

            buf_p += BLOCK_SIZE;
            r_size -= BLOCK_SIZE;
        } else {
            bm->free_block(blocks[id]);
            blocks[id] = 0;
        }

        id++;
    }

    delete[] block_buf;
    return r_size;
}

/* remove indirect block and its target blocks */
void
inode_manager::remove_indirect(blockid_t bnum_i) {
    CHECK_BNUM("remove_indirect", bnum_i)

    indirect_t block_i;
    blockid_t id = 0;

    bm->read_block(bnum_i, (char *)&block_i);
    while (id < NINDIRECT && block_i.blocks[id] > 0)
        bm->free_block(block_i.blocks[id++]);

    bm->free_block(bnum_i);
}

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type, uint32_t inum)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
    CHECK_TYPE("alloc_inode", type)

    struct inode *ino;
    if (inum == 0) {
        for (inum = 1; inum <= INODE_NUM; inum++) {
            ino = get_inode(inum);
            if (ino->type == 0) {
                memset(ino, 0, sizeof(inode_t));
                ino->type = type;
                ino->ctime = time(NULL);
                put_inode(inum, ino);
                free(ino);
                return inum;
            }
            free(ino);
        }

        PRINT_ERROR("alloc_inode", "no free inode", 0)
        return 0;
    } else {
        ino = get_inode(inum);
        if (ino->type == 0) {
            memset(ino, 0, sizeof(inode_t));
            ino->type = type;
            ino->ctime = time(NULL);
            put_inode(inum, ino);
            free(ino);
            return inum;
        }

        PRINT_ERROR("alloc_inode", "invalid inum", inum)
        return 0;
    }
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
    CHECK_INUM("free_inode", inum)

    inode_t *ino = get_inode(inum);
    if (ino == NULL)
        return;

    memset(ino, 0, sizeof(inode_t));
    put_inode(inum, ino);

    free(ino);
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino;
  /* 
   * your code goes here.
   */
    CHECK_INUM("get_inode", inum)

    char *buf = new char[BLOCK_SIZE];
    inode_t *ino_disk;

    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
    ino_disk = (inode_t *)buf +inum%IPB;
    ino = (inode_t *)malloc(sizeof(inode_t));
    *ino = *ino_disk;

    delete[] buf;
    CHECK_TYPE("get_inode", ino->type)

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
    if (ino == NULL)
        return;
    CHECK_INUM("put_inode", inum)
    CHECK_TYPE("put_inode", ino->type)

  char *buf = new char[BLOCK_SIZE];
  struct inode *ino_disk;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);

  delete[] buf;
}

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
    inode_t *ino = get_inode(inum);
    if (ino == NULL || ino->type == 0) {
        *size = 0;
        return;
    }

    *size = ino->size;
    *buf_out = (char *)malloc(*size);
    int r_size = read_blocks(ino->blocks, *buf_out, *size, NDIRECT);

    //read the extra data from indirect blocks
    if (r_size > 0) {
        indirect_t block_i;
        bm->read_block(ino->blocks[NDIRECT], (char *)&block_i);
        read_blocks(block_i.blocks, *buf_out + (*size - r_size), r_size, NINDIRECT);
    }

    ino->atime = time(NULL);
    put_inode(inum, ino);
    free(ino);
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
    CHECK_INUM("write_file", inum)
    if (size > (int)(MAXFILE * BLOCK_SIZE)) {
        PRINT_ERROR("fatal error", "file is too big\n", size)
        exit(EXIT_FAILURE);
    }

    inode_t *ino = get_inode(inum);
    if (ino == NULL || ino->type == 0)
        exit(EXIT_FAILURE);

    int r_size = write_blocks(ino->blocks, buf, size, NDIRECT);

    //write the extra data to indirect blocks
    if (r_size > 0) {
        indirect_t block_i;
        if (ino->blocks[NDIRECT] > 0)
            bm->read_block(ino->blocks[NDIRECT], (char *)&block_i);
        else {
            ino->blocks[NDIRECT] = bm->alloc_block();
            ino->ctime = time(NULL);
            memset(&block_i, 0, sizeof(indirect_t));
        }

        write_blocks(block_i.blocks, (char *)buf + (size - r_size), r_size, NINDIRECT);
        bm->write_block(ino->blocks[NDIRECT], (char *)&block_i);
    } else if (ino->blocks[NDIRECT] > 0) {
        remove_indirect(ino->blocks[NDIRECT]);
        ino->blocks[NDIRECT] = 0;
        ino->ctime = time(NULL);
    }

    ino->size = size;
    ino->mtime = time(NULL);
    if (ino->type == extent_protocol::T_DIR)
        ino->ctime = time(NULL);
    put_inode(inum, ino);
    free(ino);
}

void
inode_manager::get_attr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
    CHECK_INUM("get_attr", inum)

    inode_t *ino = get_inode(inum);
    if (ino == NULL)
        return;

    a.type = ino->type;
    a.atime = ino->atime;
    a.mtime = ino->mtime;
    a.ctime = ino->ctime;
    a.size = ino->size;

    free(ino);
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
    CHECK_INUM("remove_file", inum)

    inode_t *ino = get_inode(inum);
    if (ino == NULL)
        exit(EXIT_FAILURE);

    for (int id = 0; id < NDIRECT && ino->blocks[id] > 0; id++)
        bm->free_block(ino->blocks[id]);

    //remove the extra data in indirect blocks
    if (ino->blocks[NDIRECT] > 0)
        remove_indirect(ino->blocks[NDIRECT]);

    free_inode(inum);
    free(ino);
}
