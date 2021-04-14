////
// @file block.h
// @brief
// 定义block
// block是记录、索引的存储单元。在MySQL和HBase中，存储单元与分配单位是分开的，一般来说，
// 最小分配单元要比block大得多。
// block的布局如下，每个slot占用2B，这要求block最大为64KB。由于记录和索引要求按照4B对
// 齐，BLOCK_DATA、BLOCK_TRAILER也要求8B对齐。
//
// +--------------------+
// |   common header    |
// +--------------------+
// |  data/index header |
// +--------------------+ <--- BLOCK_DATA
// |                    |
// |     data/index     |
// |                    |
// +--------------------+ <--- BLOCK_FREE
// |     free space     |
// +--------------------+
// |       slots        |
// +--------------------+ <--- BLOCK_TRAILER
// |      trailer       |
// +--------------------+
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#ifndef __DB_BLOCK_H__
#define __DB_BLOCK_H__

#include "./checksum.h"
#include "./endian.h"
#include "./timestamp.h"
#include "./record.h"

namespace db {

const unsigned short BLOCK_TYPE_IDLE = 0;  // 空闲
const unsigned short BLOCK_TYPE_SUPER = 1; // 超块
const unsigned short BLOCK_TYPE_DATA = 2;  // 数据
const unsigned short BLOCK_TYPE_INDEX = 3; // 索引
const unsigned short BLOCK_TYPE_META = 4;  // 元数据
const unsigned short BLOCK_TYPE_LOG = 5;   // wal日志

const unsigned int SUPER_SIZE = 1024 * 4;  // 超块大小为4KB
const unsigned int BLOCK_SIZE = 1024 * 16; // 一般块大小为16KB

#if BYTE_ORDER == LITTLE_ENDIAN
static const int MAGIC_NUMBER = 0x31306264; // magic number
#else
static const int MAGIC_NUMBER = 0x64623031; // magic number
#endif

// 公共头部
struct CommonHeader
{
    unsigned int magic;       // magic number(4B)
    unsigned int spaceid;     // 表空间id(4B)
    unsigned short type;      // block类型(2B)
    unsigned short freespace; // 空闲记录链表(2B)
};

// 尾部
struct Trailer
{
    unsigned short slots[2]; // slots占位(4B)
    unsigned int checksum;   // 校验和(4B)
};

// 超块头部
struct SuperHeader : CommonHeader
{
    unsigned int first; // 第1个数据块(4B)
    TimeStamp stamp;    // 时戳(8B)
    unsigned int idle;  // 空闲块(4B)
    unsigned int pad;   // 填充位(4B)
};

// 空闲块头部
struct IdleHeader : CommonHeader
{
    unsigned int next; // 后继指针(4B)
};

// 数据块头部
struct DataHeader : CommonHeader
{
    unsigned short slots;    // slots[]长度(2B)
    unsigned short freesize; // 空闲空间大小(2B)
    TimeStamp stamp;         // 时戳(8B)
    unsigned int next;       // 下一个数据块(4B)
    unsigned int self;       // 本块id(4B)
};

// 元数据块头部
using MetaHeader = DataHeader;

////
// @brief
// 公共block
//
class Block
{
  protected:
    unsigned char *buffer_; // block对应的buffer

  public:
    Block()
        : buffer_(NULL)
    {}

    // 关联buffer
    inline void attach(unsigned char *buffer) { buffer_ = buffer; }

    // 设定magic
    inline void setMagic()
    {
        CommonHeader *header = reinterpret_cast<CommonHeader *>(buffer_);
        header->magic = MAGIC_NUMBER;
    }

    // 获取表空间id
    inline unsigned int getSpaceid()
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        return be32toh(header->spaceid);
    }
    // 设定表空间id
    inline void setSpaceid(unsigned int spaceid)
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        header->spaceid = htobe32(spaceid);
    }

    // 获取类型
    inline unsigned short getType()
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        return be16toh(header->type);
    }
    // 设定类型
    inline void setType(unsigned short type)
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        header->type = htobe16(type);
    }

    // 获取空闲链头
    inline unsigned short getFreeSpace()
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        return be16toh(header->freespace);
    }
};

////
// @brief
// 超块
//
class SuperBlock : public Block
{
  public:
    // 关联buffer
    inline void attach(unsigned char *buffer) { buffer_ = buffer; }
    // 清超块
    void clear(unsigned short spaceid);

    // 获取第1个数据块
    inline unsigned int getFirst()
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        return be32toh(header->first);
    }
    // 设定数据块链头
    inline void setFirst(unsigned int first)
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        header->first = htobe32(first);
    }

    // 获取空闲块
    inline unsigned int getIdle()
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        return be32toh(header->idle);
    }
    // 设定空闲块链头
    inline void setIdle(unsigned int idle)
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        header->idle = htobe32(idle);
    }

    // 获取时戳
    inline TimeStamp getTimeStamp()
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        TimeStamp ts;
        ::memcpy(&ts, &header->stamp, sizeof(TimeStamp));
        *((long long *) &ts) = be64toh(*((long long *) &ts));
        return ts;
    }
    // 设定时戳
    inline void setTimeStamp()
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        TimeStamp ts;
        ts.now();
        *((long long *) &ts) = htobe64(*((long long *) &ts));
        ::memcpy(&header->stamp, &ts, sizeof(TimeStamp));
    }

    // 设定checksum
    inline void setChecksum()
    {
        Trailer *trailer =
            reinterpret_cast<Trailer *>(buffer_ + SUPER_SIZE - sizeof(Trailer));
        trailer->checksum = 0;
        trailer->checksum = checksum32(buffer_, SUPER_SIZE);
    }
    // 获取checksum
    inline unsigned int getChecksum()
    {
        Trailer *trailer =
            reinterpret_cast<Trailer *>(buffer_ + SUPER_SIZE - sizeof(Trailer));
        return trailer->checksum;
    }
    // 检验checksum
    inline bool checksum()
    {
        unsigned int sum = 0;
        sum = checksum32(buffer_, SUPER_SIZE);
        return !sum;
    }
    // 设定空闲链头
    inline void setFreeSpace(unsigned short freespace)
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        header->freespace = htobe16(freespace);
    }
};

////
// @brief
// 数据块
//
class DataBlock : public Block
{
  public:
    // 清超块
    void clear(unsigned short spaceid, unsigned int self, unsigned short type);

    // 获取空闲块
    inline unsigned int getNext()
    {
        DataHeader *header = reinterpret_cast<DataHeader *>(buffer_);
        return be32toh(header->next);
    }
    // 设定block链头
    inline void setNext(unsigned int next)
    {
        DataHeader *header = reinterpret_cast<DataHeader *>(buffer_);
        header->next = htobe32(next);
    }

    // 获取时戳
    inline TimeStamp getTimeStamp()
    {
        DataHeader *header = reinterpret_cast<DataHeader *>(buffer_);
        TimeStamp ts;
        ::memcpy(&ts, &header->stamp, sizeof(TimeStamp));
        *((long long *) &ts) = be64toh(*((long long *) &ts));
        return ts;
    }
    // 设定时戳
    inline void setTimeStamp()
    {
        DataHeader *header = reinterpret_cast<DataHeader *>(buffer_);
        TimeStamp ts;
        ts.now();
        *((long long *) &ts) = htobe64(*((long long *) &ts));
        ::memcpy(&header->stamp, &ts, sizeof(TimeStamp));
    }

    // 获取空闲空间大小
    inline unsigned short getFreeSize()
    {
        DataHeader *header = reinterpret_cast<DataHeader *>(buffer_);
        return be16toh(header->freesize);
    }
    // 设定空闲空间大小
    inline void setFreeSize(unsigned short size)
    {
        DataHeader *header = reinterpret_cast<DataHeader *>(buffer_);
        header->freesize = htobe16(size);
    }

    // 设置slot存储的偏移量
    inline void setSlots(unsigned short slots)
    {
        DataHeader *header = reinterpret_cast<DataHeader *>(buffer_);
        header->slots = htobe16(slots);
    }
    // 获取slot存储的偏移量
    inline unsigned short getSlots()
    {
        DataHeader *header = reinterpret_cast<DataHeader *>(buffer_);
        return be16toh(header->slots);
    }

    // 设置self
    inline void setSelf(unsigned int id)
    {
        DataHeader *header = reinterpret_cast<DataHeader *>(buffer_);
        header->self = htobe32(id);
    }
    // 获取self
    inline unsigned int getSelf()
    {
        DataHeader *header = reinterpret_cast<DataHeader *>(buffer_);
        return be32toh(header->self);
    }

    // 设定checksum
    inline void setChecksum()
    {
        Trailer *trailer =
            reinterpret_cast<Trailer *>(buffer_ + BLOCK_SIZE - sizeof(Trailer));
        trailer->checksum = 0;
        trailer->checksum = checksum32(buffer_, BLOCK_SIZE);
    }
    // 获取checksum
    inline unsigned int getChecksum()
    {
        Trailer *trailer =
            reinterpret_cast<Trailer *>(buffer_ + BLOCK_SIZE - sizeof(Trailer));
        return trailer->checksum;
    }
    // 检验checksum
    inline bool checksum()
    {
        unsigned int sum = 0;
        sum = checksum32(buffer_, BLOCK_SIZE);
        return !sum;
    }

    // 获取trailer大小
    inline unsigned short getTrailerSize()
    {
        DataHeader *header = reinterpret_cast<DataHeader *>(buffer_);
        Trailer *trailer =
            reinterpret_cast<Trailer *>(buffer_ + BLOCK_SIZE - sizeof(Trailer));
        return (be16toh(header->slots) * sizeof(unsigned short) +
                sizeof(unsigned int) + 7) /
               8 * 8;
    }
    // 获取slots[]指针
    inline unsigned short *getSlotsPointer()
    {
        DataHeader *header = reinterpret_cast<DataHeader *>(buffer_);
        Trailer *trailer =
            reinterpret_cast<Trailer *>(buffer_ + BLOCK_SIZE - sizeof(Trailer));
        return reinterpret_cast<unsigned short *>(
                   buffer_ + BLOCK_SIZE - sizeof(unsigned int)) -
               be16toh(header->slots);
    }
    // 获取freespace空间大小
    inline unsigned short getFreespaceSize()
    {
        DataHeader *header = reinterpret_cast<DataHeader *>(buffer_);
        return BLOCK_SIZE - getTrailerSize() - be16toh(header->freespace);
    }
    // 设定空闲链头
    inline void setFreeSpace(unsigned short freespace)
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        // 判断是不是超过了Trailer的界限
        unsigned short upper = BLOCK_SIZE - getTrailerSize();
        if (freespace >= upper) freespace = 0; //超过界限则设置为0
        header->freespace = htobe16(freespace);
    }

    // 分配一个空间，直接返回指针。后续需要重新排列slots[]
    unsigned char *allocate(unsigned short space);
    // 回收一条记录，并且减少slots计数，但并未回收slots[]里的偏移量
    void deallocate(unsigned short offset);
    // 回收删除记录的资源
    void shrink();

    // 插入记录
    bool insertRecord(std::vector<struct iovec> &iov);
    // 修改记录
    bool updateRecord(std::vector<struct iovec> &iov);
    // 查询记录
    bool queryRecord(std::vector<struct iovec> &iov);
    // 枚举记录
};

using MetaBlock = DataBlock;

} // namespace db

#endif // __DB_BLOCK_H__
