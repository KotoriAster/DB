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
static const int MAGIC_NUMBER = 0x1ef0c6c1; // magic number
#else
static const int MAGIC_NUMBER = 0xc1c6f01e; // magic number
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
    unsigned int pad;        // 占位(4B)
};

// 元数据块头部
struct MetaHeader : CommonHeader
{
    unsigned int next;   // 空闲块
    TimeStamp stamp;     // 时戳
    unsigned int tables; // 表个数
};

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
    // 设定空闲链头
    inline void setFreeSpace(unsigned short freespace)
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        header->freespace = htobe16(freespace);
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
};

// TODO: slots???
////
// @brief
// 元数据块
//
class MetaBlock : public Block
{
  public:
    // 清超块
    void clear();

    // 获取空闲块
    inline unsigned int getNext()
    {
        MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
        return be32toh(header->next);
    }
    // 设定block链头
    inline void setNext(unsigned int next)
    {
        MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
        header->next = htobe32(next);
    }

    // 获取时戳
    inline TimeStamp getTimeStamp()
    {
        MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
        TimeStamp ts;
        ::memcpy(&ts, &header->stamp, sizeof(TimeStamp));
        *((long long *) &ts) = be64toh(*((long long *) &ts));
        return ts;
    }
    // 设定时戳
    inline void setTimeStamp()
    {
        MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
        TimeStamp ts;
        ts.now();
        *((long long *) &ts) = htobe64(*((long long *) &ts));
        ::memcpy(&header->stamp, &ts, sizeof(TimeStamp));
    }

    // 获取tables
    inline unsigned int getTables()
    {
        MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
        return be32toh(header->tables);
    }
    // 设定tables
    inline void setTables(unsigned int tables)
    {
        MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
        header->tables = htobe32(tables);
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
};

////
// @brief
// 数据块
//
class DataBlock : public Block
{
  public:
    // 清超块
    void clear(unsigned short spaceid);

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

    // TODO: allocate slots[]
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

    // 分配一个空间，直接返回指针
    unsigned char *allocate(unsigned short space);
    // 回收一个空间
    void deallocate(unsigned short offset);
    // 回收tomestone资源
    void shrink();

    // 插入记录
    bool insertRecord(struct iovec *iov, int iovcnt);
};

#if 0
////

// 通用block头部
class Block
{
  public:
    // 公共头部字段偏移量
    static const int BLOCK_SIZE = 1024 * 16; // block大小为16KB

#    if BYTE_ORDER == LITTLE_ENDIAN
    static const int BLOCK_MAGIC_NUMBER = 0x1ef0c6c1; // magic number
#    else
    static const int BLOCK_MAGIC_NUMBER = 0xc1c6f01e; // magic number
#    endif
    static const int BLOCK_MAGIC_OFFSET = 0; // magic number偏移量
    static const int BLOCK_MAGIC_SIZE = 4;   // magic number大小4B

    static const int BLOCK_SPACEID_OFFSET =
        BLOCK_MAGIC_OFFSET + BLOCK_MAGIC_SIZE; // 表空间id偏移量
    static const int BLOCK_SPACEID_SIZE = 4;   // 表空间id长度4B

    static const int BLOCK_NUMBER_OFFSET =
        BLOCK_SPACEID_OFFSET + BLOCK_SPACEID_SIZE; // blockid偏移量
    static const int BLOCK_NUMBER_SIZE = 4;        // blockid大小4B

    static const int BLOCK_NEXTID_OFFSET =
        BLOCK_NUMBER_OFFSET + BLOCK_NUMBER_SIZE; // 下一个blockid偏移量
    static const int BLOCK_NEXTID_SIZE = 4;      // 下一个blockid大小4B

    static const int BLOCK_TYPE_OFFSET =
        BLOCK_NEXTID_OFFSET + BLOCK_NEXTID_SIZE; // block类型偏移量
    static const int BLOCK_TYPE_SIZE = 2;        // block类型大小2B

    static const int BLOCK_SLOTS_OFFSET =
        BLOCK_TYPE_OFFSET + BLOCK_TYPE_SIZE; // slots[]数目偏移量
    static const int BLOCK_SLOTS_SIZE = 2;   // slosts[]数目大小2B

    static const int BLOCK_GARBAGE_OFFSET =
        BLOCK_SLOTS_OFFSET + BLOCK_SLOTS_SIZE; // 空闲链表偏移量
    static const int BLOCK_GARBAGE_SIZE = 2;   // 空闲链表大小2B

    static const int BLOCK_FREESPACE_OFFSET =
        BLOCK_GARBAGE_OFFSET + BLOCK_GARBAGE_SIZE; // 空闲空间偏移量
    static const int BLOCK_FREESPACE_SIZE = 2;     // 空闲空间大小2B

    static const int BLOCK_CHECKSUM_SIZE = 4; // checksum大小4B
    static const int BLOCK_CHECKSUM_OFFSET =
        BLOCK_SIZE - BLOCK_CHECKSUM_SIZE; // trailer偏移量

    static const short BLOCK_DEFAULT_FREESPACE =
        BLOCK_FREESPACE_OFFSET + BLOCK_FREESPACE_SIZE; // 空闲空间缺省偏移量
    static const int BLOCK_DEFAULT_CHECKSUM = 0xc70f393e;

  protected:
    unsigned char *buffer_; // block对应的buffer

  public:
    Block()
        : buffer_(NULL)
    {}
    Block(unsigned char *b)
        : buffer_(b)
    {}

    // 关联buffer
    inline void attach(unsigned char *buffer) { buffer_ = buffer; }
    // 清buffer
    void clear(int spaceid, int blockid);

    // 获取spaceid
    inline int spaceid()
    {
        int id;
        ::memcpy(&id, buffer_ + BLOCK_SPACEID_OFFSET, BLOCK_SPACEID_SIZE);
        return be32toh(id);
    }
    // 获取blockid
    inline int blockid()
    {
        int id;
        ::memcpy(&id, buffer_ + BLOCK_NUMBER_OFFSET, BLOCK_NUMBER_SIZE);
        return be32toh(id);
    }

    // 设置garbage
    inline void setGarbage(unsigned short garbage)
    {
        garbage = htobe16(garbage);
        ::memcpy(buffer_ + BLOCK_GARBAGE_OFFSET, &garbage, BLOCK_GARBAGE_SIZE);
    }
    // 获得garbage
    inline unsigned short getGarbage()
    {
        short garbage;
        ::memcpy(&garbage, buffer_ + BLOCK_GARBAGE_OFFSET, BLOCK_GARBAGE_SIZE);
        return be16toh(garbage);
    }

    // 设定nextid
    inline void setNextid(int id)
    {
        id = htobe32(id);
        ::memcpy(buffer_ + BLOCK_NEXTID_OFFSET, &id, BLOCK_NEXTID_SIZE);
    }
    // 获取nextid
    inline int getNextid()
    {
        int id;
        ::memcpy(&id, buffer_ + BLOCK_NEXTID_OFFSET, BLOCK_NEXTID_SIZE);
        return be32toh(id);
    }

    // 设定checksum
    inline void setChecksum()
    {
        unsigned int check = 0;
        ::memset(buffer_ + BLOCK_CHECKSUM_OFFSET, 0, BLOCK_CHECKSUM_SIZE);
        check = checksum32(buffer_, BLOCK_SIZE);
        ::memcpy(buffer_ + BLOCK_CHECKSUM_OFFSET, &check, BLOCK_CHECKSUM_SIZE);
    }
    // 获取checksum
    inline unsigned int getChecksum()
    {
        unsigned int check = 0;
        ::memcpy(&check, buffer_ + BLOCK_CHECKSUM_OFFSET, BLOCK_CHECKSUM_SIZE);
        return check;
    }
    // 检验checksum
    inline bool checksum()
    {
        unsigned int sum = 0;
        sum = checksum32(buffer_, BLOCK_SIZE);
        return !sum;
    }

    // 获取类型
    inline unsigned short getType()
    {
        unsigned short type;
        ::memcpy(&type, buffer_ + BLOCK_TYPE_OFFSET, BLOCK_TYPE_SIZE);
        return be16toh(type);
    }
    // 设定类型
    inline void setType(unsigned short type)
    {
        type = htobe16(type);
        ::memcpy(buffer_ + BLOCK_TYPE_OFFSET, &type, BLOCK_TYPE_SIZE);
    }

    // 设定slots[]数目
    inline void setSlotsNum(unsigned short count)
    {
        count = htobe16(count);
        ::memcpy(buffer_ + BLOCK_SLOTS_OFFSET, &count, BLOCK_SLOTS_SIZE);
    }
    // 获取slots[]数目
    inline unsigned short getSlotsNum()
    {
        unsigned short count;
        ::memcpy(&count, buffer_ + BLOCK_SLOTS_OFFSET, BLOCK_SLOTS_SIZE);
        return be16toh(count);
    }

    // 设定freespace偏移量
    inline void setFreespace(unsigned short freespace)
    {
        freespace = htobe16(freespace);
        ::memcpy(
            buffer_ + BLOCK_FREESPACE_OFFSET, &freespace, BLOCK_FREESPACE_SIZE);
    }
    // 获得freespace偏移量
    inline unsigned short getFreespace()
    {
        unsigned short freespace;
        ::memcpy(
            &freespace, buffer_ + BLOCK_FREESPACE_OFFSET, BLOCK_FREESPACE_SIZE);
        return be16toh(freespace);
    }
    // 获取freespace大小
    inline unsigned short getFreeLength()
    {
        unsigned short slots = getSlotsNum();
        unsigned short offset = getFreespace();
        unsigned short slots2 = // slots[]起始位置
            BLOCK_SIZE - slots * sizeof(unsigned short) - BLOCK_CHECKSUM_SIZE;
        if (offset >= slots2)
            return 0;
        else
            return BLOCK_SIZE - slots * sizeof(short) - BLOCK_CHECKSUM_SIZE -
                   offset;
    }

    // 设置slot存储的偏移量，从下向上开始，0....
    inline void setSlot(unsigned short index, unsigned short off)
    {
        unsigned short offset = BLOCK_SIZE - BLOCK_CHECKSUM_SIZE -
                                (index + 1) * sizeof(unsigned short);
        *((unsigned short *) (buffer_ + offset)) = off;
    }
    // 获取slot存储的偏移量
    inline unsigned short getSlot(unsigned short index)
    {
        unsigned short offset = BLOCK_SIZE - BLOCK_CHECKSUM_SIZE -
                                (index + 1) * sizeof(unsigned short);
        return *((unsigned short *) (buffer_ + offset));
    }

    // 分配记录及slots，返回false表示失败
    bool allocate(const unsigned char *header, struct iovec *iov, int iovcnt);
};

#endif

#if 0
// 数据block
class DataBlock : public Block
{
  public:
    // 数据block各字段偏移量
    static const int BLOCK_DATA_ROWS = BLOCK_NEXT + 8; // 记录个数，2B
    static const int BLOCK_DATA_PADDING =
        BLOCK_DATA_ROWS + 2; // data头部填充，2B
                             // TODO: 指向btree内部节点？
    static const int BLOCK_DATA_START = BLOCK_DATA_PADDING + 2; // 记录开始位置

  public:
    // 各字段位置
    inline unsigned short *rows()
    {
        return (unsigned short *) (this->buffer_ + BLOCK_DATA_ROWS);
    }
    inline char *data() { return (char *) (this->buffer_ + BLOCK_DATA_START); }
    inline unsigned short *slots()
    {
        return (
            unsigned short *) (this->buffer_ + BLOCK_CHECKSUM - 2 * (*rows()));
    }
    // 空闲空间大小
    inline int freesize()
    {
        size_t size = (char *) slots() - data();
        return size / 4 * 4; // slots按2B对齐，可能需要在slots前填充2B
    }
};
#endif

} // namespace db

#endif // __DB_BLOCK_H__
