////
// @file block.cc
// @brief
// 实现block
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <db/block.h>
#include <db/record.h>

namespace db {

void SuperBlock::clear(unsigned short spaceid)
{
    // 清buffer
    ::memset(buffer_, 0, SUPER_SIZE);
    SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);

    // 设置magic number
    header->magic = MAGIC_NUMBER;
    // 设定spaceid
    setSpaceid(spaceid);
    // 设定类型
    setType(BLOCK_TYPE_SUPER);
    // 设定时戳
    setTimeStamp();
    // 设定数据块
    setFirst(0);
    // 设定空闲块，缺省从1开始
    setIdle(1);
    // 设定空闲空间
    setFreeSpace(sizeof(SuperHeader));
    // 设置checksum
    setChecksum();
}

void MetaBlock::clear()
{
    // 清buffer
    ::memset(buffer_, 0, SUPER_SIZE);
    MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);

    // 设定magic
    header->magic = MAGIC_NUMBER;
    // 设定spaceid
    setSpaceid(0);
    // 设定类型
    setType(BLOCK_TYPE_META);
    // 设定freespace
    setFreeSpace(sizeof(MetaHeader));
    // 设定空闲块
    setNext(0);
    // 设定时戳
    setTimeStamp();
    // 设定tables
    setTables(0);
    // 设定校验和
    setChecksum();
}

unsigned char *DataBlock::allocate(unsigned short space)
{
    DataHeader *header = reinterpret_cast<DataHeader *>(buffer_);
    if (be16toh(header->freesize) < space) return NULL;

    if (getFreespaceSize() >= space) {
        unsigned char *ret = buffer_ + be16toh(header->freespace);
        // 设定空闲空间大小
        unsigned short size = (space + 7) / 8 * 8;
        unsigned short fsize = be16toh(header->freesize) - size;
        setFreeSize(fsize);
        // 设定freespace偏移量
        setFreeSpace(be16toh(header->freespace) + size);
        return ret;
    } else {
        // TODO: shrink
        return NULL;
    }
}

void DataBlock::deallocate(unsigned short offset)
{
    DataHeader *header = reinterpret_cast<DataHeader *>(buffer_);

    // 先获得Gc链表的头部
    unsigned short next = getGc();
    // 将释放的空间加到Gc的头部
    setGc(offset);

    // 修改freesize
    Record record;
    struct iovec iov[2];
    unsigned char *space = buffer_ + offset;
    record.attach(space, 8); // 只使用8个字节
    // 先获取record的长度，TODO: record跨越block？？
    unsigned short length = (unsigned short) (record.length() + 7) / 8 * 8;
    setFreeSize(getFreeSize() + length);

    // 设定记录指向后一个
    next = htobe16(next);
    iov[0].iov_base = (void *) &next;
    iov[0].iov_len = sizeof(unsigned short);
    length = htobe16(length);
    iov[1].iov_base = (void *) &length;
    iov[1].iov_len = sizeof(unsigned short);
    unsigned char h = 0;
    record.set(iov, 2, &h);
}

void DataBlock::shrinkGc()
{
    DataHeader *header = reinterpret_cast<DataHeader *>(buffer_);
}

void DataBlock::clear(unsigned short spaceid)
{
    // 清buffer
    ::memset(buffer_, 0, SUPER_SIZE);
    DataHeader *header = reinterpret_cast<DataHeader *>(buffer_);

    // 设定magic
    header->magic = MAGIC_NUMBER;
    // 设定spaceid
    setSpaceid(spaceid);
    // 设定类型
    setType(BLOCK_TYPE_DATA);
    // 设定空闲块
    setNext(0);
    // 设定时戳
    setTimeStamp();
    // 设定slots
    setSlots(0);
    // 设定Gc
    setGc(0);
    // 设定freesize
    setFreeSize(BLOCK_SIZE - sizeof(DataHeader) - sizeof(Trailer));
    // 设定freespace
    setFreeSpace(sizeof(DataHeader));
    // 设定校验和
    setChecksum();
}

} // namespace db
