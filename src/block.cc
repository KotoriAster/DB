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

    // 设置tombstone
    Record record;
    unsigned char *space = buffer_ + offset;
    record.attach(space, 8); // 只使用8个字节
    record.die();
    // 先获取record的长度，TODO: record跨越block？？
    unsigned short length = (unsigned short) (record.length() + 7) / 8 * 8;
    // 修改freesize
    setFreeSize(getFreeSize() + length);
}

void DataBlock::shrink()
{
    DataHeader *header = reinterpret_cast<DataHeader *>(buffer_);
    unsigned short offset = sizeof(DataHeader);
    unsigned short end = getFreeSpace();
    unsigned char *last = buffer_ + offset; // 拷贝指针
    unsigned short *slots = getSlotsPointer();

    // 枚举所有record，然后向前移动
    int index = 0;
    while (offset < end) {
        Record record;
        record.attach(buffer_ + offset, 8);
        unsigned short length = (unsigned short) record.allocLength();
        if (record.isactive()) {
            if (last < buffer_ + offset)
                memcpy(last, buffer_ + offset, length); // 拷贝数据
            slots[index++] = offset;                    // 设定slots槽位
            last += length;                             // 移动last指针
        }
        offset += length;
    }

    // 设定freespace
    end = (unsigned short) (last - buffer_);
    setFreeSpace(end);
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
    // 设定freesize
    setFreeSize(BLOCK_SIZE - sizeof(DataHeader) - sizeof(Trailer));
    // 设定freespace
    setFreeSpace(sizeof(DataHeader));
    // 设定校验和
    setChecksum();
}

} // namespace db
