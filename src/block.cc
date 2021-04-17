////
// @file block.cc
// @brief
// 实现block
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <algorithm>
#include <cmath>
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

void MetaBlock::clear(
    unsigned short spaceid,
    unsigned int self,
    unsigned short type)
{
    // 清buffer
    ::memset(buffer_, 0, BLOCK_SIZE);
    MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);

    // 设定magic
    header->magic = MAGIC_NUMBER;
    // 设定spaceid
    setSpaceid(spaceid);
    // 设定类型
    setType(type);
    // 设定空闲块
    setNext(0);
    // 设置本块id
    setSelf(self);
    // 设定时戳
    setTimeStamp();
    // 设定slots
    setSlots(0);
    // 设定freesize
    setFreeSize(BLOCK_SIZE - sizeof(MetaHeader) - sizeof(Trailer));
    // 设定freespace
    setFreeSpace(sizeof(MetaHeader));
    // 设定校验和
    setChecksum();
}

unsigned char *MetaBlock::allocate(unsigned short space)
{
    MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
    // TODO: 是否需要截断记录？
    if (be16toh(header->freesize) < space) return NULL;

    // 如果freespace空间不够，先回收删除的记录
    if (getFreespaceSize() < space) shrink();

    unsigned char *ret = buffer_ + be16toh(header->freespace);
    // 设定空闲空间大小
    unsigned short size = (space + 7) / 8 * 8; // 按照8字节对齐
    unsigned short fsize = be16toh(header->freesize) - size;
    setFreeSize(fsize);
    // 设定freespace偏移量
    setFreeSpace(be16toh(header->freespace) + size);

    // 写slots
    unsigned short slots = getSlots();
    setSlots(slots + 1); // 增加slots数目
    unsigned short *newslot = getSlotsPointer();
    *newslot = htobe16((unsigned short) (ret - buffer_));

    return ret;
}

void MetaBlock::deallocate(unsigned short offset)
{
    MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);

    // 设置tombstone
    Record record;
    unsigned char *space = buffer_ + offset;
    record.attach(space, 8); // 只使用8个字节
    record.die();
    // 先获取record的长度，TODO: record跨越block？？
    unsigned short length = (unsigned short) (record.length() + 7) / 8 * 8;
    // 修改freesize
    setFreeSize(getFreeSize() + length);

    // 写slots
    unsigned short slots = getSlots();
    setSlots(slots - 1); // 减少slots数目
}

void MetaBlock::shrink()
{
    MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
    unsigned short offset = sizeof(MetaHeader);
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

unsigned short DataBlock::searchRecord(void *buf, size_t len)
{
    DataHeader *header = reinterpret_cast<DataHeader *>(buffer_);

    // 获取key位置
    RelationInfo *info = getMeta();
    unsigned int key = info->key;

    // 调用数据类型的搜索
    return info->fields[key].type->search(buffer_, key, buf, len);
}

bool DataBlock::insertRecord(std::vector<struct iovec> &iov)
{
    RelationInfo *info = getMeta();
    unsigned int key = info->key;
    DataType *type = info->fields[key].type;

    // 如果block空间足够，插入
    size_t blen = getFreespaceSize();  // 该block的富余空间
    size_t length = Record::size(iov); // 记录的总长度
    if (blen >= length) {
        // 分配空间
        unsigned char *buf = allocate((unsigned short) length);
        // 填写记录
        Record record;
        record.attach(buf, (unsigned short) length);
        unsigned char header = 0;
        record.set(iov, &header);
        // 重新排序
        reorder(type, key);
        // 重设校验和
        setChecksum();
        return true;
    }

    // 找到插入的位置，计算前半部分的空间，看是否能够插入

    // 前半部分空间不够，在新block上插入

    // 挪动后半部分到新的block

    return true;
}

} // namespace db
