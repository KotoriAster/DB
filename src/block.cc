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
    // 设定maxid
    setMaxid(0);
    // 设定self
    setSelf();
    // 设定空闲块，缺省从1开始
    setIdle(0);
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

// TODO: 如果record非full，直接分配，不考虑slot
unsigned char *MetaBlock::allocate(unsigned short space)
{
    MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
    space = ALIGN_TO_SIZE(space); // 先将需要空间数对齐8B

    // 计算需要分配的空间，需要考虑到分配Slot的问题
    unsigned short demand_space = space;
    unsigned short freesize = getFreeSize(); // block当前的剩余空间
    unsigned short current_trailersize = getTrailerSize();
    unsigned short demand_trailersize =
        (getSlots() + 1) * sizeof(Slot) + sizeof(int);
    if (current_trailersize < demand_trailersize)
        demand_space += ALIGN_TO_SIZE(sizeof(Slot)); // 需要的空间数目

    // 该block空间不够
    if (freesize < demand_space) return NULL;

    // 如果freespace空间不够，先回收删除的记录
    unsigned short freespacesize = getFreespaceSize();
    // freespace的空间要减去要分配的slot的空间
    if (current_trailersize < demand_trailersize)
        freespacesize -= ALIGN_TO_SIZE(sizeof(Slot));
    if (freespacesize < demand_space) shrink();

    // 从freespace分配空间
    unsigned char *ret = buffer_ + getFreeSpace();

    // 增加slots计数
    setSlots(getSlots() + 1);
    // 在slots[]顶部增加一个条目
    Slot *slot = getSlotsPointer();
    slot->offset = htobe16(getFreeSpace());
    slot->length = htobe16(space);

    // 设定空闲空间大小
    setFreeSize(getFreeSize() - demand_space);
    // 设定freespace偏移量
    setFreeSpace(getFreeSpace() + space);

    return ret;
}

// TODO: 需要考虑record非full的情况
void MetaBlock::deallocate(unsigned short index)
{
    MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);

    // 计算需要删除的记录的槽位
    Slot *pslot = reinterpret_cast<Slot *>(
        buffer_ + BLOCK_SIZE - sizeof(int) -
        sizeof(Slot) * (getSlots() - index));
    Slot slot;
    slot.offset = be16toh(pslot->offset);
    slot.length = be16toh(pslot->length);

    // 设置tombstone
    Record record;
    unsigned char *space = buffer_ + slot.offset;
    record.attach(space, 8); // 只使用8个字节
    record.die();

    // 挤压slots[]
    for (unsigned short i = index; i > 0; --i) {
        Slot *from = pslot;
        --from;
        pslot->offset = from->offset;
        pslot->length = from->length;
        pslot = from;
    }

    // 回收slots[]空间
    unsigned short previous_trailersize = getTrailerSize();
    setSlots(getSlots() - 1);
    unsigned short current_trailersize = getTrailerSize();
    // 要把slots[]回收的空间加回来
    if (previous_trailersize > current_trailersize)
        slot.length += previous_trailersize - current_trailersize;
    // 修改freesize
    setFreeSize(getFreeSize() + slot.length);
}

void MetaBlock::shrink()
{
    MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
    Slot *slots = getSlotsPointer();

    // 按照偏移量重新排序slots[]函数
    struct OffsetSort
    {
        bool operator()(const Slot &x, const Slot &y)
        {
            return be16toh(x.offset) < be16toh(y.offset);
        }
    };
    OffsetSort osort;
    std::sort(slots, slots + getSlots(), osort);

    // 枚举所有record，然后向前移动
    unsigned short offset = sizeof(MetaHeader);
    unsigned short space = 0;
    for (unsigned short i = 0; i < getSlots(); ++i) {
        unsigned short len = be16toh((slots + i)->length);
        unsigned short off = be16toh((slots + i)->offset);
        if (offset < off) memmove(buffer_ + offset, buffer_ + off, len);
        (slots + i)->offset = htobe16(offset);
        offset += len;
        space += len;
    }

    // 设定freespace
    setFreeSpace(offset);
    // 计算freesize
    setFreeSize(BLOCK_SIZE - sizeof(MetaHeader) - getTrailerSize() - space);
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

unsigned short DataBlock::splitPosition(size_t space, unsigned short index)
{
    // 先按照键排序slots[]
    DataHeader *header = reinterpret_cast<DataHeader *>(buffer_);
    RelationInfo *info = getMeta();
    unsigned int key = info->key;
    reorder(info->fields[key].type, key);

    // 枚举所有记录
    unsigned short count = getSlots();
    size_t half = 0;
    Slot *slots = getSlotsPointer();
    for (unsigned short i = 0; i < count; ++i) {
        // 如果是index，则将需要插入的记录空间算在内
        if (i == index) {
            half += space;
            if (half > ALIGN_TO_SIZE(
                           (BLOCK_SIZE - sizeof(DataHeader)) / 2 -
                           count * sizeof(Slot))) {
                // 超过一半
                return i;
            }
        }

        // fallthrough, i != index
        half += be16toh(slots[i].length);
        if (half >
            ALIGN_TO_SIZE(
                (BLOCK_SIZE - sizeof(DataHeader)) / 2 - count * sizeof(Slot))) {
            // 超过一半
            return i;
        }
    }
    return count;
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
        return true;
    }

    // 找到插入的位置，计算前半部分的空间，看是否能够插入
    unsigned short index =
        type->search(buffer_, key, iov[key].iov_base, iov[key].iov_len);
    unsigned short half = splitPosition(length, index);

    // 前半部分空间不够，在新block上插入

    // 挪动后半部分到新的block

    return true;
}

} // namespace db
