////
// @file table.cc
// @brief
// 实现存储管理
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <db/table.h>

namespace db {

Table::BlockIterator::BlockIterator()
    : bufdesp(nullptr)
{}
Table::BlockIterator::~BlockIterator()
{
    if (bufdesp) kBuffer.releaseBuf(bufdesp);
}
Table::BlockIterator::BlockIterator(const BlockIterator &other)
    : block(other.block)
    , bufdesp(other.bufdesp)
{
    if (bufdesp) bufdesp->addref();
}

// 前置操作
Table::BlockIterator &Table::BlockIterator::operator++()
{
    if (block.buffer_ == nullptr) return *this;
    unsigned int blockid = block.getNext();
    kBuffer.releaseBuf(bufdesp);
    if (blockid)
        bufdesp = kBuffer.borrow(block.table_->name_.c_str(), blockid);
    else
        block.buffer_ = nullptr;
    return *this;
}
// 后置操作
Table::BlockIterator Table::BlockIterator::operator++(int)
{
    BlockIterator tmp(*this);
    if (block.buffer_ == nullptr) return *this;
    unsigned int blockid = block.getNext();
    kBuffer.releaseBuf(bufdesp);
    if (blockid)
        bufdesp = kBuffer.borrow(block.table_->name_.c_str(), blockid);
    else
        block.buffer_ = nullptr;
    return tmp;
}
// 数据块指针
DataBlock *Table::BlockIterator::operator->() { return &block; }
void Table::BlockIterator::release()
{
    bufdesp->relref();
    block.detach();
}

int Table::open(const char *name)
{
    // 查找table
    std::pair<Schema::TableSpace::iterator, bool> bret = kSchema.lookup(name);
    if (!bret.second) return EEXIST; // 表不存在

    // 填充结构
    name_ = name;
    info_ = &bret.first->second;

    // 加载超块
    SuperBlock super;
    BufDesp *desp = kBuffer.borrow(name, 0);
    super.attach(desp->buffer);

    // 获取元数据
    maxid_ = super.getMaxid();
    idle_ = super.getIdle();
    first_ = super.getFirst();

    // 释放超块
    super.detach();
    desp->relref();
    return S_OK;
}

unsigned int Table::allocate()
{
    // 空闲链上有block
    DataBlock data;
    SuperBlock super;
    if (idle_) {
        // 读idle块，获得下一个空闲块
        BufDesp *desp = kBuffer.borrow(name_.c_str(), idle_);
        data.attach(desp->buffer);
        unsigned int next = data.getNext();
        data.detach();
        desp->relref();

        // 读超块，设定空闲块
        desp = kBuffer.borrow(name_.c_str(), 0);
        super.attach(desp->buffer);
        super.setIdle(next);
        super.setChecksum();
        super.detach();
        kBuffer.writeBuf(desp);
        desp->relref();

        unsigned int current = next;
        idle_ = next;
        return current;
    }

    // 没有空闲块
    ++maxid_;
    // 读超块，设定空闲块
    BufDesp *desp = kBuffer.borrow(name_.c_str(), 0);
    super.attach(desp->buffer);
    super.setMaxid(maxid_);
    super.setChecksum();
    super.detach();
    kBuffer.writeBuf(desp);
    desp->relref();

    return maxid_;
}

void Table::deallocate(unsigned int blockid)
{
    // 读idle块，获得下一个空闲块
    DataBlock data;
    BufDesp *desp = kBuffer.borrow(name_.c_str(), blockid);
    data.attach(desp->buffer);
    data.setNext(idle_);
    data.setChecksum();
    data.detach();
    kBuffer.writeBuf(desp);
    desp->relref();

    // 读超块，设定空闲块
    SuperBlock super;
    desp = kBuffer.borrow(name_.c_str(), 0);
    super.attach(desp->buffer);
    super.setIdle(blockid);
    super.setChecksum();
    super.detach();
    kBuffer.writeBuf(desp);
    desp->relref();

    // 设定自己
    idle_ = blockid;
}

Table::BlockIterator Table::beginblock()
{
    // 通过超块找到第1个数据块的id
    BlockIterator bi;
    bi.block.table_ = this;

    // 获取第1个blockid
    BufDesp *bd = kBuffer.borrow(name_.c_str(), 0);
    SuperBlock super;
    super.attach(bd->buffer);
    unsigned int blockid = super.getFirst();
    kBuffer.releaseBuf(bd);

    bi.bufdesp = kBuffer.borrow(name_.c_str(), blockid);
    bi.block.attach(bi.bufdesp->buffer);
    return bi;
}

Table::BlockIterator Table::endblock()
{
    BlockIterator bi;
    bi.block.table_ = this;
    return bi;
}

unsigned int Table::locate(void *keybuf, unsigned int len)
{
    unsigned int key = info_->key;
    DataType *type = info_->fields[key].type;

    BlockIterator prev = beginblock();
    for (BlockIterator bi = beginblock(); bi != endblock(); ++bi) {
        // 获取第1个记录
        std::pair<unsigned char *, unsigned short> ret = bi->refslots(0);
        Record record;
        record.attach(ret.first, ret.second);

        // 与参数比较
        unsigned char *pkey;
        unsigned int klen;
        record.refByIndex(&pkey, &klen, key);
        bool bret = type->less(pkey, klen, (unsigned char *) keybuf, len);
        if (bret)
            prev = bi;
        else
            return prev->getSelf();
    }
    return prev->getSelf();
}

int Table::insert(unsigned int blkid, std::vector<struct iovec> &iov)
{
    DataBlock data;
    data.setTable(this);

    // 从buffer中借用
    BufDesp *bd = kBuffer.borrow(name_.c_str(), blkid);
    data.attach(bd->buffer);
    // 尝试插入
    std::pair<bool, unsigned short> ret = data.insertRecord(iov);
    if (ret.first) {
        kBuffer.releaseBuf(bd); // 释放buffer
        // 修改表头统计
        bd = kBuffer.borrow(name_.c_str(), 0);
        SuperBlock super;
        super.attach(bd->buffer);
        super.setRecords(super.getRecords() + 1);
        bd->relref();
        return S_OK; // 插入成功
    } else if (ret.second == (unsigned short) -1) {
        kBuffer.releaseBuf(bd); // 释放buffer
        return EEXIST;          // key已经存在
    }

    // 分裂block, TODO:
    unsigned short insert_position = ret.second;
    std::pair<unsigned short, bool> split_position =
        data.splitPosition(Record::size(iov), insert_position);
    // 先分配一个block
    DataBlock next;
    next.setTable(this);
    blkid = allocate();
    BufDesp *bd2 = kBuffer.borrow(name_.c_str(), blkid);
    next.attach(bd2->buffer);
    // 移动记录到新的block上

    return EFAULT;
}

size_t Table::recordCount()
{
    BufDesp *bd = kBuffer.borrow(name_.c_str(), 0);
    SuperBlock super;
    super.attach(bd->buffer);
    size_t count = super.getRecords();
    kBuffer.releaseBuf(bd);
    return count;
}

} // namespace db