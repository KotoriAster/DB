////
// @file table.cc
// @brief
// 实现存储管理
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <db/block.h>
#include <db/table.h>
#include <db/buffer.h>

namespace db {

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

#if 0
int Table::insert(std::vector<struct iovec> record)
{
    // 1. 打开文件
    // 2. 读入block
    return -1;
}
#endif

} // namespace db