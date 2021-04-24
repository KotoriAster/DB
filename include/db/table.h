////
// @file table.h
// @brief
// 存储管理
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#ifndef __DB_TABLE_H__
#define __DB_TABLE_H__

#include <string>
#include <vector>
#include "./record.h"
#include "./schema.h"

namespace db {

////
// @brief
// 表操作接口
//
class Table
{
  public:
    // 表的迭代器
    struct Iterator
    {
        unsigned short blockid; // 当前blockid
        unsigned short index;   // slots的下标
    };

  public:
    std::string name_;   // 表名
    RelationInfo *info_; // 表的元数据
    unsigned int maxid_; // 最大的blockid
    unsigned int idle_;  // 空闲链
    unsigned int first_; // meta链

  public:
    Table()
        : info_(NULL)
        , maxid_(0)
        , idle_(0)
        , first_(0)
    {}

    // 打开一张表
    int open(const char *name);

    // 插入一条记录
    int insert(std::vector<struct iovec> &iov);
    int update();
    int remove();
    // begin, end

    // 新分配一个block，范围blockid
    unsigned int allocate();
    // 回收一个block
    void deallocate(unsigned int blockid);
    // 采用枚举的方式定位一个key在哪个block
    unsigned int locate(void *keybuf, unsigned int len);
};

} // namespace db

#endif // __DB_TABLE_H__
