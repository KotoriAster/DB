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

  private:
    const char *name_;   // 表名
    RelationInfo *info_; // 表的元数据

  public:
    Table()
        : name_(NULL)
        , info_(NULL)
    {}

    // 打开一张表
    int open(const char *name);

    // 插入一条记录
    int insert(std::vector<struct iovec> &iov);
    int update();
    int remove();
    // begin, end
};

} // namespace db

#endif // __DB_TABLE_H__
