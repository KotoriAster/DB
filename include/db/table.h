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

namespace db {

////
// @brief
// 表操作接口
//
class Table
{
  public:
    // 表的迭代器
    struct iterator;

  public:
    // 打开一张表
    int open(const char *name);

    // 插入一条记录
    int insert(struct iovec *record, size_t iovcnt);
    int update();
    int removet();
    // begin, end
};

} // namespace db

#endif // __DB_TABLE_H__
