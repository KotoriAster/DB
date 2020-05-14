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

int Table::open(const char *name)
{
    // 查找schema

    // 找到后，加载meta信息
    return -1;
}

int Table::insert(struct iovec *record, size_t iovcnt)
{
    // 1. 打开文件
    // 2. 读入block
    return -1;
}

} // namespace db