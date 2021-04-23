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
    // 查找table
    std::pair<Schema::TableSpace::iterator, bool> bret = kSchema.lookup(name);
    if (!bret.second) return EEXIST; // 表不存在

    // 加载元数据
    name_ = bret.first->first.c_str();
    info_ = &bret.first->second;
    return S_OK;
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