////
// @file datatype.h
// @brief
// 定义数据类型
// https://www.w3school.com.cn/sql/sql_datatypes.asp
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#ifndef __DB_DATATYPE_H__
#define __DB_DATATYPE_H__

#include "./endian.h"

namespace db {

// sql数据类型
struct DataType
{
    using Sort = void (*)(unsigned char *block, unsigned int key);
    using Htobe = void (*)(void *);
    using Betoh = void (*)(void *);

    const char *name; // 名字
    ptrdiff_t size;   // >0表示固定，<0表示最大大小
    Sort sort;        // 排序函数
    Htobe htobe;      // 转化为大序
    Betoh betoh;      // 转化为主机字节序
};

// 根据数据类型名称数据类型，返回NULL表示失败
// CHAR VARCHAR TINYINT SMALLINT INT BIGINT
DataType *findDataType(const char *name);

} // namespace db

#endif // __DB_DATATYPE_H__
