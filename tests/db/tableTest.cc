////
// @file tableTest.cc
// @brief
// 测试存储管理
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include "../catch.hpp"
#include <db/table.h>
#include <db/buffer.h>
using namespace db;

TEST_CASE("db/table.h")
{
    SECTION("less")
    {
        Buffer::BlockMap::key_compare compare;
        const char *table = "hello";
        std::pair<const char *, unsigned int> key1(table, 1);
        const char *table2 = "hello";
        std::pair<const char *, unsigned int> key2(table2, 1);
        bool ret = !compare(key1, key2);
        REQUIRE(ret);
        ret = !compare(key2, key1);
        REQUIRE(ret);
    }

    SECTION("open")
    {
        // NOTE: schemaTest.cc中创建
        Table table;
        int ret = table.open("table");
        REQUIRE(ret == S_OK);
        REQUIRE(table.name_ == "table");
        REQUIRE(table.maxid_ == 1);
        REQUIRE(table.idle_ == 0);
        REQUIRE(table.first_ == 1);
        REQUIRE(table.info_->key == 0);
        REQUIRE(table.info_->count == 3);
    }
}