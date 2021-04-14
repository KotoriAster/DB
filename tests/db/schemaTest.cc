////
// @file schemaTest.cc
// @brief
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include "../catch.hpp"
#include <db/schema.h>
using namespace db;

TEST_CASE("db/schema.h")
{
    SECTION("relationinfo")
    {
        // 填充关系
        RelationInfo relation;
        relation.path = "table.dat";

        // id char(20) varchar
        FieldInfo field;
        field.name = "id";
        field.index = 0;
        field.length = 8;
        relation.fields.push_back(field);

        field.name = "phone";
        field.index = 1;
        field.length = 20;
        relation.fields.push_back(field);

        field.name = "name";
        field.index = 2;
        field.length = -255;
        relation.fields.push_back(field);

        relation.count = 3;
        relation.key = 0;

        int total = relation.iovSize();
        REQUIRE(total == 16);

        Schema schema;
        std::vector<struct iovec> iov(total);
        schema.initIov("table", relation, iov);

        REQUIRE(iov[0].iov_len == strlen("table") + 1);
        REQUIRE(
            strncmp((const char *) iov[0].iov_base, "table", iov[0].iov_len) ==
            0);

        REQUIRE(iov[1].iov_len == strlen("table.dat") + 1);
        REQUIRE(
            strncmp(
                (const char *) iov[1].iov_base, "table.dat", iov[1].iov_len) ==
            0);

        REQUIRE(iov[2].iov_len == 2);
        unsigned short count = be16toh(*((unsigned short *) iov[2].iov_base));
        REQUIRE(count == 3);

        REQUIRE(iov[3].iov_len == 2);
        unsigned short type = be16toh(*((unsigned short *) iov[3].iov_base));
        REQUIRE(type == 0);

        REQUIRE(iov[4].iov_len == 4);
        unsigned int key = be32toh(*((unsigned int *) iov[4].iov_base));
        REQUIRE(key == 0);
    }

    SECTION("open")
    {
        Schema schema;
        int ret = schema.open();
        REQUIRE(ret == S_OK);

        // 填充关系
        RelationInfo relation;
        relation.path = "table.dat";

        // id char(20) varchar
        FieldInfo field;
        field.name = "id";
        field.index = 0;
        field.length = 8;
        relation.fields.push_back(field);

        field.name = "phone";
        field.index = 1;
        field.length = 20;
        relation.fields.push_back(field);

        field.name = "name";
        field.index = 2;
        field.length = -255;
        relation.fields.push_back(field);

        relation.count = 3;
        relation.key = 0;

        ret = schema.create("table", relation);
        REQUIRE(ret == S_OK);
    }

    SECTION("load")
    {
        Schema schema;
        int ret = schema.open();
        REQUIRE(ret == S_OK);
        std::pair<Schema::TableSpace::iterator, bool> bret =
            schema.lookup("table");
        REQUIRE(bret.second);

        ret = schema.load(bret.first);
        REQUIRE(ret == S_OK);

        // 删除表，删除元文件
        Schema::TableSpace::iterator it = bret.first;
        it->second.file.close();
        REQUIRE(it->second.file.remove("table.dat") == S_OK);
        REQUIRE(schema.destroy() == S_OK);
    }
}