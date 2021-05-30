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
#include <db/block.h>
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

    SECTION("bi")
    {
        Table table;
        int ret = table.open("table");
        REQUIRE(ret == S_OK);

        Table::BlockIterator bi = table.beginblock();
        REQUIRE(bi.block.table_ == &table);
        REQUIRE(bi.block.buffer_);

        unsigned int blockid = bi->getSelf();
        REQUIRE(blockid == 1);
        REQUIRE(blockid == bi.bufdesp->blockid);
        REQUIRE(bi.bufdesp->ref == 1);

        Table::BlockIterator bi1 = bi;
        REQUIRE(bi.bufdesp->ref == 2);
        bi.bufdesp->relref();

        ++bi;
        bool bret = bi == table.endblock();
        REQUIRE(bret);
    }

    SECTION("locate")
    {
        Table table;
        table.open("table");

        long long id = htobe64(5);
        int blkid = table.locate(&id, sizeof(id));
        REQUIRE(blkid == 1);
        id = htobe64(1);
        blkid = table.locate(&id, sizeof(id));
        REQUIRE(blkid == 1);
        id = htobe64(32);
        blkid = table.locate(&id, sizeof(id));
        REQUIRE(blkid == 1);
    }

    SECTION("insert")
    {
        Table table;
        table.open("table");
        DataType *type = table.info_->fields[table.info_->key].type;

        // 检查表记录
        long long records = table.recordCount();
        REQUIRE(records == 0);
        Table::BlockIterator bi = table.beginblock();
        REQUIRE(bi->getSlots() == 4); // 已插入4条记录，但表上没记录
        bi.release();
        // 修正表记录
        BufDesp *bd = kBuffer.borrow("table", 0);
        REQUIRE(bd);
        SuperBlock super;
        super.attach(bd->buffer);
        super.setRecords(4);

        // table = id(BIGINT)+phone(CHAR[20])+name(VARCHAR)
        // 准备添加
        std::vector<struct iovec> iov(3);
        long long nid;
        char phone[20];
        char addr[128];

        // 先填充
        int i, ret;
        for (i = 0; i < 91; ++i) {
            // 构造一个记录
            nid = rand();
            // printf("key=%lld\n", nid);
            type->htobe(&nid);
            iov[0].iov_base = &nid;
            iov[0].iov_len = 8;
            iov[1].iov_base = phone;
            iov[1].iov_len = 20;
            iov[2].iov_base = (void *) addr;
            iov[2].iov_len = 128;

            // locate位置
            unsigned int blkid =
                table.locate(iov[0].iov_base, (unsigned int) iov[0].iov_len);
            // 插入记录
            ret = table.insert(blkid, iov);
            if (ret == EEXIST) { printf("id=%lld exist\n", be64toh(nid)); }
            if (ret == EFAULT) break;
        }
        // 这里测试表明再插入到91条记录后出现分裂
        REQUIRE(i + 4 == table.recordCount());
    }

    SECTION("split")
    {
        Table table;
        table.open("table");
        DataType *type = table.info_->fields[table.info_->key].type;

        Table::BlockIterator bi = table.beginblock();

        unsigned short slot_count = bi->getSlots();
        size_t space = 162; // 前面的记录大小

        // 测试split，考虑插入位置在一半之前
        unsigned short index = (slot_count / 2 - 10); // 95/2-10
        std::pair<unsigned short, bool> ret = bi->splitPosition(space, index);
        REQUIRE(ret.first == 47); // 95/2=47，同时将新表项算在内
        REQUIRE(ret.second);

        // 在后半部分
        index = (slot_count / 2 + 10); // 95/2+10
        ret = bi->splitPosition(space, index);
        REQUIRE(ret.first == 48); // 95/2=47，同时将新表项算在内
        REQUIRE(!ret.second);

        // 在中间的位置上
        index = slot_count / 2; // 47
        ret = bi->splitPosition(space, index);
        REQUIRE(ret.first == 47); // 95/2=47，同时将新表项算在内
        REQUIRE(ret.second);

        // 在中间后一个位置上
        index = slot_count / 2 + 1; // 48
        ret = bi->splitPosition(space, index);
        REQUIRE(ret.first == 48); // 95/2=47，同时将新表项算在内
        REQUIRE(!ret.second);

        // 考虑space大小，超过一半
        space = BLOCK_SIZE / 2;
        index = (slot_count / 2 - 10); // 95/2-10
        ret = bi->splitPosition(space, index);
        REQUIRE(ret.first == index); // 未将新插入记录考虑在内
        REQUIRE(!ret.second);

        // space>1/2，位置在后部
        index = (slot_count / 2 + 10); // 95/2+10
        ret = bi->splitPosition(space, index);
        REQUIRE(ret.first == 48); // 95/2=47，同时将新表项算在内
        REQUIRE(!ret.second);

        // 测试说明，这个分裂策略可能使得前一半小于1/2
    }

    SECTION("allocate")
    {
        Table table;
        table.open("table");

        REQUIRE(table.maxid_ == 1);
        unsigned int blkid = table.allocate();
        REQUIRE(table.maxid_ == 2);

        Table::BlockIterator bi = table.beginblock();
        REQUIRE(bi.bufdesp->blockid == 1);
        ++bi;
        REQUIRE(bi == table.endblock()); // 新分配block未插入数据链
        REQUIRE(table.idle_ == 0);       // 也未放在空闲链上
        bi.release();

        // 回收该block
        table.deallocate(blkid);
        REQUIRE(table.idle_ == blkid);

        // 再从idle上分配
        blkid = table.allocate();
        REQUIRE(table.maxid_ == 2);
        REQUIRE(table.idle_ == 0);
        table.deallocate(blkid);
    }
}