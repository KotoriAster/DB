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

namespace {
void dump(Table &table)
{
    // 打印所有记录，检查是否正确
    int rcount = 0;
    int bcount = 0;
    for (Table::BlockIterator bi = table.beginblock(); bi != table.endblock();
         ++bi, ++bcount) {
        for (unsigned short i = 0; i < bi->getSlots(); ++i, ++rcount) {
            Slot *slot = bi->getSlotsPointer() + i;
            Record record;
            record.attach(
                bi->buffer_ + be16toh(slot->offset), be16toh(slot->length));

            unsigned char *pkey;
            unsigned int len;
            long long key;
            record.refByIndex(&pkey, &len, 0);
            memcpy(&key, pkey, len);
            key = be64toh(key);
            printf(
                "key=%lld, offset=%d, rcount=%d, blkid=%d\n",
                key,
                be16toh(slot->offset),
                rcount,
                bi->getSelf());
        }
    }
    printf("total records=%zd\n", table.recordCount());
}
bool check(Table &table)
{
    int rcount = 0;
    int bcount = 0;
    long long okey = 0;
    for (Table::BlockIterator bi = table.beginblock(); bi != table.endblock();
         ++bi, ++bcount) {
        for (DataBlock::RecordIterator ri = bi->beginrecord();
             ri != bi->endrecord();
             ++ri, ++rcount) {
            unsigned char *pkey;
            unsigned int len;
            long long key;
            ri->refByIndex(&pkey, &len, 0);
            memcpy(&key, pkey, len);
            key = be64toh(key);
            if (okey >= key) {
                dump(table);
                printf("check error %d\n", rcount);
                return true;
            }
            okey = key;
        }
    }
    return false;
}
} // namespace

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

    // 插满一个block
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
        super.setDataCounts(1);
        REQUIRE(!check(table));

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
        REQUIRE(!check(table));
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

        REQUIRE(table.dataCount() == 1);
        REQUIRE(table.idleCount() == 0);

        REQUIRE(table.maxid_ == 1);
        unsigned int blkid = table.allocate();
        REQUIRE(table.maxid_ == 2);
        REQUIRE(table.dataCount() == 2);

        Table::BlockIterator bi = table.beginblock();
        REQUIRE(bi.bufdesp->blockid == 1);
        ++bi;
        REQUIRE(bi == table.endblock()); // 新分配block未插入数据链
        REQUIRE(table.idle_ == 0);       // 也未放在空闲链上
        bi.release();

        // 回收该block
        table.deallocate(blkid);
        REQUIRE(table.idleCount() == 1);
        REQUIRE(table.dataCount() == 1);
        REQUIRE(table.idle_ == blkid);

        // 再从idle上分配
        blkid = table.allocate();
        REQUIRE(table.idleCount() == 0);
        REQUIRE(table.maxid_ == 2);
        REQUIRE(table.idle_ == 0);
        table.deallocate(blkid);
        REQUIRE(table.idleCount() == 1);
        REQUIRE(table.dataCount() == 1);
    }

    SECTION("insert2")
    {
        Table table;
        table.open("table");
        DataType *type = table.info_->fields[table.info_->key].type;

        // 准备添加
        std::vector<struct iovec> iov(3);
        long long nid;
        char phone[20];
        char addr[128];

        // 构造一个记录
        nid = rand();
        type->htobe(&nid);
        iov[0].iov_base = &nid;
        iov[0].iov_len = 8;
        iov[1].iov_base = phone;
        iov[1].iov_len = 20;
        iov[2].iov_base = (void *) addr;
        iov[2].iov_len = 128;

        int ret = table.insert(1, iov);
        REQUIRE(ret == S_OK);

        Table::BlockIterator bi = table.beginblock();
        REQUIRE(bi.bufdesp->blockid == 1);
        REQUIRE(bi->getSelf() == 1);
        REQUIRE(bi->getNext() == 2);
        unsigned short count1 = bi->getSlots();
        ++bi;
        REQUIRE(bi->getSelf() == 2);
        REQUIRE(bi->getNext() == 0);
        unsigned short count2 = bi->getSlots();
        REQUIRE(count1 + count2 == 96);
        REQUIRE(count1 + count2 == table.recordCount());
        REQUIRE(!check(table));

        // dump(table);
        // bi = table.beginblock();
        // bi->shrink();
        // dump(table);
        // bi->reorder(type, 0);
        // dump(table);
        // REQUIRE(!check(table));
    }

    // 再插入10000条记录
    SECTION("insert3")
    {
        Table table;
        table.open("table");
        DataType *type = table.info_->fields[table.info_->key].type;

        // 准备添加
        std::vector<struct iovec> iov(3);
        long long nid;
        char phone[20];
        char addr[128];

        iov[0].iov_base = &nid;
        iov[0].iov_len = 8;
        iov[1].iov_base = phone;
        iov[1].iov_len = 20;
        iov[2].iov_base = (void *) addr;
        iov[2].iov_len = 128;

        int count = 96;
        int count2 = 0;
        for (int i = 0; i < 10000; ++i) {
            nid = rand();
            type->htobe(&nid);
            // locate位置
            unsigned int blkid =
                table.locate(iov[0].iov_base, (unsigned int) iov[0].iov_len);
            // 插入记录
            int ret = table.insert(blkid, iov);
            if (ret == S_OK) ++count;
        }

        for (Table::BlockIterator bi = table.beginblock();
             bi != table.endblock();
             ++bi)
            count2 += bi->getSlots();
        REQUIRE(count == count2);
        REQUIRE(count == table.recordCount());
        REQUIRE(table.idleCount() == 0);

        REQUIRE(!check(table));
    }
    SECTION("remove")
    {
        Table table;
        table.open("table");
        DataType *type = table.info_->fields[table.info_->key].type;

        // 准备添加
        std::vector<struct iovec> iov(3);
        long long nid;
        char phone[20];
        char addr[128];

        iov[0].iov_base = &nid;
        iov[0].iov_len = 8;
        iov[1].iov_base = phone;
        iov[1].iov_len = 20;
        iov[2].iov_base = (void *) addr;
        iov[2].iov_len = 128;

        //保证第一个block里保存的key取值在0~47之间且连续，便于后续测试
        //确保表中存在key=48的记录，便于后续测试
        for (long long i = 0; i <= 48; ++i) {
            nid = i;
            type->htobe(&nid);
            // locate位置
            unsigned int blkid =
                table.locate(iov[0].iov_base, (unsigned int) iov[0].iov_len);
            REQUIRE(blkid == 1);
            // 插入记录
            table.insert(blkid, iov);
        }

        unsigned int totalRecord = unsigned int(table.recordCount()); //总记录条数
        Table::BlockIterator bi = table.beginblock();

        //测试删除一条存在的记录
        //删除key=48的记录，以确保稍后可以将其插入第一个block
        nid = 48;
        type->htobe(&nid);
        int ret = table.remove(table.locate(iov[0].iov_base, (unsigned int) iov[0].iov_len), iov[0].iov_base, (unsigned int) iov[0].iov_len);
        REQUIRE(ret == S_OK);
        REQUIRE(unsigned int(table.recordCount()) == totalRecord - 1);
        totalRecord = unsigned int(table.recordCount());
        
        //测试删除一条不存在的记录
        //key=48的记录已被删除，尝试再次删除，要求返回删除失败
        ret = table.remove(table.locate(iov[0].iov_base, (unsigned int) iov[0].iov_len), iov[0].iov_base, (unsigned int) iov[0].iov_len);
        REQUIRE(ret == S_FALSE);

        bi++;
        long long bound1 = 0, bound2 = 0; //两个block中存储的record key的最小值，key=0的记录已被插入，故bound1 = 0

        //确定bound2，以便后续操作
        for (long long i = 1; i < 192; ++i) {
            nid = i;
            bound2 = i;
            type->htobe(&nid);
            // locate位置
            if(table.locate(iov[0].iov_base, (unsigned int) iov[0].iov_len) == bi->getSelf())
            break;
        }
        bi = table.beginblock();
        for (long long i = bound2; i < bound2 + 50; ++i) {
            nid = i;
            type->htobe(&nid);
            // locate位置
            unsigned int blkid =
                table.locate(iov[0].iov_base, (unsigned int) iov[0].iov_len);
            // 插入记录
            table.insert(blkid, iov);
        }

        //确保第二个block中含有50条记录
        for (long long i = bound2 + 50; i < bound2 + 96; ++i) {
            nid = i;
            type->htobe(&nid);
            unsigned int toRemove = table.locate(iov[0].iov_base, (unsigned int) iov[0].iov_len); //该record所在block编号
            table.remove(toRemove, iov[0].iov_base, (unsigned int) iov[0].iov_len);
        }

        //确保第一个block中保存的record key取值为0~47，共48条记录
        for (long long i = bound2 - 1; i > 47; --i) {
            nid = i;
            type->htobe(&nid);
            ret = table.remove(bi->getSelf(), iov[0].iov_base, (unsigned int) iov[0].iov_len);
        }

        //将key=48的一条记录插入第一个block，插入后该block后有49条记录，有效长度超过一半
        nid = 48;
        type->htobe(&nid);
        // locate位置
        unsigned int blkid =
            table.locate(iov[0].iov_base, (unsigned int) iov[0].iov_len);
        // 插入记录
        REQUIRE(blkid == 1);
        ret = table.insert(blkid, iov);
        REQUIRE(ret == S_OK);

        //至此。前两个block中分别存有49、50条记录，且key值连续。
        REQUIRE(bi->getSlots() == 49);
        bi++;
        REQUIRE(bi->getSlots() == 50);
        bi = table.beginblock();

        //下面将对block合并的各种情况进行测试。

        unsigned int totalData = table.dataCount(); //总数据块数
        unsigned int totalIdle = table.idleCount(); //总空闲块数
        unsigned short n1 = bi->getSlots(); //第一个block记录条数

        //情况1：无需合并
        //尝试移除位于第一个block上的key=0的记录
        totalRecord = unsigned int(table.recordCount());
        nid = 0;
        type->htobe(&nid);
        unsigned int toRemove = table.locate(iov[0].iov_base, (unsigned int) iov[0].iov_len); //该record所在block编号
        REQUIRE(toRemove == 1);
        ret = table.remove(toRemove, iov[0].iov_base, (unsigned int) iov[0].iov_len);
        REQUIRE(ret == S_OK);
        REQUIRE(totalData == table.dataCount());
        REQUIRE(totalIdle == table.idleCount());
        REQUIRE(bi->getSlots() == n1 - 1);
        REQUIRE(unsigned int(table.recordCount()) == totalRecord - 1);
        n1 = bi->getSlots();
        totalRecord = unsigned int(table.recordCount());

        //情况2：需要合并，但空间不足以包含下一个block，此时尽量平分slots
        //尝试移除位于第一个block上的key=1的记录
        bi++;
        unsigned short n2 = bi->getSlots();
        bi = table.beginblock();
        nid = 1;
        type->htobe(&nid);
        toRemove = table.locate(iov[0].iov_base, (unsigned int) iov[0].iov_len); //该record所在block编号
        REQUIRE(toRemove == 1);
        ret = table.remove(toRemove, iov[0].iov_base, (unsigned int) iov[0].iov_len);
        REQUIRE(ret == S_OK);
        REQUIRE(totalData == table.dataCount());
        REQUIRE(totalIdle == table.idleCount());
        REQUIRE(bi->getSlots() == n1); //删除了一条记录，为平衡slots数目，又从后一个block调来一条记录
        bi++;
        REQUIRE(bi->getSlots() == n2 - 1);
        n2 = bi->getSlots();
        bi = table.beginblock();
        REQUIRE(unsigned int(table.recordCount()) == totalRecord - 1);
        totalRecord = unsigned int(table.recordCount());

        //情况3：直接合并
        //从第一个block上再删除两条记录，确保达到了合并条件
        //尝试移除位于第一个block上的key=2与3的记录
        nid = 2;
        type->htobe(&nid);
        toRemove = table.locate(iov[0].iov_base, (unsigned int) iov[0].iov_len); //该record所在block编号
        REQUIRE(toRemove == 1);
        ret = table.remove(toRemove, iov[0].iov_base, (unsigned int) iov[0].iov_len);
        REQUIRE(ret == S_OK);
        nid = 3;
        type->htobe(&nid);
        toRemove = table.locate(iov[0].iov_base, (unsigned int) iov[0].iov_len); //该record所在block编号
        REQUIRE(toRemove == 1);
        ret = table.remove(toRemove, iov[0].iov_base, (unsigned int) iov[0].iov_len);
        REQUIRE(ret == S_OK);
        REQUIRE(table.dataCount() == totalData - 1);
        REQUIRE(table.idleCount() == totalIdle + 1);
        REQUIRE(bi->getSlots() == n1 + n2 - 2);
        REQUIRE(unsigned int(table.recordCount()) == totalRecord - 2);
    }

    SECTION("update")
    {
        Table table;
        table.open("table");
        DataType *type = table.info_->fields[table.info_->key].type;

        std::vector<struct iovec> iov(3);
        long long nid;
        char phone[20];
        char addr[128];

        iov[0].iov_base = &nid;
        iov[0].iov_len = 8;
        iov[1].iov_base = phone;
        iov[1].iov_len = 20;
        iov[2].iov_base = (void *) addr;
        iov[2].iov_len = 128;

        unsigned int totalData = table.dataCount();
        unsigned int totalIdle = table.idleCount();
        unsigned int totalRecord = unsigned int(table.recordCount());
        //尝试更新一条不存在的记录

        nid = 0;
        type->htobe(&nid);
        int ret = table.update(table.locate(iov[0].iov_base, (unsigned int) iov[0].iov_len), iov);
        REQUIRE(ret == S_FALSE);
        REQUIRE(totalData == table.dataCount());
        REQUIRE(totalIdle == table.idleCount());
        REQUIRE(totalRecord == unsigned int(table.recordCount()));
        //尝试更新一条已有的记录
        nid = 4;
        type->htobe(&nid);
        ret = table.update(table.locate(iov[0].iov_base, (unsigned int) iov[0].iov_len), iov);
        REQUIRE(ret == S_OK);
        REQUIRE(totalData == table.dataCount());
        REQUIRE(totalIdle == table.idleCount());
        REQUIRE(totalRecord == unsigned int(table.recordCount()));
    }
}