////
// @file blockTest.cc
// @brief
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include "../catch.hpp"
#include <db/block.h>
#include <db/record.h>
using namespace db;

TEST_CASE("db/block.h")
{
    SECTION("size")
    {
        REQUIRE(sizeof(CommonHeader) == sizeof(int) * 3);
        REQUIRE(sizeof(Trailer) == 2 * sizeof(int));
        REQUIRE(sizeof(Trailer) % 8 == 0);
        REQUIRE(
            sizeof(SuperHeader) ==
            sizeof(CommonHeader) + sizeof(TimeStamp) + 5 * sizeof(int));
        REQUIRE(sizeof(SuperHeader) % 8 == 0);
        REQUIRE(sizeof(IdleHeader) == sizeof(CommonHeader) + sizeof(int));
        REQUIRE(sizeof(IdleHeader) % 8 == 0);
        REQUIRE(
            sizeof(DataHeader) == sizeof(CommonHeader) + 2 * sizeof(int) +
                                      sizeof(TimeStamp) + 2 * sizeof(short));
        REQUIRE(sizeof(DataHeader) % 8 == 0);
    }

    SECTION("super")
    {
        SuperBlock super;
        unsigned char buffer[SUPER_SIZE];
        super.attach(buffer);
        super.clear(3);

        // magic number：0x64623031
        REQUIRE(buffer[0] == 0x64);
        REQUIRE(buffer[1] == 0x62);
        REQUIRE(buffer[2] == 0x30);
        REQUIRE(buffer[3] == 0x31);

        unsigned short type = super.getType();
        REQUIRE(type == BLOCK_TYPE_SUPER);
        unsigned short freespace = super.getFreeSpace();
        REQUIRE(freespace == sizeof(SuperHeader));

        unsigned int spaceid = super.getSpaceid();
        REQUIRE(spaceid == 3);

        unsigned int idle = super.getIdle();
        REQUIRE(idle == 0);

        TimeStamp ts = super.getTimeStamp();
        char tb[64];
        REQUIRE(ts.toString(tb, 64));
        // printf("ts=%s\n", tb);
        TimeStamp ts1;
        ts1.now();
        REQUIRE(ts < ts1);

        REQUIRE(super.checksum());
    }

    SECTION("data")
    {
        DataBlock data;
        unsigned char buffer[BLOCK_SIZE];

        data.attach(buffer);
        data.clear(1, 3, BLOCK_TYPE_DATA);

        // magic number：0x64623031
        REQUIRE(buffer[0] == 0x64);
        REQUIRE(buffer[1] == 0x62);
        REQUIRE(buffer[2] == 0x30);
        REQUIRE(buffer[3] == 0x31);

        unsigned int spaceid = data.getSpaceid();
        REQUIRE(spaceid == 1);

        unsigned short type = data.getType();
        REQUIRE(type == BLOCK_TYPE_DATA);

        unsigned short freespace = data.getFreeSpace();
        REQUIRE(freespace == sizeof(DataHeader));

        unsigned int next = data.getNext();
        REQUIRE(next == 0);

        unsigned int self = data.getSelf();
        REQUIRE(self == 3);

        TimeStamp ts = data.getTimeStamp();
        char tb[64];
        REQUIRE(ts.toString(tb, 64));
        // printf("ts=%s\n", tb);
        TimeStamp ts1;
        ts1.now();
        REQUIRE(ts < ts1);

        unsigned short slots = data.getSlots();
        REQUIRE(slots == 0);

        REQUIRE(data.getFreeSize() == data.getFreespaceSize());

        REQUIRE(data.checksum());

        REQUIRE(data.getTrailerSize() == 8);
        Slot *pslots =
            reinterpret_cast<Slot *>(buffer + BLOCK_SIZE - sizeof(Slot));
        REQUIRE(pslots == data.getSlotsPointer());
        REQUIRE(data.getFreespaceSize() == BLOCK_SIZE - 8 - sizeof(DataHeader));

        // 假设有5个slots槽位
        data.setSlots(5);
        REQUIRE(data.getTrailerSize() == sizeof(Slot) * 5 + sizeof(int));
        pslots =
            reinterpret_cast<Slot *>(buffer + BLOCK_SIZE - sizeof(Slot)) - 5;
        REQUIRE(pslots == data.getSlotsPointer());
        REQUIRE(
            data.getFreespaceSize() ==
            BLOCK_SIZE - data.getTrailerSize() - sizeof(DataHeader));
    }

    SECTION("allocate")
    {
        DataBlock data;
        unsigned char buffer[BLOCK_SIZE];

        data.attach(buffer);
        data.clear(1, 3, BLOCK_TYPE_DATA);

        // 分配8字节
        unsigned char *space = data.allocate(8);
        REQUIRE(space == buffer + sizeof(DataHeader));
        REQUIRE(data.getFreeSpace() == sizeof(DataHeader) + 8);
        REQUIRE(
            data.getFreeSize() ==
            BLOCK_SIZE - sizeof(DataHeader) - sizeof(Trailer) - 8);
        REQUIRE(data.getSlots() == 1);
        Slot *pslots = data.getSlotsPointer();
        REQUIRE(
            (unsigned char *) pslots ==
            buffer + BLOCK_SIZE - sizeof(int) - sizeof(Slot));
        REQUIRE(be16toh(pslots[0].offset) == sizeof(DataHeader));
        REQUIRE(be16toh(pslots[0].length) == 8);
        REQUIRE(data.getTrailerSize() == 8);

        // 随便写一个记录
        Record record;
        record.attach(buffer + sizeof(DataHeader), 8);
        std::vector<struct iovec> iov(1);
        int kkk = 3;
        iov[0].iov_base = (void *) &kkk;
        iov[0].iov_len = sizeof(int);
        unsigned char h = 0;
        record.set(iov, &h);

        // 分配5字节
        space = data.allocate(5);
        REQUIRE(space == buffer + sizeof(DataHeader) + 8);
        REQUIRE(data.getFreeSpace() == sizeof(DataHeader) + 2 * 8);
        REQUIRE(
            data.getFreeSize() ==
            BLOCK_SIZE - sizeof(DataHeader) - sizeof(Trailer) - 3 * 8);
        REQUIRE(data.getSlots() == 2);
        pslots = data.getSlotsPointer();
        REQUIRE(
            (unsigned char *) pslots ==
            buffer + BLOCK_SIZE - sizeof(int) - 2 * sizeof(Slot));
        REQUIRE(be16toh(pslots[0].offset) == sizeof(DataHeader) + 8);
        REQUIRE(be16toh(pslots[0].length) == 8);
        REQUIRE(be16toh(pslots[1].offset) == sizeof(DataHeader));
        REQUIRE(be16toh(pslots[1].length) == 8);
        REQUIRE(data.getTrailerSize() == 16);

        record.attach(buffer + sizeof(DataHeader) + 8, 8);
        kkk = 4;
        iov[0].iov_base = (void *) &kkk;
        iov[0].iov_len = sizeof(int);
        record.set(iov, &h);

        // 分配711字节
        space = data.allocate(711);
        REQUIRE(space == buffer + sizeof(DataHeader) + 8 * 2);
        REQUIRE(data.getFreeSpace() == sizeof(DataHeader) + 2 * 8 + 712);
        REQUIRE(
            data.getFreeSize() ==
            BLOCK_SIZE - sizeof(DataHeader) - sizeof(Trailer) - 3 * 8 - 712);
        REQUIRE(data.getSlots() == 3);
        pslots = data.getSlotsPointer();
        REQUIRE(
            (unsigned char *) pslots ==
            buffer + BLOCK_SIZE - sizeof(int) - 3 * sizeof(Slot));
        REQUIRE(be16toh(pslots[0].offset) == sizeof(DataHeader) + 16);
        REQUIRE(be16toh(pslots[0].length) == 712);
        REQUIRE(data.getTrailerSize() == 16);

        record.attach(buffer + sizeof(DataHeader) + 2 * 8, 712);
        char ggg[711 - 4];
        iov[0].iov_base = (void *) ggg;
        iov[0].iov_len = 711 - 4;
        record.set(iov, &h);
        REQUIRE(record.length() == 711);

        // 回收第2个空间
        unsigned short size = data.getFreeSize();
        data.deallocate(1);
        REQUIRE(data.getFreeSize() == size + 8);
        record.attach(buffer + sizeof(DataHeader) + 8, 8);
        REQUIRE(!record.isactive());

        REQUIRE(data.getSlots() == 2);
        pslots = data.getSlotsPointer();
        REQUIRE(
            (unsigned char *) pslots ==
            buffer + BLOCK_SIZE - sizeof(int) - 2 * sizeof(Slot));
        REQUIRE(be16toh(pslots[0].offset) == sizeof(DataHeader) + 16);
        REQUIRE(be16toh(pslots[0].length) == 712);
        REQUIRE(be16toh(pslots[1].offset) == sizeof(DataHeader));
        REQUIRE(be16toh(pslots[1].length) == 8);
        REQUIRE(data.getTrailerSize() == 16);

        data.shrink();
        size = data.getFreeSize();
        REQUIRE(
            size ==
            BLOCK_SIZE - sizeof(DataHeader) - data.getTrailerSize() - 8 - 712);
        unsigned short freespace = data.getFreeSpace();
        REQUIRE(freespace == sizeof(DataHeader) + 8 + 712);

        REQUIRE(data.getSlots() == 2);
        pslots = data.getSlotsPointer();
        REQUIRE(
            (unsigned char *) pslots ==
            buffer + BLOCK_SIZE - sizeof(int) - 2 * sizeof(Slot));
        REQUIRE(be16toh(pslots[0].offset) == sizeof(DataHeader));
        REQUIRE(be16toh(pslots[0].length) == 8);
        REQUIRE(be16toh(pslots[1].offset) == sizeof(DataHeader) + 8);
        REQUIRE(be16toh(pslots[1].length) == 712);
        REQUIRE(data.getTrailerSize() == 16);

        record.attach(buffer + sizeof(DataHeader) + 8, 8);
        REQUIRE(record.isactive());

        // 回收第3个空间
        size = data.getFreeSize();
        data.deallocate(1);
        REQUIRE(data.getFreeSize() == size + 712 + 8);
        record.attach(buffer + sizeof(DataHeader) + 8, 8);
        REQUIRE(!record.isactive());

        REQUIRE(data.getSlots() == 1);
        pslots = data.getSlotsPointer();
        REQUIRE(
            (unsigned char *) pslots ==
            buffer + BLOCK_SIZE - sizeof(int) - sizeof(Slot));
        REQUIRE(be16toh(pslots[0].offset) == sizeof(DataHeader));
        REQUIRE(be16toh(pslots[0].length) == 8);
        REQUIRE(data.getTrailerSize() == 8);

        // 回收第1个空间
        size = data.getFreeSize();
        data.deallocate(0);
        REQUIRE(data.getFreeSize() == size + 8);
        record.attach(buffer + sizeof(DataHeader), 8);
        REQUIRE(!record.isactive());

        // shrink
        data.shrink();
        size = data.getFreeSize();
        REQUIRE(
            size == BLOCK_SIZE - sizeof(DataHeader) - data.getTrailerSize());
        freespace = data.getFreeSpace();
        REQUIRE(freespace == sizeof(DataHeader));
    }

    SECTION("sort")
    {
        char x[3] = {'k', 'a', 'e'};
        std::sort(x, x + 3);
        REQUIRE(x[0] == 'a');
        REQUIRE(x[1] == 'e');
        REQUIRE(x[2] == 'k');
    }

    SECTION("reorder")
    {
        DataBlock data;
        unsigned char buffer[BLOCK_SIZE];

        data.attach(buffer);
        data.clear(1, 3, BLOCK_TYPE_DATA);

        // 假设表的字段是：id, char[12], varchar[512]
        std::vector<struct iovec> iov(3);
        DataType *type = findDataType("BIGINT");

        // 第1条记录
        long long id = 12;
        type->htobe(&id);
        iov[0].iov_base = &id;
        iov[0].iov_len = 8;
        iov[1].iov_base = "John Carter ";
        iov[1].iov_len = 12;
        const char *addr = "(323) 238-0693"
                           "909 - 1/2 E 49th St"
                           "Los Angeles, California(CA), 90011";
        iov[2].iov_base = (void *) addr;
        iov[2].iov_len = strlen(addr);

        // 分配空间
        unsigned short len = (unsigned short) Record::size(iov);
        unsigned char *space = data.allocate(len);
        // 填充记录
        Record record;
        record.attach(space, len);
        unsigned char header = 0;
        record.set(iov, &header);
        // 重新排序
        data.reorder(type, 0);

        REQUIRE(data.getFreeSpace() == sizeof(DataHeader) + len + 3);
        Slot *slot =
            (Slot *) (buffer + BLOCK_SIZE - sizeof(int) - sizeof(Slot));
        REQUIRE(be16toh(slot->offset) == sizeof(DataHeader));
        REQUIRE(be16toh(slot->length) == len + 3);
        REQUIRE(data.getSlots() == 1);

        // 第2条记录
        id = 3;
        type->htobe(&id);
        iov[0].iov_base = &id;
        iov[0].iov_len = 8;
        iov[1].iov_base = "Joi Biden    ";
        iov[1].iov_len = 12;
        const char *addr2 = "(323) 751-1875"
                            "7609 Mckinley Ave"
                            "Los Angeles, California(CA), 90001";
        iov[2].iov_base = (void *) addr2;
        iov[2].iov_len = strlen(addr2);

        // 分配空间
        unsigned short len2 = len;
        len = (unsigned short) Record::size(iov);
        space = data.allocate(len);
        // 填充记录
        record.attach(space, len);
        record.set(iov, &header);
        REQUIRE(be16toh(slot->offset) == sizeof(DataHeader));
        REQUIRE(be16toh(slot->length) == len + 5);
        --slot;
        REQUIRE(be16toh(slot->offset) == sizeof(DataHeader) + len2 + 3);
        REQUIRE(be16toh(slot->length) == len + 5);
        // 重新排序
        data.reorder(type, 0);

        slot = (Slot *) (buffer + BLOCK_SIZE - sizeof(int) - 2 * sizeof(Slot));
        REQUIRE(be16toh(slot->offset) == sizeof(DataHeader) + len2 + 3);
        REQUIRE(be16toh(slot->length) == len + 5);
        ++slot;
        REQUIRE(be16toh(slot->offset) == sizeof(DataHeader));
        REQUIRE(be16toh(slot->length) == len2 + 3);

        // 按照name排序
        type = findDataType("CHAR");
        data.reorder(type, 1);
        slot = (Slot *) (buffer + BLOCK_SIZE - sizeof(int) - 2 * sizeof(Slot));
        REQUIRE(be16toh(slot->offset) == sizeof(DataHeader));
        REQUIRE(be16toh(slot->length) == len2 + 3);
        ++slot;
        REQUIRE(be16toh(slot->offset) == sizeof(DataHeader) + len2 + 3);
        REQUIRE(be16toh(slot->length) == len + 5);
    }

    SECTION("lowerbound")
    {
        char x[4] = {'a', 'c', 'e', 'k'};
        char s = 'e';
        char *ret = std::lower_bound(x, x + 4, s);
        REQUIRE(ret == x + 2);

        // b总是搜索值
        struct Comp
        {
            char val;
            bool operator()(char a, char b)
            {
                REQUIRE(b == -1);
                return a < val;
            }
        };
        Comp comp;
        comp.val = 'd';
        s = -1;
        ret = std::lower_bound(x, x + 4, s, comp);
        REQUIRE(ret == x + 2);
    }

    SECTION("search")
    {
        DataBlock data;
        unsigned char buffer[BLOCK_SIZE];

        data.attach(buffer);
        data.clear(1, 3, BLOCK_TYPE_DATA);

        // 假设表的字段是：id, char[12], varchar[512]
        std::vector<struct iovec> iov(3);
        DataType *type = findDataType("BIGINT");

        // 第1条记录
        long long id = 12;
        type->htobe(&id);
        iov[0].iov_base = &id;
        iov[0].iov_len = 8;
        iov[1].iov_base = "John Carter ";
        iov[1].iov_len = 12;
        const char *addr = "(323) 238-0693"
                           "909 - 1/2 E 49th St"
                           "Los Angeles, California(CA), 90011";
        iov[2].iov_base = (void *) addr;
        iov[2].iov_len = strlen(addr);

        // 分配空间
        unsigned short len = (unsigned short) Record::size(iov);
        unsigned char *space = data.allocate(len);
        // 填充记录
        Record record;
        record.attach(space, len);
        unsigned char header = 0;
        record.set(iov, &header);
        // 重新排序
        data.reorder(type, 0);
        // 重设校验和
        data.setChecksum();

        // 第2条记录
        id = 3;
        type->htobe(&id);
        iov[0].iov_base = &id;
        iov[0].iov_len = 8;
        iov[1].iov_base = "Joi Biden    ";
        iov[1].iov_len = 12;
        const char *addr2 = "(323) 751-1875"
                            "7609 Mckinley Ave"
                            "Los Angeles, California(CA), 90001";
        iov[2].iov_base = (void *) addr2;
        iov[2].iov_len = strlen(addr2);

        // 分配空间
        unsigned short len2 = len;
        len = (unsigned short) Record::size(iov);
        space = data.allocate(len);
        // 填充记录
        record.attach(space, len);
        record.set(iov, &header);
        // 重新排序
        data.reorder(type, 0);

        Slot *slot =
            (Slot *) (buffer + BLOCK_SIZE - sizeof(int) - 2 * sizeof(Slot));
        REQUIRE(be16toh(slot->offset) == sizeof(DataHeader) + len2 + 3);
        REQUIRE(be16toh(slot->length) == len + 5);
        ++slot;
        REQUIRE(be16toh(slot->offset) == sizeof(DataHeader));
        REQUIRE(be16toh(slot->length) == len2 + 3);

        // 搜索
        id = 3;
        unsigned short ret = type->search(buffer, 0, &id, sizeof(id));
        REQUIRE(ret == 0);
        id = 12;
        ret = type->search(buffer, 0, &id, sizeof(id));
        REQUIRE(ret == 1);
        id = 2;
        ret = type->search(buffer, 0, &id, sizeof(id));
        REQUIRE(ret == 0);
    }

#if 0
    SECTION("splitposition")
    {
        // 打开meta.db
        Schema schema;
        int ret = schema.open();

        // 添加一个新表
        RelationInfo relation;
        relation.path = "table.dat";

        // id char(20) varchar
        FieldInfo field;
        field.name = "id";
        field.index = 0;
        field.length = 8;
        field.type = findDataType("BIGINT");
        relation.fields.push_back(field);

        field.name = "phone";
        field.index = 1;
        field.length = 20;
        field.type = findDataType("CHAR");
        relation.fields.push_back(field);

        field.name = "name";
        field.index = 2;
        field.length = -255;
        field.type = findDataType("VARCHAR");
        relation.fields.push_back(field);

        relation.count = 3;
        relation.key = 0;
        ret = schema.create("table", relation);

        // 找到table表
        std::pair<Schema::TableSpace::iterator, bool> bret =
            schema.lookup("table");
        ret = schema.load(bret.first);

        // 插入记录

        // 删除表，删除元文件
        Schema::TableSpace::iterator it = bret.first;
        it->second.file.close();
        REQUIRE(it->second.file.remove("table.dat") == S_OK);
        REQUIRE(schema.destroy() == S_OK);
    }
#endif
}