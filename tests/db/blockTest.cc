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
            sizeof(CommonHeader) + sizeof(TimeStamp) + 3 * sizeof(int));
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

        unsigned int head = super.getIdle();
        REQUIRE(head == 1);

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
        unsigned short *pslots = reinterpret_cast<unsigned short *>(
            buffer + BLOCK_SIZE - sizeof(unsigned int));
        REQUIRE(pslots == data.getSlotsPointer());
        REQUIRE(data.getFreespaceSize() == BLOCK_SIZE - 8 - sizeof(DataHeader));

        // 假设有5个slots槽位
        data.setSlots(5);
        REQUIRE(data.getTrailerSize() == 2 * 8);
        pslots = reinterpret_cast<unsigned short *>(
                     buffer + BLOCK_SIZE - sizeof(unsigned int)) -
                 5;
        REQUIRE(pslots == data.getSlotsPointer());
        REQUIRE(
            data.getFreespaceSize() ==
            BLOCK_SIZE - data.getTrailerSize() - sizeof(DataHeader));
    }

    SECTION("DataBlock.allocate")
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
            BLOCK_SIZE - sizeof(DataHeader) - sizeof(Trailer) - 2 * 8);

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
            BLOCK_SIZE - sizeof(DataHeader) - sizeof(Trailer) - 2 * 8 - 712);

        record.attach(buffer + sizeof(DataHeader) + 2 * 8, 712);
        char ggg[711 - 4];
        iov[0].iov_base = (void *) ggg;
        iov[0].iov_len = 711 - 4;
        record.set(iov, &h);
        REQUIRE(record.length() == 711);

        // 回收第2个空间
        unsigned short size = data.getFreeSize();
        data.deallocate(sizeof(DataHeader) + 8);
        REQUIRE(data.getFreeSize() == size + 8);
        record.attach(buffer + sizeof(DataHeader) + 8, 8);
        REQUIRE(!record.isactive());

        // 回收第3个空间
        size = data.getFreeSize();
        data.deallocate(sizeof(DataHeader) + 2 * 8);
        REQUIRE(data.getFreeSize() == size + 712);
        record.attach(buffer + sizeof(DataHeader) + 2 * 8, 8);
        REQUIRE(!record.isactive());

        data.shrink();
        size = data.getFreeSize();
        REQUIRE(
            size ==
            BLOCK_SIZE - sizeof(DataHeader) - data.getTrailerSize() - 8);
        unsigned short freespace = data.getFreeSpace();
        REQUIRE(freespace == sizeof(DataHeader) + 8);

        // 回收第1个空间
        size = data.getFreeSize();
        data.deallocate(sizeof(DataHeader));
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
}