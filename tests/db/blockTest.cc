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

        // magic number：0xc1c6f01e
        REQUIRE(buffer[0] == 0xc1);
        REQUIRE(buffer[1] == 0xc6);
        REQUIRE(buffer[2] == 0xf0);
        REQUIRE(buffer[3] == 0x1e);

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
#if 0
    SECTION("meta")
    {
        MetaBlock meta;
        unsigned char buffer[BLOCK_SIZE];

        meta.attach(buffer);
        meta.clear();

        // magic number：0xc1c6f01e
        REQUIRE(buffer[0] == 0xc1);
        REQUIRE(buffer[1] == 0xc6);
        REQUIRE(buffer[2] == 0xf0);
        REQUIRE(buffer[3] == 0x1e);

        unsigned int spaceid = meta.getSpaceid();
        REQUIRE(spaceid == 0);

        unsigned short type = meta.getType();
        REQUIRE(type == BLOCK_TYPE_META);

        unsigned short freespace = meta.getFreeSpace();
        REQUIRE(freespace == sizeof(MetaHeader));

        unsigned int next = meta.getNext();
        REQUIRE(next == 0);

        TimeStamp ts = meta.getTimeStamp();
        char tb[64];
        REQUIRE(ts.toString(tb, 64));
        // printf("ts=%s\n", tb);
        TimeStamp ts1;
        ts1.now();
        REQUIRE(ts < ts1);

        unsigned int tables = meta.getTables();
        REQUIRE(tables == 0);

        REQUIRE(meta.checksum());
    }
#endif

    SECTION("data")
    {
        DataBlock data;
        unsigned char buffer[BLOCK_SIZE];

        data.attach(buffer);
        data.clear(1);

        // magic number：0xc1c6f01e
        REQUIRE(buffer[0] == 0xc1);
        REQUIRE(buffer[1] == 0xc6);
        REQUIRE(buffer[2] == 0xf0);
        REQUIRE(buffer[3] == 0x1e);

        unsigned int spaceid = data.getSpaceid();
        REQUIRE(spaceid == 1);

        unsigned short type = data.getType();
        REQUIRE(type == BLOCK_TYPE_DATA);

        unsigned short freespace = data.getFreeSpace();
        REQUIRE(freespace == sizeof(DataHeader));

        unsigned int next = data.getNext();
        REQUIRE(next == 0);

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
        REQUIRE(data.getGc() == 0);

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
        data.clear(1);

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
        struct iovec iov[2];
        int kkk = 3;
        iov[0].iov_base = (void *) &kkk;
        iov[0].iov_len = sizeof(int);
        unsigned char h = 0;
        record.set(iov, 1, &h);

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
        record.set(iov, 1, &h);

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
        record.set(iov, 1, &h);
        REQUIRE(record.length() == 711);

        // 回收第2个空间
        unsigned short size = data.getFreeSize();
        data.deallocate(sizeof(DataHeader) + 8);
        REQUIRE(data.getFreeSize() == size + 8);
        REQUIRE(data.getGc() == sizeof(DataHeader) + 8);
        record.attach(buffer + sizeof(DataHeader) + 8, 8);
        REQUIRE(record.length() == 8);
        record.ref(iov, 2, &h);
        unsigned short next =
            be16toh(*reinterpret_cast<unsigned short *>(iov[0].iov_base));
        REQUIRE(next == 0);
        REQUIRE(iov[0].iov_len == 2);
        unsigned short length =
            be16toh(*reinterpret_cast<unsigned short *>(iov[1].iov_base));
        REQUIRE(length == 8);
        REQUIRE(iov[1].iov_len == 2);

        // 回收第3个空间
        size = data.getFreeSize();
        data.deallocate(sizeof(DataHeader) + 2 * 8);
        REQUIRE(data.getFreeSize() == size + 712);
        REQUIRE(data.getGc() == sizeof(DataHeader) + 2 * 8);
        record.attach(buffer + sizeof(DataHeader) + 2 * 8, 8);
        REQUIRE(record.length() == 8); // 仍然是8字节
        record.ref(iov, 2, &h);
        next = be16toh(*reinterpret_cast<unsigned short *>(iov[0].iov_base));
        REQUIRE(next == sizeof(DataHeader) + 8);
        REQUIRE(iov[0].iov_len == 2);
        length = be16toh(*reinterpret_cast<unsigned short *>(iov[1].iov_base));
        REQUIRE(length == 712);
        REQUIRE(iov[1].iov_len == 2);

        // 回收第1个空间
        size = data.getFreeSize();
        data.deallocate(sizeof(DataHeader));
        REQUIRE(data.getFreeSize() == size + 8);
        REQUIRE(data.getGc() == sizeof(DataHeader));
        record.attach(buffer + sizeof(DataHeader), 8);
        REQUIRE(record.length() == 8); // 仍然是8字节
        record.ref(iov, 2, &h);
        next = be16toh(*reinterpret_cast<unsigned short *>(iov[0].iov_base));
        REQUIRE(next == sizeof(DataHeader) + 2 * 8);
        REQUIRE(iov[0].iov_len == 2);
        length = be16toh(*reinterpret_cast<unsigned short *>(iov[1].iov_base));
        REQUIRE(length == 8);
        REQUIRE(iov[1].iov_len == 2);
    }

#if 0
    SECTION("clear")
    {
        Block block;
        unsigned char buffer[Block::BLOCK_SIZE];
        block.attach(buffer);
        block.clear(1, 2);

        // magic number：0xc1c6f01e
        REQUIRE(buffer[0] == 0xc1);
        REQUIRE(buffer[1] == 0xc6);
        REQUIRE(buffer[2] == 0xf0);
        REQUIRE(buffer[3] == 0x1e);

        int spaceid = block.spaceid();
        REQUIRE(spaceid == 1);

        int blockid = block.blockid();
        REQUIRE(blockid == 2);

        int nextid = block.getNextid();
        REQUIRE(nextid == 0);

        short garbage = block.getGarbage();
        REQUIRE(garbage == 0);

        short freespace = block.getFreespace();
        REQUIRE(freespace == Block::BLOCK_DEFAULT_FREESPACE);

        // block.setChecksum();
        unsigned int check = block.getChecksum();
        REQUIRE(check == Block::BLOCK_DEFAULT_CHECKSUM);
        // printf("check=%x\n", check);
        bool bcheck = block.checksum();
        REQUIRE(bcheck);

        unsigned short count = block.getSlotsNum();
        REQUIRE(count == 0);

        unsigned short f1 = block.getFreeLength();
        REQUIRE(
            f1 == Block::BLOCK_SIZE - Block::BLOCK_DEFAULT_FREESPACE -
                      Block::BLOCK_CHECKSUM_SIZE);

        unsigned short type = block.getType();
        REQUIRE(type == BLOCK_TYPE_DATA);
    }

    SECTION("allocate")
    {
        Block block;
        unsigned char buffer[Block::BLOCK_SIZE];
        block.attach(buffer);
        block.clear(1, 2);

        struct iovec iov[3];
        const char *hello = "hello";
        iov[0].iov_base = (void *) hello;
        iov[0].iov_len = strlen(hello) + 1;
        int x = 3;
        iov[1].iov_base = &x;
        iov[1].iov_len = sizeof(int);
        const char *world = "world count xxx";
        iov[2].iov_base = (void *) world;
        iov[2].iov_len = strlen(world) + 1;

        unsigned char header = 0x84;

        unsigned short f1 = block.getFreespace();
        std::pair<size_t, size_t> ret = Record::size(iov, 3);

        REQUIRE(block.allocate(&header, iov, 3));
        unsigned short spos =
            Block::BLOCK_SIZE - Block::BLOCK_CHECKSUM_SIZE - 2;
        unsigned short f2 = *((unsigned short *) (buffer + spos));
        REQUIRE(f1 == f2);

        f2 = block.getSlotsNum();
        REQUIRE(f2 == 1);

        f1 = block.getFreespace();
        f2 = f1 - Block::BLOCK_FREESPACE_OFFSET - Block::BLOCK_FREESPACE_SIZE;
        REQUIRE(f2 >= (unsigned short) ret.first);
        REQUIRE(f2 % 8 == 0);
    }
#endif
}