////
// @file datatype.cc
// @brief
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <algorithm>
#include <db/datatype.h>
#include <db/block.h>
#include <db/endian.h>

namespace db {

static void CharSort(unsigned char *block, unsigned int key)
{
    struct Compare
    {
        unsigned char *buffer; // buffer指针
        unsigned int key;      // 键的位置

        bool operator()(unsigned short x, unsigned short y)
        {
            // 先转化为主机字节序
            x = be16toh(x);
            y = be16toh(y);

            // 引用两条记录
            Record rx, ry;
            rx.attach(buffer + x, 8);
            ry.attach(buffer + y, 8);
            std::vector<struct iovec> iovrx;
            std::vector<struct iovec> iovry;
            unsigned char xheader;
            unsigned char yheader;
            rx.ref(iovrx, &xheader);
            ry.ref(iovry, &yheader);

            // 得到x，y
            const char *xchar = (const char *) iovrx[key].iov_base;
            const char *ychar = (const char *) iovry[key].iov_base;

            // CHAR长度固定
            size_t xsize = iovrx[key].iov_len; // 字符串长度
            return strncmp(xchar, ychar, xsize) < 0;
        }
    };

    DataHeader *header = reinterpret_cast<DataHeader *>(block);
    unsigned count = be16toh(header->slots);
    unsigned short *slots = reinterpret_cast<unsigned short *>(
        block + BLOCK_SIZE - sizeof(int) - count * sizeof(unsigned short));

    Compare compare;
    compare.buffer = block;
    compare.key = key;

    std::sort(slots, slots + count, compare);
}

static void VarCharSort(unsigned char *block, unsigned int key)
{
    struct Compare
    {
        unsigned char *buffer; // buffer指针
        unsigned int key;      // 键的位置

        bool operator()(unsigned short x, unsigned short y)
        {
            // 先转化为主机字节序
            x = be16toh(x);
            y = be16toh(y);

            // 引用两条记录
            Record rx, ry;
            rx.attach(buffer + x, 8);
            ry.attach(buffer + y, 8);
            std::vector<struct iovec> iovrx;
            std::vector<struct iovec> iovry;
            unsigned char xheader;
            unsigned char yheader;
            rx.ref(iovrx, &xheader);
            ry.ref(iovry, &yheader);

            // 得到x，y
            const char *xchar = (const char *) iovrx[key].iov_base;
            const char *ychar = (const char *) iovry[key].iov_base;
            size_t xsize = iovrx[key].iov_len; // x字符串长度
            size_t ysize = iovry[key].iov_len; // y字符串长度

            // 比较字符串
            int ret = strncmp(xchar, ychar, xsize);
            if (ret != 0)
                return ret;          // 已经有结果
            else if (xsize == ysize) // 相同
                return 0;
            else if (xsize < 0)
                return xsize < ysize ? 1 : -1;
            else // xsize > 0
                return xsize < ysize ? -1 : 1;
        }
    };

    DataHeader *header = reinterpret_cast<DataHeader *>(block);
    unsigned count = be16toh(header->slots);
    unsigned short *slots = reinterpret_cast<unsigned short *>(
        block + BLOCK_SIZE - sizeof(int) - count * sizeof(unsigned short));

    Compare compare;
    compare.buffer = block;
    compare.key = key;

    std::sort(slots, slots + count, compare);
}

static void TinyIntSort(unsigned char *block, unsigned int key)
{
    struct Compare
    {
        unsigned char *buffer; // buffer指针
        unsigned int key;      // 键的位置

        bool operator()(unsigned short x, unsigned short y)
        {
            // 先转化为主机字节序
            x = be16toh(x);
            y = be16toh(y);

            // 引用两条记录
            Record rx, ry;
            rx.attach(buffer + x, 8);
            ry.attach(buffer + y, 8);
            std::vector<struct iovec> iovrx;
            std::vector<struct iovec> iovry;
            unsigned char xheader;
            unsigned char yheader;
            rx.ref(iovrx, &xheader);
            ry.ref(iovry, &yheader);

            // 得到x，y
            unsigned char ix = *((const unsigned char *) iovrx[key].iov_base);
            unsigned char iy = *((const unsigned char *) iovry[key].iov_base);

            return ix < iy;
        }
    };

    DataHeader *header = reinterpret_cast<DataHeader *>(block);
    unsigned count = be16toh(header->slots);
    unsigned short *slots = reinterpret_cast<unsigned short *>(
        block + BLOCK_SIZE - sizeof(int) - count * sizeof(unsigned short));

    Compare compare;
    compare.buffer = block;
    compare.key = key;

    std::sort(slots, slots + count, compare);
}

static void SmallIntSort(unsigned char *block, unsigned int key)
{
    struct Compare
    {
        unsigned char *buffer; // buffer指针
        unsigned int key;      // 键的位置

        bool operator()(unsigned short x, unsigned short y)
        {
            // 先转化为主机字节序
            x = be16toh(x);
            y = be16toh(y);

            // 引用两条记录
            Record rx, ry;
            rx.attach(buffer + x, 8);
            ry.attach(buffer + y, 8);
            std::vector<struct iovec> iovrx;
            std::vector<struct iovec> iovry;
            unsigned char xheader;
            unsigned char yheader;
            rx.ref(iovrx, &xheader);
            ry.ref(iovry, &yheader);

            // 得到x，y
            unsigned short ix =
                be16toh(*((const unsigned short *) iovrx[key].iov_base));
            unsigned short iy =
                be16toh(*((const unsigned short *) iovry[key].iov_base));

            return ix < iy;
        }
    };

    DataHeader *header = reinterpret_cast<DataHeader *>(block);
    unsigned count = be16toh(header->slots);
    unsigned short *slots = reinterpret_cast<unsigned short *>(
        block + BLOCK_SIZE - sizeof(int) - count * sizeof(unsigned short));

    Compare compare;
    compare.buffer = block;
    compare.key = key;

    std::sort(slots, slots + count, compare);
}

static void IntSort(unsigned char *block, unsigned int key)
{
    struct Compare
    {
        unsigned char *buffer; // buffer指针
        unsigned int key;      // 键的位置

        bool operator()(unsigned short x, unsigned short y)
        {
            // 先转化为主机字节序
            x = be16toh(x);
            y = be16toh(y);

            // 引用两条记录
            Record rx, ry;
            rx.attach(buffer + x, 8);
            ry.attach(buffer + y, 8);
            std::vector<struct iovec> iovrx;
            std::vector<struct iovec> iovry;
            unsigned char xheader;
            unsigned char yheader;
            rx.ref(iovrx, &xheader);
            ry.ref(iovry, &yheader);

            // 得到x，y
            unsigned int ix =
                be32toh(*((const unsigned int *) iovrx[key].iov_base));
            unsigned int iy =
                be32toh(*((const unsigned int *) iovry[key].iov_base));

            return ix < iy;
        }
    };

    DataHeader *header = reinterpret_cast<DataHeader *>(block);
    unsigned count = be16toh(header->slots);
    unsigned short *slots = reinterpret_cast<unsigned short *>(
        block + BLOCK_SIZE - sizeof(int) - count * sizeof(unsigned short));

    Compare compare;
    compare.buffer = block;
    compare.key = key;

    std::sort(slots, slots + count, compare);
}

static void BigIntSort(unsigned char *block, unsigned int key)
{
    struct Compare
    {
        unsigned char *buffer; // buffer指针
        unsigned int key;      // 键的位置

        bool operator()(unsigned short x, unsigned short y)
        {
            // 先转化为主机字节序
            x = be16toh(x);
            y = be16toh(y);

            // 引用两条记录
            Record rx, ry;
            rx.attach(buffer + x, 8);
            ry.attach(buffer + y, 8);
            std::vector<struct iovec> iovrx;
            std::vector<struct iovec> iovry;
            unsigned char xheader;
            unsigned char yheader;
            rx.ref(iovrx, &xheader);
            ry.ref(iovry, &yheader);

            // 得到x，y
            unsigned long long ix =
                be64toh(*((const unsigned long long *) iovrx[key].iov_base));
            unsigned long long iy =
                be64toh(*((const unsigned long long *) iovry[key].iov_base));

            return ix < iy;
        }
    };

    DataHeader *header = reinterpret_cast<DataHeader *>(block);
    unsigned count = be16toh(header->slots);
    unsigned short *slots = reinterpret_cast<unsigned short *>(
        block + BLOCK_SIZE - sizeof(int) - count * sizeof(unsigned short));

    Compare compare;
    compare.buffer = block;
    compare.key = key;

    std::sort(slots, slots + count, compare);
}

static void CharHtobe(void *) {}
static void CharBetoh(void *) {}

static void SmallIntHtobe(void *buf)
{
    unsigned short *p = reinterpret_cast<unsigned short *>(buf);
    *p = htobe16(*p);
}
static void SmallIntBetoh(void *buf)
{
    unsigned short *p = reinterpret_cast<unsigned short *>(buf);
    *p = be16toh(*p);
}

static void IntHtobe(void *buf)
{
    unsigned int *p = reinterpret_cast<unsigned int *>(buf);
    *p = htobe32(*p);
}
static void IntBetoh(void *buf)
{
    unsigned int *p = reinterpret_cast<unsigned int *>(buf);
    *p = be32toh(*p);
}

static void BigIntHtobe(void *buf)
{
    unsigned long long *p = reinterpret_cast<unsigned long long *>(buf);
    *p = htobe64(*p);
}
static void BigIntBetoh(void *buf)
{
    unsigned long long *p = reinterpret_cast<unsigned long long *>(buf);
    *p = be64toh(*p);
}

DataType *findDataType(const char *name)
{
    static DataType gdatatype[] = {
        {"CHAR", 65535, CharSort, CharHtobe, CharBetoh},             // 0
        {"VARCHAR", -65535, VarCharSort, CharHtobe, CharBetoh},      // 1
        {"TINYINT", 1, TinyIntSort, CharHtobe, CharBetoh},           // 2
        {"SMALLINT", 2, SmallIntSort, SmallIntHtobe, SmallIntBetoh}, // 3
        {"INT", 4, IntSort, IntHtobe, IntBetoh},                     // 4
        {"BIGINT", 8, BigIntSort, BigIntHtobe, BigIntBetoh},         // 5
        {},                                                          // x
    };

    int index = 0;
    do {
        if (gdatatype[index].name == NULL)
            break;
        else if (strcmp(gdatatype[index].name, name) == 0)
            return &gdatatype[index];
        else
            ++index;
    } while (true);
    return NULL;
}

} // namespace db