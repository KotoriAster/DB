////
// @file schema.cc
// @brief
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <stdlib.h>
#include <db/schema.h>
#include <db/block.h>
#include <db/endian.h>
#include <db/record.h>
#include <db/file.h>
#include <db/buffer.h>

namespace db {

const char *Schema::META_FILE = "_meta.db";

void Schema::init(Buffer *buffer)
{
    // 指向buffer和filepool
    buffer_ = buffer;
    // 加入meta
    RelationInfo kMetaInfo(META_FILE);
    tablespace_[META_FILE] = kMetaInfo;
    // 打开meta
    open();
}

void Schema::open()
{
    // 读取超块
    SuperBlock super;
    BufDesp *desp = buffer_->borrow(META_FILE, 0);
    super.attach(desp->buffer);

    // meta未初始化，初始化超块
    unsigned int first; // 第1个meta块
    if (super.getMagic() != MAGIC_NUMBER) {
        super.clear(0);      // spaceid总是0
        super.setFirst(1);   // 第1个meta块
        super.setChecksum(); // 重新计算校验和

        buffer_->writeBuf(desp); // 写超块
        first = 1;
    } else {
        first = super.getFirst(); // 第1个meta块
    }
    super.detach(); // 分离超块指针
    desp->relref(); // 释放超块

    // 读第1个meta块
    MetaBlock block;
    desp = buffer_->borrow(META_FILE, first);
    block.attach(desp->buffer);
    if (block.getMagic() != MAGIC_NUMBER)
        block.clear(0, first, BLOCK_TYPE_META);

    // 枚举所有slots，加载tablespace_
    unsigned short count = block.getSlots();
    Slot *slots = block.getSlotsPointer();
    for (unsigned short i = 0; i < count; ++i) {
        RelationInfo info;

        // 得到记录
        Record record;
        unsigned char *rb = desp->buffer + be16toh(slots[i].offset);
        record.attach(rb, BLOCK_SIZE);

        // 先分配iovec
        size_t fields = record.fields();
        std::vector<struct iovec> iov(fields);
        unsigned char header;

        // 从记录得到iovec
        record.ref(iov, &header);
        std::string table;
        retrieveInfo(table, info, iov); // 填充info

        // 插入tablespace
        tablespace_.insert(std::pair<std::string, RelationInfo>(table, info));
    }

    block.detach(); // 分离超块指针
    desp->relref(); // 释放超块
}

int Schema::create(const char *table, RelationInfo &info)
{
    if ((size_t) info.count != info.fields.size()) return EINVAL;

    // 先将info转化iov
    int total = info.iovSize();
    std::vector<struct iovec> iov(total);

    // 初始化iov
    initIov(table, info, iov);

    // 在表空间中添加
    std::string t(table);
    std::pair<TableSpace::iterator, bool> pret =
        tablespace_.insert(std::pair<std::string, RelationInfo>(t, info));
    if (!pret.second) return EEXIST;

    // TODO: 读1个meta块
    MetaBlock meta;
    BufDesp *desp = buffer_->borrow(META_FILE, 1);
    meta.attach(desp->buffer);
    unsigned short length = (unsigned short) Record::size(iov);
    unsigned char *buf = meta.allocate(length);
    if (buf == NULL) {
        // TODO: 再分配一个block
    }

    // 将关系信息写入buf，这里不需要排序，因为有tablespace_
    Record record;
    record.attach(buf, length);
    unsigned char header;
    record.set(iov, &header);

    // 处理checksum
    meta.setChecksum();

    // 写meta文件
    buffer_->writeBuf(desp); // 写meta块
    meta.detach();           // 分离超块指针
    desp->relref();          // 释放超块

    return S_OK;
}

std::pair<Schema::TableSpace::iterator, bool> Schema::lookup(const char *table)
{
    std::string t(table);
    TableSpace::iterator it = tablespace_.find(t);
    bool ret = it != tablespace_.end();
    return std::pair<TableSpace::iterator, bool>(it, ret);
}

void Schema::initIov(
    const char *table,
    RelationInfo &info,
    std::vector<struct iovec> &iov)
{
    // table
    iov[0].iov_base = (void *) table;
    iov[0].iov_len = strlen(table) + 1;
    // path
    iov[1].iov_base = (void *) info.path.c_str();
    iov[1].iov_len = info.path.size() + 1;
    // count
    unsigned short count = info.count;
    info.count = htobe16(info.count); // 底层保存为big endian
    iov[2].iov_base = &info.count;
    iov[2].iov_len = sizeof(unsigned short);
    // type
    info.type = htobe16(info.type);
    iov[3].iov_base = &info.type;
    iov[3].iov_len = sizeof(unsigned short);
    // key
    info.key = htobe32(info.key);
    iov[4].iov_base = &info.key;
    iov[4].iov_len = sizeof(unsigned int);
    // size
    iov[5].iov_base = &info.size; // 初始化为0，不需要转化为big endian
    iov[5].iov_len = sizeof(unsigned long long);
    // rows
    iov[6].iov_base = &info.rows; // 初始化为0，不需要转化为big endian
    iov[6].iov_len = sizeof(unsigned long long);

    // 初始化field
    for (unsigned short i = 0; i < count; ++i) {
        // 字段的名字
        iov[7 + i * 4 + 0].iov_base = (void *) info.fields[i].name.c_str();
        iov[7 + i * 4 + 0].iov_len = info.fields[i].name.size() + 1;
        // 字段的位置
        info.fields[i].index = htobe64(info.fields[i].index);
        iov[7 + i * 4 + 1].iov_base = (void *) &info.fields[i].index;
        iov[7 + i * 4 + 1].iov_len = sizeof(unsigned long long);
        // 字段的长度
        info.fields[i].length = htobe64(info.fields[i].length);
        iov[7 + i * 4 + 2].iov_base = (void *) &info.fields[i].length;
        iov[7 + i * 4 + 2].iov_len = sizeof(long long);
        // 字段的类型
        iov[7 + i * 4 + 3].iov_base = (void *) info.fields[i].type->name;
        iov[7 + i * 4 + 3].iov_len = strlen(info.fields[i].type->name) + 1;
    }
}

void Schema::retrieveInfo(
    std::string &table,
    RelationInfo &info,
    std::vector<struct iovec> &iov)
{
    // 读取path
    table = (const char *) iov[0].iov_base;
    info.path = (const char *) iov[1].iov_base;
    // 读取count
    ::memcpy(&info.count, iov[2].iov_base, sizeof(unsigned short));
    info.count = be16toh(info.count);
    // 读取type
    ::memcpy(&info.type, iov[3].iov_base, sizeof(unsigned short));
    info.type = be16toh(info.type);
    // 读取key位置
    ::memcpy(&info.key, iov[4].iov_base, sizeof(unsigned int));
    info.key = be32toh(info.key);
    // 读取size
    ::memcpy(&info.size, iov[5].iov_base, sizeof(unsigned long long));
    info.size = be64toh(info.size);
    // 读取rows
    ::memcpy(&info.rows, iov[6].iov_base, sizeof(unsigned long long));
    info.rows = be64toh(info.rows);

    // 返回各个字段
    size_t count = info.count;
    info.fields.clear();
    for (size_t i = 0; i < count; ++i) {
        FieldInfo field;

        // 字段名字
        field.name = (const char *) iov[7 + i * 4].iov_base;
        // 字段位置
        ::memcpy(
            &field.index,
            iov[7 + i * 4 + 1].iov_base,
            sizeof(unsigned long long));
        field.index = be64toh(field.index);
        // 字段长度
        ::memcpy(&field.length, iov[7 + i * 4 + 2].iov_base, sizeof(long long));
        field.length = be64toh(field.length);
        // 字段类型
        const char *tname = (const char *) iov[7 + i * 4 + 3].iov_base;
        field.type = findDataType(tname);

        info.fields.push_back(field);
    }
}

void dbInit(size_t bufsize)
{
    static bool inited = false;
    if (!inited) {
        // 初始化全局变量
        kBuffer.init(&kFiles, bufsize);
        kFiles.init(&kSchema);
        kSchema.init(&kBuffer);
    }
}

Schema kSchema;

} // namespace db