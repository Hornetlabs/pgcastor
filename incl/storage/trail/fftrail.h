#ifndef _FFTRAIL_H
#define _FFTRAIL_H

typedef enum FTRAIL_TOKENDATATYPE
{
    FTRAIL_TOKENDATATYPE_TINYINT     = 0x00,
    FTRAIL_TOKENDATATYPE_SMALLINT    = 0x01,
    FTRAIL_TOKENDATATYPE_INT         = 0x02,
    FTRAIL_TOKENDATATYPE_BIGINT      = 0x03,
    FTRAIL_TOKENDATATYPE_STR         = 0x04
} ftrail_tokendatatype;

#define ftrail_datatype              ftrail_tokendatatype

#define FTRAIL_MAGIC                 0x78A555C0
#define TOKENHDRSIZE                 6


/* 参数说明:
 * 入参:
 *      fhdr             函数头部标识
 *      tid              tokenid
 *      tinfo            tokeninfo
 *      tlen             加上 group 的总长度
 * 出参:
 *      buffer           token 的内容保存到该缓存中，返回新的地址空间
 */
#define FTRAIL_TOKENHDR2BUFFER(fhdr, tid, tinfo, tlen, buffer) \
{ \
    uint8* __uptr__ = NULL; \
    __uptr__ = buffer; \
    CONCAT(fhdr, 8bit)(&__uptr__, tid); \
    CONCAT(fhdr, 8bit)(&__uptr__, tinfo); \
    CONCAT(fhdr, 32bit)(&__uptr__, tlen); \
    buffer = __uptr__; \
}

/* group 添加至 buffer 中 */
#define FTRAIL_GROUP2BUFFER       FTRAIL_TOKENHDR2BUFFER

/* 
 * 将 buffer 中的内容，按照 token 的格式解析数据
 * 
 * 入参:
 *  buffer        缓存内容
 * 
 * 出参:
 *  tid           token 标识
 *  tinfo         token info标识
 *  tdatalen      token 的长度
 *  tdata         token 数据内容
*/
#define FTRAIL_BUFFER2TOKEN(fhdr, buffer, tid, tinfo, tdatalen, tdata) \
{ \
    uint8* _uptr_ = NULL; \
    _uptr_ = buffer; \
    tid = CONCAT(fhdr, 8bit)(&_uptr_); \
    tinfo = CONCAT(fhdr, 8bit)(&_uptr_); \
    tdatalen = CONCAT(fhdr, 32bit)(&_uptr_); \
    tdata = _uptr_; \
    buffer = (_uptr_ + tdatalen - TOKENHDRSIZE); \
}

typedef enum FFTRAIL_GROUPTYPE
{
    FFTRAIL_GROUPTYPE_NOP        = 0x00,                 /* 无效                         */
    FFTRAIL_GROUPTYPE_FHEADER    = 0x01,                 /* 文件头                       */
    FFTRAIL_GROUPTYPE_DATA       = 0x02,                 /* 文件中的 data 信息            */
    FFTRAIL_GROUPTYPE_RESET      = 0x03,                 /* 事务重置，标识未完成的事务清理   */
    FFTRAIL_GROUPTYPE_FTAIL      = 0x04,                 /* 文件尾信息                    */
} fftrail_grouptype;

typedef enum FFTRAIL_INFOTYPE
{
    FFTRAIL_INFOTYPE_GROUP       = 0x00,                 /* 标识为 group */
    FFTRAIL_INFOTYPE_TOKEN       = 0x01,                 /* 标识为 token */
} fftrail_infotype;

/*-------------------------------trail 文件内的私有信息 begin------------------------*/

/* tables hash 对应的 key,序列化 */
typedef struct FFTRAIL_TABLE_SERIALKEY
{
    Oid                                 dbid;
    Oid                                 tbid;
} fftrail_table_serialkey;

/* tables hash 对应的 value */
typedef struct FFTRAIL_TABLE_SERIALENTRY
{
    fftrail_table_serialkey      key;
    uint32                              dbno;
    uint32                              tbno;
    char                                schema[NAMEDATALEN];
    char                                table[NAMEDATALEN];
} fftrail_table_serialentry;


/* table hash 反序列化时的key */
typedef struct FFTRAIL_TABLE_DESERIALKEY
{
    uint32                              tbnum;
} fftrail_table_deserialkey;

/* table hash 反序列化 value */
typedef struct FFTRAIL_TABLE_DESERIALENTRY
{
    fftrail_table_deserialkey    key;
    bool                                haspkey;
    uint16                              colcnt;
    Oid                                 oid;
    uint32                              dbno;
    char                                schema[NAMEDATALEN];
    char                                table[NAMEDATALEN];
    ff_column*                   columns;
} fftrail_table_deserialentry;

/* databases hash 序列化对应的 value */
typedef struct FFTRAIL_DATABASE_SERIALENTRY
{
    Oid                                 oid;
    uint32                              no;
    char                                database[NAMEDATALEN];
} fftrail_database_serialentry;

/* databases hash 反序列化对应的 value */
typedef struct FFTRAIL_DATABASE_DESERIALENTRY
{
    uint32                              no;
    Oid                                 oid;
    char                                database[NAMEDATALEN];
} fftrail_database_deserialentry;


typedef struct FFTRAIL_PRIVDATA
{
    uint32                              dbnum;
    uint32                              tbnum;
    XLogRecPtr                          redolsn;
    XLogRecPtr                          restartlsn;
    XLogRecPtr                          confirmlsn;

    /* HTAB* tables 中的数据 */
    List*                               tbentrys;

    /* HTAB* databases 中的数据 */
    List*                               dbentrys;
    HTAB*                               tables;
    HTAB*                               databases;
} fftrail_privdata;

/*-------------------------------trail 文件内的私有信息   end------------------------*/

/* 初始化 */
bool fftrail_init(int optype, void* state);

/* 数据序列化到 buffer */
bool fftrail_serial(ff_cxt_type type, void* data, void* state);

/* 数据反序列化到 data */
bool fftrail_deserial(ff_cxt_type type, void** data, void* state);

/* 内容释放 */
void fftrail_free(int optype, void* state);

bool fftrail_getrecordsubtype(void* state, uint8* record, uint16* subtype);

/* 获取 record 头中记录的长度 */
uint64 fftrail_getrecordlsn(void* state, uint8* record);

void fftrail_getrecordgrouptype(void* state, uint8* record, uint8* grouptype);

/* 获取真实数据基于 record 的偏移 */
uint16 fftrail_getrecorddataoffset(int compatibility);

/* 获取 total length */
uint64 fftrail_getrecordtotallength(void* state, uint8* record);

/* 获取 record length */
uint16 fftrail_getrecordlength(void* state, uint8* record);

/* 设置 record length */
void fftrail_setrecordlength(void* state, uint8* record, uint16 reclength);

bool fftrail_isrecordtransstart(void* state, uint8* record);

bool fftrail_validrecord(ff_cxt_type type, void* state, uint8 infotype, uint64 fileid, uint8* record);

/*
 * 序列化 
 * 在写数据之前检测是否需要切换 block
 *  需要切换 block，则执行切换操作
 */
bool fftrail_serialpreshiftblock(void* state);

/* 文件切换 */
bool fftrail_serialshiffile(void* state);

/* 将具体的数据加入到buffer中 */
uint8* fftrail_body2buffer(ftrail_tokendatatype tdtype,
                                            uint16  tdatalen,
                                            uint8*  tdata,
                                            uint8*  buffer);

/* 将具体的数据从buffer写入到 data 中 */
uint8* fftrail_buffer2body(ftrail_tokendatatype tdtype,
                                            uint64  tdatalen,
                                            uint8*  tdata,
                                            uint8*  buffer);

/* 将token数据加入到 buffer 中 */
uint8* fftrail_token2buffer(uint8 tid, uint8 tinfo,
                                    ftrail_tokendatatype tdtype,
                                    uint16  tdatalen,
                                    uint8*  tdata,
                                    uint32* tlen,
                                    uint8*  buffer);

/* 获取尾部长度 */
int fftrail_resetlen(int compatibility);

/* 获取尾部长度 */
int fftrail_taillen(int compatibility);

/* 获取 tokenminsize */
int fftrail_gettokenminsize(int compatibility);

/* 文件初始化 */
void fftrail_fileinit(void* state);

/*
 * 文件中的私有变量缓存清理
 */
void fftrail_invalidprivdata(int optype, void* privdata);

#endif
