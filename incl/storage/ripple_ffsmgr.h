#ifndef _RIPPLE_STORAGE_FFSMGR_H
#define _RIPPLE_STORAGE_FFSMGR_H

typedef enum RIPPLE_FFSMGR_IF_TYPE
{
    RIPPLE_FFSMG_IF_TYPE_TRAIL       = 0x00
} ripple_ffsmgr_if_type;

typedef enum RIPPLE_FFSMGR_STATUS
{
    RIPPLE_FFSMGR_STATUS_NOP        = 0x00,
    RIPPLE_FFSMGR_STATUS_INIT       = 0x01,                 /* 初始化               */
    RIPPLE_FFSMGR_STATUS_USED       = 0x02,                 /* 可用   */
    RIPPLE_FFSMGR_STATUS_SHIFTFILE  = 0x03                  /* 文件切换   */
} ripple_ffsmgr_status;

typedef enum RIPPLE_FFSMGR_IF_OPTYPE
{
    RIPPLE_FFSMGR_IF_OPTYPE_SERIAL        = 0x01,
    RIPPLE_FFSMGR_IF_OPTYPE_DESERIAL      = 0x02
} ripple_ffsmgr_if_optype;

typedef struct RIPPLE_FFSMGR_IF
{
    /* 初始化 */
    bool (*ffsmgr_init)(int optype, void* state);

    /* 序列化 头部 */
    bool (*ffsmgr_serial)(ripple_ff_cxt_type type, void* data, void* state);

    /* 反序列化 头部 */
    bool (*ffsmgr_deserial)(ripple_ff_cxt_type type, void** data, void* state);

    /* 内容释放 */
    void (*ffsmgr_free)(int type, void *state);

    /* 获取最小值 */
    int (*ffsmg_gettokenminsize)(int compatibility);

    /* 获取tail值 */
    int (*ffsmg_gettailsize)(int compatibility);

    /* 用于验证 record 的正确性 */
    bool (*ffsmgr_validrecord)(ripple_ff_cxt_type type, void* state, uint8 infotype, uint64 fileid, uint8* record);

    /* 获取 record 中记录的 grouptype */
    void (*ffsmgr_getrecordgrouptype)(void* state, uint8* record, uint8* grouptype);

    /* 获取 record 中记录的 subtype */
    bool (*ffsmgr_getrecordsubtype)(void* state, uint8* record, uint16* subtype);

    /* 获取 record 中记录的 lsn */
    uint64 (*ffsmgr_getrecordlsn)(void* state, uint8* record);

    /* 获取 record 真实数据基于 record 头部的偏移 */
    uint16 (*ffsmgr_getrecorddataoffset)(int compatibility);

    /* 获取 total length */
    uint64 (*ffsmgr_getrecordtotallength)(void* state, uint8* record);

    /* 获取 record length */
    uint16 (*ffsmgr_getrecordlength)(void* state, uint8* record);

    /* 设置 record length */
    void (*ffsmgr_setrecordlength)(void* state, uint8* record, uint16 reclength);

    /* 是否为事务开始的record */
    bool (*ffsmgr_isrecordtransstart)(void* state, uint8* record);

} ripple_ffsmgr_if;

typedef struct RIPPLE_FFSMGR_FDATA
{
    void*       ffdata;                 /* 格式化结构实现，内部维护的数据内容 */
                                        /* 
                                         * TAIL 格式文件说明:
                                         * 序列化:
                                         *      ripple_fftrail_privdata
                                         * 
                                         * 反序列化:
                                         *      ripple_fftrail_privdata
                                         */
    void*       ffdata2;                /* 格式化结构实现，内部维护的数据内容 */
                                        /* 
                                         * TAIL 格式文件说明:
                                         * 序列化:
                                         *      系统字典
                                         * 
                                         * 反序列化:
                                         *      ripple_fftrail_privdata
                                         *      在反序列化中,会在解析的过程中涉及到文件的切换，这时就需要在此处临时记录新文件中的字典信息
                                         */
    void*       extradata;              /* 格式化结构实现，所需的外部数据结构 */
                                        /* 
                                         * TRAIL 格式文件说明
                                         * 序列化:
                                         *      ripple_transcache
                                         * 反序列化:
                                         *      ListCell
                                         *          | ripple_cacherecord
                                         */
} ripple_ffsmgr_fdata;

typedef struct RIPPLE_FFSMGR_STATE_CALLBACK
{
    /* 获取数据库标识 */
    Oid     (*getdboid)(void* serial);

    /* 获取数据库名称 */
    char*   (*getdbname)(void* serial, Oid dboid);

    /* 回设置 dbid */
    void    (*setdboid)(void* serial, Oid dboid);

    /*获取 recordcache callback parser线程 */
    void* (*getrecords)(void* parser);

    /* pump 获取  parser线程状态 */
    int (*getparserstate)(void* parser);

    /*获取 txn2filebuffer 序列化*/
    ripple_file_buffers* (*getfilebuffer)(void* serial);

    /*获取 redosysdicts 序列化*/
    void (*setredosysdicts)(void* serial, void* catalogdata);

    /*dataset附加到list中 */
    void (*setonlinerefreshdataset)(void* serial, void* dataset);

    /* 根据 oid 获取 pg_namespace 数据 */
    void* (*getnamespace)(void* serial, Oid oid);

    /* 根据 oid 获取 pg_class 数据 */
    void* (*getclass)(void* serial, Oid oid);

    /* 根据 oid 获取 pg_index 数据 */
    void* (*getindex)(void* serial, Oid oid);

    /* 根据 oid 获取attributes */
    void* (*getattributes)(void* serial, Oid oid);

    /* 根据 oid 获取pg_type */
    void* (*gettype)(void* serial, Oid oid);

    /* 释放attributes链表, 大事务专用 */
    void (*freeattributes)(void* attrs);

    /* 应用到系统表,替换ripple_fftrail_txnmetadata的调用 */
    void (*catalog2transcache)(void* serial, void* catalog);

} ripple_ffsmgr_state_callback;

typedef struct RIPPLE_FFSMGR_STATE
{
    int                             compatibility;
    int                             bufid;                      /* 对应的 ripple_file_buffer 中缓存标识  */
    int                             maxbufid;
    void*                           privdata;                  /* 外部结构    反序列化为ripple_parserwork_traildecodecontext
                                                                *            序列化时为ripple_serialstate 
                                                                */
    uint8*                          recptr;                    /* 在组装/解析的 record 的起始位置        */
    ripple_ffsmgr_status            status;
    ripple_ffsmgr_fdata*            fdata;
    ripple_ffsmgr_if*               ffsmgr;
    ripple_ffsmgr_state_callback    callback;
} ripple_ffsmgr_state;

/* 初始化，设置使用的格式化接口 */
void ripple_ffsmgr_init(ripple_ffsmgr_if_type fftype, ripple_ffsmgr_state* ffsmgrstate);

/* 头部结构初始化 */
void* ripple_ffsmgr_headinit(int compatibility, FullTransactionId xid, uint64 fileid);

/* 初始化数据库信息 */
void* ripple_ffsmgr_dbmetadatainit(char* dbname);

#endif
