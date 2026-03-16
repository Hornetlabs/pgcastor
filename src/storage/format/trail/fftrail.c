#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/algorithm/crc/crc_check.h"
#include "misc/ripple_misc_control.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_transcache.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "storage/trail/head/ripple_fftrail_head.h"
#include "storage/trail/data/ripple_fftrail_data.h"
#include "storage/trail/tail/ripple_fftrail_tail.h"
#include "storage/trail/reset/ripple_fftrail_reset.h"
#include "storage/trail/data/ripple_fftrail_dbmetadata.h"
#include "parser/trail/ripple_parsertrail.h"
#include "parser/trail/head/ripple_parsertrail_head.h"
#include "parser/trail/data/ripple_parsertrail_dbmetadata.h"


typedef bool (*serialtoken)(void* data, void* state);

typedef bool (*deserialtoken)(void** data, void* state);

typedef struct RIPPLE_TRAIL_GROUP
{
    int8            groupid;                        /* group id                     */
    char*           desc;                           /* 描述                          */
    serialtoken     serialfunc;                     /* 将 结构体中的数据序列化到缓存中   */
    deserialtoken   deserialfunc;                   /* 将 缓存中的数据反序列化到结构体   */
} ripple_trail_group;

static ripple_trail_group           m_trailgroups[] = 
{
    {
        RIPPLE_FFTRAIL_GROUPTYPE_NOP,
        "trail nop",
        NULL,
        NULL
    },
    {
        RIPPLE_FFTRAIL_GROUPTYPE_FHEADER,
        "trail file header",
        ripple_fftrail_head_serail,
        ripple_fftrail_head_deserail
    },
    {
        RIPPLE_FFTRAIL_GROUPTYPE_DATA,
        "trail file data",
        ripple_fftrail_data_serail,
        ripple_fftrail_data_deserail
    },
    {
        RIPPLE_FFTRAIL_GROUPTYPE_RESET,
        "trail file reset",
        ripple_fftrail_reset_serail,
        ripple_fftrail_reset_deserail
    },
    {
        RIPPLE_FFTRAIL_GROUPTYPE_FTAIL,
        "trail file tail",
        ripple_fftrail_tail_serail,
        ripple_fftrail_tail_deserail
    }
};

/* 校验数据是否完整 */
/* 校验头部完整 */
static bool ripple_fftrail_validfhead(uint8* header)
{
    uint8   tokenid = RIPPLE_FFTRAIL_GROUPTYPE_NOP;
    uint8   tokeninfo = RIPPLE_FFTRAIL_INFOTYPE_TOKEN;
    uint32  tokenlen = 0;
    uint32  tfmagic = 0;

    uint8* uptr = NULL;
    uint8*  tokendata = NULL;

    /* 解析头信息 */
    uptr = header;
    RIPPLE_FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    if(RIPPLE_FFTRAIL_GROUPTYPE_FHEADER != tokenid
        || RIPPLE_FFTRAIL_INFOTYPE_GROUP != tokeninfo)
    {
        return false;
    }

    /* 查看最后的四字节内容是否为 magic */
    uptr = header;

    tokenlen -= RIPPLE_TOKENHDRSIZE;
    tokenlen -= 4;

    tokendata += tokenlen;

    /* 魔数 */
    tfmagic = RIPPLE_CONCAT(get, 32bit)(&tokendata);
    if(tfmagic != RIPPLE_FTRAIL_MAGIC)
    {
        /* 大概率是没有刷盘完成 */
        return false;
    }

    return true;
}

/* 校验数据完整 */
static bool ripple_fftrail_validdata(int compatibility, uint8* data)
{
    /*
     * 1、获取完整的 record 数据
     * 2、在头部中获取 reclength
     * 3、获取 record 尾部
    */
    uint8   tokenid = RIPPLE_FFTRAIL_GROUPTYPE_NOP;
    uint8   tokeninfo = RIPPLE_FFTRAIL_INFOTYPE_TOKEN;
    int     headlen = 0;
    uint16  reclength = 0;
    uint32  tokenlen = 0;
    r_crc32c crc32 = 0;
    r_crc32c crc32rec = 0;

    uint8*  uptr = NULL;
    uint8*  crcuptr = NULL;
    uint8*  tokendata = NULL;

    /* 获取 record 数据 */
    uptr = data;
    RIPPLE_FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    if(RIPPLE_FFTRAIL_GROUPTYPE_DATA != tokenid
        || RIPPLE_FFTRAIL_INFOTYPE_GROUP != tokeninfo)
    {
        elog(RLOG_WARNING, "1");
        return false;
    }

    /* 偏移 group 头部长度 */
    crcuptr = uptr = tokendata;
    reclength = ripple_fftrail_data_getreclengthfromhead(compatibility, uptr);
    if(0 == reclength)
    {
        elog(RLOG_WARNING, "2");
        return false;
    }

    /* 偏移 record 内容 */
    headlen = ripple_fftrail_data_headlen(0);
    uptr += headlen;

    /* 换算 crc */
    headlen -= (RIPPLE_TOKENHDRSIZE + 4);
    INIT_CRC32C(crc32);
    COMP_CRC32C(crc32, crcuptr, headlen);

    /* 获取记录中的 crc */
    crcuptr += headlen;
    RIPPLE_FTRAIL_BUFFER2TOKEN(get, crcuptr, tokenid, tokeninfo, tokenlen, tokendata)
    if(RIPPLE_TRAIL_TOKENDATAHDR_ID_CRC32 != tokenid
        || RIPPLE_FFTRAIL_INFOTYPE_TOKEN != tokeninfo)
    {
        elog(RLOG_WARNING, "invalid record, hope RIPPLE_TRAIL_TOKENDATAHDR_ID_CRC32:%u, got:%u",
                            RIPPLE_TRAIL_TOKENDATAHDR_ID_CRC32,
                            tokenid);
        return false;
    }
    crc32rec = RIPPLE_CONCAT(get, 32bit)(&tokendata);

    /* 换算数据的 crc32 */
    COMP_CRC32C(crc32, uptr, reclength);
    FIN_CRC32C(crc32);

    if (!EQ_CRC32C(crc32rec, crc32))
    {
        elog(RLOG_WARNING, "trail record crc error, crc in record %08X, calc crc:%08X",
                            crc32rec,
                            crc32);
        return false;
    }

    /* 获取 rectail 信息 */
    uptr += reclength;

    RIPPLE_FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    if(RIPPLE_TRAIL_TOKENDATA_RECTAIL != tokenid
        || RIPPLE_FFTRAIL_INFOTYPE_TOKEN != tokeninfo)
    {
        elog(RLOG_WARNING, "4,reclength:%u", reclength);
        return false;
    }

    return true;
}

/* 校验文件RESET完整 */
static bool ripple_fftrail_validreset(int compatibility, uint64 fileid, uint8* tail)
{
    uint8   tokenid = RIPPLE_FFTRAIL_GROUPTYPE_NOP;
    uint8   tokeninfo = RIPPLE_FFTRAIL_INFOTYPE_TOKEN;
    uint32  tokenlen = 0;
    uint64  nfileid = 0;
    uint8*  uptr = NULL;
    uint8*  tokendata = NULL;

    /* 获取 record 数据 */
    uptr = tail;
    RIPPLE_FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    if(RIPPLE_FFTRAIL_GROUPTYPE_RESET != tokenid
        || RIPPLE_FFTRAIL_INFOTYPE_GROUP != tokeninfo)
    {
        return false;
    }

    /* 根据版本校验长度 */
    if(tokenlen != ripple_fftrail_resetlen(compatibility))
    {
        return false;
    }

    /* 获取内容 */
    uptr = tokendata;
    uptr += RIPPLE_TOKENHDRSIZE;

    nfileid = RIPPLE_CONCAT(get, 64bit)(&uptr);
    if((fileid + 1) != nfileid)
    {
        return false;
    }

    return true;
}

/* 校验文件尾部完整 */
static bool ripple_fftrail_validftail(int compatibility, uint64 fileid, uint8* tail)
{
    uint8   tokenid = RIPPLE_FFTRAIL_GROUPTYPE_NOP;
    uint8   tokeninfo = RIPPLE_FFTRAIL_INFOTYPE_TOKEN;
    uint32  tokenlen = 0;
    uint64  nfileid = 0;
    uint8*  uptr = NULL;
    uint8*  tokendata = NULL;

    /* 获取 record 数据 */
    uptr = tail;
    RIPPLE_FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    if(RIPPLE_FFTRAIL_GROUPTYPE_FTAIL != tokenid
        || RIPPLE_FFTRAIL_INFOTYPE_GROUP != tokeninfo)
    {
        return false;
    }

    /* 根据版本校验长度 */
    if(tokenlen != ripple_fftrail_taillen(compatibility))
    {
        return false;
    }

    /* 获取内容 */
    uptr = tokendata;
    uptr += RIPPLE_TOKENHDRSIZE;

    nfileid = RIPPLE_CONCAT(get, 64bit)(&uptr);
    if((fileid + 1) != nfileid)
    {
        return false;
    }

    return true;
}

bool ripple_fftrail_validrecord(ripple_ff_cxt_type type, void* state, uint8 infotype, uint64 fileid, uint8* record)
{
    bool result = false;
    ripple_ffsmgr_state* ffstate = NULL;
    ffstate = (ripple_ffsmgr_state*)state;

    if (RIPPLE_FFTRAIL_INFOTYPE_GROUP != infotype)
    {
        return result;
    }
    
    switch (type)
    {
        case RIPPLE_FFTRAIL_GROUPTYPE_FHEADER:
            result = ripple_fftrail_validfhead(record);
            break;
        case RIPPLE_FFTRAIL_GROUPTYPE_DATA:
            result = ripple_fftrail_validdata(ffstate->compatibility, record);
            break;
        case RIPPLE_FFTRAIL_GROUPTYPE_RESET:
            result = ripple_fftrail_validreset(ffstate->compatibility, fileid, record);
            break;
        case RIPPLE_FFTRAIL_GROUPTYPE_FTAIL:
            result = ripple_fftrail_validftail(ffstate->compatibility, fileid, record);
            break;
        default:
            elog(RLOG_WARNING, "unknown group type:%u", type);
            return false;
    }
    return result;
}

/*
 * 序列化 
 * 在写数据之前检测是否需要切换 block
 *  需要切换 block，则执行切换操作
 * 
 * 返回值:
 *  false           没有切换
 *  true            切换
 */
bool ripple_fftrail_serialpreshiftblock(void* state)
{
    bool shiftfile = false;
    int minsize = 0;
    int timeout = 0;
    uint64 freespc = 0;

    ripple_file_buffer* fbuffer = NULL;                 /* 缓存信息 */
    ripple_file_buffer* tmpfbuffer = NULL;              /* 缓存信息 */
    ripple_ffsmgr_state* ffstate = NULL;
    ripple_ff_fileinfo* finfo = NULL;                   /* 文件信息 */
    ripple_ff_fileinfo* tmpfinfo = NULL;
    ripple_file_buffers* txn2filebuffer = NULL;

    /* 获取表序列化所需的信息 */
    ffstate = (ripple_ffsmgr_state*)state;

    /* 获取file_buffers */
    txn2filebuffer = ffstate->callback.getfilebuffer(ffstate->privdata);

    /*
     * 查看空间是否满足最小要求，不满足则切换 buffer
     *  1、token 需要的大小
     *  2、file tail 需要的大小
     */
    /* 根据 bufid 获取 fbuffer */
    fbuffer = ripple_file_buffer_getbybufid(txn2filebuffer, ffstate->bufid);
    finfo = (ripple_ff_fileinfo*)fbuffer->privdata;

    /* 查看 fbuffer 中的 start 是否为0,若为0,那么初始化文件 */
    if(1 == finfo->blknum && 0 == fbuffer->start)
    {
        /* 初始化文件头部信息 */
        ripple_fftrail_fileinit(state);
    }

    /*
     * 查看空间是否满足最小要求，不满足则切换 buffer
     *  1、token 需要的大小
     *  2、file tail 需要的大小
     */
    minsize = ripple_fftrail_data_tokenminsize(ffstate->compatibility);

    /* file tail 需要的大小 */
    if(finfo->blknum == ffstate->maxbufid)
    {
        shiftfile = true;
        minsize += ripple_fftrail_taillen(ffstate->compatibility);
    }

    /* 查看剩余空间 */
    freespc = (fbuffer->maxsize - fbuffer->start);

    if(freespc > minsize)
    {
        return false;
    }

    /* 切换 buffer */
    if(true == shiftfile)
    {
        /* 添加尾部信息 */
        ripple_ff_tail fftail = { 0 };                  /* tail 信息 */
        fftail.nexttrailno = (finfo->fileid + 1);
        ffstate->ffsmgr->ffsmgr_serial(RIPPLE_FFTRAIL_CXT_TYPE_FTAIL, &fftail, ffstate);

        /* 缓存清理 */
        ripple_fftrail_invalidprivdata(RIPPLE_FFSMGR_IF_OPTYPE_SERIAL, ffstate->fdata->ffdata);
    }

    /* 获取缓存 */
    while(1)
    {
        ffstate->bufid = ripple_file_buffer_get(txn2filebuffer, &timeout);
        if(RIPPLE_INVALID_BUFFERID == ffstate->bufid)
        {
            if(RIPPLE_ERROR_TIMEOUT == timeout)
            {
                usleep(10000);
                continue;
            }
            elog(RLOG_WARNING, "get file buffer error");
            return false;
        }
        break;
    }

    /* 获取对应的 buffer */
    tmpfbuffer = ripple_file_buffer_getbybufid(txn2filebuffer, ffstate->bufid);
    if(NULL == tmpfbuffer->privdata)
    {
        tmpfinfo = (ripple_ff_fileinfo*)rmalloc1(sizeof(ripple_ff_fileinfo));
        if(NULL == tmpfinfo)
        {
            elog(RLOG_ERROR, "out of memory");
        }
        rmemset0(tmpfinfo, 0, '\0', sizeof(ripple_ff_fileinfo));
        tmpfbuffer->privdata = (void*)tmpfinfo;
    }
    else
    {
        tmpfinfo = (ripple_ff_fileinfo*)tmpfbuffer->privdata;
    }

    rmemcpy0(tmpfinfo, 0, finfo, sizeof(ripple_ff_fileinfo));

    tmpfinfo->fileid = (true == shiftfile ? (finfo->fileid + 1) : finfo->fileid);
    tmpfinfo->blknum = (true == shiftfile ? 1 : (finfo->blknum + 1));

    /* 将 buffer 写入到待刷新缓存中 */
    rmemcpy1(&tmpfbuffer->extra, 0, &fbuffer->extra, sizeof(ripple_file_buffer_extra));
    ripple_file_buffer_waitflush_add(txn2filebuffer, fbuffer);

    /* 重置 tmpfbuffer 中的 信息 */
    tmpfbuffer->flag = RIPPLE_FILE_BUFFER_FLAG_DATA;

    finfo = NULL;
    fbuffer = NULL;

    /* 重置 */
    finfo = tmpfinfo;
    fbuffer = tmpfbuffer;
    if(true == shiftfile)
    {
        /* 初始化文件信息 */
        ripple_fftrail_fileinit(ffstate);
        ffstate->status = RIPPLE_FFSMGR_STATUS_SHIFTFILE;
    }
    return true;
}

bool ripple_fftrail_serialshiffile(void* state)
{
    int timeout = 0;
    ripple_ff_tail fftail = { 0 };                      /* tail 信息 */
    ripple_file_buffer* fbuffer = NULL;                 /* 缓存信息 */
    ripple_file_buffer* tmpfbuffer = NULL;              /* 缓存信息 */
    ripple_ffsmgr_state* ffstate = NULL;
    ripple_ff_fileinfo* finfo = NULL;                   /* 文件信息 */
    ripple_ff_fileinfo* tmpfinfo = NULL;
    ripple_file_buffers* txn2filebuffer = NULL;

    /* 获取表序列化所需的信息 */
    ffstate = (ripple_ffsmgr_state*)state;

    /* 获取file_buffers */
    txn2filebuffer = ffstate->callback.getfilebuffer(ffstate->privdata);

    /*
     * 查看空间是否满足最小要求，不满足则切换 buffer
     *  1、token 需要的大小
     *  2、file tail 需要的大小
     */
    /* 根据 bufid 获取 fbuffer */
    fbuffer = ripple_file_buffer_getbybufid(txn2filebuffer, ffstate->bufid);
    finfo = (ripple_ff_fileinfo*)fbuffer->privdata;

    /* 查看 fbuffer 中的 start 是否为0,若为0,那么初始化文件 */
    if(1 == finfo->blknum && 0 == fbuffer->start)
    {
        /* 初始化文件头部信息 */
        ripple_fftrail_fileinit(state);
    }

    /* 添加尾部信息 */
    fftail.nexttrailno = (finfo->fileid + 1);
    ffstate->ffsmgr->ffsmgr_serial(RIPPLE_FFTRAIL_CXT_TYPE_FTAIL, &fftail, ffstate);

    /* 缓存清理 */
    ripple_fftrail_invalidprivdata(RIPPLE_FFSMGR_IF_OPTYPE_SERIAL, ffstate->fdata->ffdata);

    /* 获取缓存 */
    while(1)
    {
        ffstate->bufid = ripple_file_buffer_get(txn2filebuffer, &timeout);
        if(RIPPLE_INVALID_BUFFERID == ffstate->bufid)
        {
            if(RIPPLE_ERROR_TIMEOUT == timeout)
            {
                usleep(10000);
                continue;
            }
            elog(RLOG_WARNING, "get file buffer error");
            return false;
        }
        break;
    }

    /* 获取对应的 buffer */
    tmpfbuffer = ripple_file_buffer_getbybufid(txn2filebuffer, ffstate->bufid);
    if(NULL == tmpfbuffer->privdata)
    {
        tmpfinfo = (ripple_ff_fileinfo*)rmalloc1(sizeof(ripple_ff_fileinfo));
        if(NULL == tmpfinfo)
        {
            elog(RLOG_ERROR, "out of memory");
        }
        rmemset0(tmpfinfo, 0, '\0', sizeof(ripple_ff_fileinfo));
        tmpfbuffer->privdata = (void*)tmpfinfo;
    }
    else
    {
        tmpfinfo = (ripple_ff_fileinfo*)tmpfbuffer->privdata;
    }

    rmemcpy0(tmpfinfo, 0, finfo, sizeof(ripple_ff_fileinfo));

    tmpfinfo->fileid = finfo->fileid + 1;
    tmpfinfo->blknum = 1;

    /* 将 buffer 写入到待刷新缓存中 */
    rmemcpy1(&tmpfbuffer->extra, 0, &fbuffer->extra, sizeof(ripple_file_buffer_extra));
    ripple_file_buffer_waitflush_add(txn2filebuffer, fbuffer);
    finfo = NULL;
    fbuffer = NULL;

    /* 重置 tmpfbuffer 中的 信息 */
    tmpfbuffer->flag = RIPPLE_FILE_BUFFER_FLAG_DATA;

    /* 初始化文件信息 */
    ripple_fftrail_fileinit(ffstate);
    ffstate->status = RIPPLE_FFSMGR_STATUS_USED;

    return true;
}

/* 将具体的数据加入到buffer中 */
uint8* ripple_fftrail_body2buffer(ripple_ftrail_tokendatatype tdtype,
                                            uint16  tdatalen,
                                            uint8*  tdata,
                                            uint8*  buffer)
{
    uint8* _uptr_ = NULL;

    _uptr_ = buffer;

    switch (tdtype)
    {
        case RIPPLE_FTRAIL_TOKENDATATYPE_TINYINT:
            RIPPLE_CONCAT(put, 8bit)(&_uptr_, *((uint8*)tdata));
            break;
        case RIPPLE_FTRAIL_TOKENDATATYPE_SMALLINT:
            RIPPLE_CONCAT(put,16bit)(&_uptr_, *((uint16*)tdata));
            break;
        case RIPPLE_FTRAIL_TOKENDATATYPE_INT:
            RIPPLE_CONCAT(put, 32bit)(&_uptr_, *((uint32*)tdata));
            break;
        case RIPPLE_FTRAIL_TOKENDATATYPE_BIGINT:
            RIPPLE_CONCAT(put, 64bit)(&_uptr_, *((uint64*)tdata));
            break;
        case RIPPLE_FTRAIL_TOKENDATATYPE_STR:
            rmemcpy1(_uptr_, 0, (char*)tdata, tdatalen);
            _uptr_ += tdatalen;
            break;
        default:
            elog(RLOG_ERROR, "unknown tokendatatype:%d", tdtype);
            break;
    }
    return _uptr_;
}

/* 将具体的数据从buffer写入到 data 中 */
uint8* ripple_fftrail_buffer2body(ripple_ftrail_tokendatatype tdtype,
                                            uint64  tdatalen,
                                            uint8*  tdata,
                                            uint8*  buffer)
{
    uint8* _uptr_ = NULL;

    _uptr_ = buffer;

    switch (tdtype)
    {
        case RIPPLE_FTRAIL_TOKENDATATYPE_TINYINT:
            *tdata = RIPPLE_CONCAT(get, 8bit)(&_uptr_);
            break;
        case RIPPLE_FTRAIL_TOKENDATATYPE_SMALLINT:
            *((uint16*)tdata) = RIPPLE_CONCAT(get, 16bit)(&_uptr_);
            break;
        case RIPPLE_FTRAIL_TOKENDATATYPE_INT:
            *((uint32*)tdata) = RIPPLE_CONCAT(get, 32bit)(&_uptr_);
            break;
        case RIPPLE_FTRAIL_TOKENDATATYPE_BIGINT:
            *((uint64*)tdata) = RIPPLE_CONCAT(get, 64bit)(&_uptr_);
            break;
        case RIPPLE_FTRAIL_TOKENDATATYPE_STR:
            rmemcpy1(tdata, 0, _uptr_, tdatalen);
            _uptr_ += tdatalen;
            break;
        default:
            elog(RLOG_ERROR, "unknown tokendatatype:%d", tdtype);
            break;
    }
    return _uptr_;
}


/* 
 * 将数据按照 token 序列化到缓存中
 * 参数说明:
 * 入参:
 *      fhdr             函数头部标识
 *      tid              tokenid
 *      tinfo            tokeninfo
 *      tdtype           tokendatatype,取值参考: ripple_ftrail_tokendatatype
 *      tdatalen         数据长度
 *      tdata            数据
 * 出参:
 *      tlen             加上 token 的总长度
 *      buffer           token 的内容保存到该缓存中，返回新的地址空间
 */
uint8* ripple_fftrail_token2buffer(uint8 tid, uint8 tinfo,
                                    ripple_ftrail_tokendatatype tdtype,
                                    uint16  tdatalen,
                                    uint8*  tdata,
                                    uint32* tlen,
                                    uint8*  buffer)
{
    uint32  _tokenlen_ = 0;
    uint8* uptr = NULL;

    uptr = buffer;
    _tokenlen_ += RIPPLE_TOKENHDRSIZE;
    _tokenlen_ += tdatalen;

    RIPPLE_FTRAIL_TOKENHDR2BUFFER(put, tid, tinfo, _tokenlen_, uptr)

    /* 组装数据 */
    uptr = ripple_fftrail_body2buffer(tdtype, tdatalen, tdata, uptr);
    *tlen += _tokenlen_;
    return uptr;
}

/*
 * 尾部长度是固定的
*/
int ripple_fftrail_taillen(int compatibility)
{
    /*
     * 文件尾部根据不同的版本长度不同，但是必须为 8 的整数
    */
   /*
    * 2024.10.15
    * v2.0 中的长度内容:
    *   groupid                 1字节
    *   tokeninfo               1字节
    *   datalen                 4字节
    *   data                    8+6字节----下个文件的编号
    */
    return 24;
}

/*
 * RESET长度是固定的
*/
int ripple_fftrail_resetlen(int compatibility)
{
    /*
     * 文件RESET根据不同的版本长度不同，但是必须为 8 的整数
    */
   /*
    * 2024.10.15
    * v2.0 中的长度内容:
    *   groupid                 1字节
    *   tokeninfo               1字节
    *   datalen                 4字节
    *   data                    8+6字节----下个文件的编号
    */
    return 24;
}


/*
 * 文件头部信息初始化
*/
void ripple_fftrail_fileinit(void* state)
{
    /* 添加文件头信息 */
    bool                found = false;
    Oid                 dbid = 0;
    char*               dbname = 0;
    ripple_file_buffer* fbuffer = NULL;
    ripple_ffsmgr_state* ffstate = NULL;
    ripple_ff_header* ffheader = NULL;              /* trail 文件头结构 */
    ripple_ff_dbmetadata* ffdbmd = NULL;            /* 数据库信息       */
    ripple_ff_fileinfo* finfo = NULL;
    ripple_fftrail_privdata* ffprivdata = NULL;
    ripple_fftrail_database_serialentry* dbserialentry = NULL;

    /* 获取初始化信息 */
    ffstate = (ripple_ffsmgr_state*)state;
    ffprivdata = (ripple_fftrail_privdata*)ffstate->fdata->ffdata;
    fbuffer = ripple_file_buffer_getbybufid(ffstate->callback.getfilebuffer(ffstate->privdata), ffstate->bufid);
    finfo = (ripple_ff_fileinfo*)fbuffer->privdata;

    ffstate->recptr = fbuffer->data + fbuffer->start;
    /* 文件开头，增加文件头和 dbmetadata 信息 */
    ffheader = ripple_ffsmgr_headinit(ffstate->compatibility, InvalidFullTransactionId, finfo->fileid);

    /* 添加lsn信息 */
    ffheader->redolsn = fbuffer->extra.chkpoint.redolsn.wal.lsn;
    ffheader->confirmlsn = fbuffer->extra.rewind.confirmlsn.wal.lsn;
    ffheader->restartlsn = fbuffer->extra.rewind.restartlsn.wal.lsn;

    ffstate->ffsmgr->ffsmgr_serial(RIPPLE_FFTRAIL_CXT_TYPE_FHEADER, ffheader, ffstate);

    if(NULL != ffheader->filename)
    {
        rfree(ffheader->filename);
    }

    rfree(ffheader);
    ffheader = NULL;

    /* 添加 dbmetadata 信息 */
    /* 查看是否存在 */
    dbid = ffstate->callback.getdboid(ffstate->privdata);
    dbname = ffstate->callback.getdbname(ffstate->privdata, dbid);

    if(NULL != ffstate->callback.setdboid)
    {
        ffstate->callback.setdboid(ffstate->privdata, dbid);
        ffstate->callback.setdboid = NULL;
    }

    ffdbmd = ripple_ffsmgr_dbmetadatainit(dbname);
    ffdbmd->oid = dbid;
    dbserialentry = hash_search(ffprivdata->databases, &dbid, HASH_ENTER, &found);
    if(false == found)
    {
        /* 向 hash 中加入数据 */
        dbserialentry->no = ffprivdata->dbnum++;
        dbserialentry->oid = dbid;
        rmemcpy1(dbserialentry->database, 0, ffdbmd->dbname, strlen(ffdbmd->dbname));
        ffprivdata->dbentrys = lappend(ffprivdata->dbentrys, dbserialentry);
    }
    ffdbmd->dbmdno = dbserialentry->no;
    ffstate->recptr = fbuffer->data + fbuffer->start;
    ffstate->ffsmgr->ffsmgr_serial(RIPPLE_FFTRAIL_CXT_TYPE_DATA, ffdbmd, ffstate);

    rfree(ffdbmd);
    return;
}

/*
 * 文件中的私有变量缓存清理
 * optype
 *  标识hash类型
 */
void ripple_fftrail_invalidprivdata(int optype, void* privdata)
{
    ListCell* lc = NULL;
    ripple_fftrail_privdata* ffprivdata = NULL;

    if(NULL == privdata)
    {
        return;
    }

    /* 清理缓存 */
    ffprivdata = (ripple_fftrail_privdata*)privdata;
    ffprivdata->dbnum = 0;
    ffprivdata->tbnum = 0;

    /* 遍历删除表 */
    foreach(lc, ffprivdata->tbentrys)
    {
        ripple_fftrail_table_serialentry* tbserialentry = NULL;
        ripple_fftrail_table_deserialentry* tbdeserialentry = NULL;
        if(optype == RIPPLE_FFSMGR_IF_OPTYPE_SERIAL)
        {
            tbserialentry = (ripple_fftrail_table_serialentry*)lfirst(lc);
            hash_search(ffprivdata->tables, &tbserialentry->key, HASH_REMOVE, NULL);
        }
        else if(optype == RIPPLE_FFSMGR_IF_OPTYPE_DESERIAL)
        {
            tbdeserialentry = (ripple_fftrail_table_deserialentry*)lfirst(lc);
            hash_search(ffprivdata->tables, &tbdeserialentry->key, HASH_REMOVE, NULL);

            /* 释放 columns */
            if(NULL != tbdeserialentry->columns)
            {
                rfree(tbdeserialentry->columns);
                tbdeserialentry->columns = NULL;
            }
        }
    }
    list_free(ffprivdata->tbentrys);
    ffprivdata->tbentrys = NULL;

    /* 遍历删除数据库 */
    foreach(lc, ffprivdata->dbentrys)
    {
        ripple_fftrail_database_serialentry* dbserialentry = NULL;
        ripple_fftrail_database_deserialentry* dbdeserialentry = NULL;

        if(optype == RIPPLE_FFSMGR_IF_OPTYPE_SERIAL)
        {
            dbserialentry = (ripple_fftrail_database_serialentry*)lfirst(lc);
            hash_search(ffprivdata->databases, &dbserialentry->oid, HASH_REMOVE, NULL);
        }
        else if(optype == RIPPLE_FFSMGR_IF_OPTYPE_DESERIAL)
        {
            dbdeserialentry = (ripple_fftrail_database_deserialentry*)lfirst(lc);
            hash_search(ffprivdata->databases, &dbdeserialentry->no, HASH_REMOVE, NULL);
        }
    }
    list_free(ffprivdata->dbentrys);
    ffprivdata->dbentrys = NULL;
}


/*
 * 文件中的私有变量缓存清理
 * optype
 *  标识hash类型
 */
static void ripple_fftrail_freeprivdata(int optype, void* privdata)
{
    ripple_fftrail_privdata* ffprivdata = NULL;
    if(NULL == privdata)
    {
        return;
    }

    ripple_fftrail_invalidprivdata(optype, privdata);
    ffprivdata = (ripple_fftrail_privdata*)privdata;

    if(NULL != ffprivdata->tables)
    {
        hash_destroy(ffprivdata->tables);
        ffprivdata->tables = NULL;
    }

    if(NULL != ffprivdata->databases)
    {
        hash_destroy(ffprivdata->databases);
        ffprivdata->databases = NULL;
    }
}


/* 数据序列化到 buffer */
bool ripple_fftrail_serial(ripple_ff_cxt_type type, void* data, void* state)
{
    return m_trailgroups[type].serialfunc(data, state);
}

/* 数据反序列化到 data */
bool ripple_fftrail_deserial(ripple_ff_cxt_type type, void** data, void* state)
{
    return m_trailgroups[type].deserialfunc(data, state);
}

/* 初始化 */
bool ripple_fftrail_init(int optype, void* state)
{
    HASHCTL hashCtl = {'\0'};
    ripple_fftrail_privdata* privdata = NULL;
    ripple_ffsmgr_state* ffstate = (ripple_ffsmgr_state*)state;

    /* 证明已经初始化过了，那么无需再次初始化 */
    if(NULL != ffstate->fdata)
    {
        return true;
    }

    /* 初始化 fdata 结构 */
    ffstate->fdata = (ripple_ffsmgr_fdata*)rmalloc1(sizeof(ripple_ffsmgr_fdata));
    if(NULL == ffstate->fdata)
    {
        elog(RLOG_WARNING, "out of memory");
        return false;
    }
    rmemset0(ffstate->fdata, 0, '\0', sizeof(ripple_ffsmgr_fdata));

    /* 初始化具体的结构 */
    privdata =(ripple_fftrail_privdata*)rmalloc1(sizeof(ripple_fftrail_privdata));
    if(NULL == privdata)
    {
        elog(RLOG_WARNING, "out of memory");
        return false;
    }
    rmemset0(privdata, 0, '\0', sizeof(ripple_fftrail_privdata));
    privdata->dbentrys = NULL;
    privdata->tbentrys = NULL;
    privdata->tables = NULL;
    privdata->databases = NULL;
    privdata->dbnum = 0;
    privdata->tbnum = 0;

    /* 内容信息 */
    ffstate->fdata->ffdata = privdata;

    /* 初始化表和数据库信息 */
    /* 表信息 */
    if(optype == RIPPLE_FFSMGR_IF_OPTYPE_SERIAL)
    {
        hashCtl.keysize = sizeof(ripple_fftrail_table_serialkey);
        hashCtl.entrysize = sizeof(ripple_fftrail_table_serialentry);
    }
    else if(optype == RIPPLE_FFSMGR_IF_OPTYPE_DESERIAL)
    {
        hashCtl.keysize = sizeof(ripple_fftrail_table_deserialkey);
        hashCtl.entrysize = sizeof(ripple_fftrail_table_deserialentry);
    }
    privdata->tables = hash_create("fftrail_privdata_tables",
                                    128,
                                    &hashCtl,
                                    HASH_ELEM | HASH_BLOBS);

    /* 数据库信息 */
    if(optype == RIPPLE_FFSMGR_IF_OPTYPE_SERIAL)
    {
        hashCtl.keysize = sizeof(Oid);
        hashCtl.entrysize = sizeof(ripple_fftrail_database_serialentry);
    }
    else if(optype == RIPPLE_FFSMGR_IF_OPTYPE_DESERIAL)
    {
        hashCtl.keysize = sizeof(uint32);
        hashCtl.entrysize = sizeof(ripple_fftrail_database_deserialentry);
    }
    privdata->databases = hash_create("fftrail_privdata_databases",
                                    128,
                                    &hashCtl,
                                    HASH_ELEM | HASH_BLOBS);

    return true;
}

/* 内容释放 */
void ripple_fftrail_free(int optype, void* state)
{
    ripple_ffsmgr_state* ffstate = (ripple_ffsmgr_state*)state;

    if(NULL == state)
    {
        return;
    }

    if(NULL == ffstate->fdata)
    {
        /* fdata 内容释放 */
        return;
    }

    /* ffdata 内容释放 */
    ripple_fftrail_freeprivdata(optype, ffstate->fdata->ffdata);
    rfree(ffstate->fdata->ffdata);
    ffstate->fdata->ffdata = NULL;

    /* ffdata2 内容释放 */
    if(RIPPLE_FFSMGR_IF_OPTYPE_DESERIAL == optype && NULL != ffstate->fdata->ffdata2)
    {
        ripple_fftrail_freeprivdata(optype, ffstate->fdata->ffdata2);
        rfree(ffstate->fdata->ffdata2);
    }
    rfree(ffstate->fdata);
    ffstate->fdata = NULL;
}

/* 获取record 中记录的 subtype */
bool ripple_fftrail_getrecordsubtype(void* state, uint8* record, uint16* subtype)
{
    ripple_ffsmgr_state* ffstate = NULL;

    ffstate = (ripple_ffsmgr_state*)state;

    /* 跳过token部分 */
    record += RIPPLE_TOKENHDRSIZE;
    *subtype = ripple_fftrail_data_getsubtypefromhead(ffstate->compatibility, record);

    return true;
}

/* 获取 record 头中记录的长度 */
uint64 ripple_fftrail_getrecordlsn(void* state, uint8* record)
{
    ripple_ffsmgr_state* ffstate = NULL;

    ffstate = (ripple_ffsmgr_state*)state;

    /* 跳过token部分 */
    record += RIPPLE_TOKENHDRSIZE;
    return ripple_fftrail_data_getorgposfromhead(ffstate->compatibility, record);
}

/* 获取真实数据基于 record 的偏移 */
uint16 ripple_fftrail_getrecorddataoffset(int compatibility)
{
    uint16 dataoffset = 0;

    /* 跳过token部分 */
    dataoffset = ripple_fftrail_data_getrecorddataoffset(compatibility);
    dataoffset += RIPPLE_TOKENHDRSIZE;

    return dataoffset;
}

/* 获取 total length */
uint64 ripple_fftrail_getrecordtotallength(void* state, uint8* record)
{
    ripple_ffsmgr_state* ffstate = NULL;

    ffstate = (ripple_ffsmgr_state*)state;

    /* 跳过token部分 */
    record += RIPPLE_TOKENHDRSIZE;
    return ripple_fftrail_data_gettotallengthfromhead(ffstate->compatibility, record);
}

/* 获取 record length */
uint16 ripple_fftrail_getrecordlength(void* state, uint8* record)
{
    ripple_ffsmgr_state* ffstate = NULL;

    ffstate = (ripple_ffsmgr_state*)state;

    /* 跳过token部分 */
    record += RIPPLE_TOKENHDRSIZE;
    return ripple_fftrail_data_getreclengthfromhead(ffstate->compatibility, record);
}

/* 设置 record length */
void ripple_fftrail_setrecordlength(void* state, uint8* record, uint16 reclength)
{
    ripple_ffsmgr_state* ffstate = NULL;

    ffstate = (ripple_ffsmgr_state*)state;

    /* 跳过token部分 */
    record += RIPPLE_TOKENHDRSIZE;

    ripple_fftrail_data_setreclengthonhead(ffstate->compatibility, record, reclength);
}


/* 获取record 中记录的 grouptype */
void ripple_fftrail_getrecordgrouptype(void* state, uint8* record, uint8* grouptype)
{
        /* 调用反序列化接口，解析数据 */
    uint8   tokenid = RIPPLE_FFTRAIL_GROUPTYPE_NOP;
    uint8   tokeninfo = RIPPLE_FFTRAIL_INFOTYPE_TOKEN;
    uint32  tokenlen = 0;

    uint8*  uptr = NULL;
    uint8*  tokendata = NULL;

    RIPPLE_UNUSED(tokendata);
    RIPPLE_UNUSED(tokeninfo);

    uptr = record;
    RIPPLE_FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)

    *grouptype = tokenid;
    return ;

}

/* 获取record 中记录的 grouptype */
bool ripple_fftrail_isrecordtransstart(void* state, uint8* record)
{
    ripple_ffsmgr_state* ffstate = NULL;

    ffstate = (ripple_ffsmgr_state*)state;
    return ripple_fftrail_data_deserail_check_transind_start(record, ffstate->compatibility);

}

/* 获取 tokenminsize */
int ripple_fftrail_gettokenminsize(int compatibility)
{
    return ripple_fftrail_data_tokenminsize(compatibility);
}