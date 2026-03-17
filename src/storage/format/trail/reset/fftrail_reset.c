#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "stmts/txnstmt.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "storage/trail/reset/fftrail_reset.h"

typedef enum TRAIL_RESET_TOKEN
{
    TRAIL_RESET_TOKEN_NEXTTRAILNO           = 0x00
} trail_reset_token;

/* 序列化reset信息 */
bool fftrail_reset_serail(void* data, void* state)
{
    uint32  reclen = 0;
    uint8*  uptr = NULL;
    ff_reset* ffreset = NULL;
    file_buffer* rfbuffer = NULL;
    ffsmgr_state* ffstate = NULL;

    /* 强制转化 */
    ffreset = (ff_reset*)data;
    ffstate = (ffsmgr_state*)state;

    /* 获取 buffer */
    rfbuffer = file_buffer_getbybufid(ffstate->callback.getfilebuffer(ffstate->privdata), ffstate->bufid);
    uptr = rfbuffer->data + rfbuffer->start;

    /* 添加数据 */
    /* 偏移出头部信息 */
    uptr += TOKENHDRSIZE;

    /* 增加 token 内容 */
    uptr = fftrail_token2buffer(TRAIL_RESET_TOKEN_NEXTTRAILNO,
                                        FFTRAIL_INFOTYPE_TOKEN,
                                        FTRAIL_TOKENDATATYPE_BIGINT,
                                        8,
                                        (uint8*)&ffreset->nexttrailno,
                                        &reclen,
                                        uptr);

    /* 增加 rectail */
    uptr = rfbuffer->data + rfbuffer->start;
    reclen += TOKENHDRSIZE;
    reclen = MAXALIGN(reclen);
    FTRAIL_GROUP2BUFFER(put,
                                FFTRAIL_GROUPTYPE_RESET,
                                FFTRAIL_INFOTYPE_GROUP,
                                reclen,
                                uptr)

    rfbuffer->start += reclen;

    return true;
}

/* 反序列化reset信息 */
bool fftrail_reset_deserail(void** data, void* state)
{
    uint8   tokenid = 0;                        /* token 标识 */
    uint8   tokeninfo = 0;                      /* token 的详情 */
    uint32  tokenlen = 0;                       /* token 长度 */

    uint8*  uptr = NULL;
    uint8*  tokendata = NULL;                   /* token 数据区 */
    ff_reset*  ffreset = NULL;
    ffsmgr_state* ffstate = NULL;

    /* 类型强转 */
    ffstate = (ffsmgr_state*)state;
    uptr = ffstate->recptr;

    /* 申请空间 */
    ffreset = (ff_reset*)rmalloc0(sizeof(ff_reset));
    if(NULL == ffreset)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(ffreset, 0, '\0', sizeof(ff_tail));
    *data = ffreset;

    ffreset->nexttrailno = 0;

    /* 获取头部标识 */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    if(FFTRAIL_GROUPTYPE_RESET != tokenid
        || FFTRAIL_INFOTYPE_GROUP != tokeninfo)
    {
        /* make gcc happy */
        uptr = tokendata;
        elog(RLOG_ERROR, "trail file data format error");
    }

    /* 解析头部数据 */
    uptr = tokendata;

    /* 获取头部标识 */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    if(TRAIL_RESET_TOKEN_NEXTTRAILNO != tokenid
        || FFTRAIL_INFOTYPE_TOKEN != tokeninfo)
    {
        /* make gcc happy */
        uptr = tokendata;
        elog(RLOG_ERROR, "trail file data format error");
    }
    uptr = tokendata;

    ffreset->nexttrailno = CONCAT(get,64bit)(&uptr);
    return true;
}
