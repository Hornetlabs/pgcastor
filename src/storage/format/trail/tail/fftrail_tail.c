#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "stmts/ripple_txnstmt.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "storage/trail/tail/ripple_fftrail_tail.h"

typedef enum RIPPLE_TRAIL_TAIL_TOKEN
{
    RIPPLE_TRAIL_TAIL_TOKEN_NEXTTRAILNO           = 0x00
} ripple_trail_tail_token;

/* 序列化尾部信息 */
bool ripple_fftrail_tail_serail(void* data, void* state)
{
    uint32  reclen = 0;
    uint8*  uptr = NULL;
    ripple_ff_tail* fftrail = NULL;
    ripple_file_buffer* rfbuffer = NULL;
    ripple_ffsmgr_state* ffstate = NULL;

    /* 强制转化 */
    fftrail = (ripple_ff_tail*)data;
    ffstate = (ripple_ffsmgr_state*)state;

    /* 获取 buffer */
    rfbuffer = ripple_file_buffer_getbybufid(ffstate->callback.getfilebuffer(ffstate->privdata), ffstate->bufid);
    uptr = rfbuffer->data + rfbuffer->start;

    /* 添加数据 */
    /* 偏移出头部信息 */
    uptr += RIPPLE_TOKENHDRSIZE;

    /* 增加 token 内容 */
    uptr = ripple_fftrail_token2buffer(RIPPLE_TRAIL_TAIL_TOKEN_NEXTTRAILNO,
                                        RIPPLE_FFTRAIL_INFOTYPE_TOKEN,
                                        RIPPLE_FTRAIL_TOKENDATATYPE_BIGINT,
                                        8,
                                        (uint8*)&fftrail->nexttrailno,
                                        &reclen,
                                        uptr);

    /* 增加 rectail */
    uptr = rfbuffer->data + rfbuffer->start;
    reclen += RIPPLE_TOKENHDRSIZE;
    reclen = RIPPLE_MAXALIGN(reclen);
    RIPPLE_FTRAIL_GROUP2BUFFER(put,
                                RIPPLE_FFTRAIL_GROUPTYPE_FTAIL,
                                RIPPLE_FFTRAIL_INFOTYPE_GROUP,
                                reclen,
                                uptr)

    rfbuffer->start += reclen;

    return true;
}

/* 反序列化尾部信息 */
bool ripple_fftrail_tail_deserail(void** data, void* state)
{
    uint8   tokenid = 0;                        /* token 标识 */
    uint8   tokeninfo = 0;                      /* token 的详情 */
    uint32  tokenlen = 0;                       /* token 长度 */

    uint8*  uptr = NULL;
    uint8*  tokendata = NULL;                   /* token 数据区 */
    ripple_ff_tail*  taildata = NULL;
    ripple_ffsmgr_state* ffstate = NULL;

    /* 类型强转 */
    ffstate = (ripple_ffsmgr_state*)state;
    uptr = ffstate->recptr;

    /* 申请空间 */
    taildata = (ripple_ff_tail*)rmalloc0(sizeof(ripple_ff_tail));
    if(NULL == taildata)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(taildata, 0, '\0', sizeof(ripple_ff_tail));
    *data = taildata;

    taildata->nexttrailno = 0;

    /* 获取头部标识 */
    RIPPLE_FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    if(RIPPLE_FFTRAIL_GROUPTYPE_FTAIL != tokenid
        || RIPPLE_FFTRAIL_INFOTYPE_GROUP != tokeninfo)
    {
        /* make gcc happy */
        uptr = tokendata;
        elog(RLOG_ERROR, "trail file data format error");
    }

    /* 解析头部数据 */
    uptr = tokendata;

    /* 获取头部标识 */
    RIPPLE_FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    if(RIPPLE_TRAIL_TAIL_TOKEN_NEXTTRAILNO != tokenid
        || RIPPLE_FFTRAIL_INFOTYPE_TOKEN != tokeninfo)
    {
        /* make gcc happy */
        uptr = tokendata;
        elog(RLOG_ERROR, "trail file data format error");
    }
    uptr = tokendata;

    taildata->nexttrailno = RIPPLE_CONCAT(get,64bit)(&uptr);
    return true;
}
