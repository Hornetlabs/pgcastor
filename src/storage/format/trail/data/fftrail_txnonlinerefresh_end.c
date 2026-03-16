#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/ripple_uuid.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "stmts/ripple_txnstmt.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "storage/trail/data/ripple_fftrail_data.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "storage/trail/data/ripple_fftrail_txnonlinerefresh_end.h"
#include "stmts/ripple_txnstmt_onlinerefresh.h"

/* online refresh 语句序列化 */
bool ripple_fftrail_txnonlinerefresh_end_serial(void* data, void* state)
{
    /*
     * Record格式为:
     *  GroupToken
     *  RecHead
     *  RecData
     *      uuid            16 字节
     *  RecTail
     */
    int hdrlen = 0;
    uint32 tlen = 0;

    uint8* uptr = NULL;
    ripple_txnstmt* rstmt = NULL;                      /* 需要写入 trail 文件的内容 */
    ripple_ff_txndata*  txndata = NULL;
    ripple_file_buffer* fbuffer = NULL;
    ripple_ffsmgr_state* ffstate = NULL;            /* state 数据信息 */
    char *uuid = NULL;

    txndata = (ripple_ff_txndata*)data;
    rstmt = (ripple_txnstmt*)txndata->data;
    ffstate = (ripple_ffsmgr_state*)state;

    /* 获取 uuid */
    uuid = (char *)rstmt->stmt;

    /* 检验并切换block */
    ripple_fftrail_serialpreshiftblock(state);
    if(RIPPLE_FFSMGR_STATUS_SHIFTFILE == ffstate->status)
    {
        ffstate->status = RIPPLE_FFSMGR_STATUS_USED;
    }

    /* 将 refresh 写入到trail文件中 */
    fbuffer = ripple_file_buffer_getbybufid(ffstate->callback.getfilebuffer(ffstate->privdata), ffstate->bufid);
    ffstate->recptr = fbuffer->data + fbuffer->start;

    /* 计算长度 */
    txndata->header.totallength = 16;

    /* 设置record头信息 */
    txndata->header.reccount = 1;
    txndata->header.reclength = 0;
    txndata->header.subtype = RIPPLE_FF_DATA_TYPE_ONLINE_REFRESH_END;
    txndata->header.formattype = RIPPLE_FF_DATA_FORMATTYPE_WAL;
    txndata->header.dbmdno = 0;
    txndata->header.tbmdno = 0;
    txndata->header.orgpos = rstmt->extra0.wal.lsn;

    /* 跳过record token 和 头长度 */
    /* 增加偏移 */
    hdrlen = RIPPLE_TOKENHDRSIZE;
    hdrlen += ripple_fftrail_data_headlen(ffstate->compatibility);
    fbuffer->start += hdrlen;

    /* online refresh uuid */
    ripple_fftrail_data_data2buffer(&txndata->header,
                                    ffstate,
                                    &fbuffer,
                                    RIPPLE_FTRAIL_TOKENDATATYPE_STR,
                                    16,
                                    (uint8*)uuid);

    /* 填充头部信息 */
    if(RIPPLE_FFSMGR_STATUS_SHIFTFILE == ffstate->status)
    {
        ffstate->status = RIPPLE_FFSMGR_STATUS_USED;
    }

    /* 写在 Record token 中的长度 */
    tlen = txndata->header.reclength;               /* 数据长度 */
    tlen += hdrlen;                                 /* 头部长度 */

    /* 增加rectail */
    uptr = fbuffer->data + fbuffer->start;
    RIPPLE_FTRAIL_GROUP2BUFFER(put,
                                RIPPLE_TRAIL_TOKENDATA_RECTAIL,
                                RIPPLE_FFTRAIL_INFOTYPE_TOKEN,
                                0,
                                uptr)

    /* 添加尾部长度 */
    tlen += RIPPLE_TOKENHDRSIZE;
    fbuffer->start += RIPPLE_TOKENHDRSIZE;

    /* 字节对齐 */
    tlen = RIPPLE_MAXALIGN(tlen);
    fbuffer->start = RIPPLE_MAXALIGN(fbuffer->start);

    /* 写头部数据 */
    /* 增加GROUP信息 */
    RIPPLE_FTRAIL_GROUP2BUFFER(put,
                                RIPPLE_FFTRAIL_GROUPTYPE_DATA,
                                RIPPLE_FFTRAIL_INFOTYPE_GROUP,
                                tlen,
                                ffstate->recptr)

    /* 增加头部信息 */
    ripple_fftrail_data_hdrserail(&txndata->header, ffstate);

    return true;
}

/* online refresh 信息反序列化 */
bool ripple_fftrail_txnonlinerefresh_end_deserial(void** data, void* state)
{
    uint8   tokenid = 0;                        /* token 标识 */
    uint8   tokeninfo = 0;                      /* token 的详情 */
    uint32  recoffset = 0;
    uint32  dataoffset = 0;
    uint16  subtype = RIPPLE_FF_DATA_TYPE_NOP;
    uint32  tokenlen = 0;                       /* token 长度 */
    uint64  totallen = 0;

    uint8*  uptr = NULL;
    uint8*  tokendata = NULL;                   /* token 数据区 */
    ripple_ff_txndata*  txndata = NULL;
    ripple_ffsmgr_state* ffstate = NULL;
    ripple_txnstmt* rstmt = NULL;
    char *uuid = NULL;

    /* 类型强转 */
    ffstate = (ripple_ffsmgr_state*)state;
    uptr = ffstate->recptr;

    /* 申请空间 */
    txndata = (ripple_ff_txndata*)rmalloc0(sizeof(ripple_ff_txndata));
    if(NULL == txndata)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(txndata, 0, '\0', sizeof(ripple_ff_txndata));
    *data = txndata;

    /* 申请空间 */
    rstmt = (ripple_txnstmt*)rmalloc0(sizeof(ripple_txnstmt));
    if(NULL == rstmt)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(rstmt, 0, '\0', sizeof(ripple_txnstmt));
    txndata->data = (void*)rstmt;

    /* 申请uuid空间 */
    uuid = rmalloc0(16);
    if (!uuid)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(uuid, 0, 0, 16);
    rstmt->type = RIPPLE_TXNSTMT_TYPE_ONLINEREFRESH_END;
    rstmt->stmt = (void*)uuid;
    rstmt->len = txndata->header.totallength;

    /* 获取头部标识 */
    RIPPLE_FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    if(RIPPLE_FFTRAIL_GROUPTYPE_DATA != tokenid
        || RIPPLE_FFTRAIL_INFOTYPE_GROUP != tokeninfo)
    {
        /* make gcc happy */
        uptr = tokendata;
        elog(RLOG_ERROR, "trail file data format error");
    }
    recoffset = RIPPLE_TOKENHDRSIZE;

    /* 解析头部数据 */
    uptr = ffstate->recptr;
    ffstate->recptr += recoffset;
    ripple_fftrail_data_hdrdeserail(&txndata->header, ffstate);

    /* rstmt添加orgpos */ 
    rstmt->extra0.wal.lsn = txndata->header.orgpos;

    /* 保留信息，因为在后续的处理逻辑中，这些数据可能会被清理 */
    subtype = txndata->header.subtype;

    /* 重新指向头部 */
    ffstate->recptr = uptr;
    recoffset += (uint16)ripple_fftrail_data_headlen(ffstate->compatibility);

    totallen = txndata->header.totallength;

    /* 获取uuid */
    if(false  == ripple_fftrail_data_buffer2data(&txndata->header,
                                                    ffstate,
                                                    &recoffset,
                                                    &dataoffset,
                                                    RIPPLE_FTRAIL_TOKENDATATYPE_STR,
                                                    16,
                                                    (uint8 *)uuid))
    {
        return false;
    }
    totallen -= 16;
    rstmt->len = 16;

    /* 重设，因为在切换block或file时，subtype的值为:RIPPLE_FF_DATA_SUBTYPE_REC_CONTRECORD */
    txndata->header.subtype = subtype;
    return true;
}
