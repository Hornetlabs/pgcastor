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
#include "storage/trail/data/fftrail_data.h"
#include "storage/trail/data/fftrail_txnddl.h"

/* ddl 语句序列化 */
bool fftrail_txnddl_serial(void* data, void* state)
{
    /*
     * Record格式为:
     *  GroupToken
     *  RecHead
     *  RecData
     *      type            2 字节
     *      subtype         2 字节
     *      length          4 字节
     *      stmtdata        长度
     *  RecTail
     */
    int hdrlen = 0;
    uint32 tlen = 0;

    uint8* uptr = NULL;
    txnstmt* rstmt = NULL;                      /* 需要写入 trail 文件的内容 */
    txnstmt_ddl* ddlstmt = NULL;                /* ddl 语句内容 */
    ff_txndata*  txndata = NULL;
    file_buffer* fbuffer = NULL;
    ffsmgr_state* ffstate = NULL;            /* state 数据信息 */

    txndata = (ff_txndata*)data;
    rstmt = (txnstmt*)txndata->data;
    ffstate = (ffsmgr_state*)state;

    /* 获取 ddl 信息 */
    ddlstmt = (txnstmt_ddl*)rstmt->stmt;

    /* 检验并切换block */
    fftrail_serialpreshiftblock(state);
    if(FFSMGR_STATUS_SHIFTFILE == ffstate->status)
    {
        ffstate->status = FFSMGR_STATUS_USED;
    }

    /* 将 ddl 写入到trail文件中 */
    fbuffer = file_buffer_getbybufid(ffstate->callback.getfilebuffer(ffstate->privdata), ffstate->bufid);
    ffstate->recptr = fbuffer->data + fbuffer->start;

    /* 计算长度 */
    txndata->header.totallength = rstmt->len;           /* 语句的长度             */
    txndata->header.totallength += (2 + 2 + 4);         /* 语句的内容所占的长度     */

    /* 设置record头信息 */
    txndata->header.reccount = 1;
    txndata->header.reclength = 0;
    txndata->header.subtype = FF_DATA_TYPE_DDL_STMT;
    txndata->header.formattype = FF_DATA_FORMATTYPE_WAL;
    txndata->header.dbmdno = 0;
    txndata->header.tbmdno = 0;
    txndata->header.orgpos = rstmt->extra0.wal.lsn;

    /* 跳过record token 和 头长度 */
    /* 增加偏移 */
    hdrlen = TOKENHDRSIZE;
    hdrlen += fftrail_data_headlen(ffstate->compatibility);
    fbuffer->start += hdrlen;

    /* 添加内容 */
    /* ddl 类型 */
    fftrail_data_data2buffer(&txndata->header,
                                    ffstate,
                                    &fbuffer,
                                    FTRAIL_TOKENDATATYPE_SMALLINT,
                                    2,
                                    (uint8*)&ddlstmt->type);

    /* ddl 子类型 */
    fftrail_data_data2buffer(&txndata->header,
                                    ffstate,
                                    &fbuffer,
                                    FTRAIL_TOKENDATATYPE_SMALLINT,
                                    2,
                                    (uint8*)&ddlstmt->subtype);

    /* ddl 字符串长度 */
    fftrail_data_data2buffer(&txndata->header,
                                    ffstate,
                                    &fbuffer,
                                    FTRAIL_TOKENDATATYPE_INT,
                                    4,
                                    (uint8*)&rstmt->len);

    /* ddl 字符串内容 */
    fftrail_data_data2buffer(&txndata->header,
                                    ffstate,
                                    &fbuffer,
                                    FTRAIL_TOKENDATATYPE_STR,
                                    rstmt->len,
                                    (uint8*)ddlstmt->ddlstmt);

    /* 填充头部信息 */
    if(FFSMGR_STATUS_SHIFTFILE == ffstate->status)
    {
        ffstate->status = FFSMGR_STATUS_USED;
    }

    /* 写在 Record token 中的长度 */
    tlen = txndata->header.reclength;               /* 数据长度 */
    tlen += hdrlen;                                 /* 头部长度 */

    /* 增加rectail */
    uptr = fbuffer->data + fbuffer->start;
    FTRAIL_GROUP2BUFFER(put,
                                TRAIL_TOKENDATA_RECTAIL,
                                FFTRAIL_INFOTYPE_TOKEN,
                                0,
                                uptr)

    /* 添加尾部长度 */
    tlen += TOKENHDRSIZE;
    fbuffer->start += TOKENHDRSIZE;

    /* 字节对齐 */
    tlen = MAXALIGN(tlen);
    fbuffer->start = MAXALIGN(fbuffer->start);

    /* 写头部数据 */
    /* 增加GROUP信息 */
    FTRAIL_GROUP2BUFFER(put,
                                FFTRAIL_GROUPTYPE_DATA,
                                FFTRAIL_INFOTYPE_GROUP,
                                tlen,
                                ffstate->recptr)

    /* 增加头部信息 */
    fftrail_data_hdrserail(&txndata->header, ffstate);
    ffstate->recptr = NULL;

    return true;
}

/* ddl信息反序列化 */
bool fftrail_txnddl_deserial(void** data, void* state)
{
    uint8   tokenid = 0;                        /* token 标识 */
    uint8   tokeninfo = 0;                      /* token 的详情 */
    uint32  recoffset = 0;
    uint32  dataoffset = 0;
    uint16  subtype = FF_DATA_TYPE_NOP;
    uint32  tokenlen = 0;                       /* token 长度 */

    uint8*  uptr = NULL;
    txnstmt_ddl* ddlstmt = NULL;
    uint8*  tokendata = NULL;                   /* token 数据区 */
    ff_txndata*  txndata = NULL;
    ffsmgr_state* ffstate = NULL;
    txnstmt* rstmt = NULL;

    /* 类型强转 */
    ffstate = (ffsmgr_state*)state;
    uptr = ffstate->recptr;

    /* 申请空间 */
    txndata = (ff_txndata*)rmalloc0(sizeof(ff_txndata));
    if(NULL == txndata)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(txndata, 0, '\0', sizeof(ff_txndata));
    *data = txndata;

    /* 申请空间 */
    rstmt = (txnstmt*)rmalloc0(sizeof(txnstmt));
    if(NULL == rstmt)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(rstmt, 0, '\0', sizeof(txnstmt));
    txndata->data = (void*)rstmt;

    /* 申请ddl data空间 */
    ddlstmt = (txnstmt_ddl*)rmalloc0(sizeof(txnstmt_ddl));
    if(NULL == ddlstmt)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(ddlstmt, 0, '\0', sizeof(txnstmt_ddl));
    rstmt->type = TXNSTMT_TYPE_DDL;
    rstmt->stmt = (void*)ddlstmt;
    rstmt->len = txndata->header.totallength;
    rstmt->len -= (2 + 2 + 4);

    /* 获取头部标识 */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    if(FFTRAIL_GROUPTYPE_DATA != tokenid
        || FFTRAIL_INFOTYPE_GROUP != tokeninfo)
    {
        /* make gcc happy */
        uptr = tokendata;
        elog(RLOG_ERROR, "trail file data format error");
    }
    recoffset = TOKENHDRSIZE;

    /* 解析头部数据 */
    uptr = ffstate->recptr;
    ffstate->recptr += recoffset;
    fftrail_data_hdrdeserail(&txndata->header, ffstate);

    /* rstmt添加orgpos */ 
    rstmt->extra0.wal.lsn = txndata->header.orgpos;

    /* 保留信息，因为在后续的处理逻辑中，这些数据可能会被清理 */
    subtype = txndata->header.subtype;

    /* 重新指向头部 */
    ffstate->recptr = uptr;
    recoffset += (uint16)fftrail_data_headlen(ffstate->compatibility);

    /* 
     * 解析真实数据
     *  1、查看是否为空的 record
     *  2、数据拼装
     */
    /* 获取ddl类型 */
    if(false  == fftrail_data_buffer2data(&txndata->header,
                                                ffstate,
                                                &recoffset,
                                                &dataoffset,
                                                FTRAIL_TOKENDATATYPE_SMALLINT,
                                                2,
                                                (uint8*)&ddlstmt->type))
    {
        return false;
    }

    /* 获取ddl的子类型 */
    if(false  == fftrail_data_buffer2data(&txndata->header,
                                                ffstate,
                                                &recoffset,
                                                &dataoffset,
                                                FTRAIL_TOKENDATATYPE_SMALLINT,
                                                2,
                                                (uint8*)&ddlstmt->subtype))
    {
        return false;
    }
    /* 获取ddl 字符串长度 */
    if(false  == fftrail_data_buffer2data(&txndata->header,
                                                ffstate,
                                                &recoffset,
                                                &dataoffset,
                                                FTRAIL_TOKENDATATYPE_INT,
                                                4,
                                                (uint8*)&rstmt->len))
    {
        return false;
    }

    ddlstmt->ddlstmt = rmalloc0(rstmt->len + 1);
    if(NULL == ddlstmt->ddlstmt)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(ddlstmt->ddlstmt, 0, '\0', rstmt->len + 1);
    /* 获取ddl 字符串内容 */
    if(false  == fftrail_data_buffer2data(&txndata->header,
                                                ffstate,
                                                &recoffset,
                                                &dataoffset,
                                                FTRAIL_TOKENDATATYPE_STR,
                                                rstmt->len,
                                                (uint8*)ddlstmt->ddlstmt))
    {
        return false;
    }
    
    rstmt->stmt = (void*)ddlstmt;
    txndata->data = rstmt;

    /* 重设，因为在切换block或file时，subtype的值为:FF_DATA_SUBTYPE_REC_CONTRECORD */
    txndata->header.subtype = subtype;
    return true;
}
