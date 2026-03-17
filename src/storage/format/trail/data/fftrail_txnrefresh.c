#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "stmts/txnstmt.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "storage/trail/data/fftrail_data.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "storage/trail/data/fftrail_txnrefresh.h"

/* refresh 语句序列化 */
bool fftrail_txnrefresh_serial(void* data, void* state)
{
    /*
     * Record格式为:
     *  GroupToken
     *  RecHead
     *  RecData
     *      长度            2 字节
     *      schema          长度
     *      长度            2 字节
     *      table           长度
     *  RecTail
     */
    int hdrlen = 0;
    int len = 0;
    uint32 tlen = 0;
    uint8* uptr = NULL;
    txnstmt* rstmt = NULL;                      /* 需要写入 trail 文件的内容 */
    refresh_table* table = NULL;
    refresh_tables* refretables = NULL;                /* refresh 语句内容 */
    ff_txndata*  txndata = NULL;
    file_buffer* fbuffer = NULL;
    ffsmgr_state* ffstate = NULL;            /* state 数据信息 */

    txndata = (ff_txndata*)data;
    rstmt = (txnstmt*)txndata->data;
    ffstate = (ffsmgr_state*)state;

    /* 获取 refresh 信息 */
    refretables = (refresh_tables*)rstmt->stmt;

    /* 检验并切换block */
    fftrail_serialpreshiftblock(state);
    if(FFSMGR_STATUS_SHIFTFILE == ffstate->status)
    {
        ffstate->status = FFSMGR_STATUS_USED;
    }

    /* 将 refresh 写入到trail文件中 */
    fbuffer = file_buffer_getbybufid(ffstate->callback.getfilebuffer(ffstate->privdata), ffstate->bufid);
    ffstate->recptr = fbuffer->data + fbuffer->start;

    /* 计算长度 */
    txndata->header.totallength += refretables->cnt * (2 + 2);

    /* 设置record头信息 */
    txndata->header.reccount = 1;
    txndata->header.reclength = 0;
    txndata->header.subtype = FF_DATA_TYPE_REFRESH;
    txndata->header.formattype = FF_DATA_FORMATTYPE_WAL;
    txndata->header.dbmdno = 0;
    txndata->header.tbmdno = 0;
    txndata->header.orgpos = rstmt->extra0.wal.lsn;

    /* 跳过record token 和 头长度 */
    /* 增加偏移 */
    hdrlen = TOKENHDRSIZE;
    hdrlen += fftrail_data_headlen(ffstate->compatibility);
    fbuffer->start += hdrlen;

    table = refretables->tables;

    while (NULL != table)
    {
        len = strlen(table->schema);
        txndata->header.totallength += len;
        /* 添加内容 */
        /* refresh schema长度 */
        fftrail_data_data2buffer(&txndata->header,
                                        ffstate,
                                        &fbuffer,
                                        FTRAIL_TOKENDATATYPE_SMALLINT,
                                        2,
                                        (uint8*)&len);

        /* refresh schema内容 */
        fftrail_data_data2buffer(&txndata->header,
                                        ffstate,
                                        &fbuffer,
                                        FTRAIL_TOKENDATATYPE_STR,
                                        len,
                                        (uint8*)table->schema);

        len = strlen(table->table);
        txndata->header.totallength += len;
        /* refresh table长度 */
        fftrail_data_data2buffer(&txndata->header,
                                        ffstate,
                                        &fbuffer,
                                        FTRAIL_TOKENDATATYPE_SMALLINT,
                                        2,
                                        (uint8*)&len);

        /* refresh 字符串内容 */
        fftrail_data_data2buffer(&txndata->header,
                                        ffstate,
                                        &fbuffer,
                                        FTRAIL_TOKENDATATYPE_STR,
                                        len,
                                        (uint8*)table->table);
        table = table->next;
    }

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

    /* 为了能够正确记录切换文件,所以设置segno = segno + 1 */
    fbuffer = file_buffer_getbybufid(ffstate->callback.getfilebuffer(ffstate->privdata), ffstate->bufid);
    fbuffer->extra.chkpoint.segno.trail.fileid++;

    /* refresh 事务接换文件保证后续解析正确 */
    if(false == fftrail_serialshiffile(state))
    {
        return false;
    }

    ffstate->recptr = NULL;

    return true;
}

/* ddl信息反序列化 */
bool fftrail_txnrefresh_deserial(void** data, void* state)
{
    uint8   tokenid = 0;                        /* token 标识 */
    uint8   tokeninfo = 0;                      /* token 的详情 */
    uint32  recoffset = 0;
    uint32  dataoffset = 0;
    uint16  subtype = FF_DATA_TYPE_NOP;
    uint32  len = 0;
    uint32  tokenlen = 0;                       /* token 长度 */
    uint64  totallen = 0;

    uint8*  uptr = NULL;
    uint8*  tokendata = NULL;                   /* token 数据区 */
    refresh_table* table = NULL;
    refresh_tables* refreshtables = NULL;
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
    refreshtables = refresh_tables_init();
    rstmt->type = TXNSTMT_TYPE_REFRESH;
    rstmt->stmt = (void*)refreshtables;
    rstmt->len = txndata->header.totallength;

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

    totallen = txndata->header.totallength;

    /* 
     * 解析真实数据
     *  1、查看是否为空的 record
     *  2、数据拼装
     */
    while (0 < totallen)
    {
        table = refresh_table_init();
        /* 获取ddl类型 */
        if(false  == fftrail_data_buffer2data(&txndata->header,
                                                    ffstate,
                                                    &recoffset,
                                                    &dataoffset,
                                                    FTRAIL_TOKENDATATYPE_SMALLINT,
                                                    2,
                                                    (uint8*)&len))
        {
            return false;
        }
        table->schema = rmalloc0(len + 1);
        if(NULL == table->schema)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(table->schema, 0, '\0', len + 1);

        /* 获取ddl的子类型 */
        if(false  == fftrail_data_buffer2data(&txndata->header,
                                                    ffstate,
                                                    &recoffset,
                                                    &dataoffset,
                                                    FTRAIL_TOKENDATATYPE_STR,
                                                    len,
                                                    (uint8*)table->schema))
        {
            return false;
        }
        totallen -=(2 + len);
        /* 获取ddl 字符串长度 */
        if(false  == fftrail_data_buffer2data(&txndata->header,
                                                    ffstate,
                                                    &recoffset,
                                                    &dataoffset,
                                                    FTRAIL_TOKENDATATYPE_SMALLINT,
                                                    2,
                                                    (uint8*)&len))
        {
            return false;
        }

        table->table = rmalloc0(len + 1);
        if(NULL == table->table)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(table->table, 0, '\0', len + 1);

        /* 获取ddl 字符串内容 */
        if(false  == fftrail_data_buffer2data(&txndata->header,
                                                    ffstate,
                                                    &recoffset,
                                                    &dataoffset,
                                                    FTRAIL_TOKENDATATYPE_STR,
                                                    len,
                                                    (uint8*)table->table))
        {
            return false;
        }
        totallen -=(2 + len);

        refresh_tables_add(table, refreshtables);
    }
    rstmt->len -= (refreshtables->cnt * 4);


    /* 重设，因为在切换block或file时，subtype的值为:FF_DATA_SUBTYPE_REC_CONTRECORD */
    txndata->header.subtype = subtype;
    return true;
}
