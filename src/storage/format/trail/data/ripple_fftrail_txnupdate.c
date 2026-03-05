#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "stmts/ripple_txnstmt.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "storage/trail/data/ripple_fftrail_data.h"
#include "storage/trail/data/ripple_fftrail_tbmetadata.h"
#include "storage/trail/data/ripple_fftrail_txn.h"
#include "storage/trail/data/ripple_fftrail_txnupdate.h"

/* update 语句序列化 */
bool ripple_fftrail_txnupdate_serial(void* data, void* state)
{
    /*
     * Record格式为:
     *  GroupToken
     *  RecHead
     *  RecData
     *      colid           2 字节
     *      flag            2 字节
     *      length          4 字节
     *      stmtdata        长度
     *  RecTail
     */
    int hdrlen = 0;
    uint32 dbmdno = 0;
    uint32 tbmdno = 0;
    uint32 tlen = 0;
    int colid = 0;
    int i = 0;

    uint8* tmpuptr = NULL;
    uint8* uptr = NULL;
    ripple_txnstmt* rstmt = NULL;                      /* 需要写入 trail 文件的内容 */
    uint16 flag = 0;
    ripple_ff_txndata*  txndata = NULL;
    ripple_file_buffer* fbuffer = NULL;
    ripple_ffsmgr_state* ffstate = NULL;            /* state 数据信息 */
    xk_pg_parser_translog_tbcol_values* colvalues = NULL;
    xk_pg_parser_translog_tbcol_value col;

    txndata = (ripple_ff_txndata*)data;
    rstmt = (ripple_txnstmt*)txndata->data;
    ffstate = (ripple_ffsmgr_state*)state;

    colvalues = (xk_pg_parser_translog_tbcol_values*)rstmt->stmt;

ripple_fftrail_txnupdate_serial_retry:
    do
    {
        /* 将表写入到 trail 文件中 */
        ripple_fftrail_tbmetadata_serial(false,
                                         rstmt->database,//colvalues->m_base.m_dbid,
                                         colvalues->m_relid,
                                         txndata->header.transid,
                                         &dbmdno,
                                         &tbmdno,
                                         state);
    } while(RIPPLE_FFSMGR_STATUS_SHIFTFILE == ffstate->status);         /* 内容发生了切换，那么需要在写一次 */

    /* 设置状态 */
    ffstate->status = RIPPLE_FFSMGR_STATUS_USED;

    /* 检验并切换block */
    ripple_fftrail_serialpreshiftblock(state);
    if(RIPPLE_FFSMGR_STATUS_SHIFTFILE == ffstate->status)
    {
        /* 发生了文件切换，那么此时，应该重新写入 table 信息 */
        ffstate->status = RIPPLE_FFSMGR_STATUS_USED;
        goto ripple_fftrail_txnupdate_serial_retry;
    }

    /* 获取 buffer 信息,并设置写入的位置 */
    fbuffer = ripple_file_buffer_getbybufid(ffstate->callback.getfilebuffer(ffstate->privdata), ffstate->bufid);
    tmpuptr = ffstate->recptr = fbuffer->data + fbuffer->start;

    /* 长度计算，换算该条记录的总长度 */
    /* colid(2) + flag(2) + (length 4) + data*/
    txndata->header.totallength = rstmt->len;
    /* update保存old和new值长度×2 */
    txndata->header.totallength += (colvalues->m_valueCnt * 8 * 2);

    /* 设置record头信息， 关注 dbmdno 和 tbmdno */
    txndata->header.reccount = 1;
    txndata->header.reclength = 0;
    txndata->header.subtype = RIPPLE_FF_DATA_TYPE_DML_UPDATE;
    txndata->header.formattype = RIPPLE_FF_DATA_FORMATTYPE_WAL;
    txndata->header.dbmdno = dbmdno;
    txndata->header.tbmdno = tbmdno;
    txndata->header.orgpos = rstmt->extra0.wal.lsn;

    /* 跳过record token 和 头长度 */
    /* 增加偏移 */
    hdrlen = RIPPLE_TOKENHDRSIZE;
    hdrlen += ripple_fftrail_data_headlen(ffstate->compatibility);
    fbuffer->start += hdrlen;

    tmpuptr += hdrlen;

    /* 先写入new_value所有列信息 */
    for (i = 0; i < colvalues->m_valueCnt; i++)
    {
        col = colvalues->m_new_values[i];
        colid = i + 1;
        flag = RIPPLE_FF_COL_IS_NORMAL;
        
        switch (col.m_info)
        {
            case INFO_COL_IS_NULL:
                flag = RIPPLE_FF_COL_IS_NULL;
                break;
            case INFO_COL_MAY_NULL:
                flag = RIPPLE_FF_COL_IS_MISSING;
                break;
            case INFO_COL_IS_DROPED:
                flag = RIPPLE_FF_COL_IS_DROPED;
                break;
            case INFO_COL_IS_CUSTOM:
                flag = RIPPLE_FF_COL_IS_CUSTOM;
                break;
            default:
                break;
        }

        /* colid(2) */
        ripple_fftrail_data_data2buffer(&txndata->header,
                                        ffstate,
                                        &fbuffer,
                                        RIPPLE_FTRAIL_TOKENDATATYPE_SMALLINT,
                                        2,
                                        (uint8*)&colid);



        /* flag(2) */
        ripple_fftrail_data_data2buffer(&txndata->header,
                                        ffstate,
                                        &fbuffer,
                                        RIPPLE_FTRAIL_TOKENDATATYPE_SMALLINT,
                                        2,
                                        (uint8*)&flag);

        /* (length 4) */
        ripple_fftrail_data_data2buffer(&txndata->header,
                                        ffstate,
                                        &fbuffer,
                                        RIPPLE_FTRAIL_TOKENDATATYPE_INT,
                                        4,
                                        (uint8*)&col.m_valueLen);

        /* 写入列值 */
        if (0 != col.m_valueLen)
        {
            /* data* */
            ripple_fftrail_data_data2buffer(&txndata->header,
                                            ffstate,
                                            &fbuffer,
                                            RIPPLE_FTRAIL_TOKENDATATYPE_STR,
                                            col.m_valueLen,
                                            (uint8*)col.m_value);
        }
    }
    
    /* 再写入old_value所有列信息*/
    for (i = 0; i < colvalues->m_valueCnt; i++)
    {
        col = colvalues->m_old_values[i];
        colid = i + 1;
        flag = RIPPLE_FF_COL_IS_NORMAL;

        switch (col.m_info)
        {
            case INFO_COL_IS_NULL:
                flag = RIPPLE_FF_COL_IS_NULL;
                break;
            case INFO_COL_MAY_NULL:
                flag = RIPPLE_FF_COL_IS_MISSING;
                break;
            case INFO_COL_IS_DROPED:
                flag = RIPPLE_FF_COL_IS_DROPED;
                break;
            case INFO_COL_IS_CUSTOM:
                flag = RIPPLE_FF_COL_IS_CUSTOM;
                break;
            default:
                break;
        }

        /* colid(2) */
        ripple_fftrail_data_data2buffer(&txndata->header,
                                        ffstate,
                                        &fbuffer,
                                        RIPPLE_FTRAIL_TOKENDATATYPE_SMALLINT,
                                        2,
                                        (uint8*)&colid);

        /* flag(2) */
        ripple_fftrail_data_data2buffer(&txndata->header,
                                        ffstate,
                                        &fbuffer,
                                        RIPPLE_FTRAIL_TOKENDATATYPE_SMALLINT,
                                        2,
                                        (uint8*)&flag);

        /* (length 4) */
        ripple_fftrail_data_data2buffer(&txndata->header,
                                        ffstate,
                                        &fbuffer,
                                        RIPPLE_FTRAIL_TOKENDATATYPE_INT,
                                        4,
                                        (uint8*)&col.m_valueLen);

        /* 写入列值 */
        if (0 != col.m_valueLen)
        {
            /* data* */
            ripple_fftrail_data_data2buffer(&txndata->header,
                                            ffstate,
                                            &fbuffer,
                                            RIPPLE_FTRAIL_TOKENDATATYPE_STR,
                                            col.m_valueLen,
                                            (uint8*)col.m_value);
        }
    }

    /* 重置状态 */
    if(RIPPLE_FFSMGR_STATUS_SHIFTFILE == ffstate->status)
    {
        /* 发生了文件切换，那么此时，应该重新写入 table 信息 */
        ffstate->status = RIPPLE_FFSMGR_STATUS_USED;
    }
    /*
     * 1、写尾部数据
     * 2、写头部数据
     */
    /* 增加 rectail 尾部数据 */
    uptr = fbuffer->data + fbuffer->start;
    RIPPLE_FTRAIL_GROUP2BUFFER(put,
                                RIPPLE_TRAIL_TOKENDATA_RECTAIL,
                                RIPPLE_FFTRAIL_INFOTYPE_TOKEN,
                                0,
                                uptr)

    /* 估算总长度 */
    tlen = hdrlen;                              /* record 的头部数据 */
    tlen += txndata->header.reclength;          /* record data 内容数据 */
    tlen += RIPPLE_TOKENHDRSIZE;                /* record 尾部数据 */

    /* 字节对齐 */
    tlen = RIPPLE_MAXALIGN(tlen);

    /* 增加record尾部长度 */
    fbuffer->start += RIPPLE_TOKENHDRSIZE;

    /* 对齐 */
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
    ffstate->recptr = NULL;
    return true;
}

/* update信息反序列化 */
bool ripple_fftrail_txnupdate_deserial(void** data, void* state)
{
    bool    found = false;
    uint8   tokenid = 0;                        /* token 标识 */
    uint8   tokeninfo = 0;                      /* token 的详情 */
    uint32  recoffset = 0;
    uint32  dataoffset = 0;                     /* 基于 数据 的偏移，用于计算当前 record 数据部分的剩余空间 */
    uint16  subtype = RIPPLE_FF_DATA_TYPE_NOP;
    uint16  colid;
    uint16  flag = 0;
    uint32  mlen = 0;
    uint32  tokenlen = 0;                       /* token 长度 */
    uint64  totallen = 0;

    uint8*  uptr = NULL;
    uint8*  tokendata = NULL;                   /* token 数据区 */
    ripple_ff_txndata*  txndata = NULL;
    ripple_ffsmgr_state* ffstate = NULL;
    ripple_txnstmt* rstmt = NULL;
    ripple_fftrail_privdata* privdata = NULL; 
    ripple_fftrail_table_deserialentry* tbdeserialentry = NULL;
    ripple_fftrail_database_deserialentry* dbdeserialentry = NULL;
    xk_pg_parser_translog_tbcol_values* colvalues = NULL;
    xk_pg_parser_translog_tbcol_value *col;
    xk_pg_parser_translog_tbcolbase tbcolbase;

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

    /* 申请update data空间 */
    colvalues = (xk_pg_parser_translog_tbcol_values*)rmalloc0(sizeof(xk_pg_parser_translog_tbcol_values));
    if(NULL == colvalues)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(colvalues, 0, '\0', sizeof(xk_pg_parser_translog_tbcol_values));
    rstmt->stmt = (void*)colvalues;
    rstmt->type = RIPPLE_TXNSTMT_TYPE_DML;

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

    /* 获取表信息 */
    privdata = (ripple_fftrail_privdata*)ffstate->fdata->ffdata;
    tbdeserialentry = hash_search(privdata->tables, &txndata->header.tbmdno, HASH_FIND, &found);
    if (!found)
    {
        elog(RLOG_ERROR, "not found table,%lu.%lu, xid:%lu.%u,crc:%u",
                            privdata->tbnum,
                            txndata->header.tbmdno,
                            txndata->header.transid,
                            txndata->header.transind,
                            txndata->header.crc32);
        return false;
    }

    /* 获取数据库信息 */
    dbdeserialentry = hash_search(privdata->databases, &txndata->header.dbmdno, HASH_FIND, &found);
    if (!found)
    {
        elog(RLOG_ERROR, "not found tdatabase %u", tbdeserialentry->dbno);
        return false;
    }

    rstmt->database = dbdeserialentry->oid;
    tbcolbase.m_dmltype = XK_PG_PARSER_TRANSLOG_DMLTYPE_UPDATE;

    /* 模式名 */
    tbcolbase.m_schemaname = (char*)rmalloc0(NAMEDATALEN);
    if(NULL == tbcolbase.m_schemaname)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(tbcolbase.m_schemaname, 0, '\0', NAMEDATALEN);
    rmemcpy0(tbcolbase.m_schemaname, 0, tbdeserialentry->schema, NAMEDATALEN);

    tbcolbase.m_tbname = (char*)rmalloc0(NAMEDATALEN);
    if(NULL == tbcolbase.m_tbname)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(tbcolbase.m_tbname, 0, '\0', NAMEDATALEN);
    rmemcpy0(tbcolbase.m_tbname, 0, tbdeserialentry->table, NAMEDATALEN);

    // 申请m_new_values空间
    mlen = sizeof(xk_pg_parser_translog_tbcol_value) * tbdeserialentry->colcnt;
    colvalues->m_new_values = (xk_pg_parser_translog_tbcol_value*)rmalloc0(mlen);
    if(NULL == colvalues->m_new_values)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(colvalues->m_new_values, 0, '\0', mlen);

    /* 申请m_old_values空间 */ 
    mlen = sizeof(xk_pg_parser_translog_tbcol_value) * tbdeserialentry->colcnt;
    colvalues->m_old_values = (xk_pg_parser_translog_tbcol_value*)rmalloc0(mlen);
    if(NULL == colvalues->m_old_values)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(colvalues->m_old_values, 0, '\0', mlen);

    colvalues->m_base = tbcolbase;
    colvalues->m_valueCnt = tbdeserialentry->colcnt;
    colvalues->m_relid = tbdeserialentry->oid;
    colvalues->m_haspkey = tbdeserialentry->haspkey;
    colvalues->m_relfilenode = 0;
    colvalues->m_tuple = NULL;
    colvalues->m_tupleCnt = 0;
    rstmt->len = txndata->header.totallength;
    rstmt->len -= (colvalues->m_valueCnt * 8 * 2);

    /* 
     * 解析真实数据
     *  1、查看是否为空的 record
     *  2、数据拼装
     */
    totallen = txndata->header.totallength;

    /* 获取update new_value列信息 */
    while(0 < totallen)
    {
        /* 获取colid */
        if(false  == ripple_fftrail_data_buffer2data(&txndata->header,
                                                    ffstate,
                                                    &recoffset,
                                                    &dataoffset,
                                                    RIPPLE_FTRAIL_TOKENDATATYPE_SMALLINT,
                                                    2,
                                                    (uint8*)&colid))
        {
            return false;
        }
        totallen -= 2;

        /* 列信息初始化 */
        col = &colvalues->m_new_values[colid - 1];
        col->m_coltype = tbdeserialentry->columns[colid - 1].typid;

        mlen = strlen(tbdeserialentry->columns[colid - 1].column);
        mlen += 1;
        col->m_colName = (char*)rmalloc0(mlen);
        if(NULL == col->m_colName)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(col->m_colName, 0, '\0', mlen);
        mlen -= 1;
        rmemcpy0(col->m_colName, 0, tbdeserialentry->columns[colid - 1].column, mlen);

        /* 获取flag */
        if(false  == ripple_fftrail_data_buffer2data(&txndata->header,
                                                    ffstate,
                                                    &recoffset,
                                                    &dataoffset,
                                                    RIPPLE_FTRAIL_TOKENDATATYPE_SMALLINT,
                                                    2,
                                                    (uint8*)&flag))
        {
            return false;
        }

        switch (flag)
        {
            case RIPPLE_FF_COL_IS_NORMAL:
                col->m_info = INFO_NOTHING;
                break;
            case RIPPLE_FF_COL_IS_NULL:
                col->m_info = INFO_COL_IS_NULL;
                break;
            case RIPPLE_FF_COL_IS_MISSING:
                col->m_info = INFO_COL_MAY_NULL;
                break;
            case RIPPLE_FF_COL_IS_DROPED:
                col->m_info = INFO_COL_IS_DROPED;
                break;
            case RIPPLE_FF_COL_IS_CUSTOM:
                col->m_info = INFO_COL_IS_CUSTOM;
                break;
            default:
                elog(RLOG_ERROR, "txndata deserial unknown flag:%d", flag);
                break;
        }
        totallen -= 2;
        
        /* 获取列值的长度 */
        if(false  == ripple_fftrail_data_buffer2data(&txndata->header,
                                                    ffstate,
                                                    &recoffset,
                                                    &dataoffset,
                                                    RIPPLE_FTRAIL_TOKENDATATYPE_INT,
                                                    4,
                                                    (uint8*)&col->m_valueLen))
        {
            return false;
        }
        totallen -= 4;

        if (0 != col->m_valueLen)
        {
            col->m_value = (char*)rmalloc0(col->m_valueLen + 1);
            if(NULL == col->m_value)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(col->m_value, 0, '\0', col->m_valueLen + 1);

            /* 获取列信息内容 */
            if(false  == ripple_fftrail_data_buffer2data(&txndata->header,
                                                        ffstate,
                                                        &recoffset,
                                                        &dataoffset,
                                                        RIPPLE_FTRAIL_TOKENDATATYPE_STR,
                                                        col->m_valueLen,
                                                        (uint8*)col->m_value))
            {
                return false;
            }
            totallen -= col->m_valueLen;
        }
        else
        {
            col->m_value = NULL;
        }
        /* 当colid等于列数结束 */
        if (tbdeserialentry->colcnt == colid)
        {
            break;
        }
        
    }

    /* 获取update old_value列信息，totallen不为零一值获取 */
    while(0 < totallen)
    {
        /* 获取update 列信息 */
        /* 获取colid */
        if(false  == ripple_fftrail_data_buffer2data(&txndata->header,
                                                    ffstate,
                                                    &recoffset,
                                                    &dataoffset,
                                                    RIPPLE_FTRAIL_TOKENDATATYPE_SMALLINT,
                                                    2,
                                                    (uint8*)&colid))
        {
            return false;
        }
        
        totallen -= 2;
        /* 列信息初始化 */
        col = &colvalues->m_old_values[colid - 1];
        col->m_coltype = tbdeserialentry->columns[colid - 1].typid;

        mlen = strlen(tbdeserialentry->columns[colid - 1].column);
        mlen += 1;
        col->m_colName = (char*)rmalloc0(mlen);
        if(NULL == col->m_colName)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(col->m_colName, 0, '\0', mlen);
        mlen -= 1;
        rmemcpy0(col->m_colName, 0, tbdeserialentry->columns[colid - 1].column, mlen);

        /* 获取flag */
        if(false  == ripple_fftrail_data_buffer2data(&txndata->header,
                                                    ffstate,
                                                    &recoffset,
                                                    &dataoffset,
                                                    RIPPLE_FTRAIL_TOKENDATATYPE_SMALLINT,
                                                    2,
                                                    (uint8*)&flag))
        {
            return false;
        }

        switch (flag)
        {
            case RIPPLE_FF_COL_IS_NORMAL:
                col->m_info = INFO_NOTHING;
                break;
            case RIPPLE_FF_COL_IS_NULL:
                col->m_info = INFO_COL_IS_NULL;
                break;
            case RIPPLE_FF_COL_IS_MISSING:
                col->m_info = INFO_COL_MAY_NULL;
                break;
            case RIPPLE_FF_COL_IS_DROPED:
                col->m_info = INFO_COL_IS_DROPED;
                break;
            case RIPPLE_FF_COL_IS_CUSTOM:
                col->m_info = INFO_COL_IS_CUSTOM;
                break;
            default:
                elog(RLOG_ERROR, "txndata deserial unknown flag:%d", flag);
                break;
        }
        totallen -= 2;
        
        /* 获取列值的长度 */
        if(false  == ripple_fftrail_data_buffer2data(&txndata->header,
                                                    ffstate,
                                                    &recoffset,
                                                    &dataoffset,
                                                    RIPPLE_FTRAIL_TOKENDATATYPE_INT,
                                                    4,
                                                    (uint8*)&col->m_valueLen))
        {
            return false;
        }
        totallen -= 4;
        if (0 != col->m_valueLen)
        {
            col->m_value = (char*)rmalloc0(col->m_valueLen + 1);
            if(NULL == col->m_value)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(col->m_value, 0, '\0', col->m_valueLen + 1);

            /* 获取列信息内容 */
            if(false  == ripple_fftrail_data_buffer2data(&txndata->header,
                                                        ffstate,
                                                        &recoffset,
                                                        &dataoffset,
                                                        RIPPLE_FTRAIL_TOKENDATATYPE_STR,
                                                        col->m_valueLen,
                                                        (uint8*)col->m_value))
            {
                return false;
            }
            totallen -= col->m_valueLen;
        }
        else
        {
            col->m_value = NULL;
        }
    }
    txndata->header.subtype = subtype;
    return true;
}


