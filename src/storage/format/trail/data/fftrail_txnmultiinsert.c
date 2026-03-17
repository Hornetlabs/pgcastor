#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "stmts/txnstmt.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "storage/trail/data/fftrail_data.h"
#include "storage/trail/data/fftrail_tbmetadata.h"
#include "storage/trail/data/fftrail_txn.h"
#include "storage/trail/data/fftrail_txnmultiinsert.h"

/* multiinsert语句序列化 */
bool fftrail_txnmultiinsert_serial(void* data, void* state)
{
    int hdrlen = 0;
    uint32 dbmdno = 0;
    uint32 tbmdno = 0;
    uint32 tlen = 0;
    int colid = 0;
    int i = 0;
    int j = 0;

    uint8* tmpuptr = NULL;
    uint8* uptr = NULL;
    txnstmt* rstmt = NULL;                      /* 需要写入 trail 文件的内容 */
    uint16 flag = 0;
    ff_txndata*  txndata = NULL;
    file_buffer* fbuffer = NULL;
    ffsmgr_state* ffstate = NULL;            /* state 数据信息 */
    xk_pg_parser_translog_tbcol_nvalues* colnvalues = NULL;
    xk_pg_parser_translog_tbcol_value col;

    txndata = (ff_txndata*)data;
    rstmt = (txnstmt*)txndata->data;
    ffstate = (ffsmgr_state*)state;

    colnvalues = (xk_pg_parser_translog_tbcol_nvalues*)rstmt->stmt;

fftrail_txnmultiinsert_serial_retry:
    do
    {
        /* 将表写入到 trail 文件中 */
        fftrail_tbmetadata_serial(false,
                                         rstmt->database,//colnvalues->m_base.m_dbid,
                                         colnvalues->m_relid,
                                         txndata->header.transid,
                                         &dbmdno,
                                         &tbmdno,
                                         state);
    } while(FFSMGR_STATUS_SHIFTFILE == ffstate->status);         /* 内容发生了切换，那么需要在写一次 */

    /* 设置状态 */
    ffstate->status = FFSMGR_STATUS_USED;

    /* 检验并切换block */
    fftrail_serialpreshiftblock(state);
    if(FFSMGR_STATUS_SHIFTFILE == ffstate->status)
    {
        /* 发生了文件切换，那么此时，应该重新写入 table 信息 */
        ffstate->status = FFSMGR_STATUS_USED;
        goto fftrail_txnmultiinsert_serial_retry;
    }

    /* 获取 buffer 信息,并设置写入的位置 */
    fbuffer = file_buffer_getbybufid(ffstate->callback.getfilebuffer(ffstate->privdata), ffstate->bufid);
    tmpuptr = ffstate->recptr = fbuffer->data + fbuffer->start;

    /* 长度计算，换算该条记录的总长度 */
    /* colid(2) + flag(2) + (length 4) + data*/
    txndata->header.totallength = rstmt->len;
    txndata->header.totallength += (colnvalues->m_valueCnt * colnvalues->m_rowCnt * 8);

    /* 设置record头信息， 关注 dbmdno 和 tbmdno */
    txndata->header.reccount = colnvalues->m_rowCnt;
    txndata->header.reclength = 0;
    txndata->header.subtype = FF_DATA_TYPE_DML_MULTIINSERT;
    txndata->header.formattype = FF_DATA_FORMATTYPE_WAL;
    txndata->header.dbmdno = dbmdno;
    txndata->header.tbmdno = tbmdno;
    txndata->header.orgpos = rstmt->extra0.wal.lsn;

    /* 跳过record token 和 头长度 */
    /* 增加偏移 */
    hdrlen = TOKENHDRSIZE;
    hdrlen += fftrail_data_headlen(ffstate->compatibility);
    fbuffer->start += hdrlen;

    tmpuptr += hdrlen;

    /* 根据m_rowCnt和m_rowCnt获取values列信息 */
    for (i = 0; i < colnvalues->m_rowCnt; i++)
    {
         for (j = 0; j < colnvalues->m_valueCnt; j++)
         {
            col = colnvalues->m_rows[i].m_new_values[j];
            colid = j + 1;
            flag = FF_COL_IS_NORMAL;
            switch (col.m_info)
            {
                case INFO_COL_IS_NULL:
                    flag = FF_COL_IS_NULL;
                    break;
                case INFO_COL_MAY_NULL:
                    flag = FF_COL_IS_MISSING;
                    break;
                case INFO_COL_IS_DROPED:
                    flag = FF_COL_IS_DROPED;
                    break;
                case INFO_COL_IS_CUSTOM:
                    flag = FF_COL_IS_CUSTOM;
                    break;
                default:
                    break;
            }

            /* colid(2) */
            fftrail_data_data2buffer(&txndata->header,
                                            ffstate,
                                            &fbuffer,
                                            FTRAIL_TOKENDATATYPE_SMALLINT,
                                            2,
                                            (uint8*)&colid);

            /* flag(2) */
            fftrail_data_data2buffer(&txndata->header,
                                            ffstate,
                                            &fbuffer,
                                            FTRAIL_TOKENDATATYPE_SMALLINT,
                                            2,
                                            (uint8*)&flag);

            /* (length 4) */
            fftrail_data_data2buffer(&txndata->header,
                                            ffstate,
                                            &fbuffer,
                                            FTRAIL_TOKENDATATYPE_INT,
                                            4,
                                            (uint8*)&col.m_valueLen);

            if (0 != col.m_valueLen)
            {
            /* data* */
            fftrail_data_data2buffer(&txndata->header,
                                            ffstate,
                                            &fbuffer,
                                            FTRAIL_TOKENDATATYPE_STR,
                                            col.m_valueLen,
                                            (uint8*)col.m_value);
            }
        }
    }
    /* 重置状态 */
    if(FFSMGR_STATUS_SHIFTFILE == ffstate->status)
    {
        /* 发生了文件切换，那么此时，应该重新写入 table 信息 */
        ffstate->status = FFSMGR_STATUS_USED;
    }
    /*
     * 1、写尾部数据
     * 2、写头部数据
     */
    /* 增加 rectail 尾部数据 */
    uptr = fbuffer->data + fbuffer->start;
    FTRAIL_GROUP2BUFFER(put,
                                TRAIL_TOKENDATA_RECTAIL,
                                FFTRAIL_INFOTYPE_TOKEN,
                                0,
                                uptr)

    /* 估算总长度 */
    tlen = hdrlen;                              /* record 的头部数据 */
    tlen += txndata->header.reclength;          /* record data 内容数据 */
    tlen += TOKENHDRSIZE;                /* record 尾部数据 */

    /* 字节对齐 */
    tlen = MAXALIGN(tlen);

    /* 增加record尾部长度 */
    fbuffer->start += TOKENHDRSIZE;

    /* 对齐 */
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

/*multiinsert信息反序列化 */
bool fftrail_txnmultiinsert_deserial(void** data, void* state)
{
    bool    found = false;
    uint8   tokenid = 0;                        /* token 标识 */
    uint8   tokeninfo = 0;                      /* token 的详情 */
    uint32  recoffset = 0;
    uint32  dataoffset = 0; 
    uint16  subtype = FF_DATA_TYPE_NOP;
    uint16  colid;
    uint16  flag = 0;
    uint32  mlen = 0;
    uint32  tokenlen = 0;                       /* token 长度 */
    uint64  totallen = 0;
    int     i = 0;

    uint8*  uptr = NULL;
    uint8*  tokendata = NULL;                   /* token 数据区 */
    ff_txndata*  txndata = NULL;
    ffsmgr_state* ffstate = NULL;
    txnstmt* rstmt = NULL;
    fftrail_privdata* privdata = NULL; 
    fftrail_table_deserialentry* tbdeserialentry = NULL;
    fftrail_database_deserialentry* dbdeserialentry = NULL;
    xk_pg_parser_translog_tbcol_nvalues* colnvalues = NULL;
    xk_pg_parser_translog_tbcol_value* col = NULL;
    xk_pg_parser_translog_tbcolbase tbcolbase;

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

    *data = txndata;
    /* 申请multiinsert 空间 */
    colnvalues = (xk_pg_parser_translog_tbcol_nvalues*)rmalloc0(sizeof(xk_pg_parser_translog_tbcol_nvalues));
    if(NULL == colnvalues)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(colnvalues, 0, '\0', sizeof(xk_pg_parser_translog_tbcol_nvalues));
    rstmt->stmt = (void*)colnvalues;
    rstmt->type = TXNSTMT_TYPE_DML;

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

    /* 获取表信息 */
    privdata = (fftrail_privdata*)ffstate->fdata->ffdata;
    tbdeserialentry = hash_search(privdata->tables, &txndata->header.tbmdno, HASH_FIND, &found);
    if (!found)
    {
        elog(RLOG_ERROR, "not found table %lu", privdata->tbnum);
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
    tbcolbase.m_dmltype = XK_PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT;

    /* 模式 */
    tbcolbase.m_schemaname = (char*)rmalloc0(NAMEDATALEN);
    if(NULL == tbcolbase.m_schemaname)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(tbcolbase.m_schemaname, 0, '\0', NAMEDATALEN);
    rmemcpy0(tbcolbase.m_schemaname, 0, tbdeserialentry->schema, NAMEDATALEN);

    /* 表名 */
    tbcolbase.m_tbname = (char*)rmalloc0(NAMEDATALEN);
    if(NULL == tbcolbase.m_tbname)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(tbcolbase.m_tbname, 0, '\0', NAMEDATALEN);
    rmemcpy0(tbcolbase.m_tbname, 0, tbdeserialentry->table, NAMEDATALEN);

    /* 申请rows空间 */
    mlen = sizeof(xk_pg_parser_translog_tbcol_rows) * txndata->header.reccount;
    colnvalues->m_rows = (xk_pg_parser_translog_tbcol_rows*)rmalloc0(mlen);
    if(NULL == colnvalues->m_rows)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(colnvalues->m_rows, 0, '\0', mlen);

    colnvalues->m_base = tbcolbase;
    colnvalues->m_valueCnt = tbdeserialentry->colcnt;
    colnvalues->m_rowCnt = txndata->header.reccount;
    colnvalues->m_relid = tbdeserialentry->oid;
    colnvalues->m_haspkey = tbdeserialentry->haspkey;
    colnvalues->m_relfilenode = 0;
    colnvalues->m_tuple = NULL;
    colnvalues->m_tupleCnt = 0;
    rstmt->len = txndata->header.totallength;
    rstmt->len -= (colnvalues->m_valueCnt * colnvalues->m_rowCnt * 8);

    totallen = txndata->header.totallength;

    /* 
     * 解析真实数据
     *  1、查看是否为空的 record
     *  2、数据拼装
     */
    /* 根据txndata->header.reccount获取每行的信息 */
    for (i = 0; i < colnvalues->m_rowCnt; i++)
    {
        /* 申请每行列信息空间 */
        mlen = sizeof(xk_pg_parser_translog_tbcol_value) * tbdeserialentry->colcnt;
        colnvalues->m_rows[i].m_new_values = (xk_pg_parser_translog_tbcol_value*)rmalloc0(mlen);
        if(NULL ==  colnvalues->m_rows[i].m_new_values)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0( colnvalues->m_rows[i].m_new_values, 0, '\0', mlen);

        /* 获取multiinsert 列信息 */
        while(0 < totallen)
        {
            /* 获取colid */
            if(false  == fftrail_data_buffer2data(&txndata->header,
                                                        ffstate,
                                                        &recoffset,
                                                        &dataoffset,
                                                        FTRAIL_TOKENDATATYPE_SMALLINT,
                                                        2,
                                                        (uint8*)&colid))
            {
                return false;
            }
            totallen -= 2;
            /* 列信息初始化 */
            col = &colnvalues->m_rows[i].m_new_values[colid - 1];
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
            if(false  == fftrail_data_buffer2data(&txndata->header,
                                                        ffstate,
                                                        &recoffset,
                                                        &dataoffset,
                                                        FTRAIL_TOKENDATATYPE_SMALLINT,
                                                        2,
                                                        (uint8*)&flag))
            {
                return false;
            }
            switch (flag)
            {
                case FF_COL_IS_NORMAL:
                    col->m_info = INFO_NOTHING;
                    break;
                case FF_COL_IS_NULL:
                    col->m_info = INFO_COL_IS_NULL;
                    break;
                case FF_COL_IS_MISSING:
                    col->m_info = INFO_COL_MAY_NULL;
                    break;
                case FF_COL_IS_DROPED:
                    col->m_info = INFO_COL_IS_DROPED;
                    break;
                case FF_COL_IS_CUSTOM:
                    col->m_info = INFO_COL_IS_CUSTOM;
                    break;
                default:
                    elog(RLOG_ERROR, "txndata deserial unknown flag:%d", flag);
                    break;
            }
            totallen -= 2;
            
            /* 获取列值的长度 */
            if(false  == fftrail_data_buffer2data(&txndata->header,
                                                        ffstate,
                                                        &recoffset,
                                                        &dataoffset,
                                                        FTRAIL_TOKENDATATYPE_INT,
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
                if(false  == fftrail_data_buffer2data(&txndata->header,
                                                            ffstate,
                                                            &recoffset,
                                                            &dataoffset,
                                                            FTRAIL_TOKENDATATYPE_STR,
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
            /* 一行语句结束 */
            if (tbdeserialentry->colcnt == colid)
            {
                break;
            }
            
        }
    }
    

    /* 重设，因为在切换block或file时，subtype的值为:FF_DATA_SUBTYPE_REC_CONTRECORD */
    txndata->header.subtype = subtype;
    return true;
}

