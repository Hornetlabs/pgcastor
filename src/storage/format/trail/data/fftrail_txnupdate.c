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
#include "storage/trail/data/fftrail_tbmetadata.h"
#include "storage/trail/data/fftrail_txn.h"
#include "storage/trail/data/fftrail_txnupdate.h"

/* Serialize update statement */
bool fftrail_txnupdate_serial(void* data, void* state)
{
    /*
     * Record format:
     *  GroupToken
     *  RecHead
     *  RecData
     *      colid           2 bytes
     *      flag            2 bytes
     *      length          4 bytes
     *      stmtdata        length
     *  RecTail
     */
    int    hdrlen = 0;
    uint32 dbmdno = 0;
    uint32 tbmdno = 0;
    uint32 tlen = 0;
    int    colid = 0;
    int    i = 0;

    uint8*                           tmpuptr = NULL;
    uint8*                           uptr = NULL;
    txnstmt*                         rstmt = NULL; /* Content to write to trail file */
    uint16                           flag = 0;
    ff_txndata*                      txndata = NULL;
    file_buffer*                     fbuffer = NULL;
    ffsmgr_state*                    ffstate = NULL; /* state data info */
    pg_parser_translog_tbcol_values* colvalues = NULL;
    pg_parser_translog_tbcol_value   col;

    txndata = (ff_txndata*)data;
    rstmt = (txnstmt*)txndata->data;
    ffstate = (ffsmgr_state*)state;

    colvalues = (pg_parser_translog_tbcol_values*)rstmt->stmt;

fftrail_txnupdate_serial_retry:
    do
    {
        /* Write table to trail file */
        fftrail_tbmetadata_serial(false,
                                  rstmt->database,  // colvalues->m_base.m_dbid,
                                  colvalues->m_relid, txndata->header.transid, &dbmdno, &tbmdno,
                                  state);
    } while (FFSMGR_STATUS_SHIFTFILE ==
             ffstate->status); /* Content switched, need to write again */

    /* Set state */
    ffstate->status = FFSMGR_STATUS_USED;

    /* Validate and switch block */
    fftrail_serialpreshiftblock(state);
    if (FFSMGR_STATUS_SHIFTFILE == ffstate->status)
    {
        /* File switched, should rewrite table info */
        ffstate->status = FFSMGR_STATUS_USED;
        goto fftrail_txnupdate_serial_retry;
    }

    /* Get buffer info and set write position */
    fbuffer =
        file_buffer_getbybufid(ffstate->callback.getfilebuffer(ffstate->privdata), ffstate->bufid);
    tmpuptr = ffstate->recptr = fbuffer->data + fbuffer->start;

    /* Calculate length, compute total length of this record */
    /* colid(2) + flag(2) + (length 4) + data*/
    txndata->header.totallength = rstmt->len;
    /* Update saves old and new value length×2 */
    txndata->header.totallength += (colvalues->m_valueCnt * 8 * 2);

    /* Set record header info, focus on dbmdno and tbmdno */
    txndata->header.reccount = 1;
    txndata->header.reclength = 0;
    txndata->header.subtype = FF_DATA_TYPE_DML_UPDATE;
    txndata->header.formattype = FF_DATA_FORMATTYPE_WAL;
    txndata->header.dbmdno = dbmdno;
    txndata->header.tbmdno = tbmdno;
    txndata->header.orgpos = rstmt->extra0.wal.lsn;

    /* Skip record token and header length */
    /* Add offset */
    hdrlen = TOKENHDRSIZE;
    hdrlen += fftrail_data_headlen(ffstate->compatibility);
    fbuffer->start += hdrlen;

    tmpuptr += hdrlen;

    /* Write all new_value column info first */
    for (i = 0; i < colvalues->m_valueCnt; i++)
    {
        col = colvalues->m_new_values[i];
        colid = i + 1;
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
        fftrail_data_data2buffer(&txndata->header, ffstate, &fbuffer, FTRAIL_TOKENDATATYPE_SMALLINT,
                                 2, (uint8*)&colid);

        /* flag(2) */
        fftrail_data_data2buffer(&txndata->header, ffstate, &fbuffer, FTRAIL_TOKENDATATYPE_SMALLINT,
                                 2, (uint8*)&flag);

        /* (length 4) */
        fftrail_data_data2buffer(&txndata->header, ffstate, &fbuffer, FTRAIL_TOKENDATATYPE_INT, 4,
                                 (uint8*)&col.m_valueLen);

        /* Write column values */
        if (0 != col.m_valueLen)
        {
            /* data* */
            fftrail_data_data2buffer(&txndata->header, ffstate, &fbuffer, FTRAIL_TOKENDATATYPE_STR,
                                     col.m_valueLen, (uint8*)col.m_value);
        }
    }

    /* Then write all old_value column info */
    for (i = 0; i < colvalues->m_valueCnt; i++)
    {
        col = colvalues->m_old_values[i];
        colid = i + 1;
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
        fftrail_data_data2buffer(&txndata->header, ffstate, &fbuffer, FTRAIL_TOKENDATATYPE_SMALLINT,
                                 2, (uint8*)&colid);

        /* flag(2) */
        fftrail_data_data2buffer(&txndata->header, ffstate, &fbuffer, FTRAIL_TOKENDATATYPE_SMALLINT,
                                 2, (uint8*)&flag);

        /* (length 4) */
        fftrail_data_data2buffer(&txndata->header, ffstate, &fbuffer, FTRAIL_TOKENDATATYPE_INT, 4,
                                 (uint8*)&col.m_valueLen);

        /* Write column values */
        if (0 != col.m_valueLen)
        {
            /* data* */
            fftrail_data_data2buffer(&txndata->header, ffstate, &fbuffer, FTRAIL_TOKENDATATYPE_STR,
                                     col.m_valueLen, (uint8*)col.m_value);
        }
    }

    /* Reset state */
    if (FFSMGR_STATUS_SHIFTFILE == ffstate->status)
    {
        /* File switched, should rewrite table info */
        ffstate->status = FFSMGR_STATUS_USED;
    }
    /*
     * 1. Write tail data
     * 2. Write header data
     */
    /* Add rectail tail data */
    uptr = fbuffer->data + fbuffer->start;
    FTRAIL_GROUP2BUFFER(put, TRAIL_TOKENDATA_RECTAIL, FFTRAIL_INFOTYPE_TOKEN, 0, uptr)

    /* Estimate total length */
    tlen = hdrlen;                     /* Record header data */
    tlen += txndata->header.reclength; /* Record data content data */
    tlen += TOKENHDRSIZE;              /* Record tail data */

    /* Byte alignment */
    tlen = MAXALIGN(tlen);

    /* Add record tail length */
    fbuffer->start += TOKENHDRSIZE;

    /* Align */
    fbuffer->start = MAXALIGN(fbuffer->start);

    /* Write header data */
    /* Add GROUP info */
    FTRAIL_GROUP2BUFFER(put, FFTRAIL_GROUPTYPE_DATA, FFTRAIL_INFOTYPE_GROUP, tlen, ffstate->recptr)

    /* Add header info */
    fftrail_data_hdrserail(&txndata->header, ffstate);
    ffstate->recptr = NULL;
    return true;
}

/* Deserialize update info */
bool fftrail_txnupdate_deserial(void** data, void* state)
{
    bool   found = false;
    uint8  tokenid = 0;   /* token id */
    uint8  tokeninfo = 0; /* token details */
    uint32 recoffset = 0;
    uint32 dataoffset =
        0; /* Offset based on data, used to calculate remaining space in current record data */
    uint16 subtype = FF_DATA_TYPE_NOP;
    uint16 colid;
    uint16 flag = 0;
    uint32 mlen = 0;
    uint32 tokenlen = 0; /* token length */
    uint64 totallen = 0;

    uint8*                           uptr = NULL;
    uint8*                           tokendata = NULL; /* token data area */
    ff_txndata*                      txndata = NULL;
    ffsmgr_state*                    ffstate = NULL;
    txnstmt*                         rstmt = NULL;
    fftrail_privdata*                privdata = NULL;
    fftrail_table_deserialentry*     tbdeserialentry = NULL;
    fftrail_database_deserialentry*  dbdeserialentry = NULL;
    pg_parser_translog_tbcol_values* colvalues = NULL;
    pg_parser_translog_tbcol_value*  col;
    pg_parser_translog_tbcolbase     tbcolbase;

    /* Type cast */
    ffstate = (ffsmgr_state*)state;
    uptr = ffstate->recptr;

    /* Allocate space */
    txndata = (ff_txndata*)rmalloc0(sizeof(ff_txndata));
    if (NULL == txndata)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(txndata, 0, '\0', sizeof(ff_txndata));
    *data = txndata;

    /* Allocate space */
    rstmt = (txnstmt*)rmalloc0(sizeof(txnstmt));
    if (NULL == rstmt)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(rstmt, 0, '\0', sizeof(txnstmt));
    txndata->data = (void*)rstmt;

    /* Allocate update data space */
    colvalues = (pg_parser_translog_tbcol_values*)rmalloc0(sizeof(pg_parser_translog_tbcol_values));
    if (NULL == colvalues)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(colvalues, 0, '\0', sizeof(pg_parser_translog_tbcol_values));
    rstmt->stmt = (void*)colvalues;
    rstmt->type = TXNSTMT_TYPE_DML;

    /* Get header id */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    if (FFTRAIL_GROUPTYPE_DATA != tokenid || FFTRAIL_INFOTYPE_GROUP != tokeninfo)
    {
        /* make gcc happy */
        uptr = tokendata;
        elog(RLOG_ERROR, "trail file data format error");
    }
    recoffset = TOKENHDRSIZE;

    /* Parse header data */
    uptr = ffstate->recptr;
    ffstate->recptr += recoffset;
    fftrail_data_hdrdeserail(&txndata->header, ffstate);

    /* Add orgpos to rstmt */
    rstmt->extra0.wal.lsn = txndata->header.orgpos;

    /* Preserve info, as these data may be cleared in subsequent processing */
    subtype = txndata->header.subtype;

    /* Re-point to header */
    ffstate->recptr = uptr;
    recoffset += (uint16)fftrail_data_headlen(ffstate->compatibility);

    /* Get table info */
    privdata = (fftrail_privdata*)ffstate->fdata->ffdata;
    tbdeserialentry = hash_search(privdata->tables, &txndata->header.tbmdno, HASH_FIND, &found);
    if (!found)
    {
        elog(RLOG_ERROR, "not found table,%lu.%lu, xid:%lu.%u,crc:%u", privdata->tbnum,
             txndata->header.tbmdno, txndata->header.transid, txndata->header.transind,
             txndata->header.crc32);
        return false;
    }

    /* Get database info */
    dbdeserialentry = hash_search(privdata->databases, &txndata->header.dbmdno, HASH_FIND, &found);
    if (!found)
    {
        elog(RLOG_ERROR, "not found tdatabase %u", tbdeserialentry->dbno);
        return false;
    }

    rstmt->database = dbdeserialentry->oid;
    tbcolbase.m_dmltype = PG_PARSER_TRANSLOG_DMLTYPE_UPDATE;

    /* Schema name */
    tbcolbase.m_schemaname = (char*)rmalloc0(NAMEDATALEN);
    if (NULL == tbcolbase.m_schemaname)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(tbcolbase.m_schemaname, 0, '\0', NAMEDATALEN);
    rmemcpy0(tbcolbase.m_schemaname, 0, tbdeserialentry->schema, NAMEDATALEN);

    tbcolbase.m_tbname = (char*)rmalloc0(NAMEDATALEN);
    if (NULL == tbcolbase.m_tbname)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(tbcolbase.m_tbname, 0, '\0', NAMEDATALEN);
    rmemcpy0(tbcolbase.m_tbname, 0, tbdeserialentry->table, NAMEDATALEN);

    // Allocate m_new_values space
    mlen = sizeof(pg_parser_translog_tbcol_value) * tbdeserialentry->colcnt;
    colvalues->m_new_values = (pg_parser_translog_tbcol_value*)rmalloc0(mlen);
    if (NULL == colvalues->m_new_values)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(colvalues->m_new_values, 0, '\0', mlen);

    /* Allocate m_old_values space */
    mlen = sizeof(pg_parser_translog_tbcol_value) * tbdeserialentry->colcnt;
    colvalues->m_old_values = (pg_parser_translog_tbcol_value*)rmalloc0(mlen);
    if (NULL == colvalues->m_old_values)
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
     * Parse real data
     *  1. Check if record is empty
     *  2. Data assembly
     */
    totallen = txndata->header.totallength;

    /* Get update new_value column info */
    while (0 < totallen)
    {
        /* Get colid */
        if (false == fftrail_data_buffer2data(&txndata->header, ffstate, &recoffset, &dataoffset,
                                              FTRAIL_TOKENDATATYPE_SMALLINT, 2, (uint8*)&colid))
        {
            return false;
        }
        totallen -= 2;

        /* Column info initialization */
        col = &colvalues->m_new_values[colid - 1];
        col->m_coltype = tbdeserialentry->columns[colid - 1].typid;

        mlen = strlen(tbdeserialentry->columns[colid - 1].column);
        mlen += 1;
        col->m_colName = (char*)rmalloc0(mlen);
        if (NULL == col->m_colName)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(col->m_colName, 0, '\0', mlen);
        mlen -= 1;
        rmemcpy0(col->m_colName, 0, tbdeserialentry->columns[colid - 1].column, mlen);

        /* Get flag */
        if (false == fftrail_data_buffer2data(&txndata->header, ffstate, &recoffset, &dataoffset,
                                              FTRAIL_TOKENDATATYPE_SMALLINT, 2, (uint8*)&flag))
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

        /* Get column value length */
        if (false == fftrail_data_buffer2data(&txndata->header, ffstate, &recoffset, &dataoffset,
                                              FTRAIL_TOKENDATATYPE_INT, 4,
                                              (uint8*)&col->m_valueLen))
        {
            return false;
        }
        totallen -= 4;

        if (0 != col->m_valueLen)
        {
            col->m_value = (char*)rmalloc0(col->m_valueLen + 1);
            if (NULL == col->m_value)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(col->m_value, 0, '\0', col->m_valueLen + 1);

            /* Get column info content */
            if (false == fftrail_data_buffer2data(&txndata->header, ffstate, &recoffset,
                                                  &dataoffset, FTRAIL_TOKENDATATYPE_STR,
                                                  col->m_valueLen, (uint8*)col->m_value))
            {
                return false;
            }
            totallen -= col->m_valueLen;
        }
        else
        {
            col->m_value = NULL;
        }
        /* End when colid equals column count */
        if (tbdeserialentry->colcnt == colid)
        {
            break;
        }
    }

    /* Get update old_value column info, keep getting while totallen is not zero */
    while (0 < totallen)
    {
        /* Get update column info */
        /* Get colid */
        if (false == fftrail_data_buffer2data(&txndata->header, ffstate, &recoffset, &dataoffset,
                                              FTRAIL_TOKENDATATYPE_SMALLINT, 2, (uint8*)&colid))
        {
            return false;
        }

        totallen -= 2;
        /* Column info initialization */
        col = &colvalues->m_old_values[colid - 1];
        col->m_coltype = tbdeserialentry->columns[colid - 1].typid;

        mlen = strlen(tbdeserialentry->columns[colid - 1].column);
        mlen += 1;
        col->m_colName = (char*)rmalloc0(mlen);
        if (NULL == col->m_colName)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(col->m_colName, 0, '\0', mlen);
        mlen -= 1;
        rmemcpy0(col->m_colName, 0, tbdeserialentry->columns[colid - 1].column, mlen);

        /* Get flag */
        if (false == fftrail_data_buffer2data(&txndata->header, ffstate, &recoffset, &dataoffset,
                                              FTRAIL_TOKENDATATYPE_SMALLINT, 2, (uint8*)&flag))
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

        /* Get column value length */
        if (false == fftrail_data_buffer2data(&txndata->header, ffstate, &recoffset, &dataoffset,
                                              FTRAIL_TOKENDATATYPE_INT, 4,
                                              (uint8*)&col->m_valueLen))
        {
            return false;
        }
        totallen -= 4;
        if (0 != col->m_valueLen)
        {
            col->m_value = (char*)rmalloc0(col->m_valueLen + 1);
            if (NULL == col->m_value)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(col->m_value, 0, '\0', col->m_valueLen + 1);

            /* Get column info content */
            if (false == fftrail_data_buffer2data(&txndata->header, ffstate, &recoffset,
                                                  &dataoffset, FTRAIL_TOKENDATATYPE_STR,
                                                  col->m_valueLen, (uint8*)col->m_value))
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
