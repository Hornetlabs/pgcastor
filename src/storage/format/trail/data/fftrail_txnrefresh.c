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

/* Serialize refresh statement */
bool fftrail_txnrefresh_serial(void* data, void* state)
{
    /*
     * Record format:
     *  GroupToken
     *  RecHead
     *  RecData
     *      length            2 bytes
     *      schema            length
     *      length            2 bytes
     *      table             length
     *  RecTail
     */
    int             hdrlen = 0;
    int             len = 0;
    uint32          tlen = 0;
    uint8*          uptr = NULL;
    txnstmt*        rstmt = NULL; /* Content to write to trail file */
    refresh_table*  table = NULL;
    refresh_tables* refretables = NULL; /* Refresh statement content */
    ff_txndata*     txndata = NULL;
    file_buffer*    fbuffer = NULL;
    ffsmgr_state*   ffstate = NULL; /* state data info */

    txndata = (ff_txndata*)data;
    rstmt = (txnstmt*)txndata->data;
    ffstate = (ffsmgr_state*)state;

    /* Get refresh info */
    refretables = (refresh_tables*)rstmt->stmt;

    /* Validate and switch block */
    fftrail_serialpreshiftblock(state);
    if (FFSMGR_STATUS_SHIFTFILE == ffstate->status)
    {
        ffstate->status = FFSMGR_STATUS_USED;
    }

    /* Write refresh to trail file */
    fbuffer = file_buffer_getbybufid(ffstate->callback.getfilebuffer(ffstate->privdata), ffstate->bufid);
    ffstate->recptr = fbuffer->data + fbuffer->start;

    /* Calculate length */
    txndata->header.totallength += refretables->cnt * (2 + 2);

    /* Set record header info */
    txndata->header.reccount = 1;
    txndata->header.reclength = 0;
    txndata->header.subtype = FF_DATA_TYPE_REFRESH;
    txndata->header.formattype = FF_DATA_FORMATTYPE_WAL;
    txndata->header.dbmdno = 0;
    txndata->header.tbmdno = 0;
    txndata->header.orgpos = rstmt->extra0.wal.lsn;

    /* Skip record token and header length */
    /* Add offset */
    hdrlen = TOKENHDRSIZE;
    hdrlen += fftrail_data_headlen(ffstate->compatibility);
    fbuffer->start += hdrlen;

    table = refretables->tables;

    while (NULL != table)
    {
        len = strlen(table->schema);
        txndata->header.totallength += len;
        /* Add content */
        /* Refresh schema length */
        fftrail_data_data2buffer(&txndata->header, ffstate, &fbuffer, FTRAIL_TOKENDATATYPE_SMALLINT, 2, (uint8*)&len);

        /* Refresh schema content */
        fftrail_data_data2buffer(&txndata->header,
                                 ffstate,
                                 &fbuffer,
                                 FTRAIL_TOKENDATATYPE_STR,
                                 len,
                                 (uint8*)table->schema);

        len = strlen(table->table);
        txndata->header.totallength += len;
        /* Refresh table length */
        fftrail_data_data2buffer(&txndata->header, ffstate, &fbuffer, FTRAIL_TOKENDATATYPE_SMALLINT, 2, (uint8*)&len);

        /* Refresh string content */
        fftrail_data_data2buffer(&txndata->header,
                                 ffstate,
                                 &fbuffer,
                                 FTRAIL_TOKENDATATYPE_STR,
                                 len,
                                 (uint8*)table->table);
        table = table->next;
    }

    /* Fill header info */
    if (FFSMGR_STATUS_SHIFTFILE == ffstate->status)
    {
        ffstate->status = FFSMGR_STATUS_USED;
    }

    /* Length in Record token */
    tlen = txndata->header.reclength; /* Data length */
    tlen += hdrlen;                   /* Header length */

    /* Add rectail */
    uptr = fbuffer->data + fbuffer->start;
    FTRAIL_GROUP2BUFFER(put, TRAIL_TOKENDATA_RECTAIL, FFTRAIL_INFOTYPE_TOKEN, 0, uptr)

    /* Add tail length */
    tlen += TOKENHDRSIZE;
    fbuffer->start += TOKENHDRSIZE;

    /* Byte alignment */
    tlen = MAXALIGN(tlen);
    fbuffer->start = MAXALIGN(fbuffer->start);

    /* Write header data */
    /* Add GROUP info */
    FTRAIL_GROUP2BUFFER(put, FFTRAIL_GROUPTYPE_DATA, FFTRAIL_INFOTYPE_GROUP, tlen, ffstate->recptr)

    /* Add header info */
    fftrail_data_hdrserail(&txndata->header, ffstate);

    /* Set segno = segno + 1 to correctly record file switch */
    fbuffer = file_buffer_getbybufid(ffstate->callback.getfilebuffer(ffstate->privdata), ffstate->bufid);
    fbuffer->extra.chkpoint.segno.trail.fileid++;

    /* Refresh transaction file switch ensures subsequent parsing is correct */
    if (false == fftrail_serialshiffile(state))
    {
        return false;
    }

    ffstate->recptr = NULL;

    return true;
}

/* Deserialize refresh info */
bool fftrail_txnrefresh_deserial(void** data, void* state)
{
    uint8           tokenid = 0;   /* token id */
    uint8           tokeninfo = 0; /* token details */
    uint32          recoffset = 0;
    uint32          dataoffset = 0;
    uint16          subtype = FF_DATA_TYPE_NOP;
    uint32          len = 0;
    uint32          tokenlen = 0; /* token length */
    uint64          totallen = 0;

    uint8*          uptr = NULL;
    uint8*          tokendata = NULL; /* token data area */
    refresh_table*  table = NULL;
    refresh_tables* refreshtables = NULL;
    ff_txndata*     txndata = NULL;
    ffsmgr_state*   ffstate = NULL;
    txnstmt*        rstmt = NULL;

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

    /* Allocate refresh data space */
    refreshtables = refresh_tables_init();
    rstmt->type = TXNSTMT_TYPE_REFRESH;
    rstmt->stmt = (void*)refreshtables;
    rstmt->len = txndata->header.totallength;

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

    /* Preserve info, as these data may be cleared in subsequent processing logic */
    subtype = txndata->header.subtype;

    /* Re-point to header */
    ffstate->recptr = uptr;
    recoffset += (uint16)fftrail_data_headlen(ffstate->compatibility);

    totallen = txndata->header.totallength;

    /*
     * Parse real data
     *  1. Check if record is empty
     *  2. Data assembly
     */
    while (0 < totallen)
    {
        table = refresh_table_init();
        /* Get schema type */
        if (false == fftrail_data_buffer2data(&txndata->header,
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
        if (NULL == table->schema)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(table->schema, 0, '\0', len + 1);

        /* Get schema subtype */
        if (false == fftrail_data_buffer2data(&txndata->header,
                                              ffstate,
                                              &recoffset,
                                              &dataoffset,
                                              FTRAIL_TOKENDATATYPE_STR,
                                              len,
                                              (uint8*)table->schema))
        {
            return false;
        }
        totallen -= (2 + len);
        /* Get schema string length */
        if (false == fftrail_data_buffer2data(&txndata->header,
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
        if (NULL == table->table)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(table->table, 0, '\0', len + 1);

        /* Get schema string content */
        if (false == fftrail_data_buffer2data(&txndata->header,
                                              ffstate,
                                              &recoffset,
                                              &dataoffset,
                                              FTRAIL_TOKENDATATYPE_STR,
                                              len,
                                              (uint8*)table->table))
        {
            return false;
        }
        totallen -= (2 + len);

        refresh_tables_add(table, refreshtables);
    }
    rstmt->len -= (refreshtables->cnt * 4);

    /* Reset, because when switching block or file, subtype value is: FF_DATA_SUBTYPE_REC_CONTRECORD
     */
    txndata->header.subtype = subtype;
    return true;
}
