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
#include "storage/trail/data/fftrail_txnbigtxn_begin.h"

/* Serialize bigtxn begin statement */
bool fftrail_txnbigtxn_begin_serial(void* data, void* state)
{
    /*
     * Record format:
     *  GroupToken
     *  RecHead
     *  RecData
     *      xid            8 bytes
     *  RecTail
     */
    int                hdrlen = 0;
    uint32             tlen = 0;
    FullTransactionId* xid = NULL;

    uint8*        uptr = NULL;
    txnstmt*      rstmt = NULL; /* Content to write to trail file */
    ff_txndata*   txndata = NULL;
    file_buffer*  fbuffer = NULL;
    ffsmgr_state* ffstate = NULL; /* state data info */

    txndata = (ff_txndata*)data;
    rstmt = (txnstmt*)txndata->data;
    ffstate = (ffsmgr_state*)state;

    /* Get xid info */
    xid = (FullTransactionId*)rstmt->stmt;

    /* Validate and switch block */
    fftrail_serialpreshiftblock(state);
    if (FFSMGR_STATUS_SHIFTFILE == ffstate->status)
    {
        ffstate->status = FFSMGR_STATUS_USED;
    }

    /* Write bigtxn_begin to trail file */
    fbuffer =
        file_buffer_getbybufid(ffstate->callback.getfilebuffer(ffstate->privdata), ffstate->bufid);
    ffstate->recptr = fbuffer->data + fbuffer->start;

    /* Calculate length bigtxn xid 8 */
    txndata->header.totallength = 8;

    /* Set record header info */
    txndata->header.reccount = 1;
    txndata->header.reclength = 0;
    txndata->header.subtype = FF_DATA_TYPE_BIGTXN_BEGIN;
    txndata->header.formattype = FF_DATA_FORMATTYPE_WAL;
    txndata->header.dbmdno = 0;
    txndata->header.tbmdno = 0;
    txndata->header.orgpos = rstmt->extra0.wal.lsn;

    /* Skip record token and header length */
    /* Add offset */
    hdrlen = TOKENHDRSIZE;
    hdrlen += fftrail_data_headlen(ffstate->compatibility);
    fbuffer->start += hdrlen;

    /* bigtxn begin txid */
    fftrail_data_data2buffer(&txndata->header, ffstate, &fbuffer, FTRAIL_TOKENDATATYPE_BIGINT, 8,
                             (uint8*)xid);

    /* Fill header info */
    if (FFSMGR_STATUS_SHIFTFILE == ffstate->status)
    {
        ffstate->status = FFSMGR_STATUS_USED;
    }

    /* Length written in Record token */
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

    return true;
}

/* Deserialize bigtxn begin info */
bool fftrail_txnbigtxn_begin_deserial(void** data, void* state)
{
    uint8              tokenid = 0;   /* token id */
    uint8              tokeninfo = 0; /* token details */
    uint32             recoffset = 0;
    uint32             dataoffset = 0;
    uint16             subtype = FF_DATA_TYPE_NOP;
    uint32             tokenlen = 0; /* token length */
    uint64             totallen = 0;
    FullTransactionId* xid = NULL;

    uint8*        uptr = NULL;
    uint8*        tokendata = NULL; /* token data area */
    ff_txndata*   txndata = NULL;
    ffsmgr_state* ffstate = NULL;
    txnstmt*      rstmt = NULL;

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
    rstmt = txnstmt_init();
    if (NULL == rstmt)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return false;
    }
    txndata->data = (void*)rstmt;
    rstmt->type = TXNSTMT_TYPE_BIGTXN_BEGIN;
    rstmt->len = txndata->header.totallength;

    xid = (FullTransactionId*)rmalloc0(sizeof(FullTransactionId));
    if (NULL == xid)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(xid, 0, '\0', sizeof(FullTransactionId));

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

    /* Preserve info, because these data may be cleared in subsequent processing */
    subtype = txndata->header.subtype;

    /* Re-point to header */
    ffstate->recptr = uptr;
    recoffset += (uint16)fftrail_data_headlen(ffstate->compatibility);

    totallen = txndata->header.totallength;

    /* Get xid */
    if (false == fftrail_data_buffer2data(&txndata->header, ffstate, &recoffset, &dataoffset,
                                          FTRAIL_TOKENDATATYPE_BIGINT, 8, (uint8*)xid))
    {
        elog(RLOG_WARNING, "big txn begin deserial error");
        return false;
    }
    rstmt->stmt = (void*)xid;
    totallen -= 8;

    /* Reset, because when switching block or file, subtype value is: FF_DATA_SUBTYPE_REC_CONTRECORD
     */
    txndata->header.subtype = subtype;
    return true;
}
