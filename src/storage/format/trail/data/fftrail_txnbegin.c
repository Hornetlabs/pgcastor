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
#include "storage/trail/data/fftrail_txnbegin.h"

/*
 * Serialize transaction begin marker
 * No parser available for this type yet
 */
bool fftrail_txnbegin_serial(void* data, void* state)
{
    /*
     * Record format:
     *  GroupToken
     *  RecHead
     *  RecData
     *      1 byte content, meaningless
     *  RecTail
     */

    uint8         nop = 1;
    int           hdrlen = 0;
    uint32        tlen = 0;

    uint8*        uptr = NULL;
    ff_txndata*   txndata = NULL;
    file_buffer*  fbuffer = NULL;
    ffsmgr_state* ffstate = NULL; /* state data info */

    txndata = (ff_txndata*)data;
    ffstate = (ffsmgr_state*)state;

    /* Validate and switch block */
    fftrail_serialpreshiftblock(state);
    if (FFSMGR_STATUS_SHIFTFILE == ffstate->status)
    {
        ffstate->status = FFSMGR_STATUS_USED;
    }

    /* Write to trail file */
    fbuffer = file_buffer_getbybufid(ffstate->callback.getfilebuffer(ffstate->privdata), ffstate->bufid);
    ffstate->recptr = fbuffer->data + fbuffer->start;

    /* Calculate length */
    txndata->header.totallength = 1; /* Statement length             */

    /* Set record header info */
    txndata->header.reccount = 1;
    txndata->header.reclength = 0;
    txndata->header.subtype = FF_DATA_TYPE_TXNBEGIN;
    txndata->header.formattype = FF_DATA_FORMATTYPE_WAL;
    txndata->header.dbmdno = 0;
    txndata->header.tbmdno = 0;

    /* Skip record token and header length */
    /* Add offset */
    hdrlen = TOKENHDRSIZE;
    hdrlen += fftrail_data_headlen(ffstate->compatibility);
    fbuffer->start += hdrlen;

    /* Add content */
    fftrail_data_data2buffer(&txndata->header, ffstate, &fbuffer, FTRAIL_TOKENDATATYPE_TINYINT, 1, (uint8*)&nop);

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
    ffstate->recptr = NULL;
    return true;
}

/* Deserialize transaction begin marker */
bool fftrail_txnbegin_deserial(void** data, void* state)
{
    uint8         nop = 0;
    uint8         tokenid = 0;   /* token id */
    uint8         tokeninfo = 0; /* token details */
    uint32        recoffset = 0;
    uint32        dataoffset = 0;
    uint16        subtype = FF_DATA_TYPE_NOP;
    uint32        tokenlen = 0; /* token length */

    uint8*        uptr = NULL;
    uint8*        tokendata = NULL; /* token data area */
    ff_txndata*   txndata = NULL;
    ffsmgr_state* ffstate = NULL;

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

    /* Preserve info, as these data may be cleared in subsequent processing */
    subtype = txndata->header.subtype;

    /* Re-point to header */
    ffstate->recptr = uptr;
    recoffset += (uint16)fftrail_data_headlen(ffstate->compatibility);

    /* Get meaningless data */
    if (false == fftrail_data_buffer2data(&txndata->header,
                                          ffstate,
                                          &recoffset,
                                          &dataoffset,
                                          FTRAIL_TOKENDATATYPE_TINYINT,
                                          1,
                                          (uint8*)&nop))
    {
        return false;
    }

    /* Reset, as subtype value is FF_DATA_SUBTYPE_REC_CONTRECORD when switching block or file */
    txndata->header.subtype = subtype;

    return true;
}
