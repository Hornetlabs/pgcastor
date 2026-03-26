#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/uuid.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "stmts/txnstmt.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "storage/trail/data/fftrail_data.h"
#include "storage/trail/data/fftrail_txnonlinerefreshabandon.h"

/* Serialize online refresh statement */
bool fftrail_txnonlinerefreshabandon_serial(void* data, void* state)
{
    /*
     * Record format:
     *  GroupToken
     *  RecHead
     *  RecData
     *      uuid            16 bytes
     *  RecTail
     */
    int           hdrlen = 0;
    uint32        tlen = 0;

    uint8*        uptr = NULL;
    List*         uuid_list = NULL;
    ListCell*     lc = NULL;
    uuid_t*       uuid = NULL;
    txnstmt*      rstmt = NULL; /* Content to write to trail file */
    ff_txndata*   txndata = NULL;
    file_buffer*  fbuffer = NULL;
    ffsmgr_state* ffstate = NULL; /* state data info */

    txndata = (ff_txndata*)data;
    rstmt = (txnstmt*)txndata->data;
    ffstate = (ffsmgr_state*)state;

    uuid_list = (List*)rstmt->stmt;

    /* Validate and switch block */
    fftrail_serialpreshiftblock(state);
    if (FFSMGR_STATUS_SHIFTFILE == ffstate->status)
    {
        ffstate->status = FFSMGR_STATUS_USED;
    }

    /* Write refresh to trail file */
    fbuffer =
        file_buffer_getbybufid(ffstate->callback.getfilebuffer(ffstate->privdata), ffstate->bufid);
    ffstate->recptr = fbuffer->data + fbuffer->start;

    /* Calculate length */
    txndata->header.totallength = (uuid_list->length * UUID_LEN);

    /* Set record header info */
    txndata->header.reccount = 1;
    txndata->header.reclength = 0;
    txndata->header.subtype = FF_DATA_TYPE_ONLINEREFRESH_ABANDON;
    txndata->header.formattype = FF_DATA_FORMATTYPE_WAL;
    txndata->header.dbmdno = 0;
    txndata->header.tbmdno = 0;
    txndata->header.orgpos = rstmt->extra0.wal.lsn;

    /* Skip record token and header length */
    /* Add offset */
    hdrlen = TOKENHDRSIZE;
    hdrlen += fftrail_data_headlen(ffstate->compatibility);
    fbuffer->start += hdrlen;

    foreach (lc, uuid_list)
    {
        uuid = (uuid_t*)lfirst(lc);

        /* online refresh uuid */
        fftrail_data_data2buffer(
            &txndata->header, ffstate, &fbuffer, FTRAIL_TOKENDATATYPE_STR, 16, (uint8*)uuid->data);
    }

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

/* Deserialize online refresh info */
bool fftrail_txnonlinerefreshabandon_deserial(void** data, void* state)
{
    uint8         tokenid = 0;   /* token id */
    uint8         tokeninfo = 0; /* token details */
    uint32        recoffset = 0;
    uint32        dataoffset = 0;
    uint16        subtype = FF_DATA_TYPE_NOP;
    uint32        tokenlen = 0; /* token length */
    uint64        totallen = 0;

    uint8*        uptr = NULL;
    uint8*        tokendata = NULL; /* token data area */
    List*         uuid_list = NULL;
    uuid_t*       uuid = NULL;
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
    rstmt = (txnstmt*)rmalloc0(sizeof(txnstmt));
    if (NULL == rstmt)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(rstmt, 0, '\0', sizeof(txnstmt));
    txndata->data = (void*)rstmt;
    rstmt->type = TXNSTMT_TYPE_ONLINEREFRESHABANDON;
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

    /* Preserve info, as these data may be cleared in subsequent processing */
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
        uuid = uuid_init();
        if (NULL == uuid)
        {
            return false;
        }

        /* Get uuid */
        if (false == fftrail_data_buffer2data(&txndata->header,
                                              ffstate,
                                              &recoffset,
                                              &dataoffset,
                                              FTRAIL_TOKENDATATYPE_STR,
                                              16,
                                              (uint8*)uuid->data))
        {
            return false;
        }
        totallen -= 16;

        uuid_list = lappend(uuid_list, uuid);
    }
    rstmt->stmt = (void*)uuid_list;

    /* Reset, as subtype becomes FF_DATA_SUBTYPE_REC_CONTRECORD when switching block or file */
    txndata->header.subtype = subtype;
    return true;
}
