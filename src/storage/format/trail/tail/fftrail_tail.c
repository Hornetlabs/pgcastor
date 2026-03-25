#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "stmts/txnstmt.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "storage/trail/tail/fftrail_tail.h"

typedef enum TRAIL_TAIL_TOKEN
{
    TRAIL_TAIL_TOKEN_NEXTTRAILNO = 0x00
} trail_tail_token;

/* Serialize tail info */
bool fftrail_tail_serail(void* data, void* state)
{
    uint32        reclen = 0;
    uint8*        uptr = NULL;
    ff_tail*      fftrail = NULL;
    file_buffer*  rfbuffer = NULL;
    ffsmgr_state* ffstate = NULL;

    /* Force cast */
    fftrail = (ff_tail*)data;
    ffstate = (ffsmgr_state*)state;

    /* Get buffer */
    rfbuffer =
        file_buffer_getbybufid(ffstate->callback.getfilebuffer(ffstate->privdata), ffstate->bufid);
    uptr = rfbuffer->data + rfbuffer->start;

    /* Add data */
    /* Offset out header info */
    uptr += TOKENHDRSIZE;

    /* Add token content */
    uptr = fftrail_token2buffer(TRAIL_TAIL_TOKEN_NEXTTRAILNO, FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_BIGINT, 8, (uint8*)&fftrail->nexttrailno,
                                &reclen, uptr);

    /* Add rectail */
    uptr = rfbuffer->data + rfbuffer->start;
    reclen += TOKENHDRSIZE;
    reclen = MAXALIGN(reclen);
    FTRAIL_GROUP2BUFFER(put, FFTRAIL_GROUPTYPE_FTAIL, FFTRAIL_INFOTYPE_GROUP, reclen, uptr)

    rfbuffer->start += reclen;

    return true;
}

/* Deserialize tail info */
bool fftrail_tail_deserail(void** data, void* state)
{
    uint8  tokenid = 0;   /* token id */
    uint8  tokeninfo = 0; /* token details */
    uint32 tokenlen = 0;  /* token length */

    uint8*        uptr = NULL;
    uint8*        tokendata = NULL; /* token data area */
    ff_tail*      taildata = NULL;
    ffsmgr_state* ffstate = NULL;

    /* Type cast */
    ffstate = (ffsmgr_state*)state;
    uptr = ffstate->recptr;

    /* Allocate space */
    taildata = (ff_tail*)rmalloc0(sizeof(ff_tail));
    if (NULL == taildata)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(taildata, 0, '\0', sizeof(ff_tail));
    *data = taildata;

    taildata->nexttrailno = 0;

    /* Get header id */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    if (FFTRAIL_GROUPTYPE_FTAIL != tokenid || FFTRAIL_INFOTYPE_GROUP != tokeninfo)
    {
        /* make gcc happy */
        uptr = tokendata;
        elog(RLOG_ERROR, "trail file data format error");
    }

    /* Parse header data */
    uptr = tokendata;

    /* Get header id */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    if (TRAIL_TAIL_TOKEN_NEXTTRAILNO != tokenid || FFTRAIL_INFOTYPE_TOKEN != tokeninfo)
    {
        /* make gcc happy */
        uptr = tokendata;
        elog(RLOG_ERROR, "trail file data format error");
    }
    uptr = tokendata;

    taildata->nexttrailno = CONCAT(get, 64bit)(&uptr);
    return true;
}
