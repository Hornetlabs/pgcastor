#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "storage/trail/data/fftrail_data.h"
#include "storage/trail/data/fftrail_dbmetadata.h"

/* Database metadata serialization */
bool fftrail_dbmetadata_serial(void* data, void* state)
{
    /*
     * 1. First write data part to buffer
     * 2. Add rectail
     * 3. Write GROUP info
     * 4. Write header
     */
    uint16 reclen = 0; /* Data length */
    int    hdrlen = 0; /* Header length */
    int    tmplen = 0; /* Temp variable used in calculation */

    uint8*         uptr = NULL;
    ff_dbmetadata* ffdbmd = NULL;
    ffsmgr_state*  ffstate = NULL;
    file_buffer*   rfbuffer = NULL;

    /* Type cast */
    ffdbmd = (ff_dbmetadata*)data;
    ffstate = (ffsmgr_state*)state;

    /* Get buffer */
    rfbuffer =
        file_buffer_getbybufid(ffstate->callback.getfilebuffer(ffstate->privdata), ffstate->bufid);

    /* Set data to buffer */
    ffstate->recptr = uptr = rfbuffer->data + rfbuffer->start;

    /* Increase offset */
    hdrlen = TOKENHDRSIZE;
    hdrlen += fftrail_data_headlen(ffstate->compatibility);

    /* Data offset */
    uptr += hdrlen;

    /* Fill data */
    /* Database number */
    CONCAT(put, 16bit)(&uptr, ffdbmd->dbmdno);
    reclen += 2;

    /* Database unique identifier */
    CONCAT(put, 32bit)(&uptr, ffdbmd->oid);
    reclen += 4;

    /* Database name */
    tmplen = strlen(ffdbmd->dbname);
    CONCAT(put, 16bit)(&uptr, tmplen);
    reclen += 2;
    rmemcpy1(uptr, 0, ffdbmd->dbname, tmplen);
    uptr += tmplen;
    reclen += tmplen;

    /* Database encoding */
    tmplen = strlen(ffdbmd->charset);
    CONCAT(put, 16bit)(&uptr, tmplen);
    reclen += 2;
    rmemcpy1(uptr, 0, ffdbmd->charset, tmplen);
    uptr += tmplen;
    reclen += tmplen;

    /* Timezone */
    tmplen = strlen(ffdbmd->timezone);
    CONCAT(put, 16bit)(&uptr, tmplen);
    reclen += 2;
    rmemcpy1(uptr, 0, ffdbmd->timezone, tmplen);
    uptr += tmplen;
    reclen += tmplen;

    /* Currency */
    tmplen = strlen(ffdbmd->money);
    CONCAT(put, 16bit)(&uptr, tmplen);
    reclen += 2;
    rmemcpy1(uptr, 0, ffdbmd->money, tmplen);
    uptr += tmplen;
    reclen += tmplen;

    ffdbmd->header.reclength = reclen;
    ffdbmd->header.totallength = reclen;

    /* Increase rectail */
    FTRAIL_GROUP2BUFFER(put, TRAIL_TOKENDATA_RECTAIL, FFTRAIL_INFOTYPE_TOKEN, 0, uptr)
    reclen += TOKENHDRSIZE;

    /* Total length info, includes group length */
    reclen += hdrlen;

    /* Record total length */
    reclen = MAXALIGN(reclen);

    rfbuffer->start += reclen;

    /* Increase GROUP info */
    FTRAIL_GROUP2BUFFER(put, FFTRAIL_GROUPTYPE_DATA, FFTRAIL_INFOTYPE_GROUP, reclen,
                        ffstate->recptr)

    /* Increase header info */
    fftrail_data_hdrserail(&ffdbmd->header, ffstate);

    ffstate->recptr = NULL;
    return true;
}

/* Database metadata deserialization */
bool fftrail_dbmetadata_deserial(void** data, void* state)
{
    uint8  tokenid = 0;   /* token ID */
    uint8  tokeninfo = 0; /* token info */
    uint16 blkoffset = 0;
    uint16 tmplen = 0;       /* Data temp length */
    uint32 tokenlen = 0;     /* Token length */
    uint8* tokendata = NULL; /* Token data area */

    uint8*         uptr = NULL;
    ff_dbmetadata* ffdbmd = NULL;
    ffsmgr_state*  ffstate = NULL;

    /* Type cast */
    ffstate = (ffsmgr_state*)state;

    /* Assemble content in buffer */
    uptr = ffstate->recptr;

    /* Allocate space */
    ffdbmd = (ff_dbmetadata*)rmalloc0(sizeof(ff_dbmetadata));
    if (NULL == ffdbmd)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ffdbmd, 0, '\0', sizeof(ff_dbmetadata));
    *data = ffdbmd;

    /* Get header token */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    if (FFTRAIL_GROUPTYPE_DATA != tokenid || FFTRAIL_INFOTYPE_GROUP != tokeninfo)
    {
        /* make gcc happy */
        uptr = tokendata;
        elog(RLOG_ERROR, "trail file format error");
    }

    /* Parse header data */
    blkoffset = TOKENHDRSIZE;

    /* Parse header data */
    uptr = ffstate->recptr;
    ffstate->recptr += blkoffset;
    fftrail_data_hdrdeserail(&ffdbmd->header, ffstate);
    ffstate->recptr = uptr;
    blkoffset += fftrail_data_headlen(ffstate->compatibility);

    /* Parse real data */
    uptr += blkoffset;

    /* Get database number */
    ffdbmd->dbmdno = CONCAT(get, 16bit)(&uptr);

    /* Database unique identifier */
    ffdbmd->oid = CONCAT(get, 32bit)(&uptr);

    /* Get database name */
    tmplen = CONCAT(get, 16bit)(&uptr);
    ffdbmd->dbname = (char*)rmalloc0(tmplen + 1);
    if (NULL == ffdbmd->dbname)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ffdbmd->dbname, 0, '\0', (tmplen + 1));
    rmemcpy0(ffdbmd->dbname, 0, uptr, tmplen);
    ffdbmd->dbname[tmplen] = '\0';
    uptr += tmplen;

    /* Get database encoding */
    tmplen = CONCAT(get, 16bit)(&uptr);
    ffdbmd->charset = (char*)rmalloc0(tmplen + 1);
    if (NULL == ffdbmd->charset)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ffdbmd->charset, 0, '\0', (tmplen + 1));
    rmemcpy0(ffdbmd->charset, 0, uptr, tmplen);
    ffdbmd->charset[tmplen] = '\0';
    uptr += tmplen;

    /* Get database timezone */
    tmplen = CONCAT(get, 16bit)(&uptr);
    ffdbmd->timezone = (char*)rmalloc0(tmplen + 1);
    if (NULL == ffdbmd->timezone)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ffdbmd->timezone, 0, '\0', (tmplen + 1));
    rmemcpy0(ffdbmd->timezone, 0, uptr, tmplen);
    ffdbmd->timezone[tmplen] = '\0';
    uptr += tmplen;

    /* Get database currency */
    tmplen = CONCAT(get, 16bit)(&uptr);
    ffdbmd->money = (char*)rmalloc0(tmplen + 1);
    if (NULL == ffdbmd->money)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ffdbmd->money, 0, '\0', (tmplen + 1));
    rmemcpy0(ffdbmd->money, 0, uptr, tmplen);
    ffdbmd->money[tmplen] = '\0';
    uptr += tmplen;

    /* Get tail */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)

    /* Log level is debug */
    if (RLOG_DEBUG == g_loglevel)
    {
        /* Output debug log */
        elog(RLOG_DEBUG, "----------Trail MetaDB Begin----------------");
        elog(RLOG_DEBUG, "dbmdno:           %u", ffdbmd->header.dbmdno);
        elog(RLOG_DEBUG, "tbmdno:           %u", ffdbmd->header.tbmdno);
        elog(RLOG_DEBUG, "transid:          %lu", ffdbmd->header.transid);
        elog(RLOG_DEBUG, "transind:         %u", ffdbmd->header.transind);
        elog(RLOG_DEBUG, "totallength:      %lu", ffdbmd->header.totallength);
        elog(RLOG_DEBUG, "reclength:        %u", ffdbmd->header.reclength);
        elog(RLOG_DEBUG, "reccount:         %u", ffdbmd->header.reccount);
        elog(RLOG_DEBUG, "formattype:       %u", ffdbmd->header.formattype);
        elog(RLOG_DEBUG, "subtype:          %u", ffdbmd->header.subtype);
        elog(RLOG_DEBUG, "dbmdno:           %u", ffdbmd->dbmdno);
        elog(RLOG_DEBUG, "oid:              %u", ffdbmd->oid);
        elog(RLOG_DEBUG, "dbname:           %s", ffdbmd->dbname);
        elog(RLOG_DEBUG, "charset:          %s", ffdbmd->charset);
        elog(RLOG_DEBUG, "timezone:         %s", ffdbmd->timezone);
        elog(RLOG_DEBUG, "money:            %s", ffdbmd->money);
        elog(RLOG_DEBUG, "----------Trail MetaDB   End----------------");
    }

    return true;
}
