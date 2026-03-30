#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "storage/trail/head/fftrail_head.h"

typedef enum TRAIL_HEAD_TOKEN
{
    TRAIL_HEAD_TOKEN_VERSION = 0x00,
    TRAIL_HEAD_TOKEN_DBTYPE = 0x01,
    TRAIL_HEAD_TOKEN_DBVERSION = 0x02,
    TRAIL_HEAD_TOKEN_REDOLSN = 0x03,
    TRAIL_HEAD_TOKEN_RESTARTLSN = 0x04,
    TRAIL_HEAD_TOKEN_CONFIRMLSN = 0x05,
    TRAIL_HEAD_TOKEN_FILENAME = 0x06,
    TRAIL_HEAD_TOKEN_FILESIZE = 0x07,
    TRAIL_HEAD_TOKEN_COMPATIBILITY = 0x08,
    TRAIL_HEAD_TOKEN_ENCRYPTION = 0x09,
    TRAIL_HEAD_TOKEN_STARTXID = 0x0A,
    TRAIL_HEAD_TOKEN_ENDXID = 0x0B,
    TRAIL_HEAD_TOKEN_MAGIC = 0xFF /* always at the end */
} trail_head_token;

/* serialize header info */
bool fftrail_head_serail(void* data, void* state)
{
    uint32        len = 0;
    uint8*        uptr = NULL;
    ff_header*    ffheader = NULL;
    file_buffer*  rfbuffer = NULL;
    ffsmgr_state* ffstate = NULL;

    ffheader = (ff_header*)data;
    ffstate = (ffsmgr_state*)state;

    /* get buffer */
    rfbuffer = file_buffer_getbybufid(ffstate->callback.getfilebuffer(ffstate->privdata), ffstate->bufid);

    /* assemble content into buffer */
    uptr = rfbuffer->data + rfbuffer->start;

    /* add file header info */
    /* group info, add last */
    len = TOKENHDRSIZE;
    uptr += TOKENHDRSIZE;

    /* fill ffheader content */
    /* add version */
    uptr = fftrail_token2buffer(TRAIL_HEAD_TOKEN_VERSION,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_STR,
                                (uint16)strlen(ffheader->version),
                                (uint8*)ffheader->version,
                                &len,
                                uptr);

    /* add dbtype */
    uptr = fftrail_token2buffer(TRAIL_HEAD_TOKEN_DBTYPE,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_INT,
                                sizeof(ffheader->dbtype),
                                (uint8*)&ffheader->dbtype,
                                &len,
                                uptr);

    /* add dbversion */
    uptr = fftrail_token2buffer(TRAIL_HEAD_TOKEN_DBVERSION,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_STR,
                                strlen(ffheader->dbversion),
                                (uint8*)ffheader->dbversion,
                                &len,
                                uptr);

    /* add redolsn */
    uptr = fftrail_token2buffer(TRAIL_HEAD_TOKEN_REDOLSN,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_BIGINT,
                                sizeof(ffheader->redolsn),
                                (uint8*)&ffheader->redolsn,
                                &len,
                                uptr);

    /* add restartlsn */
    uptr = fftrail_token2buffer(TRAIL_HEAD_TOKEN_RESTARTLSN,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_BIGINT,
                                sizeof(ffheader->restartlsn),
                                (uint8*)&ffheader->restartlsn,
                                &len,
                                uptr);

    /* add confirmlsn */
    uptr = fftrail_token2buffer(TRAIL_HEAD_TOKEN_CONFIRMLSN,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_BIGINT,
                                sizeof(ffheader->confirmlsn),
                                (uint8*)&ffheader->confirmlsn,
                                &len,
                                uptr);

    /* add filename */
    uptr = fftrail_token2buffer(TRAIL_HEAD_TOKEN_FILENAME,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_STR,
                                strlen(ffheader->filename),
                                (uint8*)ffheader->filename,
                                &len,
                                uptr);

    /* add filesize */
    uptr = fftrail_token2buffer(TRAIL_HEAD_TOKEN_FILESIZE,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_BIGINT,
                                sizeof(ffheader->filesize),
                                (uint8*)&ffheader->filesize,
                                &len,
                                uptr);

    /* add compatibility */
    uptr = fftrail_token2buffer(TRAIL_HEAD_TOKEN_COMPATIBILITY,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_INT,
                                sizeof(ffheader->compatibility),
                                (uint8*)&ffheader->compatibility,
                                &len,
                                uptr);

    /* add encryption */
    uptr = fftrail_token2buffer(TRAIL_HEAD_TOKEN_ENCRYPTION,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_INT,
                                sizeof(ffheader->encryption),
                                (uint8*)&ffheader->encryption,
                                &len,
                                uptr);

    /* add startxid */
    uptr = fftrail_token2buffer(TRAIL_HEAD_TOKEN_STARTXID,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_BIGINT,
                                sizeof(ffheader->startxid),
                                (uint8*)&ffheader->startxid,
                                &len,
                                uptr);

    /* add endxid */
    uptr = fftrail_token2buffer(TRAIL_HEAD_TOKEN_ENDXID,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_BIGINT,
                                sizeof(ffheader->endxid),
                                (uint8*)&ffheader->endxid,
                                &len,
                                uptr);

    /* add magic identifier */
    ffheader->magic = FTRAIL_MAGIC;
    uptr = fftrail_token2buffer(TRAIL_HEAD_TOKEN_MAGIC,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_INT,
                                4,
                                (uint8*)&ffheader->magic,
                                &len,
                                uptr);

    /* byte alignment */
    uptr = rfbuffer->data + rfbuffer->start;
    FTRAIL_GROUP2BUFFER(put, FFTRAIL_GROUPTYPE_FHEADER, FFTRAIL_INFOTYPE_GROUP, len, uptr)

    /* no offset here, offset after getting position info */
    len = MAXALIGN(len);
    rfbuffer->start += len;
    return true;
}

/* deserialize info */
bool fftrail_head_deserail(void** data, void* state)
{
    uint8         tokenid = 0;      /* token identifier */
    uint8         tokeninfo = 0;    /* token details */
    uint32        tokenlen = 0;     /* token length */
    uint8*        tokendata = NULL; /* token data area */
    uint8*        uptr = NULL;
    ff_header*    ffheader = NULL;
    ffsmgr_state* ffstate = NULL;

    /* get buffer */
    ffstate = (ffsmgr_state*)state;

    /* assemble content in buffer */
    uptr = ffstate->recptr;

    /* allocate space */
    ffheader = (ff_header*)rmalloc0(sizeof(ff_header));
    if (NULL == ffheader)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ffheader, 0, '\0', sizeof(ff_header));
    *data = ffheader;

    /* get header identifier */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    if (FFTRAIL_GROUPTYPE_FHEADER != tokenid || FFTRAIL_INFOTYPE_GROUP != tokeninfo)
    {
        elog(RLOG_ERROR, "trail file format error");
    }

    /* reset position */
    uptr = ffstate->recptr;
    uptr += TOKENHDRSIZE;

    /* get version */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->version = (char*)rmalloc0(tokenlen);
    if (NULL == ffheader->version)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ffheader->version, 0, '\0', tokenlen);
    rmemcpy0(ffheader->version, 0, tokendata, tokenlen - TOKENHDRSIZE);

    /* get dbtype */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->dbtype = CONCAT(get, 32bit)(&tokendata);

    /* get dbversion */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->dbversion = (char*)rmalloc0(tokenlen);
    if (NULL == ffheader->dbversion)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ffheader->dbversion, 0, '\0', tokenlen);
    rmemcpy0(ffheader->dbversion, 0, tokendata, tokenlen - TOKENHDRSIZE);

    /* get redolsn */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->redolsn = CONCAT(get, 64bit)(&tokendata);

    /* get restartlsn */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->restartlsn = CONCAT(get, 64bit)(&tokendata);

    /* get confirmlsn */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->confirmlsn = CONCAT(get, 64bit)(&tokendata);

    /* get filename */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->filename = (char*)rmalloc0(tokenlen);
    if (NULL == ffheader->filename)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ffheader->filename, 0, '\0', tokenlen);
    rmemcpy0(ffheader->filename, 0, tokendata, tokenlen - TOKENHDRSIZE);

    /* get filesize */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->filesize = CONCAT(get, 64bit)(&tokendata);

    /* get file compatibility version */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->compatibility = CONCAT(get, 32bit)(&tokendata);

    /* get encryption */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->encryption = CONCAT(get, 32bit)(&tokendata);

    /* get startxid */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->startxid = CONCAT(get, 64bit)(&tokendata);

    /* get endxid */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->endxid = CONCAT(get, 64bit)(&tokendata);

    /* get magic number */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->magic = CONCAT(get, 32bit)(&tokendata);

    return true;
}
