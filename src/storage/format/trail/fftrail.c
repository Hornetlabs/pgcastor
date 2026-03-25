#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/algorithm/crc/crc_check.h"
#include "misc/misc_control.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/cache_sysidcts.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/transcache.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "storage/trail/head/fftrail_head.h"
#include "storage/trail/data/fftrail_data.h"
#include "storage/trail/tail/fftrail_tail.h"
#include "storage/trail/reset/fftrail_reset.h"
#include "storage/trail/data/fftrail_dbmetadata.h"
#include "parser/trail/parsertrail.h"
#include "parser/trail/head/parsertrail_head.h"
#include "parser/trail/data/parsertrail_dbmetadata.h"

typedef bool (*serialtoken)(void* data, void* state);

typedef bool (*deserialtoken)(void** data, void* state);

typedef struct TRAIL_GROUP
{
    int8          groupid;      /* group id                     */
    char*         desc;         /* Description                          */
    serialtoken   serialfunc;   /* Serialize struct data to buffer   */
    deserialtoken deserialfunc; /* Deserialize buffer data to struct   */
} trail_group;

static trail_group m_trailgroups[] = {
    {FFTRAIL_GROUPTYPE_NOP, "trail nop", NULL, NULL},
    {FFTRAIL_GROUPTYPE_FHEADER, "trail file header", fftrail_head_serail, fftrail_head_deserail},
    {FFTRAIL_GROUPTYPE_DATA, "trail file data", fftrail_data_serail, fftrail_data_deserail},
    {FFTRAIL_GROUPTYPE_RESET, "trail file reset", fftrail_reset_serail, fftrail_reset_deserail},
    {FFTRAIL_GROUPTYPE_FTAIL, "trail file tail", fftrail_tail_serail, fftrail_tail_deserail}};

/* Validate data integrity */
/* Validate header integrity */
static bool fftrail_validfhead(uint8* header)
{
    uint8  tokenid = FFTRAIL_GROUPTYPE_NOP;
    uint8  tokeninfo = FFTRAIL_INFOTYPE_TOKEN;
    uint32 tokenlen = 0;
    uint32 tfmagic = 0;

    uint8* uptr = NULL;
    uint8* tokendata = NULL;

    /* Parse header info */
    uptr = header;
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    if (FFTRAIL_GROUPTYPE_FHEADER != tokenid || FFTRAIL_INFOTYPE_GROUP != tokeninfo)
    {
        return false;
    }

    /* Check if last four bytes are magic */
    uptr = header;

    tokenlen -= TOKENHDRSIZE;
    tokenlen -= 4;

    tokendata += tokenlen;

    /* Magic number */
    tfmagic = CONCAT(get, 32bit)(&tokendata);
    if (tfmagic != FTRAIL_MAGIC)
    {
        /* Most likely flush not completed */
        return false;
    }

    return true;
}

/* Validate data integrity */
static bool fftrail_validdata(int compatibility, uint8* data)
{
    /*
     * 1. Get complete record data
     * 2. Get reclength from header
     * 3. Get record tail
     */
    uint8    tokenid = FFTRAIL_GROUPTYPE_NOP;
    uint8    tokeninfo = FFTRAIL_INFOTYPE_TOKEN;
    int      headlen = 0;
    uint16   reclength = 0;
    uint32   tokenlen = 0;
    r_crc32c crc32 = 0;
    r_crc32c crc32rec = 0;

    uint8* uptr = NULL;
    uint8* crcuptr = NULL;
    uint8* tokendata = NULL;

    /* Get record data */
    uptr = data;
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    if (FFTRAIL_GROUPTYPE_DATA != tokenid || FFTRAIL_INFOTYPE_GROUP != tokeninfo)
    {
        elog(RLOG_WARNING, "1");
        return false;
    }

    /* Offset group header length */
    crcuptr = uptr = tokendata;
    reclength = fftrail_data_getreclengthfromhead(compatibility, uptr);
    if (0 == reclength)
    {
        elog(RLOG_WARNING, "2");
        return false;
    }

    /* Offset record content */
    headlen = fftrail_data_headlen(0);
    uptr += headlen;

    /* Calculate crc */
    headlen -= (TOKENHDRSIZE + 4);
    INIT_CRC32C(crc32);
    COMP_CRC32C(crc32, crcuptr, headlen);

    /* Get crc from record */
    crcuptr += headlen;
    FTRAIL_BUFFER2TOKEN(get, crcuptr, tokenid, tokeninfo, tokenlen, tokendata)
    if (TRAIL_TOKENDATAHDR_ID_CRC32 != tokenid || FFTRAIL_INFOTYPE_TOKEN != tokeninfo)
    {
        elog(RLOG_WARNING, "invalid record, hope TRAIL_TOKENDATAHDR_ID_CRC32:%u, got:%u",
             TRAIL_TOKENDATAHDR_ID_CRC32, tokenid);
        return false;
    }
    crc32rec = CONCAT(get, 32bit)(&tokendata);

    /* Calculate data crc32 */
    COMP_CRC32C(crc32, uptr, reclength);
    FIN_CRC32C(crc32);

    if (!EQ_CRC32C(crc32rec, crc32))
    {
        elog(RLOG_WARNING, "trail record crc error, crc in record %08X, calc crc:%08X", crc32rec,
             crc32);
        return false;
    }

    /* Get rectail info */
    uptr += reclength;

    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    if (TRAIL_TOKENDATA_RECTAIL != tokenid || FFTRAIL_INFOTYPE_TOKEN != tokeninfo)
    {
        elog(RLOG_WARNING, "4,reclength:%u", reclength);
        return false;
    }

    return true;
}

/* Validate file RESET integrity */
static bool fftrail_validreset(int compatibility, uint64 fileid, uint8* tail)
{
    uint8  tokenid = FFTRAIL_GROUPTYPE_NOP;
    uint8  tokeninfo = FFTRAIL_INFOTYPE_TOKEN;
    uint32 tokenlen = 0;
    uint64 nfileid = 0;
    uint8* uptr = NULL;
    uint8* tokendata = NULL;

    /* Get record data */
    uptr = tail;
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    if (FFTRAIL_GROUPTYPE_RESET != tokenid || FFTRAIL_INFOTYPE_GROUP != tokeninfo)
    {
        return false;
    }

    /* Validate length by version */
    if (tokenlen != fftrail_resetlen(compatibility))
    {
        return false;
    }

    /* Get content */
    uptr = tokendata;
    uptr += TOKENHDRSIZE;

    nfileid = CONCAT(get, 64bit)(&uptr);
    if ((fileid + 1) != nfileid)
    {
        return false;
    }

    return true;
}

/* Validate file tail integrity */
static bool fftrail_validftail(int compatibility, uint64 fileid, uint8* tail)
{
    uint8  tokenid = FFTRAIL_GROUPTYPE_NOP;
    uint8  tokeninfo = FFTRAIL_INFOTYPE_TOKEN;
    uint32 tokenlen = 0;
    uint64 nfileid = 0;
    uint8* uptr = NULL;
    uint8* tokendata = NULL;

    /* Get record data */
    uptr = tail;
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    if (FFTRAIL_GROUPTYPE_FTAIL != tokenid || FFTRAIL_INFOTYPE_GROUP != tokeninfo)
    {
        return false;
    }

    /* Validate length by version */
    if (tokenlen != fftrail_taillen(compatibility))
    {
        return false;
    }

    /* Get content */
    uptr = tokendata;
    uptr += TOKENHDRSIZE;

    nfileid = CONCAT(get, 64bit)(&uptr);
    if ((fileid + 1) != nfileid)
    {
        return false;
    }

    return true;
}

bool fftrail_validrecord(ff_cxt_type type, void* state, uint8 infotype, uint64 fileid,
                         uint8* record)
{
    bool          result = false;
    ffsmgr_state* ffstate = NULL;
    ffstate = (ffsmgr_state*)state;

    if (FFTRAIL_INFOTYPE_GROUP != infotype)
    {
        return result;
    }

    switch (type)
    {
        case FFTRAIL_GROUPTYPE_FHEADER:
            result = fftrail_validfhead(record);
            break;
        case FFTRAIL_GROUPTYPE_DATA:
            result = fftrail_validdata(ffstate->compatibility, record);
            break;
        case FFTRAIL_GROUPTYPE_RESET:
            result = fftrail_validreset(ffstate->compatibility, fileid, record);
            break;
        case FFTRAIL_GROUPTYPE_FTAIL:
            result = fftrail_validftail(ffstate->compatibility, fileid, record);
            break;
        default:
            elog(RLOG_WARNING, "unknown group type:%u", type);
            return false;
    }
    return result;
}

/*
 * Serialization
 * Check if block switch is needed before writing data
 *  If block switch is needed, execute switch operation
 *
 * Return value:
 *  false           no switch
 *  true            switched
 */
bool fftrail_serialpreshiftblock(void* state)
{
    bool   shiftfile = false;
    int    minsize = 0;
    int    timeout = 0;
    uint64 freespc = 0;

    file_buffer*  fbuffer = NULL;    /* Cache info */
    file_buffer*  tmpfbuffer = NULL; /* Cache info */
    ffsmgr_state* ffstate = NULL;
    ff_fileinfo*  finfo = NULL; /* File info */
    ff_fileinfo*  tmpfinfo = NULL;
    file_buffers* txn2filebuffer = NULL;

    /* Get info needed for table serialization */
    ffstate = (ffsmgr_state*)state;

    /* Get file_buffers */
    txn2filebuffer = ffstate->callback.getfilebuffer(ffstate->privdata);

    /*
     * Check if space meets minimum requirement, switch buffer if not
     *  1. Size required by token
     *  2. Size required by file tail
     */
    /* Get fbuffer by bufid */
    fbuffer = file_buffer_getbybufid(txn2filebuffer, ffstate->bufid);
    finfo = (ff_fileinfo*)fbuffer->privdata;

    /* Check if fbuffer start is 0, if so, initialize file */
    if (1 == finfo->blknum && 0 == fbuffer->start)
    {
        /* Initialize file header info */
        fftrail_fileinit(state);
    }

    /*
     * Check if space meets minimum requirement, switch buffer if not
     *  1. Size required by token
     *  2. Size required by file tail
     */
    minsize = fftrail_data_tokenminsize(ffstate->compatibility);

    /* Size required for file tail */
    if (finfo->blknum == ffstate->maxbufid)
    {
        shiftfile = true;
        minsize += fftrail_taillen(ffstate->compatibility);
    }

    /* Check remaining space */
    freespc = (fbuffer->maxsize - fbuffer->start);

    if (freespc > minsize)
    {
        return false;
    }

    /* Switch buffer */
    if (true == shiftfile)
    {
        /* Add tail info */
        ff_tail fftail = {0}; /* tail info */
        fftail.nexttrailno = (finfo->fileid + 1);
        ffstate->ffsmgr->ffsmgr_serial(FFTRAIL_CXT_TYPE_FTAIL, &fftail, ffstate);

        /* Cache cleanup */
        fftrail_invalidprivdata(FFSMGR_IF_OPTYPE_SERIAL, ffstate->fdata->ffdata);
    }

    /* Get cache */
    while (1)
    {
        ffstate->bufid = file_buffer_get(txn2filebuffer, &timeout);
        if (INVALID_BUFFERID == ffstate->bufid)
        {
            if (ERROR_TIMEOUT == timeout)
            {
                usleep(10000);
                continue;
            }
            elog(RLOG_WARNING, "get file buffer error");
            return false;
        }
        break;
    }

    /* Get corresponding buffer */
    tmpfbuffer = file_buffer_getbybufid(txn2filebuffer, ffstate->bufid);
    if (NULL == tmpfbuffer->privdata)
    {
        tmpfinfo = (ff_fileinfo*)rmalloc1(sizeof(ff_fileinfo));
        if (NULL == tmpfinfo)
        {
            elog(RLOG_ERROR, "out of memory");
        }
        rmemset0(tmpfinfo, 0, '\0', sizeof(ff_fileinfo));
        tmpfbuffer->privdata = (void*)tmpfinfo;
    }
    else
    {
        tmpfinfo = (ff_fileinfo*)tmpfbuffer->privdata;
    }

    rmemcpy0(tmpfinfo, 0, finfo, sizeof(ff_fileinfo));

    tmpfinfo->fileid = (true == shiftfile ? (finfo->fileid + 1) : finfo->fileid);
    tmpfinfo->blknum = (true == shiftfile ? 1 : (finfo->blknum + 1));

    /* Write buffer to pending flush cache */
    rmemcpy1(&tmpfbuffer->extra, 0, &fbuffer->extra, sizeof(file_buffer_extra));
    file_buffer_waitflush_add(txn2filebuffer, fbuffer);

    /* Reset info in tmpfbuffer */
    tmpfbuffer->flag = FILE_BUFFER_FLAG_DATA;

    finfo = NULL;
    fbuffer = NULL;

    /* Reset */
    finfo = tmpfinfo;
    fbuffer = tmpfbuffer;
    if (true == shiftfile)
    {
        /* Initialize file info */
        fftrail_fileinit(ffstate);
        ffstate->status = FFSMGR_STATUS_SHIFTFILE;
    }
    return true;
}

bool fftrail_serialshiffile(void* state)
{
    int           timeout = 0;
    ff_tail       fftail = {0};      /* tail info */
    file_buffer*  fbuffer = NULL;    /* Cache info */
    file_buffer*  tmpfbuffer = NULL; /* Cache info */
    ffsmgr_state* ffstate = NULL;
    ff_fileinfo*  finfo = NULL; /* File info */
    ff_fileinfo*  tmpfinfo = NULL;
    file_buffers* txn2filebuffer = NULL;

    /* Get info needed for table serialization */
    ffstate = (ffsmgr_state*)state;

    /* Get file_buffers */
    txn2filebuffer = ffstate->callback.getfilebuffer(ffstate->privdata);

    /*
     * Check if space meets minimum requirement, switch buffer if not
     *  1. Size required by token
     *  2. Size required by file tail
     */
    /* Get fbuffer by bufid */
    fbuffer = file_buffer_getbybufid(txn2filebuffer, ffstate->bufid);
    finfo = (ff_fileinfo*)fbuffer->privdata;

    /* Check if fbuffer start is 0, if so, initialize file */
    if (1 == finfo->blknum && 0 == fbuffer->start)
    {
        /* Initialize file header info */
        fftrail_fileinit(state);
    }

    /* Add tail info */
    fftail.nexttrailno = (finfo->fileid + 1);
    ffstate->ffsmgr->ffsmgr_serial(FFTRAIL_CXT_TYPE_FTAIL, &fftail, ffstate);

    /* Cache cleanup */
    fftrail_invalidprivdata(FFSMGR_IF_OPTYPE_SERIAL, ffstate->fdata->ffdata);

    /* Get cache */
    while (1)
    {
        ffstate->bufid = file_buffer_get(txn2filebuffer, &timeout);
        if (INVALID_BUFFERID == ffstate->bufid)
        {
            if (ERROR_TIMEOUT == timeout)
            {
                usleep(10000);
                continue;
            }
            elog(RLOG_WARNING, "get file buffer error");
            return false;
        }
        break;
    }

    /* Get corresponding buffer */
    tmpfbuffer = file_buffer_getbybufid(txn2filebuffer, ffstate->bufid);
    if (NULL == tmpfbuffer->privdata)
    {
        tmpfinfo = (ff_fileinfo*)rmalloc1(sizeof(ff_fileinfo));
        if (NULL == tmpfinfo)
        {
            elog(RLOG_ERROR, "out of memory");
        }
        rmemset0(tmpfinfo, 0, '\0', sizeof(ff_fileinfo));
        tmpfbuffer->privdata = (void*)tmpfinfo;
    }
    else
    {
        tmpfinfo = (ff_fileinfo*)tmpfbuffer->privdata;
    }

    rmemcpy0(tmpfinfo, 0, finfo, sizeof(ff_fileinfo));

    tmpfinfo->fileid = finfo->fileid + 1;
    tmpfinfo->blknum = 1;

    /* Write buffer to pending flush cache */
    rmemcpy1(&tmpfbuffer->extra, 0, &fbuffer->extra, sizeof(file_buffer_extra));
    file_buffer_waitflush_add(txn2filebuffer, fbuffer);
    finfo = NULL;
    fbuffer = NULL;

    /* Reset info in tmpfbuffer */
    tmpfbuffer->flag = FILE_BUFFER_FLAG_DATA;

    /* Initialize file info */
    fftrail_fileinit(ffstate);
    ffstate->status = FFSMGR_STATUS_USED;

    return true;
}

/* Add specific data to buffer */
uint8* fftrail_body2buffer(ftrail_tokendatatype tdtype, uint16 tdatalen, uint8* tdata,
                           uint8* buffer)
{
    uint8* _uptr_ = NULL;

    _uptr_ = buffer;

    switch (tdtype)
    {
        case FTRAIL_TOKENDATATYPE_TINYINT:
            CONCAT(put, 8bit)(&_uptr_, *((uint8*)tdata));
            break;
        case FTRAIL_TOKENDATATYPE_SMALLINT:
            CONCAT(put, 16bit)(&_uptr_, *((uint16*)tdata));
            break;
        case FTRAIL_TOKENDATATYPE_INT:
            CONCAT(put, 32bit)(&_uptr_, *((uint32*)tdata));
            break;
        case FTRAIL_TOKENDATATYPE_BIGINT:
            CONCAT(put, 64bit)(&_uptr_, *((uint64*)tdata));
            break;
        case FTRAIL_TOKENDATATYPE_STR:
            rmemcpy1(_uptr_, 0, (char*)tdata, tdatalen);
            _uptr_ += tdatalen;
            break;
        default:
            elog(RLOG_ERROR, "unknown tokendatatype:%d", tdtype);
            break;
    }
    return _uptr_;
}

/* Write specific data from buffer to data */
uint8* fftrail_buffer2body(ftrail_tokendatatype tdtype, uint64 tdatalen, uint8* tdata,
                           uint8* buffer)
{
    uint8* _uptr_ = NULL;

    _uptr_ = buffer;

    switch (tdtype)
    {
        case FTRAIL_TOKENDATATYPE_TINYINT:
            *tdata = CONCAT(get, 8bit)(&_uptr_);
            break;
        case FTRAIL_TOKENDATATYPE_SMALLINT:
            *((uint16*)tdata) = CONCAT(get, 16bit)(&_uptr_);
            break;
        case FTRAIL_TOKENDATATYPE_INT:
            *((uint32*)tdata) = CONCAT(get, 32bit)(&_uptr_);
            break;
        case FTRAIL_TOKENDATATYPE_BIGINT:
            *((uint64*)tdata) = CONCAT(get, 64bit)(&_uptr_);
            break;
        case FTRAIL_TOKENDATATYPE_STR:
            rmemcpy1(tdata, 0, _uptr_, tdatalen);
            _uptr_ += tdatalen;
            break;
        default:
            elog(RLOG_ERROR, "unknown tokendatatype:%d", tdtype);
            break;
    }
    return _uptr_;
}

/*
 * Serialize data to cache by token
 * Parameter description:
 * Input parameters:
 *      fhdr             Function header identifier
 *      tid              tokenid
 *      tinfo            tokeninfo
 *      tdtype           tokendatatype, see: ftrail_tokendatatype
 *      tdatalen         Data length
 *      tdata            Data
 * Output parameters:
 *      tlen             Total length including token
 *      buffer           Token content saved to this cache, return new address space
 */
uint8* fftrail_token2buffer(uint8 tid, uint8 tinfo, ftrail_tokendatatype tdtype, uint16 tdatalen,
                            uint8* tdata, uint32* tlen, uint8* buffer)
{
    uint32 _tokenlen_ = 0;
    uint8* uptr = NULL;

    uptr = buffer;
    _tokenlen_ += TOKENHDRSIZE;
    _tokenlen_ += tdatalen;

    FTRAIL_TOKENHDR2BUFFER(put, tid, tinfo, _tokenlen_, uptr)

    /* Assemble data */
    uptr = fftrail_body2buffer(tdtype, tdatalen, tdata, uptr);
    *tlen += _tokenlen_;
    return uptr;
}

/*
 * Tail length is fixed
 */
int fftrail_taillen(int compatibility)
{
    /*
     * File tail length varies by version, but must be multiple of 8
     */
    /*
     * 2024.10.15
     * v2.0 length content:
     *   groupid                 1 byte
     *   tokeninfo               1 byte
     *   datalen                 4 bytes
     *   data                    8+6 bytes----next file number
     */
    return 24;
}

/*
 * RESET length is fixed
 */
int fftrail_resetlen(int compatibility)
{
    /*
     * File RESET length varies by version, but must be multiple of 8
     */
    /*
     * 2024.10.15
     * v2.0 length content:
     *   groupid                 1 byte
     *   tokeninfo               1 byte
     *   datalen                 4 bytes
     *   data                    8+6 bytes----next file number
     */
    return 24;
}

/*
 * File header info initialization
 */
void fftrail_fileinit(void* state)
{
    /* Add file header info */
    bool                          found = false;
    Oid                           dbid = 0;
    char*                         dbname = 0;
    file_buffer*                  fbuffer = NULL;
    ffsmgr_state*                 ffstate = NULL;
    ff_header*                    ffheader = NULL; /* trail file header struct */
    ff_dbmetadata*                ffdbmd = NULL;   /* Database info */
    ff_fileinfo*                  finfo = NULL;
    fftrail_privdata*             ffprivdata = NULL;
    fftrail_database_serialentry* dbserialentry = NULL;

    /* Get initialization info */
    ffstate = (ffsmgr_state*)state;
    ffprivdata = (fftrail_privdata*)ffstate->fdata->ffdata;
    fbuffer =
        file_buffer_getbybufid(ffstate->callback.getfilebuffer(ffstate->privdata), ffstate->bufid);
    finfo = (ff_fileinfo*)fbuffer->privdata;

    ffstate->recptr = fbuffer->data + fbuffer->start;
    /* At file start, add file header and dbmetadata info */
    ffheader = ffsmgr_headinit(ffstate->compatibility, InvalidFullTransactionId, finfo->fileid);

    /* Add lsn info */
    ffheader->redolsn = fbuffer->extra.chkpoint.redolsn.wal.lsn;
    ffheader->confirmlsn = fbuffer->extra.rewind.confirmlsn.wal.lsn;
    ffheader->restartlsn = fbuffer->extra.rewind.restartlsn.wal.lsn;

    ffstate->ffsmgr->ffsmgr_serial(FFTRAIL_CXT_TYPE_FHEADER, ffheader, ffstate);

    if (NULL != ffheader->filename)
    {
        rfree(ffheader->filename);
    }

    rfree(ffheader);
    ffheader = NULL;

    /* Add dbmetadata info */
    /* Check if exists */
    dbid = ffstate->callback.getdboid(ffstate->privdata);
    dbname = ffstate->callback.getdbname(ffstate->privdata, dbid);

    if (NULL != ffstate->callback.setdboid)
    {
        ffstate->callback.setdboid(ffstate->privdata, dbid);
        ffstate->callback.setdboid = NULL;
    }

    ffdbmd = ffsmgr_dbmetadatainit(dbname);
    ffdbmd->oid = dbid;
    dbserialentry = hash_search(ffprivdata->databases, &dbid, HASH_ENTER, &found);
    if (false == found)
    {
        /* Add data to hash */
        dbserialentry->no = ffprivdata->dbnum++;
        dbserialentry->oid = dbid;
        rmemcpy1(dbserialentry->database, 0, ffdbmd->dbname, strlen(ffdbmd->dbname));
        ffprivdata->dbentrys = lappend(ffprivdata->dbentrys, dbserialentry);
    }
    ffdbmd->dbmdno = dbserialentry->no;
    ffstate->recptr = fbuffer->data + fbuffer->start;
    ffstate->ffsmgr->ffsmgr_serial(FFTRAIL_CXT_TYPE_DATA, ffdbmd, ffstate);

    rfree(ffdbmd);
    return;
}

/*
 * Private variable cache cleanup in file
 * optype
 *  Identify hash type
 */
void fftrail_invalidprivdata(int optype, void* privdata)
{
    ListCell*         lc = NULL;
    fftrail_privdata* ffprivdata = NULL;

    if (NULL == privdata)
    {
        return;
    }

    /* Clean cache */
    ffprivdata = (fftrail_privdata*)privdata;
    ffprivdata->dbnum = 0;
    ffprivdata->tbnum = 0;

    /* Iterate and delete tables */
    foreach (lc, ffprivdata->tbentrys)
    {
        fftrail_table_serialentry*   tbserialentry = NULL;
        fftrail_table_deserialentry* tbdeserialentry = NULL;
        if (optype == FFSMGR_IF_OPTYPE_SERIAL)
        {
            tbserialentry = (fftrail_table_serialentry*)lfirst(lc);
            hash_search(ffprivdata->tables, &tbserialentry->key, HASH_REMOVE, NULL);
        }
        else if (optype == FFSMGR_IF_OPTYPE_DESERIAL)
        {
            tbdeserialentry = (fftrail_table_deserialentry*)lfirst(lc);
            hash_search(ffprivdata->tables, &tbdeserialentry->key, HASH_REMOVE, NULL);

            /* Free columns */
            if (NULL != tbdeserialentry->columns)
            {
                rfree(tbdeserialentry->columns);
                tbdeserialentry->columns = NULL;
            }
        }
    }
    list_free(ffprivdata->tbentrys);
    ffprivdata->tbentrys = NULL;

    /* Iterate and delete databases */
    foreach (lc, ffprivdata->dbentrys)
    {
        fftrail_database_serialentry*   dbserialentry = NULL;
        fftrail_database_deserialentry* dbdeserialentry = NULL;

        if (optype == FFSMGR_IF_OPTYPE_SERIAL)
        {
            dbserialentry = (fftrail_database_serialentry*)lfirst(lc);
            hash_search(ffprivdata->databases, &dbserialentry->oid, HASH_REMOVE, NULL);
        }
        else if (optype == FFSMGR_IF_OPTYPE_DESERIAL)
        {
            dbdeserialentry = (fftrail_database_deserialentry*)lfirst(lc);
            hash_search(ffprivdata->databases, &dbdeserialentry->no, HASH_REMOVE, NULL);
        }
    }
    list_free(ffprivdata->dbentrys);
    ffprivdata->dbentrys = NULL;
}

/*
 * Private variable cache cleanup in file
 * optype
 *  Identify hash type
 */
static void fftrail_freeprivdata(int optype, void* privdata)
{
    fftrail_privdata* ffprivdata = NULL;
    if (NULL == privdata)
    {
        return;
    }

    fftrail_invalidprivdata(optype, privdata);
    ffprivdata = (fftrail_privdata*)privdata;

    if (NULL != ffprivdata->tables)
    {
        hash_destroy(ffprivdata->tables);
        ffprivdata->tables = NULL;
    }

    if (NULL != ffprivdata->databases)
    {
        hash_destroy(ffprivdata->databases);
        ffprivdata->databases = NULL;
    }
}

/* Serialize data to buffer */
bool fftrail_serial(ff_cxt_type type, void* data, void* state)
{
    return m_trailgroups[type].serialfunc(data, state);
}

/* Deserialize data to data */
bool fftrail_deserial(ff_cxt_type type, void** data, void* state)
{
    return m_trailgroups[type].deserialfunc(data, state);
}

/* Initialization */
bool fftrail_init(int optype, void* state)
{
    HASHCTL           hashCtl = {'\0'};
    fftrail_privdata* privdata = NULL;
    ffsmgr_state*     ffstate = (ffsmgr_state*)state;

    /* Already initialized, no need to initialize again */
    if (NULL != ffstate->fdata)
    {
        return true;
    }

    /* Initialize fdata struct */
    ffstate->fdata = (ffsmgr_fdata*)rmalloc1(sizeof(ffsmgr_fdata));
    if (NULL == ffstate->fdata)
    {
        elog(RLOG_WARNING, "out of memory");
        return false;
    }
    rmemset0(ffstate->fdata, 0, '\0', sizeof(ffsmgr_fdata));

    /* Initialize specific struct */
    privdata = (fftrail_privdata*)rmalloc1(sizeof(fftrail_privdata));
    if (NULL == privdata)
    {
        elog(RLOG_WARNING, "out of memory");
        return false;
    }
    rmemset0(privdata, 0, '\0', sizeof(fftrail_privdata));
    privdata->dbentrys = NULL;
    privdata->tbentrys = NULL;
    privdata->tables = NULL;
    privdata->databases = NULL;
    privdata->dbnum = 0;
    privdata->tbnum = 0;

    /* Content info */
    ffstate->fdata->ffdata = privdata;

    /* Initialize table and database info */
    /* Table info */
    if (optype == FFSMGR_IF_OPTYPE_SERIAL)
    {
        hashCtl.keysize = sizeof(fftrail_table_serialkey);
        hashCtl.entrysize = sizeof(fftrail_table_serialentry);
    }
    else if (optype == FFSMGR_IF_OPTYPE_DESERIAL)
    {
        hashCtl.keysize = sizeof(fftrail_table_deserialkey);
        hashCtl.entrysize = sizeof(fftrail_table_deserialentry);
    }
    privdata->tables =
        hash_create("fftrail_privdata_tables", 128, &hashCtl, HASH_ELEM | HASH_BLOBS);

    /* Database info */
    if (optype == FFSMGR_IF_OPTYPE_SERIAL)
    {
        hashCtl.keysize = sizeof(Oid);
        hashCtl.entrysize = sizeof(fftrail_database_serialentry);
    }
    else if (optype == FFSMGR_IF_OPTYPE_DESERIAL)
    {
        hashCtl.keysize = sizeof(uint32);
        hashCtl.entrysize = sizeof(fftrail_database_deserialentry);
    }
    privdata->databases =
        hash_create("fftrail_privdata_databases", 128, &hashCtl, HASH_ELEM | HASH_BLOBS);

    return true;
}

/* Content release */
void fftrail_free(int optype, void* state)
{
    ffsmgr_state* ffstate = (ffsmgr_state*)state;

    if (NULL == state)
    {
        return;
    }

    if (NULL == ffstate->fdata)
    {
        /* fdata content release */
        return;
    }

    /* ffdata content release */
    fftrail_freeprivdata(optype, ffstate->fdata->ffdata);
    rfree(ffstate->fdata->ffdata);
    ffstate->fdata->ffdata = NULL;

    /* ffdata2 content release */
    if (FFSMGR_IF_OPTYPE_DESERIAL == optype && NULL != ffstate->fdata->ffdata2)
    {
        fftrail_freeprivdata(optype, ffstate->fdata->ffdata2);
        rfree(ffstate->fdata->ffdata2);
    }
    rfree(ffstate->fdata);
    ffstate->fdata = NULL;
}

/* Get subtype from record */
bool fftrail_getrecordsubtype(void* state, uint8* record, uint16* subtype)
{
    ffsmgr_state* ffstate = NULL;

    ffstate = (ffsmgr_state*)state;

    /* Skip token part */
    record += TOKENHDRSIZE;
    *subtype = fftrail_data_getsubtypefromhead(ffstate->compatibility, record);

    return true;
}

/* Get length from record header */
uint64 fftrail_getrecordlsn(void* state, uint8* record)
{
    ffsmgr_state* ffstate = NULL;

    ffstate = (ffsmgr_state*)state;

    /* Skip token part */
    record += TOKENHDRSIZE;
    return fftrail_data_getorgposfromhead(ffstate->compatibility, record);
}

/* Get offset of real data based on record */
uint16 fftrail_getrecorddataoffset(int compatibility)
{
    uint16 dataoffset = 0;

    /* Skip token part */
    dataoffset = fftrail_data_getrecorddataoffset(compatibility);
    dataoffset += TOKENHDRSIZE;

    return dataoffset;
}

/* Get total length */
uint64 fftrail_getrecordtotallength(void* state, uint8* record)
{
    ffsmgr_state* ffstate = NULL;

    ffstate = (ffsmgr_state*)state;

    /* Skip token part */
    record += TOKENHDRSIZE;
    return fftrail_data_gettotallengthfromhead(ffstate->compatibility, record);
}

/* Get record length */
uint16 fftrail_getrecordlength(void* state, uint8* record)
{
    ffsmgr_state* ffstate = NULL;

    ffstate = (ffsmgr_state*)state;

    /* Skip token part */
    record += TOKENHDRSIZE;
    return fftrail_data_getreclengthfromhead(ffstate->compatibility, record);
}

/* Set record length */
void fftrail_setrecordlength(void* state, uint8* record, uint16 reclength)
{
    ffsmgr_state* ffstate = NULL;

    ffstate = (ffsmgr_state*)state;

    /* Skip token part */
    record += TOKENHDRSIZE;

    fftrail_data_setreclengthonhead(ffstate->compatibility, record, reclength);
}

/* Get grouptype from record */
void fftrail_getrecordgrouptype(void* state, uint8* record, uint8* grouptype)
{
    /* Call deserialization interface, parse data */
    uint8  tokenid = FFTRAIL_GROUPTYPE_NOP;
    uint8  tokeninfo = FFTRAIL_INFOTYPE_TOKEN;
    uint32 tokenlen = 0;

    uint8* uptr = NULL;
    uint8* tokendata = NULL;

    UNUSED(tokendata);
    UNUSED(tokeninfo);

    uptr = record;
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)

    *grouptype = tokenid;
    return;
}

/* Get grouptype from record */
bool fftrail_isrecordtransstart(void* state, uint8* record)
{
    ffsmgr_state* ffstate = NULL;

    ffstate = (ffsmgr_state*)state;
    return fftrail_data_deserail_check_transind_start(record, ffstate->compatibility);
}

/* Get tokenminsize */
int fftrail_gettokenminsize(int compatibility)
{
    return fftrail_data_tokenminsize(compatibility);
}