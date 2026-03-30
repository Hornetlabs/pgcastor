#include "app_incl.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/mpage/mpage.h"
#include "storage/ff_detail.h"
#include "storage/file_buffer.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "loadrecords/record.h"
#include "loadrecords/loadpage.h"
#include "loadrecords/loadpageam.h"
#include "loadrecords/loadrecords.h"
#include "loadrecords/loadtrailrecords.h"
#include "loadrecords/trailpage.h"

/* Validate page */
bool trailpage_valid(mpage* mp)
{
    if (NULL == mp)
    {
        return true;
    }

    return true;
}

/* Split page into records */
bool trailpage_page2records(loadtrailrecords* ltrailrecords, mpage* mp)
{
    uint8        tokenid = FFTRAIL_GROUPTYPE_NOP;
    uint8        tokeninfo = FFTRAIL_INFOTYPE_TOKEN;
    uint16       subtype = FF_DATA_TYPE_NOP;
    record_type  rectype = RECORD_TYPE_NOP;
    uint32       tokenlen = 0;
    uint32       recminsize = 0;
    uint32       blkoffset = 0;
    uint64       foffset = 0;
    XLogRecPtr   temporgpos = 0;
    uint8*       uptr = NULL;
    uint8*       uptr1 = NULL;
    uint8*       tokendata = NULL;
    dlistnode*   dlnode = NULL;
    record*      rec = NULL;
    ffsmgr_state ffsmgrstate;

    UNUSED(tokendata);

    /* Initialize validation interface */
    ffsmgr_init(FFSMG_IF_TYPE_TRAIL, &ffsmgrstate);
    ffsmgrstate.compatibility = ltrailrecords->compatibility;

    /* Get reserved size */
    recminsize = ffsmgrstate.ffsmgr->ffsmg_gettokenminsize(ltrailrecords->compatibility);
    foffset = ltrailrecords->foffset;
    foffset -= (foffset & LOADPAGEBLKSIZEMASK(ltrailrecords->loadpage->blksize));
    if (ltrailrecords->loadpage->filesize == ((uint64)ltrailrecords->loadpage->blksize + foffset))
    {
        recminsize += ffsmgrstate.ffsmgr->ffsmg_gettailsize(ltrailrecords->compatibility);
    }

    /* Calculate actual position */
    blkoffset = (uint32)(ltrailrecords->foffset & (LOADPAGEBLKSIZEMASK(ltrailrecords->loadpage->blksize)));
    uptr = mp->data;
    uptr += blkoffset;

    uptr1 = uptr;
    while (1)
    {
        uptr = uptr1;
        FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
        switch (tokenid)
        {
            case FFTRAIL_GROUPTYPE_FHEADER:
                if (false == ffsmgrstate.ffsmgr->ffsmgr_validrecord(tokenid,
                                                                    (void*)&ffsmgrstate,
                                                                    tokeninfo,
                                                                    ltrailrecords->fileid,
                                                                    uptr1))
                {
                    /* Validation failed, this marks the end of a complete record */
                    ltrailrecords->loadrecords.error = ERROR_BLK_INCOMPLETE;
                    goto trailpage_page2records_done;
                }

                /* Offset only at header */
                rectype = RECORD_TYPE_TRAIL_HEADER;
                tokenlen = MAXALIGN(tokenlen);
                break;
            case FFTRAIL_GROUPTYPE_DATA:
                if (false == ffsmgrstate.ffsmgr->ffsmgr_validrecord(tokenid,
                                                                    (void*)&ffsmgrstate,
                                                                    tokeninfo,
                                                                    ltrailrecords->fileid,
                                                                    uptr1))
                {
                    ltrailrecords->loadrecords.error = ERROR_BLK_INCOMPLETE;
                    goto trailpage_page2records_done;
                }

                /* Not yet focused on this */
                rectype = RECORD_TYPE_TRAIL_NORMAL;
                temporgpos = ffsmgrstate.ffsmgr->ffsmgr_getrecordlsn(&ffsmgrstate, uptr1);
                if (InvalidXLogRecPtr != temporgpos)
                {
                    ltrailrecords->orgpos.wal.lsn = temporgpos;
                }

                break;
            case FFTRAIL_GROUPTYPE_RESET:
                if (false == ffsmgrstate.ffsmgr->ffsmgr_validrecord(tokenid,
                                                                    (void*)&ffsmgrstate,
                                                                    tokeninfo,
                                                                    ltrailrecords->fileid,
                                                                    uptr1))
                {
                    ltrailrecords->loadrecords.error = ERROR_BLK_INCOMPLETE;
                    goto trailpage_page2records_done;
                }

                /* Type is RESET */
                rectype = RECORD_TYPE_TRAIL_RESET;
                break;
            case FFTRAIL_GROUPTYPE_FTAIL:
                if (false == ffsmgrstate.ffsmgr->ffsmgr_validrecord(tokenid,
                                                                    (void*)&ffsmgrstate,
                                                                    tokeninfo,
                                                                    ltrailrecords->fileid,
                                                                    uptr1))
                {
                    ltrailrecords->loadrecords.error = ERROR_BLK_INCOMPLETE;
                    goto trailpage_page2records_done;
                }
                rectype = RECORD_TYPE_TRAIL_TAIL;
                break;
            case FFTRAIL_GROUPTYPE_NOP:
                /* Reached end, waiting for new writes, mark wait, return recentry */
                ltrailrecords->loadrecords.error = ERROR_BLK_INCOMPLETE;
                goto trailpage_page2records_done;
            default:
                elog(RLOG_WARNING,
                     "unknown group type:%u, foffset:%lu, fileid:%lu",
                     tokenid,
                     ltrailrecords->foffset,
                     ltrailrecords->fileid);
                return false;
        }

        rec = record_init();
        if (NULL == rec)
        {
            elog(RLOG_WARNING, "record init error, out of memory");
            return false;
        }

        rec->type = rectype;
        rec->start.trail.offset = ltrailrecords->foffset; /* file-based start position */
        rec->start.trail.fileid = ltrailrecords->fileid;
        rec->end.trail.offset = (rec->start.trail.offset + tokenlen); /* file-based end position */
        rec->end.trail.fileid = ltrailrecords->fileid;
        rec->len = tokenlen;
        rec->data = (uint8*)rmalloc0(rec->len);
        if (NULL == rec->data)
        {
            elog(RLOG_WARNING, "out of memory");
            return false;
        }
        rmemcpy0(rec->data, 0, uptr1, rec->len);

        /* RESET/HEADER/TAIL special records */
        if (RECORD_TYPE_TRAIL_RESET != rectype && RECORD_TYPE_TRAIL_HEADER != rectype &&
            RECORD_TYPE_TRAIL_TAIL != rectype)
        {
            /* get totallength and reclength */
            rec->totallength = ffsmgrstate.ffsmgr->ffsmgr_getrecordtotallength(&ffsmgrstate, rec->data);
            rec->reallength = ffsmgrstate.ffsmgr->ffsmgr_getrecordlength(&ffsmgrstate, rec->data);
            rec->dataoffset = ffsmgrstate.ffsmgr->ffsmgr_getrecorddataoffset(ffsmgrstate.compatibility);
        }
        ltrailrecords->records = dlist_put(ltrailrecords->records, rec);

        /* buffer offset */
        uptr1 += tokenlen;
        blkoffset += tokenlen;
        ltrailrecords->foffset += tokenlen;

        /* check if remaining content meets minimum requirement */
        if ((ltrailrecords->loadpage->blksize - blkoffset) > recminsize)
        {
            /* there is still remaining data */
            continue;
        }

        /* point to start of next page */
        ltrailrecords->foffset += (ltrailrecords->loadpage->blksize - blkoffset);
        break;
    }

trailpage_page2records_done:
    /*
     * reset rectype for returned data
     * 1. check if the last one is cross-page
     * 2. check if the first record is continuation of previous record
     */
    if (true == dlist_isnull(ltrailrecords->records))
    {
        /* if empty, mining has reached the end */
        return true;
    }

    for (dlnode = ltrailrecords->records->tail; NULL != dlnode; dlnode = dlnode->prev)
    {
        rec = (record*)dlnode->value;
        if (RECORD_TYPE_TRAIL_RESET == rec->type || RECORD_TYPE_TRAIL_TAIL == rec->type ||
            RECORD_TYPE_TRAIL_HEADER == rec->type)
        {
            continue;
        }

        /*
         * check if the last one is cross-page
         *  get total length and record length, if total length > record length, record is
         * incomplete
         */
        if (0 == rec->totallength || rec->totallength == rec->reallength)
        {
            /*
             * when length is 0, this record is continuation of previous record, same record cannot
             * be both cont and cross when lengths are equal, it's a complete record
             */
            break;
        }
        rec->type = RECORD_TYPE_TRAIL_CROSS;

        /* reset the recorded length in this record */
        ffsmgrstate.ffsmgr->ffsmgr_setrecordlength(&ffsmgrstate, rec->data, rec->totallength);
    }

    /*
     * set from front to back, check if it's a continue record
     *  first get subtype, if it's a continue record, this record is continuation of previous page
     */
    for (dlnode = ltrailrecords->records->head; NULL != dlnode; dlnode = dlnode->next)
    {
        rec = (record*)dlnode->value;
        if (RECORD_TYPE_TRAIL_RESET == rec->type || RECORD_TYPE_TRAIL_TAIL == rec->type ||
            RECORD_TYPE_TRAIL_HEADER == rec->type)
        {
            continue;
        }

        /*
         * check if the first one is cross-page, note that this record may be a database record
         *  1. first get subtype, if it's a continue record, this record is continuation of previous
         * page
         */
        if (false == ffsmgrstate.ffsmgr->ffsmgr_getrecordsubtype(&ffsmgrstate, rec->data, &subtype))
        {
            elog(RLOG_WARNING, "get record sub type error");
            return false;
        }

        /* dbmetadata needs to be filtered out, dbmetadata does not cross pages or files */
        if (FF_DATA_TYPE_DBMETADATA == subtype)
        {
            rec->type = RECORD_TYPE_TRAIL_DBMETA;
            continue;
        }

        /* table meta can cross files, but considering trail file format,
         * cross-page or cross-file non-table meta
         * won't appear new table meta at the beginning of file or page */
        if (FF_DATA_TYPE_REC_CONTRECORD == subtype)
        {
            rec->type = RECORD_TYPE_TRAIL_CONT;
            continue;
        }
        break;
    }

    return true;
}
