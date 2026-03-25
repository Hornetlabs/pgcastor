#include "app_incl.h"
#include "utils/hash/hash_search.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/guc/guc.h"
#include "utils/mpage/mpage.h"
#include "storage/ff_detail.h"
#include "storage/file_buffer.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "loadrecords/record.h"
#include "loadrecords/loadpage.h"
#include "loadrecords/loadpageam.h"
#include "loadrecords/loadpagefromfile.h"
#include "loadrecords/loadrecords.h"
#include "loadrecords/loadtrailrecords.h"
#include "loadrecords/trailpage.h"

/* Initialization */
loadtrailrecords* loadtrailrecords_init(void)
{
    loadtrailrecords* loadrecords = NULL;

    loadrecords = (loadtrailrecords*)rmalloc0(sizeof(loadtrailrecords));
    if (NULL == loadrecords)
    {
        elog(RLOG_WARNING, "load trail records init error, out of memory");
        return NULL;
    }
    rmemset0(loadrecords, 0, '\0', sizeof(loadtrailrecords));
    loadrecords->loadrecords.filesize = MB2BYTE(guc_getConfigOptionInt(CFG_KEY_TRAIL_MAX_SIZE));
    loadrecords->loadrecords.blksize = FILE_BUFFER_SIZE;
    loadrecords->loadrecords.type = LOADRECORDS_TYPE_TRAIL;
    loadrecords->compatibility = guc_getConfigOptionInt(CFG_KEY_COMPATIBILITY);
    loadrecords->fileid = 0;
    loadrecords->foffset = 0;
    loadrecords->loadpage = NULL;
    loadrecords->loadpageroutine = NULL;
    loadrecords->records = NULL;
    loadrecords->recordcross.record = NULL;
    loadrecords->mp = rmalloc0(sizeof(mpage));
    if (NULL == loadrecords->mp)
    {
        elog(RLOG_WARNING, "loadtrailrecords init mpage error, out of memory");
        rfree(loadrecords);
        return NULL;
    }
    rmemset0(loadrecords->mp, 0, '\0', sizeof(mpage));
    rmemset1(loadrecords->recordcross.rectail, 0, '\0', RECORD_TAIL_LEN);
    return loadrecords;
}

/* Set the method for loading trail files */
bool loadtrailrecords_setloadpageroutine(loadtrailrecords* loadrecords, loadpage_type type)
{
    /* Load in file */
    loadrecords->loadpageroutine = loadpage_getpageroutine(type);
    if (NULL == loadrecords->loadpageroutine)
    {
        elog(RLOG_WARNING, "set loadpage routine error");
        return false;
    }

    loadrecords->loadpage = loadrecords->loadpageroutine->loadpageinit();
    if (NULL == loadrecords->loadpage)
    {
        elog(RLOG_WARNING, "load page init error");
        return false;
    }

    /* Set file block size and file size */
    loadrecords->loadpage->blksize = loadrecords->loadrecords.blksize;
    loadrecords->loadpage->filesize = loadrecords->loadrecords.filesize;

    /* Set loadpagefromfile type */
    loadrecords->loadpageroutine->loadpagesettype(loadrecords->loadpage,
                                                  LOADPAGEFROMFILE_TYPE_TRAIL);

    return true;
}

/* Set the loading start position */
void loadtrailrecords_setloadposition(loadtrailrecords* loadrecords, uint64 fileid, uint64 foffset)
{
    recpos recpos;
    loadrecords->fileid = fileid;
    loadrecords->foffset = foffset;

    recpos.trail.type = RECPOS_TYPE_TRAIL;
    recpos.trail.fileid = fileid;
    recpos.trail.offset = foffset;
    loadrecords->loadpageroutine->loadpagesetstartpos(loadrecords->loadpage, recpos);
}

/* Set the loading source path */
bool loadtrailrecords_setloadsource(loadtrailrecords* loadrecords, char* source)
{
    if (false == loadrecords->loadpageroutine->loadpagesetfilesource(loadrecords->loadpage, source))
    {
        elog(RLOG_WARNING, "load trail record set load source error");
        return false;
    }
    return true;
}

/* Load records */
bool loadtrailrecords_load(loadtrailrecords* loadrecords)
{
    /*
     * 1、Load file blocks
     * 2、Split file blocks into records
     * 3、Based on record types, processing is divided into 2 logics for complete and incomplete
     * record data 1、When incomplete, there are two types: reading RESET 1.1 When reading RESET,
     * discard the incomplete record and pass the reset record to the subsequent process This proves
     * a restart occurred, and even if the previous record is incomplete, it doesn't matter. 1.2
     * Temporarily store this record, and when reading the next page, assemble them together
     *      2、Complete records require no processing
     */
    bool       shiftfile = false;      /* File switch encountered when merging continue record */
    bool       shiftfilecross = false; /* File switch encountered when searching for cross record */
    bool       crossreset = false;     /* Reset encountered after cross */
    uint64     reclen = 0;
    dlistnode* dlnode = NULL;
    dlistnode* dlnodenext = NULL;
    record*    record_obj = NULL;
    recpos     recpos = {{0}};

    recpos.trail.type = RECPOS_TYPE_TRAIL;

    /* Load file blocks */
    loadrecords->loadrecords.error = ERROR_SUCCESS;

    /* Set the read starting point */
    if (false == loadrecords->loadpageroutine->loadpage(loadrecords->loadpage, loadrecords->mp))
    {
        if (ERROR_NOENT == loadrecords->loadpage->error)
        {
            /* No need to return error due to file not existing */
            return true;
        }

        elog(RLOG_WARNING, "load page error, %d", loadrecords->loadpage->error);
        return false;
    }

    /*
     * 1、Validate the correctness of file blocks
     * 2、Split file blocks into records
     */
    if (false == trailpage_valid(loadrecords->mp))
    {
        elog(RLOG_WARNING, "load page valid page error");
        return false;
    }

    /* Split page into records */
    if (false == trailpage_page2records(loadrecords, loadrecords->mp))
    {
        elog(RLOG_WARNING, "page 2 records error");
        return false;
    }

    /* records is empty, indicating no content was extracted */
    if (true == dlist_isnull(loadrecords->records))
    {
        return true;
    }

    /*
     * Processing begins based on the scenario
     * 1、Check if there are incomplete records. If there are incomplete records, the first valid
     * record in the returned records should be a continue record or RESET record
     *   1.1 When it's a continue record, merge the two records
     *   1.2 If it's a reset record, clear the incomplete record
     *
     * 2、Check if records contain a CROSS record. If a cross record is present, check if subsequent
     * records contain reset
     *   2.1 If reset is present, clear the cross record
     *   2.2 If reset is not present, temporarily store the cross record
     */
    if (NULL != loadrecords->recordcross.record)
    {
        /* Contains records */
        for (dlnode = loadrecords->records->head; NULL != dlnode;)
        {
            record_obj = (record*)dlnode->value;

            /* May appear at the file header */
            if (RECORD_TYPE_TRAIL_HEADER == record_obj->type ||
                RECORD_TYPE_TRAIL_DBMETA == record_obj->type)
            {
                loadrecords->remainrecords = dlist_put(loadrecords->remainrecords, record_obj);

                /* Remove dlnode from the linked list */
                dlnode->value = NULL;
                dlnodenext = dlnode->next;
                loadrecords->records = dlist_delete(loadrecords->records, dlnode, NULL);
                dlnode = dlnodenext;
                continue;
            }
            else if (RECORD_TYPE_TRAIL_TAIL == record_obj->type)
            {
                shiftfile = true;
                loadrecords->remainrecords = dlist_put(loadrecords->remainrecords, record_obj);

                /* Remove dlnode from the linked list */
                dlnode->value = NULL;
                dlnodenext = dlnode->next;
                loadrecords->records = dlist_delete(loadrecords->records, dlnode, NULL);
                dlnode = dlnodenext;
                continue;
            }
            else if (RECORD_TYPE_TRAIL_RESET == record_obj->type)
            {
                shiftfile = true;
                /* For RESET, clear it */
                record_free(loadrecords->recordcross.record);
                loadrecords->recordcross.record = NULL;
                loadrecords->recordcross.remainlen = 0;
                loadrecords->recordcross.totallen = 0;

                /* Linked list merging */
                loadrecords->records =
                    dlist_concat(loadrecords->remainrecords, loadrecords->records);
                loadrecords->remainrecords = NULL;
                break;
            }

            if (RECORD_TYPE_TRAIL_CONT != record_obj->type)
            {
                /* At this point it should be a continue record; if not, it indicates a logical
                 * error */
                elog(RLOG_WARNING,
                     "need continue record, but now type:%d, cross record:%lu.%lu, %lu.%u ,%lu",
                     record_obj->type, loadrecords->recordcross.record->start.trail.fileid,
                     loadrecords->recordcross.record->start.trail.offset,
                     loadrecords->recordcross.record->totallength,
                     loadrecords->recordcross.record->reallength,
                     loadrecords->recordcross.record->len);
                return false;
            }

            /* Record merging */
            if (loadrecords->recordcross.record->len < loadrecords->recordcross.totallen)
            {
                /* Increase tail length */
                reclen = loadrecords->recordcross.totallen + loadrecords->recordcross.rectaillen;
                loadrecords->recordcross.record->data =
                    rrealloc0(loadrecords->recordcross.record->data, reclen);
                if (NULL == loadrecords->recordcross.record->data)
                {
                    elog(RLOG_WARNING, "realloc cross record error");
                    return false;
                }
                /* Point to the copy data position */
                loadrecords->recordcross.record->dataoffset = loadrecords->recordcross.record->len;

                /* Reset the total data length */
                loadrecords->recordcross.record->len = loadrecords->recordcross.totallen;
            }

            /* Append record tail to the end of data */
            rmemcpy1(loadrecords->recordcross.record->data,
                     loadrecords->recordcross.record->dataoffset,
                     record_obj->data + record_obj->dataoffset, record_obj->reallength);
            loadrecords->recordcross.record->dataoffset += record_obj->reallength;
            loadrecords->recordcross.remainlen -= record_obj->reallength;

            /* Delete the record and remove the dlnode node from the doubly linked list */
            loadrecords->recordcross.record->end.trail.fileid = record_obj->end.trail.fileid;
            loadrecords->recordcross.record->end.trail.offset = record_obj->end.trail.offset;
            record_free(record_obj);

            /* Remove dlnode from the linked list */
            dlnode->value = NULL;
            dlnodenext = dlnode->next;
            loadrecords->records = dlist_delete(loadrecords->records, dlnode, NULL);
            dlnode = dlnodenext;

            /* After merging is complete, check if the record is complete. If complete, attach the
             * cross record to the head of the normal linked list
             */
            if (0 != loadrecords->recordcross.remainlen)
            {
                continue;
            }

            /* Assembly completed, append tail data to the end of data */
            rmemcpy1(loadrecords->recordcross.record->data,
                     loadrecords->recordcross.record->dataoffset, loadrecords->recordcross.rectail,
                     loadrecords->recordcross.rectaillen);
            loadrecords->recordcross.record->len += loadrecords->recordcross.rectaillen;
            loadrecords->recordcross.rectaillen = 0;

            /*
             * Connect two linked lists and add the cross record to the linked list
             */
            loadrecords->records = dlist_concat(loadrecords->remainrecords, loadrecords->records);
            loadrecords->remainrecords = NULL;
            loadrecords->recordcross.record->type = RECORD_TYPE_TRAIL_NORMAL;
            loadrecords->records =
                dlist_puthead(loadrecords->records, loadrecords->recordcross.record);
            loadrecords->recordcross.record = NULL;
            loadrecords->recordcross.rectaillen = 0;
            loadrecords->recordcross.remainlen = 0;
            loadrecords->recordcross.totallen = 0;
            break;
        }
    }

    /* Check if there is still data to be processed */
    if (true == dlist_isnull(loadrecords->records))
    {
        goto loadtrailrecords_load_done;
    }

    /* Process from back to front */
    for (dlnode = loadrecords->records->tail; NULL != dlnode; dlnode = dlnode->prev)
    {
        record_obj = (record*)dlnode->value;
        /* May appear at the file header */
        if (RECORD_TYPE_TRAIL_HEADER == record_obj->type ||
            RECORD_TYPE_TRAIL_DBMETA == record_obj->type)
        {
            continue;
        }
        else if (RECORD_TYPE_TRAIL_TAIL == record_obj->type)
        {
            shiftfilecross = true;
            continue;
        }
        else if (RECORD_TYPE_TRAIL_RESET == record_obj->type)
        {
            shiftfilecross = true;
            crossreset = true;
            continue;
        }
        else if (RECORD_TYPE_TRAIL_CROSS != record_obj->type)
        {
            break;
        }

        /*
         * Encountered cross
         *   1、If crossreset is present, it indicates a restart occurred. Since a restart occurred,
         * the cross record has no meaning, clear this record. 2、If not present, save it to the
         * cross record
         */
        if (true == crossreset)
        {
            record_free(record_obj);
            dlnode->value = NULL;
            loadrecords->records = dlist_delete(loadrecords->records, dlnode, NULL);
            break;
        }

        /* Add the record to the cross record for subsequent merging */
        if (NULL != loadrecords->recordcross.record)
        {
            elog(RLOG_WARNING,
                 "The previous processing contained incomplete records, prev fileid:%lu:%lu, "
                 "current fileid:%lu:%lu",
                 loadrecords->recordcross.record->start.trail.fileid,
                 loadrecords->recordcross.record->start.trail.offset,
                 record_obj->start.trail.fileid, record_obj->start.trail.offset);
            return false;
        }

        /* Place it on the cross record for convenient subsequent merging */
        loadrecords->recordcross.record = record_obj;

        /* Calculate length */
        /* Remaining actual data length needed */
        loadrecords->recordcross.remainlen =
            (record_obj->totallength - (uint64)record_obj->reallength);

        /* Total data length */
        loadrecords->recordcross.totallen = record_obj->totallength;
        loadrecords->recordcross.totallen += (uint64)record_obj->dataoffset;

        /*
         * Copy record tail data
         *   1、Calculate the start position of record tail
         *   2、Trim the tail data length
         *   3、Copy the data
         */
        /* Calculate the start position of record tail */
        record_obj->dataoffset += record_obj->reallength;
        loadrecords->recordcross.rectaillen =
            (uint16)(record_obj->len - (uint64)record_obj->dataoffset);

        /* Trim the tail data length from the original length */
        record_obj->len -= (uint64)(loadrecords->recordcross.rectaillen);
        rmemcpy1(loadrecords->recordcross.rectail, 0, (record_obj->data + record_obj->dataoffset),
                 loadrecords->recordcross.rectaillen);

        /* Remove dlnode from the linked list and place the subsequent nodes into the temporary node
         */
        dlnode->value = NULL;
        dlnodenext = dlnode->next;
        loadrecords->records = dlist_truncate(loadrecords->records, dlnode);
        dlist_node_free(dlnode, NULL);

        /* Add subsequent nodes to the remain node */
        if (false == dlist_append(&loadrecords->remainrecords, dlnodenext))
        {
            elog(RLOG_WARNING, "dlist append error");
            return false;
        }
        break;
    }

loadtrailrecords_load_done:
    /* After the above process is completed, determine if a file switch is needed */
    /* If the file switch identifier appears when searching for continue records, no need to process
     * cross and offset */
    if (true == shiftfile)
    {
        /* Need to switch file */
        loadrecords->fileid++;
        loadrecords->foffset = 0;
        recpos.trail.fileid = loadrecords->fileid;
        recpos.trail.offset = loadrecords->foffset;
        loadrecords->loadpageroutine->loadpagesetstartpos(loadrecords->loadpage, recpos);
        return true;
    }

    /* File switch identifier appears when processing cross record */
    if (true == shiftfilecross)
    {
        /* Need to switch file */
        loadrecords->fileid++;
        loadrecords->foffset = 0;
        recpos.trail.fileid = loadrecords->fileid;
        recpos.trail.offset = loadrecords->foffset;
        loadrecords->loadpageroutine->loadpagesetstartpos(loadrecords->loadpage, recpos);
        return true;
    }

    /* Determine if the file switch condition is met */
    if (loadrecords->foffset == loadrecords->loadpage->filesize)
    {
        /* Switch file */
        loadrecords->fileid++;
        loadrecords->foffset = 0;
        recpos.trail.fileid = loadrecords->fileid;
        recpos.trail.offset = loadrecords->foffset;
        loadrecords->loadpageroutine->loadpagesetstartpos(loadrecords->loadpage, recpos);
        return true;
    }
    else
    {
        recpos.trail.fileid = loadrecords->fileid;
        recpos.trail.offset = loadrecords->foffset;
        loadrecords->loadpageroutine->loadpagesetstartpos(loadrecords->loadpage, recpos);
    }

    return true;
}

/* Close file descriptor */
void loadtrailrecords_fileclose(loadtrailrecords* loadrecords)
{
    loadrecords->loadpageroutine->loadpageclose(loadrecords->loadpage);
}

/*
 * Filter records to find the beginning of a transaction
 *  Return value explanation:
 *   true           Still need to continue filtering
 *   false          No need to continue filtering
 */
bool loadtrailrecords_filterfortransbegin(loadtrailrecords* loadrecords)
{
    /*
     * Filtering explanation:
     * trail file format
     * -------------------------------
     * |file header|database metadata |
     * |record     |                  |
     * | -----------------------------
     *
     * When restarting, parsing begins at the file header, as we have found the beginning of the
     * first transaction in this file. 1、Metadata is needed, including database metadata and table
     * metadata 2、If the file contains RESET or TAIL, it indicates that parsing has reached the end
     * of the file without finding needed data, so filtering is no longer required at this point
     */
    uint16       subtype = FF_DATA_TYPE_NOP;
    dlistnode*   dlnode = NULL;
    dlistnode*   dlnodenext = NULL;
    record*      record_obj = NULL;
    ffsmgr_state ffsmgrstate;

    /* Initialize validation interface */
    ffsmgr_init(FFSMG_IF_TYPE_TRAIL, &ffsmgrstate);
    ffsmgrstate.compatibility = loadrecords->compatibility;

    for (dlnode = dlnodenext = loadrecords->records->head; NULL != dlnode; dlnode = dlnodenext)
    {
        dlnodenext = dlnode->next;
        record_obj = (record*)dlnode->value;
        /* At file beginning or page switch, TRAIL_CONT records should be deleted directly */
        if (RECORD_TYPE_TRAIL_HEADER == record_obj->type ||
            RECORD_TYPE_TRAIL_DBMETA == record_obj->type)
        {
            continue;
        }
        else if (RECORD_TYPE_TRAIL_RESET == record_obj->type ||
                 RECORD_TYPE_TRAIL_TAIL == record_obj->type)
        {
            return false;
        }

        /* Keep metadata data in data, no processing needed here */
        ffsmgrstate.ffsmgr->ffsmgr_getrecordsubtype(&ffsmgrstate, record_obj->data, &subtype);
        if (FF_DATA_TYPE_DBMETADATA == subtype || FF_DATA_TYPE_TBMETADATA == subtype)
        {
            continue;
        }

        /* Check if this is a transaction start record, which is needed for all subsequent records
         */
        if (ffsmgrstate.ffsmgr->ffsmgr_isrecordtransstart(&ffsmgrstate, record_obj->data))
        {
            return false;
        }

        loadrecords->records = dlist_delete(loadrecords->records, dlnode, record_freevoid);
    }
    return true;
}

/*
 * Filter based on fileid and offset, records less than this value are not needed
 */
void loadtrailrecords_filter(loadtrailrecords* loadrecords, uint64 fileid, uint64 foffset)
{
    /*
     * Filter description:
     *  Based on fileid and foffset
     */
    dlistnode* dlnode = NULL;
    dlistnode* dlnodenext = NULL;
    record*    record_obj = NULL;

    for (dlnode = dlnodenext = loadrecords->records->head; NULL != dlnode; dlnode = dlnodenext)
    {
        dlnodenext = dlnode->next;
        record_obj = (record*)dlnode->value;

        if (fileid < record_obj->start.trail.fileid)
        {
            return;
        }

        if (foffset <= record_obj->start.trail.offset)
        {
            break;
        }

        /* This record is not needed */
        loadrecords->records = dlist_delete(loadrecords->records, dlnode, record_freevoid);
    }
}

/*
 * Filter based on fileid and offset, but keep metadata
 * Return value:
 *   true           Need to continue filtering
 *   false          No need to continue filtering
 */
bool loadtrailrecords_filterremainmetadata(loadtrailrecords* loadrecords, uint64 fileid,
                                           uint64 foffset)
{
    /*
     * Filter description:
     *  Based on fileid and foffset
     */
    uint16       subtype = FF_DATA_TYPE_NOP;
    dlistnode*   dlnode = NULL;
    dlistnode*   dlnodenext = NULL;
    record*      record_obj = NULL;
    ffsmgr_state ffsmgrstate;

    /* Initialize validation interface */
    ffsmgr_init(FFSMG_IF_TYPE_TRAIL, &ffsmgrstate);
    ffsmgrstate.compatibility = loadrecords->compatibility;

    for (dlnode = dlnodenext = loadrecords->records->head; NULL != dlnode; dlnode = dlnodenext)
    {
        dlnodenext = dlnode->next;
        record_obj = (record*)dlnode->value;

        if (fileid < record_obj->start.trail.fileid)
        {
            return false;
        }

        if (record_obj->start.trail.offset >= foffset)
        {
            return false;
        }

        /* reset/tail indicates file switch, so no further filtering needed */
        if (RECORD_TYPE_TRAIL_RESET == record_obj->type ||
            RECORD_TYPE_TRAIL_TAIL == record_obj->type)
        {
            return false;
        }

        /* Keep metadata data in data, no processing needed here */
        ffsmgrstate.ffsmgr->ffsmgr_getrecordsubtype(&ffsmgrstate, record_obj->data, &subtype);
        if (FF_DATA_TYPE_DBMETADATA == subtype || FF_DATA_TYPE_TBMETADATA == subtype)
        {
            continue;
        }

        loadrecords->records = dlist_delete(loadrecords->records, dlnode, record_freevoid);
    }

    return true;
}

/* Free resources */
void loadtrailrecords_free(loadtrailrecords* loadrecords)
{
    if (NULL == loadrecords)
    {
        return;
    }

    if (NULL != loadrecords->loadpageroutine)
    {
        loadrecords->loadpageroutine->loadpagefree(loadrecords->loadpage);
        loadrecords->loadpage = NULL;
        loadrecords->loadpageroutine = NULL;
    }

    if (NULL != loadrecords->mp)
    {
        if (NULL != loadrecords->mp->data)
        {
            rfree(loadrecords->mp->data);
        }
        rfree(loadrecords->mp);
    }

    record_free(loadrecords->recordcross.record);
    dlist_free(loadrecords->records, record_freevoid);
    dlist_free(loadrecords->remainrecords, record_freevoid);
    rfree(loadrecords);
}
