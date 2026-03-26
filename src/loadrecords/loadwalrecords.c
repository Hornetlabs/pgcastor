#include "app_incl.h"
#include "utils/dlist/dlist.h"
#include "utils/guc/guc.h"
#include "utils/mpage/mpage.h"
#include "utils/algorithm/crc/crc_check.h"
#include "works/splitwork/wal/wal_define.h"
#include "loadrecords/record.h"
#include "loadrecords/loadpage.h"
#include "loadrecords/loadpageam.h"
#include "loadrecords/loadpagefromfile.h"
#include "loadrecords/loadrecords.h"
#include "loadrecords/loadwalrecords.h"

#define XLOG_PAGE_MAGIC_PG127            0xD101
#define XLOG_PAGE_MAGIC_PG149            0xD10D
#define XLOG_PAGE_MAGIC_PG95             0xD087

#define RecoedMaxAllocSize               ((Size)0x3fffffff) /* 1 gigabyte - 1 */
#define RecordAllocSizeIsValid(size)     ((Size)(size) <= RecoedMaxAllocSize)
#define updateCurrentPtr(cur, load, off) cur = (load)->block_startptr + off

typedef struct totalLen
{
    uint32 len;
} totalLen;

static bool splitwork_wal_crccheck(uint8* bytes)
{
    pg_crc32c   crc;
    XLogRecord* record = NULL;

    record = (XLogRecord*)bytes;

    /* Calculate the CRC */
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, ((char*)record) + SizeOfXLogRecord, record->xl_tot_len - SizeOfXLogRecord);
    /* include the record header last */
    COMP_CRC32C(crc, (char*)record, offsetof(XLogRecord, xl_crc));
    FIN_CRC32C(crc);

    if (!EQ_CRC32C(record->xl_crc, crc))
    {
        elog(RLOG_WARNING, "record crc check failed, waiting");
        usleep(10000);
        return false;
    }

    return true;
}

static bool check_magic(uint16 magic)
{
    return (magic == XLOG_PAGE_MAGIC_PG127 || magic == XLOG_PAGE_MAGIC_PG149 ||
            magic == XLOG_PAGE_MAGIC_PG95);
}

static bool check_is_same_segno(XLogRecPtr ptr1, XLogRecPtr ptr2, loadwalrecords* rctl)
{
    uint32_t segno1 = 0;
    uint32_t segno2 = 0;

    XLByteToSeg(ptr1, segno1, rctl->loadpage->filesize);
    XLByteToSeg(ptr2, segno2, rctl->loadpage->filesize);

    if (segno1 == segno2)
    {
        return true;
    }
    return false;
}

static bool check_prev_lsn(XLogRecord* record, XLogRecPtr prev)
{
    if (prev == InvalidXLogRecPtr)
    {
        return true;
    }

    if (record->xl_prev == prev)
    {
        return true;
    }
    return false;
}

static bool check_record_len_is_valid(uint32_t len)
{
    return (RecordAllocSizeIsValid(len) && len >= SizeOfXLogRecord);
}

static bool check_seg_first_incomplete_record_incompleted(loadwalrecords* rctl)
{
    if (rctl->seg_first_incomplete)
    {
        if (rctl->seg_first_incomplete->totallen == rctl->seg_first_incomplete->remainlen)
        {
            return false;
        }
        else if (rctl->seg_first_incomplete->totallen > rctl->seg_first_incomplete->remainlen)
        {
            return true;
        }
        else
        {
            elog(RLOG_ERROR, "warn seg first incomplete record len");
            return false;
        }
    }
    return false;
}

static void wal_usleep(long microsec)
{
    if (microsec > 0)
    {
        struct timeval delay;
        delay.tv_sec = microsec / 1000000L;
        delay.tv_usec = microsec % 1000000L;
        (void)select(0, NULL, NULL, NULL, &delay);
    }
}

/* initialize loadpage for record splitting */
static bool loadwalrecords_initloadpage(loadwalrecords* loadrecords, loadpage_type type)
{
    char* dirpath = NULL;

    /* get function pointer */
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

    /* set file block size and file size */
    loadrecords->loadpage->blksize = g_blocksize;
    loadrecords->loadpage->filesize = g_walsegsize * 1048576;
    loadrecords->loadpageroutine->loadpagesettype(loadrecords->loadpage, LOADPAGEFROMFILE_TYPE_WAL);

    /* set directory path */
    dirpath = guc_getConfigOption(CFG_KEY_WAL_DIR);
    if (false ==
        loadrecords->loadpageroutine->loadpagesetfilesource(loadrecords->loadpage, dirpath))
    {
        elog(RLOG_WARNING, "load trail record set load source error");
        return false;
    }

    return true;
}

loadwalrecords* loadwalrecords_init(void)
{
    loadwalrecords* result = NULL;
    mpage*          page = NULL;

    result = rmalloc0(sizeof(loadwalrecords));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(loadwalrecords));

    /* initialize loadpage */
    if (!loadwalrecords_initloadpage(result, LOADPAGE_TYPE_FILE))
    {
        return NULL;
    }

    /* initialize mpage */
    page = rmalloc0(sizeof(mpage));
    if (!page)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(page, 0, 0, sizeof(mpage));

    page->size = result->loadpage->blksize;

    page->data = rmalloc0(sizeof(uint8_t) * page->size);
    if (!page->data)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(page->data, 0, 0, sizeof(uint8_t) * page->size);

    result->page = page;

    return result;
}

void loadwalrecords_free(loadwalrecords* loadrecords)
{
    if (!loadrecords)
    {
        return;
    }

    /* free incomplete record */
    if (NULL != loadrecords->seg_first_incomplete)
    {
        recordcross_free(loadrecords->seg_first_incomplete);
    }

    if (NULL != loadrecords->seg_first_incomplete_next)
    {
        recordcross_free(loadrecords->seg_first_incomplete_next);
    }

    if (NULL != loadrecords->page_last_record_incomplete)
    {
        recordcross_free(loadrecords->page_last_record_incomplete);
    }

    /* free records list */
    if (NULL != loadrecords->records)
    {
        dlist_free(loadrecords->records, (dlistvaluefree)record_free);
    }

    /* free loadpage */
    if (NULL != loadrecords->loadpage)
    {
        loadrecords->loadpageroutine->loadpagefree(loadrecords->loadpage);
    }

    /* loadpageroutine does not need to be freed, set to NULL */
    loadrecords->loadpageroutine = NULL;

    /* free mpage */
    if (loadrecords->page)
    {
        if (loadrecords->page->data)
        {
            rfree(loadrecords->page->data);
        }
        rfree(loadrecords->page);
    }

    rfree(loadrecords);
}

static bool loadtwalrecords_loadpage(loadwalrecords* loadrecords)
{
    bool   result = false;
    recpos pos = {{'\0'}};
    mpage* buff = loadrecords->page;

    /* calculate block start lsn */
    loadrecords->block_startptr =
        loadrecords->startptr - (loadrecords->startptr & (loadrecords->loadpage->blksize - 1));
    pos.wal.type = RECPOS_TYPE_WAL;
    pos.wal.timeline = loadrecords->timeline;
    pos.wal.lsn = loadrecords->block_startptr;

    /* set to 0 */
    rmemset0(buff->data, 0, 0, loadrecords->loadpage->blksize);

    /* set pos */
    loadrecords->loadpageroutine->loadpagesetstartpos(loadrecords->loadpage, pos);

    /* read page */
    result = loadrecords->loadpageroutine->loadpage(loadrecords->loadpage, buff);

    /* check if next read is from next wal file */
    if (0 == ((loadrecords->block_startptr + loadrecords->loadpage->blksize) %
              loadrecords->loadpage->filesize))
    {
        /* close file descriptor */
        loadrecords->loadpageroutine->loadpageclose(loadrecords->loadpage);
    }

    return result;
}

bool loadwalrecords_checkend(XLogRecPtr cur, loadwalrecords* rctl)
{
    if (!rctl->endptr)
    {
        return true;
    }

    if (cur >= rctl->endptr)
    {
        return false;
    }
    return true;
}

#define CHECK_STATUS_REWIND(start, end) (start && end ? true : false)

/* split from block header */
static bool splitStartWithBlockBegin(char*           currentBuf,
                                     loadwalrecords* loadrecords,
                                     mpage*          buff,
                                     XLogRecPtr*     currentPtr,
                                     uint32_t*       offset)
{
    XLogPageHeader phdr = (XLogPageHeader)(currentBuf);
    bool           seg_first = false;

    /* magic number verification */
    if (!(check_magic(phdr->xlp_magic)) || phdr->xlp_pageaddr != loadrecords->block_startptr)
    {
        /* on verification failure, wait 200ms */
        wal_usleep(20 * 1000);
        elog(RLOG_DEBUG,
             "page header magic check failed, wait, lsn:%X/%X",
             (uint32)(loadrecords->startptr >> 32),
             (uint32)loadrecords->startptr);
        return false;
    }

    /* check if this is wal file start */
    if (IsXLogSegmentBegin(*currentPtr, loadrecords->loadpage->filesize))
    {
        seg_first = true;
        *offset += SizeOfXLogLongPHD;
    }
    else
    {
        *offset += SizeOfXLogShortPHD;
    }

    updateCurrentPtr(*currentPtr, loadrecords, *offset);

    if (phdr->xlp_rem_len == 0 && loadrecords->page_last_record_incomplete)
    {
        /* on failure, wait 200ms */
        wal_usleep(20 * 1000);

        if (loadrecords->page_last_record_incomplete)
        {
            /* incomplete record should not exist here, first half data error */

            /* no incomplete record and page header records remaining length */
            elog(RLOG_INFO,
                 "split record, have incomplete record but not remain data, may flush slow, try "
                 "again");

            /* when not in same wal file */
            if (!check_is_same_segno(
                    loadrecords->startptr,
                    loadrecords->page_last_record_incomplete->record->start.wal.lsn,
                    loadrecords))
            {
                /* close file descriptor, reopen previous file on read */
                loadrecords->loadpageroutine->loadpageclose(loadrecords->loadpage);
            }
            loadrecords->startptr = loadrecords->page_last_record_incomplete->record->start.wal.lsn;

            /* free */
            recordcross_free(loadrecords->page_last_record_incomplete);
            loadrecords->page_last_record_incomplete = NULL;

            return false;
        }
    }

    if (phdr->xlp_rem_len > 0)
    {
        /* whether incomplete record exists */
        if (loadrecords->page_last_record_incomplete &&
            !check_seg_first_incomplete_record_incompleted(loadrecords))
        {
            recordcross* record_cross = loadrecords->page_last_record_incomplete;
            uint32       temp_offset = 0;
            char*        temp_record_ptr = (char*)record_cross->record->data;
            uint32       realSize = MAXALIGN(phdr->xlp_rem_len);
            uint32       freeSpace = buff->size - *offset;
            uint32       irecord_need_len = record_cross->totallen - record_cross->remainlen;

            realSize = (realSize <= freeSpace) ? realSize : freeSpace;

            if (realSize > irecord_need_len)
            {
                /* on verification failure, wait 200ms */
                wal_usleep(20 * 1000);

                elog(RLOG_INFO,
                     "record split, page hdr remain len > incomplete record len, waitting");

                /* when not in same wal file */
                if (!check_is_same_segno(
                        loadrecords->startptr, record_cross->record->start.wal.lsn, loadrecords))
                {
                    /* close file descriptor, reopen previous file on read */
                    loadrecords->loadpageroutine->loadpageclose(loadrecords->loadpage);
                }
                loadrecords->startptr = record_cross->record->start.wal.lsn;

                /* free */
                recordcross_free(record_cross);
                loadrecords->page_last_record_incomplete = NULL;

                return false;
            }

            temp_offset = record_cross->remainlen;

            record_cross->remainlen += realSize;

            /* assemble */
            temp_record_ptr = (char*)record_cross->record->data;
            rmemcpy0(temp_record_ptr, temp_offset, buff->data + *offset, realSize);

            if (record_cross->remainlen == record_cross->totallen)
            {
                /* already a complete record, append to results */
                record* complete_record = record_cross->record;

                /* set type to NORMAL */
                complete_record->type = RECORD_TYPE_WAL_NORMAL;
                complete_record->totallength = record_cross->totallen;

                /* set end */
                complete_record->end.wal.type = RECPOS_TYPE_WAL;
                complete_record->end.wal.lsn = loadrecords->block_startptr + *offset + realSize;
                complete_record->end.wal.timeline = loadrecords->timeline;

                if (!splitwork_wal_crccheck((uint8*)complete_record->data) ||
                    !check_prev_lsn((XLogRecord*)complete_record->data, loadrecords->prev))
                {
                    /* when not in same wal file */
                    if (!check_is_same_segno(
                            loadrecords->startptr, complete_record->start.wal.lsn, loadrecords))
                    {
                        /* close file descriptor, reopen previous file on read */
                        loadrecords->loadpageroutine->loadpageclose(loadrecords->loadpage);
                    }

                    loadrecords->startptr = complete_record->start.wal.lsn;
                    elog(RLOG_DEBUG,
                         "record crc or prev check failed, lsn:%X/%X",
                         (uint32)(loadrecords->startptr >> 32),
                         (uint32)loadrecords->startptr);
                    /* free */
                    recordcross_free(record_cross);
                    loadrecords->page_last_record_incomplete = NULL;

                    /* on verification failure, wait 200ms */
                    wal_usleep(20 * 1000);
                    return false;
                }

                /* clear page_last_record_incomplete */
                record_cross->record = NULL;

                /* append complete record to doubly linked list */
                loadrecords->records = dlist_put(loadrecords->records, complete_record);

                /* update prev */
                loadrecords->prev = complete_record->start.wal.lsn;

                g_walrecno++;

                /* free */
                recordcross_free(loadrecords->page_last_record_incomplete);
                loadrecords->page_last_record_incomplete = NULL;
            }

            *offset += realSize;
            updateCurrentPtr(*currentPtr, loadrecords, *offset);
        }
        else if (CHECK_STATUS_REWIND(loadrecords->startptr, loadrecords->endptr) &&
                 (seg_first || check_seg_first_incomplete_record_incompleted(loadrecords)))
        {
            /* first incomplete record of first segment (may span multiple blocks) */
            uint32 realSize = MAXALIGN(phdr->xlp_rem_len);
            /* free size */
            uint32 freeSpace = buff->size - *offset;

            if (loadrecords->seg_first_incomplete)
            {
                /* first block of file already exists */
                recordcross* record_cross = loadrecords->seg_first_incomplete;

                /* determine actual length */
                realSize = (realSize <= freeSpace) ? realSize : freeSpace;

                /* copy */
                rmemcpy0(record_cross->record->data,
                         record_cross->remainlen,
                         buff->data + *offset,
                         realSize);

                record_cross->remainlen += realSize;

                *offset += realSize;
                updateCurrentPtr(*currentPtr, loadrecords, *offset);

                /* cannot be complete here, so skip integrity check */

                /* always set end lsn */
                record_cross->record->end.wal.type = RECPOS_TYPE_WAL;
                record_cross->record->end.wal.lsn = *currentPtr;
                record_cross->record->end.wal.timeline = loadrecords->timeline;
            }
            else
            {
                /* first block of file does not exist */
                recordcross* record_cross = recordcross_init();
                record*      record = record_init();

                record_cross->record = record;

                /* determine actual length */
                realSize = (realSize <= freeSpace) ? realSize : freeSpace;

                /* set length */
                record_cross->totallen = MAXALIGN(phdr->xlp_rem_len);
                record_cross->remainlen = realSize;

                record->type = RECORD_TYPE_WAL_CROSS;
                record->data = rmalloc0(record_cross->totallen);
                if (!record->data)
                {
                    elog(RLOG_ERROR, "oom");
                }
                rmemset0(record->data, 0, 0, record_cross->totallen);

                /* copy */
                rmemcpy0(record->data, 0, buff->data + *offset, realSize);

                *offset += realSize;

                updateCurrentPtr(*currentPtr, loadrecords, *offset);

                /* set end lsn */
                record->end.wal.type = RECPOS_TYPE_WAL;
                record->end.wal.lsn = *currentPtr;
                record->end.wal.timeline = loadrecords->timeline;

                /* append to rctl */
                loadrecords->seg_first_incomplete = record_cross;
            }
        }
        else
        {
            /* on verification failure, wait 200ms */
            wal_usleep(20 * 1000);

            /* no incomplete record and page header records remaining length */
            elog(RLOG_INFO,
                 "split record, have remain data, may flush slow, try again, lsn:%X/%X",
                 (uint32)(*currentPtr >> 32),
                 currentPtr);
            return false;
        }
    }
    return true;
}

/* split from record start */
static bool splitStartWithRecordBegin(char*           currentBuf,
                                      loadwalrecords* loadrecords,
                                      mpage*          buff,
                                      XLogRecPtr*     currentPtr,
                                      uint32_t*       offset)
{
    /* read 4-byte length information */
    totalLen* total_len = (totalLen*)(buff->data + *offset);

    if (loadrecords->page_last_record_incomplete)
    {
        /* incomplete record from previous block should not exist here, first half data error */

        /* when not in same wal file */
        if (!check_is_same_segno(loadrecords->startptr,
                                 loadrecords->page_last_record_incomplete->record->start.wal.lsn,
                                 loadrecords))
        {
            /* close file descriptor, reopen previous file on read */
            loadrecords->loadpageroutine->loadpageclose(loadrecords->loadpage);
        }
        loadrecords->startptr = loadrecords->page_last_record_incomplete->record->start.wal.lsn;

        /* free */
        recordcross_free(loadrecords->page_last_record_incomplete);
        loadrecords->page_last_record_incomplete = NULL;

        return false;
    }

    /* first check if length is 0 */
    if (total_len->len == 0)
    {
        /* WAL data may not be written yet, or switch xlog occurred, or timeline changed */
        if (loadrecords->records)
        {
            /* existing record exists */
            XLogRecord* record = (XLogRecord*)loadrecords->records->tail->value;

            /* check if last record is switch */
            if (IsSwitchXlog(record->xl_rmid, record->xl_info))
            {
                loadrecords->startptr += loadrecords->loadpage->filesize;
                loadrecords->startptr -=
                    XLogSegmentOffset(loadrecords->startptr, loadrecords->loadpage->filesize);
                elog(RLOG_DEBUG, "get switch log, start ptr reset to %lu", loadrecords->startptr);

                /* close file descriptor */
                loadrecords->loadpageroutine->loadpageclose(loadrecords->loadpage);
            }
        }

        /* here, assume reached latest point, return false */
        loadrecords->startptr = *currentPtr;
        return false;
    }

    /* check if crosses block */
    if (*offset + total_len->len > buff->size)
    {
        /* crosses block, initialize incomplete record */
        recordcross* recordcross = recordcross_init();

        recordcross->totallen = MAXALIGN(total_len->len);

        /* maximum value is 1G - 1, minimum is 24 */
        if (!check_record_len_is_valid(total_len->len))
        {
            /* on verification failure, wait 200ms */
            wal_usleep(20 * 1000);

            elog(RLOG_INFO, "irecord malloc len:%u, wait next flush", recordcross->totallen);

            recordcross_free(recordcross);
            loadrecords->startptr = *currentPtr;

            return false;
        }

        /* calculate valid length */
        recordcross->remainlen = buff->size - *offset;

        recordcross->record = record_init();

        /* allocate length */
        recordcross->record->data = rmalloc0(recordcross->totallen);
        rmemset0(recordcross->record->data, 0, 0, recordcross->totallen);
        rmemcpy0(recordcross->record->data, 0, buff->data + *offset, buff->size - *offset);

        recordcross->record->totallength = recordcross->totallen;
        recordcross->record->start.wal.type = RECPOS_TYPE_WAL;
        recordcross->record->start.wal.timeline = loadrecords->timeline;
        recordcross->record->start.wal.lsn = *currentPtr;

        loadrecords->page_last_record_incomplete = recordcross;

        *offset = buff->size;
        updateCurrentPtr(*currentPtr, loadrecords, *offset);
    }
    else
    {
        /* does not cross block, normal parsing */
        uint32  realSize = MAXALIGN(total_len->len);
        record* recordEntry = record_init();

        /* maximum value is 1G - 1, minimum is 24 */
        if (!check_record_len_is_valid(total_len->len))
        {
            /* on verification failure, wait 200ms */
            wal_usleep(20 * 1000);

            record_free(recordEntry);
            loadrecords->startptr = *currentPtr;

            return false;
        }

        recordEntry->totallength = realSize;
        recordEntry->start.wal.type = RECPOS_TYPE_WAL;
        recordEntry->start.wal.lsn = *currentPtr;
        recordEntry->start.wal.timeline = loadrecords->timeline;

        recordEntry->end.wal.type = RECPOS_TYPE_WAL;
        recordEntry->end.wal.lsn = *currentPtr + realSize;
        recordEntry->end.wal.timeline = loadrecords->timeline;

        recordEntry->data = rmalloc0(realSize);
        rmemset0(recordEntry->data, 0, 0, realSize);
        rmemcpy0(recordEntry->data, 0, buff->data + *offset, realSize);

        if (!splitwork_wal_crccheck((uint8*)recordEntry->data) ||
            !check_prev_lsn((XLogRecord*)recordEntry->data, loadrecords->prev))
        {
            /* on verification failure, wait 200ms */
            wal_usleep(20 * 1000);

            loadrecords->startptr = recordEntry->start.wal.lsn;
            elog(RLOG_DEBUG,
                 "record crc or prev check failed, lsn:%X/%X",
                 (uint32)(loadrecords->startptr >> 32),
                 (uint32)loadrecords->startptr);
            record_free(recordEntry);

            return false;
        }

        loadrecords->records = dlist_put(loadrecords->records, recordEntry);

        /* update prev */
        loadrecords->prev = recordEntry->start.wal.lsn;

        g_walrecno++;

        *offset += realSize;
        updateCurrentPtr(*currentPtr, loadrecords, *offset);
    }

    return true;
}

bool loadwalrecords_load(loadwalrecords* loadrecords)
{
    XLogRecPtr currentPtr = loadrecords->startptr;
    uint32     offset = 0;
    char*      currentBuf = NULL;
    mpage*     buff = loadrecords->page;
    recpos     pos = {{'\0'}};

    pos.wal.type = RECPOS_TYPE_WAL;
    pos.wal.lsn = currentPtr;
    pos.wal.timeline = loadrecords->timeline;

    loadrecords->loadpageroutine->loadpagesetstartpos(loadrecords->loadpage, pos);

    /* read page */
    if (false == loadtwalrecords_loadpage(loadrecords))
    {
        elog(RLOG_WARNING, "in loadwalrecords_load, call loadtwalrecords_loadpage false");
        return false;
    }

    offset = (uint32)(loadrecords->startptr - loadrecords->block_startptr);

    currentBuf = (char*)buff->data + offset;

    while (offset < buff->size && loadwalrecords_checkend(currentPtr, loadrecords))
    {
        /* marks start from block header */
        if (IsXlogPageBegin(currentPtr, buff->size))
        {
            if (!splitStartWithBlockBegin(currentBuf, loadrecords, buff, &currentPtr, &offset))
            {
                /* return directly */
                return false;
            }
        }
        else /* start from record */
        {
            if (!splitStartWithRecordBegin(currentBuf, loadrecords, buff, &currentPtr, &offset))
            {
                /* return directly */
                return false;
            }
        }
    }

    /* update startptr */
    loadrecords->startptr = currentPtr;

    return true;
}

static void loadwalrecords_recorddlist_clean(loadwalrecords* loadrecords)
{
    if (!loadrecords->records)
    {
        return;
    }

    /* delete doubly linked list */
    dlist_free(loadrecords->records, (dlistvaluefree)record_free);

    /* set to null */
    loadrecords->records = NULL;
}

void loadwalrecords_clean(loadwalrecords* loadrecords)
{
    /* close file */
    loadrecords->loadpageroutine->loadpageclose(loadrecords->loadpage);

    /* free already split records */
    loadwalrecords_recorddlist_clean(loadrecords);

    /* reset */
    loadrecords->block_startptr = InvalidXLogRecPtr;
    loadrecords->startptr = InvalidXLogRecPtr;
    loadrecords->endptr = InvalidXLogRecPtr;
    loadrecords->prev = InvalidXLogRecPtr;

    /* clean */
    if (loadrecords->seg_first_incomplete)
    {
        recordcross_free(loadrecords->seg_first_incomplete);
        loadrecords->seg_first_incomplete = NULL;
    }
    if (loadrecords->seg_first_incomplete_next)
    {
        recordcross_free(loadrecords->seg_first_incomplete_next);
        loadrecords->seg_first_incomplete_next = NULL;
    }

    if (loadrecords->page_last_record_incomplete)
    {
        recordcross_free(loadrecords->page_last_record_incomplete);
        loadrecords->page_last_record_incomplete = NULL;
    }

    /* cleanup mpage */
    rmemset0(loadrecords->page->data, 0, 0, loadrecords->loadpage->blksize);
}

bool loadwalrecords_merge_seg_last_record(loadwalrecords* rctl)
{
    recordcross* f_record = rctl->page_last_record_incomplete;
    recordcross* l_record = rctl->seg_first_incomplete_next;

    record*      record = NULL;

    /* check if exists, if not exists is error case, return directly */
    if (!rctl->page_last_record_incomplete)
    {
        elog(RLOG_ERROR, "split merge seg first incomplete record false");
        return false;
    }

    /* check if length matches */
    if (f_record->totallen != f_record->remainlen + l_record->remainlen)
    {
        elog(RLOG_ERROR, "when split rewinding, incomplete record have wrong length");
        return false;
    }

    /* verification passed, assemble, use previous incomplete record */
    record = f_record->record;
    record->totallength = f_record->totallen;
    record->end.wal.type = RECPOS_TYPE_WAL;
    record->end.wal.lsn = l_record->record->end.wal.lsn;
    record->end.wal.timeline = l_record->record->end.wal.timeline;

    /* copy second half after first half */
    rmemcpy0(record->data, f_record->remainlen, l_record->record->data, l_record->totallen);

    /* set to null after copy */
    f_record->record = NULL;

    /* crc and prev verification */
    if (!splitwork_wal_crccheck((uint8*)record->data) ||
        !check_prev_lsn((XLogRecord*)record->data, rctl->prev))
    {
        /* verification should not fail, there is a problem here */
        elog(RLOG_ERROR,
             "when split rewind, record crc or prev check failed, lsn:%X/%X",
             (uint32)(rctl->startptr >> 32),
             (uint32)rctl->startptr);
    }

    /* verification passed */
    rctl->records = dlist_put(rctl->records, record);

    /* update */
    rctl->prev = record->start.wal.lsn;
    g_walrecno++;

    /* cleanup, cleanup previous incomplete record and next file header incomplete record, previous
     * incomplete record already set to null */
    recordcross_free(f_record);
    recordcross_free(l_record);

    /* set to null */
    rctl->seg_first_incomplete_next = NULL;
    rctl->page_last_record_incomplete = NULL;

    return true;
}
