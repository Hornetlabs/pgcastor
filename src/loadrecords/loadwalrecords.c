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

#define XLOG_PAGE_MAGIC_PG127 0xD101
#define XLOG_PAGE_MAGIC_PG149 0xD10D
#define XLOG_PAGE_MAGIC_PG95  0xD087

#define RecoedMaxAllocSize	            ((Size) 0x3fffffff) /* 1 gigabyte - 1 */
#define RecordAllocSizeIsValid(size)    ((Size) (size) <= RecoedMaxAllocSize)
#define updateCurrentPtr(cur, load, off) cur = (load)->block_startptr + off

typedef struct totalLen
{
    uint32 len;
}totalLen;

static bool splitwork_wal_crccheck(uint8 *bytes)
{
    pg_crc32c    crc;
    XLogRecord *record = NULL;

    record = (XLogRecord*)bytes;

    /* Calculate the CRC */
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, ((char *) record) + SizeOfXLogRecord, record->xl_tot_len - SizeOfXLogRecord);
    /* include the record header last */
    COMP_CRC32C(crc, (char *) record, offsetof(XLogRecord, xl_crc));
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
    return (magic == XLOG_PAGE_MAGIC_PG127 || magic == XLOG_PAGE_MAGIC_PG149 || magic == XLOG_PAGE_MAGIC_PG95);
}

static bool check_is_same_segno(XLogRecPtr ptr1, XLogRecPtr ptr2, loadwalrecords *rctl)
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

static bool check_prev_lsn(XLogRecord *record, XLogRecPtr prev)
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

static bool check_seg_first_incomplete_record_incompleted(loadwalrecords *rctl)
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
        (void) select(0, NULL, NULL, NULL, &delay);
    }
}

/* 初始化划分record的loadpage部分 */
static bool loadwalrecords_initloadpage(loadwalrecords* loadrecords, loadpage_type type)
{
    char *dirpath = NULL;

    /* 获取函数指针 */
    loadrecords->loadpageroutine = loadpage_getpageroutine(type);

    if(NULL == loadrecords->loadpageroutine)
    {
        elog(RLOG_WARNING, "set loadpage routine error");
        return false;
    }

    loadrecords->loadpage = loadrecords->loadpageroutine->loadpageinit();
    if(NULL == loadrecords->loadpage)
    {
        elog(RLOG_WARNING, "load page init error");
        return false;
    }

    /* 设置文件块大小和文件大小 */
    loadrecords->loadpage->blksize = g_blocksize;
    loadrecords->loadpage->filesize = g_walsegsize * 1048576;
    loadrecords->loadpageroutine->loadpagesettype(loadrecords->loadpage, LOADPAGEFROMFILE_TYPE_WAL);

    /* 设置文件夹路径 */
    dirpath = guc_getConfigOption(CFG_KEY_WAL_DIR);
    if(false == loadrecords->loadpageroutine->loadpagesetfilesource(loadrecords->loadpage, dirpath))
    {
        elog(RLOG_WARNING, "load trail record set load source error");
        return false;
    }

    return true;
}

loadwalrecords* loadwalrecords_init(void)
{
    loadwalrecords* result = NULL;
    mpage* page = NULL;

    result = rmalloc0(sizeof(loadwalrecords));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(loadwalrecords));

    /* 初始化loadpage相关 */
    if (!loadwalrecords_initloadpage(result, LOADPAGE_TYPE_FILE))
    {
        return NULL;
    }

    /* 初始化mpage */
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

    /* 释放不完整的record */
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

    /* 释放records链表 */
    if (NULL != loadrecords->records)
    {
        dlist_free(loadrecords->records, (dlistvaluefree)record_free);
    }

    /* 释放loadpage */
    if (NULL != loadrecords->loadpage)
    {
        loadrecords->loadpageroutine->loadpagefree(loadrecords->loadpage);
    }

    /* loadpageroutine不用释放, 置空 */
    loadrecords->loadpageroutine = NULL;

    /* 释放mpage */
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

static bool loadtwalrecords_loadpage(loadwalrecords *loadrecords)
{
    bool result = false;
    recpos pos = {{'\0'}};
    mpage *buff = loadrecords->page;

    /* 计算block开始的lsn */
    loadrecords->block_startptr = loadrecords->startptr - (loadrecords->startptr & (loadrecords->loadpage->blksize - 1));
    pos.wal.type = RECPOS_TYPE_WAL;
    pos.wal.timeline = loadrecords->timeline;
    pos.wal.lsn = loadrecords->block_startptr;

    /* 置0 */
    rmemset0(buff->data, 0, 0, loadrecords->loadpage->blksize);

    /* 设置pos */
    loadrecords->loadpageroutine->loadpagesetstartpos(loadrecords->loadpage, pos);

    /* 读取page */
    result = loadrecords->loadpageroutine->loadpage(loadrecords->loadpage, buff);

    /* 判断下一次读取是否是下一个wal文件 */
    if (0 == ((loadrecords->block_startptr + loadrecords->loadpage->blksize) % loadrecords->loadpage->filesize))
    {
        /* 关闭文件描述符 */
        loadrecords->loadpageroutine->loadpageclose(loadrecords->loadpage);
    }

    return result;
}

bool loadwalrecords_checkend(XLogRecPtr cur, loadwalrecords *rctl)
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

/* 从bolck头开始划分 */
static bool splitStartWithBlockBegin(char *currentBuf,
                                     loadwalrecords* loadrecords,
                                     mpage* buff,
                                     XLogRecPtr *currentPtr,
                                     uint32_t *offset)
{
    XLogPageHeader phdr = (XLogPageHeader) (currentBuf);
    bool seg_first = false;

    /* magic校验 */
    if (!(check_magic(phdr->xlp_magic)) || phdr->xlp_pageaddr != loadrecords->block_startptr)
    {
        /* 校验失败时, 等待200ms */
        wal_usleep(20 * 1000);
        elog(RLOG_DEBUG, "page header magic check failed, wait, lsn:%X/%X",
                               (uint32) (loadrecords->startptr >> 32),
                               (uint32) loadrecords->startptr);
        return false;
    }

    /* 判断是否为wal文件开始 */
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
        /* 失败时, 等待200ms */
        wal_usleep(20 * 1000);

        if (loadrecords->page_last_record_incomplete)
        {
            /* 此处不应存在不完整的record, 前半段数据错误 */

            /* 不存在不完整record且pageheader中记录了剩余长度 */
            elog(RLOG_INFO, "split record, have incomplete record but not remain data, may flush slow, try again");

            /* 不处于同一个wal文件时,  */
            if (!check_is_same_segno(loadrecords->startptr, loadrecords->page_last_record_incomplete->record->start.wal.lsn, loadrecords))
            {
                /* 关闭文件描述符, 在读取时重新打开上个文件 */
                loadrecords->loadpageroutine->loadpageclose(loadrecords->loadpage);
            }
            loadrecords->startptr = loadrecords->page_last_record_incomplete->record->start.wal.lsn;

            /* 释放 */
            recordcross_free(loadrecords->page_last_record_incomplete);
            loadrecords->page_last_record_incomplete = NULL;

            return false;
        }
    }

    if (phdr->xlp_rem_len > 0)
    {
        /* 是否存在不完整record */
        if (loadrecords->page_last_record_incomplete && !check_seg_first_incomplete_record_incompleted(loadrecords))
        {
            recordcross *record_cross = loadrecords->page_last_record_incomplete;
            uint32      temp_offset = 0;
            char       *temp_record_ptr = (char *)record_cross->record->data;
            uint32      realSize = MAXALIGN(phdr->xlp_rem_len);
            uint32      freeSpace = buff->size - *offset;
            uint32      irecord_need_len = record_cross->totallen - record_cross->remainlen;

            realSize = (realSize <= freeSpace) ? realSize : freeSpace;

            if (realSize > irecord_need_len)
            {
                /* 校验失败时, 等待200ms */
                wal_usleep(20 * 1000);

                elog(RLOG_INFO, "record split, page hdr remain len > incomplete record len, waitting");

                /* 不处于同一个wal文件时,  */
                if (!check_is_same_segno(loadrecords->startptr, record_cross->record->start.wal.lsn, loadrecords))
                {
                    /* 关闭文件描述符, 在读取时重新打开上个文件 */
                    loadrecords->loadpageroutine->loadpageclose(loadrecords->loadpage);
                }
                loadrecords->startptr = record_cross->record->start.wal.lsn;

                /* 释放 */
                recordcross_free(record_cross);
                loadrecords->page_last_record_incomplete = NULL;

                return false;
            }

            temp_offset = record_cross->remainlen;

            record_cross->remainlen += realSize;

            /* 组装 */
            temp_record_ptr = (char *)record_cross->record->data;
            rmemcpy0(temp_record_ptr, temp_offset, buff->data + *offset, realSize);

            if (record_cross->remainlen == record_cross->totallen)
            {
                /* 已经是完整record, 附加到结果中 */
                record *complete_record = record_cross->record;

                /* type设置为NORMAL */
                complete_record->type = RECORD_TYPE_WAL_NORMAL;
                complete_record->totallength = record_cross->totallen;

                /* 设置end */
                complete_record->end.wal.type = RECPOS_TYPE_WAL;
                complete_record->end.wal.lsn = loadrecords->block_startptr + *offset + realSize;
                complete_record->end.wal.timeline = loadrecords->timeline;

                if (!splitwork_wal_crccheck((uint8 *)complete_record->data)
                 || !check_prev_lsn((XLogRecord *)complete_record->data, loadrecords->prev))
                {
                    /* 不处于同一个wal文件时,  */
                    if (!check_is_same_segno(loadrecords->startptr, complete_record->start.wal.lsn, loadrecords))
                    {
                        /* 关闭文件描述符, 在读取时重新打开上个文件 */
                        loadrecords->loadpageroutine->loadpageclose(loadrecords->loadpage);
                    }

                    loadrecords->startptr = complete_record->start.wal.lsn;
                    elog(RLOG_DEBUG, "record crc or prev check failed, lsn:%X/%X",
                                    (uint32) (loadrecords->startptr >> 32),
                                    (uint32) loadrecords->startptr);
                    /* 释放 */
                    recordcross_free(record_cross);
                    loadrecords->page_last_record_incomplete = NULL;

                    /* 校验失败时, 等待200ms */
                    wal_usleep(20 * 1000);
                    return false;
                }

                /* 清空 page_last_record_incomplete */
                record_cross->record = NULL;

                /* 将完整的record附加到双向链表中 */
                loadrecords->records = dlist_put(loadrecords->records, complete_record);

                /* 更新prev */
                loadrecords->prev = complete_record->start.wal.lsn;

                g_walrecno++;

                /* 释放 */
                recordcross_free(loadrecords->page_last_record_incomplete);
                loadrecords->page_last_record_incomplete = NULL;
            }

            *offset += realSize;
            updateCurrentPtr(*currentPtr, loadrecords, *offset);
        }
        else if (CHECK_STATUS_REWIND(loadrecords->startptr, loadrecords->endptr)
                 && (seg_first || check_seg_first_incomplete_record_incompleted(loadrecords)))
        {
            /* 第一个seg段的第一条不完整record(可能跨多个块) */
            uint32 realSize = MAXALIGN(phdr->xlp_rem_len);
            /* 空闲大小 */
            uint32 freeSpace = buff->size - *offset;

            if (loadrecords->seg_first_incomplete)
            {
                /* 已经存在文件第一个block */
                recordcross *record_cross = loadrecords->seg_first_incomplete;

                /* 确定真实长度 */
                realSize = (realSize <= freeSpace) ? realSize : freeSpace;

                /* 拷贝 */
                rmemcpy0(record_cross->record->data, record_cross->remainlen, buff->data + *offset, realSize);

                record_cross->remainlen += realSize;

                *offset += realSize;
                updateCurrentPtr(*currentPtr, loadrecords, *offset);

                /* 这里不可能完整, 因此不做完整性检查 */

                /* 每次都设置结束lsn */
                record_cross->record->end.wal.type = RECPOS_TYPE_WAL;
                record_cross->record->end.wal.lsn = *currentPtr;
                record_cross->record->end.wal.timeline = loadrecords->timeline;
            }
            else
            {
                /* 不存在文件第一个block */
                recordcross *record_cross = recordcross_init();
                record *record = record_init();

                record_cross->record = record;

                /* 确定真实长度 */
                realSize = (realSize <= freeSpace) ? realSize : freeSpace;

                /* 设置长度 */
                record_cross->totallen = MAXALIGN(phdr->xlp_rem_len);
                record_cross->remainlen = realSize;

                record->type = RECORD_TYPE_WAL_CROSS;
                record->data = rmalloc0(record_cross->totallen);
                if (!record->data)
                {
                    elog(RLOG_ERROR, "oom");
                }
                rmemset0(record->data, 0, 0, record_cross->totallen);

                /* 拷贝 */
                rmemcpy0(record->data, 0, buff->data + *offset, realSize);

                *offset += realSize;

                updateCurrentPtr(*currentPtr, loadrecords, *offset);

                /* 设置结束lsn */
                record->end.wal.type = RECPOS_TYPE_WAL;
                record->end.wal.lsn = *currentPtr;
                record->end.wal.timeline = loadrecords->timeline;

                /* 附加到rctl中 */
                loadrecords->seg_first_incomplete = record_cross;
            }
        }
        else
        {
            /* 校验失败时, 等待200ms */
            wal_usleep(20 * 1000);

            /* 不存在不完整record且pageheader中记录了剩余长度 */
            elog(RLOG_INFO, "split record, have remain data, may flush slow, try again, lsn:%X/%X", (uint32)(*currentPtr >> 32), currentPtr);
            return false;
        }
    }
    return true;
}

/* 从record开始划分 */
static bool splitStartWithRecordBegin(char* currentBuf,
                                      loadwalrecords* loadrecords,
                                      mpage* buff,
                                      XLogRecPtr* currentPtr,
                                      uint32_t* offset)
{
    /* 读取4字节的长度信息 */
    totalLen* total_len = (totalLen*) (buff->data + *offset);

    if (loadrecords->page_last_record_incomplete)
    {
        /* 此处不应存在前一个块不完整的record, 前半段数据错误 */

        /* 不处于同一个wal文件时,  */
        if (!check_is_same_segno(loadrecords->startptr, loadrecords->page_last_record_incomplete->record->start.wal.lsn, loadrecords))
        {
            /* 关闭文件描述符, 在读取时重新打开上个文件 */
            loadrecords->loadpageroutine->loadpageclose(loadrecords->loadpage);
        }
        loadrecords->startptr = loadrecords->page_last_record_incomplete->record->start.wal.lsn;

        /* 释放 */
        recordcross_free(loadrecords->page_last_record_incomplete);
        loadrecords->page_last_record_incomplete = NULL;

        return false;
    }

    /* 首先判断长度是否为0 */
    if (total_len->len == 0)
    {
        /* 可能WAL数据还未写入, 或者发生了switch xlog, 或者时间线变更 */
        if (loadrecords->records)
        {
            /* 存在已有record */
            XLogRecord* record = (XLogRecord*) loadrecords->records->tail->value;

            /* 判断最后一条record是否为switch */
            if (IsSwitchXlog(record->xl_rmid, record->xl_info))
            {
                loadrecords->startptr += loadrecords->loadpage->filesize;
                loadrecords->startptr -= XLogSegmentOffset(loadrecords->startptr, loadrecords->loadpage->filesize);
                elog(RLOG_DEBUG, "get switch log, start ptr reset to %lu", loadrecords->startptr);

                /* 关闭文件描述符 */
                loadrecords->loadpageroutine->loadpageclose(loadrecords->loadpage);
            }
        }

        /* 在这里, 暂时认为追到了最新点, 返回false */
        loadrecords->startptr = *currentPtr;
        return false;
    }

    /* 判断是否跨block */
    if (*offset + total_len->len > buff->size)
    {
        /* 跨block, 初始化incomplete record */
        recordcross* recordcross = recordcross_init();

        recordcross->totallen = MAXALIGN(total_len->len);

        /* 最大值为 1G - 1 最小值为 24 */
        if(!check_record_len_is_valid(total_len->len))
        {
            /* 校验失败时, 等待200ms */
            wal_usleep(20 * 1000);

            elog(RLOG_INFO, "irecord malloc len:%u, wait next flush", recordcross->totallen);

            recordcross_free(recordcross);
            loadrecords->startptr = *currentPtr;

            return false;
        }

        /* 计算有效长度 */
        recordcross->remainlen = buff->size - *offset;

        recordcross->record = record_init();

        /* 分配长度 */
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
        /* 不跨block, 正常解析 */
        uint32 realSize = MAXALIGN(total_len->len);
        record* recordEntry = record_init();

        /* 最大值为 1G - 1 最小值为 24 */
        if(!check_record_len_is_valid(total_len->len))
        {
            /* 校验失败时, 等待200ms */
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

        if (!splitwork_wal_crccheck((uint8 *)recordEntry->data)
         || !check_prev_lsn((XLogRecord *)recordEntry->data, loadrecords->prev))
        {
            /* 校验失败时, 等待200ms */
            wal_usleep(20 * 1000);

            loadrecords->startptr = recordEntry->start.wal.lsn;
            elog(RLOG_DEBUG, "record crc or prev check failed, lsn:%X/%X",
                               (uint32) (loadrecords->startptr >> 32),
                               (uint32) loadrecords->startptr);
            record_free(recordEntry);

            return false;
        }

        loadrecords->records = dlist_put(loadrecords->records, recordEntry);

        /* 更新prev */
        loadrecords->prev = recordEntry->start.wal.lsn;

        g_walrecno++;

        *offset += realSize;
        updateCurrentPtr(*currentPtr, loadrecords, *offset);
    }

    return true;
}

bool loadwalrecords_load(loadwalrecords* loadrecords)
{
    XLogRecPtr  currentPtr = loadrecords->startptr;
    uint32      offset = 0;
    char*       currentBuf = NULL;
    mpage*      buff = loadrecords->page;
    recpos pos = {{'\0'}};

    pos.wal.type = RECPOS_TYPE_WAL;
    pos.wal.lsn = currentPtr;
    pos.wal.timeline = loadrecords->timeline;

    loadrecords->loadpageroutine->loadpagesetstartpos(loadrecords->loadpage, pos);

    /* 读取页 */
    if (false == loadtwalrecords_loadpage(loadrecords))
    {
        elog(RLOG_WARNING, "in loadwalrecords_load, call loadtwalrecords_loadpage false");
        return false;
    }

    offset = (uint32)(loadrecords->startptr - loadrecords->block_startptr);

    currentBuf = (char*)buff->data + offset;

    while (offset < buff->size && loadwalrecords_checkend(currentPtr, loadrecords))
    {
        /* 标志着从blockheader开始 */
        if (IsXlogPageBegin(currentPtr, buff->size))
        {
            if (!splitStartWithBlockBegin(currentBuf, loadrecords, buff, &currentPtr, &offset))
            {
                /* 直接返回 */
                return false;
            }
        }
        else /* 从record开始 */
        {
            if (!splitStartWithRecordBegin(currentBuf, loadrecords, buff, &currentPtr, &offset))
            {
                /* 直接返回 */
                return false;
            }
        }
    }

    /* 更新startptr */
    loadrecords->startptr = currentPtr;

    return true;
}

static void loadwalrecords_recorddlist_clean(loadwalrecords* loadrecords)
{
    if (!loadrecords->records)
    {
        return;
    }

    /* 删除双向链表 */
    dlist_free(loadrecords->records, (dlistvaluefree )record_free);

    /* 置空 */
    loadrecords->records = NULL;
}

void loadwalrecords_clean(loadwalrecords* loadrecords)
{
    /* 关闭文件 */
    loadrecords->loadpageroutine->loadpageclose(loadrecords->loadpage);

    /* 释放已经划分好的record */
    loadwalrecords_recorddlist_clean(loadrecords);

    /* 重置 */
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

    /* 清理mpage */
    rmemset0(loadrecords->page->data, 0, 0, loadrecords->loadpage->blksize);
}

bool loadwalrecords_merge_seg_last_record(loadwalrecords *rctl)
{
    recordcross* f_record = rctl->page_last_record_incomplete;
    recordcross* l_record = rctl->seg_first_incomplete_next;

    record* record = NULL;

    /* 检查是否存在, 不存在时是错误情况, 直接返回 */
    if (!rctl->page_last_record_incomplete)
    {
        elog(RLOG_ERROR, "split merge seg first incomplete record false");
        return false;
    }

    /* 检查长度是否对应 */
    if (f_record->totallen != f_record->remainlen + l_record->remainlen)
    {
        elog(RLOG_ERROR, "when split rewinding, incomplete record have wrong length");
        return false;
    }

    /* 校验通过, 组装, 使用前一个不完整record */
    record = f_record->record;
    record->totallength = f_record->totallen;
    record->end.wal.type = RECPOS_TYPE_WAL;
    record->end.wal.lsn = l_record->record->end.wal.lsn;
    record->end.wal.timeline = l_record->record->end.wal.timeline;

    /* 复制后半部分到前半部分后 */
    rmemcpy0(record->data, f_record->remainlen, l_record->record->data, l_record->totallen);

    /* 复制完后置空 */
    f_record->record = NULL;

    /* crc和prev校验 */
    if (!splitwork_wal_crccheck((uint8 *)record->data)
     || !check_prev_lsn((XLogRecord *)record->data, rctl->prev))
    {
        /* 校验不应该失败, 这里有问题 */
        elog(RLOG_ERROR, "when split rewind, record crc or prev check failed, lsn:%X/%X",
                           (uint32) (rctl->startptr >> 32),
                           (uint32) rctl->startptr);
    }

    /* 校验通过 */
    rctl->records = dlist_put(rctl->records, record);

    /* 更新 */
    rctl->prev = record->start.wal.lsn;
    g_walrecno++;

    /* 清理, 清理前一个不完整record和下一个文件开头的不完整record, 前一个不完整record已经置空 */
    recordcross_free(f_record);
    recordcross_free(l_record);

    /* 置空 */
    rctl->seg_first_incomplete_next = NULL;
    rctl->page_last_record_incomplete = NULL;

    return true;
}
