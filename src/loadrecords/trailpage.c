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

/* page 校验 */
bool trailpage_valid(mpage* mp)
{
    if(NULL == mp)
    {
        return true;
    }

    return true;
}

/* 将 page 拆分为 records */
bool trailpage_page2records(loadtrailrecords* ltrailrecords, mpage* mp)
{
    uint8 tokenid                       = FFTRAIL_GROUPTYPE_NOP;
    uint8 tokeninfo                     = FFTRAIL_INFOTYPE_TOKEN;
    uint16 subtype                      = FF_DATA_TYPE_NOP;
    record_type rectype          = RECORD_TYPE_NOP;
    uint32 tokenlen                     = 0;
    uint32 recminsize                   = 0;
    uint32 blkoffset                    = 0;
    uint64 foffset                      = 0;
    XLogRecPtr temporgpos               = 0;
    uint8* uptr                         = NULL;
    uint8* uptr1                        = NULL;
    uint8* tokendata                    = NULL;
    dlistnode* dlnode                   = NULL;
    record* rec                  = NULL;
    ffsmgr_state ffsmgrstate;

    UNUSED(tokendata);

    /* 初始化验证接口 */
    ffsmgr_init(FFSMG_IF_TYPE_TRAIL, &ffsmgrstate);
    ffsmgrstate.compatibility = ltrailrecords->compatibility;

    /* 获取保留长度 */
    recminsize = ffsmgrstate.ffsmgr->ffsmg_gettokenminsize(ltrailrecords->compatibility);
    foffset = ltrailrecords->foffset;
    foffset -= (foffset & LOADPAGEBLKSIZEMASK(ltrailrecords->loadpage->blksize));
    if(ltrailrecords->loadpage->filesize == ((uint64)ltrailrecords->loadpage->blksize + foffset))
    {
        recminsize += ffsmgrstate.ffsmgr->ffsmg_gettailsize(ltrailrecords->compatibility);
    }

    /* 换算实际的位置 */
    blkoffset = (uint32)(ltrailrecords->foffset & (LOADPAGEBLKSIZEMASK(ltrailrecords->loadpage->blksize)));
    uptr = mp->data;
    uptr += blkoffset;
    
    uptr1 = uptr;
    while(1)
    {
        uptr = uptr1;
        FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
        switch (tokenid)
        {
            case FFTRAIL_GROUPTYPE_FHEADER:
                if(false == ffsmgrstate.ffsmgr->ffsmgr_validrecord(tokenid, (void*)&ffsmgrstate, tokeninfo, ltrailrecords->fileid, uptr1))
                {
                    /* 没有验证通过，说明到此为一个完整的 record */
                    ltrailrecords->loadrecords.error = ERROR_BLK_INCOMPLETE;
                    goto trailpage_page2records_done;
                }

                /* 只在头部处偏移 */
                rectype = RECORD_TYPE_TRAIL_HEADER;
                tokenlen = MAXALIGN(tokenlen);
                break;
            case FFTRAIL_GROUPTYPE_DATA:
                if(false == ffsmgrstate.ffsmgr->ffsmgr_validrecord(tokenid, (void*)&ffsmgrstate, tokeninfo, ltrailrecords->fileid, uptr1))
                {
                    ltrailrecords->loadrecords.error = ERROR_BLK_INCOMPLETE;
                    goto trailpage_page2records_done;
                }

                /* 暂不关注 */
                rectype = RECORD_TYPE_TRAIL_NORMAL;
                temporgpos = ffsmgrstate.ffsmgr->ffsmgr_getrecordlsn(&ffsmgrstate, uptr1);
                if (InvalidXLogRecPtr != temporgpos)
                {
                    ltrailrecords->orgpos.wal.lsn = temporgpos;
                }

                break;
            case FFTRAIL_GROUPTYPE_RESET:
                if(false == ffsmgrstate.ffsmgr->ffsmgr_validrecord(tokenid, (void*)&ffsmgrstate, tokeninfo, ltrailrecords->fileid, uptr1))
                {
                    ltrailrecords->loadrecords.error = ERROR_BLK_INCOMPLETE;
                    goto trailpage_page2records_done;
                }
                
                /* type 为 RESET */
                rectype = RECORD_TYPE_TRAIL_RESET;
                break;
            case FFTRAIL_GROUPTYPE_FTAIL:
                if(false == ffsmgrstate.ffsmgr->ffsmgr_validrecord(tokenid, (void*)&ffsmgrstate, tokeninfo, ltrailrecords->fileid, uptr1))
                {
                    ltrailrecords->loadrecords.error = ERROR_BLK_INCOMPLETE;
                    goto trailpage_page2records_done;
                }
                rectype = RECORD_TYPE_TRAIL_TAIL;
                break;
            case FFTRAIL_GROUPTYPE_NOP:
                    /* 读取到了尾部，等待新的写入, 标记等待, 返回 recentry */
                    ltrailrecords->loadrecords.error = ERROR_BLK_INCOMPLETE;
                    goto trailpage_page2records_done;
            default:
                elog(RLOG_WARNING, "unknown group type:%u, foffset:%lu, fileid:%lu",
                                    tokenid, ltrailrecords->foffset, ltrailrecords->fileid);
                return false;
        }

        rec = record_init();
        if(NULL == rec)
        {
            elog(RLOG_WARNING, "record init error, out of memory");
            return false;
        }

        rec->type = rectype;
        rec->start.trail.offset = ltrailrecords->foffset;               /* 基于文件的起始位置 */
        rec->start.trail.fileid = ltrailrecords->fileid;
        rec->end.trail.offset = (rec->start.trail.offset + tokenlen);   /* 基于文件的结束位置 */
        rec->end.trail.fileid = ltrailrecords->fileid;
        rec->len = tokenlen;
        rec->data = (uint8*)rmalloc0(rec->len);
        if(NULL == rec->data)
        {
            elog(RLOG_WARNING, "out of memory");
            return false;
        }
        rmemcpy0(rec->data, 0, uptr1, rec->len);

        /* RESET/HEAHDER/TAIL 特殊的 record */
        if(RECORD_TYPE_TRAIL_RESET != rectype
            && RECORD_TYPE_TRAIL_HEADER  != rectype
            && RECORD_TYPE_TRAIL_TAIL != rectype)
        {
            /* 获取 totallength 和 reclength */
            rec->totallength = ffsmgrstate.ffsmgr->ffsmgr_getrecordtotallength(&ffsmgrstate, rec->data);
            rec->reallength = ffsmgrstate.ffsmgr->ffsmgr_getrecordlength(&ffsmgrstate, rec->data);
            rec->dataoffset = ffsmgrstate.ffsmgr->ffsmgr_getrecorddataoffset(ffsmgrstate.compatibility);
        }
        ltrailrecords->records = dlist_put(ltrailrecords->records, rec);

        /* buffer 偏移 */
        uptr1 += tokenlen;
        blkoffset += tokenlen;
        ltrailrecords->foffset += tokenlen;

        /* 查看剩余的内容是否足够满足最小要求 */
        if((ltrailrecords->loadpage->blksize - blkoffset) > recminsize)
        {
            /* 证明还有剩余的数据 */
            continue;
        }

        /* 指向下个页的开始 */
        ltrailrecords->foffset += (ltrailrecords->loadpage->blksize - blkoffset);
        break;
    }

trailpage_page2records_done:
    /* 
     * 对返回的数据重新设置 rectype
     * 1、查看最后一个是不是跨页的
     * 2、首个 record 是不是上个 record 的延续
     */
    if(true == dlist_isnull(ltrailrecords->records))
    {
        /* 如果为空, 那么证明挖掘到最后了 */
        return true;
    }

    for(dlnode = ltrailrecords->records->tail; NULL != dlnode; dlnode = dlnode->prev)
    {
        rec = (record*)dlnode->value;
        if(RECORD_TYPE_TRAIL_RESET == rec->type
            || RECORD_TYPE_TRAIL_TAIL == rec->type
            || RECORD_TYPE_TRAIL_HEADER == rec->type)
        {
            continue;
        }

        /* 
         * 查看最后一个是不是跨页的
         *  获取 总长度 和 record 长度， 总长度 > record 长度 说明此 record 不完整
         */
        if(0 == rec->totallength || rec->totallength == rec->reallength)
        {
            /* 
                * 长度为0时, 那么说明此 record 是上个 record 延续, 同一个 record 不会即为 cont 又为 cross
                * 长度相等, 说明是一个完整的 record
                */
            break;
        }
        rec->type = RECORD_TYPE_TRAIL_CROSS;

        /* 重置该 record 中记录的长度 */
        ffsmgrstate.ffsmgr->ffsmgr_setrecordlength(&ffsmgrstate, rec->data, rec->totallength);
    }

    /* 
        * 从前向后设置,判断是否为 continue record
        *  首先获取 subtype, 若为 continue record 那么说明此 record 为上个页的延续 
        */
    for(dlnode = ltrailrecords->records->head; NULL != dlnode; dlnode = dlnode->next)
    {
        rec = (record*)dlnode->value;
        if(RECORD_TYPE_TRAIL_RESET == rec->type
            || RECORD_TYPE_TRAIL_TAIL == rec->type
            || RECORD_TYPE_TRAIL_HEADER == rec->type)
        {
            continue;
        }

        /* 
            * 查看首个是不是跨页的, 需要关注的是, 此 record 可能为 database record
            *  1、首先获取 subtype, 若为 continue record 那么说明此 record 为上个页的延续
            */
        if(false == ffsmgrstate.ffsmgr->ffsmgr_getrecordsubtype(&ffsmgrstate, rec->data, &subtype))
        {
            elog(RLOG_WARNING, "get record sub type error");
            return false;
        }

        /* dbmetadata 需要过滤掉, dbmetadata 不会跨页或跨文件 */
        if(FF_DATA_TYPE_DBMETADATA == subtype)
        {
            rec->type = RECORD_TYPE_TRAIL_DBMETA;
            continue;
        }

        /* table meta 可能会跨文件，但是在 trail 文件格式上考虑, 跨页或跨文件的非 table meta 在文件或页的首部不会出现新的 table meta */
        if(FF_DATA_TYPE_REC_CONTRECORD == subtype)
        {
            rec->type = RECORD_TYPE_TRAIL_CONT;
            continue;
        }
        break;
    }

    return true;
}

