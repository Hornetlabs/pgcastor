#include "ripple_app_incl.h"
#include "utils/hash/hash_search.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/guc/guc.h"
#include "utils/mpage/mpage.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadpagefromfile.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadtrailrecords.h"
#include "loadrecords/ripple_trailpage.h"

/* 初始化 */
ripple_loadtrailrecords* ripple_loadtrailrecords_init(void)
{
    ripple_loadtrailrecords* loadrecords = NULL;

    loadrecords = (ripple_loadtrailrecords*)rmalloc0(sizeof(ripple_loadtrailrecords));
    if(NULL == loadrecords)
    {
        elog(RLOG_WARNING, "load trail records init error, out of memory");
        return NULL;
    }
    rmemset0(loadrecords, 0, '\0', sizeof(ripple_loadtrailrecords));
    loadrecords->loadrecords.filesize = RIPPLE_MB2BYTE(guc_getConfigOptionInt(RIPPLE_CFG_KEY_TRAIL_MAX_SIZE));
    loadrecords->loadrecords.blksize = RIPPLE_FILE_BUFFER_SIZE;
    loadrecords->loadrecords.type = RIPPLE_LOADRECORDS_TYPE_TRAIL;
    loadrecords->compatibility = guc_getConfigOptionInt(RIPPLE_CFG_KEY_COMPATIBILITY);
    loadrecords->fileid = 0;
    loadrecords->foffset = 0;
    loadrecords->loadpage = NULL;
    loadrecords->loadpageroutine = NULL;
    loadrecords->records = NULL;
    loadrecords->recordcross.record = NULL;
    loadrecords->mp = rmalloc0(sizeof(mpage));
    if(NULL == loadrecords->mp)
    {
        elog(RLOG_WARNING, "loadtrailrecords init mpage error, out of memory");
        rfree(loadrecords);
        return NULL;
    }
    rmemset0(loadrecords->mp, 0, '\0', sizeof(mpage));
    rmemset1(loadrecords->recordcross.rectail, 0, '\0', RIPPLE_RECORD_TAIL_LEN);
    return loadrecords;
}

/* 设置加载trail文件的方式 */
bool ripple_loadtrailrecords_setloadpageroutine(ripple_loadtrailrecords* loadrecords, ripple_loadpage_type type)
{
    /* 在文件中加载 */
    loadrecords->loadpageroutine = ripple_loadpage_getpageroutine(type);
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
    loadrecords->loadpage->blksize = loadrecords->loadrecords.blksize;
    loadrecords->loadpage->filesize = loadrecords->loadrecords.filesize;

    /* 设置loadpagefromfile的类型 */
    loadrecords->loadpageroutine->loadpagesettype(loadrecords->loadpage, RIPPLE_LOADPAGEFROMFILE_TYPE_TRAIL);

    return true;
}

/* 设置加载的起点 */
void ripple_loadtrailrecords_setloadposition(ripple_loadtrailrecords* loadrecords, uint64 fileid, uint64 foffset)
{
    ripple_recpos recpos;
    loadrecords->fileid = fileid;
    loadrecords->foffset = foffset;

    recpos.trail.type = RIPPLE_RECPOS_TYPE_TRAIL;
    recpos.trail.fileid = fileid;
    recpos.trail.offset = foffset;
    loadrecords->loadpageroutine->loadpagesetstartpos(loadrecords->loadpage, recpos);
}

/* 设置加载的路径 */
bool ripple_loadtrailrecords_setloadsource(ripple_loadtrailrecords* loadrecords, char* source)
{
    if(false == loadrecords->loadpageroutine->loadpagesetfilesource(loadrecords->loadpage, source))
    {
        elog(RLOG_WARNING, "load trail record set load source error");
        return false;
    }
    return true;
}

/* 加载 records */
bool ripple_loadtrailrecords_load(ripple_loadtrailrecords* loadrecords)
{
    /* 
     * 1、加载文件块
     * 2、将文件块拆分为 records
     * 3、根据 records 的种类，处理分为 2 个逻辑，最有一个 record 的数据完整和不完整
     *      1、不完整时，分为两种: 读取到 RESET
     *          1.1 读取到 RESET, 放弃不完整的 record，将 reset record 向后续的流程传递
     *              证明重启了, 即使上个 record 不完整也不影响.
     *          1.2 暂存该 record, 等读取下一个页面时 拼装在一起
     *      2、完整的不做处理
     */
    bool shiftfile = false;                             /* 合并 continue record 时, 遇到了文件切换 */
    bool shiftfilecross = false;                        /* 查找 cross record 时, 遇到了文件切换 */
    bool crossreset = false;                            /* 在 cross 后面出现了 reset */
    uint64 reclen = 0;
    dlistnode* dlnode = NULL;
    dlistnode* dlnodenext = NULL;
    ripple_record* record = NULL;
    ripple_recpos recpos = { {0} };

    recpos.trail.type = RIPPLE_RECPOS_TYPE_TRAIL;

    /* 加载文件块 */
    loadrecords->loadrecords.error = RIPPLE_ERROR_SUCCESS;

    /* 设置读取的起点 */
    if(false == loadrecords->loadpageroutine->loadpage(loadrecords->loadpage, loadrecords->mp))
    {
        if(RIPPLE_ERROR_NOENT == loadrecords->loadpage->error)
        {
            /* 因文件不存在导致的问题, 不需要返回错误 */
            return true;
        }

        elog(RLOG_WARNING, "load page error, %d", loadrecords->loadpage->error);
        return false;
    }

    /* 
     * 1、验证文件块的正确性
     * 2、将文件块拆分为 records
     */
    if(false == ripple_trailpage_valid(loadrecords->mp))
    {
        elog(RLOG_WARNING, "load page valid page error");
        return false;
    }

    /* 将 page 拆分为 records */
    if(false == ripple_trailpage_page2records(loadrecords, loadrecords->mp))
    {
        elog(RLOG_WARNING, "page 2 records error");
        return false;
    }

    /* records 为空, 说明没有挖掘到任何内容 */
    if(true == dlist_isnull(loadrecords->records))
    {
        return true;
    }

    /* 
     * 根据场景开始处理
     * 1、查看是否含有不完整的 record, 含有不完整的 record, 那么在返回的 records 中的首个有效的 record 应为 continue record 或 RESET record
     *  1.1 当为 continue record 时, 那么合并两个 record
     *  1.2 若为 reset record, 那么清理掉不完整的 record
     * 
     * 2、查看 records 中是否含有CROSS record, 若含有 cross record, 那么查看后续的 record 中是否含有 reset
     *  2.1 含有 reset,那么清理掉 cross record
     *  2.2 不含有 reset, 那么将 cross record 暂存
     */
    if(NULL != loadrecords->recordcross.record)
    {
        /* 含有 records */
        for(dlnode = loadrecords->records->head; NULL != dlnode;)
        {
            record = (ripple_record*)dlnode->value;

            /* 会出现在文件的头部 */
            if(RIPPLE_RECORD_TYPE_TRAIL_HEADER == record->type
                || RIPPLE_RECORD_TYPE_TRAIL_DBMETA == record->type)
            {
                loadrecords->remainrecords = dlist_put(loadrecords->remainrecords, record);
                
                /* 将 dlnode 在链表中移除 */
                dlnode->value = NULL;
                dlnodenext = dlnode->next;
                loadrecords->records = dlist_delete(loadrecords->records, dlnode, NULL);
                dlnode = dlnodenext;
                continue;
            }
            else if(RIPPLE_RECORD_TYPE_TRAIL_TAIL == record->type)
            {
                shiftfile = true;
                loadrecords->remainrecords = dlist_put(loadrecords->remainrecords, record);
                
                /* 将 dlnode 在链表中移除 */
                dlnode->value = NULL;
                dlnodenext = dlnode->next;
                loadrecords->records = dlist_delete(loadrecords->records, dlnode, NULL);
                dlnode = dlnodenext;
                continue;
            }
            else if(RIPPLE_RECORD_TYPE_TRAIL_RESET == record->type)
            {
                shiftfile = true;
                /* 为 RESET, 清理掉 */
                ripple_record_free(loadrecords->recordcross.record);
                loadrecords->recordcross.record = NULL;
                loadrecords->recordcross.remainlen = 0;
                loadrecords->recordcross.totallen = 0;

                /* 链表合并 */
                loadrecords->records = dlist_concat(loadrecords->remainrecords, loadrecords->records);
                loadrecords->remainrecords = NULL;
                break;
            }

            if(RIPPLE_RECORD_TYPE_TRAIL_CONT != record->type)
            {
                /* 此时应该为 continue record, 若不为 continue record 说明逻辑出现了错误 */
                elog(RLOG_WARNING, "need continue record, but now type:%d, cross record:%lu.%lu, %lu.%u ,%lu",
                                    record->type,
                                    loadrecords->recordcross.record->start.trail.fileid,
                                    loadrecords->recordcross.record->start.trail.offset,
                                    loadrecords->recordcross.record->totallength,
                                    loadrecords->recordcross.record->reallength,
                                    loadrecords->recordcross.record->len);
                return false;
            }

            /* record 合并 */
            if(loadrecords->recordcross.record->len < loadrecords->recordcross.totallen)
            {
                /* 增加一个尾部长度 */
                reclen = loadrecords->recordcross.totallen + loadrecords->recordcross.rectaillen;
                loadrecords->recordcross.record->data = rrealloc0(loadrecords->recordcross.record->data, reclen);
                if(NULL == loadrecords->recordcross.record->data)
                {
                    elog(RLOG_WARNING, "realloc cross record error");
                    return false;
                }
                /* 指向 copy 数据的位置 */
                loadrecords->recordcross.record->dataoffset = loadrecords->recordcross.record->len;

                /* 重置数据的总长度 */
                loadrecords->recordcross.record->len = loadrecords->recordcross.totallen;
            }

            /* 将record tail 附加到数据的后面 */
            rmemcpy1(loadrecords->recordcross.record->data,
                    loadrecords->recordcross.record->dataoffset,
                    record->data + record->dataoffset,
                    record->reallength);
            loadrecords->recordcross.record->dataoffset += record->reallength;
            loadrecords->recordcross.remainlen -= record->reallength;

            /* 将 record 的删除并将 dlnode 节点在双向链表中移除 */
            loadrecords->recordcross.record->end.trail.fileid = record->end.trail.fileid;
            loadrecords->recordcross.record->end.trail.offset = record->end.trail.offset;
            ripple_record_free(record);

            /* 将 dlnode 在链表中移除 */
            dlnode->value = NULL;
            dlnodenext = dlnode->next;
            loadrecords->records = dlist_delete(loadrecords->records, dlnode, NULL);
            dlnode = dlnodenext;

            /* 合并完成后, 需要判断该 record 是否完整, 完整后将 cross record 挂载到正常的链表的头部 */
            if(0 != loadrecords->recordcross.remainlen)
            {
                continue;
            }

            /* 组装完成了, 将尾部数据附加到数据尾部 */
            rmemcpy1(loadrecords->recordcross.record->data,
                    loadrecords->recordcross.record->dataoffset,
                    loadrecords->recordcross.rectail,
                    loadrecords->recordcross.rectaillen);
            loadrecords->recordcross.record->len += loadrecords->recordcross.rectaillen;
            loadrecords->recordcross.rectaillen = 0;

            /* 
             * 连接两个链表, 并将 cross record 加入到链表中
             */
            loadrecords->records = dlist_concat(loadrecords->remainrecords, loadrecords->records);
            loadrecords->remainrecords = NULL;
            loadrecords->recordcross.record->type = RIPPLE_RECORD_TYPE_TRAIL_NORMAL;
            loadrecords->records = dlist_puthead(loadrecords->records, loadrecords->recordcross.record);
            loadrecords->recordcross.record = NULL;
            loadrecords->recordcross.rectaillen = 0;
            loadrecords->recordcross.remainlen = 0;
            loadrecords->recordcross.totallen = 0;
            break;
        }
    }

    /* 查看是否还有数据需要处理 */
    if(true == dlist_isnull(loadrecords->records))
    {
        goto ripple_loadtrailrecords_load_done;
    }

    /* 从后向前处理 */
    for(dlnode = loadrecords->records->tail; NULL != dlnode; dlnode = dlnode->prev)
    {
        record = (ripple_record*)dlnode->value;
        /* 会出现在文件的头部 */
        if(RIPPLE_RECORD_TYPE_TRAIL_HEADER == record->type || RIPPLE_RECORD_TYPE_TRAIL_DBMETA == record->type)
        {
            continue;
        }
        else if(RIPPLE_RECORD_TYPE_TRAIL_TAIL == record->type)
        {
            shiftfilecross = true;
            continue;
        }
        else if(RIPPLE_RECORD_TYPE_TRAIL_RESET == record->type)
        {
            shiftfilecross = true;
            crossreset = true;
            continue;
        }
        else if(RIPPLE_RECORD_TYPE_TRAIL_CROSS != record->type)
        {
            break;
        }

        /* 
         * 遇到了 cross
         *  1、含有 crossreset, 那么说明出现了重启,既然重启了那么 cross record 没有任何意义, 清理掉此 record
         *  2、不含有, 那么保存到 cross record 中
         */
        if(true == crossreset)
        {
            ripple_record_free(record);
            dlnode->value = NULL;
            loadrecords->records = dlist_delete(loadrecords->records, dlnode, NULL);
            break;
        }

        /* 将 record 加入到 cross record 上 */
        if(NULL != loadrecords->recordcross.record)
        {
            elog(RLOG_WARNING, "The previous processing contained incomplete records, prev fileid:%lu:%lu, current fileid:%lu:%lu",
                                loadrecords->recordcross.record->start.trail.fileid,
                                loadrecords->recordcross.record->start.trail.offset,
                                record->start.trail.fileid,
                                record->start.trail.offset);
            return false;
        }

        /* 放到 cross record 上,方便后续的合并 */
        loadrecords->recordcross.record = record;

        /* 计算长度 */
        /* 还需要的真实数据长度 */
        loadrecords->recordcross.remainlen = (record->totallength - (uint64)record->reallength);

        /* 数据总长度 */
        loadrecords->recordcross.totallen = record->totallength;
        loadrecords->recordcross.totallen += (uint64)record->dataoffset;

        /* 
         * copy record tail 数据
         *  1、计算到record tail 的开始位置
         *  2、将 tail 数据的长度裁剪掉
         *  3、复制数据
         */
        /* 计算 record tail 的开始位置 */
        record->dataoffset += record->reallength;
        loadrecords->recordcross.rectaillen = (uint16)(record->len - (uint64)record->dataoffset);

        /* 将 tail 数据的长度在原长度上裁剪掉 */
        record->len -= (uint64)(loadrecords->recordcross.rectaillen);
        rmemcpy1(loadrecords->recordcross.rectail, 0, (record->data + record->dataoffset), loadrecords->recordcross.rectaillen);

        /* 将 dlnode 在链表中移除,并将 dlnode 之后的节点放入到暂存节点中 */
        dlnode->value = NULL;
        dlnodenext = dlnode->next;
        loadrecords->records = dlist_truncate(loadrecords->records, dlnode);
        dlist_node_free(dlnode, NULL);

        /* 将后续的节点加入到remain节点中 */
        if(false == dlist_append(&loadrecords->remainrecords, dlnodenext))
        {
            elog(RLOG_WARNING, "dlist append error");
            return false;
        }
        break;
    }

ripple_loadtrailrecords_load_done:
    /* 上面流程处理完成后, 需要判断是否需要切换文件 */
    /* 在查找 continue record 时出现了切换文件的标识, 就不需要处理 cross 和 offset */
    if(true == shiftfile)
    {
        /* 需要切换文件 */
        loadrecords->fileid++;
        loadrecords->foffset = 0;
        recpos.trail.fileid = loadrecords->fileid;
        recpos.trail.offset = loadrecords->foffset;
        loadrecords->loadpageroutine->loadpagesetstartpos(loadrecords->loadpage, recpos);
        return true;
    }

    /* 处理 cross record 时出现了切换文件的标识 */
    if(true == shiftfilecross)
    {
        /* 需要切换文件 */
        loadrecords->fileid++;
        loadrecords->foffset = 0;
        recpos.trail.fileid = loadrecords->fileid;
        recpos.trail.offset = loadrecords->foffset;
        loadrecords->loadpageroutine->loadpagesetstartpos(loadrecords->loadpage, recpos);
        return true;
    }

    /* 判断是否满足切换文件的条件 */
    if(loadrecords->foffset == loadrecords->loadpage->filesize)
    {
        /* 切换文件 */
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

/* 关闭文件描述符 */
void ripple_loadtrailrecords_fileclose(ripple_loadtrailrecords* loadrecords)
{
    loadrecords->loadpageroutine->loadpageclose(loadrecords->loadpage);
}

/* 
 * 过滤 record,找到事务的开始
 *  返回值说明:
 *   true           还需要继续过滤
 *   false          不需要继续过滤
 */
bool ripple_loadtrailrecords_filterfortransbegin(ripple_loadtrailrecords* loadrecords)
{
    /*
     * 过滤说明:
     * trail 文件的格式
     * -------------------------------
     * |file header|database metadata |
     * |record     |                  |
     * | -----------------------------
     * 
     * 当重启时, 会在文件头部开始解析, 此时因为找到该文件中第一个事务的开始.
     *  1、元数据需要, 包含 database metadata 和 table metadata
     *  2、若该文件存在了 RESET 或 TAIL,那说明到了解析到了文件的尾部也没有需要的数据, 所以此时也不在需要在过滤了
     */
    uint16 subtype = RIPPLE_FF_DATA_TYPE_NOP;
    dlistnode* dlnode = NULL;
    dlistnode* dlnodenext = NULL;
    ripple_record* record = NULL;
    ripple_ffsmgr_state ffsmgrstate;

    /* 初始化验证接口 */
    ripple_ffsmgr_init(RIPPLE_FFSMG_IF_TYPE_TRAIL, &ffsmgrstate);
    ffsmgrstate.compatibility = loadrecords->compatibility;

    for(dlnode = dlnodenext = loadrecords->records->head; NULL != dlnode; dlnode = dlnodenext)
    {
        dlnodenext = dlnode->next;
        record = (ripple_record*)dlnode->value;
        /* 文件开始并且切页TRAIL_CONT直接删除 */
        if(RIPPLE_RECORD_TYPE_TRAIL_HEADER == record->type
            || RIPPLE_RECORD_TYPE_TRAIL_DBMETA == record->type)
        {
            continue;
        }
        else if(RIPPLE_RECORD_TYPE_TRAIL_RESET == record->type
            || RIPPLE_RECORD_TYPE_TRAIL_TAIL == record->type)
        {
            return false;
        }

        /* 保留data的meta data数据, 这里无需处理 */
        ffsmgrstate.ffsmgr->ffsmgr_getrecordsubtype(&ffsmgrstate, record->data, &subtype);
        if (RIPPLE_FF_DATA_TYPE_DBMETADATA == subtype
            || RIPPLE_FF_DATA_TYPE_TBMETADATA == subtype)
        {
            continue;
        }

        /* 是否为事务开始的record，是之后全需要 */
        if (ffsmgrstate.ffsmgr->ffsmgr_isrecordtransstart(&ffsmgrstate, record->data))
        {
            return false;
        }

        loadrecords->records = dlist_delete(loadrecords->records, dlnode, ripple_record_freevoid);
    }
    return true;
}

/*
 * 根据 fileid 和 offset 过滤,小于此值的不需要
*/
void ripple_loadtrailrecords_filter(ripple_loadtrailrecords* loadrecords, uint64 fileid, uint64 foffset)
{
    /*
     * 过滤说明:
     *  根据 fileid 和 foffset 
     */
    dlistnode* dlnode = NULL;
    dlistnode* dlnodenext = NULL;
    ripple_record* record = NULL;

    for(dlnode = dlnodenext = loadrecords->records->head; NULL != dlnode; dlnode = dlnodenext)
    {
        dlnodenext = dlnode->next;
        record = (ripple_record*)dlnode->value;

        if(fileid < record->start.trail.fileid)
        {
            return;
        }

        if(foffset <= record->start.trail.offset)
        {
            break;
        }

        /* 此 record 不需要 */
        loadrecords->records = dlist_delete(loadrecords->records, dlnode, ripple_record_freevoid);
    }
}

/*
 * 根据 fileid 和 offset 过滤，但是保留 metadata
 * 返回值说明:
 *   true           还需要继续过滤
 *   false          不需要继续过滤
*/
bool ripple_loadtrailrecords_filterremainmetadata(ripple_loadtrailrecords* loadrecords, uint64 fileid, uint64 foffset)
{
    /*
     * 过滤说明:
     *  根据 fileid 和 foffset 
     */
    uint16 subtype = RIPPLE_FF_DATA_TYPE_NOP;
    dlistnode* dlnode               = NULL;
    dlistnode* dlnodenext           = NULL;
    ripple_record* record           = NULL;
    ripple_ffsmgr_state ffsmgrstate;

    /* 初始化验证接口 */
    ripple_ffsmgr_init(RIPPLE_FFSMG_IF_TYPE_TRAIL, &ffsmgrstate);
    ffsmgrstate.compatibility = loadrecords->compatibility;

    for(dlnode = dlnodenext = loadrecords->records->head; NULL != dlnode; dlnode = dlnodenext)
    {
        dlnodenext = dlnode->next;
        record = (ripple_record*)dlnode->value;

        if(fileid < record->start.trail.fileid)
        {
            return false;
        }

        if (record->start.trail.offset >= foffset)
        {
            return false;
        }

        /* reset/tail证明切换了文件, 那么就证明不需要在过滤了 */
        if(RIPPLE_RECORD_TYPE_TRAIL_RESET == record->type
            || RIPPLE_RECORD_TYPE_TRAIL_TAIL == record->type)
        {
            return false;
        }

        /* 保留data的meta data数据, 这里无需处理 */
        ffsmgrstate.ffsmgr->ffsmgr_getrecordsubtype(&ffsmgrstate, record->data, &subtype);
        if (RIPPLE_FF_DATA_TYPE_DBMETADATA == subtype
            || RIPPLE_FF_DATA_TYPE_TBMETADATA == subtype)
        {
            continue;
        }

        loadrecords->records = dlist_delete(loadrecords->records, dlnode, ripple_record_freevoid);
    }

    return true;
}


/* 释放 */
void ripple_loadtrailrecords_free(ripple_loadtrailrecords* loadrecords)
{
    if(NULL == loadrecords)
    {
        return;
    }

    if(NULL != loadrecords->loadpageroutine)
    {
        loadrecords->loadpageroutine->loadpagefree(loadrecords->loadpage);
        loadrecords->loadpage = NULL;
        loadrecords->loadpageroutine = NULL;
    }

    if(NULL != loadrecords->mp)
    {
        if(NULL != loadrecords->mp->data)
        {
            rfree(loadrecords->mp->data);
        }
        rfree(loadrecords->mp);
    }

    ripple_record_free(loadrecords->recordcross.record);
    dlist_free(loadrecords->records, ripple_record_freevoid);
    dlist_free(loadrecords->remainrecords, ripple_record_freevoid);
    rfree(loadrecords);
}
