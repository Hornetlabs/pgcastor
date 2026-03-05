#include "ripple_app_incl.h"
#include "utils/dlist/dlist.h"
#include "port/file/fd.h"
#include "queue/ripple_queue.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "bigtransaction/persist/ripple_bigtxn_persist.h"

ripple_bigtxn_persistnode* ripple_bigtxn_persist_node_init(void)
{
    ripple_bigtxn_persistnode* persistnode = NULL;

    persistnode = (ripple_bigtxn_persistnode*)rmalloc0(sizeof(ripple_bigtxn_persistnode));
    if (NULL == persistnode)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(persistnode, 0, '\0', sizeof(ripple_bigtxn_persistnode));
    persistnode->begin.trail.fileid = 0;
    persistnode->begin.trail.offset = 0;
    persistnode->end.trail.fileid = 0;
    persistnode->end.trail.offset = 0;
    persistnode->xid = InvalidFullTransactionId;
    persistnode->stat = RIPPLE_BIGTXN_PERSISTNODE_STAT_INIT;
    return persistnode;
}

ripple_bigtxn_persist* ripple_bigtxn_persist_init(void)
{
    ripple_bigtxn_persist* persist = NULL;

    persist = (ripple_bigtxn_persist*)rmalloc0(sizeof(ripple_bigtxn_persist));
    if (NULL == persist)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(persist, 0, '\0', sizeof(ripple_bigtxn_persist));

    persist->rewind.trail.fileid = 0;
    persist->rewind.trail.offset = 0;
    persist->count = 0;
    persist->dpersistnodes = NULL;
    return persist;
}


/* 筛选新的 rewind 节点 */
void ripple_bigtxn_persist_electionrewindbyxid(ripple_bigtxn_persist* persist, FullTransactionId xid)
{
    dlistnode* dlnode = NULL;
    ripple_bigtxn_persistnode* persistnode = NULL;
    if (NULL == persist)
    {
        return;
    }

    for(dlnode = persist->dpersistnodes->head; NULL != dlnode; )
    {
        persistnode = (ripple_bigtxn_persistnode*)dlnode->value;

        if(persistnode->xid != xid)
        {
            return;
        }

        persist->rewind.trail.fileid = persistnode->begin.trail.fileid;
        persist->rewind.trail.offset = persistnode->begin.trail.offset + 1;

        persist->dpersistnodes = dlist_delete(persist->dpersistnodes, dlnode, ripple_bigtxn_persistnode_free);
        persist->count -= 1;
        break;
    }


    for(dlnode = persist->dpersistnodes->head; NULL != dlnode; dlnode = persist->dpersistnodes->head)
    {
        persistnode = (ripple_bigtxn_persistnode*)dlnode->value;
        if(RIPPLE_BIGTXN_PERSISTNODE_STAT_DONE != persistnode->stat 
           && RIPPLE_BIGTXN_PERSISTNODE_STAT_ABANDON != persistnode->stat)
        {
            persist->rewind.trail.fileid = persistnode->begin.trail.fileid;
            persist->rewind.trail.offset = persistnode->begin.trail.offset;
            return;
        }

        persist->rewind.trail.fileid = persistnode->begin.trail.fileid;
        persist->rewind.trail.offset = persistnode->begin.trail.offset + 1;
        persist->dpersistnodes = dlist_delete(persist->dpersistnodes, dlnode, ripple_bigtxn_persistnode_free);
        persist->count -= 1;
    }
    return;
}


/* 筛选新的 rewind 节点 */
void ripple_bigtxn_persist_electionrewind(ripple_bigtxn_persist* persist, ripple_recpos* pos)
{
    dlistnode* dlnode = NULL;
    dlistnode* dlnodetmp = NULL;
    ripple_bigtxn_persistnode* persistnode = NULL;
    if (NULL == persist)
    {
        return;
    }
    if (NULL == persist->dpersistnodes || NULL == persist->dpersistnodes->head)
    {
        if (pos)
        {
            persist->rewind.trail.fileid = pos->trail.fileid;
            persist->rewind.trail.offset = pos->trail.offset;
        }
        return;
    }

    for(dlnode = persist->dpersistnodes->head; NULL != dlnode; dlnode = dlnodetmp)
    {
        dlnodetmp = dlnode->next;

        persistnode = (ripple_bigtxn_persistnode*)dlnode->value;

        if (RIPPLE_BIGTXN_PERSISTNODE_STAT_DONE != persistnode->stat)
        {
            persist->rewind.trail.fileid = persistnode->begin.trail.fileid;
            persist->rewind.trail.offset = persistnode->begin.trail.offset;
            break;
        }

        persist->rewind.trail.fileid = persistnode->begin.trail.fileid;
        persist->rewind.trail.offset = persistnode->begin.trail.offset;
    }

    return;
}

/* 清理xid对应的大事务 */
void ripple_bigtxn_persist_removebyxid(ripple_bigtxn_persist* persist, FullTransactionId xid)
{
    if (NULL == persist || NULL == persist->dpersistnodes)
    {
        return;
    }

    persist->dpersistnodes = dlist_deletebyvalue(persist->dpersistnodes,
                                                 (void*)&xid,
                                                 ripple_bigtxn_integratepersist_delectbyxidcmp,
                                                 ripple_bigtxn_persistnode_free);
    persist->count = persist->dpersistnodes->length;

    return;

}

int ripple_bigtxn_integratepersist_delectbyxidcmp(void* vala, void* valb)
{
    FullTransactionId* xida = NULL;
    ripple_bigtxn_persistnode* persistnode = NULL;;
    
    if (NULL == vala || NULL == valb)
    {
        return 1;
    }
    xida = (FullTransactionId*)vala;
    persistnode = (ripple_bigtxn_persistnode*)valb;

    if (*xida == persistnode->xid)
    {
        return 0;
    }

    return 1;

}

/* 清理未完成的事务 */
void ripple_bigtxn_integratepersist_cleannotdone(ripple_bigtxn_persist* persist)
{
    dlistnode* dlnode = NULL;
    dlistnode* dlnodetmp = NULL;
    ripple_bigtxn_persistnode* persistnode = NULL;

    if (NULL == persist || NULL == persist->dpersistnodes)
    {
        return;
    }

    for(dlnode = persist->dpersistnodes->head; NULL != dlnode; dlnode = dlnodetmp)
    {
        dlnodetmp = dlnode->next;

        persistnode = (ripple_bigtxn_persistnode*)dlnode->value;
        if (RIPPLE_BIGTXN_PERSISTNODE_STAT_DONE > persistnode->stat)
        {
            dlist_delete(persist->dpersistnodes, dlnode, NULL);
            rfree(persistnode);
            persist->count--;
            break;
        }
    }

    return;

}

/* 清理内存 */
void ripple_bigtxn_persist_free(ripple_bigtxn_persist* persist)
{
    if (NULL == persist)
    {
        return;
    }

    if (persist->dpersistnodes)
    {
        dlist_free(persist->dpersistnodes, ripple_bigtxn_persistnode_free);
    }

    rfree(persist);
    return;
}

/* 清理未完成的事务 */
void ripple_bigtxn_persistnode_free(void* persistnode)
{
    if (NULL == persistnode)
    {
        return;
    }

    rfree(persistnode);

    return;
}

/*
 * fileid + offset + cnt
 *    8   +   8    +  4
 */
#define BIGTRANSACTION_FILE_HEAD_LEN (sizeof(uint64) + sizeof(uint64) + sizeof(uint32))

/*
 * beginfileid + beginoffset + endfileid + endoffset + xid + stat
 *      8      +      8      +     8     +     8     +  8  +   4
 */
#define BIGTRANSACTION_FILE_NODE_LEN (sizeof(uint64) + sizeof(uint64) + sizeof(uint64) + sizeof(uint64) + sizeof(FullTransactionId) + sizeof(ripple_bigtxn_persistnode_stat))

bool ripple_bigtxn_write_persist(ripple_bigtxn_persist *persist)
{
    char path[RIPPLE_MAXPATH] = {'\0'};
    char temp_path[RIPPLE_MAXPATH] = {'\0'};
    int fd = -1;
    uint8 *buffer = NULL;
    uint8 *buffer_cursor = NULL;
    size_t buffer_sz = 0;
    dlistnode *dnode = NULL;
    ripple_bigtxn_persistnode *persistnode = NULL;

    /* 生成路径 */
    snprintf(path, RIPPLE_MAXPATH, "%s", RIPPLE_BIGTRANSACTION_FILE);
    snprintf(temp_path, RIPPLE_MAXPATH, "%s", RIPPLE_BIGTRANSACTION_FILE_TEMP);

    /* 删除临时文件 */
    unlink(temp_path);

    /* 打开临时文件 */
    fd = BasicOpenFile(temp_path, O_RDWR | O_CREAT| RIPPLE_BINARY);
    if (fd  < 0)
    {
        elog(RLOG_WARNING, "open file %s error %s", temp_path, strerror(errno));
        return false;
    }

    /* 计算 buffer 大小 */
    buffer_sz = BIGTRANSACTION_FILE_HEAD_LEN; 
    buffer_sz += persist->count * BIGTRANSACTION_FILE_NODE_LEN;

    buffer = rmalloc0(buffer_sz);
    if (!buffer)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(buffer, 0, 0, buffer_sz);

    /* 填充buffer */
    buffer_cursor = buffer;

    /* rewind fileid, offset */
    put64bit(&buffer_cursor, persist->rewind.trail.fileid);
    put64bit(&buffer_cursor, persist->rewind.trail.offset);

    /* count */
    put32bit(&buffer_cursor, persist->count);

    dnode = persist->dpersistnodes ? persist->dpersistnodes->head : NULL;

    /* 遍历persist, 放置相关信息 */
    for (; dnode; dnode = dnode->next)
    {
        persistnode = (ripple_bigtxn_persistnode *)dnode->value;

        /* persistnode begin fileid, offset */
        put64bit(&buffer_cursor, persistnode->begin.trail.fileid);
        put64bit(&buffer_cursor, persistnode->begin.trail.offset);

        /* persistnode end fileid, offset */
        put64bit(&buffer_cursor, persistnode->end.trail.fileid);
        put64bit(&buffer_cursor, persistnode->end.trail.offset);

        /* persistnode xid */
        put64bit(&buffer_cursor, persistnode->xid);

        /* persistnode stat */
        put32bit(&buffer_cursor, persistnode->stat);
    }

    FileWrite(fd, (char*)buffer, buffer_sz);

    FileSync(fd);

    FileClose(fd);

    rfree(buffer);

    /* 重命名文件 */
    if (durable_rename(temp_path, path, RLOG_DEBUG)) 
    {
        elog(RLOG_WARNING, "Error renaming file %s to %s", temp_path, path);
        return false;
    }

    return true;
}

ripple_bigtxn_persist *ripple_bigtxn_read_persist(void)
{
    ripple_bigtxn_persist *result = NULL;
    char path[RIPPLE_MAXPATH] = {'\0'};
    uint8_t head_s[BIGTRANSACTION_FILE_HEAD_LEN] = {'\0'};
    uint8_t node_buff_s[BIGTRANSACTION_FILE_NODE_LEN] = {'\0'};
    uint8_t *head = head_s;
    uint8_t *node_buff = node_buff_s;
    int fd = -1;
    int read_size = 0;
    int node_index = 0;
    size_t offset = 0;
    struct stat st;

    /* 生成路径 */
    snprintf(path, RIPPLE_MAXPATH, "%s", RIPPLE_BIGTRANSACTION_FILE);

    /* 分配返回值 */
    result = rmalloc0(sizeof(ripple_bigtxn_persist));
    if (!result)
    {
        elog(RLOG_WARNING, "open bigtxn persist malloc persist  error %s", strerror(errno));
        return NULL;
    }
    rmemset0(result, 0, 0, sizeof(ripple_bigtxn_persist));

    /* 检测文件是否存在, 第一次启动是文件不存在, 因此简单返回分配好的结构体 */
    if(0 != stat(path, &st))
    {
        return result;
    }

    /* 只读方式打开文件 */
    fd = BasicOpenFile(path, O_RDONLY | RIPPLE_BINARY);
    if (fd  < 0)
    {
        elog(RLOG_WARNING, "open bigtxn persist file %s error %s", path, strerror(errno));
        ripple_bigtxn_persistnode_free(result);
        return NULL;
    }

    /* 读取文件, 从文件开始获取persist的rewind信息和count */
    read_size = FilePRead(fd, (char *)head, BIGTRANSACTION_FILE_HEAD_LEN, 0);
    if (read_size <= 0)
    {
        elog(RLOG_WARNING, "try read file %s head, read 0, error %s", path, strerror(errno));
        FileClose(fd);
        ripple_bigtxn_persistnode_free(result);
        return NULL;
    }

    /* 获取rewind和count信息 */
    result->rewind.trail.fileid = get64bit(&head);
    result->rewind.trail.offset = get64bit(&head);
    result->count = get32bit(&head);

    offset = BIGTRANSACTION_FILE_HEAD_LEN;

    /* 根据count获取剩余的persistnode */
    for (node_index = 0; node_index < result->count; node_index++)
    {
        ripple_bigtxn_persistnode *persistnode = NULL;

        read_size = FilePRead(fd, (char *)node_buff, BIGTRANSACTION_FILE_NODE_LEN, offset);
        if (read_size <= 0)
        {
            elog(RLOG_WARNING, "try read file %s head, read 0, error %s", path, strerror(errno));
            ripple_bigtxn_persistnode_free(result);
            FileClose(fd);
            return NULL;
        }

        persistnode = rmalloc0(sizeof(ripple_bigtxn_persistnode));
        if (!persistnode)
        {
            elog(RLOG_WARNING, "open bigtxn persist malloc persistnode error %s", strerror(errno));
            ripple_bigtxn_persistnode_free(result);
            FileClose(fd);
            return NULL;
        }
        rmemset0(persistnode, 0, 0, sizeof(ripple_bigtxn_persistnode));

        /* 获取begin, end的fileid, offset */
        persistnode->begin.trail.fileid = get64bit(&node_buff);
        persistnode->begin.trail.offset = get64bit(&node_buff);
        persistnode->end.trail.fileid = get64bit(&node_buff);
        persistnode->end.trail.offset = get64bit(&node_buff);
        persistnode->xid = get64bit(&node_buff);
        persistnode->stat = get32bit(&node_buff);

        result->dpersistnodes = dlist_put(result->dpersistnodes, persistnode);

        rmemset1(node_buff_s, 0, 0, BIGTRANSACTION_FILE_NODE_LEN);
        node_buff = node_buff_s;
        offset += BIGTRANSACTION_FILE_NODE_LEN;
    }

    /* 处理完毕, 关闭文件 */
    FileClose(fd);
    return result;
}

void ripple_bigtxn_persist_set_state_by_xid(ripple_bigtxn_persist* persist, FullTransactionId xid, int state)
{
    dlistnode *dnode = NULL;
    ripple_bigtxn_persistnode *persistnode = NULL;

    dnode = persist->dpersistnodes ? persist->dpersistnodes->head : NULL;

    /* 遍历persist, 查找node */
    for (; dnode; dnode = dnode->next)
    {
        persistnode = (ripple_bigtxn_persistnode *)dnode->value;
        if (persistnode->xid == xid)
        {
            persistnode->stat = state;
            return;
        }
    }
    return;
}

void ripple_bigtxn_persistnode_set_begin(ripple_bigtxn_persistnode *node, ripple_recpos *pos)
{
    node->begin.trail.fileid = pos->trail.fileid;
    node->begin.trail.offset = pos->trail.offset;
}

void ripple_bigtxn_persistnode_set_end(ripple_bigtxn_persistnode *node, ripple_recpos *pos)
{
    node->end.trail.fileid = pos->trail.fileid;
    node->end.trail.offset = pos->trail.offset;
}

void ripple_bigtxn_persistnode_set_xid(ripple_bigtxn_persistnode *node, FullTransactionId xid)
{
    node->xid = xid;
}

void ripple_bigtxn_persistnode_set_stat_init(ripple_bigtxn_persistnode *node)
{
    node->stat = RIPPLE_BIGTXN_PERSISTNODE_STAT_INIT;
}