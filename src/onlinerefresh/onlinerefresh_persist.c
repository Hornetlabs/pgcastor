#include "app_incl.h"
#include "port/file/fd.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/uuid.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "refresh/refresh_table_syncstats.h"
#include "onlinerefresh/onlinerefresh_persist.h"

/*
 * fileid + offset + cnt
 *    8   +   8    +  8
 */
#define ONLINEREFRESHPERSIST_FILE_HEAD_LEN (sizeof(uint64) + sizeof(uint64) + sizeof(uint64))

/*
 * tabcnt + beginoffset + endfileid + endoffset + beginfileid + stat + incement + txid + uuid
 *    8   +      8      +     8     +     8     +      8      +  4   +    1     +   8  +  16
 */
#define ONLINEREFRESHPERSIST_FILE_NODE_LEN                                                \
    (sizeof(uint64) + sizeof(uint64) + sizeof(uint64) + sizeof(uint64) + sizeof(uint64) + \
     sizeof(int) + sizeof(bool) + sizeof(FullTransactionId) + sizeof(uuid_t))

/*
 * cnt + completecnt + tablestat + oid + schema + table + stat
 *  4  +      4      +     4     +  4  +   64   +  64   + cnt * 1
 */
#define ONLINEREFRESHPERSIST_FILE_TABLE_LEN \
    (sizeof(int) + sizeof(int) + sizeof(int) + sizeof(Oid) + NAMEDATALEN + NAMEDATALEN)

onlinerefresh_persistnode* onlinerefresh_persistnode_init(void)
{
    onlinerefresh_persistnode* persistnode = NULL;

    persistnode = (onlinerefresh_persistnode*)rmalloc0(sizeof(onlinerefresh_persistnode));
    if (NULL == persistnode)
    {
        elog(RLOG_WARNING, "onlinerefresh persistnode malloc out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(persistnode, 0, 0, sizeof(onlinerefresh_persistnode));

    persistnode->increment = false;
    persistnode->stat = ONLINEREFRESH_PERSISTNODE_STAT_NOP;
    persistnode->begin.trail.fileid = 0;
    persistnode->begin.trail.offset = 0;
    persistnode->end.trail.fileid = 0;
    persistnode->end.trail.offset = 0;
    rmemset1(persistnode->uuid.data, 0, 0, sizeof(uuid_t));
    persistnode->refreshtbs = NULL;

    return persistnode;
}

onlinerefresh_persist* onlinerefresh_persist_init(void)
{
    onlinerefresh_persist* persist = NULL;

    persist = (onlinerefresh_persist*)rmalloc0(sizeof(onlinerefresh_persist));
    if (NULL == persist)
    {
        elog(RLOG_WARNING, "onlinerefresh persist malloc out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(persist, 0, 0, sizeof(onlinerefresh_persist));

    persist->rewind.trail.fileid = 0;
    persist->rewind.trail.offset = 0;
    persist->dpersistnodes = NULL;
    return persist;
}

void onlinerefresh_persistnode_statset(onlinerefresh_persistnode* persistnode, int stat)
{
    persistnode->stat = stat;
}

void onlinerefresh_persistnode_txidset(onlinerefresh_persistnode* persistnode,
                                       FullTransactionId          txid)
{
    persistnode->txid = txid;
}

void onlinerefresh_persistnode_uuidset(onlinerefresh_persistnode* persistnode, uuid_t* uuid)
{
    rmemcpy1(persistnode->uuid.data, 0, uuid->data, sizeof(uuid_t));
}

void onlinerefresh_persistnode_beginset(onlinerefresh_persistnode* persistnode, recpos pos)
{
    persistnode->begin.trail.fileid = pos.trail.fileid;
    persistnode->begin.trail.offset = pos.trail.offset;
}

void onlinerefresh_persistnode_endset(onlinerefresh_persistnode* persistnode, recpos pos)
{
    persistnode->end.trail.fileid = pos.trail.fileid;
    persistnode->end.trail.offset = pos.trail.offset;
}

void onlinerefresh_persistnode_incrementset(onlinerefresh_persistnode* persistnode, bool incrment)
{
    persistnode->increment = incrment;
}

int onlinerefresh_persist_delectbyuuidcmp(void* vala, void* valb)
{
    uuid_t*                    uuid = NULL;
    onlinerefresh_persistnode* persistnode = NULL;

    if (NULL == vala || NULL == valb)
    {
        return 1;
    }
    uuid = (uuid_t*)vala;
    persistnode = (onlinerefresh_persistnode*)valb;

    if (0 == memcmp(uuid->data, persistnode->uuid.data, UUID_LEN))
    {
        return 0;
    }

    return 1;
}

void onlinerefresh_persist_statesetbyuuid(onlinerefresh_persist* persist, uuid_t* uuid, int state)
{
    dlistnode*                 dnode = NULL;
    onlinerefresh_persistnode* persistnode = NULL;

    dnode = persist->dpersistnodes ? persist->dpersistnodes->head : NULL;

    /* Traverse persist to find node */
    for (; dnode; dnode = dnode->next)
    {
        persistnode = (onlinerefresh_persistnode*)dnode->value;
        if (0 == memcmp(uuid->data, persistnode->uuid.data, UUID_LEN))
        {
            persistnode->stat = state;
            return;
        }
    }
    return;
}

void onlinerefresh_persist_removerefreshtbsbyuuid(onlinerefresh_persist* persist, uuid_t* uuid)
{
    dlistnode*                 dnode = NULL;
    onlinerefresh_persistnode* persistnode = NULL;

    dnode = persist->dpersistnodes ? persist->dpersistnodes->head : NULL;

    /* Traverse persist, find node */
    for (; dnode; dnode = dnode->next)
    {
        persistnode = (onlinerefresh_persistnode*)dnode->value;
        if (0 == memcmp(uuid->data, persistnode->uuid.data, UUID_LEN))
        {
            persistnode->refreshtbs = NULL;
            return;
        }
    }
    return;
}

bool onlinerefresh_persist_write(onlinerefresh_persist* persist)
{
    int                        fd = -1;
    uint64                     tbcnt = 0;
    uint64                     tbsize = 0;
    uint64                     offset = 0;
    uint64                     nodeoffset = 0;
    uint64                     bufferoffset = 0;
    uint8*                     buffer = NULL;
    uint8*                     buffer_tb = NULL;
    dlistnode*                 dnode = NULL;
    char                       head[ONLINEREFRESHPERSIST_FILE_HEAD_LEN] = {'\0'};
    char                       node[ONLINEREFRESHPERSIST_FILE_NODE_LEN] = {'\0'};
    char                       path[MAXPATH] = {'\0'};
    char                       temp_path[MAXPATH] = {'\0'};
    refresh_table_syncstat*    table = NULL;
    onlinerefresh_persistnode* persistnode = NULL;

    if (NULL == persist)
    {
        return true;
    }

    /* Generate path */
    snprintf(path, MAXPATH, "%s", ONLINEREFRESH_STATUS);
    snprintf(temp_path, MAXPATH, "%s.tmp", ONLINEREFRESH_STATUS);

    /* Delete temporary file */
    unlink(temp_path);

    /* Open temporary file */
    fd = osal_basic_open_file(temp_path, O_RDWR | O_CREAT | BINARY);
    if (fd < 0)
    {
        elog(RLOG_WARNING, "open file %s error %s", temp_path, strerror(errno));
        return false;
    }

    rmemset1(head, 0, '\0', ONLINEREFRESHPERSIST_FILE_HEAD_LEN);
    buffer = (uint8*)head;

    /* rewind fileid, offset */
    put64bit(&buffer, persist->rewind.trail.fileid);
    put64bit(&buffer, persist->rewind.trail.offset);
    /* count */
    if (true == dlist_isnull(persist->dpersistnodes))
    {
        put64bit(&buffer, 0);
    }
    else
    {
        put64bit(&buffer, persist->dpersistnodes->length);
    }

    osal_file_pwrite(fd, head, ONLINEREFRESHPERSIST_FILE_HEAD_LEN, offset);
    osal_file_sync(fd);
    offset += ONLINEREFRESHPERSIST_FILE_HEAD_LEN;

    dnode = persist->dpersistnodes ? persist->dpersistnodes->head : NULL;

    /* Traverse persist to write related info */
    for (; dnode; dnode = dnode->next)
    {
        tbcnt = 0;
        bufferoffset = 0;
        persistnode = (onlinerefresh_persistnode*)dnode->value;

        rmemset1(node, 0, '\0', ONLINEREFRESHPERSIST_FILE_NODE_LEN);

        bufferoffset += sizeof(tbcnt);
        rmemcpy1(node,
                 bufferoffset,
                 &persistnode->begin.trail.fileid,
                 sizeof(persistnode->begin.trail.fileid));
        bufferoffset += sizeof(persistnode->begin.trail.fileid);
        rmemcpy1(node,
                 bufferoffset,
                 &persistnode->begin.trail.offset,
                 sizeof(persistnode->begin.trail.offset));
        bufferoffset += sizeof(persistnode->begin.trail.offset);
        rmemcpy1(node,
                 bufferoffset,
                 &persistnode->end.trail.fileid,
                 sizeof(persistnode->end.trail.fileid));
        bufferoffset += sizeof(persistnode->end.trail.fileid);
        rmemcpy1(node,
                 bufferoffset,
                 &persistnode->end.trail.offset,
                 sizeof(persistnode->end.trail.offset));
        bufferoffset += sizeof(persistnode->end.trail.offset);
        rmemcpy1(node, bufferoffset, &persistnode->stat, sizeof(persistnode->stat));
        bufferoffset += sizeof(persistnode->stat);
        rmemcpy1(node, bufferoffset, &persistnode->increment, sizeof(persistnode->increment));
        bufferoffset += sizeof(persistnode->increment);
        rmemcpy1(node, bufferoffset, &persistnode->txid, sizeof(persistnode->txid));
        bufferoffset += sizeof(persistnode->txid);
        rmemcpy1(node, bufferoffset, persistnode->uuid.data, sizeof(uuid_t));
        bufferoffset += sizeof(uuid_t);
        nodeoffset = offset;
        offset += ONLINEREFRESHPERSIST_FILE_NODE_LEN;
        refresh_table_syncstats_lock(persistnode->refreshtbs);
        table = persistnode->refreshtbs ? persistnode->refreshtbs->tablesyncing : NULL;
        for (; NULL != table; table = table->next)
        {
            bufferoffset = 0;
            tbsize = (ONLINEREFRESHPERSIST_FILE_TABLE_LEN + table->cnt * 1);
            buffer_tb = rmalloc0(tbsize);
            if (!buffer_tb)
            {
                refresh_table_syncstats_unlock(persistnode->refreshtbs);
                elog(RLOG_ERROR, "oom");
            }
            rmemset0(buffer_tb, 0, 0, tbsize);
            rmemcpy0(buffer_tb, bufferoffset, &table->cnt, sizeof(table->cnt));
            bufferoffset += sizeof(table->cnt);
            rmemcpy0(buffer_tb, bufferoffset, &table->completecnt, sizeof(table->completecnt));
            bufferoffset += sizeof(table->completecnt);
            rmemcpy0(buffer_tb, bufferoffset, &table->tablestat, sizeof(table->tablestat));
            bufferoffset += sizeof(table->tablestat);
            rmemcpy0(buffer_tb, bufferoffset, &table->oid, sizeof(table->oid));
            bufferoffset += sizeof(table->oid);
            rmemcpy0(buffer_tb, bufferoffset, table->schema, strlen(table->schema));
            bufferoffset += NAMEDATALEN;
            rmemcpy0(buffer_tb, bufferoffset, table->table, strlen(table->table));
            bufferoffset += NAMEDATALEN;
            /* No shards or not yet added to task */
            if (0 != table->cnt && NULL != table->stat)
            {
                rmemcpy0(buffer_tb, bufferoffset, table->stat, (table->cnt * sizeof(int8_t)));
                bufferoffset += (table->cnt * sizeof(int8_t));
            }
            osal_file_pwrite(fd, (char*)buffer_tb, bufferoffset, offset);
            osal_file_sync(fd);
            offset += bufferoffset;
            rfree(buffer_tb);
            buffer_tb = NULL;
            tbcnt++;
        }
        refresh_table_syncstats_unlock(persistnode->refreshtbs);
        rmemcpy1(node, 0, &tbcnt, sizeof(tbcnt));
        osal_file_pwrite(fd, node, ONLINEREFRESHPERSIST_FILE_NODE_LEN, nodeoffset);
        osal_file_sync(fd);
    }
    osal_file_close(fd);

    /* Rename file */
    if (osal_durable_rename(temp_path, path, RLOG_DEBUG))
    {
        elog(RLOG_WARNING, "Error renaming file %s to %s", temp_path, path);
        return false;
    }

    return true;
}

onlinerefresh_persist* onlinerefresh_persist_read(void)
{
    struct stat            st;
    int                    fd = -1;
    int                    read_size = 0;
    int                    index_stat = 0;
    int                    node_index = 0;
    int                    table_index = 0;
    uint64                 tbcnt = 0;
    uint64                 offset = 0;
    uint64                 persistcnt = 0;
    uint64                 bufferoffset = 0;
    uint8*                 buffer = NULL;
    char                   path[MAXPATH] = {'\0'};
    char                   head[ONLINEREFRESHPERSIST_FILE_HEAD_LEN] = {'\0'};
    char                   node[ONLINEREFRESHPERSIST_FILE_NODE_LEN] = {'\0'};
    char                   synctable[ONLINEREFRESHPERSIST_FILE_TABLE_LEN] = {'\0'};
    onlinerefresh_persist* persist = NULL;

    /* Generate path */
    snprintf(path, MAXPATH, "%s", ONLINEREFRESH_STATUS);

    /* Allocate return value */
    persist = onlinerefresh_persist_init();
    if (NULL == persist)
    {
        elog(RLOG_WARNING, "open bigtxn persist malloc persist  error %s", strerror(errno));
        return NULL;
    }

    /* Check if file exists, on first startup file doesn't exist, so simply return allocated struct
     */
    if (0 != stat(path, &st))
    {
        return persist;
    }

    /* Open file in read-only mode */
    fd = osal_basic_open_file(path, O_RDONLY | BINARY);
    if (fd < 0)
    {
        elog(RLOG_WARNING, "open bigtxn persist file %s error %s", path, strerror(errno));
        onlinerefresh_persist_free(persist);
        return NULL;
    }

    /* Read file to get persist's rewind info and count from file start */
    read_size = osal_file_pread(fd, head, ONLINEREFRESHPERSIST_FILE_HEAD_LEN, 0);
    if (read_size <= 0)
    {
        elog(RLOG_WARNING, "try read file %s head, read 0, error %s", path, strerror(errno));
        osal_file_close(fd);
        onlinerefresh_persist_free(persist);
        return NULL;
    }

    buffer = (uint8*)head;

    /* Get rewind and count info */
    persist->rewind.trail.fileid = get64bit(&buffer);
    persist->rewind.trail.offset = get64bit(&buffer);
    persistcnt = get64bit(&buffer);

    offset = ONLINEREFRESHPERSIST_FILE_HEAD_LEN;

    /* Get remaining persistnodes based on count */
    for (node_index = 0; node_index < persistcnt; node_index++)
    {
        onlinerefresh_persistnode* persistnode = NULL;
        bufferoffset = 0;

        read_size = osal_file_pread(fd, node, ONLINEREFRESHPERSIST_FILE_NODE_LEN, offset);
        if (read_size <= 0)
        {
            elog(RLOG_WARNING, "try read file %s head, read 0, error %s", path, strerror(errno));
            onlinerefresh_persistnode_free(persist);
            osal_file_close(fd);
            return NULL;
        }
        buffer = (uint8*)head;

        persistnode = rmalloc0(sizeof(onlinerefresh_persistnode));
        if (!persistnode)
        {
            elog(RLOG_WARNING, "open bigtxn persist malloc persistnode error %s", strerror(errno));
            onlinerefresh_persist_free(persist);
            osal_file_close(fd);
            return NULL;
        }
        rmemset0(persistnode, 0, 0, sizeof(onlinerefresh_persistnode));

        persistnode->refreshtbs = refresh_table_syncstats_init();
        if (!persistnode->refreshtbs)
        {
            elog(RLOG_WARNING,
                 "open bigtxn persistnode refreshtbs malloc persistnode error %s",
                 strerror(errno));
            onlinerefresh_persistnode_free(persistnode);
            onlinerefresh_persist_free(persist);
            osal_file_close(fd);
            return NULL;
        }

        buffer = (uint8*)node;

        /* Get begin, end fileid and offset */
        rmemcpy1(&tbcnt, 0, buffer + bufferoffset, sizeof(tbcnt));
        bufferoffset += sizeof(tbcnt);
        rmemcpy1(&persistnode->begin.trail.fileid,
                 0,
                 buffer + bufferoffset,
                 sizeof(persistnode->begin.trail.fileid));
        bufferoffset += sizeof(persistnode->begin.trail.fileid);
        rmemcpy1(&persistnode->begin.trail.offset,
                 0,
                 buffer + bufferoffset,
                 sizeof(persistnode->begin.trail.offset));
        bufferoffset += sizeof(persistnode->begin.trail.offset);
        rmemcpy1(&persistnode->end.trail.fileid,
                 0,
                 buffer + bufferoffset,
                 sizeof(persistnode->end.trail.fileid));
        bufferoffset += sizeof(persistnode->end.trail.fileid);
        rmemcpy1(&persistnode->end.trail.offset,
                 0,
                 buffer + bufferoffset,
                 sizeof(persistnode->end.trail.offset));
        bufferoffset += sizeof(persistnode->end.trail.offset);
        rmemcpy1(&persistnode->stat, 0, buffer + bufferoffset, sizeof(persistnode->stat));
        bufferoffset += sizeof(persistnode->stat);
        rmemcpy1(&persistnode->increment, 0, buffer + bufferoffset, sizeof(persistnode->increment));
        bufferoffset += sizeof(persistnode->increment);
        rmemcpy1(&persistnode->txid, 0, buffer + bufferoffset, sizeof(persistnode->txid));
        bufferoffset += sizeof(persistnode->txid);
        rmemcpy1(persistnode->uuid.data, 0, buffer + bufferoffset, UUID_LEN);
        offset += ONLINEREFRESHPERSIST_FILE_NODE_LEN;

        for (table_index = 0; table_index < tbcnt; table_index++)
        {
            char                    table[NAMEDATALEN] = {'\0'};
            char                    schema[NAMEDATALEN] = {'\0'};
            refresh_table_syncstat* new_syncstat = refresh_table_syncstat_init();

            bufferoffset = 0;

            read_size = osal_file_pread(fd, synctable, ONLINEREFRESHPERSIST_FILE_TABLE_LEN, offset);
            if (read_size <= 0)
            {
                elog(
                    RLOG_WARNING, "try read file %s head, read 0, error %s", path, strerror(errno));
                onlinerefresh_persistnode_free(persistnode);
                onlinerefresh_persist_free(persist);
                osal_file_close(fd);
                return NULL;
            }

            buffer = (uint8*)synctable;

            rmemcpy1(&new_syncstat->cnt, 0, buffer + bufferoffset, sizeof(new_syncstat->cnt));
            bufferoffset += sizeof(new_syncstat->cnt);
            rmemcpy1(
                &new_syncstat->completecnt, 0, buffer + bufferoffset, new_syncstat->completecnt);
            bufferoffset += sizeof(new_syncstat->completecnt);
            rmemcpy1(&new_syncstat->tablestat,
                     0,
                     buffer + bufferoffset,
                     sizeof(new_syncstat->tablestat));
            bufferoffset += sizeof(new_syncstat->tablestat);
            rmemcpy1(&new_syncstat->oid, 0, buffer + bufferoffset, sizeof(new_syncstat->oid));
            bufferoffset += sizeof(new_syncstat->oid);

            rmemcpy1(schema, 0, buffer + bufferoffset, NAMEDATALEN);
            bufferoffset += NAMEDATALEN;
            rmemcpy1(table, 0, buffer + bufferoffset, NAMEDATALEN);

            offset += ONLINEREFRESHPERSIST_FILE_TABLE_LEN;

            /* Copy table info */
            refresh_table_syncstat_schema_set(schema, new_syncstat);
            refresh_table_syncstat_table_set(table, new_syncstat);

            if (0 < new_syncstat->cnt)
            {
                new_syncstat->stat = (int8_t*)rmalloc0(new_syncstat->cnt * sizeof(int8_t));
                if (NULL == new_syncstat->stat)
                {
                    elog(RLOG_WARNING,
                         "try read file %s head, read 0, error %s",
                         path,
                         strerror(errno));
                    onlinerefresh_persistnode_free(persist);
                    osal_file_close(fd);
                    return NULL;
                }
                rmemset0(new_syncstat->stat, 0, '\0', new_syncstat->cnt * sizeof(int8_t));

                read_size = osal_file_pread(
                    fd, (char*)new_syncstat->stat, (new_syncstat->cnt * sizeof(int8_t)), offset);
                if (read_size <= 0)
                {
                    elog(RLOG_WARNING,
                         "try read file %s head, read 0, error %s",
                         path,
                         strerror(errno));
                    refresh_table_syncstat_free(new_syncstat);
                    onlinerefresh_persistnode_free(persist);
                    osal_file_close(fd);
                    return NULL;
                }
                /* stats->stat */
                for (index_stat = 0; index_stat < new_syncstat->cnt; index_stat++)
                {
                    if (REFRESH_TABLE_SYNCS_SHARD_STAT_SYNCING == new_syncstat->stat[index_stat])
                    {
                        new_syncstat->stat[index_stat] = REFRESH_TABLE_SYNCS_SHARD_STAT_INIT;
                    }
                }

                offset += (new_syncstat->cnt * sizeof(int8_t));
            }

            new_syncstat->next = persistnode->refreshtbs->tablesyncing;
            if (persistnode->refreshtbs->tablesyncing)
            {
                persistnode->refreshtbs->tablesyncing->prev = new_syncstat;
            }

            persistnode->refreshtbs->tablesyncing = new_syncstat;
        }
        persist->dpersistnodes = dlist_put(persist->dpersistnodes, persistnode);
    }

    /* Done, close file */
    osal_file_close(fd);
    return persist;
}

void onlinerefresh_persist_removebyuuid(onlinerefresh_persist* persist, uuid_t* uuid)
{
    if (NULL == persist || NULL == persist->dpersistnodes)
    {
        return;
    }

    persist->dpersistnodes = dlist_deletebyvalue(
        persist->dpersistnodes, (void*)uuid, onlinerefresh_persist_delectbyuuidcmp, NULL);
    return;
}

void onlinerefresh_persist_electionrewindbyuuid(onlinerefresh_persist* persist, uuid_t* uuid)
{
    dlistnode*                 dlnode = NULL;
    onlinerefresh_persistnode* persistnode = NULL;
    if (NULL == persist)
    {
        return;
    }

    for (dlnode = persist->dpersistnodes->head; NULL != dlnode;)
    {
        persistnode = (onlinerefresh_persistnode*)dlnode->value;

        if (0 != memcmp(uuid->data, persistnode->uuid.data, UUID_LEN))
        {
            return;
        }

        persist->rewind.trail.fileid = persistnode->begin.trail.fileid;
        persist->rewind.trail.offset = persistnode->begin.trail.offset + 1;

        persist->dpersistnodes =
            dlist_delete(persist->dpersistnodes, dlnode, onlinerefresh_persistnode_free);
        break;
    }

    for (dlnode = persist->dpersistnodes->head; NULL != dlnode;
         dlnode = persist->dpersistnodes->head)
    {
        persistnode = (onlinerefresh_persistnode*)dlnode->value;
        if (ONLINEREFRESH_PERSISTNODE_STAT_DONE != persistnode->stat &&
            ONLINEREFRESH_PERSISTNODE_STAT_ABANDON != persistnode->stat)
        {
            persist->rewind.trail.fileid = persistnode->begin.trail.fileid;
            persist->rewind.trail.offset = persistnode->begin.trail.offset;
            return;
        }

        persist->rewind.trail.fileid = persistnode->begin.trail.fileid;
        persist->rewind.trail.offset = persistnode->begin.trail.offset + 1;
        persist->dpersistnodes =
            dlist_delete(persist->dpersistnodes, dlnode, onlinerefresh_persistnode_free);
    }
    return;
}

/* Free persistnode memory */
void onlinerefresh_persistnode_free(void* privdata)
{
    if (NULL == privdata)
    {
        return;
    }

    rfree(privdata);

    return;
}

/* Free memory */
void onlinerefresh_persist_free(onlinerefresh_persist* persist)
{
    if (NULL == persist)
    {
        return;
    }

    if (persist->dpersistnodes)
    {
        dlist_free(persist->dpersistnodes, onlinerefresh_persistnode_free);
    }

    rfree(persist);
    return;
}
