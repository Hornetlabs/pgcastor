#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/guc/guc.h"
#include "utils/string/stringinfo.h"
#include "utils/conn/ripple_conn.h"
#include "queue/ripple_queue.h"
#include "task/ripple_task_slot.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netclient.h"
#include "net/netmsg/ripple_netmsg.h"
#include "fastcompare/ripple_fastcompare_chunk.h"
#include "fastcompare/ripple_fastcompare_columndefine.h"
#include "fastcompare/ripple_fastcompare_tablecomparecatalog.h"
#include "fastcompare/ripple_fastcompare_tableslicetask.h"
#include "fastcompare/ripple_fastcompare_tableslice.h"
#include "fastcompare/ripple_fastcompare_tablesmanager.h"

typedef struct ShardMgr
{
    uint64 maxmem;         /* 最大内存 */
    uint64 multicnt;       /* 并行数 */
    uint64 shardnum;       /* 分片内最多条数 */
    uint64 shardcnt;       /* 分片个数 */
    PGconn *conn;       /* 数据库链接 */
    PGresult *res;      /* 数据库执行结果 */
} ShardMgr;

static void tableManagerSliceTableEasyMode(ripple_fastcompare_tablesmanger *mgr,
                                           riple_fastcompare_cmparetable *table,
                                           ShardMgr *shard)
{
    ripple_fastcompare_tablecomparecatalog *catalog = NULL;
    Oid relid = InvalidOid;
    ripple_fastcompare_tableslice *slice = NULL;

    RIPPLE_UNUSED(table);
    RIPPLE_UNUSED(shard);

    catalog = mgr->catalog;

    elog(RLOG_DEBUG, "table: [%s.%s] use easy mode", table->schema, table->table);

    /* 通过模式名表名获取oid */
    relid = ripple_fastcompare_tablecomparecatalog_getoidbytable(catalog->table2oid, table->table, table->schema);

    slice = ripple_fastcompare_tableslice_init();
    slice->no = 1;
    slice->oid = relid;
    slice->condition = rstrdup("ALL");
    RIPPLE_FASTCOMPARE_TABLESLICE_SLICE_SET_FIRST(slice->flag);
    RIPPLE_FASTCOMPARE_TABLESLICE_SLICE_SET_LAST(slice->flag);
    ripple_queue_put(mgr->queue, (void *)slice);
}

static void tableManagerSliceTableSamplingMode(ripple_fastcompare_tablesmanger *mgr,
                                               riple_fastcompare_cmparetable *table,
                                               ShardMgr *shard)
{
    ripple_fastcompare_tablecomparecatalog *catalog = NULL;
    List *pkey = NULL;
    ripple_fastcompare_columndefine *coldefine = NULL;
    Oid relid = InvalidOid;
    StringInfoData sql = {'\0'};
    StringInfoData pkey_name = {'\0'};
    StringInfoData condition = {'\0'};
    uint32 sample_num = 0;
    uint32 shard_threshold = 0;
    int index_shrard = 0;
    ripple_fastcompare_tableslice *slice = NULL;

    ListCell *cell = NULL;
    bool first = true;

    initStringInfo(&sql);
    initStringInfo(&pkey_name);
    initStringInfo(&condition);

    catalog = mgr->catalog;

    elog(RLOG_DEBUG, "table [%s.%s] use sampling mode, shard num: %d", table->schema, table->table, shard->shardcnt);

    /* 通过模式名表名获取oid */
    relid = ripple_fastcompare_tablecomparecatalog_getoidbytable(catalog->table2oid, table->table, table->schema);

    /* 获取主键列信息 */
    pkey = ripple_fastcompare_tablecomparecatalog_getpkcoldefinebyoid(catalog->pg_constraint, relid);

    /* 遍历主键列, 获取值拼接 */
    foreach(cell, pkey)
    {
        coldefine = (ripple_fastcompare_columndefine *) lfirst(cell);

        if (!first)
        {
            appendStringInfo(&pkey_name, ", \"%s\"", coldefine->colname);
            continue;
        }
        first = false;
        appendStringInfo(&pkey_name, "\"%s\"", coldefine->colname);
    }

    /* 采样语句拼接 */
    appendStringInfo(&sql, "SELECT %s from \"%s\".\"%s\" TABLESAMPLE BERNOULLI(1) ORDER BY %s ASC;",
                            pkey_name.data,
                            table->schema,
                            table->table,
                            pkey_name.data);

    elog(RLOG_DEBUG, "table [%s.%s] use sampling mode, sampling sql: [%s]", table->schema, table->table, sql.data);

    /* 执行 */
    shard->res = ripple_conn_exec(shard->conn, sql.data);
    if (!shard->res)
    {
        shard->conn = NULL;
        elog(RLOG_ERROR, "excute query failed: %s", sql.data);
    }

    /* 获取采样数 */
    sample_num = PQntuples(shard->res);

    if (sample_num == 0)
    {
        elog(RLOG_ERROR, "get sampling data cnt is 0");
    }

    elog(RLOG_DEBUG, "table [%s.%s] use sampling mode, cnt %u", table->schema, table->table, sample_num);

    /* 计算阈值 */
    shard_threshold = (sample_num + shard->shardcnt + 1) / shard->shardcnt;

    elog(RLOG_DEBUG, "table [%s.%s] use sampling mode, shard_threshold:%u, sample_num:%u, shard->shardcnt:%u", table->schema, table->table, shard_threshold, sample_num, shard->shardcnt);

    /* 构建slice */
    for (index_shrard = 0; index_shrard < shard->shardcnt; index_shrard++)
    {
        int temp_colnum = 0;
        bool first = true;

        /* 第一片 */
        if (index_shrard == 0)
        {
            appendStringInfo(&condition, "(%s) < ", pkey_name.data);
            foreach(cell, pkey)
            {
                coldefine = (ripple_fastcompare_columndefine *) lfirst(cell);
                if (!first)
                {
                    appendStringInfo(&condition, ", '%s'", PQgetvalue(shard->res, shard_threshold - 1, temp_colnum++));
                    continue;
                }
                first = false;
                appendStringInfo(&condition, "( '%s'", PQgetvalue(shard->res, shard_threshold - 1, temp_colnum++));
            }
            appendStringInfo(&condition, ")");
            slice = ripple_fastcompare_tableslice_init();
            slice->no = index_shrard + 1;
            slice->oid = relid;
            slice->condition = rstrdup(condition.data);
            RIPPLE_FASTCOMPARE_TABLESLICE_SLICE_SET_FIRST(slice->flag);
            elog(RLOG_DEBUG, "table [%s.%s] use sampling mode, first shard[%d] condirion [%s]", table->schema,
                                                                                                table->table,
                                                                                                index_shrard + 1,
                                                                                                condition.data);
            ripple_queue_put(mgr->queue, (void *)slice);

            slice = NULL;
            resetStringInfo(&condition);
        }
        /* 最后一片 */
        else if (index_shrard == shard->shardcnt - 1)
        {
            appendStringInfo(&condition, "(%s) >= ", pkey_name.data);
            foreach(cell, pkey)
            {
                coldefine = (ripple_fastcompare_columndefine *) lfirst(cell);
                if (!first)
                {
                    appendStringInfo(&condition, " , '%s'",
                                      PQgetvalue(shard->res, (shard_threshold * index_shrard) - 1, temp_colnum++));
                    continue;
                }
                first = false;
                appendStringInfo(&condition, "('%s' ",
                                  PQgetvalue(shard->res, (shard_threshold * index_shrard) - 1, temp_colnum++));
            }
            appendStringInfo(&condition, ")");
            slice = ripple_fastcompare_tableslice_init();
            slice->no = index_shrard + 1;
            slice->oid = relid;
            slice->condition = rstrdup(condition.data);
            RIPPLE_FASTCOMPARE_TABLESLICE_SLICE_SET_LAST(slice->flag);
            elog(RLOG_DEBUG, "table [%s.%s] use sampling mode, last shard [%d] condirion [%s]", table->schema,
                                                                                                table->table,
                                                                                                index_shrard + 1,
                                                                                                condition.data);
            ripple_queue_put(mgr->queue, (void *)slice);

            slice = NULL;
            resetStringInfo(&condition);
        }
        else
        {
            StringInfoData temp_max_value = {'\0'};
            StringInfoData temp_min_value = {'\0'};

            initStringInfo(&temp_max_value);
            initStringInfo(&temp_min_value);

            foreach(cell, pkey)
            {
                coldefine = (ripple_fastcompare_columndefine *) lfirst(cell);
                if (!first)
                {
                    appendStringInfo(&temp_min_value, ", '%s'", PQgetvalue(shard->res, (shard_threshold * index_shrard) - 1, temp_colnum));
                    appendStringInfo(&temp_max_value, ", '%s'", PQgetvalue(shard->res, (shard_threshold * (index_shrard + 1)) - 1, temp_colnum));
                    temp_colnum++;
                    continue;
                }
                first = false;
                appendStringInfo(&temp_min_value, "'%s'", PQgetvalue(shard->res, (shard_threshold * index_shrard) - 1, temp_colnum));
                appendStringInfo(&temp_max_value, "'%s'", PQgetvalue(shard->res, (shard_threshold * (index_shrard + 1)) - 1, temp_colnum));
                temp_colnum++;
            }

            appendStringInfo(&condition, "(%s) >= (%s) AND (%s) < (%s)", pkey_name.data,
                                                                         temp_min_value.data,
                                                                         pkey_name.data,
                                                                         temp_max_value.data);

            rfree(temp_min_value.data);
            rfree(temp_max_value.data);

            slice = ripple_fastcompare_tableslice_init();
            slice->no = index_shrard + 1;
            slice->oid = relid;
            slice->condition = rstrdup(condition.data);
            elog(RLOG_DEBUG, "table [%s.%s] use sampling mode, shard [%d] condirion [%s]", table->schema,
                                                                                           table->table,
                                                                                           index_shrard + 1,
                                                                                           condition.data);
            ripple_queue_put(mgr->queue, (void *)slice);

            slice = NULL;
            resetStringInfo(&condition);
        }
    }

    /* 清理 */
    PQclear(shard->res);
    rfree(sql.data);
    rfree(pkey_name.data);
    rfree(condition.data);
}

void ripple_fastcompare_tablesmanager_slice_table(ripple_fastcompare_tablesmanger *mgr)
{
    riple_fastcompare_cmparetables *tables = mgr->tables;
    riple_fastcompare_cmparetable *table = NULL;
    int index_tables = 0;
    ShardMgr shard = {'\0'};
    StringInfoData sql = {'\0'};

    initStringInfo(&sql);

    //todo liuzihe, guc参数添加
    shard.maxmem = (uint64)1024 * (uint64)1024 * (uint64)guc_getConfigOptionInt(RIPPLE_CFG_KEY_FASTCMP_MAXMEMARY);
    shard.multicnt = mgr->parallelcnt;

    /* 打开数据库链接 */
    shard.conn = ripple_conn_get(guc_getConfigOption("url"));

    /* 连接错误报错退出 */
    if(NULL == shard.conn)
    {
        elog(RLOG_ERROR, "can't connect to database!");
    }

    /* 计算每个分片包含的最大数量约值 */
    shard.shardnum = shard.maxmem / shard.multicnt / 256;

    for (index_tables = 0; index_tables < tables->cnt; index_tables++)
    {
        uint64  rowcnt = 0;
        table = &tables->table_name[index_tables];
        resetStringInfo(&sql);
        appendStringInfo(&sql, "SELECT COUNT(*) FROM \"%s\".\"%s\";", table->schema, table->table);

        shard.res = ripple_conn_exec(shard.conn, sql.data);
        if (NULL == shard.res)
        {
            shard.conn = NULL;
            elog(RLOG_ERROR, "excute query failed: %s", sql.data);
        }

        rowcnt = (uint64) strtoul(PQgetvalue(shard.res, 0, 0), NULL, 10);
        PQclear(shard.res);

        /* 计算分片数 */
        shard.shardcnt = (rowcnt + shard.shardnum - 1) / shard.shardnum;

        if (shard.shardcnt == 1)
        {
            /* 只有一片, 走简易流程 */
            tableManagerSliceTableEasyMode(mgr, table, &shard);
        }
        else
        {
            /* 采样模式 */
            tableManagerSliceTableSamplingMode(mgr, table, &shard);
        }
    }
    PQfinish(shard.conn);
    rfree(sql.data);
}

ripple_fastcompare_tablesmanger* ripple_fastcompare_tablesmanger_init(void)
{
    ripple_fastcompare_tablesmanger* tablemanager = (ripple_fastcompare_tablesmanger*)rmalloc0(sizeof(ripple_fastcompare_tablesmanger));
    if(NULL == tablemanager)
    {
        return NULL;
    }
    rmemset0(tablemanager, 0, '\0', sizeof(ripple_fastcompare_tablesmanger));
    tablemanager->catalog = NULL;
    tablemanager->tables = NULL;
    return tablemanager;
}

/*
 * 制作数据集
 */
bool ripple_fastcompare_tablesmanger_load_compare_tables(ripple_fastcompare_tablesmanger* mgr, List *tables)
{
    riple_fastcompare_cmparetables *cmptables = NULL;
    ListCell *cell = NULL;
    int index_table = 0;

    if (tables->length == 0)
    {
        return false;
    }

    cmptables = rmalloc0(sizeof(riple_fastcompare_cmparetables));
    if (!cmptables)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(cmptables, 0, 0, sizeof(riple_fastcompare_cmparetables));

    cmptables->cnt = tables->length;
    cmptables->table_name = rmalloc0(sizeof(riple_fastcompare_cmparetable) * cmptables->cnt);
    if (!cmptables->table_name)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(cmptables->table_name, 0, 0, sizeof(riple_fastcompare_cmparetable) * cmptables->cnt);

    foreach(cell, tables)
    {
        char *name = (char *)lfirst(cell);
        char *point = NULL;
        char tempname[64] = {'\0'};
        if (strlen(name) > 129)
        {
            elog(RLOG_ERROR, "invalid table name");
        }

        point = strstr(name, ".");
        if (!point)
        {
            elog(RLOG_ERROR, "invalid table name");
        }
        rmemcpy1(tempname, 0, name, point - name);

        cmptables->table_name[index_table].schema = rstrdup(tempname);
        rmemset1(tempname, 0, 0, 64);

        rmemcpy1(tempname, 0, point + 1, strlen(point));
        cmptables->table_name[index_table].table = rstrdup(tempname);
        rmemset1(tempname, 0, 0, 64);
        index_table++;
    }
    mgr->tables = cmptables;
    return true;
}
