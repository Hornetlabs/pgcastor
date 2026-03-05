#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/string/stringinfo.h"
#include "utils/hash/hash_search.h"
#include "queue/ripple_queue.h"
#include "task/ripple_task_slot.h"
#include "utils/conn/ripple_conn.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netclient.h"
#include "net/netmsg/ripple_netmsg.h"
#include "fastcompare/ripple_fastcompare_chunk.h"
#include "fastcompare/ripple_fastcompare_beginchunk.h"
#include "fastcompare/ripple_fastcompare_columnvalue.h"
#include "fastcompare/ripple_fastcompare_columndefine.h"
#include "fastcompare/ripple_fastcompare_tablecomparecatalog.h"
#include "fastcompare/ripple_fastcompare_tableslicetask.h"
#include "fastcompare/ripple_fastcompare_tableslice.h"
#include "fastcompare/ripple_fastcompare_beginslice.h"
#include "fastcompare/ripple_fastcompare_simpledatachunk.h"
#include "fastcompare/ripple_fastcompare_datachunk.h"
#include "fastcompare/ripple_fastcompare_row.h"
#include "fastcompare/ripple_fastcompare_endslice.h"

#define TABLE_SLICE_CURSOR_FETCH_NUM 1000000
#define TABLE_SLICE_CURSOR_NAME "slicecur"

typedef struct ReSearchTableCtx
{
    int   no;
    char *schema;
    char *table;
    List *pkey_col_list;
    List *col_list;
    ripple_fastcompare_chunk *chunk;
} ReSearchTableCtx;

ripple_fastcompare_tableslice *ripple_fastcompare_tableslice_init(void)
{
    ripple_fastcompare_tableslice *result = NULL;

    result = rmalloc0(sizeof(ripple_fastcompare_tableslice));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result , 0, 0, sizeof(ripple_fastcompare_tableslice));

    return result;
}

static void ripple_fastcompare_tableslice_SendChunk(ripple_fastcompare_tableslicetask *task,
                                                    ripple_fastcompare_simpledatachunk *chunk)
{
    ripple_fastcompare_beginchunk *begin = NULL;

    /* 初始化beginchunk */
    begin = ripple_fastcompare_beginchunk_init();
    begin->flag = chunk->flag;
    begin->minprivalue = chunk->minprivalues;
    begin->maxprivalue = chunk->maxprivalues;

    /* 发送beginchunk */
    ripple_fastcompare_beginchunk_send(&task->client, begin);
    ripple_fastcompare_beginchunk_clean(begin);

    /* 发送simpledatachunk */
    ripple_fastcompare_simpledatachunk_send(&task->client, chunk);
    ripple_fastcompare_simpledatachunk_clean(chunk);
}

/* 打印待纠错数据的信息 */
static bool ripple_fastcompare_tableslice_print_d2schunk(ripple_fastcompare_row *row,
                                                         ReSearchTableCtx *research)
{
    StringInfoData tablelog = {'\0'};
    char *op = NULL;
    bool result = true;
    ListCell *cell = NULL;
    int index_col = 0;

    initStringInfo(&tablelog);

    if (row->op == RIPPLE_FASTCOMPARE_ROW_OP_DELETE)
    {
        op = "DELETE";
        result = false;
    }
    else if (row->op == RIPPLE_FASTCOMPARE_ROW_OP_INSERT)
    {
        op = "INSERT";
    }
    else if (row->op == RIPPLE_FASTCOMPARE_ROW_OP_UPDATE)
    {
        op = "UPDATE";
    }
    else
    {
        elog(RLOG_ERROR, "unknown op type: %d", row->op);
    }

    appendStringInfo(&tablelog, "data compare result report: [%s]", op);

    foreach(cell, research->pkey_col_list)
    {
        ripple_fastcompare_columndefine *pkey = (ripple_fastcompare_columndefine *)lfirst(cell);

        if (index_col > 0)
        {
            appendStringInfo(&tablelog, ", %s = %s", pkey->colname, row->column[index_col++].value);
        }
        else
        {
            appendStringInfo(&tablelog, " %s = %s", pkey->colname, row->column[index_col++].value);
        }
    }
    elog(RLOG_INFO, "%s", tablelog.data);

    rfree(tablelog.data);
    return result;
}

/* 用task->dataconn链接重查数据库, 获取需要纠错的行 */
static void ripple_fastcompare_tableslice_research_table(ripple_fastcompare_tableslicetask *task,
                                                         ReSearchTableCtx *research)
{
    ripple_fastcompare_datachunk *d2schunk = NULL;
    ripple_fastcompare_datachunk *s2dchunk = NULL;
    ListCell *row_cell = NULL;
    ListCell *col_cell = NULL;
    StringInfoData sql = {'\0'};
    int index_col = 0;
    bool first = true;
    PGresult *res = NULL;
    int nfields = 0;

    initStringInfo(&sql);

    d2schunk = ripple_fastcompare_datachunk_deserial((uint8 *)research->chunk);

    foreach(row_cell, d2schunk->rows)
    {
        ripple_fastcompare_row *row = (ripple_fastcompare_row *)lfirst(row_cell);
        ripple_fastcompare_row *s2drow = NULL;

        /* 打印日志, 如果op = delete, 返回false, 这种情况下无需查询源数据 */
        if (!ripple_fastcompare_tableslice_print_d2schunk(row, research))
        {
            continue;
        }

        s2drow = ripple_fastcompare_row_init();

        if (!s2dchunk)
        {
            s2dchunk = ripple_fastcompare_datachunk_init();
        }

        resetStringInfo(&sql);
        appendStringInfo(&sql, "SELECT");

        first = true;
        foreach(col_cell, research->col_list)
        {
            ripple_fastcompare_columndefine *coldef = 
                    (ripple_fastcompare_columndefine *)lfirst(col_cell);

            if (!first)
            {
                appendStringInfo(&sql, ", %s", coldef->colname);
                continue;
            }
            first = false;
            appendStringInfo(&sql, " %s", coldef->colname);
        }

        appendStringInfo(&sql, " FROM \"%s\".\"%s\" WHERE", research->schema, research->table);
        first = true;
        index_col = 0;
        foreach(col_cell, research->pkey_col_list)
        {
            ripple_fastcompare_columndefine *coldef = 
                    (ripple_fastcompare_columndefine *)lfirst(col_cell);

            if (!first)
            {
                appendStringInfo(&sql, " AND %s = '%s'", coldef->colname, row->column[index_col++].value);
                continue;
            }
            first = false;
            appendStringInfo(&sql, " %s = '%s'", coldef->colname, row->column[index_col++].value);
        }
        appendStringInfo(&sql, ";");

        elog(RLOG_DEBUG, "table [%s.%s] task, shard [%d], execute search seql[%s]", research->schema,
                                                                               research->table,
                                                                               research->no,
                                                                               sql.data);

        /* sql语句已经拼接好, 执行, 获取数据 */
        res = ripple_conn_exec(task->dataconn, sql.data);
        if (!res)
        {
            task->dataconn = NULL;
            elog(RLOG_ERROR, "excute query failed: %s", sql.data);
        }

        nfields = PQnfields(res);
        ripple_fastcompare_row_column_init(s2drow, nfields);

        col_cell = list_head(research->col_list);
        for (index_col = 0; index_col < s2drow->cnt; index_col++)
        {
            ripple_fastcompare_columndefine *coldef = 
                    (ripple_fastcompare_columndefine *)lfirst(col_cell);

            s2drow->column[index_col].colid = coldef->colid;
            s2drow->column[index_col].value = rstrdup(PQgetvalue(res, 0, index_col));
            s2drow->column[index_col].len = strlen(s2drow->column[index_col].value);
            col_cell = lnext(col_cell);
        }
        PQclear(res);
        s2dchunk->rows = lappend(s2dchunk->rows, s2drow);
    }

    /* 只有s2dchunk不为空时才发送数据 */
    if (s2dchunk)
    {
        elog(RLOG_DEBUG, "table [%s.%s] task, shard [%d], send datachunk to dest", research->schema,
                                                                               research->table,
                                                                               research->no);
        ripple_fastcompare_datachunk_send(&task->client, s2dchunk);
        ripple_fastcompare_datachunk_clean(s2dchunk);
    }
    ripple_fastcompare_datachunk_clean(d2schunk);
    rfree(sql.data);
}

static void ripple_fastcompare_tableslice_WaitChunkEnd(ripple_fastcompare_tableslicetask *task,
                                                       ReSearchTableCtx *research)
{
    uint8 *get_chunk = NULL;
    uint32 type = 0;
    uint8 *uptr = NULL;

ripple_fastcompare_tableslice_waitchunkend_reget:
    while (!get_chunk)
    {
        /* 消息处理 */
        if(false == ripple_netclient_desc(&task->client))
        {
            task->client.status = RIPPLE_NETCLIENTCONN_STATUS_NOP;

            elog(RLOG_ERROR, "can't get data from net");
        }

        get_chunk = task->netresult;
        task->netresult = NULL;
        usleep(10000);
        //todo liuzihe, 退出判断
    }
    uptr = get_chunk;
    type = RIPPLE_CONCAT(get, 32bit)(&uptr);

    switch (type)
    {
        /* 接收到ENDCHUNK */
        case RIPPLE_NETMSG_TYPE_FASTCOMPARE_ENDCHUNK:
        {
            // todo liuzihe, 有时间加个检查
            elog(RLOG_DEBUG, "table [%s.%s] task, shard [%d], get chunk end", research->schema,
                                                                               research->table,
                                                                               research->no);
            rfree(get_chunk);
            get_chunk = NULL;
            return;
        }
        case RIPPLE_NETMSG_TYPE_FASTCOMPARE_D2SCORRECTDATACHUNK:
        {
            research->chunk = (ripple_fastcompare_chunk *)get_chunk;
            get_chunk = NULL;
            type = 0;
            elog(RLOG_DEBUG, "table [%s.%s] task, shard [%d], get d2scorrectdata", research->schema,
                                                                                   research->table,
                                                                                   research->no);
            ripple_fastcompare_tableslice_research_table(task, research);
            rfree(research->chunk);
            research->chunk = NULL;
            goto ripple_fastcompare_tableslice_waitchunkend_reget;
            break;
        }
        default:
        {
            elog(RLOG_ERROR, "invalid chunk type: %d", type);
        }
    }
}

void ripple_fastcompare_tableslice_Slice2Chunk(ripple_fastcompare_tableslicetask *task,
                                               ripple_fastcompare_tableslice *slice)
{
    ripple_fastcompare_tablecomparecatalog *catalog = NULL;
    ripple_fastcompare_beginslice *beginslice = NULL;
    ripple_fastcompare_simpledatachunk *chunk = NULL;
    StringInfoData sql = {'\0'};
    StringInfoData pkey_name = {'\0'};
    char schema[64] = {'\0'};
    char table[64] = {'\0'};
    List *pkey_col_list = NULL;
    List *col_list = NULL;
    ListCell *cell = NULL;
    bool first = true;
    bool chunk_first = false;
    bool chunk_last = false;
    PGresult *res = NULL;
    uint64 count = 0;

    catalog = task->catalog;

    /* 获取表名模式名 */
    ripple_fastcompare_tablecomparecatalog_gettablebyoid(catalog->pg_class, slice->oid, table, schema);

    /* 获取主键列 */
    pkey_col_list = ripple_fastcompare_tablecomparecatalog_getpkcoldefinebyoid(catalog->pg_constraint, slice->oid);

    /* 获取全部列 */
    col_list = ripple_fastcompare_tablecomparecatalog_getcoldefinebyoid(catalog->pg_attribute, slice->oid);

    /* 构建begin slice */
    beginslice = ripple_fastcompare_beginslice_init();
    beginslice->columns = pkey_col_list;
    beginslice->schema = rstrdup(schema);
    beginslice->table = rstrdup(table);
    beginslice->condition = rstrdup(slice->condition);
    beginslice->flag = slice->flag;
    beginslice->num = slice->no;

    /* begin slice 发送 */
    ripple_fastcompare_beginslice_send(&task->client, beginslice);
    ripple_fastcompare_beginslice_clean(beginslice);
    elog(RLOG_DEBUG, "table [%s.%s] task, shard [%d], begin slice send", schema, table, slice->no);
    beginslice = NULL;

    initStringInfo(&sql);
    initStringInfo(&pkey_name);

    /* 获取表数据 */
    appendStringInfo(&sql, "BEGIN; DECLARE %s CURSOR FOR SELECT", TABLE_SLICE_CURSOR_NAME);

    /* 拼接主键名称 */
    foreach(cell, pkey_col_list)
    {
        ripple_fastcompare_columndefine *col = (ripple_fastcompare_columndefine *)lfirst(cell);

        if (!first)
        {
            appendStringInfo(&pkey_name, ", %s", col->colname);
            continue;
        }
        first = false;
        appendStringInfo(&pkey_name, " %s", col->colname);
    }

    appendStringInfo(&sql, "%s", pkey_name.data);

    /* md5 */
    appendStringInfo(&sql, ", md5(");
    first = true;

    /* 拼接列 */
    foreach(cell, col_list)
    {
        ripple_fastcompare_columndefine *col = (ripple_fastcompare_columndefine *)lfirst(cell);

        if (!first)
        {
            appendStringInfo(&sql, " || COALESCE(\"%s\"::text, '')", col->colname);
            continue;
        }
        first = false;
        appendStringInfo(&sql, " COALESCE(\"%s\"::text, '')", col->colname);
    }
    appendStringInfo(&sql, ")");

    /* 拼接要查询的表 */
    appendStringInfo(&sql, " FROM \"%s\".\"%s\"", schema, table);

    /* 不是一片时才附加where条件 condition */
    if (!(RIPPLE_FASTCOMPARE_TABLESLICE_SLICE_IS_FIRST(slice->flag)
     && RIPPLE_FASTCOMPARE_TABLESLICE_SLICE_IS_LAST(slice->flag)))
    {
        appendStringInfo(&sql, " WHERE %s", slice->condition);
    }

    /* 指定排序规则 */
    appendStringInfo(&sql, " ORDER BY %s ASC;", pkey_name.data);

    elog(RLOG_DEBUG, "table [%s.%s] task, shard [%d], sql [%s]", schema, table, slice->no, sql.data);

    /* 执行 */
    res = ripple_conn_exec(task->chunkconn, sql.data);
    if (!res)
    {
        task->chunkconn = NULL;
        elog(RLOG_ERROR, "excute query failed: %s", sql.data);
    }
    /* 不需要关注返回结果, 只需要确保正确执行 */
    PQclear(res);

    /* 重置 stringinfo */
    resetStringInfo(&sql);

    /* fetch数据, 一次fetch TABLE_SLICE_CURSOR_FETCH_NUM 条数据 */
    appendStringInfo(&sql, "fetch %d %s;", TABLE_SLICE_CURSOR_FETCH_NUM, TABLE_SLICE_CURSOR_NAME);

    chunk_first = RIPPLE_FASTCOMPARE_TABLESLICE_SLICE_IS_FIRST(slice->flag);
    chunk_last = RIPPLE_FASTCOMPARE_TABLESLICE_SLICE_IS_LAST(slice->flag);

    elog(RLOG_DEBUG, "table [%s.%s] task, shard [%d], fetch sql [%s]", schema, table, slice->no, sql.data);

    while (true)
    {
        // todo liuzihe, 退出检测
        int tuple_num = 0;
        int tuple_index = 0;

        /* 用游标获取数据 */
        res = ripple_conn_exec(task->chunkconn, sql.data);
        if (!res)
        {
            task->chunkconn = NULL;
            elog(RLOG_ERROR, "excute query failed: %s", sql.data);
        }
        tuple_num = PQntuples(res);
        elog(RLOG_DEBUG, "table [%s.%s] task, shard [%d], fetch num [%d]", schema, table, slice->no, tuple_num);

        /* 构建chunk */
        while (tuple_index < tuple_num)
        {
            //todo liuzihe, 退出检测
            if (!chunk)
            {
                chunk = ripple_fastcompare_simpledatachunk_init();
                if (chunk_first)
                {
                    elog(RLOG_DEBUG, "table [%s.%s] task, shard [%d], chunk init", schema, table, slice->no);

                    RIPPLE_FASTCOMPARE_SIMPLEDATACHUNK_CHUNK_SET_FIRST(chunk->flag);
                    chunk_first = false;
                }
            }

            count++;
            ripple_fastcompare_simpledatachunk_append(chunk, res, pkey_col_list, tuple_index);
            tuple_index++;

            /* 达到最大大小, 发送 */
            if (chunk->size >= RIPPLE_FASTCOMPARE_SIMPLEDATACHUNK_MAX_SEND_SIZE)
            {
                ReSearchTableCtx research = {'\0'};

                ripple_fastcompare_simpledatachunk_appendFin(chunk);

                if ((tuple_num < TABLE_SLICE_CURSOR_FETCH_NUM) && chunk_last && (tuple_index == tuple_num))
                {
                    RIPPLE_FASTCOMPARE_SIMPLEDATACHUNK_CHUNK_SET_LAST(chunk->flag);
                }

                elog(RLOG_DEBUG, "table [%s.%s] task, shard [%d], chunk finish", schema, table, slice->no);

                ripple_fastcompare_tableslice_SendChunk(task, chunk);
                chunk = NULL;
                research.col_list = col_list;
                research.pkey_col_list = pkey_col_list;
                research.schema = schema;
                research.table = table;
                research.no = slice->no;
                ripple_fastcompare_tableslice_WaitChunkEnd(task, &research);
            }
        }

        /* 清除结果 */
        PQclear(res);
        /* 如果返回的数据量小于了 TABLE_SLICE_CURSOR_FETCH_NUM, 则数据已经读完了 */
        if (tuple_num < TABLE_SLICE_CURSOR_FETCH_NUM)
        {
            break;
        }
    }

    /* 检查是否有剩余的chunk */
    if (chunk)
    {
        ReSearchTableCtx research = {'\0'};
        ripple_fastcompare_simpledatachunk_appendFin(chunk);

        if (chunk_last)
        {
            RIPPLE_FASTCOMPARE_SIMPLEDATACHUNK_CHUNK_SET_LAST(chunk->flag);
        }

        ripple_fastcompare_tableslice_SendChunk(task, chunk);
        chunk = NULL;

        research.col_list = col_list;
        research.pkey_col_list = pkey_col_list;
        research.schema = schema;
        research.table = table;
        research.no = slice->no;

        ripple_fastcompare_tableslice_WaitChunkEnd(task, &research);
    }

    /* 重置 stringinfo */
    resetStringInfo(&sql);

    /* 为防止对数据库有影响 ROLLBACK */
    appendStringInfo(&sql, "ROLLBACK;");
    res = ripple_conn_exec(task->chunkconn, sql.data);
    if (!res)
    {
        task->chunkconn = NULL;
        elog(RLOG_ERROR, "excute query failed: %s", sql.data);
    }
    PQclear(res);

    /* 发送endslice */
    ripple_fastcompare_endslice_send(&task->client);

    elog(RLOG_DEBUG, "table slice check end, check cnt: %lu", count);

    /* 清理 */
    rfree(sql.data);
    rfree(pkey_name.data);
    ripple_fastcompare_columndefine_list_clean(pkey_col_list);
    ripple_fastcompare_columndefine_list_clean(col_list);
}