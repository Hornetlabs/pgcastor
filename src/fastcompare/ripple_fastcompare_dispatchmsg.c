#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/conn/ripple_conn.h"
#include "utils/string/stringinfo.h"
#include "utils/algorithm/crc/crc_check.h"
#include "queue/ripple_queue.h"
#include "task/ripple_task_slot.h"
#include "net/netmsg/ripple_netmsg.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netclient.h"
#include "fastcompare/ripple_fastcompare_chunk.h"
#include "fastcompare/ripple_fastcompare_columnvalue.h"
#include "fastcompare/ripple_fastcompare_row.h"
#include "fastcompare/ripple_fastcompare_columndefine.h"
#include "fastcompare/ripple_fastcompare_beginchunk.h"
#include "fastcompare/ripple_fastcompare_beginslice.h"
#include "fastcompare/ripple_fastcompare_dispatchmsg.h"
#include "fastcompare/ripple_fastcompare_simpledatachunk.h"
#include "fastcompare/ripple_fastcompare_datacmpresult.h"
#include "fastcompare/ripple_fastcompare_datacompare.h"
#include "fastcompare/ripple_fastcompare_datachunk.h"
#include "fastcompare/ripple_fastcompare_tablecomparecatalog.h"
#include "fastcompare/ripple_fastcompare_tableslicetask.h"
#include "fastcompare/ripple_fastcompare_tableslice.h"
#include "fastcompare/ripple_fastcompare_tablecorrectslice.h"
#include "fastcompare/ripple_fastcompare_tableslicecorrectmanager.h"
#include "fastcompare/ripple_fastcompare_datacmpresultitem.h"

static char* insert = "insert_prepared";
static char* updata = "updata_prepared";
static char* delete = "delete_prepared";

/* 创建 d2scorrect数据包挂载到 wpackets 上 */
static void ripple_fastcompare_dispatchmsg_d2scorrect_add(ripple_netclient* netclient)
{
    uint32 wmsglen = 0;
    uint8* wuptr = NULL;
    ripple_netpacket* netpacket = NULL;
    ripple_fastcompare_tableslicecorrectmanager* manager = NULL;

    manager = (ripple_fastcompare_tableslicecorrectmanager*)netclient;

    wuptr = ripple_fastcompare_datachunk_d2scorrectdata_build((void*)manager, &wmsglen);

    netpacket = ripple_netpacket_init();
    netpacket->max = RIPPLE_MAXALIGN(wmsglen);
    netpacket->offset = RIPPLE_NETMSG_TYPE_HDR_SIZE;
    netpacket->used = wmsglen;
    netpacket->data = ripple_netpacket_data_init(netpacket->max);
    rmemcpy0(netpacket->data, 0, wuptr, wmsglen);
    rfree(wuptr);

    elog(RLOG_DEBUG,"Send d2scorrectdata msglen:%u ", wmsglen);

    ripple_netclient_addwpacket(netclient, (void*)netpacket);
}

/* 创建 endchunk 数据包挂载到 wpackets 上 */
static void ripple_fastcompare_dispatchmsg_endchunk_add(ripple_netclient* netclient)
{
    uint32 crc = 0;
    uint8* wuptr = NULL;
    ripple_netpacket* netpacket = NULL;
    netpacket = ripple_netpacket_init();
    netpacket->max = RIPPLE_MAXALIGN(12);
    netpacket->offset = 8;
    netpacket->used = 12;
    netpacket->data = ripple_netpacket_data_init(netpacket->max);

    /* 发送请求，获取标识 */
    wuptr = netpacket->data;
    RIPPLE_CONCAT(put, 32bit)(&wuptr, RIPPLE_NETMSG_TYPE_FASTCOMPARE_ENDCHUNK);
    RIPPLE_CONCAT(put, 32bit)(&wuptr, 12);
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, netpacket->data, 8);
    FIN_CRC32C(crc);
    RIPPLE_CONCAT(put, 32bit)(&wuptr, crc);

    elog(RLOG_DEBUG,"Send endchunk msg  ");

    ripple_netclient_addwpacket(netclient, (void*)netpacket);
}

static void ripple_fastcompare_dispatchmsg_set_conkey(List* pkcol, uint16* prikey, ripple_fastcompare_row* row)
{
    uint32 colindex = 0;
    uint32 conkeyidx = 0;
    ListCell* pklc = NULL;
    ripple_fastcompare_columndefine* columndef = NULL;
    ripple_fastcompare_columnvalue* colvalue = NULL;

    foreach(pklc, pkcol)
    {
        columndef = (ripple_fastcompare_columndefine*)lfirst(pklc);
        for (colindex = 0; colindex < row->cnt; colindex++)
        {
            colvalue = &row->column[colindex];
            if (colvalue->colid == columndef->colid)
            {
                prikey[conkeyidx] = colindex;
                conkeyidx ++;
                break;
            }
        }
    }
}

/*
 * 遍历一个传入的row 和 prikey, 对其值和列编号进行crc计算
 */
static uint32 ripple_fastcompare_dispatchmsg_pkcol_crc(ripple_fastcompare_row* row, uint16* prikey, uint16 pkcnt)
{
    uint16 pkindex = 0;
    uint32 offset = 0;
    uint32  result = 0;

    if (!row)
    {
        elog(RLOG_ERROR, "row is NULL");
    }

    INIT_CRC32C(result);
    for (pkindex = 0; pkindex < pkcnt; pkindex++)
    {
        offset = prikey[pkindex];
        // elog(RLOG_DEBUG,"dispatchmsg_pkcol col->colid:%u, col->value:%s", row->column[offset].colid, row->column[offset].value);
        /* 先对列编号进行crc运算 */
        COMP_CRC32C(result, &row->column[offset].colid, sizeof(uint32));
        /* 再对列值进行crc运算 */
        COMP_CRC32C(result, row->column[offset].value, row->column[offset].len);
    }
    FIN_CRC32C(result);

    return result;
}

static uint32 ripple_fastcompare_dispatchmsg_make_insertprepared(PGconn* conn, List* colnum, char* schema, char* table)
{
    bool first = true;
    uint32 nParams = 0;
    uint32 colindex = 0;
    PGresult *res = NULL;
    ListCell* collc = NULL;
    StringInfo str = NULL;
    ripple_fastcompare_columndefine* columndef = NULL;

    str = makeStringInfo();

    appendStringInfo(str, "INSERT INTO %s.%s (", schema, table);

    foreach(collc, colnum)
    {
        columndef = (ripple_fastcompare_columndefine*)lfirst(collc);

        if (!first)
        {
            appendStringInfo(str, ", %s", columndef->colname);
            continue;
        }
        first = false;
        appendStringInfo(str, " %s", columndef->colname);
    }

    appendStringInfo(str, " ) VALUES (");

    first = true;
    for (colindex = 0; colindex < colnum->length; colindex++)
    {
        if (!first)
        {
            appendStringInfo(str, ", $%d", ++nParams);
            continue;
        }
        first = false;
        appendStringInfo(str, "$%d", ++nParams);
    }
    appendStringInfo(str, " );");

    res = PQprepare(conn, insert, str->data, nParams, NULL);

    if (PGRES_COMMAND_OK != PQresultStatus(res))
    {
        PQclear(res);
        elog(RLOG_ERROR,"Failed to prepare insert: %s", PQerrorMessage(conn));
    }
    PQclear(res);
    deleteStringInfo(str);
    return nParams;
}

static uint32 ripple_fastcompare_dispatchmsg_make_deleteprepared(PGconn* conn, List* pkcol, char* schema, char* table)
{
    bool first = true;
    uint32 nParams = 0;
    PGresult *res = NULL;
    ListCell* pklc = NULL;
    StringInfo str = NULL;
    ripple_fastcompare_columndefine* columndef = NULL;

    str = makeStringInfo();

    appendStringInfo(str, "DELETE FROM %s.%s WHERE ", schema, table);

    foreach(pklc, pkcol)
    {
        columndef = (ripple_fastcompare_columndefine*)lfirst(pklc);

        if (!first)
        {
            appendStringInfo(str, "AND  %s = $%d ", columndef->colname, ++nParams);
            continue;
        }
        first = false;
        appendStringInfo(str, "%s = $%d", columndef->colname, ++nParams);
    }

    appendStringInfo(str,";");

    res = PQprepare(conn, delete, str->data, nParams, NULL);

    if (PGRES_COMMAND_OK != PQresultStatus(res))
    {
        PQclear(res);
        elog(RLOG_ERROR,"Failed to prepare delete: %s", PQerrorMessage(conn));
    }
    PQclear(res);
    deleteStringInfo(str);
    return nParams;
}

static uint32 ripple_fastcompare_dispatchmsg_make_updataprepared(PGconn* conn, List* colnum, List* pkcol, char* schema, char* table)
{
    bool first = true;
    uint32 nParams = 0;
    PGresult *res = NULL;
    ListCell* pklc = NULL;
    ListCell* collc = NULL;
    StringInfo str = NULL;
    ripple_fastcompare_columndefine* columndef = NULL;

    str = makeStringInfo();

    appendStringInfo(str, "UPDATE %s.%s SET ", schema, table);

    foreach(collc, colnum)
    {
        columndef = (ripple_fastcompare_columndefine*)lfirst(collc);

        if (!first)
        {
            appendStringInfo(str, ", %s = $%d ", columndef->colname, ++nParams);
            continue;
        }
        first = false;
        appendStringInfo(str, "%s = $%d ", columndef->colname, ++nParams);
    }

    appendStringInfo(str," WHERE ");

    first = true;
    foreach(pklc, pkcol)
    {
        columndef = (ripple_fastcompare_columndefine*)lfirst(pklc);

        if (!first)
        {
            appendStringInfo(str, "AND  %s = $%d ", columndef->colname, ++nParams);
            continue;
        }
        first = false;
        appendStringInfo(str, "%s = $%d", columndef->colname, ++nParams);
    }

    appendStringInfo(str,";");

    res = PQprepare(conn, updata, str->data, nParams, NULL);

    if (PGRES_COMMAND_OK != PQresultStatus(res))
    {
        PQclear(res);
        elog(RLOG_ERROR,"Failed to prepare updata: %s", PQerrorMessage(conn));
    }
    PQclear(res);
    deleteStringInfo(str);
    return nParams;
}

static void ripple_fastcompare_dispatchmsg_exec_insertprepared(PGconn* conn, ripple_fastcompare_row* row)
{
    uint32 colindex = 0;
    PGresult *res = NULL;
    ripple_fastcompare_columnvalue *column = NULL;
    const char	*paramValues[1600];

    for ( colindex  = 0; colindex < row->cnt; colindex++)
    {
        column = &row->column[colindex];
        paramValues[colindex] = (char*)column->value;
    }

    res = PQexecPrepared(conn, insert, row->cnt, paramValues, NULL, NULL, 0);

    if (PGRES_COMMAND_OK != PQresultStatus(res))
    {
        PQclear(res);
        elog(RLOG_ERROR,"Failed to insert: %s", PQerrorMessage(conn));
    }

    PQclear(res);
}

static void ripple_fastcompare_dispatchmsg_exec_deleteprepared(PGconn* conn, List* pkcol)
{
    uint32 nParams = 0;
    ListCell* pklc = NULL;
    PGresult *res = NULL;
    ripple_fastcompare_columnvalue *column = NULL;
    const char	*paramValues[1600];

    foreach (pklc, pkcol)
    {
        column = (ripple_fastcompare_columnvalue*)lfirst(pklc);
        paramValues[nParams++] = (char*)column->value;
    }

    res = PQexecPrepared(conn, delete, nParams, paramValues, NULL, NULL, 0);

    if (PGRES_COMMAND_OK != PQresultStatus(res))
    {
        PQclear(res);
        elog(RLOG_ERROR,"Failed to delete: %s", PQerrorMessage(conn));
    }

    PQclear(res);
}

static void ripple_fastcompare_dispatchmsg_exec_updataprepared(PGconn* conn, ripple_fastcompare_row* row, uint16 *prikey, uint16 pkcnt)
{
    uint32 colindex = 0;
    uint32 nParams = 0;
    PGresult *res = NULL;
    ripple_fastcompare_columnvalue *column = NULL;
    const char	*paramValues[1600];

    for (colindex  = 0; colindex < row->cnt; colindex++)
    {
        column = &row->column[colindex];
        paramValues[nParams++] = (char*)column->value;
    }

    for (colindex = 0; colindex < pkcnt; colindex++)
    {
        column = &row->column[prikey[colindex]];
        paramValues[nParams++] = (char*)column->value;
    }

    res = PQexecPrepared(conn, updata, nParams, paramValues, NULL, NULL, 0);

    if (PGRES_COMMAND_OK != PQresultStatus(res))
    {
        PQclear(res);
        elog(RLOG_ERROR,"Failed to updata: %s", PQerrorMessage(conn));
    }

    PQclear(res);
}

/* 删除prepared语句 */
static void ripple_fastcompare_dispatchmsg_deallocate(PGconn* conn, char* name)
{
    PGresult *res = NULL;
    char    sql_exec[128] = {'\0'};

    rmemset1(sql_exec, 0, '\0',128);
    sprintf(sql_exec,"DEALLOCATE %s ", name);
    res = PQexec(conn, sql_exec);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) 
    {
        elog(RLOG_ERROR, "DEALLOCATE failed: %s", PQerrorMessage(conn));
    }
    PQclear(res);
}

/* 根据cmpresult 生成hash */
static HTAB *ripple_fastcompare_dispatchmsg_datacmpresult2hash(ripple_fastcompare_datacmpresult* cmpresult, PGconn* conn)
{
    HTAB *result = NULL;
    HASHCTL hctl = {'\0'};
    ListCell *cell = NULL;
    ripple_fastcompare_datacmpresultitem *resultitem = NULL;

    hctl.keysize = sizeof(uint32);
    hctl.entrysize = sizeof(ripple_fastcompare_compareresulthashentry);
    result = hash_create("compareresulthash",
                                      1024,
                                     &hctl,
                                      HASH_ELEM | HASH_BLOBS);

    if (!cmpresult)
    {
        elog(RLOG_ERROR, "oom");
    }

    /* 遍历链表, 写入hash */
    foreach(cell, cmpresult->checkresult)
    {
        ripple_fastcompare_compareresulthashentry *entry = NULL;
        uint32 pkey_crc = 0;
        bool find = false;

        resultitem = (ripple_fastcompare_datacmpresultitem *)lfirst(cell);
        pkey_crc = ripple_fastcompare_columnvalue_list_crc(resultitem->privalues);
        entry = hash_search(result, &pkey_crc, HASH_ENTER, &find);

        /* 40亿分之1的概率碰撞 */
        if (!find)
        {
            entry->privalues = NIL;
        }

        entry->crc = pkey_crc;
        entry->privalues = lappend(entry->privalues, resultitem);
    }
    /* 遍历链表, 写入hash */
    foreach(cell, cmpresult->corrresult)
    {
        resultitem = (ripple_fastcompare_datacmpresultitem *)lfirst(cell);
        ripple_fastcompare_dispatchmsg_exec_deleteprepared(conn, resultitem->privalues);
    }
    return result;
}


static ripple_fastcompare_datacmpresultitem * ripple_fastcompare_dispatchmsg_datacmpresulthash_findtitem(ripple_fastcompare_compareresulthashentry* entry,
                                                                                                         uint32 pkcrc,
                                                                                                         uint16* prikey,
                                                                                                         ripple_fastcompare_row* row)
{
    bool find = true;
    uint16 pkindex = 0;
    ListCell *cell = NULL;
    ListCell *cell_col = NULL;
    ripple_fastcompare_columnvalue* refcol = NULL;
    ripple_fastcompare_columnvalue* coorcol = NULL;
    ripple_fastcompare_datacmpresultitem *resultitem = NULL;

    /* 遍历链表, 写入hash */
    foreach(cell, entry->privalues)
    {
        pkindex = 0;
        find = true;
        resultitem = (ripple_fastcompare_datacmpresultitem *)lfirst(cell);
        foreach(cell_col, resultitem->privalues)
        {
            coorcol = (ripple_fastcompare_columnvalue *)lfirst(cell_col);
            refcol = &row->column[prikey[pkindex++]];
            if (coorcol->len != refcol->len)
            {
                find = false;
                continue;
            }
            if (memcmp(coorcol->value, refcol->value, refcol->len))
            {
                find = false;
            }
        }
        if (true == find)
        {
            entry->privalues = list_delete(entry->privalues, resultitem);
            return resultitem;
        }
    }
    return NULL;
}
/* beginslice消息处理 */
void ripple_fastcompare_dispatchmsg_beginslice(void* privdata, void* buffer)
{
    ripple_fastcompare_tableslicecorrectmanager* manager = NULL;

    manager = (ripple_fastcompare_tableslicecorrectmanager*)privdata;

    manager->correctslice->beginslice = ripple_fastcompare_beginslice_fetchdata((void*) manager, buffer);

    manager->conn = ripple_conn_get(manager->conninfo);
    if (NULL == manager->conn)
    {
        elog(RLOG_ERROR,"Connection to database failed");
        return;
    }
    return;
}

/* beginchunk消息处理生成dstchunk */
void ripple_fastcompare_dispatchmsg_beginchunk(void* privdata, void* buffer)
{
    bool first = true;
    uint32 tuple_num = 0;
    uint32 tuple_index = 0;
    Oid tableoid = InvalidXLogRecPtr;
    ListCell* pklc = NULL;
    ListCell* maxlc = NULL;
    ListCell* minlc = NULL;
    PGresult *res = NULL;
    List* collist = NULL;
    StringInfo sql = NULL;
    StringInfo pkey_name = NULL;
    ripple_fastcompare_simpledatachunk* chunk = NULL;
    ripple_fastcompare_beginchunk* beginchunk = NULL;
    ripple_fastcompare_beginslice* beginslice = NULL;
    ripple_fastcompare_columnvalue* columnvalue = NULL;
    ripple_fastcompare_tableslicecorrectmanager* manager = NULL;

    manager = (ripple_fastcompare_tableslicecorrectmanager*)privdata;

    beginslice = manager->correctslice->beginslice;

    beginchunk = ripple_fastcompare_beginchunk_fetchdata(privdata, buffer);
    manager->correctslice->beginchunk = beginchunk;
    sql = makeStringInfo();
    pkey_name = makeStringInfo();

    /* 获取表数据 */
    appendStringInfo(sql, "SELECT ");

    /* 拼接主键名称 */
    first = true;
    foreach(pklc, beginslice->columns)
    {
        ripple_fastcompare_columndefine *col = (ripple_fastcompare_columndefine *)lfirst(pklc);

        if (!first)
        {
            appendStringInfo(pkey_name, ", \"%s\"", col->colname);
            continue;
        }
        first = false;
        appendStringInfo(pkey_name, "\"%s\"", col->colname);
    }

    appendStringInfo(sql, "%s", pkey_name->data);

    /* md5 */
    appendStringInfo(sql, ", md5(");

    tableoid = ripple_fastcompare_tablecomparecatalog_getoidbytable(manager->catalog->table2oid, beginslice->table, beginslice->schema);

    if (InvalidOid == tableoid)
    {
        elog(RLOG_ERROR, "Not found %s.%s", beginslice->schema, beginslice->table);
    }

    collist = ripple_fastcompare_tablecomparecatalog_getcoldefinebyoid(manager->catalog->pg_attribute, tableoid);

    /* 拼接列 */
    first = true;
    foreach(pklc, collist)
    {
        ripple_fastcompare_columndefine *col = (ripple_fastcompare_columndefine *)lfirst(pklc);

        if (!first)
        {
            appendStringInfo(sql, "|| COALESCE(\"%s\"::text, '')", col->colname);
            continue;
        }
        first = false;
        appendStringInfo(sql, "COALESCE(\"%s\"::text, '')", col->colname);
    }
    appendStringInfo(sql, ")");

    appendStringInfo(sql, " FROM \"%s\".\"%s\" ", beginslice->schema, beginslice->table);

    if (RIPPLE_FASTCOMPARE_TABLESLICE_SLICE_IS_FIRST(manager->correctslice->sliceflag)
        && RIPPLE_FASTCOMPARE_TABLESLICE_SLICE_IS_LAST(manager->correctslice->sliceflag)
        && RIPPLE_FASTCOMPARE_SIMPLEDATACHUNK_CHUNK_IS_FIRST(manager->correctslice->chunkflag)
        && RIPPLE_FASTCOMPARE_SIMPLEDATACHUNK_CHUNK_IS_LAST(manager->correctslice->chunkflag))
    {
        /* 不添加where条件 */
    }
    else if (RIPPLE_FASTCOMPARE_TABLESLICE_SLICE_IS_FIRST(manager->correctslice->sliceflag)
            && RIPPLE_FASTCOMPARE_SIMPLEDATACHUNK_CHUNK_IS_FIRST(manager->correctslice->chunkflag))
    {
        appendStringInfo(sql, "WHERE");
        first = true;
        appendStringInfo(sql, " (%s) <= ", pkey_name->data);
        foreach(maxlc, beginchunk->maxprivalue)
        {
            columnvalue = (ripple_fastcompare_columnvalue*) lfirst(maxlc);
            if (!first)
            {
                appendStringInfo(sql, ", '%s'", columnvalue->value);
                continue;
            }
            first = false;
            appendStringInfo(sql, "( '%s'", columnvalue->value);
        }
        appendStringInfo(sql, ")");
    }
    else if (RIPPLE_FASTCOMPARE_TABLESLICE_SLICE_IS_LAST(manager->correctslice->sliceflag)
            && RIPPLE_FASTCOMPARE_SIMPLEDATACHUNK_CHUNK_IS_LAST(manager->correctslice->chunkflag))
    {
        appendStringInfo(sql, "WHERE");
        first = true;
        appendStringInfo(sql, " (%s) >= ", pkey_name->data);
        foreach(minlc, beginchunk->minprivalue)
        {
            columnvalue = (ripple_fastcompare_columnvalue*) lfirst(minlc);
            if (!first)
            {
                appendStringInfo(sql, ", '%s'", columnvalue->value);
                continue;
            }
            first = false;
            appendStringInfo(sql, "( '%s'", columnvalue->value);
        }
        appendStringInfo(sql, ")");
    }
    else
    {
        appendStringInfo(sql, "WHERE");
        first = true;
        appendStringInfo(sql, " (%s) >= ", pkey_name->data);
        foreach(minlc, beginchunk->minprivalue)
        {
            columnvalue = (ripple_fastcompare_columnvalue*) lfirst(minlc);
            if (!first)
            {
                appendStringInfo(sql, ", '%s'", columnvalue->value);
                continue;
            }
            first = false;
            appendStringInfo(sql, "( '%s'", columnvalue->value);
        }
        appendStringInfo(sql, ")");
        appendStringInfo(sql, "AND ");
        first = true;
        appendStringInfo(sql, "(%s) <= ", pkey_name->data);
        foreach(maxlc, beginchunk->maxprivalue)
        {
            columnvalue = (ripple_fastcompare_columnvalue*) lfirst(maxlc);
            if (!first)
            {
                appendStringInfo(sql, ", '%s'", columnvalue->value);
                continue;
            }
            first = false;
            appendStringInfo(sql, "( '%s'", columnvalue->value);
        }
        appendStringInfo(sql, ")");
    }

    appendStringInfo(sql, " ORDER BY %s", pkey_name->data);

    appendStringInfo(sql, " ASC");

    elog(RLOG_DEBUG,"dstchunk bulid sql:%s ", sql->data);

    /* 用获取数据 */
    res = ripple_conn_exec(manager->conn, sql->data);
    if (!res)
    {
        manager->conn = NULL;
        elog(RLOG_ERROR, "excute query failed: %s", sql->data);
    }
    tuple_num = PQntuples(res);

    /* 构建chunk */
    while (tuple_index < tuple_num)
    {
        //退出检测
        if (!chunk)
        {
            chunk = ripple_fastcompare_simpledatachunk_init();
        }

        ripple_fastcompare_simpledatachunk_append(chunk, res, beginslice->columns, tuple_index);
        tuple_index++;
    }
    PQclear(res);
    if (0 != tuple_num)
    {
        ripple_fastcompare_simpledatachunk_appendFin(chunk);
        elog(RLOG_DEBUG,"dstchunk cnt:%u", chunk->datacnt);
    }
    ripple_fastcompare_tablecorrectslice_set_dstchunk(manager->correctslice, chunk);
    deleteStringInfo(sql);
    deleteStringInfo(pkey_name);
    ripple_fastcompare_columnvalue_list_clean(beginchunk->minprivalue);
    ripple_fastcompare_columnvalue_list_clean(beginchunk->maxprivalue);
    ripple_fastcompare_beginchunk_clean(beginchunk);
    manager->correctslice->beginchunk = NULL;
    ripple_fastcompare_columndefine_list_clean(collist);
}

/* simpledatachunk消息处理 */
bool ripple_fastcompare_dispatchmsg_simpledatachunk(void* privdata, void* buffer)
{
    bool result = true;
    ripple_fastcompare_simpledatachunk* dstchunk = NULL;
    ripple_fastcompare_simpledatachunk* simpledatachunk = NULL;
    ripple_fastcompare_tableslicecorrectmanager* manager = NULL;

    manager = (ripple_fastcompare_tableslicecorrectmanager*)privdata;

    simpledatachunk = ripple_fastcompare_simpledatachunk_fetchdata(manager, buffer);
    dstchunk = (ripple_fastcompare_simpledatachunk*)manager->correctslice->dstchunk;

    ripple_fastcompare_datacompare_set_chunk(manager->datacompare, simpledatachunk, dstchunk);

    /* 比较数据 */
    if (false == ripple_fastcompare_compare_simple_chunk(manager->datacompare))
    {
        /* 生成比较结果，删除多余数据 */
        ripple_fastcompare_dispatchmsg_make_deleteprepared(manager->conn, 
                                                            manager->correctslice->beginslice->columns,
                                                            manager->correctslice->beginslice->schema,
                                                            manager->correctslice->beginslice->table);
        manager->correctslice->compresult = ripple_fastcompare_dispatchmsg_datacmpresult2hash(manager->datacompare->result, manager->conn);
        ripple_fastcompare_dispatchmsg_deallocate(manager->conn, delete);
        result = false;
    }

    return result;

}

/* 应用s2dcorrectdata到目标端 */
void ripple_fastcompare_dispatchmsg_s2dcorrectdata(void* privdata, void* buffer)
{
    bool find = false;
    bool conflict = false;
    uint32 pkcrc = 0;
    Oid tableoid = InvalidXLogRecPtr;
    uint16* prikey = NULL;
    ListCell* rowlc = NULL;
    List* collist = NULL;
    ripple_fastcompare_row* row = NULL;
    ripple_fastcompare_datachunk* chunk = NULL;
    ripple_fastcompare_beginslice* beginslice = NULL;
    ripple_fastcompare_tableslicecorrectmanager* manager = NULL;
    ripple_fastcompare_compareresulthashentry *entry = NULL;
    ripple_fastcompare_datacmpresultitem* resultitem = NULL;

    manager = (ripple_fastcompare_tableslicecorrectmanager*)privdata;

    beginslice = manager->correctslice->beginslice;

    chunk = ripple_fastcompare_datachunk_s2dcorrectdata_fetchdata(manager, buffer);

    tableoid = ripple_fastcompare_tablecomparecatalog_getoidbytable(manager->catalog->table2oid, beginslice->table, beginslice->schema);

    if (InvalidOid == tableoid)
    {
        elog(RLOG_ERROR, "Not found %s.%s", beginslice->schema, beginslice->table);
    }

    collist = ripple_fastcompare_tablecomparecatalog_getcoldefinebyoid(manager->catalog->pg_attribute, tableoid);

    ripple_fastcompare_dispatchmsg_make_insertprepared(manager->conn, collist, beginslice->schema, beginslice->table);

    ripple_fastcompare_dispatchmsg_make_updataprepared(manager->conn, collist, beginslice->columns, beginslice->schema, beginslice->table);

    /* 申请空间 */
    prikey = (uint16 *)rmalloc0(manager->correctslice->prikeycnt * sizeof(uint16));
    if(NULL == prikey)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(prikey, 0, '\0', manager->correctslice->prikeycnt * sizeof(uint16));

    /* 获取一条数据 */
    row = (ripple_fastcompare_row*)lfirst(list_head(chunk->rows));

    ripple_fastcompare_dispatchmsg_set_conkey(beginslice->columns, prikey, row);

    foreach(rowlc, chunk->rows)
    {
        row = (ripple_fastcompare_row*)lfirst(rowlc);

        pkcrc = ripple_fastcompare_dispatchmsg_pkcol_crc(row, prikey, manager->correctslice->prikeycnt);

        entry = hash_search(manager->correctslice->compresult, &pkcrc, HASH_FIND, &find);
        if(false == find)
        {
            elog(RLOG_ERROR, "No row data found in hash ");
        }

        /* 主键列比较 */
        if(1 < entry->privalues->length)
        {
            /* 主键列比较 */
            conflict = true;

            /* 通过主键列比较拿到 op */
            resultitem = ripple_fastcompare_dispatchmsg_datacmpresulthash_findtitem(entry, pkcrc, prikey, row);
        }
        else
        {
            resultitem = (ripple_fastcompare_datacmpresultitem*)lfirst(list_head(entry->privalues));
            list_free(entry->privalues);
            conflict = false;
        }

        if (RIPPLE_FASTCOMPARE_DATACMPRESULTITEM_OP_INSERT == resultitem->op)
        {
            // 组装insert
            ripple_fastcompare_dispatchmsg_exec_insertprepared(manager->conn, row);
        }
        else if (RIPPLE_FASTCOMPARE_DATACMPRESULTITEM_OP_UPDATE == resultitem->op)
        {
            // 组装updata
            ripple_fastcompare_dispatchmsg_exec_updataprepared(manager->conn, row, prikey, manager->correctslice->prikeycnt);
        }
        else
        {
            elog(RLOG_ERROR, "Wrong type of operation, %d", resultitem->op);
        }

        //执行语句
        if(false == conflict)
        {

            hash_search(manager->correctslice->compresult, &pkcrc, HASH_REMOVE, NULL);
        }
    }

    ripple_fastcompare_dispatchmsg_deallocate(manager->conn, insert);

    ripple_fastcompare_dispatchmsg_deallocate(manager->conn, updata);

    if (false == ripple_fastcompare_datacmpresult_hashisnull(manager->correctslice->compresult))
    {
        elog(RLOG_ERROR, "Processing failed: result hash is not null ");
    }
    hash_destroy(manager->correctslice->compresult);
    manager->correctslice->compresult = NULL;
    ripple_fastcompare_columndefine_list_clean(collist);
    ripple_fastcompare_datachunk_clean(chunk);
    rfree(prikey);

    elog(RLOG_DEBUG,"s2dcorrectdata updata complete");
}

/* 根据消息状态分发处理 */
bool ripple_fastcompare_dispatchmsg_netmsg(void* privdata, uint8* msg)
{
    uint32 msgtype = 0;
    uint32 msgcrc = 0;
    uint32 checkcrc = 0;
    uint8* uptr = NULL;
    ripple_fastcompare_tableslicecorrectmanager* manager = NULL;

    manager = (ripple_fastcompare_tableslicecorrectmanager*)privdata;

    if (NULL == manager)
    {
        return false;
    }

    uptr = msg;

    /* 读取协议头部信息 */
    msgtype = RIPPLE_CONCAT(get, 32bit)(&uptr);

    elog(RLOG_DEBUG,"msgtype:%u",msgtype);

    /* 根据状态分发处理 */
    if (RIPPLE_NETMSG_TYPE_FASTCOMPARE_BEGINSLICE == msgtype)
    {
        ripple_fastcompare_dispatchmsg_beginslice((void*) manager, msg);
    }
    else if (RIPPLE_NETMSG_TYPE_FASTCOMPARE_BEGINCHNUK == msgtype)
    {
        ripple_fastcompare_dispatchmsg_beginchunk((void*) manager, msg);
    }
    else if (RIPPLE_NETMSG_TYPE_FASTCOMPARE_SIMPLEDATACHUNK == msgtype)
    {
        if (false == ripple_fastcompare_dispatchmsg_simpledatachunk((void*) manager, msg))
        {
            ripple_fastcompare_dispatchmsg_d2scorrect_add((ripple_netclient*)manager);
            if (!manager->datacompare->result->checkresult)
            {
                ripple_fastcompare_dispatchmsg_endchunk_add((ripple_netclient*)manager);
            }
        }
        else
        {
            ripple_fastcompare_dispatchmsg_endchunk_add((ripple_netclient*)manager);
        }
    }
    else if (RIPPLE_NETMSG_TYPE_FASTCOMPARE_S2DCORRECTDATACHUNK == msgtype)
    {
        ripple_fastcompare_dispatchmsg_s2dcorrectdata((void*) manager, msg);

        ripple_fastcompare_dispatchmsg_endchunk_add((ripple_netclient*)manager);
    }
    else if (RIPPLE_NETMSG_TYPE_FASTCOMPARE_ENDSLICE == msgtype)
    {
        /* 计算crc */
        INIT_CRC32C(checkcrc);
        COMP_CRC32C(checkcrc, msg, 8);
        FIN_CRC32C(checkcrc);
        uptr += 4;
        msgcrc = RIPPLE_CONCAT(get, 32bit)(&uptr);
        if (checkcrc != msgcrc)
        {
            elog(RLOG_ERROR, "Endslice CRC check failed");
        }
        elog(RLOG_DEBUG,"Get Endslice Msg");
        manager->state = RIPPLE_FASTCOMPARE_TABLESLICECORRECTMANAGER_STATE_EXIT;
        PQfinish(manager->conn);
        manager->conn = NULL;
    }

    return true;

}
