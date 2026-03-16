#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/dlist/dlist.h"
#include "utils/rbtree/rbtree.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/string/stringinfo.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "catalog/ripple_catalog.h"
#include "catalog/ripple_index.h"
#include "stmts/ripple_txnstmt.h"
#include "stmts/ripple_txnstmt_prepared.h"
#include "rebuild/ripple_rebuild_burst.h"
#include "rebuild/ripple_rebuild.h"
#include "rebuild/ripple_rebuild_preparestmt.h"
#include "works/parserwork/wal/ripple_decode_heap_util.h"

/* multiinsert拆分为多条insert */
static bool ripple_rebuild_prepared_multiinsert2insert(xk_pg_parser_translog_tbcol_nvalues* nvalues,
                                                       xk_pg_parser_translog_tbcol_value* column,
                                                       ripple_txnstmt_prepared* stmtprepared)
{
    xk_pg_parser_translog_tbcol_values* row = NULL;

    /* 申请insert data空间 */
    row = (xk_pg_parser_translog_tbcol_values*)rmalloc0(sizeof(xk_pg_parser_translog_tbcol_values));
    if(NULL == row)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return false;
    }
    rmemset0(row, 0, '\0', sizeof(xk_pg_parser_translog_tbcol_values));

    /* 复制m_base */
    row->m_base.m_dmltype = nvalues->m_base.m_dmltype;
    row->m_base.m_originid = nvalues->m_base.m_originid;
    row->m_base.m_tabletype = nvalues->m_base.m_tabletype;
    row->m_base.m_type = XK_PG_PARSER_TRANSLOG_DMLTYPE_INSERT;
    row->m_base.m_schemaname = rstrdup(nvalues->m_base.m_schemaname);
    row->m_base.m_tbname = rstrdup(nvalues->m_base.m_tbname);

    /* 复制values值 */
    row->m_haspkey = nvalues->m_haspkey;
    row->m_relfilenode = nvalues->m_relfilenode;
    row->m_relid = nvalues->m_relid;
    row->m_tupleCnt = nvalues->m_tupleCnt;
    row->m_valueCnt = nvalues->m_valueCnt;
    row->m_new_values = column;
    row->m_old_values = NULL;

    stmtprepared->row = row;
    return true;
}

static char* ripple_rebuild_makehatb(ripple_rebuild* rebuild,
                                     ripple_txnstmt_prepared* stmtprepared,
                                     Oid relid)
{
    bool found = false;
    tableoptype table_optype = {'\0'};
    ripple_rebuild_preparestmt tmp_stmt = {'\0'};
    rbtreenode* treenode = NULL;
    ripple_rebuild_preparestmt* pstmt = NULL;
    ripple_tableop2preparestmt *hash_entry = NULL;

    table_optype.optype = stmtprepared->optype;
    table_optype.relid = relid;

    stmtprepared->number = rebuild->prepareno;

    hash_entry = hash_search(rebuild->hatatb2prepare, &table_optype, HASH_ENTER, &found);
    /* 没有找到初始化rbtree */
    if(false == found)
    {
        hash_entry->tableop.optype = table_optype.optype;
        hash_entry->tableop.relid = table_optype.relid;
        hash_entry->rbtree = rbtree_init(ripple_rebuild_preparestmt_cmp, ripple_rebuild_preparestmt_free, ripple_rebuild_preparestmt_debug);
        if(NULL == hash_entry->rbtree)
        {
            elog(RLOG_WARNING, "rebuild makehatb rbtree out of memory");
            return NULL;
        }
    }

    /* 添加新的节点 */
    pstmt = ripple_rebuild_preparestmt_init();
    if(NULL == pstmt)
    {
        elog(RLOG_WARNING, "ripple rebuild preparestmt out of memory");
        return NULL;
    }
    pstmt->number = stmtprepared->number;
    snprintf(pstmt->stmtname, RIPPLE_NAMEDATALEN, "query_%lu", rebuild->prepareno);
    pstmt->preparesql = rstrdup(stmtprepared->preparedsql);
    rbtree_insert(hash_entry->rbtree, pstmt);
    (rebuild->prepareno)++;

    /* 获取stmtname并返回 */
    tmp_stmt.preparesql = stmtprepared->preparedsql;
    treenode = rbtree_get_value(hash_entry->rbtree, (void*)&tmp_stmt);

    if (NULL == treenode)
    {
        elog(RLOG_WARNING,"preparedsql not find from hash ");
        return NULL;
    }

    pstmt = (ripple_rebuild_preparestmt*)treenode->data;
    
    return pstmt->stmtname;
}

static char* ripple_rebuild_get_stmtname(ripple_rebuild* rebuild, 
                                         ripple_txnstmt_prepared* stmtprepared,
                                         Oid relid)
{
    bool found = false;
    tableoptype table_optype = {'\0'};
    ripple_rebuild_preparestmt tmp_stmt = {'\0'};
    rbtreenode* treenode = NULL;
    ripple_rebuild_preparestmt* pstmt = NULL;
    ripple_tableop2preparestmt *hash_entry = NULL;

    table_optype.optype = stmtprepared->optype;
    table_optype.relid = relid;

    hash_entry = hash_search(rebuild->hatatb2prepare, &table_optype, HASH_FIND, &found);
    if(found)
    {
        /* 查询rbtree,查到返回，查不到添加到该rbtree */
        tmp_stmt.preparesql = stmtprepared->preparedsql;
        treenode = rbtree_get_value(hash_entry->rbtree, (void*)&tmp_stmt);
        /* 返回值执行语句 */
        if (NULL != treenode)
        {
            pstmt = (ripple_rebuild_preparestmt*)treenode->data;
            stmtprepared->number = pstmt->number;
            return pstmt->stmtname;
        }
    }
    return NULL;
}

/* 表中是否含有主键或唯一约束 */
static bool ripple_rebuild_hasconskey(HTAB* hindex, Oid tboid)
{
    List* lindex                                = NULL;

    /* 获取index */
    lindex = (List*)ripple_index_getbyoid(tboid, hindex);

    /* 不含有唯一索引退出 */
    if (NULL == lindex || NULL == lindex->head)
    {
        return false;
    }
    list_free(lindex);
    return true;
}

/* 初始化syncstate->hatables2prepare哈希表 */
static HTAB* ripple_rebuild_hatables2prepare_init(void)
{
    HASHCTL        hash_ctl;
    HTAB* stmthtab;

    rmemset1(&hash_ctl, 0, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(tableoptype);
    hash_ctl.entrysize = sizeof(ripple_tableop2preparestmt);
    stmthtab = hash_create("rebuild_hatables2prepare", 2048, &hash_ctl, HASH_ELEM | HASH_BLOBS);
    return stmthtab;
}

/* 释放syncstate->hatables2prepare哈希表 */
static void ripple_rebuild_hatables2prepare_free(ripple_rebuild* rebuild)
{
    HASH_SEQ_STATUS status;
    ripple_tableop2preparestmt* entry;

    if (NULL == rebuild->hatatb2prepare)
    {
        return;
    }
    
    hash_seq_init(&status, rebuild->hatatb2prepare);
    while ((entry = hash_seq_search(&status)) != NULL)
    {
        if (entry->rbtree != NULL)
        {
            rbtree_free(entry->rbtree);
        }
    }
    hash_destroy(rebuild->hatatb2prepare);
    return;
}

/* 初始化 */
void ripple_rebuild_reset(ripple_rebuild* rebuild)
{
    if (NULL == rebuild)
    {
        return;
    }
    rebuild->prepareno = 1;
    if(NULL != rebuild->hatatb2prepare)
    {
        ripple_rebuild_hatables2prepare_free(rebuild);
    }

    if (NULL != rebuild->sysdicts)
    {
        ripple_cache_sysdicts_free(rebuild->sysdicts);
        rebuild->sysdicts = NULL;
    }

    rebuild->sysdicts = ripple_cache_sysdicts_integrate_init();

    rebuild->hatatb2prepare = ripple_rebuild_hatables2prepare_init();

    return ;
}

static ripple_txnstmt* ripple_rebuild_initpreparestmt(ripple_rebuild* rebuild,
                                                      Oid relid,
                                                      uint8 op,
                                                      uint32 colcnt,
                                                      char* preparedstmt,
                                                      uint32 preparedstmtlen)
{
    uint32 len = 0;
    char* stmtname = NULL;
    ripple_txnstmt* stmt = NULL;
    ripple_txnstmt_prepared* stmtprepared = NULL;

    /* 初始化 ripple_txnstmt */
    stmt = ripple_txnstmt_init();
    if(NULL == stmt)
    {
        return NULL;
    }
    stmt->stmt = NULL;
    stmt->type = RIPPLE_TXNSTMT_TYPE_PREPARED;

    /* 初始化 prepared 结构体 */
    stmtprepared = ripple_txnstmt_prepared_init();
    if(NULL == stmtprepared)
    {
        ripple_txnstmt_free(stmt);
        return NULL;
    }
    stmt->stmt = (void*)stmtprepared;
    stmtprepared->optype = op;

    /* 申请空间 */
    stmtprepared->values = rmalloc0(sizeof(char*) * colcnt);
    if(NULL == stmtprepared->values)
    {
        ripple_txnstmt_free(stmt);
        elog(RLOG_WARNING,"stmtprepared->values malloc error %s", strerror(errno));
        return NULL;
    }
    /* 复制 prepared */
    stmtprepared->preparedsql = rmalloc0(preparedstmtlen + 1);
    if(NULL == stmtprepared->preparedsql)
    {
        ripple_txnstmt_free(stmt);
        elog(RLOG_WARNING,"stmtprepared->preparedsql malloc error %s", strerror(errno));
        return NULL;
    }
    rmemset0(stmtprepared->preparedsql, 0, '\0', preparedstmtlen + 1);
    rmemcpy0(stmtprepared->preparedsql, 0, preparedstmt, preparedstmtlen);
    stmtprepared->preparedsql[preparedstmtlen] = '\0';

    stmtname = ripple_rebuild_get_stmtname(rebuild, stmtprepared, relid);

    if (NULL == stmtname)
    {
        stmtname = ripple_rebuild_makehatb(rebuild, stmtprepared, relid);
        if (NULL == stmtname)
        {
            ripple_txnstmt_free(stmt);
            return NULL;
        }
    }

    /* 复制 prepared */
    len = strlen(stmtname);
    stmtprepared->preparedname = rmalloc0(len + 1);
    if(NULL == stmtprepared->preparedname)
    {
        ripple_txnstmt_free(stmt);
        elog(RLOG_WARNING,"stmtprepared->preparedname malloc error %s", strerror(errno));
        return NULL;
    }
    rmemset0(stmtprepared->preparedname, 0, '\0', len + 1);
    rmemcpy0(stmtprepared->preparedname, 0, stmtname, len);
    stmtprepared->preparedname[len] = '\0';
    return stmt;
}

/* multiinsert */
static bool ripple_rebuild_prepared_multiinsert(ripple_rebuild* rebuild,
                                                ripple_txnstmt* stmt,
                                                List** lststmt)
{
    bool                                    has_valid_column = false;
    uint16_t                                index = 0;
    uint16_t                                index_rowcnt = 0;
    uint32                                  colcnts = 0;
    ripple_txnstmt*                         nstmt = NULL;
    ripple_txnstmt_prepared*                stmtprepared = NULL;
    xk_pg_parser_translog_tbcol_value*      column = NULL;
    xk_pg_parser_translog_tbcol_nvalues*    nvalues = NULL;
    StringInfo                              preparestmtname;

    /* MULTIINSERT处理 */
    has_valid_column = false;
    nvalues = (xk_pg_parser_translog_tbcol_nvalues *)stmt->stmt;

    preparestmtname = makeStringInfo();
    appendStringInfo(preparestmtname,
                        "insert into \"%s\".\"%s\" (",
                        nvalues->m_base.m_schemaname,
                        nvalues->m_base.m_tbname);

    /* 拼接列名 */
    column = nvalues->m_rows[0].m_new_values;
    for (index = 0; index < nvalues->m_valueCnt; index ++)
    {
        if (column[index].m_info == INFO_COL_MAY_NULL
            || column[index].m_info == INFO_COL_IS_DROPED) 
        {
            continue;
        }
        if (has_valid_column)
        {
            appendStringInfo(preparestmtname, ", ");
        }
        appendStringInfo(preparestmtname, "\"%s\"", column[index].m_colName);
        has_valid_column = true;
    }

    /* 检查是否至少存在一个有效列 */
    if (has_valid_column) 
    {
        appendStringInfo(preparestmtname, " ) values (");
    } 
    else 
    {
        /* 没有列, 在PG系列中是允许插入的 */
        appendStringInfo(preparestmtname, " ) values ();");
    }

    /* 拼接bind参数 */
    if(true == has_valid_column)
    {
        has_valid_column = false;
        for (index = 0; index < nvalues->m_valueCnt; index++)
        {
            if (column[index].m_info == INFO_COL_MAY_NULL
                || column[index].m_info == INFO_COL_IS_DROPED)
            {
                continue;
            }
            if (has_valid_column)
            {
                appendStringInfo(preparestmtname, ", ");
            }
            appendStringInfo(preparestmtname, "$%d", ++colcnts);
            has_valid_column = true;
        }
        appendStringInfo(preparestmtname, ");");
    }
    else
    {
        /* 不需要绑定值了，直接执行就可以了 */
        elog(RLOG_WARNING,
                "%s.%s no column, not currently supported",
                nvalues->m_base.m_schemaname,
                nvalues->m_base.m_tbname);
        deleteStringInfo(preparestmtname);
        return false;
    }

    /* 遍历每一行，并将每行的数据组装为 prepared 语句 */
    for (index_rowcnt = 0; index_rowcnt < nvalues->m_rowCnt; index_rowcnt++)
    {
        /* 初始化 ripple_txnstmt */
        nstmt = ripple_rebuild_initpreparestmt(rebuild,
                                                nvalues->m_relid,
                                                XK_PG_PARSER_TRANSLOG_DMLTYPE_INSERT,
                                                colcnts,
                                                preparestmtname->data,
                                                preparestmtname->len);
        if(NULL == nstmt)
        {
            deleteStringInfo(preparestmtname);
            return false;
        }
        nstmt->database = stmt->database;
        nstmt->end = stmt->end;
        nstmt->extra0 = stmt->extra0;
        nstmt->len = stmt->len;
        nstmt->start = stmt->start;
        nstmt->type = RIPPLE_TXNSTMT_TYPE_PREPARED;
        stmtprepared = (ripple_txnstmt_prepared*)nstmt->stmt;

        if(false == ripple_rebuild_prepared_multiinsert2insert(nvalues, 
                                                               nvalues->m_rows[index_rowcnt].m_new_values,
                                                               stmtprepared))
        {
            ripple_txnstmt_free(nstmt);
            deleteStringInfo(preparestmtname);
            return false;
        }

        /* 组装数据 */
        for (index = 0; index < nvalues->m_valueCnt; index++)
        {
            column = &(nvalues->m_rows[index_rowcnt].m_new_values[index]);
            if (column->m_info == INFO_NOTHING)
            {
                stmtprepared->values[stmtprepared->valuecnt++] = rstrdup((char*)column->m_value);
            }
            else if (column->m_info == INFO_COL_IS_NULL)
            {
                stmtprepared->values[stmtprepared->valuecnt++] = NULL;
            }
            else if (column->m_info == INFO_COL_MAY_NULL || column->m_info == INFO_COL_IS_DROPED)
            {
                continue;
            }
        }
        nvalues->m_rows[index_rowcnt].m_new_values = NULL;

        /* 加入到列表中 */
        *lststmt = lappend(*lststmt, nstmt);
    }

    deleteStringInfo(preparestmtname);
    return true;
}

/* insert */
static bool ripple_rebuild_prepared_insert(ripple_rebuild* rebuild,
                                           ripple_txnstmt* stmt,
                                           List** lststmt)
{
    int                                     index = 0;
    int                                     colcnt = 0;
    bool                                    has_valid_column = false;
    StringInfo                              preparedstmt = NULL;
    ripple_txnstmt*                         nstmt = NULL;
    ripple_txnstmt_prepared*                stmtprepared = NULL;
    xk_pg_parser_translog_tbcol_values*     row = NULL;
    xk_pg_parser_translog_tbcol_value*      colvalue = NULL;

    /* 入参转换 */
    row = (xk_pg_parser_translog_tbcol_values *)stmt->stmt;

    /* 申请空间 */
    preparedstmt = makeStringInfo();
    appendStringInfo(preparedstmt,
                    "insert into \"%s\".\"%s\" (",
                    row->m_base.m_schemaname,
                    row->m_base.m_tbname);

    /* 拼接列名 */
    for (index = 0; index < row->m_valueCnt; index++)
    {
        if (row->m_new_values[index].m_info == INFO_COL_MAY_NULL
            || row->m_new_values[index].m_info == INFO_COL_IS_DROPED)
        {
            continue;
        }

        if (has_valid_column)
        {
            appendStringInfo(preparedstmt, ", ");
        }

        appendStringInfo(preparedstmt, "\"%s\"", row->m_new_values[index].m_colName);
        has_valid_column = true;
    }

    /* 检查是否至少存在一个有效列 */
    if (has_valid_column)
    {
        appendStringInfo(preparedstmt, " ) values (");
    }
    else
    {
        elog(RLOG_WARNING,
                "%s.%s no column, not currently supported",
                row->m_base.m_schemaname,
                row->m_base.m_tbname);
        deleteStringInfo(preparedstmt);
        return false;
    }

    /* 拼接bind参数 */
    has_valid_column = false;
    for (index = 0; index < row->m_valueCnt; index++)
    {
        if (row->m_new_values[index].m_info == INFO_COL_MAY_NULL
            || row->m_new_values[index].m_info == INFO_COL_IS_DROPED)
        {
            continue;
        }

        if (has_valid_column)
        {
            appendStringInfo(preparedstmt, ", ");
        }
        appendStringInfo(preparedstmt, "$%d", ++colcnt);
        has_valid_column = true;
    }
    appendStringInfo(preparedstmt, " );");

    nstmt = ripple_rebuild_initpreparestmt(rebuild,
                                           row->m_relid,
                                           XK_PG_PARSER_TRANSLOG_DMLTYPE_INSERT,
                                           colcnt,
                                           preparedstmt->data,
                                           preparedstmt->len);
    if(NULL == nstmt)
    {
        deleteStringInfo(preparedstmt);
        return false;
    }
    nstmt->database = stmt->database;
    nstmt->end = stmt->end;
    nstmt->extra0 = stmt->extra0;
    nstmt->len = stmt->len;
    nstmt->start = stmt->start;
    nstmt->type = RIPPLE_TXNSTMT_TYPE_PREPARED;
    stmtprepared = (ripple_txnstmt_prepared*)nstmt->stmt;
    stmtprepared->row = row;
    stmt->stmt = NULL;

    /* 加入到列表中 */
    *lststmt = lappend(*lststmt, nstmt);

    /* 组装值  colvalue */
    for (index = 0; index < row->m_valueCnt; index++)
    {
        colvalue = row->m_new_values + index;
        if (INFO_NOTHING == colvalue->m_info)
        {
            stmtprepared->values[stmtprepared->valuecnt++] = rstrdup((char*)colvalue->m_value);
        }
        else if (INFO_COL_IS_NULL == colvalue->m_info)
        {
            stmtprepared->values[stmtprepared->valuecnt++] = NULL;
        }
        else if (INFO_COL_MAY_NULL == colvalue->m_info
                || INFO_COL_IS_DROPED == colvalue->m_info)
        {
            continue;
        }
    }

    deleteStringInfo(preparedstmt);
    return true;
}

/* 拼接bind参数函数 */
static int ripple_rebuild_appendbindparam(StringInfoData *stmt,
                                          xk_pg_parser_translog_tbcol_value *values,
                                          int count,
                                          int nParams,
                                          bool with_comma)
{
    bool is_first = true; 
    int index_colcnt = 0;
    for (index_colcnt = 0; index_colcnt < count; index_colcnt++)
    {
        if (values[index_colcnt].m_info == INFO_COL_MAY_NULL
            || values[index_colcnt].m_info == INFO_COL_IS_DROPED)
        {
            continue;
        }

        if (!is_first && with_comma)
        {
            appendStringInfo(stmt, ", ");
        }
        else if (!is_first && !with_comma)
        {
            appendStringInfo(stmt, " AND ");
        }

        appendStringInfo(stmt, "\"%s\" = $%d", values[index_colcnt].m_colName, ++nParams);
        is_first = false; 
    }

    return nParams;
}

/* delete */
static bool ripple_rebuild_prepared_delete(ripple_rebuild* rebuild,
                                           ripple_txnstmt* stmt,
                                           List** lststmt)
{
    int                                     index = 0;
    int                                     colcnt = 0;
    StringInfo                              preparedstmt = NULL;
    ripple_txnstmt*                         nstmt = NULL;
    ripple_txnstmt_prepared*                stmtprepared = NULL;
    xk_pg_parser_translog_tbcol_values*     row = NULL;
    xk_pg_parser_translog_tbcol_value*      colvalue = NULL;

    /* 入参转换 */
    row = (xk_pg_parser_translog_tbcol_values *)stmt->stmt;

    /* 申请空间 */
    preparedstmt = makeStringInfo();
    appendStringInfo(preparedstmt,
                        "DELETE FROM \"%s\".\"%s\" WHERE ",
                        row->m_base.m_schemaname,
                        row->m_base.m_tbname);

    /* 没有主键或唯一约束 */
    if(false == row->m_haspkey
       && false ==  ripple_rebuild_hasconskey(rebuild->sysdicts->by_index, row->m_relid))
    {
        appendStringInfo(preparedstmt,
                            "CTID = (SELECT CTID FROM \"%s\".\"%s\" WHERE ",
                            row->m_base.m_schemaname,
                            row->m_base.m_tbname);
    }

    colcnt = ripple_rebuild_appendbindparam(preparedstmt,
                                            row->m_old_values,
                                            row->m_valueCnt,
                                            0,
                                            false);

    if (false == row->m_haspkey
        && false ==  ripple_rebuild_hasconskey(rebuild->sysdicts->by_index, row->m_relid))
    {
        appendStringInfo(preparedstmt, " LIMIT 1)");
    }

    nstmt = ripple_rebuild_initpreparestmt(rebuild,
                                           row->m_relid,
                                           XK_PG_PARSER_TRANSLOG_DMLTYPE_DELETE,
                                           colcnt,
                                           preparedstmt->data,
                                           preparedstmt->len);
    if(NULL == nstmt)
    {
        deleteStringInfo(preparedstmt);
        return false;
    }
    nstmt->database = stmt->database;
    nstmt->end = stmt->end;
    nstmt->extra0 = stmt->extra0;
    nstmt->len = stmt->len;
    nstmt->start = stmt->start;
    nstmt->type = RIPPLE_TXNSTMT_TYPE_PREPARED;
    stmtprepared = (ripple_txnstmt_prepared*)nstmt->stmt;
    stmtprepared->row = row;
    stmt->stmt = NULL;

    /* 加入到列表中 */
    *lststmt = lappend(*lststmt, nstmt);

    /* 组装值  colvalue */
    for (index = 0; index < row->m_valueCnt; index++)
    {
        colvalue = row->m_old_values + index;
        if (INFO_NOTHING == colvalue->m_info)
        {
            stmtprepared->values[stmtprepared->valuecnt++] = rstrdup((char*)colvalue->m_value);
        }
        else if (INFO_COL_IS_NULL == colvalue->m_info)
        {
            stmtprepared->values[stmtprepared->valuecnt++] = NULL;
        }
        else if (INFO_COL_MAY_NULL == colvalue->m_info
                || INFO_COL_IS_DROPED == colvalue->m_info)
        {
            continue;
        }
    }

    deleteStringInfo(preparedstmt);
    return true;
}

/* update */
static bool ripple_rebuild_prepared_update(ripple_rebuild* rebuild,
                                           ripple_txnstmt* stmt,
                                           List** lststmt)
{
    int                                     index = 0;
    int                                     colcnt = 0;
    StringInfo                              preparedstmt = NULL;
    ripple_txnstmt*                         nstmt = NULL;
    ripple_txnstmt_prepared*                stmtprepared = NULL;
    xk_pg_parser_translog_tbcol_values*     row = NULL;
    xk_pg_parser_translog_tbcol_value*      colvalue = NULL;

    /* 入参转换 */
    row = (xk_pg_parser_translog_tbcol_values *)stmt->stmt;

    /* 申请空间 */
    preparedstmt = makeStringInfo();
    appendStringInfo(preparedstmt,
                        "UPDATE \"%s\".\"%s\" SET ",
                        row->m_base.m_schemaname,
                        row->m_base.m_tbname);

    /* 新值 */
    colcnt = ripple_rebuild_appendbindparam(preparedstmt,
                                            row->m_new_values,
                                            row->m_valueCnt,
                                            0,
                                            true);
    
    if (row->m_haspkey || true == ripple_rebuild_hasconskey(rebuild->sysdicts->by_index, row->m_relid))
    {
        appendStringInfo(preparedstmt," WHERE ");
        colcnt = ripple_rebuild_appendbindparam(preparedstmt,
                                                row->m_old_values,
                                                row->m_valueCnt,
                                                colcnt,
                                                false);
    }
    else
    {
        appendStringInfo(preparedstmt,
                            " WHERE CTID = (SELECT CTID FROM \"%s\".\"%s\" WHERE ",
                            row->m_base.m_schemaname,
                            row->m_base.m_tbname);
        colcnt = ripple_rebuild_appendbindparam(preparedstmt,
                                                row->m_old_values,
                                                row->m_valueCnt,
                                                colcnt,
                                                false);
        appendStringInfo(preparedstmt, " LIMIT 1)");
    }

    nstmt = ripple_rebuild_initpreparestmt(rebuild,
                                           row->m_relid,
                                           XK_PG_PARSER_TRANSLOG_DMLTYPE_UPDATE,
                                           colcnt,
                                           preparedstmt->data,
                                           preparedstmt->len);
    if(NULL == nstmt)
    {
        deleteStringInfo(preparedstmt);
        return false;
    }
    nstmt->database = stmt->database;
    nstmt->end = stmt->end;
    nstmt->extra0 = stmt->extra0;
    nstmt->len = stmt->len;
    nstmt->start = stmt->start;
    nstmt->type = RIPPLE_TXNSTMT_TYPE_PREPARED;
    stmtprepared = (ripple_txnstmt_prepared*)nstmt->stmt;
    stmtprepared->row = row;
    stmt->stmt = NULL;

    /* 加入到列表中 */
    *lststmt = lappend(*lststmt, nstmt);

    /* 组装新值  colvalue */
    for (index = 0; index < row->m_valueCnt; index++)
    {
        colvalue = row->m_new_values + index;
        if (INFO_NOTHING == colvalue->m_info)
        {
            stmtprepared->values[stmtprepared->valuecnt++] = rstrdup((char*)colvalue->m_value);
        }
        else if (INFO_COL_IS_NULL == colvalue->m_info)
        {
            stmtprepared->values[stmtprepared->valuecnt++] = NULL;
        }
        else if (INFO_COL_MAY_NULL == colvalue->m_info
                || INFO_COL_IS_DROPED == colvalue->m_info)
        {
            continue;
        }
    }

    /* 组装值  colvalue */
    for (index = 0; index < row->m_valueCnt; index++)
    {
        colvalue = row->m_old_values + index;
        if (INFO_NOTHING == colvalue->m_info)
        {
            stmtprepared->values[stmtprepared->valuecnt++] = rstrdup((char*)colvalue->m_value);
        }
        else if (INFO_COL_IS_NULL == colvalue->m_info)
        {
            stmtprepared->values[stmtprepared->valuecnt++] = NULL;
        }
        else if (INFO_COL_MAY_NULL == colvalue->m_info
                || INFO_COL_IS_DROPED == colvalue->m_info)
        {
            continue;
        }
    }

    deleteStringInfo(preparedstmt);
    return true;
}

/* 对 txn 的内容重组 */
bool ripple_rebuild_prepared(ripple_rebuild* rebuild, ripple_txn* txn)
{
    bool complete                               = false;
    ListCell* lc                                = NULL;
    List* lststmt                               = NULL;
    ListCell* metadatalc                        = NULL;
    ripple_txnstmt* stmtnode                    = NULL;
    ripple_catalogdata *catalogdata             = NULL;
    ripple_txnstmt_metadata* metadatastmt       = NULL;
    xk_pg_parser_translog_tbcolbase* tbcolbase  = NULL;

    if(NULL == txn->stmts)
    {
        return true;
    }

    /* 重组 */
    lststmt = txn->stmts;
    txn->stmts = NULL;
    foreach(lc, lststmt)
    {
        stmtnode = (ripple_txnstmt*)lfirst(lc);

        if (stmtnode->type == RIPPLE_TXNSTMT_TYPE_DML)
        {
            tbcolbase = (xk_pg_parser_translog_tbcolbase *)stmtnode->stmt;
            if(XK_PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT == tbcolbase->m_dmltype)
            {
                if(false == ripple_rebuild_prepared_multiinsert(rebuild, stmtnode, &txn->stmts))
                {
                    return false;
                }
            }
            else if(XK_PG_PARSER_TRANSLOG_DMLTYPE_INSERT == tbcolbase->m_dmltype)
            {
                if(false == ripple_rebuild_prepared_insert(rebuild, stmtnode, &txn->stmts))
                {
                    return false;
                }
            }
            else if(XK_PG_PARSER_TRANSLOG_DMLTYPE_DELETE == tbcolbase->m_dmltype)
            {
                if(false == ripple_rebuild_prepared_delete(rebuild, stmtnode, &txn->stmts))
                {
                    return false;
                }
            }
            else if(XK_PG_PARSER_TRANSLOG_DMLTYPE_UPDATE == tbcolbase->m_dmltype)
            {
                if(false == ripple_rebuild_prepared_update(rebuild, stmtnode, &txn->stmts))
                {
                    return false;
                }
            }

            /* stmtnode 释放 */
            ripple_txnstmt_free(stmtnode);
        }
        else if(stmtnode->type == RIPPLE_TXNSTMT_TYPE_DDL)
        {
            txn->stmts = lappend(txn->stmts, stmtnode);
        }
        else if(stmtnode->type == RIPPLE_TXNSTMT_TYPE_METADATA)
        {
            /* 应用系统表用于主键或唯一约束判断 */
            metadatastmt = (ripple_txnstmt_metadata*)stmtnode->stmt;

            complete = false;
            metadatalc = metadatastmt->begin;
            while(1)
            {
                catalogdata = (ripple_catalogdata*)lfirst(metadatalc);
                ripple_cache_sysdicts_txnsysdicthisitem2cache(rebuild->sysdicts, metadatalc);
                if (RIPPLE_CATALOG_TYPE_CLASS == catalogdata->type)
                {
                    ripple_cache_sysdicts_clearsysdicthisbyclass(rebuild->sysdicts, metadatalc);
                }

                /* 只有一个 */
                if(metadatalc == metadatastmt->end
                    || true == complete)
                {
                    break;
                }
                /* 校验是否到达最后一个 */
                metadatalc = metadatalc->next;
                if(metadatalc == metadatastmt->end)
                {
                    complete = true;
                }
            }
            /* integrate不需要，会导致大事务退出 */
            ripple_txnstmt_free(stmtnode);
        }
        else if(stmtnode->type == RIPPLE_TXNSTMT_TYPE_SHIFTFILE)
        {
            /* integrate不需要，会导致大事务退出 */
            ripple_txnstmt_free(stmtnode);
        }
        else
        {
            txn->stmts = lappend(txn->stmts, stmtnode);
        }
        lc->data.ptr_value = NULL;
    }

    list_free(lststmt);
    return true;
}

/* 对 txn 的内容重组 */
bool ripple_rebuild_txnburst(ripple_rebuild* rebuild, ripple_txn* txn)
{
    ripple_rebuild_burst* burst = NULL;  
    if(NULL == txn->stmts)
    {
        return true;
    }

    burst = ripple_rebuild_burst_init();
    if (NULL == burst)
    {
        return false;
    }
    
    /* 事务重组为burstnode */
    if(false == ripple_rebuild_burst_txn2bursts(burst, rebuild->sysdicts, txn))
    {
        return false;
    }

    /* burstnode合并为sql语句 */
    if (false == ripple_rebuild_burst_bursts2stmt(burst, rebuild->sysdicts, txn))
    {
        return false;
    }

    ripple_rebuild_burst_free(burst);
    burst = NULL;

    return true;
}

/* 内存清理 */
void ripple_rebuild_destroy(ripple_rebuild* rebuild)
{
    if (NULL == rebuild)
    {
        return;
    }

    if (rebuild->hatatb2prepare)
    {
       ripple_rebuild_hatables2prepare_free(rebuild);
    }

    if (rebuild->sysdicts)
    {
        ripple_cache_sysdicts_free(rebuild->sysdicts);
        rebuild->sysdicts = NULL;
    }
    
    return;
}

