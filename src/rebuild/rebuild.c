#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/dlist/dlist.h"
#include "utils/rbtree/rbtree.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/string/stringinfo.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/cache_sysidcts.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "stmts/txnstmt.h"
#include "stmts/txnstmt_prepared.h"
#include "rebuild/rebuild_burst.h"
#include "rebuild/rebuild.h"
#include "rebuild/rebuild_preparestmt.h"
#include "works/parserwork/wal/decode_heap_util.h"

/* Split multiinsert into multiple insert statements */
static bool rebuild_prepared_multiinsert2insert(pg_parser_translog_tbcol_nvalues* nvalues,
                                                pg_parser_translog_tbcol_value*   column,
                                                txnstmt_prepared*                 stmtprepared)
{
    pg_parser_translog_tbcol_values* row = NULL;

    /* Allocate insert data space */
    row = (pg_parser_translog_tbcol_values*)rmalloc0(sizeof(pg_parser_translog_tbcol_values));
    if (NULL == row)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return false;
    }
    rmemset0(row, 0, '\0', sizeof(pg_parser_translog_tbcol_values));

    /* Copy m_base */
    row->m_base.m_dmltype = nvalues->m_base.m_dmltype;
    row->m_base.m_originid = nvalues->m_base.m_originid;
    row->m_base.m_tabletype = nvalues->m_base.m_tabletype;
    row->m_base.m_type = PG_PARSER_TRANSLOG_DMLTYPE_INSERT;
    row->m_base.m_schemaname = rstrdup(nvalues->m_base.m_schemaname);
    row->m_base.m_tbname = rstrdup(nvalues->m_base.m_tbname);

    /* Copy values */
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

static char* rebuild_makehatb(rebuild* rebuild, txnstmt_prepared* stmtprepared, Oid relid)
{
    bool                 found = false;
    tableoptype          table_optype = {'\0'};
    rebuild_preparestmt  tmp_stmt = {'\0'};
    rbtreenode*          treenode = NULL;
    rebuild_preparestmt* pstmt = NULL;
    tableop2preparestmt* hash_entry = NULL;

    table_optype.optype = stmtprepared->optype;
    table_optype.relid = relid;

    stmtprepared->number = rebuild->prepareno;

    hash_entry = hash_search(rebuild->hatatb2prepare, &table_optype, HASH_ENTER, &found);
    /* Not found, initialize rbtree */
    if (false == found)
    {
        hash_entry->tableop.optype = table_optype.optype;
        hash_entry->tableop.relid = table_optype.relid;
        hash_entry->rbtree = rbtree_init(rebuild_preparestmt_cmp, rebuild_preparestmt_free,
                                         rebuild_preparestmt_debug);
        if (NULL == hash_entry->rbtree)
        {
            elog(RLOG_WARNING, "rebuild makehatb rbtree out of memory");
            return NULL;
        }
    }

    /* Add new node */
    pstmt = rebuild_preparestmt_init();
    if (NULL == pstmt)
    {
        elog(RLOG_WARNING, "ripple rebuild preparestmt out of memory");
        return NULL;
    }
    pstmt->number = stmtprepared->number;
    snprintf(pstmt->stmtname, NAMEDATALEN, "query_%lu", rebuild->prepareno);
    pstmt->preparesql = rstrdup(stmtprepared->preparedsql);
    rbtree_insert(hash_entry->rbtree, pstmt);
    (rebuild->prepareno)++;

    /* Get and return stmtname */
    tmp_stmt.preparesql = stmtprepared->preparedsql;
    treenode = rbtree_get_value(hash_entry->rbtree, (void*)&tmp_stmt);

    if (NULL == treenode)
    {
        elog(RLOG_WARNING, "preparedsql not find from hash ");
        return NULL;
    }

    pstmt = (rebuild_preparestmt*)treenode->data;

    return pstmt->stmtname;
}

static char* rebuild_get_stmtname(rebuild* rebuild, txnstmt_prepared* stmtprepared, Oid relid)
{
    bool                 found = false;
    tableoptype          table_optype = {'\0'};
    rebuild_preparestmt  tmp_stmt = {'\0'};
    rbtreenode*          treenode = NULL;
    rebuild_preparestmt* pstmt = NULL;
    tableop2preparestmt* hash_entry = NULL;

    table_optype.optype = stmtprepared->optype;
    table_optype.relid = relid;

    hash_entry = hash_search(rebuild->hatatb2prepare, &table_optype, HASH_FIND, &found);
    if (found)
    {
        /* Query rbtree, return if found, add to rbtree if not found */
        tmp_stmt.preparesql = stmtprepared->preparedsql;
        treenode = rbtree_get_value(hash_entry->rbtree, (void*)&tmp_stmt);
        /* Return value for execution statement */
        if (NULL != treenode)
        {
            pstmt = (rebuild_preparestmt*)treenode->data;
            stmtprepared->number = pstmt->number;
            return pstmt->stmtname;
        }
    }
    return NULL;
}

/* Check if table has primary key or unique constraint */
static bool rebuild_hasconskey(HTAB* hindex, Oid tboid)
{
    List* lindex = NULL;

    /* Get index */
    lindex = (List*)index_getbyoid(tboid, hindex);

    /* Exit if no unique index exists */
    if (NULL == lindex || NULL == lindex->head)
    {
        return false;
    }
    list_free(lindex);
    return true;
}

/* Initialize syncstate->hatables2prepare hash table */
static HTAB* rebuild_hatables2prepare_init(void)
{
    HASHCTL hash_ctl;
    HTAB*   stmthtab;

    rmemset1(&hash_ctl, 0, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(tableoptype);
    hash_ctl.entrysize = sizeof(tableop2preparestmt);
    stmthtab = hash_create("rebuild_hatables2prepare", 2048, &hash_ctl, HASH_ELEM | HASH_BLOBS);
    return stmthtab;
}

/* Free syncstate->hatables2prepare hash table */
static void rebuild_hatables2prepare_free(rebuild* rebuild)
{
    HASH_SEQ_STATUS      status;
    tableop2preparestmt* entry;

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

/* Initialize */
void rebuild_reset(rebuild* rebuild)
{
    if (NULL == rebuild)
    {
        return;
    }
    rebuild->prepareno = 1;
    if (NULL != rebuild->hatatb2prepare)
    {
        rebuild_hatables2prepare_free(rebuild);
    }

    if (NULL != rebuild->sysdicts)
    {
        cache_sysdicts_free(rebuild->sysdicts);
        rebuild->sysdicts = NULL;
    }

    rebuild->sysdicts = cache_sysdicts_integrate_init();

    rebuild->hatatb2prepare = rebuild_hatables2prepare_init();

    return;
}

static txnstmt* rebuild_initpreparestmt(rebuild* rebuild, Oid relid, uint8 op, uint32 colcnt,
                                        char* preparedstmt, uint32 preparedstmtlen)
{
    uint32            len = 0;
    char*             stmtname = NULL;
    txnstmt*          stmt = NULL;
    txnstmt_prepared* stmtprepared = NULL;

    /* Initialize txnstmt */
    stmt = txnstmt_init();
    if (NULL == stmt)
    {
        return NULL;
    }
    stmt->stmt = NULL;
    stmt->type = TXNSTMT_TYPE_PREPARED;

    /* Initialize prepared structure */
    stmtprepared = txnstmt_prepared_init();
    if (NULL == stmtprepared)
    {
        txnstmt_free(stmt);
        return NULL;
    }
    stmt->stmt = (void*)stmtprepared;
    stmtprepared->optype = op;

    /* Allocate memory */
    stmtprepared->values = rmalloc0(sizeof(char*) * colcnt);
    if (NULL == stmtprepared->values)
    {
        txnstmt_free(stmt);
        elog(RLOG_WARNING, "stmtprepared->values malloc error %s", strerror(errno));
        return NULL;
    }
    /* Copy prepared */
    stmtprepared->preparedsql = rmalloc0(preparedstmtlen + 1);
    if (NULL == stmtprepared->preparedsql)
    {
        txnstmt_free(stmt);
        elog(RLOG_WARNING, "stmtprepared->preparedsql malloc error %s", strerror(errno));
        return NULL;
    }
    rmemset0(stmtprepared->preparedsql, 0, '\0', preparedstmtlen + 1);
    rmemcpy0(stmtprepared->preparedsql, 0, preparedstmt, preparedstmtlen);
    stmtprepared->preparedsql[preparedstmtlen] = '\0';

    stmtname = rebuild_get_stmtname(rebuild, stmtprepared, relid);

    if (NULL == stmtname)
    {
        stmtname = rebuild_makehatb(rebuild, stmtprepared, relid);
        if (NULL == stmtname)
        {
            txnstmt_free(stmt);
            return NULL;
        }
    }

    /* Copy prepared */
    len = strlen(stmtname);
    stmtprepared->preparedname = rmalloc0(len + 1);
    if (NULL == stmtprepared->preparedname)
    {
        txnstmt_free(stmt);
        elog(RLOG_WARNING, "stmtprepared->preparedname malloc error %s", strerror(errno));
        return NULL;
    }
    rmemset0(stmtprepared->preparedname, 0, '\0', len + 1);
    rmemcpy0(stmtprepared->preparedname, 0, stmtname, len);
    stmtprepared->preparedname[len] = '\0';
    return stmt;
}

/* Multiinsert */
static bool rebuild_prepared_multiinsert(rebuild* rebuild, txnstmt* stmt, List** lststmt)
{
    bool                              has_valid_column = false;
    uint16_t                          index = 0;
    uint16_t                          index_rowcnt = 0;
    uint32                            colcnts = 0;
    txnstmt*                          nstmt = NULL;
    txnstmt_prepared*                 stmtprepared = NULL;
    pg_parser_translog_tbcol_value*   column = NULL;
    pg_parser_translog_tbcol_nvalues* nvalues = NULL;
    StringInfo                        preparestmtname;

    /* MULTIINSERT processing */
    has_valid_column = false;
    nvalues = (pg_parser_translog_tbcol_nvalues*)stmt->stmt;

    preparestmtname = makeStringInfo();
    appendStringInfo(preparestmtname, "insert into \"%s\".\"%s\" (", nvalues->m_base.m_schemaname,
                     nvalues->m_base.m_tbname);

    /* Assemble column names */
    column = nvalues->m_rows[0].m_new_values;
    for (index = 0; index < nvalues->m_valueCnt; index++)
    {
        if (column[index].m_info == INFO_COL_MAY_NULL || column[index].m_info == INFO_COL_IS_DROPED)
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

    /* Check if at least one valid column exists */
    if (has_valid_column)
    {
        appendStringInfo(preparestmtname, " ) values (");
    }
    else
    {
        /* No columns, allowed in PG series */
        appendStringInfo(preparestmtname, " ) values ();");
    }

    /* Assemble bind parameters */
    if (true == has_valid_column)
    {
        has_valid_column = false;
        for (index = 0; index < nvalues->m_valueCnt; index++)
        {
            if (column[index].m_info == INFO_COL_MAY_NULL ||
                column[index].m_info == INFO_COL_IS_DROPED)
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
        /* No need to bind values, can execute directly */
        elog(RLOG_WARNING, "%s.%s no column, not currently supported", nvalues->m_base.m_schemaname,
             nvalues->m_base.m_tbname);
        deleteStringInfo(preparestmtname);
        return false;
    }

    /* Iterate each row, assemble row data into prepared statement */
    for (index_rowcnt = 0; index_rowcnt < nvalues->m_rowCnt; index_rowcnt++)
    {
        /* Initialize txnstmt */
        nstmt =
            rebuild_initpreparestmt(rebuild, nvalues->m_relid, PG_PARSER_TRANSLOG_DMLTYPE_INSERT,
                                    colcnts, preparestmtname->data, preparestmtname->len);
        if (NULL == nstmt)
        {
            deleteStringInfo(preparestmtname);
            return false;
        }
        nstmt->database = stmt->database;
        nstmt->end = stmt->end;
        nstmt->extra0 = stmt->extra0;
        nstmt->len = stmt->len;
        nstmt->start = stmt->start;
        nstmt->type = TXNSTMT_TYPE_PREPARED;
        stmtprepared = (txnstmt_prepared*)nstmt->stmt;

        if (false == rebuild_prepared_multiinsert2insert(
                         nvalues, nvalues->m_rows[index_rowcnt].m_new_values, stmtprepared))
        {
            txnstmt_free(nstmt);
            deleteStringInfo(preparestmtname);
            return false;
        }

        /* Assemble data */
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

        /* Add to list */
        *lststmt = lappend(*lststmt, nstmt);
    }

    deleteStringInfo(preparestmtname);
    return true;
}

/* Insert */
static bool rebuild_prepared_insert(rebuild* rebuild, txnstmt* stmt, List** lststmt)
{
    int                              index = 0;
    int                              colcnt = 0;
    bool                             has_valid_column = false;
    StringInfo                       preparedstmt = NULL;
    txnstmt*                         nstmt = NULL;
    txnstmt_prepared*                stmtprepared = NULL;
    pg_parser_translog_tbcol_values* row = NULL;
    pg_parser_translog_tbcol_value*  colvalue = NULL;

    /* Input parameter conversion */
    row = (pg_parser_translog_tbcol_values*)stmt->stmt;

    /* Allocate memory */
    preparedstmt = makeStringInfo();
    appendStringInfo(preparedstmt, "insert into \"%s\".\"%s\" (", row->m_base.m_schemaname,
                     row->m_base.m_tbname);

    /* Assemble column names */
    for (index = 0; index < row->m_valueCnt; index++)
    {
        if (row->m_new_values[index].m_info == INFO_COL_MAY_NULL ||
            row->m_new_values[index].m_info == INFO_COL_IS_DROPED)
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

    /* Check if at least one valid column exists */
    if (has_valid_column)
    {
        appendStringInfo(preparedstmt, " ) values (");
    }
    else
    {
        elog(RLOG_WARNING, "%s.%s no column, not currently supported", row->m_base.m_schemaname,
             row->m_base.m_tbname);
        deleteStringInfo(preparedstmt);
        return false;
    }

    /* Assemble bind parameters */
    has_valid_column = false;
    for (index = 0; index < row->m_valueCnt; index++)
    {
        if (row->m_new_values[index].m_info == INFO_COL_MAY_NULL ||
            row->m_new_values[index].m_info == INFO_COL_IS_DROPED)
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

    nstmt = rebuild_initpreparestmt(rebuild, row->m_relid, PG_PARSER_TRANSLOG_DMLTYPE_INSERT,
                                    colcnt, preparedstmt->data, preparedstmt->len);
    if (NULL == nstmt)
    {
        deleteStringInfo(preparedstmt);
        return false;
    }
    nstmt->database = stmt->database;
    nstmt->end = stmt->end;
    nstmt->extra0 = stmt->extra0;
    nstmt->len = stmt->len;
    nstmt->start = stmt->start;
    nstmt->type = TXNSTMT_TYPE_PREPARED;
    stmtprepared = (txnstmt_prepared*)nstmt->stmt;
    stmtprepared->row = row;
    stmt->stmt = NULL;

    /* Add to list */
    *lststmt = lappend(*lststmt, nstmt);

    /* Assemble values colvalue */
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
        else if (INFO_COL_MAY_NULL == colvalue->m_info || INFO_COL_IS_DROPED == colvalue->m_info)
        {
            continue;
        }
    }

    deleteStringInfo(preparedstmt);
    return true;
}

/* Concatenate bind parameters function */
static int rebuild_appendbindparam(StringInfoData* stmt, pg_parser_translog_tbcol_value* values,
                                   int count, int nParams, bool with_comma)
{
    bool is_first = true;
    int  index_colcnt = 0;
    for (index_colcnt = 0; index_colcnt < count; index_colcnt++)
    {
        if (values[index_colcnt].m_info == INFO_COL_MAY_NULL ||
            values[index_colcnt].m_info == INFO_COL_IS_DROPED)
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

/* Delete */
static bool rebuild_prepared_delete(rebuild* rebuild, txnstmt* stmt, List** lststmt)
{
    int                              index = 0;
    int                              colcnt = 0;
    StringInfo                       preparedstmt = NULL;
    txnstmt*                         nstmt = NULL;
    txnstmt_prepared*                stmtprepared = NULL;
    pg_parser_translog_tbcol_values* row = NULL;
    pg_parser_translog_tbcol_value*  colvalue = NULL;

    /* Input parameter conversion */
    row = (pg_parser_translog_tbcol_values*)stmt->stmt;

    /* Allocate space */
    preparedstmt = makeStringInfo();
    appendStringInfo(preparedstmt, "DELETE FROM \"%s\".\"%s\" WHERE ", row->m_base.m_schemaname,
                     row->m_base.m_tbname);

    /* No primary key or unique constraint */
    if (false == row->m_haspkey &&
        false == rebuild_hasconskey(rebuild->sysdicts->by_index, row->m_relid))
    {
        appendStringInfo(preparedstmt, "CTID = (SELECT CTID FROM \"%s\".\"%s\" WHERE ",
                         row->m_base.m_schemaname, row->m_base.m_tbname);
    }

    colcnt = rebuild_appendbindparam(preparedstmt, row->m_old_values, row->m_valueCnt, 0, false);

    if (false == row->m_haspkey &&
        false == rebuild_hasconskey(rebuild->sysdicts->by_index, row->m_relid))
    {
        appendStringInfo(preparedstmt, " LIMIT 1)");
    }

    nstmt = rebuild_initpreparestmt(rebuild, row->m_relid, PG_PARSER_TRANSLOG_DMLTYPE_DELETE,
                                    colcnt, preparedstmt->data, preparedstmt->len);
    if (NULL == nstmt)
    {
        deleteStringInfo(preparedstmt);
        return false;
    }
    nstmt->database = stmt->database;
    nstmt->end = stmt->end;
    nstmt->extra0 = stmt->extra0;
    nstmt->len = stmt->len;
    nstmt->start = stmt->start;
    nstmt->type = TXNSTMT_TYPE_PREPARED;
    stmtprepared = (txnstmt_prepared*)nstmt->stmt;
    stmtprepared->row = row;
    stmt->stmt = NULL;

    /* Add to list */
    *lststmt = lappend(*lststmt, nstmt);

    /* Assemble values colvalue */
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
        else if (INFO_COL_MAY_NULL == colvalue->m_info || INFO_COL_IS_DROPED == colvalue->m_info)
        {
            continue;
        }
    }

    deleteStringInfo(preparedstmt);
    return true;
}

/* Update */
static bool rebuild_prepared_update(rebuild* rebuild, txnstmt* stmt, List** lststmt)
{
    int                              index = 0;
    int                              colcnt = 0;
    StringInfo                       preparedstmt = NULL;
    txnstmt*                         nstmt = NULL;
    txnstmt_prepared*                stmtprepared = NULL;
    pg_parser_translog_tbcol_values* row = NULL;
    pg_parser_translog_tbcol_value*  colvalue = NULL;

    /* Input parameter conversion */
    row = (pg_parser_translog_tbcol_values*)stmt->stmt;

    /* Allocate space */
    preparedstmt = makeStringInfo();
    appendStringInfo(preparedstmt, "UPDATE \"%s\".\"%s\" SET ", row->m_base.m_schemaname,
                     row->m_base.m_tbname);

    /* New values */
    colcnt = rebuild_appendbindparam(preparedstmt, row->m_new_values, row->m_valueCnt, 0, true);

    if (row->m_haspkey || true == rebuild_hasconskey(rebuild->sysdicts->by_index, row->m_relid))
    {
        appendStringInfo(preparedstmt, " WHERE ");
        colcnt = rebuild_appendbindparam(preparedstmt, row->m_old_values, row->m_valueCnt, colcnt,
                                         false);
    }
    else
    {
        appendStringInfo(preparedstmt, " WHERE CTID = (SELECT CTID FROM \"%s\".\"%s\" WHERE ",
                         row->m_base.m_schemaname, row->m_base.m_tbname);
        colcnt = rebuild_appendbindparam(preparedstmt, row->m_old_values, row->m_valueCnt, colcnt,
                                         false);
        appendStringInfo(preparedstmt, " LIMIT 1)");
    }

    nstmt = rebuild_initpreparestmt(rebuild, row->m_relid, PG_PARSER_TRANSLOG_DMLTYPE_UPDATE,
                                    colcnt, preparedstmt->data, preparedstmt->len);
    if (NULL == nstmt)
    {
        deleteStringInfo(preparedstmt);
        return false;
    }
    nstmt->database = stmt->database;
    nstmt->end = stmt->end;
    nstmt->extra0 = stmt->extra0;
    nstmt->len = stmt->len;
    nstmt->start = stmt->start;
    nstmt->type = TXNSTMT_TYPE_PREPARED;
    stmtprepared = (txnstmt_prepared*)nstmt->stmt;
    stmtprepared->row = row;
    stmt->stmt = NULL;

    /* Add to list */
    *lststmt = lappend(*lststmt, nstmt);

    /* Assemble new values colvalue */
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
        else if (INFO_COL_MAY_NULL == colvalue->m_info || INFO_COL_IS_DROPED == colvalue->m_info)
        {
            continue;
        }
    }

    /* Assemble values colvalue */
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
        else if (INFO_COL_MAY_NULL == colvalue->m_info || INFO_COL_IS_DROPED == colvalue->m_info)
        {
            continue;
        }
    }

    deleteStringInfo(preparedstmt);
    return true;
}

/* Reorganize txn content */
bool rebuild_prepared(rebuild* rebuild, txn* txn)
{
    bool                          complete = false;
    ListCell*                     lc = NULL;
    List*                         lststmt = NULL;
    ListCell*                     metadatalc = NULL;
    txnstmt*                      stmtnode = NULL;
    catalogdata*                  catalog_data = NULL;
    txnstmt_metadata*             metadatastmt = NULL;
    pg_parser_translog_tbcolbase* tbcolbase = NULL;

    if (NULL == txn->stmts)
    {
        return true;
    }

    /* Rebuild */
    lststmt = txn->stmts;
    txn->stmts = NULL;
    foreach (lc, lststmt)
    {
        stmtnode = (txnstmt*)lfirst(lc);

        if (stmtnode->type == TXNSTMT_TYPE_DML)
        {
            tbcolbase = (pg_parser_translog_tbcolbase*)stmtnode->stmt;
            if (PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT == tbcolbase->m_dmltype)
            {
                if (false == rebuild_prepared_multiinsert(rebuild, stmtnode, &txn->stmts))
                {
                    return false;
                }
            }
            else if (PG_PARSER_TRANSLOG_DMLTYPE_INSERT == tbcolbase->m_dmltype)
            {
                if (false == rebuild_prepared_insert(rebuild, stmtnode, &txn->stmts))
                {
                    return false;
                }
            }
            else if (PG_PARSER_TRANSLOG_DMLTYPE_DELETE == tbcolbase->m_dmltype)
            {
                if (false == rebuild_prepared_delete(rebuild, stmtnode, &txn->stmts))
                {
                    return false;
                }
            }
            else if (PG_PARSER_TRANSLOG_DMLTYPE_UPDATE == tbcolbase->m_dmltype)
            {
                if (false == rebuild_prepared_update(rebuild, stmtnode, &txn->stmts))
                {
                    return false;
                }
            }

            /* Free stmtnode */
            txnstmt_free(stmtnode);
        }
        else if (stmtnode->type == TXNSTMT_TYPE_DDL)
        {
            txn->stmts = lappend(txn->stmts, stmtnode);
        }
        else if (stmtnode->type == TXNSTMT_TYPE_METADATA)
        {
            /* Apply system table for primary key or unique constraint judgment */
            metadatastmt = (txnstmt_metadata*)stmtnode->stmt;

            complete = false;
            metadatalc = metadatastmt->begin;
            while (1)
            {
                catalog_data = (catalogdata*)lfirst(metadatalc);
                cache_sysdicts_txnsysdicthisitem2cache(rebuild->sysdicts, metadatalc);
                if (CATALOG_TYPE_CLASS == catalog_data->type)
                {
                    cache_sysdicts_clearsysdicthisbyclass(rebuild->sysdicts, metadatalc);
                }

                /* Only one */
                if (metadatalc == metadatastmt->end || true == complete)
                {
                    break;
                }
                /* Verify if reached the last one */
                metadatalc = metadatalc->next;
                if (metadatalc == metadatastmt->end)
                {
                    complete = true;
                }
            }
            /* Not needed for integrate, will cause large transaction exit */
            txnstmt_free(stmtnode);
        }
        else if (stmtnode->type == TXNSTMT_TYPE_SHIFTFILE)
        {
            /* Not needed for integrate, will cause large transaction exit */
            txnstmt_free(stmtnode);
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

/* Reorganize txn content */
bool rebuild_txnburst(rebuild* rebuild, txn* txn)
{
    rebuild_burst* burst = NULL;
    if (NULL == txn->stmts)
    {
        return true;
    }

    burst = rebuild_burst_init();
    if (NULL == burst)
    {
        return false;
    }

    /* Rebuild transaction into burstnode */
    if (false == rebuild_burst_txn2bursts(burst, rebuild->sysdicts, txn))
    {
        return false;
    }

    /* Merge burstnode into sql statement */
    if (false == rebuild_burst_bursts2stmt(burst, rebuild->sysdicts, txn))
    {
        return false;
    }

    rebuild_burst_free(burst);
    burst = NULL;

    return true;
}

/* Memory cleanup */
void rebuild_destroy(rebuild* rebuild)
{
    if (NULL == rebuild)
    {
        return;
    }

    if (rebuild->hatatb2prepare)
    {
        rebuild_hatables2prepare_free(rebuild);
    }

    if (rebuild->sysdicts)
    {
        cache_sysdicts_free(rebuild->sysdicts);
        rebuild->sysdicts = NULL;
    }

    return;
}
