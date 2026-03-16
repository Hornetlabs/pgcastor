#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/hash/hash_utils.h"
#include "utils/string/stringinfo.h"
#include "misc/ripple_misc_stat.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_errnodef.h"
#include "common/xk_pg_parser_translog.h"
#include "common/xk_pg_parser_common_utils.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "utils/mpage/mpage.h"
#include "queue/ripple_queue.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadpagefromfile.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadwalrecords.h"
#include "catalog/ripple_catalog.h"
#include "stmts/ripple_txnstmt.h"
#include "snapshot/ripple_snapshot.h"
#include "works/parserwork/wal/ripple_rewind.h"
#include "works/parserwork/wal/ripple_parserwork_decode.h"
#include "works/parserwork/wal/ripple_decode_ddl.h"
#include "works/parserwork/wal/ripple_decode_colvalue.h"
#include "utils/regex/ripple_regex.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "strategy/ripple_filter_dataset.h"

#define RIPPLE_INDEX_HEAP_TYPID                   (uint32_t) 2
#define RIPPLE_INDEX_BTREE_TYPID                  (uint32_t) 403
#define RIPPLE_INDEX_HASH_TYPID                   (uint32_t) 405
#define RIPPLE_INDEX_GIST_TYPID                   (uint32_t) 783
#define RIPPLE_INDEX_GIN_TYPID                    (uint32_t) 2742
#define RIPPLE_INDEX_SPGIST_TYPID                 (uint32_t) 4000
#define RIPPLE_INDEX_BRIN_TYPID                   (uint32_t) 3580

#define RIPPLE_UNUSED(x) (void) (x)

static char *ddl_get_index_typname_by_oid(Oid oid)
{
    switch (oid)
    {
    case RIPPLE_INDEX_HEAP_TYPID:
        return rstrdup("HEAP");
        break;
    case RIPPLE_INDEX_BTREE_TYPID:
        return rstrdup("BTREE");
        break;
    case RIPPLE_INDEX_HASH_TYPID:
        return rstrdup("HASH");
        break;
    case RIPPLE_INDEX_GIST_TYPID:
        return rstrdup("GIST");
        break;
    case RIPPLE_INDEX_GIN_TYPID:
        return rstrdup("GIN");
        break;
    case RIPPLE_INDEX_SPGIST_TYPID:
        return rstrdup("SPGIST");
        break;
    case RIPPLE_INDEX_BRIN_TYPID:
        return rstrdup("BRIN");
        break;
    default:
        return NULL;
        break;
    }
}

static bool ddl_check_in_dataset(Oid oid, ripple_decodingcontext *decodingctx, ripple_txn *txn)
{
    if (oid != InvalidOid)
    {
        if (ripple_filter_dataset_ddl(txn->hsyncdataset, oid)
         || ripple_filter_dataset_ddl(decodingctx->transcache->hsyncdataset, oid))
        {
            /* 再数据集名单中 */
            return true;
        }
    }
    return false;
}

static char *ddl_get_rolename_by_oid(HTAB *pg_authid, uint32_t oid)
{
    xk_pg_sysdict_Form_pg_authid authid = NULL;
    char *result = NULL;
    authid = (xk_pg_sysdict_Form_pg_authid) ripple_catalog_get_authid_sysdict(pg_authid,
                                                                              NULL,
                                                                              NULL,
                                                                              oid);
    if (authid)
    {
        result = authid->rolname.data;
    }
    return result;
}

/* ddl语句可能需要查找事务内缓存 */
static char *ddl_get_namespace_name_by_oid(ripple_decodingcontext *decodingctx,
                                           Oid nspoid,
                                           ripple_txn *txn)
{
    HTAB *pgnsp_htab = decodingctx->transcache->sysdicts->by_namespace;
    xk_pg_parser_sysdict_pgnamespace *nsp = NULL;
    List *list_dict_his = txn->sysdictHis;
    List *list_dict = txn->sysdict;

    /* 从缓存中查找系统表记录 */
    nsp = (xk_pg_parser_sysdict_pgnamespace *) ripple_catalog_get_namespace_sysdict(pgnsp_htab,
                                                                                    list_dict,
                                                                                    list_dict_his,
                                                                                    nspoid);
    if (!nsp)
    {
        elog(RLOG_ERROR, "can't find nspname by oid %u", nspoid);
    }
    return nsp->nspname.data;
}

static char *ddl_get_type_name_by_oid_in_sysdict(ripple_decodingcontext *decodingctx,
                                           Oid typeoid,
                                           ripple_txn *txn)
{
    HTAB *pgtyp_htab = decodingctx->transcache->sysdicts->by_type;
    xk_pg_parser_sysdict_pgtype *typ = NULL;
    List *list_dict_his = txn->sysdictHis;
    List *list_dict = txn->sysdict;

    typ = (xk_pg_parser_sysdict_pgtype *) ripple_catalog_get_type_sysdict(pgtyp_htab,
                                                                          list_dict,
                                                                          list_dict_his,
                                                                          typeoid);

    if (!typ)
    {
        elog(RLOG_ERROR, "can't find type name by oid %u", typeoid);
    }
    return typ->typname.data;
}

static char *ddl_get_relname_by_oid(ripple_decodingcontext *decodingctx,
                                    Oid oid,
                                    ripple_txn *txn)
{
    HTAB *pgclass_htab = decodingctx->transcache->sysdicts->by_class;
    xk_pg_parser_sysdict_pgclass *rel = NULL;
    List *list_dict_his = txn->sysdictHis;
    List *list_dict = txn->sysdict;

    rel = (xk_pg_parser_sysdict_pgclass *) ripple_catalog_get_class_sysdict(pgclass_htab,
                                                                            list_dict,
                                                                            list_dict_his,
                                                                            oid);

    if (!rel)
    {
        elog(RLOG_ERROR, "can't find relname by oid %u", oid);
    }
    return rel->relname.data;
}

static uint32_t ddl_get_relnspoid_by_reloid(ripple_decodingcontext *decodingctx,
                                    Oid oid,
                                    ripple_txn *txn)
{
    HTAB *pgclass_htab = decodingctx->transcache->sysdicts->by_class;
    xk_pg_parser_sysdict_pgclass *rel = NULL;
    List *list_dict_his = txn->sysdictHis;
    List *list_dict = txn->sysdict;

    rel = (xk_pg_parser_sysdict_pgclass *) ripple_catalog_get_class_sysdict(pgclass_htab,
                                                                            list_dict,
                                                                            list_dict_his,
                                                                            oid);

    if (!rel)
    {
        elog(RLOG_ERROR, "can't find relnamespace by oid %u", oid);
    }
    return rel->relnamespace;
}

static char *ddl_get_attname_by_attrelid_attnum(ripple_decodingcontext *decodingctx,
                                                ripple_txn *txn,
                                                Oid attrelid,
                                                uint16_t attnum)
{
    HTAB *pgatt_htab = decodingctx->transcache->sysdicts->by_attribute;
    xk_pg_parser_sysdict_pgattributes *att = NULL;
    List *list_dict_his = txn->sysdictHis;
    List *list_dict = txn->sysdict;

    att = (xk_pg_parser_sysdict_pgattributes *)ripple_catalog_get_attribute_sysdict(pgatt_htab,
                                                                                    list_dict,
                                                                                    list_dict_his,
                                                                                    attrelid,
                                                                                    attnum);
    if (!att)
    {
        elog(RLOG_ERROR, "can't find attname by oid: %u, attnum: %d", attrelid, attnum);
    }
    return att->attname.data;
}

static char *ddl_get_typename_by_oid(ripple_decodingcontext *decodingctx, Oid typoid, ripple_txn *txn)
{
    HTAB *pgtyp_htab = decodingctx->transcache->sysdicts->by_type;
   xk_pg_parser_sysdict_pgtype *typ = NULL;

    typ = (xk_pg_parser_sysdict_pgtype *)ripple_catalog_get_type_sysdict(pgtyp_htab,
                                                                         NULL,
                                                                         NULL,
                                                                         typoid);
    if (!typ)
    {
        elog(RLOG_ERROR, "can't find typename by oid: %u", typoid);
    }
    return typ->typname.data;
}

static char *ddl_get_op_name_by_oid(ripple_decodingcontext *decodingctx, Oid oid)
{
    HTAB *pgop_htab = decodingctx->transcache->sysdicts->by_operator;
    xk_pg_parser_sysdict_pgoperator *op = NULL;

    op = (xk_pg_parser_sysdict_pgoperator *)ripple_catalog_get_operator_sysdict(pgop_htab,
                                                                                NULL,
                                                                                NULL,
                                                                                oid);
    if (!op)
    {
        elog(RLOG_ERROR, "can't find operator name by oid: %u", oid);
    }
    return op->oprname.data;
}

static char *ddl_get_funcname_by_oid(ripple_decodingcontext *decodingctx, Oid oid)
{
    HTAB *pgproc_htab = decodingctx->transcache->sysdicts->by_proc;
    xk_pg_parser_sysdict_pgproc *proc = NULL;

    proc = (xk_pg_parser_sysdict_pgproc *)ripple_catalog_get_proc_sysdict(pgproc_htab,
                                                                              NULL,
                                                                              NULL,
                                                                              oid);
    if (!proc)
    {
        elog(RLOG_ERROR, "can't find proc name by oid: %u", oid);
    }
    return proc->proname.data;
}

static StringInfo ddl_parser_node(ripple_decodingcontext *decodingctx,
                                  ripple_txn *txn,
                                  xk_pg_parser_nodetree *node,
                                  Oid relid,
                                  int local)
{
    StringInfo result = makeStringInfo();
    char *temp = NULL;
    int current_local = 1;
    bool location = false;

    while (node)
    {
        if (node->m_node_type == XK_PG_PARSER_NODETYPE_SEPARATOR)
            current_local += 1;
        if (current_local == local)
            location = true;
        else
            location = false;
        if (!location && local)
        {
            node = node->m_next;
            continue;
        }

        switch (node->m_node_type)
        {
            case XK_PG_PARSER_NODETYPE_VAR:
            {
                xk_pg_parser_node_var *node_var = (xk_pg_parser_node_var *)node->m_node;
                temp = ddl_get_attname_by_attrelid_attnum(decodingctx, txn, relid, node_var->m_attno);
                appendStringInfo(result, "%s", temp);
                temp = NULL;
                break;
            }
            case XK_PG_PARSER_NODETYPE_CONST:
            {
                xk_pg_parser_node_const *node_const = (xk_pg_parser_node_const *)node->m_node;
                if (node_const->m_char_value)
                {
                    /* 这里是表名 */
                    if (node_const->m_typid == XK_PG_SYSDICT_REGCLASSOID)
                    {
                        uint32_t nsp_oid = 0;
                        uint32_t oid = (uint32_t)atoi(node_const->m_char_value);
                        char *nspname_temp = NULL;
                        temp = ddl_get_relname_by_oid(decodingctx, oid, txn);
                        nsp_oid = ddl_get_relnspoid_by_reloid(decodingctx, oid, txn);
                        nspname_temp = ddl_get_namespace_name_by_oid(decodingctx, nsp_oid, txn);

                        appendStringInfo(result, "\'%s.%s\'::", nspname_temp, temp);
                        temp = NULL;
                    }
                    else
                        appendStringInfo(result, "\'%s\'::", node_const->m_char_value);

                    temp = ddl_get_typename_by_oid(decodingctx, node_const->m_typid, txn);
                    appendStringInfo(result, "%s", temp);
                }
                else
                    elog(RLOG_ERROR, "can't convert node tree");
                break;
            }
            case XK_PG_PARSER_NODETYPE_FUNC:
            {
                xk_pg_parser_node_func *node_func = (xk_pg_parser_node_func *)node->m_node;
                if (node_func->m_funcname)
                    appendStringInfo(result, "%s", node_func->m_funcname);
                else
                {
                    temp = ddl_get_funcname_by_oid(decodingctx, node_func->m_funcid);
                    appendStringInfo(result, "%s", temp);
                }
                break;
            }
            case XK_PG_PARSER_NODETYPE_OP:
            {
                xk_pg_parser_node_op *node_op = (xk_pg_parser_node_op *)node->m_node;
                if (node_op->m_opname)
                    appendStringInfo(result, "%s", node_op->m_opname);
                else
                {
                    appendStringInfo(result, "%s", ddl_get_op_name_by_oid(decodingctx, node_op->m_opid));
                }
                break;
            }
            case XK_PG_PARSER_NODETYPE_CHAR:
            {
                appendStringInfo(result, "%s", (char*)node->m_node);
                break;
            }
            case XK_PG_PARSER_NODETYPE_TYPE:
            {
                xk_pg_parser_node_type * node_type = (xk_pg_parser_node_type *)node->m_node;
                char *typname = ddl_get_typename_by_oid(decodingctx, node_type->m_typeid, txn);
                appendStringInfo(result, "%s", typname);
                break;
            }
            case XK_PG_PARSER_NODETYPE_SEPARATOR:
                break;
        }
        node = node->m_next;
    }
    return result;
}

#define TYPE_NAME_GEOGRAPHY_L "geography"
#define TYPE_NAME_GEOGRAPHY_U "GEOGRAPHY"
#define TYPE_NAME_GEOMETRY_L "geometry"
#define TYPE_NAME_GEOMETRY_U "GEOMETRY"

#define CHECK_TYPE_IS_GEOGRAPHY_L(typname) (!strcmp(typname, TYPE_NAME_GEOGRAPHY_L))
#define CHECK_TYPE_IS_GEOGRAPHY_U(typname) (!strcmp(typname, TYPE_NAME_GEOGRAPHY_U))
#define CHECK_TYPE_IS_GEOMETRY_L(typname) (!strcmp(typname, TYPE_NAME_GEOMETRY_L))
#define CHECK_TYPE_IS_GEOMETRY_U(typname) (!strcmp(typname, TYPE_NAME_GEOMETRY_U))

#define CHECK_TYPE_IS_GEOGRAPHY(typname) (CHECK_TYPE_IS_GEOGRAPHY_L(typname) || CHECK_TYPE_IS_GEOGRAPHY_U(typname))
#define CHECK_TYPE_IS_GEOMETRY(typname) (CHECK_TYPE_IS_GEOMETRY_L(typname) || CHECK_TYPE_IS_GEOMETRY_U(typname))


/* CREATE STMT */
static xk_pg_parser_translog_ddlstmt *prepare_create_table(ripple_decodingcontext *decodingctx,
                                 ripple_txn *txn,
                                 xk_pg_parser_translog_ddlstmt *ddl_result)
{
    xk_pg_parser_translog_ddlstmt_createtable *table = (xk_pg_parser_translog_ddlstmt_createtable *)
                                                        ddl_result->m_ddlstmt;
    StringInfo result = NULL;
    char *nspname = NULL;
    int i = 0;
    bool identity = false;

    ripple_txnstmt *stmt = NULL;
    ripple_txnstmt_ddl *dstmt = NULL;

    nspname = ddl_get_namespace_name_by_oid(decodingctx, table->m_nspoid, txn);

    /* 过滤 */
    /* CREATE_TABLE ADD FILTER */
    if (ripple_filter_dataset_matchforcreate(decodingctx->transcache->addtablepattern, nspname, table->m_tabname))
    {
        if (!txn->hsyncdataset)
        {
            HASHCTL hctl = {'\0'};
            HTAB *temp_filter = NULL;
            hctl.keysize = sizeof(Oid);
            hctl.entrysize = sizeof(ripple_filter_oid2datasetnode);
            temp_filter = hash_create("txn_filter_d2o_htab",
                                    128,
                                    &hctl,
                                    HASH_ELEM | HASH_BLOBS);
            txn->hsyncdataset = temp_filter;
        }
        ripple_filter_dataset_add(txn->hsyncdataset, table->m_relid, nspname, table->m_tabname);
    }
    else
    {
        return (ddl_result);
    }

    result = makeStringInfo();
    stmt = rmalloc0(sizeof(ripple_txnstmt));
    dstmt = rmalloc0(sizeof(ripple_txnstmt_ddl));
    rmemset0(dstmt, 0, 0, sizeof(ripple_txnstmt_ddl));
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    stmt->type = RIPPLE_TXNSTMT_TYPE_DDL;

    appendStringInfo(result, "CREATE ");
    if (table->m_logtype == XK_PG_PARSER_DDL_TABLE_LOG_TEMP)
        appendStringInfo(result, "TEMP ");
    else if (table->m_logtype == XK_PG_PARSER_DDL_TABLE_LOG_UNLOGGED)
        appendStringInfo(result, "UNLOGGED ");
    else
    {
        /* do nothing */
    }
    appendStringInfo(result, "TABLE \"%s\".\"%s\" ", nspname, table->m_tabname);
    if (table->m_tableflag == XK_PG_PARSER_DDL_TABLE_FLAG_EMPTY)
    {
        appendStringInfo(result, "()");
    }
    else
    {
        if (table->m_tabletype == XK_PG_PARSER_DDL_TABLE_TYPE_NORMAL
        || table->m_tabletype == XK_PG_PARSER_DDL_TABLE_TYPE_PARTITION)
        {
            appendStringInfo(result, "(");
            for (i = 0; i < table->m_colcnt; i ++)
            {
                char *typname = ddl_get_typename_by_oid(decodingctx, table->m_cols[i].m_coltypid, txn);

                if (table->m_cols[i].m_length > 0)
                    appendStringInfo(result, "\"%s\" %s(%d)", table->m_cols[i].m_colname, typname, table->m_cols[i].m_length);
                else if (table->m_cols[i].m_precision > 0 && table->m_cols[i].m_scale < 0)
                    appendStringInfo(result, "\"%s\" %s(%d)", table->m_cols[i].m_colname, typname, table->m_cols[i].m_precision);
                else if (table->m_cols[i].m_precision > 0 && table->m_cols[i].m_scale >= 0)
                    appendStringInfo(result, "\"%s\" %s(%d, %d)", table->m_cols[i].m_colname,
                                            typname,
                                            table->m_cols[i].m_precision,
                                            table->m_cols[i].m_scale);
                else if ((table->m_cols[i].m_typemod > 0)
                      && (CHECK_TYPE_IS_GEOGRAPHY(typname) || CHECK_TYPE_IS_GEOMETRY(typname)))
                {
                    char *typmod = xk_pg_parser_postgis_typmod_out(table->m_cols[i].m_typemod);
                    appendStringInfo(result, "\"%s\" %s%s", table->m_cols[i].m_colname, typname, typmod);
                    rfree(typmod);
                }
                else
                    appendStringInfo(result, "\"%s\" %s", table->m_cols[i].m_colname, typname);

                if (table->m_cols[i].m_flag & XK_PG_PARSER_DDL_COLUMN_NOTNULL)
                    appendStringInfo(result, " NOT NULL");

                if (table->m_cols[i].m_default)
                {
                    StringInfo node_str = NULL;
                    appendStringInfo(result, " DEFAULT(");
                    node_str = ddl_parser_node(decodingctx,
                                               txn,
                                               table->m_cols[i].m_default,
                                               table->m_relid,
                                               0);
                    appendStringInfo(result, node_str->data);
                    deleteStringInfo(node_str);
                    appendStringInfo(result, ")");
                }

                if (i != table->m_colcnt - 1)
                    appendStringInfo(result, ", ");
            }
            appendStringInfo(result, ")");
        }
    }
    if (table->m_inherits_cnt > 0)
    {
        Oid nsp_oid = InvalidOid;
        char *temp_nsp_name = NULL;
        char *temp_relname = NULL;
        appendStringInfo(result, " INHERITS (");
        for (i = 0; i < table->m_inherits_cnt; i++)
        {
            nsp_oid = ddl_get_relnspoid_by_reloid(decodingctx, table->m_inherits[i], txn);
            temp_relname = ddl_get_relname_by_oid(decodingctx, table->m_inherits[i], txn);
            temp_nsp_name = ddl_get_namespace_name_by_oid(decodingctx, nsp_oid, txn);
            appendStringInfo(result, "\"%s\".\"%s\"", temp_nsp_name, temp_relname);
            if (i != table->m_inherits_cnt - 1)
                appendStringInfo(result, ", ");
        }
        appendStringInfo(result, ")");
    }
    if (table->m_tabletype == XK_PG_PARSER_DDL_TABLE_TYPE_PARTITION_SUB
     || table->m_tabletype == XK_PG_PARSER_DDL_TABLE_TYPE_PARTITION_BOTH)
    {
        StringInfo node_str = NULL;
        Oid nsp_oid = InvalidOid;
        char *temp_nsp_name = NULL;
        char *relname = ddl_get_relname_by_oid(decodingctx, table->m_partitionof->m_partitionof_table_oid, txn);

        nsp_oid = ddl_get_relnspoid_by_reloid(decodingctx, table->m_partitionof->m_partitionof_table_oid, txn);
        temp_nsp_name = ddl_get_namespace_name_by_oid(decodingctx, nsp_oid, txn);

        appendStringInfo(result, " PARTITION OF \"%s\".\"%s\" ", temp_nsp_name, relname);
        node_str = ddl_parser_node(decodingctx,
                           txn,
                           table->m_partitionof->m_partitionof_node,
                           table->m_relid,
                           0);
        appendStringInfo(result, node_str->data);
        deleteStringInfo(node_str);
    }
    if (table->m_tabletype == XK_PG_PARSER_DDL_TABLE_TYPE_PARTITION
     || table->m_tabletype == XK_PG_PARSER_DDL_TABLE_TYPE_PARTITION_BOTH)
    {
        int count = 1;
        appendStringInfo(result, " PARTITION BY ");
        if (table->m_partitionby->m_partition_type == XK_PG_PARSER_DDL_PARTITION_TABLE_HASH)
            appendStringInfo(result, "HASH ");
        else if (table->m_partitionby->m_partition_type == XK_PG_PARSER_DDL_PARTITION_TABLE_RANGE)
            appendStringInfo(result, "RANGE ");
        else if (table->m_partitionby->m_partition_type == XK_PG_PARSER_DDL_PARTITION_TABLE_LIST)
            appendStringInfo(result, "LIST ");
        appendStringInfo(result, "(");
        for (i = 0; i < table->m_partitionby->m_column_num; i++)
        {
            if (table->m_partitionby->m_column[i] != 0)
                appendStringInfo(result, "%s", table->m_cols[table->m_partitionby->m_column[i] - 1].m_colname);
            else
            {
                StringInfo node_str = NULL;
                node_str = ddl_parser_node(decodingctx,
                                           txn,
                                           table->m_partitionby->m_colnode,
                                           table->m_relid,
                                           count++);
                appendStringInfo(result, node_str->data);
                deleteStringInfo(node_str);
            }
            if (i != table->m_partitionby->m_column_num - 1)
                appendStringInfo(result, ", ");
        }
        appendStringInfo(result, ")");
    }
    appendStringInfo(result, ";");

    stmt->len = strlen(result->data);
    //txn->stmtsize += 20;
    txn->stmtsize += stmt->len;
    decodingctx->transcache->totalsize += stmt->len;
    dstmt->type = ddl_result->m_base.m_ddltype;
    dstmt->subtype = ddl_result->m_base.m_ddlinfo;
    dstmt->ddlstmt = rstrdup(result->data);
    stmt->stmt = dstmt;
    deleteStringInfo(result);
    elog(RLOG_DEBUG, "\nddl trans result: %s", dstmt->ddlstmt);

    txn->stmts = lappend(txn->stmts, (void*)stmt);
    if (table->m_owner)
    {
        ripple_txnstmt *stmt_owner = NULL;
        StringInfo owner = NULL;
        ripple_txnstmt_ddl *temp_stmt = rmalloc0(sizeof(ripple_txnstmt_ddl));
        char *rolename = ddl_get_rolename_by_oid(decodingctx->transcache->sysdicts->by_authid,
                                                 table->m_owner);
        rmemset0(temp_stmt, 0, 0, sizeof(ripple_txnstmt_ddl));
        if (!rolename)
        {
            elog(RLOG_WARNING, "can't find owner by oid %u", table->m_owner);
            return (ddl_result);
        }

        stmt_owner = rmalloc0(sizeof(ripple_txnstmt));
        owner = makeStringInfo();

        rmemset0(stmt_owner, 0, 0, sizeof(ripple_txnstmt));
        stmt_owner->type = RIPPLE_TXNSTMT_TYPE_DDL;

        appendStringInfo(owner, "ALTER TABLE \"%s\".\"%s\" OWNER TO \"%s\";", nspname,
                                                                      table->m_tabname,
                                                                      rolename);
        stmt_owner->len = strlen(owner->data);
        //txn->stmtsize += 20;
        txn->stmtsize += stmt_owner->len;
        decodingctx->transcache->totalsize += stmt_owner->len;
        temp_stmt->type = XK_PG_PARSER_DDLTYPE_ALTER;
        temp_stmt->subtype = XK_PG_PARSER_DDLINFO_ALTER_TABLE_OWNER;
        temp_stmt->ddlstmt = rstrdup(owner->data);
        stmt_owner->stmt = temp_stmt;

        deleteStringInfo(owner);
        txn->stmts = lappend(txn->stmts, (void*)stmt_owner);
    }

    identity = guc_getConfigOptionInt(RIPPLE_CFG_KEY_ENABLE_REPLICA_IDENTITY) ? true : false;
    if (identity)
    {
        ripple_txnstmt *stmt_ident = NULL;
        StringInfo ident = NULL;
        ripple_txnstmt_ddl *temp_stmt = NULL;

        temp_stmt = rmalloc0(sizeof(ripple_txnstmt_ddl));
        if (!temp_stmt)
        {
            elog(RLOG_ERROR, "oom");
        }

        rmemset0(temp_stmt, 0, 0, sizeof(ripple_txnstmt_ddl));

        stmt_ident = rmalloc0(sizeof(ripple_txnstmt));

        ident = makeStringInfo();

        rmemset0(stmt_ident, 0, 0, sizeof(ripple_txnstmt));
        stmt_ident->type = RIPPLE_TXNSTMT_TYPE_DDL;

        appendStringInfo(ident, "ALTER TABLE \"%s\".\"%s\" REPLICA IDENTITY FULL;", nspname,
                                                                                    table->m_tabname);
        stmt_ident->len = ident->len;
        //txn->stmtsize += 20;
        txn->stmtsize += stmt_ident->len;
        decodingctx->transcache->totalsize += stmt_ident->len;
        temp_stmt->type = XK_PG_PARSER_DDLTYPE_ALTER;
        temp_stmt->subtype = XK_PG_PARSER_DDLINFO_ALTER_TABLE_REPLICA_IDENTIFITY;
        temp_stmt->ddlstmt = rstrdup(ident->data);
        stmt_ident->stmt = temp_stmt;

        deleteStringInfo(ident);
        txn->stmts = lappend(txn->stmts, (void*)stmt_ident);
    }
    return (ddl_result);
}

static xk_pg_parser_translog_ddlstmt *prepare_create_namespace(ripple_decodingcontext *decodingctx,
                                     ripple_txn *txn,
                                     xk_pg_parser_translog_ddlstmt *ddl_result)
{
    xk_pg_parser_translog_ddlstmt_valuebase *schema = (xk_pg_parser_translog_ddlstmt_valuebase *)
                                                        ddl_result->m_ddlstmt;
    StringInfo result = makeStringInfo();
    ripple_txnstmt *stmt = rmalloc0(sizeof(ripple_txnstmt));
    ripple_txnstmt_ddl *dstmt = rmalloc0(sizeof(ripple_txnstmt_ddl));
    rmemset0(dstmt, 0, 0, sizeof(ripple_txnstmt_ddl));
    RIPPLE_UNUSED(decodingctx);
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    stmt->type = RIPPLE_TXNSTMT_TYPE_DDL;

    appendStringInfo(result, "CREATE SCHEMA \"%s\";", schema->m_value);

    stmt->len = strlen(result->data);
    //txn->stmtsize += 20;
    txn->stmtsize += stmt->len;
    decodingctx->transcache->totalsize += stmt->len;
    dstmt->type = ddl_result->m_base.m_ddltype;
    dstmt->subtype = ddl_result->m_base.m_ddlinfo;
    dstmt->ddlstmt = rstrdup(result->data);
    stmt->stmt = dstmt;
    deleteStringInfo(result);
    elog(RLOG_DEBUG, "\nddl trans result: %s", dstmt->ddlstmt);
    txn->stmts = lappend(txn->stmts, (void*)stmt);

    if (schema->m_owner)
    {
        ripple_txnstmt *stmt_owner = NULL;
        ripple_txnstmt_ddl *temp_stmt = rmalloc0(sizeof(ripple_txnstmt_ddl));
        StringInfo owner = NULL;
        char *rolename = ddl_get_rolename_by_oid(decodingctx->transcache->sysdicts->by_authid,
                                                 schema->m_owner);

        rmemset0(temp_stmt, 0, 0, sizeof(ripple_txnstmt_ddl));
        if (!rolename)
        {
            elog(RLOG_WARNING, "can't find owner by oid %u", schema->m_owner);
            return (ddl_result);
        }

        stmt_owner = rmalloc0(sizeof(ripple_txnstmt));
        owner = makeStringInfo();

        rmemset0(stmt_owner, 0, 0, sizeof(ripple_txnstmt));
        stmt_owner->type = RIPPLE_TXNSTMT_TYPE_DDL;

        appendStringInfo(owner, "ALTER SCHEMA \"%s\" OWNER TO \"%s\";", schema->m_value,
                                                                        rolename);
        stmt_owner->len = strlen(owner->data);
        //txn->stmtsize += 20;
        txn->stmtsize += stmt_owner->len;
    decodingctx->transcache->totalsize += stmt_owner->len;
        temp_stmt->type = XK_PG_PARSER_DDLTYPE_ALTER;
        temp_stmt->subtype = XK_PG_PARSER_DDLINFO_ALTER_TABLE_OWNER;
        temp_stmt->ddlstmt = rstrdup(owner->data);
        stmt_owner->stmt = temp_stmt;
        deleteStringInfo(owner);
        txn->stmts = lappend(txn->stmts, (void*)stmt_owner);
    }

    return (ddl_result);
}

static xk_pg_parser_translog_ddlstmt *prepare_create_index(ripple_decodingcontext *decodingctx,
                                 ripple_txn *txn,
                                 xk_pg_parser_translog_ddlstmt *ddl_result)
{
    xk_pg_parser_translog_ddlstmt_index *index = (xk_pg_parser_translog_ddlstmt_index *)
                                                        ddl_result->m_ddlstmt;
    int i = 0;
    char *relname = NULL;
    char *temp_nsp_name = NULL;
    char *indextype = NULL;
    int count = 1;
    Oid nsp_oid = InvalidOid;
    StringInfo result = NULL;
    ripple_txnstmt *stmt = NULL;
    ripple_txnstmt_ddl *dstmt = NULL;

    if (false == ddl_check_in_dataset(index->m_relid, decodingctx, txn))
    {
        return (ddl_result);
    }

    result = makeStringInfo();
    stmt = rmalloc0(sizeof(ripple_txnstmt));
    dstmt = rmalloc0(sizeof(ripple_txnstmt_ddl));
    rmemset0(dstmt, 0, 0, sizeof(ripple_txnstmt_ddl));
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    stmt->type = RIPPLE_TXNSTMT_TYPE_DDL;

    relname = ddl_get_relname_by_oid(decodingctx, index->m_relid, txn);
    nsp_oid = ddl_get_relnspoid_by_reloid(decodingctx, index->m_relid, txn);
    temp_nsp_name = ddl_get_namespace_name_by_oid(decodingctx, nsp_oid, txn);

    indextype = ddl_get_index_typname_by_oid(index->m_indtype);
    if (!indextype)
        elog(RLOG_ERROR, "unknown index type oid: %u", index->m_indtype);

    appendStringInfo(result,  "CREATE");
    if (index->m_option & XK_PG_PARSER_DDL_INDEX_UNIQUE)
        appendStringInfo(result,  " UNIQUE");

    appendStringInfo(result,  " INDEX \"%s\" ON \"%s\".\"%s\" USING %s (", index->m_indname, temp_nsp_name, relname, indextype);
    for (i = 0; i < index->m_colcnt; i++)
    {
        if (index->m_column[i] > 0)
            appendStringInfo(result,  "\"%s\"", index->m_includecols[i].m_colname);
        else
        {
            StringInfo node = NULL;
            node = ddl_parser_node(decodingctx,
                                   txn,
                                   index->m_colnode,
                                   index->m_relid,
                                   count++);
            appendStringInfo(result, node->data);
            deleteStringInfo(node);
        }
        if (i < index->m_colcnt - 1)
            appendStringInfo(result,  ", ");
    }
    appendStringInfo(result,  ");");

    stmt->len = strlen(result->data);
    //txn->stmtsize += 20;
    txn->stmtsize += stmt->len;
    decodingctx->transcache->totalsize += stmt->len;
    dstmt->type = ddl_result->m_base.m_ddltype;
    dstmt->subtype = ddl_result->m_base.m_ddlinfo;
    dstmt->ddlstmt = rstrdup(result->data);
    stmt->stmt = dstmt;
    deleteStringInfo(result);
    elog(RLOG_DEBUG, "\nddl trans result: %s", dstmt->ddlstmt);
    rfree(indextype);

    txn->stmts = lappend(txn->stmts, (void*)stmt);
    return (ddl_result);
}

static xk_pg_parser_translog_ddlstmt *prepare_create_sequence(ripple_decodingcontext *decodingctx,
                                    ripple_txn *txn,
                                    xk_pg_parser_translog_ddlstmt *ddl_result)
{
    xk_pg_parser_translog_ddlstmt_sequence *seq = (xk_pg_parser_translog_ddlstmt_sequence *)
                                                   ddl_result->m_ddlstmt;
    StringInfo result = makeStringInfo();
    ripple_txnstmt *stmt = rmalloc0(sizeof(ripple_txnstmt));
    ripple_txnstmt_ddl *dstmt = rmalloc0(sizeof(ripple_txnstmt_ddl));
    char *typname = NULL;
    char *temp_nsp_name = NULL;

    rmemset0(dstmt, 0, 0, sizeof(ripple_txnstmt_ddl));
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    stmt->type = RIPPLE_TXNSTMT_TYPE_DDL;

    typname = ddl_get_typename_by_oid(decodingctx, seq->m_seqtypid, txn);
    temp_nsp_name = ddl_get_namespace_name_by_oid(decodingctx, seq->m_seqnspid, txn);

    appendStringInfo(result, "CREATE SEQUENCE \"%s\".\"%s\" AS %s ", temp_nsp_name, seq->m_seqname, typname);
    appendStringInfo(result, "INCREMENT BY %lu ", seq->m_seqincrement);
    appendStringInfo(result, "MINVALUE %lu ", seq->m_seqmin);
    appendStringInfo(result, "MAXVALUE %lu ", seq->m_seqmax);
    appendStringInfo(result, "START WITH %lu ", seq->m_seqstart);
    appendStringInfo(result, "CACHE %lu ", seq->m_seqcache);
    if (seq->m_seqcycle)
        appendStringInfo(result, "CYCLE");
    else
        appendStringInfo(result, "NO CYCLE");
    appendStringInfo(result, ";");

    stmt->len = strlen(result->data);
    //txn->stmtsize += 20;
    txn->stmtsize += stmt->len;
    decodingctx->transcache->totalsize += stmt->len;
    dstmt->type = ddl_result->m_base.m_ddltype;
    dstmt->subtype = ddl_result->m_base.m_ddlinfo;
    dstmt->ddlstmt = rstrdup(result->data);
    stmt->stmt = dstmt;
    deleteStringInfo(result);
    elog(RLOG_DEBUG, "\nddl trans result: %s", dstmt->ddlstmt);

    txn->stmts = lappend(txn->stmts, (void*)stmt);
    return (ddl_result);
}

static xk_pg_parser_translog_ddlstmt *prepare_create_type(ripple_decodingcontext *decodingctx,
                                ripple_txn *txn,
                                xk_pg_parser_translog_ddlstmt *ddl_result)
{
    xk_pg_parser_translog_ddlstmt_type *typ = (xk_pg_parser_translog_ddlstmt_type *)
                                               ddl_result->m_ddlstmt;
    StringInfo result = makeStringInfo();
    ripple_txnstmt *stmt = rmalloc0(sizeof(ripple_txnstmt));
    ripple_txnstmt_ddl *dstmt = rmalloc0(sizeof(ripple_txnstmt_ddl));
    char *nspname = NULL;
    char *typname = NULL;
    int i = 0;

    rmemset0(dstmt, 0, 0, sizeof(ripple_txnstmt_ddl));
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    stmt->type = RIPPLE_TXNSTMT_TYPE_DDL;

    nspname = ddl_get_namespace_name_by_oid(decodingctx, typ->m_typnspid, txn);
    if (typ->m_typtype == XK_PG_PARSER_TRANSLOG_DDLSTMT_TYPE_TYPTYPE_DOMAIN)
    {
        typname = ddl_get_type_name_by_oid_in_sysdict(decodingctx, *((uint32_t *)typ->m_typptr), txn);
        appendStringInfo(result, "CREATE DOMAIN \"%s\".\"%s\" AS %s;", nspname, typ->m_type_name, typname);
    }
    else
    {
        appendStringInfo(result, "CREATE TYPE \"%s\".\"%s\"", nspname, typ->m_type_name);
        if (typ->m_typtype == XK_PG_PARSER_TRANSLOG_DDLSTMT_TYPE_TYPTYPE_COMPOSITE)
        {
            xk_pg_parser_translog_ddlstmt_typcol *typcol = NULL;
            appendStringInfo(result, " AS ");
            appendStringInfo(result, "(");
            for (i = 0; i < typ->m_typvalcnt; i++)
            {
                typcol = (xk_pg_parser_translog_ddlstmt_typcol *)(typ->m_typptr);
                typname = ddl_get_type_name_by_oid_in_sysdict(decodingctx, typcol[i].m_coltypid, txn);
                appendStringInfo(result, "%s %s", typcol[i].m_colname, typname);
                if (i < typ->m_typvalcnt - 1)
                    appendStringInfo(result, ", ");
            }
            appendStringInfo(result, ")");
        }
        else if(typ->m_typtype == XK_PG_PARSER_TRANSLOG_DDLSTMT_TYPE_TYPTYPE_RANGE)
        {
            xk_pg_parser_translog_ddlstmt_typrange *typcol = NULL;
            typcol = (xk_pg_parser_translog_ddlstmt_typrange *)(typ->m_typptr);
            typname = ddl_get_type_name_by_oid_in_sysdict(decodingctx, typcol->m_subtype, txn);
            appendStringInfo(result, " AS ");
            appendStringInfo(result, "RANGE (");
            appendStringInfo(result, "SUBTYPE = %s)", typname);
        }
        else if(typ->m_typtype == XK_PG_PARSER_TRANSLOG_DDLSTMT_TYPE_TYPTYPE_ENUM)
        {
            xk_pg_parser_translog_ddlstmt_valuebase *typcol = NULL;
            appendStringInfo(result, " AS ");
            appendStringInfo(result, "ENUM (");
            for (i = 0; i < typ->m_typvalcnt; i++)
            {
                typcol = (xk_pg_parser_translog_ddlstmt_valuebase *)(typ->m_typptr);
                appendStringInfo(result, "'%s'", typcol[i].m_value);
                if (i < typ->m_typvalcnt - 1)
                    appendStringInfo(result, ", ");
            }
            appendStringInfo(result, ")");
        }
        appendStringInfo(result, ";");
    }

    stmt->len = strlen(result->data);
    //txn->stmtsize += 20;
    txn->stmtsize += stmt->len;
    decodingctx->transcache->totalsize += stmt->len;
    dstmt->type = ddl_result->m_base.m_ddltype;
    dstmt->subtype = ddl_result->m_base.m_ddlinfo;
    dstmt->ddlstmt = rstrdup(result->data);
    stmt->stmt = dstmt;
    deleteStringInfo(result);
    elog(RLOG_DEBUG, "\nddl trans result: %s", dstmt->ddlstmt);

    txn->stmts = lappend(txn->stmts, (void*)stmt);
    if (typ->m_owner)
    {
        ripple_txnstmt *stmt_owner = NULL;
        ripple_txnstmt_ddl *temp_stmt = rmalloc0(sizeof(ripple_txnstmt_ddl));
        StringInfo owner = NULL;
        char *rolename = ddl_get_rolename_by_oid(decodingctx->transcache->sysdicts->by_authid,
                                                 typ->m_owner);

        rmemset0(temp_stmt, 0, 0, sizeof(ripple_txnstmt_ddl));
        if (!rolename)
        {
            elog(RLOG_WARNING, "can't find owner by oid %u", typ->m_owner);
            return (ddl_result);
        }

        stmt_owner = rmalloc0(sizeof(ripple_txnstmt));
        owner = makeStringInfo();

        rmemset0(stmt_owner, 0, 0, sizeof(ripple_txnstmt));
        stmt_owner->type = RIPPLE_TXNSTMT_TYPE_DDL;

        appendStringInfo(owner, "ALTER TYPE \"%s\".\"%s\" OWNER TO \"%s\";", nspname,
                                                                      typ->m_type_name,
                                                                      rolename);
        stmt_owner->len = strlen(owner->data);
        //txn->stmtsize += 20;
        txn->stmtsize += stmt_owner->len;
    decodingctx->transcache->totalsize += stmt_owner->len;
        temp_stmt->type = XK_PG_PARSER_DDLTYPE_ALTER;
        temp_stmt->subtype = XK_PG_PARSER_DDLINFO_ALTER_TABLE_OWNER;
        temp_stmt->ddlstmt = rstrdup(owner->data);
        stmt_owner->stmt = temp_stmt;
        deleteStringInfo(owner);
        txn->stmts = lappend(txn->stmts, (void*)stmt_owner);
    }
    return (ddl_result);
}

static xk_pg_parser_translog_ddlstmt *prepare_drop_namespace(ripple_decodingcontext *decodingctx,
                                   ripple_txn *txn,
                                   xk_pg_parser_translog_ddlstmt *ddl_result)
{
    xk_pg_parser_translog_ddlstmt_valuebase *schema = (xk_pg_parser_translog_ddlstmt_valuebase *)
                                                        ddl_result->m_ddlstmt;
    StringInfo result = makeStringInfo();
    ripple_txnstmt *stmt = rmalloc0(sizeof(ripple_txnstmt));
    ripple_txnstmt_ddl *dstmt = rmalloc0(sizeof(ripple_txnstmt_ddl));

    RIPPLE_UNUSED(decodingctx);

    rmemset0(dstmt, 0, 0, sizeof(ripple_txnstmt_ddl));
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    stmt->type = RIPPLE_TXNSTMT_TYPE_DDL;

    appendStringInfo(result, "DROP SCHEMA \"%s\";", schema->m_value);

    stmt->len = strlen(result->data);
    //txn->stmtsize += 20;
    txn->stmtsize += stmt->len;
    decodingctx->transcache->totalsize += stmt->len;
    dstmt->type = ddl_result->m_base.m_ddltype;
    dstmt->subtype = ddl_result->m_base.m_ddlinfo;
    dstmt->ddlstmt = rstrdup(result->data);
    stmt->stmt = dstmt;
    deleteStringInfo(result);
    elog(RLOG_DEBUG, "\nddl trans result: %s", dstmt->ddlstmt);

    txn->stmts = lappend(txn->stmts, (void*)stmt);
    return (ddl_result);
}

static xk_pg_parser_translog_ddlstmt *prepare_drop_table(ripple_decodingcontext *decodingctx,
                               ripple_txn *txn,
                               xk_pg_parser_translog_ddlstmt *ddl_result)
{
    xk_pg_parser_translog_ddlstmt_drop_base *table = (xk_pg_parser_translog_ddlstmt_drop_base *)
                                                        ddl_result->m_ddlstmt;
    StringInfo result = NULL;
    ripple_txnstmt *stmt = NULL;
    ripple_txnstmt_ddl *dstmt = NULL;
    char *nspname = NULL;

    if (false == ddl_check_in_dataset(table->m_relid, decodingctx, txn))
    {
        return (ddl_result);
    }

    result = makeStringInfo();
    stmt = rmalloc0(sizeof(ripple_txnstmt));
    dstmt = rmalloc0(sizeof(ripple_txnstmt_ddl));

    rmemset0(dstmt, 0, 0, sizeof(ripple_txnstmt_ddl));
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    stmt->type = RIPPLE_TXNSTMT_TYPE_DDL;

    nspname = ddl_get_namespace_name_by_oid(decodingctx, table->m_namespace_oid, txn);

    appendStringInfo(result, "DROP TABLE \"%s\".\"%s\";", nspname, table->m_name);

    stmt->len = strlen(result->data);
    //txn->stmtsize += 20;
    txn->stmtsize += stmt->len;
    decodingctx->transcache->totalsize += stmt->len;
    dstmt->type = ddl_result->m_base.m_ddltype;
    dstmt->subtype = ddl_result->m_base.m_ddlinfo;
    dstmt->ddlstmt = rstrdup(result->data);
    stmt->stmt = dstmt;
    deleteStringInfo(result);
    elog(RLOG_DEBUG, "\nddl trans result: %s", dstmt->ddlstmt);

    txn->stmts = lappend(txn->stmts, (void*)stmt);
    return (ddl_result);
}

static xk_pg_parser_translog_ddlstmt *prepare_drop_index(ripple_decodingcontext *decodingctx,
                               ripple_txn *txn,
                               xk_pg_parser_translog_ddlstmt *ddl_result)
{
    xk_pg_parser_translog_ddlstmt_drop_base *index = (xk_pg_parser_translog_ddlstmt_drop_base *)
                                                        ddl_result->m_ddlstmt;
    StringInfo result = NULL;
    ripple_txnstmt *stmt = NULL;
    ripple_txnstmt_ddl *dstmt = NULL;
    char *nspname = NULL;

    if (false == ddl_check_in_dataset(index->m_relid, decodingctx, txn))
    {
        return (ddl_result);
    }

    result = makeStringInfo();
    stmt = rmalloc0(sizeof(ripple_txnstmt));
    dstmt = rmalloc0(sizeof(ripple_txnstmt_ddl));
    rmemset0(dstmt, 0, 0, sizeof(ripple_txnstmt_ddl));
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    stmt->type = RIPPLE_TXNSTMT_TYPE_DDL;

    nspname = ddl_get_namespace_name_by_oid(decodingctx, index->m_namespace_oid, txn);

    appendStringInfo(result, "DROP INDEX \"%s\".\"%s\";", nspname, index->m_name);

    stmt->len = strlen(result->data);
    //txn->stmtsize += 20;
    txn->stmtsize += stmt->len;
    decodingctx->transcache->totalsize += stmt->len;
    dstmt->type = ddl_result->m_base.m_ddltype;
    dstmt->subtype = ddl_result->m_base.m_ddlinfo;
    dstmt->ddlstmt = rstrdup(result->data);
    stmt->stmt = dstmt;
    deleteStringInfo(result);
    elog(RLOG_DEBUG, "\nddl trans result: %s", dstmt->ddlstmt);

    txn->stmts = lappend(txn->stmts, (void*)stmt);
    return (ddl_result);
}

static xk_pg_parser_translog_ddlstmt *prepare_drop_sequence(ripple_decodingcontext *decodingctx,
                                  ripple_txn *txn,
                                  xk_pg_parser_translog_ddlstmt *ddl_result)
{
    xk_pg_parser_translog_ddlstmt_drop_base *seq = (xk_pg_parser_translog_ddlstmt_drop_base *)
                                                        ddl_result->m_ddlstmt;
    StringInfo result = makeStringInfo();
    ripple_txnstmt *stmt = rmalloc0(sizeof(ripple_txnstmt));
    ripple_txnstmt_ddl *dstmt = rmalloc0(sizeof(ripple_txnstmt_ddl));
    char *nspname = NULL;

    rmemset0(dstmt, 0, 0, sizeof(ripple_txnstmt_ddl));
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    stmt->type = RIPPLE_TXNSTMT_TYPE_DDL;

    nspname = ddl_get_namespace_name_by_oid(decodingctx, seq->m_namespace_oid, txn);

    appendStringInfo(result, "DROP SEQUENCE \"%s\".\"%s\";", nspname, seq->m_name);

    stmt->len = strlen(result->data);
    //txn->stmtsize += 20;
    txn->stmtsize += stmt->len;
    decodingctx->transcache->totalsize += stmt->len;
    dstmt->type = ddl_result->m_base.m_ddltype;
    dstmt->subtype = ddl_result->m_base.m_ddlinfo;
    dstmt->ddlstmt = rstrdup(result->data);
    stmt->stmt = dstmt;
    deleteStringInfo(result);
    elog(RLOG_DEBUG, "\nddl trans result: %s", dstmt->ddlstmt);

    txn->stmts = lappend(txn->stmts, (void*)stmt);
    return (ddl_result);
}

static xk_pg_parser_translog_ddlstmt *prepare_drop_type(ripple_decodingcontext *decodingctx,
                              ripple_txn *txn,
                              xk_pg_parser_translog_ddlstmt *ddl_result)
{
    xk_pg_parser_translog_ddlstmt_drop_base *typ = (xk_pg_parser_translog_ddlstmt_drop_base *)
                                                        ddl_result->m_ddlstmt;
    StringInfo result = makeStringInfo();
    ripple_txnstmt *stmt = rmalloc0(sizeof(ripple_txnstmt));
    ripple_txnstmt_ddl *dstmt = rmalloc0(sizeof(ripple_txnstmt_ddl));
    char *nspname = NULL;

    rmemset0(dstmt, 0, 0, sizeof(ripple_txnstmt_ddl));
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    stmt->type = RIPPLE_TXNSTMT_TYPE_DDL;

    nspname = ddl_get_namespace_name_by_oid(decodingctx, typ->m_namespace_oid, txn);

    appendStringInfo(result, "DROP TYPE \"%s\".\"%s\";", nspname, typ->m_name);

    stmt->len = strlen(result->data);
    //txn->stmtsize += 20;
    txn->stmtsize += stmt->len;
    decodingctx->transcache->totalsize += stmt->len;
    dstmt->type = ddl_result->m_base.m_ddltype;
    dstmt->subtype = ddl_result->m_base.m_ddlinfo;
    dstmt->ddlstmt = rstrdup(result->data);
    stmt->stmt = dstmt;
    deleteStringInfo(result);
    elog(RLOG_DEBUG, "\nddl trans result: %s", dstmt->ddlstmt);

    txn->stmts = lappend(txn->stmts, (void*)stmt);
    return (ddl_result);
}

static xk_pg_parser_translog_ddlstmt *prepare_alter_table_rename_column(ripple_decodingcontext *decodingctx,
                                              ripple_txn *txn,
                                              xk_pg_parser_translog_ddlstmt *ddl_result)
{
    xk_pg_parser_translog_ddlstmt_altercolumn *rename = (xk_pg_parser_translog_ddlstmt_altercolumn *)
                                                        ddl_result->m_ddlstmt;
    StringInfo result = NULL;
    ripple_txnstmt *stmt = NULL;
    ripple_txnstmt_ddl *dstmt = NULL;
    char *relname = NULL;
    char *nspname = NULL;
    uint32_t nspoid = 0;

    if (false == ddl_check_in_dataset(rename->m_relid, decodingctx, txn))
    {
        return (ddl_result);
    }

    result = makeStringInfo();
    stmt = rmalloc0(sizeof(ripple_txnstmt));
    dstmt = rmalloc0(sizeof(ripple_txnstmt_ddl));

    rmemset0(dstmt, 0, 0, sizeof(ripple_txnstmt_ddl));
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    stmt->type = RIPPLE_TXNSTMT_TYPE_DDL;

    relname = ddl_get_relname_by_oid(decodingctx, rename->m_relid, txn);
    nspoid = ddl_get_relnspoid_by_reloid(decodingctx, rename->m_relid, txn);
    nspname = ddl_get_namespace_name_by_oid(decodingctx, nspoid, txn);
    appendStringInfo(result, "ALTER TABLE \"%s\".\"%s\" RENAME \"%s\" TO \"%s\";", nspname,
                                                      relname,
                                                      rename->m_colname,
                                                      rename->m_colname_new);

    stmt->len = strlen(result->data);
    //txn->stmtsize += 20;
    txn->stmtsize += stmt->len;
    decodingctx->transcache->totalsize += stmt->len;
    dstmt->type = ddl_result->m_base.m_ddltype;
    dstmt->subtype = ddl_result->m_base.m_ddlinfo;
    dstmt->ddlstmt = rstrdup(result->data);
    stmt->stmt = dstmt;
    deleteStringInfo(result);
    elog(RLOG_DEBUG, "\nddl trans result: %s", dstmt->ddlstmt);

    txn->stmts = lappend(txn->stmts, (void*)stmt);
    return (ddl_result);
}

static xk_pg_parser_translog_ddlstmt *prepare_alter_table_alter_column_null_set(ripple_decodingcontext *decodingctx,
                                                      ripple_txn *txn,
                                                      xk_pg_parser_translog_ddlstmt *ddl_result)
{
    xk_pg_parser_translog_ddlstmt_altercolumn *alter = (xk_pg_parser_translog_ddlstmt_altercolumn *)
                                                        ddl_result->m_ddlstmt;
    StringInfo result = NULL;
    ripple_txnstmt *stmt = NULL;
    ripple_txnstmt_ddl *dstmt = NULL;
    char *relname = NULL;
    char *nspname = NULL;
    uint32_t nspoid = 0;

    if (false == ddl_check_in_dataset(alter->m_relid, decodingctx, txn))
    {
        return (ddl_result);
    }

    result = makeStringInfo();
    stmt = rmalloc0(sizeof(ripple_txnstmt));
    dstmt = rmalloc0(sizeof(ripple_txnstmt_ddl));

    rmemset0(dstmt, 0, 0, sizeof(ripple_txnstmt_ddl));
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    stmt->type = RIPPLE_TXNSTMT_TYPE_DDL;


    relname = ddl_get_relname_by_oid(decodingctx, alter->m_relid, txn);
    nspoid = ddl_get_relnspoid_by_reloid(decodingctx, alter->m_relid, txn);
    nspname = ddl_get_namespace_name_by_oid(decodingctx, nspoid, txn);
    appendStringInfo(result, "ALTER TABLE \"%s\".\"%s\" ALTER COLUMN \"%s\" ", nspname,
                                                      relname,
                                                      alter->m_colname);
    if (alter->m_notnull == true)
        appendStringInfo(result, "SET NOT NULL;");
    else
        appendStringInfo(result, "DROP NOT NULL;");

    stmt->len = strlen(result->data);
    //txn->stmtsize += 20;
    txn->stmtsize += stmt->len;
    decodingctx->transcache->totalsize += stmt->len;
    dstmt->type = ddl_result->m_base.m_ddltype;
    dstmt->subtype = ddl_result->m_base.m_ddlinfo;
    dstmt->ddlstmt = rstrdup(result->data);
    stmt->stmt = dstmt;
    deleteStringInfo(result);
    elog(RLOG_DEBUG, "\nddl trans result: %s", dstmt->ddlstmt);

    txn->stmts = lappend(txn->stmts, (void*)stmt);
    return (ddl_result);
}

static xk_pg_parser_translog_ddlstmt *prepare_alter_table_alter_column_type(ripple_decodingcontext *decodingctx,
                                                  ripple_txn *txn,
                                                  xk_pg_parser_translog_ddlstmt *ddl_result)
{
    xk_pg_parser_translog_ddlstmt_altercolumn *alter = (xk_pg_parser_translog_ddlstmt_altercolumn *)
                                                        ddl_result->m_ddlstmt;
    StringInfo result = NULL;
    ripple_txnstmt *stmt = NULL;
    ripple_txnstmt_ddl *dstmt = NULL;
    char *relname = NULL;
    char *nspname = NULL;
    char *typname = NULL;
    uint32_t nspoid = 0;

    if (false == ddl_check_in_dataset(alter->m_relid, decodingctx, txn))
    {
        return (ddl_result);
    }

    result = makeStringInfo();
    stmt = rmalloc0(sizeof(ripple_txnstmt));
    dstmt = rmalloc0(sizeof(ripple_txnstmt_ddl));

    rmemset0(dstmt, 0, 0, sizeof(ripple_txnstmt_ddl));
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    stmt->type = RIPPLE_TXNSTMT_TYPE_DDL;

    relname = ddl_get_relname_by_oid(decodingctx, alter->m_relid, txn);
    nspoid = ddl_get_relnspoid_by_reloid(decodingctx, alter->m_relid, txn);
    nspname = ddl_get_namespace_name_by_oid(decodingctx, nspoid, txn);
    typname = ddl_get_typename_by_oid(decodingctx, alter->m_type_new, txn);
    appendStringInfo(result, "ALTER TABLE \"%s\".\"%s\" ALTER COLUMN \"%s\" TYPE %s", nspname,
                                                      relname,
                                                      alter->m_colname,
                                                      typname);
    if (alter->m_length > 0)
        appendStringInfo(result, "(%d)", alter->m_length);
    else if (alter->m_precision > 0 && alter->m_scale < 0)
        appendStringInfo(result, "(%d)", alter->m_precision);
    else if (alter->m_precision > 0 && alter->m_scale >= 0)
        appendStringInfo(result, "(%d, %d)", alter->m_precision, alter->m_scale);
    else if ((alter->m_typemod > 0)
          && (CHECK_TYPE_IS_GEOGRAPHY(typname) || CHECK_TYPE_IS_GEOMETRY(typname)))
    {
        char *typmod = xk_pg_parser_postgis_typmod_out(alter->m_typemod);
        appendStringInfo(result, "%s", typmod);
        rfree(typmod);
    }
    else
        appendStringInfo(result, ";");

    stmt->len = strlen(result->data);
    //txn->stmtsize += 20;
    txn->stmtsize += stmt->len;
    decodingctx->transcache->totalsize += stmt->len;
    dstmt->type = ddl_result->m_base.m_ddltype;
    dstmt->subtype = ddl_result->m_base.m_ddlinfo;
    dstmt->ddlstmt = rstrdup(result->data);
    stmt->stmt = dstmt;
    deleteStringInfo(result);
    elog(RLOG_DEBUG, "\nddl trans result: %s", dstmt->ddlstmt);

    txn->stmts = lappend(txn->stmts, (void*)stmt);
    return (ddl_result);
}

static xk_pg_parser_translog_ddlstmt *prepare_alter_table_alter_column_drop_default(ripple_decodingcontext *decodingctx,
                                     ripple_txn *txn,
                                     xk_pg_parser_translog_ddlstmt *ddl_result)
{
    xk_pg_parser_translog_ddlstmt_default *alter = (xk_pg_parser_translog_ddlstmt_default *)
                                                        ddl_result->m_ddlstmt;
    StringInfo result = NULL;
    ripple_txnstmt *stmt = NULL;
    ripple_txnstmt_ddl *dstmt = NULL;
    uint32_t nspoid = 0;
    char *relname = NULL;
    char *nspname = NULL;

    if (false == ddl_check_in_dataset(alter->m_relid, decodingctx, txn))
    {
        return (ddl_result);
    }

    result = makeStringInfo();
    stmt = rmalloc0(sizeof(ripple_txnstmt));
    dstmt = rmalloc0(sizeof(ripple_txnstmt_ddl));

    rmemset0(dstmt, 0, 0, sizeof(ripple_txnstmt_ddl));
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    stmt->type = RIPPLE_TXNSTMT_TYPE_DDL;


    relname = ddl_get_relname_by_oid(decodingctx, alter->m_relid, txn);
    nspoid = ddl_get_relnspoid_by_reloid(decodingctx, alter->m_relid, txn);
    nspname = ddl_get_namespace_name_by_oid(decodingctx, nspoid, txn);

    appendStringInfo(result, "ALTER TABLE \"%s\".\"%s\" ALTER COLUMN \"%s\" DROP DEFAULT;",
                                                      nspname,
                                                      relname,
                                                      alter->m_colname);

    stmt->len = strlen(result->data);
    //txn->stmtsize += 20;
    txn->stmtsize += stmt->len;
    decodingctx->transcache->totalsize += stmt->len;
    dstmt->type = ddl_result->m_base.m_ddltype;
    dstmt->subtype = ddl_result->m_base.m_ddlinfo;
    dstmt->ddlstmt = rstrdup(result->data);
    stmt->stmt = dstmt;
    deleteStringInfo(result);
    elog(RLOG_DEBUG, "\nddl trans result: %s", dstmt->ddlstmt);

    txn->stmts = lappend(txn->stmts, (void*)stmt);
    return (ddl_result);
}
static xk_pg_parser_translog_ddlstmt *prepare_alter_table_alter_column_default(ripple_decodingcontext *decodingctx,
                                                     ripple_txn *txn,
                                                     xk_pg_parser_translog_ddlstmt *ddl_result)
{
    xk_pg_parser_translog_ddlstmt_default *alter = (xk_pg_parser_translog_ddlstmt_default *)
                                                        ddl_result->m_ddlstmt;
    StringInfo node = NULL;
    StringInfo result = NULL;
    ripple_txnstmt *stmt = NULL;
    ripple_txnstmt_ddl *dstmt = NULL;
    char *relname = NULL;
    char *nspname = NULL;
    uint32_t nspoid = 0;

    if (false == ddl_check_in_dataset(alter->m_relid, decodingctx, txn))
    {
        return (ddl_result);
    }

    result = makeStringInfo();
    stmt = rmalloc0(sizeof(ripple_txnstmt));
    dstmt = rmalloc0(sizeof(ripple_txnstmt_ddl));

    rmemset0(dstmt, 0, 0, sizeof(ripple_txnstmt_ddl));
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    stmt->type = RIPPLE_TXNSTMT_TYPE_DDL;

    relname = ddl_get_relname_by_oid(decodingctx, alter->m_relid, txn);
    nspoid = ddl_get_relnspoid_by_reloid(decodingctx, alter->m_relid, txn);
    nspname = ddl_get_namespace_name_by_oid(decodingctx, nspoid, txn);
    appendStringInfo(result, "ALTER TABLE \"%s\".\"%s\" ALTER COLUMN \"%s\" SET DEFAULT(", nspname,
                                                      relname,
                                                      alter->m_colname);
    node = ddl_parser_node(decodingctx, txn, alter->m_default_node, alter->m_relid, 0);
    appendStringInfo(result, node->data);
    deleteStringInfo(node);

    appendStringInfo(result, ");");

    stmt->len = strlen(result->data);
    //txn->stmtsize += 20;
    txn->stmtsize += stmt->len;
    decodingctx->transcache->totalsize += stmt->len;
    dstmt->type = ddl_result->m_base.m_ddltype;
    dstmt->subtype = ddl_result->m_base.m_ddlinfo;
    dstmt->ddlstmt = rstrdup(result->data);
    stmt->stmt = dstmt;

    deleteStringInfo(result);
    elog(RLOG_DEBUG, "\nddl trans result: %s", dstmt->ddlstmt);

    txn->stmts = lappend(txn->stmts, (void*)stmt);
    return (ddl_result);
}

static xk_pg_parser_translog_ddlstmt *prepare_alter_table_add_column(ripple_decodingcontext *decodingctx,
                                     ripple_txn *txn,
                                     xk_pg_parser_translog_ddlstmt *ddl_result)
{
    xk_pg_parser_translog_ddlstmt_addcolumn *add = (xk_pg_parser_translog_ddlstmt_addcolumn *)
                                                        ddl_result->m_ddlstmt;
    StringInfo result = NULL;
    ripple_txnstmt *stmt = NULL;
    ripple_txnstmt_ddl *dstmt = NULL;
    char *nspname = NULL;
    char *typname = NULL;

    if (false == ddl_check_in_dataset(add->m_relid, decodingctx, txn))
    {
        return (ddl_result);
    }

    result = makeStringInfo();
    stmt = rmalloc0(sizeof(ripple_txnstmt));
    dstmt = rmalloc0(sizeof(ripple_txnstmt_ddl));

    rmemset0(dstmt, 0, 0, sizeof(ripple_txnstmt_ddl));
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    stmt->type = RIPPLE_TXNSTMT_TYPE_DDL;

    nspname = ddl_get_namespace_name_by_oid(decodingctx, add->m_relnamespace, txn);
    typname = ddl_get_typename_by_oid(decodingctx, add->m_addcolumn->m_coltypid, txn);

    appendStringInfo(result, "ALTER TABLE \"%s\".\"%s\" ADD COLUMN \"%s\" %s", nspname,
                                                       add->m_relname,
                                                       add->m_addcolumn->m_colname,
                                                       typname);

    if (add->m_addcolumn->m_length > 0)
        appendStringInfo(result, "(%d) ", add->m_addcolumn->m_length);
    else if (add->m_addcolumn->m_precision > 0 && add->m_addcolumn->m_scale < 0)
        appendStringInfo(result, "(%d) ", add->m_addcolumn->m_precision);
    else if (add->m_addcolumn->m_precision > 0 && add->m_addcolumn->m_scale >= 0)
        appendStringInfo(result, "(%d, %d) ", add->m_addcolumn->m_precision, add->m_addcolumn->m_scale);
    else if ((add->m_addcolumn->m_typemod > 0)
          && (CHECK_TYPE_IS_GEOGRAPHY(typname) || CHECK_TYPE_IS_GEOMETRY(typname)))
    {
        char *typmod = xk_pg_parser_postgis_typmod_out(add->m_addcolumn->m_typemod);
        appendStringInfo(result, "%s", typmod);
        rfree(typmod);
    }
    else
        appendStringInfo(result, " ");

    if (add->m_addcolumn->m_flag & XK_PG_PARSER_DDL_COLUMN_NOTNULL)
        appendStringInfo(result, "NOT NULL");

    if (ddl_result->m_next
     && ddl_result->m_next->m_base.m_ddlinfo == XK_PG_PARSER_DDLINFO_ALTER_COLUMN_DEFAULT
     && ddl_result->m_next->m_base.m_ddltype == XK_PG_PARSER_DDLTYPE_ALTER)
    {
        xk_pg_parser_translog_ddlstmt *next_ddl = NULL;
        uint32_t nspoid = 0;
        char *relname = NULL;
        xk_pg_parser_translog_ddlstmt_default *alter = NULL;

        next_ddl = ddl_result->m_next;
        alter = (xk_pg_parser_translog_ddlstmt_default *) next_ddl->m_ddlstmt;
        if (alter->m_att_default)
        {
            relname = ddl_get_relname_by_oid(decodingctx, alter->m_relid, txn);
            nspoid = ddl_get_relnspoid_by_reloid(decodingctx, alter->m_relid, txn);
            if (!strcmp(relname, add->m_relname) && add->m_relnamespace == nspoid)
            {
                StringInfo node = NULL;
                node = ddl_parser_node(decodingctx, txn, alter->m_default_node, alter->m_relid, 0);
                appendStringInfo(result, "DEFAULT(");
                appendStringInfo(result, node->data);
                deleteStringInfo(node);
                appendStringInfo(result, ")");
            }
            ddl_result = ddl_result->m_next;
        }
    }
    appendStringInfo(result, ";");

    stmt->len = strlen(result->data);
    //txn->stmtsize += 20;
    txn->stmtsize += stmt->len;
    decodingctx->transcache->totalsize += stmt->len;
    dstmt->type = ddl_result->m_base.m_ddltype;
    dstmt->subtype = ddl_result->m_base.m_ddlinfo;
    dstmt->ddlstmt = rstrdup(result->data);
    stmt->stmt = dstmt;
    deleteStringInfo(result);
    elog(RLOG_DEBUG, "\nddl trans result: %s", dstmt->ddlstmt);

    txn->stmts = lappend(txn->stmts, (void*)stmt);
    return (ddl_result);
}

static xk_pg_parser_translog_ddlstmt *prepare_alter_table_rename(ripple_decodingcontext *decodingctx,
                                     ripple_txn *txn,
                                     xk_pg_parser_translog_ddlstmt *ddl_result)
{
    xk_pg_parser_translog_ddlstmt_altertable *table = (xk_pg_parser_translog_ddlstmt_altertable *)
                                                        ddl_result->m_ddlstmt;
    StringInfo result = NULL;
    ripple_txnstmt *stmt = NULL;
    ripple_txnstmt_ddl *dstmt = NULL;
    char *nspname = NULL;

    if (false == ddl_check_in_dataset(table->m_relid, decodingctx, txn))
    {
        return (ddl_result);
    }

    result = makeStringInfo();
    stmt = rmalloc0(sizeof(ripple_txnstmt));
    dstmt = rmalloc0(sizeof(ripple_txnstmt_ddl));

    rmemset0(dstmt, 0, 0, sizeof(ripple_txnstmt_ddl));
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    stmt->type = RIPPLE_TXNSTMT_TYPE_DDL;

    nspname = ddl_get_namespace_name_by_oid(decodingctx, table->m_relnamespaceid, txn);

    appendStringInfo(result, "ALTER TABLE \"%s\".\"%s\" RENAME TO \"%s\";", nspname, table->m_relname, table->m_relname_new);

    stmt->len = strlen(result->data);
    //txn->stmtsize += 20;
    txn->stmtsize += stmt->len;
    decodingctx->transcache->totalsize += stmt->len;
    dstmt->type = ddl_result->m_base.m_ddltype;
    dstmt->subtype = ddl_result->m_base.m_ddlinfo;
    dstmt->ddlstmt = rstrdup(result->data);
    stmt->stmt = dstmt;
    deleteStringInfo(result);
    elog(RLOG_DEBUG, "\nddl trans result: %s", dstmt->ddlstmt);

    txn->stmts = lappend(txn->stmts, (void*)stmt);
    return (ddl_result);
}

static xk_pg_parser_translog_ddlstmt *prepare_alter_table_alter_column_drop_column(ripple_decodingcontext *decodingctx,
                                     ripple_txn *txn,
                                     xk_pg_parser_translog_ddlstmt *ddl_result)
{
    xk_pg_parser_translog_ddlstmt_altercolumn *drop = (xk_pg_parser_translog_ddlstmt_altercolumn *)
                                                        ddl_result->m_ddlstmt;
    StringInfo result = NULL;
    ripple_txnstmt *stmt = NULL;
    ripple_txnstmt_ddl *dstmt = NULL;
    char *relname = NULL;
    char *nspname = NULL;
    uint32_t nspoid = 0;

    if (false == ddl_check_in_dataset(drop->m_relid, decodingctx, txn))
    {
        return (ddl_result);
    }

    result = makeStringInfo();
    stmt = rmalloc0(sizeof(ripple_txnstmt));
    dstmt = rmalloc0(sizeof(ripple_txnstmt_ddl));

    rmemset0(dstmt, 0, 0, sizeof(ripple_txnstmt_ddl));
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    stmt->type = RIPPLE_TXNSTMT_TYPE_DDL;

    relname = ddl_get_relname_by_oid(decodingctx, drop->m_relid, txn);
    nspoid = ddl_get_relnspoid_by_reloid(decodingctx, drop->m_relid, txn);
    nspname = ddl_get_namespace_name_by_oid(decodingctx, nspoid, txn);

    appendStringInfo(result, "ALTER TABLE \"%s\".\"%s\" DROP COLUMN \"%s\";", nspname,
                                                      relname,
                                                      drop->m_colname);

    stmt->len = strlen(result->data);
    //txn->stmtsize += 20;
    txn->stmtsize += stmt->len;
    decodingctx->transcache->totalsize += stmt->len;
    dstmt->type = ddl_result->m_base.m_ddltype;
    dstmt->subtype = ddl_result->m_base.m_ddlinfo;
    dstmt->ddlstmt = rstrdup(result->data);
    stmt->stmt = dstmt;
    deleteStringInfo(result);
    elog(RLOG_DEBUG, "\nddl trans result: %s", dstmt->ddlstmt);

    txn->stmts = lappend(txn->stmts, (void*)stmt);
    return (ddl_result);
}

static xk_pg_parser_translog_ddlstmt *prepare_alter_table_add_constraint(ripple_decodingcontext *decodingctx,
                                     ripple_txn *txn,
                                     xk_pg_parser_translog_ddlstmt *ddl_result)
{
    xk_pg_parser_translog_ddlstmt_tbconstraint *table = (xk_pg_parser_translog_ddlstmt_tbconstraint *)
                                                        ddl_result->m_ddlstmt;
    StringInfo result = NULL;
    ripple_txnstmt *stmt = NULL;
    ripple_txnstmt_ddl *dstmt = NULL;
    char *relname = NULL;
    char *nspname = NULL;
    int i = 0;

    if (false == ddl_check_in_dataset(table->m_relid, decodingctx, txn))
    {
        return (ddl_result);
    }

    result = makeStringInfo();
    stmt = rmalloc0(sizeof(ripple_txnstmt));
    dstmt = rmalloc0(sizeof(ripple_txnstmt_ddl));

    rmemset0(dstmt, 0, 0, sizeof(ripple_txnstmt_ddl));
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    stmt->type = RIPPLE_TXNSTMT_TYPE_DDL;

    relname = ddl_get_relname_by_oid(decodingctx, table->m_relid, txn);
    nspname = ddl_get_namespace_name_by_oid(decodingctx, table->m_consnspoid, txn);

    appendStringInfo(result, "ALTER TABLE \"%s\".\"%s\" ADD CONSTRAINT \"%s\" ", nspname, relname, table->m_consname);
    if (table->m_type == XK_PG_PARSER_DDL_CONSTRAINT_PRIMARYKEY)
    {
        xk_pg_parser_translog_ddlstmt_tbconstraint_key *pkey = 
        (xk_pg_parser_translog_ddlstmt_tbconstraint_key *)table->m_constraint_stmt;

        appendStringInfo(result, "PRIMARY KEY (");
        for (i = 0; i < pkey->m_colcnt; i++)
        {
            appendStringInfo(result, "\"%s\"", pkey->m_concols[i].m_colname);
            if (i < pkey->m_colcnt - 1)
            appendStringInfo(result, ", ");
        }
        appendStringInfo(result, ");");
    }
    else if (table->m_type == XK_PG_PARSER_DDL_CONSTRAINT_UNIQUE)
    {
        xk_pg_parser_translog_ddlstmt_tbconstraint_key *ukey = 
        (xk_pg_parser_translog_ddlstmt_tbconstraint_key *)table->m_constraint_stmt;

        appendStringInfo(result, "UNIQUE (");
        for (i = 0; i < ukey->m_colcnt; i++)
        {
            appendStringInfo(result, "\"%s\"", ukey->m_concols[i].m_colname);
            if (i < ukey->m_colcnt - 1)
            appendStringInfo(result, ", ");
        }
        appendStringInfo(result, ");");
    }
    else if (table->m_type == XK_PG_PARSER_DDL_CONSTRAINT_FOREIGNKEY)
    {
        xk_pg_parser_translog_ddlstmt_tbconstraint_fkey *fkey = 
        (xk_pg_parser_translog_ddlstmt_tbconstraint_fkey *)table->m_constraint_stmt;
        uint32_t nspoid = 0;

        appendStringInfo(result, "FOREIGN KEY (");
        for (i = 0; i < fkey->m_colcnt; i++)
        {
            char *tempcolname = ddl_get_attname_by_attrelid_attnum(decodingctx,
                                                                   txn,
                                                                   table->m_relid,
                                                                   fkey->m_concols_position[i]);
            appendStringInfo(result, "\"%s\"", tempcolname);
            if (i < fkey->m_colcnt - 1)
                appendStringInfo(result, ", ");
        }
        appendStringInfo(result, ") ");

        relname = ddl_get_relname_by_oid(decodingctx, fkey->m_consfkeyid, txn);
        nspoid = ddl_get_relnspoid_by_reloid(decodingctx, fkey->m_consfkeyid, txn);
        nspname = ddl_get_namespace_name_by_oid(decodingctx, nspoid, txn);
        appendStringInfo(result, " REFERENCES \"%s\".\"%s\"(", nspname, relname);
        for (i = 0; i < fkey->m_colcnt; i++)
        {
            char *tempcolname = ddl_get_attname_by_attrelid_attnum(decodingctx,
                                                                   txn,
                                                                   fkey->m_consfkeyid,
                                                                   fkey->m_fkeycols_position[i]);
            appendStringInfo(result, "\"%s\"", tempcolname);
            if (i < fkey->m_colcnt - 1)
            appendStringInfo(result, ", ");
        }
        appendStringInfo(result, ");");
    }
    else if (table->m_type == XK_PG_PARSER_DDL_CONSTRAINT_CHECK)
    {
        StringInfo node = NULL;
        xk_pg_parser_translog_ddlstmt_tbconstraint_check *check = 
        (xk_pg_parser_translog_ddlstmt_tbconstraint_check *)table->m_constraint_stmt;

        appendStringInfo(result, "CHECK (");
        node = ddl_parser_node(decodingctx, txn, check->m_check_node, table->m_relid, 0);
        appendStringInfo(result, node->data);
        deleteStringInfo(node);
        appendStringInfo(result, ");");
    }

    stmt->len = strlen(result->data);
    //txn->stmtsize += 20;
    txn->stmtsize += stmt->len;
    decodingctx->transcache->totalsize += stmt->len;
    dstmt->type = ddl_result->m_base.m_ddltype;
    dstmt->subtype = ddl_result->m_base.m_ddlinfo;
    dstmt->ddlstmt = rstrdup(result->data);
    stmt->stmt = dstmt;
    deleteStringInfo(result);
    elog(RLOG_DEBUG, "\nddl trans result: %s", dstmt->ddlstmt);

    txn->stmts = lappend(txn->stmts, (void*)stmt);
    return (ddl_result);
}

static xk_pg_parser_translog_ddlstmt *prepare_alter_table_drop_constraint(ripple_decodingcontext *decodingctx,
                                     ripple_txn *txn,
                                     xk_pg_parser_translog_ddlstmt *ddl_result)
{
    xk_pg_parser_translog_ddlstmt_drop_constraint *cons = (xk_pg_parser_translog_ddlstmt_drop_constraint *)
                                                        ddl_result->m_ddlstmt;
    StringInfo result = NULL;
    ripple_txnstmt *stmt = NULL;
    ripple_txnstmt_ddl *dstmt = NULL;
    char *nspname = NULL;
    char *relname = NULL;

    if (false == ddl_check_in_dataset(cons->m_relid, decodingctx, txn))
    {
        return (ddl_result);
    }

    /* 忽略继承表的constraint drop */
    if (!cons->m_islocal)
    {
        return ddl_result;
    }

    result = makeStringInfo();
    stmt = rmalloc0(sizeof(ripple_txnstmt));
    dstmt = rmalloc0(sizeof(ripple_txnstmt_ddl));
    rmemset0(dstmt, 0, 0, sizeof(ripple_txnstmt_ddl));
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    stmt->type = RIPPLE_TXNSTMT_TYPE_DDL;

    nspname = ddl_get_namespace_name_by_oid(decodingctx, cons->m_namespace_oid, txn);
    relname = ddl_get_relname_by_oid(decodingctx, cons->m_relid, txn);

    appendStringInfo(result, "ALTER TABLE \"%s\".\"%s\" DROP CONSTRAINT \"%s\";", nspname, relname, cons->m_consname);

    stmt->len = strlen(result->data);
    //txn->stmtsize += 20;
    txn->stmtsize += stmt->len;
    decodingctx->transcache->totalsize += stmt->len;
    dstmt->type = ddl_result->m_base.m_ddltype;
    dstmt->subtype = ddl_result->m_base.m_ddlinfo;
    dstmt->ddlstmt = rstrdup(result->data);
    stmt->stmt = dstmt;
    deleteStringInfo(result);
    elog(RLOG_DEBUG, "\nddl trans result: %s", dstmt->ddlstmt);

    txn->stmts = lappend(txn->stmts, (void*)stmt);
    return (ddl_result);
}

static xk_pg_parser_translog_ddlstmt *prepare_alter_table_alter_column_alter_schema(ripple_decodingcontext *decodingctx,
                                     ripple_txn *txn,
                                     xk_pg_parser_translog_ddlstmt *ddl_result)
{
    xk_pg_parser_translog_ddlstmt_altertable *table = (xk_pg_parser_translog_ddlstmt_altertable *)
                                                        ddl_result->m_ddlstmt;
    StringInfo result = NULL;
    ripple_txnstmt *stmt = NULL;
    ripple_txnstmt_ddl *dstmt = NULL;
    char *nspname_old = NULL;
    char *nspname_new = NULL;

    if (false == ddl_check_in_dataset(table->m_relid, decodingctx, txn))
    {
        return (ddl_result);
    }

    result = makeStringInfo();
    stmt = rmalloc0(sizeof(ripple_txnstmt));
    dstmt = rmalloc0(sizeof(ripple_txnstmt_ddl));

    rmemset0(dstmt, 0, 0, sizeof(ripple_txnstmt_ddl));
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    stmt->type = RIPPLE_TXNSTMT_TYPE_DDL;

    nspname_old = ddl_get_namespace_name_by_oid(decodingctx, table->m_relnamespaceid, txn);
    nspname_new = ddl_get_namespace_name_by_oid(decodingctx, table->m_relnamespaceid_new, txn);

    appendStringInfo(result, "ALTER TABLE \"%s\".\"%s\" SET SCHEMA \"%s\";", nspname_old,
                                                                 table->m_relname,
                                                                 nspname_new);

    stmt->len = strlen(result->data);
    //txn->stmtsize += 20;
    txn->stmtsize += stmt->len;
    decodingctx->transcache->totalsize += stmt->len;
    dstmt->type = ddl_result->m_base.m_ddltype;
    dstmt->subtype = ddl_result->m_base.m_ddlinfo;
    dstmt->ddlstmt = rstrdup(result->data);
    stmt->stmt = dstmt;
    deleteStringInfo(result);
    elog(RLOG_DEBUG, "\nddl trans result: %s", dstmt->ddlstmt);

    txn->stmts = lappend(txn->stmts, (void*)stmt);
    return (ddl_result);
}

static xk_pg_parser_translog_ddlstmt *prepare_alter_table_set_logged(ripple_decodingcontext *decodingctx,
                                     ripple_txn *txn,
                                     xk_pg_parser_translog_ddlstmt *ddl_result)
{
    xk_pg_parser_translog_ddlstmt_setlog *setlog = (xk_pg_parser_translog_ddlstmt_setlog *)
                                                        ddl_result->m_ddlstmt;
    StringInfo result = NULL;
    ripple_txnstmt *stmt = NULL;
    ripple_txnstmt_ddl *dstmt = NULL;
    char *nspname = NULL;


    if (false == ddl_check_in_dataset(setlog->m_relid, decodingctx, txn))
    {
        return (ddl_result);
    }

    result = makeStringInfo();
    stmt = rmalloc0(sizeof(ripple_txnstmt));
    dstmt = rmalloc0(sizeof(ripple_txnstmt_ddl));

    rmemset0(dstmt, 0, 0, sizeof(ripple_txnstmt_ddl));
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    stmt->type = RIPPLE_TXNSTMT_TYPE_DDL;
    nspname = ddl_get_namespace_name_by_oid(decodingctx, setlog->m_relnamespace, txn);

    appendStringInfo(result, "ALTER TABLE \"%s\".\"%s\" SET LOGGED;", nspname,
                                                   setlog->m_relname);

    stmt->len = strlen(result->data);
    //txn->stmtsize += 20;
    txn->stmtsize += stmt->len;
    decodingctx->transcache->totalsize += stmt->len;
    dstmt->type = ddl_result->m_base.m_ddltype;
    dstmt->subtype = ddl_result->m_base.m_ddlinfo;
    dstmt->ddlstmt = rstrdup(result->data);
    stmt->stmt = dstmt;
    deleteStringInfo(result);
    elog(RLOG_DEBUG, "\nddl trans result: %s", dstmt->ddlstmt);

    txn->stmts = lappend(txn->stmts, (void*)stmt);
    return (ddl_result);
}

static xk_pg_parser_translog_ddlstmt *prepare_alter_table_set_unlogged(ripple_decodingcontext *decodingctx,
                                     ripple_txn *txn,
                                     xk_pg_parser_translog_ddlstmt *ddl_result)
{
    xk_pg_parser_translog_ddlstmt_setlog *setlog = (xk_pg_parser_translog_ddlstmt_setlog *)
                                                        ddl_result->m_ddlstmt;
    StringInfo result = NULL;
    ripple_txnstmt *stmt = NULL;
    ripple_txnstmt_ddl *dstmt = NULL;
    char *nspname = NULL;

    if (false == ddl_check_in_dataset(setlog->m_relid, decodingctx, txn))
    {
        return (ddl_result);
    }

    result = makeStringInfo();
    stmt = rmalloc0(sizeof(ripple_txnstmt));
    dstmt = rmalloc0(sizeof(ripple_txnstmt_ddl));

    rmemset0(dstmt, 0, 0, sizeof(ripple_txnstmt_ddl));
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    stmt->type = RIPPLE_TXNSTMT_TYPE_DDL;
    nspname = ddl_get_namespace_name_by_oid(decodingctx, setlog->m_relnamespace, txn);

    appendStringInfo(result, "ALTER TABLE \"%s\".\"%s\" SET UNLOGGED;", nspname,
                                                   setlog->m_relname);

    stmt->len = strlen(result->data);
    //txn->stmtsize += 20;
    txn->stmtsize += stmt->len;
    decodingctx->transcache->totalsize += stmt->len;
    dstmt->type = ddl_result->m_base.m_ddltype;
    dstmt->subtype = ddl_result->m_base.m_ddlinfo;
    dstmt->ddlstmt = rstrdup(result->data);
    stmt->stmt = dstmt;
    deleteStringInfo(result);
    elog(RLOG_DEBUG, "\nddl trans result: %s", dstmt->ddlstmt);

    txn->stmts = lappend(txn->stmts, (void*)stmt);
    return (ddl_result);
}

static xk_pg_parser_translog_ddlstmt *prepare_truncate_table(ripple_decodingcontext *decodingctx,
                                     ripple_txn *txn,
                                     xk_pg_parser_translog_ddlstmt *ddl_result)
{
    xk_pg_parser_translog_ddlstmt_drop_base *table = (xk_pg_parser_translog_ddlstmt_drop_base *)
                                                        ddl_result->m_ddlstmt;
    StringInfo result = NULL;
    ripple_txnstmt *stmt = NULL;
    ripple_txnstmt_ddl *dstmt = NULL;
    char *nspname = NULL;

    if (false == ddl_check_in_dataset(table->m_relid, decodingctx, txn))
    {
        return (ddl_result);
    }

    if (table->m_namespace_oid == PG_TOAST_NAMESPACE)
    {
        return ddl_result;
    }

    dstmt = rmalloc0(sizeof(ripple_txnstmt_ddl));
    rmemset0(dstmt, 0, 0, sizeof(ripple_txnstmt_ddl));
    result = makeStringInfo();
    stmt = rmalloc0(sizeof(ripple_txnstmt));

    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    stmt->type = RIPPLE_TXNSTMT_TYPE_DDL;
    nspname = ddl_get_namespace_name_by_oid(decodingctx, table->m_namespace_oid, txn);

    appendStringInfo(result, "TRUNCATE TABLE \"%s\".\"%s\";", nspname,
                                                   table->m_name);

    stmt->len = strlen(result->data);
    //txn->stmtsize += 20;
    txn->stmtsize += stmt->len;
    decodingctx->transcache->totalsize += stmt->len;
    dstmt->type = ddl_result->m_base.m_ddltype;
    dstmt->subtype = ddl_result->m_base.m_ddlinfo;
    dstmt->ddlstmt = rstrdup(result->data);
    stmt->stmt = dstmt;
    deleteStringInfo(result);
    elog(RLOG_DEBUG, "\nddl trans result: %s", dstmt->ddlstmt);

    txn->stmts = lappend(txn->stmts, (void*)stmt);
    return (ddl_result);
}

static xk_pg_parser_translog_ddlstmt *prepare_reindex(ripple_decodingcontext *decodingctx,
                                     ripple_txn *txn,
                                     xk_pg_parser_translog_ddlstmt *ddl_result)
{
    xk_pg_parser_translog_ddlstmt_drop_base *index = (xk_pg_parser_translog_ddlstmt_drop_base *)
                                                        ddl_result->m_ddlstmt;
    StringInfo result = NULL;
    ripple_txnstmt *stmt = NULL;
    ripple_txnstmt_ddl *dstmt = NULL;
    char *nspname = NULL;

    if (false == ddl_check_in_dataset(index->m_relid, decodingctx, txn))
    {
        return (ddl_result);
    }

    if (index->m_namespace_oid == PG_TOAST_NAMESPACE)
    {
        return ddl_result;
    }

    dstmt = rmalloc0(sizeof(ripple_txnstmt_ddl));
    rmemset0(dstmt, 0, 0, sizeof(ripple_txnstmt_ddl));
    result = makeStringInfo();
    stmt = rmalloc0(sizeof(ripple_txnstmt));

    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    stmt->type = RIPPLE_TXNSTMT_TYPE_DDL;
    nspname = ddl_get_namespace_name_by_oid(decodingctx, index->m_namespace_oid, txn);

    appendStringInfo(result, "REINDEX INDEX \"%s\".\"%s\";", nspname,
                                                   index->m_name);

    stmt->len = strlen(result->data);
    //txn->stmtsize += 20;
    txn->stmtsize += stmt->len;
    decodingctx->transcache->totalsize += stmt->len;
    dstmt->type = ddl_result->m_base.m_ddltype;
    dstmt->subtype = ddl_result->m_base.m_ddlinfo;
    dstmt->ddlstmt = rstrdup(result->data);
    stmt->stmt = dstmt;
    deleteStringInfo(result);
    elog(RLOG_DEBUG, "\nddl trans result: %s", dstmt->ddlstmt);

    txn->stmts = lappend(txn->stmts, (void*)stmt);
    return (ddl_result);
}

static void ddl_stmt2sql(ripple_decodingcontext *decodingctx,
                         ripple_txn *txn,
                         xk_pg_parser_translog_ddlstmt *ddl_result)
{
    xk_pg_parser_translog_ddlstmt *current_ddl = ddl_result;

    while (current_ddl)
    {
        elog(RLOG_DEBUG, "trans ddl, txn: %lu", txn->xid);
        if (current_ddl->m_base.m_ddltype == XK_PG_PARSER_DDLTYPE_CREATE)
        {
            switch (current_ddl->m_base.m_ddlinfo)
            {
                case XK_PG_PARSER_DDLINFO_CREATE_TABLE:
                    current_ddl = prepare_create_table(decodingctx, txn, current_ddl);
                    break;

                 case XK_PG_PARSER_DDLINFO_CREATE_NAMESPACE:
                    current_ddl = prepare_create_namespace(decodingctx, txn, current_ddl);
                    break;

                case XK_PG_PARSER_DDLINFO_CREATE_INDEX:
                    current_ddl = prepare_create_index(decodingctx, txn, current_ddl);
                    break;

                case XK_PG_PARSER_DDLINFO_CREATE_SEQUENCE:
                    current_ddl = prepare_create_sequence(decodingctx, txn, current_ddl);
                    break;

                case XK_PG_PARSER_DDLINFO_CREATE_TYPE:
                    current_ddl = prepare_create_type(decodingctx, txn, current_ddl);
                    break;
            }
        }
        else if (current_ddl->m_base.m_ddltype == XK_PG_PARSER_DDLTYPE_DROP)
        {
            switch (current_ddl->m_base.m_ddlinfo)
            {
                case XK_PG_PARSER_DDLINFO_DROP_NAMESPACE:
                    current_ddl = prepare_drop_namespace(decodingctx, txn, current_ddl);
                    break;

                case XK_PG_PARSER_DDLINFO_DROP_TABLE:
                    current_ddl = prepare_drop_table(decodingctx, txn, current_ddl);
                    break;

                case XK_PG_PARSER_DDLINFO_DROP_INDEX:
                    current_ddl = prepare_drop_index(decodingctx, txn, current_ddl);
                    break;

                case XK_PG_PARSER_DDLINFO_DROP_SEQUENCE:
                    current_ddl = prepare_drop_sequence(decodingctx, txn, current_ddl);
                    break;

                case XK_PG_PARSER_DDLINFO_DROP_TYPE:
                    current_ddl = prepare_drop_type(decodingctx, txn, current_ddl);
                    break;
            }
        }
        else if (current_ddl->m_base.m_ddltype == XK_PG_PARSER_DDLTYPE_ALTER)
        {
            switch (current_ddl->m_base.m_ddlinfo)
            {
                case XK_PG_PARSER_DDLINFO_ALTER_COLUMN_RENAME:
                    current_ddl = prepare_alter_table_rename_column(decodingctx, txn, current_ddl);
                    break;

                case XK_PG_PARSER_DDLINFO_ALTER_COLUMN_NOTNULL:
                case XK_PG_PARSER_DDLINFO_ALTER_COLUMN_NULL:
                    current_ddl = prepare_alter_table_alter_column_null_set(decodingctx, txn, current_ddl);
                    break;

                case XK_PG_PARSER_DDLINFO_ALTER_COLUMN_TYPE:
                    current_ddl = prepare_alter_table_alter_column_type(decodingctx, txn, current_ddl);
                    break;

                case XK_PG_PARSER_DDLINFO_ALTER_COLUMN_DEFAULT:
                    current_ddl = prepare_alter_table_alter_column_default(decodingctx, txn, current_ddl);
                    break;

                case XK_PG_PARSER_DDLINFO_ALTER_COLUMN_DROP_DEFAULT:
                    current_ddl = prepare_alter_table_alter_column_drop_default(decodingctx, txn, current_ddl);
                    break;

                case XK_PG_PARSER_DDLINFO_ALTER_TABLE_ADD_COLUMN:
                    current_ddl = prepare_alter_table_add_column(decodingctx, txn, current_ddl);
                    break;

                case XK_PG_PARSER_DDLINFO_ALTER_TABLE_RENAME:
                    current_ddl = prepare_alter_table_rename(decodingctx, txn, current_ddl);
                    break;

                case XK_PG_PARSER_DDLINFO_ALTER_TABLE_DROP_COLUMN:
                    current_ddl = prepare_alter_table_alter_column_drop_column(decodingctx, txn, current_ddl);
                    break;

                case XK_PG_PARSER_DDLINFO_ALTER_TABLE_ADD_CONSTRAINT:
                    current_ddl = prepare_alter_table_add_constraint(decodingctx, txn, current_ddl);
                    break;

                case XK_PG_PARSER_DDLINFO_ALTER_TABLE_DROP_CONSTRAINT:
                    current_ddl = prepare_alter_table_drop_constraint(decodingctx, txn, current_ddl);
                    break;

                case XK_PG_PARSER_DDLINFO_ALTER_TABLE_NAMESPACE:
                    current_ddl = prepare_alter_table_alter_column_alter_schema(decodingctx, txn, current_ddl);
                    break;

                case XK_PG_PARSER_DDLINFO_ALTER_TABLE_SET_LOGGED:
                    current_ddl = prepare_alter_table_set_logged(decodingctx, txn, current_ddl);
                    break;

                case XK_PG_PARSER_DDLINFO_ALTER_TABLE_SET_UNLOGGED:
                    current_ddl = prepare_alter_table_set_unlogged(decodingctx, txn, current_ddl);
                    break;
            }
        }
        else if (current_ddl->m_base.m_ddltype == XK_PG_PARSER_DDLTYPE_SPECIAL)
        {
            switch (current_ddl->m_base.m_ddlinfo)
            {
                case XK_PG_PARSER_DDLINFO_TRUNCATE:
                    current_ddl = prepare_truncate_table(decodingctx, txn, current_ddl);
                    break;

                case XK_PG_PARSER_DDLINFO_REINDEX:
                    current_ddl = prepare_reindex(decodingctx, txn, current_ddl);
                    break;
            }
        }
        current_ddl = current_ddl->m_next;
    }
}


void ripple_dml2ddl(ripple_decodingcontext* decodingctx, ripple_txn *txn)
{

    xk_pg_parser_translog_systb2ddl *ddl_data = NULL;
    xk_pg_parser_translog_convertinfo *convert_info = NULL;
    xk_pg_parser_translog_ddlstmt *ddl_result = NULL;
    xk_pg_parser_translog_systb2dll_record *ddl_record_head = NULL;
    xk_pg_parser_translog_systb2dll_record *ddl_record_tail = NULL;
    ListCell *cell = NULL;
    int error_num = 0;

    if(g_parserddl == 0)
        return;

    if (!txn->sysdict)
        return;

    convert_info = rmalloc0(sizeof(xk_pg_parser_translog_convertinfo));
    rmemset0(convert_info, 0, 0, sizeof(xk_pg_parser_translog_convertinfo));

    ddl_data = rmalloc0(sizeof(xk_pg_parser_translog_systb2ddl));
    rmemset0(ddl_data, 0, 0, sizeof(xk_pg_parser_translog_systb2ddl));

    convert_info->m_dbcharset = decodingctx->orgdbcharset;
    convert_info->m_tartgetcharset = decodingctx->tgtdbcharset;
    convert_info->m_monetary = decodingctx->monetary;
    convert_info->m_numeric = decodingctx->numeric;
    convert_info->m_tzname = decodingctx->tzname;

    ddl_data->m_convert = convert_info;
    ddl_data->m_dbtype = decodingctx->walpre.m_dbtype;
    ddl_data->m_dbversion = decodingctx->walpre.m_dbversion;
    ddl_data->m_debugLevel = decodingctx->walpre.m_debugLevel;

    foreach(cell, txn->sysdict)
    {
        xk_pg_parser_translog_systb2dll_record *ddl_record_current = NULL;
        ripple_txn_sysdict *dict = lfirst(cell);

        ddl_record_current = rmalloc0(sizeof(xk_pg_parser_translog_systb2dll_record));
        rmemset0(ddl_record_current, 0, 0, sizeof(xk_pg_parser_translog_systb2dll_record));

        ddl_record_current->m_record = dict->colvalues;
        if (!ddl_record_tail)
            ddl_record_head = ddl_record_tail = ddl_record_current;
        else
        {
            ddl_record_tail->m_next = ddl_record_current;
            ddl_record_tail = ddl_record_tail->m_next;
        }
    }
    ddl_data->m_record = ddl_record_head;

    if (!xk_pg_parser_trans_DDLtrans(ddl_data, &ddl_result, &error_num))
    {
        elog(RLOG_ERROR, "error in trans ddl, errcode: %x msg: %s", error_num, xk_pg_parser_errno_getErrInfo(error_num));
    }
    ddl_stmt2sql(decodingctx, txn, ddl_result);
    xk_pg_parser_trans_ddl_free(ddl_data, ddl_result);
}

void heap_ddl_assemble_truncate(ripple_decodingcontext* decodingctx,
                                ripple_txn *txn,
                                uint32_t oid)
{
    StringInfo truncate_stmt = NULL;
    ripple_txnstmt *stmt = NULL;
    ripple_txnstmt_ddl *dstmt = NULL;
    char *relname = NULL;

    if (false == ddl_check_in_dataset(oid, decodingctx, txn))
    {
        return;
    }

    truncate_stmt = makeStringInfo();
    stmt = rmalloc0(sizeof(ripple_txnstmt));
    dstmt = rmalloc0(sizeof(ripple_txnstmt_ddl));
    rmemset0(stmt, 0, '\0', sizeof(ripple_txnstmt));
    stmt->type = RIPPLE_TXNSTMT_TYPE_DDL;
    rmemset0(dstmt, 0, 0, sizeof(ripple_txnstmt_ddl));
    appendStringInfo(truncate_stmt, "TRUNCATE ");
    relname = ddl_get_relname_by_oid(decodingctx, oid, txn);
    appendStringInfo(truncate_stmt, "%s;", relname);

    stmt->len = strlen(truncate_stmt->data);
    //txn->stmtsize += 20;
    txn->stmtsize += stmt->len;
    decodingctx->transcache->totalsize += stmt->len;
    dstmt->type = XK_PG_PARSER_DDLTYPE_SPECIAL;
    dstmt->subtype = XK_PG_PARSER_DDLINFO_TRUNCATE;
    dstmt->ddlstmt = rstrdup(truncate_stmt->data);
    stmt->stmt = dstmt;
    deleteStringInfo(truncate_stmt);
    elog(RLOG_DEBUG, "\nddl trans result: %s", dstmt->ddlstmt);

    txn->stmts = lappend(txn->stmts, (void*)stmt);
}
