#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/hash/hash_utils.h"
#include "utils/string/stringinfo.h"
#include "misc/misc_stat.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_errnodef.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "utils/mpage/mpage.h"
#include "queue/queue.h"
#include "loadrecords/record.h"
#include "loadrecords/loadpage.h"
#include "loadrecords/loadpageam.h"
#include "loadrecords/loadpagefromfile.h"
#include "loadrecords/loadrecords.h"
#include "loadrecords/loadwalrecords.h"
#include "cache/fpwcache.h"
#include "cache/toastcache.h"
#include "catalog/catalog.h"
#include "catalog/class.h"
#include "catalog/attribute.h"
#include "stmts/txnstmt.h"
#include "snapshot/snapshot.h"
#include "works/parserwork/wal/rewind.h"
#include "works/parserwork/wal/parserwork_decode.h"
#include "works/parserwork/wal/decode_heap.h"
#include "works/parserwork/wal/decode_heap_util.h"


static void get_all_sysdict(decodingcontext *decodingctx,
                            txn *txn,
                            Oid search_oid,
                            bool search_his,
                            List **class_list,
                            List **namespace_list,
                            List **attributes_list,
                            List **type_list,
                            List **range_list,
                            List **enum_list,
                            List **proc_list,
                            Oid *typid_cache,
                            int *typ_cache_num)
{
    Oid nspid = 0;
    int natts = 0;
    int i = 0;
    ListCell *cell = NULL;
    xk_pg_parser_sysdict_pgattributes *att = NULL;
    xk_pg_parser_sysdict_pgtype *typ = NULL;
    Oid att_typid = 0;
    bool have_type_cache = false;
    List *temp_class_list = NULL;
    List *temp_namespace_list = NULL;
    List *temp_attributes_list = NULL;
    List *temp_type_list = NULL;
    List *temp_range_list = NULL;
    List *temp_enum_list = NULL;
    List *temp_proc_list = NULL;

    void *temp_dict = NULL;
    List *temp_sysdicthis = NULL;
    if (search_his)
    {
        temp_sysdicthis = txn->sysdictHis;
    }
    else
    {
        temp_sysdicthis = NULL;
    }

    /* 查找pg_class */
     temp_dict = catalog_get_class_sysdict(decodingctx->trans_cache->sysdicts->by_class, txn->sysdict, temp_sysdicthis, search_oid);
    if (!temp_dict)
    {
        elog(RLOG_ERROR, "can't find pg_class relation");
    }
    natts = ((xk_pg_parser_sysdict_pgclass *) temp_dict)->relnatts;
    nspid = ((xk_pg_parser_sysdict_pgclass *) temp_dict)->relnamespace;
    temp_class_list = lappend(temp_class_list, temp_dict);

    /* 查找pg_namespace */
    temp_dict = catalog_get_namespace_sysdict(decodingctx->trans_cache->sysdicts->by_namespace, txn->sysdict, temp_sysdicthis, nspid);
    if (!temp_dict)
    {
        elog(RLOG_ERROR, "can't find pg_namespace relation");
    }
    temp_namespace_list = lappend(temp_namespace_list, temp_dict);
    /* 查找pg_attribute */
    for (i = 0; i < natts; i++)
    {
        void *temp_att = NULL;
        temp_att = catalog_get_attribute_sysdict(decodingctx->trans_cache->sysdicts->by_attribute,
                                                      txn->sysdict,
                                                      temp_sysdicthis,
                                                      search_oid,
                                                      i + 1);
        if (!temp_att)
        {
            elog(RLOG_ERROR, "can't find pg_attribute relation");
        }
        temp_attributes_list = lappend(temp_attributes_list, temp_att);
    }
    /* 查找pg_type */
    foreach(cell, temp_attributes_list)
    {
        att = (xk_pg_parser_sysdict_pgattributes*)lfirst(cell);
        if (att->attisdropped)
            continue;
        att_typid = att->atttypid;
        have_type_cache = false;
        for (i = 0; i < 128; i++)
        {
            if (typid_cache[i] == 0)
                break;
            if (typid_cache[i] == att_typid)
            {
                have_type_cache = true;
                break;
            }
        }
        if (!have_type_cache)
        {
            void *temp_type = catalog_get_type_sysdict(decodingctx->trans_cache->sysdicts->by_type,
                                                              NULL,
                                                              temp_sysdicthis,
                                                              att_typid);
            if (!temp_type)
            {
                elog(RLOG_ERROR, "can't find pg_type relation");
            }
            temp_type_list = lappend(temp_type_list, temp_type);
            typid_cache[*typ_cache_num] = att_typid;
            *typ_cache_num += 1;
        }
    }
    /* 如果pg_type的typelem不为0, 需要将其加入到type链表中 */
    foreach(cell, temp_type_list)
    {
        typ = (xk_pg_parser_sysdict_pgtype*)lfirst(cell);
        have_type_cache = false;
        if (!typ->typelem)
            continue;
        for (i = 0; i < 128; i++)
        {
            if (typid_cache[i] == 0)
                break;
            if (typid_cache[i] == typ->typelem)
            {
                have_type_cache = true;
                break;
            }
        }
        if (!have_type_cache)
        {
           void *temp_type = catalog_get_type_sysdict(decodingctx->trans_cache->sysdicts->by_type,
                                                              NULL,
                                                              temp_sysdicthis,
                                                              typ->typelem);
            if (!temp_type)
            {
                elog(RLOG_ERROR, "can't find pg_type relation");
            }
            temp_type_list = lappend(temp_type_list, temp_type);
            typid_cache[*typ_cache_num] = typ->typelem;
            *typ_cache_num += 1;
        }
    }
    
    cell = NULL;
    /* 查找pg_proc, pg_enum, pg_range */
    foreach(cell, temp_type_list)
    {
        typ = (xk_pg_parser_sysdict_pgtype*)lfirst(cell);
        if (typ->typtype == 'r')
        {
            xk_pg_parser_sysdict_pgrange *temp_range = 
                (xk_pg_parser_sysdict_pgrange *) catalog_get_range_sysdict(decodingctx->trans_cache->sysdicts->by_range,
                                                                                  NULL,
                                                                                  temp_sysdicthis,
                                                                                  typ->oid);
            if (!temp_range)
            {
                elog(RLOG_ERROR, "can't find pg_range relation");
            }
            have_type_cache = false;
            for (i = 0; i < 128; i++)
            {
                if (typid_cache[i] == 0)
                    break;
                if (typid_cache[i] == temp_range->rngsubtype)
                {
                    have_type_cache = true;
                    break;
                }
            }
            if (!have_type_cache)
            {
                void *temp_type = catalog_get_type_sysdict(decodingctx->trans_cache->sysdicts->by_type,
                                                                  NULL,
                                                                  temp_sysdicthis,
                                                                  temp_range->rngsubtype);
                if (!temp_type)
                {
                    elog(RLOG_ERROR, "can't find pg_type relation");
                }
                temp_type_list = lappend(temp_type_list, temp_type);
                typid_cache[*typ_cache_num] = temp_range->rngsubtype;
                *typ_cache_num += 1;
            }
            temp_range_list = lappend(temp_range_list, temp_range);
        }
        else if (typ->typtype == 'e')
        {
            temp_enum_list = (List *) catalog_get_enum_sysdict_list(decodingctx->trans_cache->sysdicts->by_enum,
                                                                           NULL,
                                                                           temp_sysdicthis,
                                                                           typ->oid);
        }
        temp_dict = catalog_get_proc_sysdict(decodingctx->trans_cache->sysdicts->by_proc,
                                                    NULL,
                                                    temp_sysdicthis,
                                                    typ->typoutput);
        if (!temp_dict)
        {
            elog(RLOG_ERROR, "can't find pg_proc relation");
        }
        temp_proc_list = lappend(temp_proc_list, temp_dict);
    }

    /* 将临时链表附加到总链表中 */
    if (temp_class_list)
    {
        cell = NULL;
        foreach(cell, temp_class_list)
        {
            *class_list = lappend(*class_list, lfirst(cell));
        }
        list_free(temp_class_list);
    }

    if (temp_attributes_list)
    {
        cell = NULL;
        foreach(cell, temp_attributes_list)
        {
            *attributes_list = lappend(*attributes_list, lfirst(cell));
        }

        list_free(temp_attributes_list);
    }

    if (temp_namespace_list)
    {
        cell = NULL;
        foreach(cell, temp_namespace_list)
        {
            *namespace_list = lappend(*namespace_list, lfirst(cell));
        }
        list_free(temp_namespace_list);
    }

    if (temp_type_list)
    {
        cell = NULL;
        foreach(cell, temp_type_list)
        {
            *type_list = lappend(*type_list, lfirst(cell));
        }
    }

    if (temp_range_list)
    {
        cell = NULL;
        foreach(cell, temp_range_list)
        {
            *range_list = lappend(*range_list, lfirst(cell));
        }
        list_free(temp_range_list);
    }

    if (temp_enum_list)
    {
        cell = NULL;
        foreach(cell, temp_enum_list)
        {
            *enum_list = lappend(*enum_list, lfirst(cell));
        }
        list_free(temp_enum_list);
    }

    if (temp_proc_list)
    {
        cell = NULL;
        foreach(cell, temp_proc_list)
        {
            *proc_list = lappend(*proc_list, lfirst(cell));
        }
        list_free(temp_proc_list);
    }

    if (temp_type_list)
    {
        cell = NULL;
        typ = NULL;
        foreach(cell, temp_type_list)
        {
            typ = (xk_pg_parser_sysdict_pgtype*)lfirst(cell);
            if (typ->typrelid != 0 && typ->typtype == 'c')
            {
                /* 递归获取子类型系统表 */
                get_all_sysdict(decodingctx,
                                txn,
                                typ->typrelid,
                                search_his,
                                class_list,
                                namespace_list,
                                attributes_list,
                                type_list,
                                range_list,
                                enum_list,
                                proc_list,
                                typid_cache,
                                typ_cache_num);
            }
        }
        list_free(temp_type_list);
    }

}

xk_pg_parser_sysdicts *heap_get_sysdict_by_oid(void *decodingctx_in,
                                          txn *txn,
                                          Oid oid,
                                          bool search_his)
{
    decodingcontext *decodingctx = (decodingcontext *)decodingctx_in;
    xk_pg_parser_sysdicts *result = NULL;
    List *class_list = NULL,
         *attributes_list = NULL,
         *namespace_list = NULL,
         *type_list = NULL,
         *range_list = NULL,
         *enum_list = NULL,
         *proc_list = NULL;
    ListCell *cell = NULL;

    /* 使用一个数组做缓存 */
    Oid typid_cache[128] = {'\0'};

    int i = 0,
        typ_cache_num = 0;

    result = rmalloc0(sizeof(xk_pg_parser_sysdicts));
    rmemset0(result, 0, 0, sizeof(xk_pg_parser_sysdicts));

    get_all_sysdict(decodingctx,
                    txn,
                    oid,
                    search_his,
                    &class_list,
                    &namespace_list,
                    &attributes_list,
                    &type_list,
                    &range_list,
                    &enum_list,
                    &proc_list,
                    typid_cache,
                    &typ_cache_num);

    /* 组装结构体 */
    if (class_list)
    {
        result->m_pg_class.m_count = class_list->length;
        result->m_pg_class.m_pg_class = rmalloc0(sizeof(xk_pg_parser_sysdict_pgclass)
                                                       * class_list->length);
        rmemset0(result->m_pg_class.m_pg_class, 0, 0, sizeof(xk_pg_parser_sysdict_pgclass) * class_list->length);
        cell = NULL;
        i = 0;
        foreach(cell, class_list)
            rmemcpy1(&result->m_pg_class.m_pg_class[i++], 0, lfirst(cell),
                                                      sizeof(xk_pg_parser_sysdict_pgclass));
        list_free(class_list);
    }

    if (attributes_list)
    {
        result->m_pg_attribute.m_count = attributes_list->length;
        result->m_pg_attribute.m_pg_attributes = rmalloc0(sizeof(xk_pg_parser_sysdict_pgattributes)
                                                       * attributes_list->length);
        rmemset0(result->m_pg_attribute.m_pg_attributes, 0, 0, sizeof(xk_pg_parser_sysdict_pgattributes)
                                                       * attributes_list->length);
        cell = NULL;
        i = 0;
        foreach(cell, attributes_list)
            rmemcpy1(&result->m_pg_attribute.m_pg_attributes[i++], 0, lfirst(cell),
                                                      sizeof(xk_pg_parser_sysdict_pgattributes));
        list_free(attributes_list);
    }

    if (namespace_list)
    {
        result->m_pg_namespace.m_count = namespace_list->length;
        result->m_pg_namespace.m_pg_namespace = rmalloc0(sizeof(xk_pg_parser_sysdict_pgnamespace)
                                                       * namespace_list->length);
        rmemset0(result->m_pg_namespace.m_pg_namespace, 0, 0, sizeof(xk_pg_parser_sysdict_pgnamespace)
                                                       * namespace_list->length);
        cell = NULL;
        i = 0;
        foreach(cell, namespace_list)
            rmemcpy1(&result->m_pg_namespace.m_pg_namespace[i++], 0, lfirst(cell),
                                                      sizeof(xk_pg_parser_sysdict_pgnamespace));
        list_free(namespace_list);
    }

    if (type_list)
    {
        result->m_pg_type.m_count = type_list->length;
        result->m_pg_type.m_pg_type = rmalloc0(sizeof(xk_pg_parser_sysdict_pgtype)
                                                       * type_list->length);
        rmemset0(result->m_pg_type.m_pg_type, 0, 0, sizeof(xk_pg_parser_sysdict_pgtype)
                                                       * type_list->length);
        cell = NULL;
        i = 0;
        foreach(cell, type_list)
            rmemcpy1(&result->m_pg_type.m_pg_type[i++], 0, lfirst(cell),
                                                      sizeof(xk_pg_parser_sysdict_pgtype));
        list_free(type_list);
    }

    if (range_list)
    {
        result->m_pg_range.m_count = range_list->length;
        result->m_pg_range.m_pg_range = rmalloc0(sizeof(xk_pg_parser_sysdict_pgrange)
                                                       * range_list->length);
        rmemset0(result->m_pg_range.m_pg_range, 0, 0, sizeof(xk_pg_parser_sysdict_pgrange)
                                                       * range_list->length);
        cell = NULL;
        i = 0;
        foreach(cell, range_list)
            rmemcpy1(&result->m_pg_range.m_pg_range[i++], 0, lfirst(cell),
                                                      sizeof(xk_pg_parser_sysdict_pgrange));
        list_free(range_list);
    }

    if (enum_list)
    {
        result->m_pg_enum.m_count = enum_list->length;
        result->m_pg_enum.m_pg_enum = rmalloc0(sizeof(xk_pg_parser_sysdict_pgenum)
                                                       * enum_list->length);
        rmemset0(result->m_pg_enum.m_pg_enum, 0, 0, sizeof(xk_pg_parser_sysdict_pgenum)
                                                       * enum_list->length);
        cell = NULL;
        i = 0;
        foreach(cell, enum_list)
            rmemcpy1(&result->m_pg_enum.m_pg_enum[i++], 0, lfirst(cell),
                                                      sizeof(xk_pg_parser_sysdict_pgenum));
        list_free(enum_list);
    }

    if (proc_list)
    {
        result->m_pg_proc.m_count = proc_list->length;
        result->m_pg_proc.m_pg_proc = rmalloc0(sizeof(xk_pg_parser_sysdict_pgproc)
                                                       * proc_list->length);
        rmemset0(result->m_pg_proc.m_pg_proc, 0, 0, sizeof(xk_pg_parser_sysdict_pgproc)
                                                       * proc_list->length);
        cell = NULL;
        i = 0;
        foreach(cell, proc_list)
            rmemcpy1(&result->m_pg_proc.m_pg_proc[i++], 0, lfirst(cell),
                                                      sizeof(xk_pg_parser_sysdict_pgproc));
        list_free(proc_list);
    }

    return result;
}

static HTAB *init_toast_hash(void)
{
    HTAB *result = NULL;
    HASHCTL hashCtl = {'\0'};

    hashCtl.keysize = sizeof(Oid);
    hashCtl.entrysize = sizeof(toast_cache_entry);

    result = hash_create("TOAST_HASH",
                          128,
                         &hashCtl,
                          HASH_ELEM | HASH_BLOBS);
    return result;
}

#define CHECK_TOAST_VALUE(value) \
    do \
    { \
        if (value[0].m_info != INFO_NOTHING || value[1].m_info != INFO_NOTHING) \
            elog(RLOG_ERROR, "toast return values invalid"); \
        if (value[2].m_info != INFO_COL_IS_BYTEA) \
            elog(RLOG_ERROR, "toast return bytea values invalid"); \
    } \
    while (0)

#define TOAST_COLUMN_NUM 3

void heap_storage_external_data(txn *txn, xk_pg_parser_translog_tbcolbase *trans_return)
{
    Oid     oid = 0;
    bool    find = false;
    xk_pg_parser_translog_tbcol_values *tb_col = (xk_pg_parser_translog_tbcol_values *)trans_return;
    xk_pg_parser_translog_tbcol_value *value = NULL;
    toast_cache_entry *toast_entry = NULL;
    chunk_data *chunk_data = NULL;

    if (trans_return->m_dmltype == XK_PG_PARSER_TRANSLOG_DMLTYPE_DELETE)
        return;

    chunk_data = rmalloc0(sizeof(chunk_data));
    rmemset0(chunk_data, 0, 0, sizeof(chunk_data));

    if (tb_col->m_valueCnt != TOAST_COLUMN_NUM)
        elog(RLOG_ERROR, "toast return trans have invalid vaule count");

    if (tb_col->m_base.m_dmltype == XK_PG_PARSER_TRANSLOG_DMLTYPE_INSERT)
        value = tb_col->m_new_values;
    else if (tb_col->m_base.m_dmltype == XK_PG_PARSER_TRANSLOG_DMLTYPE_DELETE)
        value = tb_col->m_old_values;
    else
        elog(RLOG_ERROR, "toast return trans have invalid dml type");

    CHECK_TOAST_VALUE(value);

    oid = (uint32_t)atoi((char *)value[0].m_value);
    chunk_data->chunk_seq = atoi((char *)value[1].m_value);
    chunk_data->chunk_len = value[2].m_valueLen;
    chunk_data->chunk_data = rmalloc0(chunk_data->chunk_len);
    rmemset0(chunk_data->chunk_data, 0, 0, chunk_data->chunk_len);
    rmemcpy0(chunk_data->chunk_data, 0, value[2].m_value, chunk_data->chunk_len);

    if (!txn->toast_hash)
        txn->toast_hash = init_toast_hash();

    toast_entry = hash_search(txn->toast_hash, &oid, HASH_ENTER, &find);
    if (!find)
    {
        rmemset1(toast_entry, 0, 0, sizeof(toast_cache_entry));
        toast_entry->chunk_id = oid;
    }

    toast_entry->chunk_list = lappend(toast_entry->chunk_list, (void *)chunk_data);
}

static void heap_free_node_tree(xk_pg_parser_nodetree *node)
{
    xk_pg_parser_nodetree *next_node = NULL;
    xk_pg_parser_nodetree *node_tree = node;
    while (node_tree)
    {
        next_node = node_tree->m_next;
        switch (node_tree->m_node_type)
        {
            case XK_PG_PARSER_NODETYPE_CONST:
            {
                xk_pg_parser_node_const *node_const = (xk_pg_parser_node_const *)node_tree->m_node;
                if (node_const->m_char_value)
                    rfree(node_const->m_char_value);
                rfree(node_tree->m_node);
                break;
            }
            case XK_PG_PARSER_NODETYPE_FUNC:
            {
                xk_pg_parser_node_func *node_func = (xk_pg_parser_node_func *)node_tree->m_node;
                if (node_func->m_funcname)
                    rfree(node_func->m_funcname);
                rfree(node_tree->m_node);
                break;
            }
            case XK_PG_PARSER_NODETYPE_OP:
            {
                xk_pg_parser_node_op *node_op = (xk_pg_parser_node_op *)node_tree->m_node;
                if (node_op->m_opname)
                    rfree(node_op->m_opname);
                rfree(node_tree->m_node);
                break;
            }
            default:
                rfree(node_tree->m_node);
                break;
        }
        rfree(node_tree);
        node_tree = next_node;
    }
}

static void heap_free_value(xk_pg_parser_translog_tbcol_value *value)
{
    if (value)
    {
        if (value->m_value)
        {
            if (value->m_info == INFO_COL_IS_CUSTOM
            || value->m_info == INFO_COL_IS_ARRAY)
            {
                xk_pg_parser_translog_tbcol_valuetype_customer *custom = 
                    (xk_pg_parser_translog_tbcol_valuetype_customer *) value->m_value;
                while (custom)
                {
                    heap_free_value(custom->m_value);
                    rfree(custom->m_value);
                    custom = custom->m_next;
                }
                rfree(value->m_value);
            }
            else if (value->m_info == INFO_COL_IS_NODE)
                heap_free_node_tree((xk_pg_parser_nodetree*)value->m_value);
            else
                rfree(value->m_value);
        }
        if (value->m_colName)
            rfree(value->m_colName);
    }

}

static void heap_free_toast(xk_pg_parser_translog_tbcol_value *toast_col)
{
    if (toast_col)
    {
        if (toast_col->m_colName)
            rfree(toast_col->m_colName);
        if (toast_col->m_value)
            rfree(toast_col->m_value);
        rfree(toast_col);
    }
}

void heap_free_trans_result(xk_pg_parser_translog_tbcolbase *trans_return)
{
    if (trans_return)
    {
        int32_t i = 0;

        if (trans_return->m_tbname)
            rfree(trans_return->m_tbname);

        if (trans_return->m_schemaname)
            rfree(trans_return->m_schemaname);

        if (trans_return->m_dmltype == XK_PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT)
        {
            int j = 0;
            xk_pg_parser_translog_tbcol_nvalues *trans = (xk_pg_parser_translog_tbcol_nvalues *)
                                                          trans_return;
            if (trans->m_rows)
            {
                for (i = 0; i < trans->m_rowCnt; i++)
                {
                    if (trans->m_rows[i].m_new_values)
                    {
                        for (j =0; j < trans->m_valueCnt; j++)
                        {
                            heap_free_value(&trans->m_rows[i].m_new_values[j]);
                        }
                        rfree(trans->m_rows[i].m_new_values);
                    }
                }
                rfree(trans->m_rows);
            }

            if (trans->m_tuple)
            {
                for (i = 0; i < (int32_t) trans->m_tupleCnt; i++)
                    rfree(trans->m_tuple[i].m_tupledata);
                rfree(trans->m_tuple);
            }
        }
        else
        {
            xk_pg_parser_translog_tbcol_values *trans = (xk_pg_parser_translog_tbcol_values *)
                                                         trans_return;
            if (trans->m_valueCnt > 0)
            {
                for (i = 0; i < trans->m_valueCnt; i++)
                {
                    if (trans->m_new_values)
                        heap_free_value(&trans->m_new_values[i]);
                    if (trans->m_old_values)
                        heap_free_value(&trans->m_old_values[i]);
                }
            }
            if (trans->m_new_values)
                rfree(trans->m_new_values);

            if (trans->m_old_values)
                rfree(trans->m_old_values);

            if (trans->m_tuple)
            {
                for (i = 0; i < (int32_t) trans->m_tupleCnt; i++)
                    rfree(trans->m_tuple[i].m_tupledata);
                rfree(trans->m_tuple);
            }
        }
        rfree(trans_return);
    }
}

void heap_free_trans_pre(xk_pg_parser_translog_translog2col *trans_data)
{
    if (trans_data)
    {
        /* 释放保存convert的指针 */
        if (trans_data->m_convert)
            rfree(trans_data->m_convert);

        /* 释放拷贝的系统表 */
        if (trans_data->m_sysdicts)
        {
            if (trans_data->m_sysdicts->m_pg_class.m_pg_class)
                rfree(trans_data->m_sysdicts->m_pg_class.m_pg_class);

            if (trans_data->m_sysdicts->m_pg_attribute.m_pg_attributes)
                rfree(trans_data->m_sysdicts->m_pg_attribute.m_pg_attributes);

            if (trans_data->m_sysdicts->m_pg_namespace.m_pg_namespace)
                rfree(trans_data->m_sysdicts->m_pg_namespace.m_pg_namespace);

            if (trans_data->m_sysdicts->m_pg_type.m_pg_type)
                rfree(trans_data->m_sysdicts->m_pg_type.m_pg_type);

            if (trans_data->m_sysdicts->m_pg_range.m_pg_range)
                rfree(trans_data->m_sysdicts->m_pg_range.m_pg_range);

            if (trans_data->m_sysdicts->m_pg_enum.m_pg_enum)
                rfree(trans_data->m_sysdicts->m_pg_enum.m_pg_enum);

            if (trans_data->m_sysdicts->m_pg_proc.m_pg_proc)
                rfree(trans_data->m_sysdicts->m_pg_proc.m_pg_proc);

            rfree(trans_data->m_sysdicts);
        }

        /* 释放拷贝的tuple缓存 */
        if (trans_data->m_tuples)
        {
            if (trans_data->m_tuples->m_tupledata)
                rfree(trans_data->m_tuples->m_tupledata);
            
            rfree(trans_data->m_tuples);
        }
        rfree(trans_data);
    }
}

static char *heap_get_output_name(decodingcontext* decodingctx, Oid oid)
{
    catalog_type_value *pgtype_v = NULL;
    catalog_proc_value   *pgproc_v = NULL;

    Oid typout = InvalidOid;
    bool find = false;

    pgtype_v = hash_search(decodingctx->trans_cache->sysdicts->by_type, &oid, HASH_FIND, &find);
    if (!find)
        elog(RLOG_ERROR, "can't find pg_type record by oid: %u", oid);

    typout = pgtype_v->type->typoutput;

    pgproc_v = hash_search(decodingctx->trans_cache->sysdicts->by_proc, &typout, HASH_FIND, &find);
    if (!find)
        elog(RLOG_ERROR, "can't find pg_proc record by oid: %u", typout);

    return rstrdup(pgproc_v->proc->proname.data);
}

static bool check_high_version(int dbtype, char *dbversion)
{
    bool result = false;
    switch (dbtype)
    {
        case XK_DATABASE_TYPE_POSTGRESQL:
        {
            if (!strcmp(XK_DATABASE_PG1410, dbversion))
            {
                result = true;
            }
            break;
        }
        default:
        {
            break;
        }
    }
    return result;
}

static char *heap_get_external_data(txn *txn,
                                    uint32_t chunkid,
                                    uint32_t *chunk_len,
                                    xk_pg_parser_translog_tbcol_valuetype_external *ext,
                                    int dbtype,
                                    char *dbversion)
{
    int i = 0;
    ListCell *cell = NULL;
    char *result = NULL;
    char *point;

    int len = 0;
    bool find = false;
    toast_cache_entry *toast_entry = NULL;

    if (!txn->toast_hash)
        return NULL;

    toast_entry = hash_search(txn->toast_hash, &chunkid, HASH_FIND, &find);

    /* 如果没有找到, 这是正常的, 返回NULL */
    if (!find)
        return NULL;

    /* 是顺序放好的, 首先遍历一遍获取长度信息 */
    foreach(cell, toast_entry->chunk_list)
    {
        chunk_data *chunk = (chunk_data *)lfirst(cell);
        len += XK_PG_PARSER_VARSIZE_ANY_EXHDR(chunk->chunk_data);
    }
    len += VARHDRSZ;

    result = rmalloc0(len);
    rmemset0(result, 0, 0, len);
    *chunk_len = len;
    point = result;

    i = 0;
    foreach(cell, toast_entry->chunk_list)
    {
        chunk_data *chunk = (chunk_data *)lfirst(cell);

        /* 第一次, 组装头部信息 */
        if (i == 0)
        {
            bool high_version = false;

            high_version = check_high_version(dbtype, dbversion);
            if (high_version)
            {
                if ((((ext)->m_extsize & ((1U << 30) - 1)) < (ext)->m_rawsize - ((int32_t) sizeof(int32_t))))
                {
                    XK_PG_PARSER_SET_VARSIZE_COMPRESSED(result, len);
                }
                else
                {
                    XK_PG_PARSER_SET_VARSIZE(result, len);
                }
            }
            else
            {
                if (XK_PG_PARSER_VARATT_EXTERNAL_IS_COMPRESSED(ext))
                    XK_PG_PARSER_SET_VARSIZE_COMPRESSED(result, len);
                else
                    XK_PG_PARSER_SET_VARSIZE(result, len);
            }
            point += VARHDRSZ;
            rmemcpy1(point, 0, chunk->chunk_data + (XK_PG_PARSER_VARSIZE_ANY(chunk->chunk_data) 
                   - XK_PG_PARSER_VARSIZE_ANY_EXHDR(chunk->chunk_data)), 
                   XK_PG_PARSER_VARSIZE_ANY_EXHDR(chunk->chunk_data));
            point += XK_PG_PARSER_VARSIZE_ANY_EXHDR(chunk->chunk_data);
        }

        else
        {
            rmemcpy1(point, 0, chunk->chunk_data + (XK_PG_PARSER_VARSIZE_ANY(chunk->chunk_data) 
                   - XK_PG_PARSER_VARSIZE_ANY_EXHDR(chunk->chunk_data)), 
                   XK_PG_PARSER_VARSIZE_ANY_EXHDR(chunk->chunk_data));
            point += XK_PG_PARSER_VARSIZE_ANY_EXHDR(chunk->chunk_data);
        }
        i++;
    }

    /* 我们已经将toast数据拼装完了, 删除缓存 */
    foreach(cell, toast_entry->chunk_list)
    {
        chunk_data *chunk = (chunk_data *)lfirst(cell);
        if (chunk->chunk_data)
            rfree(chunk->chunk_data);
        rfree(chunk);
    }
    list_free(toast_entry->chunk_list);
    toast_entry->chunk_list = NULL;

    hash_search(txn->toast_hash, &chunkid, HASH_REMOVE, &find);

    return result;
}

static xk_pg_parser_translog_tbcol_value *heap_get_detoast_column(decodingcontext* decodingctx,
                                                                  txn *txn,
                                                                  xk_pg_parser_translog_tbcol_value *col)
{
    int ereno = 0;
    xk_pg_parser_translog_tbcol_value *result = NULL;
    xk_pg_parser_translog_convertinfo convert = {'\0'};
    xk_pg_parser_translog_external xk_pg_parser_exdata = {'\0'};
    xk_pg_parser_translog_tbcol_valuetype_external *ext = (xk_pg_parser_translog_tbcol_valuetype_external *)col->m_value;

    /* 入参准备 */
    convert.m_dbcharset = decodingctx->orgdbcharset;
    convert.m_tartgetcharset = decodingctx->tgtdbcharset;
    convert.m_tzname = decodingctx->tzname;
    convert.m_monetary = decodingctx->monetary;
    convert.m_numeric = decodingctx->numeric;

    xk_pg_parser_exdata.m_colName = col->m_colName;
    xk_pg_parser_exdata.m_dbtype = decodingctx->walpre.m_dbtype;
    xk_pg_parser_exdata.m_dbversion = decodingctx->walpre.m_dbversion;
    xk_pg_parser_exdata.m_convertInfo = &convert;
    xk_pg_parser_exdata.m_typeid = col->m_coltype;

    xk_pg_parser_exdata.m_typout = heap_get_output_name(decodingctx, xk_pg_parser_exdata.m_typeid);
    xk_pg_parser_exdata.m_chunkdata = heap_get_external_data(txn,
                                                             ext->m_valueid,
                                                             &xk_pg_parser_exdata.m_datalen,
                                                             ext,
                                                             xk_pg_parser_exdata.m_dbtype,
                                                             xk_pg_parser_exdata.m_dbversion);

    if (!xk_pg_parser_exdata.m_chunkdata)
        return NULL;

    if (!xk_pg_parser_trans_external_trans(&xk_pg_parser_exdata, &result, &ereno))
        elog(RLOG_ERROR, "failed detoast external , errcode: %x, msg: %s",
                          ereno,
                          xk_pg_parser_errno_getErrInfo(ereno));

    /* 行外存储解析完成, 清理入参 */
    rfree(xk_pg_parser_exdata.m_chunkdata);
    rfree(xk_pg_parser_exdata.m_typout);

    return result;
}

static void heap_assemble_insert(decodingcontext* decodingctx,
                                 txn *txn,
                                 xk_pg_parser_translog_tbcol_values *tbcol_values)
{
    StringInfo result_str = makeStringInfo();
    StringInfo column_name_str = makeStringInfo();
    StringInfo column_value_str = makeStringInfo();

    txnstmt *stmt = rmalloc0(sizeof(txnstmt));

    int column_index = 0;
    uint32_t stmt_len = 0;
    bool need_comma = false;

    rmemset0(stmt, 0, 0, sizeof(txnstmt));
    stmt->type = TXNSTMT_TYPE_DML;
    stmt->start.wal.lsn = decodingctx->decode_record->start.wal.lsn;
    stmt->end.wal.lsn = decodingctx->decode_record->end.wal.lsn;

    appendStringInfo(result_str, "INSERT INTO \"%s\".\"%s\" ",
                                  tbcol_values->m_base.m_schemaname,
                                  tbcol_values->m_base.m_tbname);

    /* 处理列 */
    for (column_index = 0; column_index < tbcol_values->m_valueCnt; column_index++)
    {
        xk_pg_parser_translog_tbcol_value *col = &tbcol_values->m_new_values[column_index];
        bool skip = false;

        /* 当列被drop或可能为空时, 不设置该列的值 */
        if (col->m_info == INFO_COL_MAY_NULL || col->m_info == INFO_COL_IS_DROPED)
            skip = true;

        if (!skip)
        {
            if (col->m_info == INFO_COL_IS_NULL)
            {
                if (need_comma)
                {
                    appendStringInfo(column_name_str, ", ");
                    appendStringInfo(column_value_str, ", ");
                }
                appendStringInfo(column_name_str, "\"%s\"", col->m_colName);
                appendStringInfo(column_value_str, "NULL");
                need_comma = true;
            }
            else if (col->m_info == INFO_COL_IS_TOAST)
            {
                char *temp_str = NULL;
                xk_pg_parser_translog_tbcol_value *toast_col = NULL;

                toast_col = heap_get_detoast_column(decodingctx, txn, col);

                if (!toast_col)
                {
                    elog(RLOG_WARNING, "decode heap insert, toast data missing");
                    continue;
                }

                if (need_comma)
                {
                    appendStringInfo(column_name_str, ", ");
                    appendStringInfo(column_value_str, ", ");
                }

                appendStringInfo(column_name_str, "\"%s\"", col->m_colName);

                temp_str = strSpecialCharReplace((char *)toast_col->m_value);
                if (temp_str)
                {
                    appendStringInfo(column_value_str, "'%s'", temp_str);
                    rfree(temp_str);
                }
                else
                    appendStringInfo(column_value_str, "'%s'", (char *)toast_col->m_value);
                heap_free_toast(toast_col);
                need_comma = true;
            }
            else if (col->m_info == INFO_NOTHING)
            {
                char *temp_str = strSpecialCharReplace((char *)col->m_value);

                if (need_comma)
                {
                    appendStringInfo(column_name_str, ", ");
                    appendStringInfo(column_value_str, ", ");
                }

                appendStringInfo(column_name_str, "\"%s\"", col->m_colName);
                if (temp_str)
                {
                    appendStringInfo(column_value_str, "'%s'", temp_str);
                    rfree(temp_str);
                }
                else
                    appendStringInfo(column_value_str, "'%s'", (char *)col->m_value);
                need_comma = true;
            }
            else
            {
                elog(RLOG_WARNING, "decode heap insert, column value type unsupport");
            }
        }
    }
    appendStringInfo(result_str, "(%s) VALUES(%s);", column_name_str->data, column_value_str->data);

    stmt->len = strlen(result_str->data) + 1;
    stmt->stmt = rmalloc0(stmt->len);
    rmemset0(stmt->stmt, 0, 0, stmt->len);
    rmemcpy0(stmt->stmt, 0, result_str->data, stmt->len - 1);

    txn->stmtsize += stmt->len;
    decodingctx->trans_cache->totalsize += stmt->len;

    deleteStringInfo(result_str);
    deleteStringInfo(column_name_str);
    deleteStringInfo(column_value_str);

    txn->stmts = lappend(txn->stmts, (void*)stmt);

    stmt_len = strlen(stmt->stmt) + 1;
    if (stmt_len > 1000)
        elog(RLOG_DEBUG, "dml trans length:%u, result: stmt length > 1000, undisplay", stmt_len);
    else
        elog(RLOG_DEBUG, "dml trans length:%u, result: %s", stmt_len, stmt->stmt);
}

static bool check_have_pkey(decodingcontext* decodingctx, txn *txn, Oid oid)
{
    HTAB *class_htab = decodingctx->trans_cache->sysdicts->by_class;
    xk_pg_sysdict_Form_pg_class class = NULL;

    class = (xk_pg_sysdict_Form_pg_class) catalog_get_class_sysdict(class_htab,
                                                                           NULL,
                                                                           txn->sysdictHis,
                                                                           oid);
    return class->relhaspk;
}

static void heap_assemble_delete(decodingcontext* decodingctx,
                                 txn *txn,
                                 xk_pg_parser_translog_tbcol_values *tbcol_values,
                                 Oid oid)
{
    StringInfo result_str = makeStringInfo();
    StringInfo where_str = makeStringInfo();

    txnstmt *stmt = rmalloc0(sizeof(txnstmt));

    int column_index = 0;
    bool have_data = false;
    bool need_and = false;

    rmemset0(stmt, 0, 0, sizeof(txnstmt));
    stmt->type = TXNSTMT_TYPE_DML;
    stmt->start.wal.lsn = decodingctx->decode_record->start.wal.lsn;
    stmt->end.wal.lsn = decodingctx->decode_record->end.wal.lsn;

    appendStringInfo(result_str, "DELETE FROM \"%s\".\"%s\" WHERE ",
                                  tbcol_values->m_base.m_schemaname,
                                  tbcol_values->m_base.m_tbname);

    /* 处理列 */
    for (column_index = 0; column_index < tbcol_values->m_valueCnt; column_index++)
    {
        xk_pg_parser_translog_tbcol_value *col = &tbcol_values->m_old_values[column_index];
        bool skip = false;

        /* 当列被drop或可能为空时, 不设置该列的值 */
        if (col->m_info == INFO_COL_MAY_NULL || col->m_info == INFO_COL_IS_DROPED
         || col->m_info == INFO_COL_IS_TOAST)
            skip = true;

        if (!skip)
        {
            if (col->m_info == INFO_COL_IS_NULL)
            {
                have_data = true;
                if (need_and)
                    appendStringInfo(where_str, "AND ");
                appendStringInfo(where_str, "\"%s\" = ", col->m_colName);
                appendStringInfo(where_str, "NULL ");
                need_and = true;
            }
            else if (col->m_info == INFO_NOTHING)
            {
                char *temp_str = strSpecialCharReplace((char *)col->m_value);
                have_data = true;
                if (need_and)
                    appendStringInfo(where_str, "AND ");
                appendStringInfo(where_str, "\"%s\" = ", col->m_colName);
                if (temp_str)
                {
                    appendStringInfo(where_str, "'%s' ", temp_str);
                    rfree(temp_str);
                }
                else
                    appendStringInfo(where_str, "'%s' ", (char *)col->m_value);

                need_and = true;
            }
            else
            {
                elog(RLOG_WARNING, "decode heap delete, column value type unsupport");
            }
        }
    }
    if (check_have_pkey(decodingctx, txn, oid))
    {
        appendStringInfo(result_str, "%s;", where_str->data);
    }
    else
    {
        appendStringInfo(result_str, "CTID = (SELECT CTID FROM \"%s\".\"%s\" WHERE %s LIMIT 1);", 
                                      tbcol_values->m_base.m_schemaname,
                                      tbcol_values->m_base.m_tbname,
                                      where_str->data);
    }

    stmt->len = strlen(result_str->data) + 1;
    stmt->stmt = rmalloc0(stmt->len);
    rmemset0(stmt->stmt, 0, 0, stmt->len);
    rmemcpy0(stmt->stmt, 0, result_str->data, stmt->len - 1);
    deleteStringInfo(result_str);
    deleteStringInfo(where_str);

    /* 不存在旧值, 警告 */
    if (!have_data)
    {
        elog(RLOG_WARNING, "decode heap delete, all of old values missing, ignore");
        rfree(stmt->stmt);
        rfree(stmt);
    }
    else
    {
        uint32_t stmt_len = 0;
        //txn->stmtsize += 20;
        txn->stmtsize += stmt->len;
        decodingctx->trans_cache->totalsize += stmt->len;
        txn->stmts = lappend(txn->stmts, (void*)stmt);
        stmt_len = strlen(stmt->stmt) + 1;
        if (stmt_len > 1000)
            elog(RLOG_DEBUG, "dml trans length:%u, result: stmt length > 1000, undisplay", stmt_len);
        else
            elog(RLOG_DEBUG, "dml trans length:%u, result: %s", stmt_len, stmt->stmt);
    }
}

static void heap_assemble_update(decodingcontext* decodingctx,
                                 txn *txn,
                                 xk_pg_parser_translog_tbcol_values *tbcol_values,
                                 Oid oid)
{
    StringInfo result_str = makeStringInfo();
    StringInfo column_new_str = makeStringInfo();
    StringInfo column_old_str = makeStringInfo();

    txnstmt *stmt = rmalloc0(sizeof(txnstmt));

    int column_index = 0;
    bool have_data = false;
    bool need_sep = false;
    bool need_and = false;

    rmemset0(stmt, 0, 0, sizeof(txnstmt));
    stmt->type = TXNSTMT_TYPE_DML;
    stmt->start.wal.lsn = decodingctx->decode_record->start.wal.lsn;
    stmt->end.wal.lsn = decodingctx->decode_record->end.wal.lsn;

    appendStringInfo(result_str, "UPDATE \"%s\".\"%s\" SET ",
                                  tbcol_values->m_base.m_schemaname,
                                  tbcol_values->m_base.m_tbname);

    /* 处理列 */
    for (column_index = 0; column_index < tbcol_values->m_valueCnt; column_index++)
    {
        xk_pg_parser_translog_tbcol_value *col_new = &tbcol_values->m_new_values[column_index];
        xk_pg_parser_translog_tbcol_value *col_old = &tbcol_values->m_old_values[column_index];

        bool skip_new = false;
        bool skip_old = false;

        /* 当列被drop或可能为空时, 不设置该列的值 */
        if (col_new->m_info == INFO_COL_MAY_NULL || col_new->m_info == INFO_COL_IS_DROPED)
            skip_new = true;

        if (col_new->m_info == INFO_COL_IS_TOAST)
        {
            char *temp_str = NULL;
            xk_pg_parser_translog_tbcol_value *toast_col = NULL;
            toast_col = heap_get_detoast_column(decodingctx, txn, col_new);

            if (!toast_col)
            {
                elog(RLOG_WARNING, "decode heap update, toast data missing, ignore");
            }
            skip_new = true;
            if (need_sep)
                    appendStringInfo(column_new_str, ", ");
            appendStringInfo(column_new_str, "\"%s\" = ", col_new->m_colName);
            temp_str = strSpecialCharReplace((char *)toast_col->m_value);
            if (temp_str)
            {
                appendStringInfo(column_new_str, "'%s'", temp_str);
                rfree(temp_str);
            }
            else
                appendStringInfo(column_new_str, "'%s'", (char *)toast_col->m_value);
            heap_free_toast(toast_col);
            need_sep = true;
        }

        if (!skip_new)
        {
            if (col_new->m_info == INFO_COL_IS_NULL)
            {
                if (need_sep)
                    appendStringInfo(column_new_str, ", ");
                appendStringInfo(column_new_str, "\"%s\" = ", col_new->m_colName);
                appendStringInfo(column_new_str, "NULL");
                need_sep = true;
            }
            else if (col_new->m_info == INFO_NOTHING)
            {
                char *temp_str = strSpecialCharReplace((char *)col_new->m_value);
                if (need_sep)
                    appendStringInfo(column_new_str, ", ");
                appendStringInfo(column_new_str, "\"%s\" = ", col_new->m_colName);
                if (temp_str)
                {
                    appendStringInfo(column_new_str, "'%s'", temp_str);
                    rfree(temp_str);
                }
                else
                    appendStringInfo(column_new_str, "'%s'", (char *)col_new->m_value);

                need_sep = true;
            }
            else
            {
                elog(RLOG_WARNING, "decode heap update, column value type unsupport");
            }
        }

        /* 当列被drop或可能为空时, 不设置该列的值 */
        if (col_old->m_info == INFO_COL_MAY_NULL || col_old->m_info == INFO_COL_IS_DROPED
            || col_old->m_info == INFO_COL_IS_TOAST)
            skip_old = true;

        if (!skip_old)
        {

            if (col_old->m_info == INFO_COL_IS_NULL)
            {
                have_data = true;
                if (need_and)
                    appendStringInfo(column_old_str, " AND ");
                appendStringInfo(column_old_str, "\"%s\" = ", col_old->m_colName);
                appendStringInfo(column_old_str, "NULL");
                need_and = true;
            }
            else if (col_old->m_info == INFO_NOTHING)
            {
                char *temp_str = strSpecialCharReplace((char *)col_old->m_value);
                have_data = true;
                if (need_and)
                    appendStringInfo(column_old_str, " AND ");
                appendStringInfo(column_old_str, "\"%s\" = ", col_old->m_colName);
                if (temp_str)
                {
                    appendStringInfo(column_old_str, "'%s'", temp_str);
                    rfree(temp_str);
                }
                else
                    appendStringInfo(column_old_str, "'%s'", (char *)col_old->m_value);

                need_and = true;
            }
            else
            {
                elog(RLOG_WARNING, "decode heap update, column value type unsupport");
            }
        }
    }
    appendStringInfo(result_str, "%s WHERE ", column_new_str->data);
    if (check_have_pkey(decodingctx, txn, oid))
    {
        appendStringInfo(result_str, "%s;", column_old_str->data);
    }
    else
    {
        appendStringInfo(result_str, "CTID = (SELECT CTID FROM \"%s\".\"%s\" WHERE %s LIMIT 1);", 
                                      tbcol_values->m_base.m_schemaname,
                                      tbcol_values->m_base.m_tbname,
                                      column_old_str->data);
    }

    stmt->len = strlen(result_str->data) + 1;
    stmt->stmt = rmalloc0(stmt->len);
    rmemset0(stmt->stmt, 0, 0, stmt->len);
    rmemcpy0(stmt->stmt, 0, result_str->data, stmt->len - 1);

    deleteStringInfo(result_str);
    deleteStringInfo(column_new_str);
    deleteStringInfo(column_old_str);

    /* 不存在旧值, 警告 */
    if (!have_data)
    {
        //elog(RLOG_WARNING, "decode heap update, all of old values missing, ignore");
        rfree(stmt->stmt);
        rfree(stmt);
    }
    else
    {
        uint32_t stmt_len = 0;
        //txn->stmtsize += 20;
        txn->stmtsize += stmt->len;
        decodingctx->trans_cache->totalsize += stmt->len;
        txn->stmts = lappend(txn->stmts, (void*)stmt);
        stmt_len = strlen(stmt->stmt) + 1;
        if (stmt_len > 1000)
            elog(RLOG_DEBUG, "dml trans length:%u, result: stmt length > 1000, undisplay", stmt_len);
        else
            elog(RLOG_DEBUG, "dml trans length:%u, result: %s", stmt_len, stmt->stmt);
    }
}

static void heap_assemble_multi_insert(decodingcontext* decodingctx,
                                       txn *txn,
                                       xk_pg_parser_translog_tbcol_nvalues *tbcol_nvalues)
{
    StringInfo result_str = NULL;
    StringInfo column_name_str = NULL;
    StringInfo column_value_str = NULL;

    txnstmt *stmt = NULL;

    int column_index = 0;
    int row_index = 0;

    for (row_index = 0; row_index < tbcol_nvalues->m_rowCnt; row_index++)
    {
        uint32_t stmt_len = 0;
        result_str = makeStringInfo();
        column_name_str = makeStringInfo();
        column_value_str = makeStringInfo();

        stmt = rmalloc0(sizeof(txnstmt));
        rmemset0(stmt, 0, 0, sizeof(txnstmt));
        stmt->type = TXNSTMT_TYPE_DML;
        stmt->start.wal.lsn = decodingctx->decode_record->start.wal.lsn;
        stmt->end.wal.lsn = decodingctx->decode_record->end.wal.lsn;

        appendStringInfo(result_str, "INSERT INTO \"%s\".\"%s\" ",
                                    tbcol_nvalues->m_base.m_schemaname,
                                    tbcol_nvalues->m_base.m_tbname);

        /* 处理列 */
        for (column_index = 0; column_index < tbcol_nvalues->m_valueCnt; column_index++)
        {
            xk_pg_parser_translog_tbcol_value *col = &tbcol_nvalues->m_rows[row_index].m_new_values[column_index];
            bool skip = false;

            /* 当列被drop或可能为空时, 不设置该列的值 */
            if (col->m_info == INFO_COL_MAY_NULL || col->m_info == INFO_COL_IS_DROPED)
                skip = true;

            if (!skip)
            {
                appendStringInfo(column_name_str, "\"%s\"", col->m_colName);

                if (col->m_info == INFO_COL_IS_NULL)
                {
                    appendStringInfo(column_value_str, "NULL");
                }
                else if (col->m_info == INFO_COL_IS_TOAST)
                {
                    char *temp_str = NULL;
                    xk_pg_parser_translog_tbcol_value *toast_col = NULL;
                    toast_col = heap_get_detoast_column(decodingctx, txn, col);

                    if (!toast_col)
                        elog(RLOG_ERROR, "decode heap2 multi insert, toast data missing");

                    temp_str = strSpecialCharReplace((char *)toast_col->m_value);
                    if (temp_str)
                    {
                        appendStringInfo(column_value_str, "'%s'", temp_str);
                        rfree(temp_str);
                    }
                    else
                        appendStringInfo(column_value_str, "'%s'", (char *)toast_col->m_value);

                    heap_free_toast(toast_col);
                }
                else if (col->m_info == INFO_NOTHING)
                {
                    char *temp_str = strSpecialCharReplace((char *)col->m_value);
                    if (temp_str)
                    {
                        appendStringInfo(column_value_str, "'%s'", temp_str);
                        rfree(temp_str);
                    }
                    else
                        appendStringInfo(column_value_str, "'%s'", (char *)col->m_value);

                }
                else
                {
                    elog(RLOG_WARNING, "decode heap2 multi insert, column value type unsupport");
                    appendStringInfo(column_value_str, "NULL");
                }

                /* 解析toast时可能将skip改为真 */
                if (column_index < tbcol_nvalues->m_valueCnt - 1 && !skip)
                {
                    appendStringInfo(column_name_str, ", ");
                    appendStringInfo(column_value_str, ", ");
                }
            }
        }
        appendStringInfo(result_str, "(%s) VALUES(%s);", column_name_str->data, column_value_str->data);

        stmt->len = strlen(result_str->data) + 1;
        stmt->stmt = rmalloc0(stmt->len);
        rmemset0(stmt->stmt, 0, 0, stmt->len);
        rmemcpy0(stmt->stmt, 0, result_str->data, stmt->len - 1);

        deleteStringInfo(result_str);
        deleteStringInfo(column_name_str);
        deleteStringInfo(column_value_str);
        //txn->stmtsize += 20;
        txn->stmtsize += stmt->len;
        decodingctx->trans_cache->totalsize += stmt->len;
        txn->stmts = lappend(txn->stmts, (void*)stmt);

        stmt_len = strlen(stmt->stmt) + 1;
        if (stmt_len > 1000)
            elog(RLOG_DEBUG, "dml trans length:%u, result: stmt length > 1000, undisplay", stmt_len);
        else
            elog(RLOG_DEBUG, "dml trans length:%u, result: %s", stmt_len, stmt->stmt);
    }
}

void heap_parser2sql(void* decodingctx_in,
                     txn *txn,
                     xk_pg_parser_translog_tbcolbase *trans_return,
                     Oid oid)
{
    decodingcontext* decodingctx = (decodingcontext*) decodingctx_in;
    switch (trans_return->m_dmltype)
    {
        case XK_PG_PARSER_TRANSLOG_DMLTYPE_INSERT:
        {
            xk_pg_parser_translog_tbcol_values *insert = 
                (xk_pg_parser_translog_tbcol_values *) trans_return;
            heap_assemble_insert(decodingctx, txn, insert);
            break;
        }
        case XK_PG_PARSER_TRANSLOG_DMLTYPE_DELETE:
        {
            xk_pg_parser_translog_tbcol_values *delete = 
                (xk_pg_parser_translog_tbcol_values *) trans_return;
            heap_assemble_delete(decodingctx, txn, delete, oid);
            break;
        }
        case XK_PG_PARSER_TRANSLOG_DMLTYPE_UPDATE:
        {
            xk_pg_parser_translog_tbcol_values *update = 
                (xk_pg_parser_translog_tbcol_values *) trans_return;
            heap_assemble_update(decodingctx, txn, update, oid);
            break;
        }
        case XK_PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT:
        {
            xk_pg_parser_translog_tbcol_nvalues *minsert = 
                (xk_pg_parser_translog_tbcol_nvalues *) trans_return;
            heap_assemble_multi_insert(decodingctx, txn, minsert);
            break;
        }
        default:
            elog(RLOG_ERROR, "decode heap: invalid dml type");
            break;
    }
}

HTAB *init_oidmap_hash(void)
{
    HTAB *result = NULL;
    HASHCTL hashCtl = {'\0'};

    hashCtl.keysize = sizeof(Oid);
    hashCtl.entrysize = sizeof(oidmap_entry);

    result = hash_create("OID_MAP_HASH",
                          128,
                         &hashCtl,
                          HASH_ELEM | HASH_BLOBS);
    return result;
}

void add_oidmap(HTAB *htab, Oid temp, Oid real)
{
    oidmap_entry *oidmap = NULL;
    bool find = false;
    oidmap = hash_search(htab, &temp, HASH_ENTER, &find);

    oidmap->temp_oid = temp;
    oidmap->real_oid = real;
}

Oid get_real_oid_from_oidmap(HTAB *htab, Oid temp)
{
    oidmap_entry *oidmap = NULL;
    bool find = false;
    oidmap = hash_search(htab, &temp, HASH_FIND, &find);
    if (find)
        return oidmap->real_oid;
    else
        return InvalidOid;
}

void heap_parser_count_size(void* decodingctx_in,
                            txn *txn,
                            xk_pg_parser_translog_tbcolbase *trans_return,
                            Oid oid)
{
    decodingcontext* decodingctx = (decodingcontext*)decodingctx_in;
    bool deal_toast = false;
    txnstmt *stmt = rmalloc0(sizeof(txnstmt));
    rmemset0(stmt, 0, 0, sizeof(txnstmt));
    stmt->type = TXNSTMT_TYPE_DML;
    stmt->start.wal.lsn = decodingctx->decode_record->start.wal.lsn;
    stmt->end.wal.lsn = decodingctx->decode_record->end.wal.lsn;
    stmt->extra0.wal.lsn = decodingctx->decode_record->end.wal.lsn;
    stmt->len = 0;

    /* multi instert 单独处理 */
    if (trans_return->m_dmltype == XK_PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT)
    {
        xk_pg_parser_translog_tbcol_nvalues *minsert =
            (xk_pg_parser_translog_tbcol_nvalues*) trans_return;
        int index_row = 0;
        int index_column = 0;

        for (index_row = 0; index_row < minsert->m_rowCnt; index_row++)
        {
            for (index_column = 0; index_column < minsert->m_valueCnt; index_column++)
            {
                xk_pg_parser_translog_tbcol_value *column =
                        &(minsert->m_rows[index_row].m_new_values[index_column]);
                /* 只处理toast, 将toast的值解析出来 */
                if (column->m_info == INFO_COL_IS_TOAST)
                {
                    xk_pg_parser_translog_tbcol_value *toast_col = NULL;
                    deal_toast = true;
                    toast_col = heap_get_detoast_column(decodingctx, txn, column);
                    if (!toast_col)
                        elog(RLOG_ERROR, "heap2 multi insert, toast data missing");
                    if (toast_col->m_info != INFO_NOTHING)
                    {
                        elog(RLOG_WARNING, "heap2 multi insert, when decode detoast data, unsupport data type");
                        toast_col->m_info = INFO_COL_MAY_NULL;

                        /* 在这里处理掉toast解析出来的值 */
                        if (toast_col->m_value)
                        {
                            rfree(toast_col->m_value);
                            toast_col->m_valueLen = 0;
                            toast_col->m_value = NULL;
                        }
                    }
                    column->m_info = toast_col->m_info;
                    if (column->m_value)
                    {
                        rfree(column->m_value);
                        column->m_value = NULL;
                    }

                    column->m_valueLen = toast_col->m_valueLen;

                    if (column->m_valueLen > 0)
                    {
                        column->m_value = rmalloc0(column->m_valueLen + 1);
                        rmemset0(column->m_value, 0, 0, column->m_valueLen + 1);
                        rmemcpy0(column->m_value, 0, toast_col->m_value, column->m_valueLen);
                    }
                    heap_free_toast(toast_col);
                }
                /* 排除null和may null */
                if (column->m_valueLen > 0)
                {
                    stmt->len += column->m_valueLen;
                }
            }
        }
    }
    else /* 其他DML语句 */
    {
        xk_pg_parser_translog_tbcol_values * tbcolvalues=
            (xk_pg_parser_translog_tbcol_values*) trans_return;

        int index_column = 0;
        int index_key = 0;
        List* index_list = NULL;
        catalog_index_value* index_value = NULL;
        uint32_t index_key_num = 0;
        uint32_t* index_key_ptr = NULL;

        if (XK_PG_PARSER_TRANSLOG_DMLTYPE_UPDATE == trans_return->m_dmltype)
        {
            ListCell* cell = NULL;

            index_list = catalog_get_index_sysdict_list(decodingctx->trans_cache->sysdicts->by_index,
                                                               NULL,
                                                               txn->sysdictHis,
                                                               oid);

            /* 存在索引 */
            if (index_list)
            {
                foreach(cell, index_list)
                {
                    catalog_index_value* temp_index_value = (catalog_index_value*)lfirst(cell);

                    index_value = temp_index_value;

                    /* 优先设置主键索引 */
                    if (temp_index_value->index->indisprimary)
                    {
                        break;
                    }
                }
                /* 设置索引键 */
                index_key_num = index_value->index->indnatts;
                index_key_ptr = index_value->index->indkey;
            }
        }

        for (index_column = 0; index_column < tbcolvalues->m_valueCnt; index_column++)
        {
            xk_pg_parser_translog_tbcol_value *column_new = NULL;
            xk_pg_parser_translog_tbcol_value *column_old = NULL;

            if (tbcolvalues->m_new_values)
            {
                column_new = &tbcolvalues->m_new_values[index_column];

                /* 只处理toast, 将toast的值解析出来 */
                if (INFO_COL_IS_TOAST == column_new->m_info)
                {
                    xk_pg_parser_translog_tbcol_value *toast_col = NULL;
                    deal_toast = true;
                    toast_col = heap_get_detoast_column(decodingctx, txn, column_new);
                    if (toast_col)
                    {
                        if (INFO_NOTHING != toast_col->m_info)
                        {
                            elog(RLOG_WARNING, "decode heap record, when decode detoast data, unsupport data type");
                            toast_col->m_info = INFO_COL_MAY_NULL;

                            /* 在这里处理掉toast解析出来的值 */
                            if (toast_col->m_value)
                            {
                                rfree(toast_col->m_value);
                                toast_col->m_valueLen = 0;
                                toast_col->m_value = NULL;
                            }
                        }
                        column_new->m_info = toast_col->m_info;
                        if (column_new->m_value)
                        {
                            rfree(column_new->m_value);
                            column_new->m_value = NULL;
                        }

                        column_new->m_valueLen = toast_col->m_valueLen;

                        if (column_new->m_valueLen > 0)
                        {
                            column_new->m_value = rmalloc0(column_new->m_valueLen + 1);
                            rmemset0(column_new->m_value, 0, 0, column_new->m_valueLen + 1);
                            rmemcpy0(column_new->m_value, 0, toast_col->m_value, column_new->m_valueLen);
                        }
                        heap_free_toast(toast_col);
                    }
                    else
                    {
                        column_new->m_valueLen = 0;
                        if (column_new->m_value)
                        {
                            rfree(column_new->m_value);
                            column_new->m_value = NULL;
                        }
                        column_new->m_info = INFO_COL_MAY_NULL;
                    }
                }
                /* 排除null和may null */
                if (column_new->m_valueLen > 0)
                {
                    stmt->len += column_new->m_valueLen;
                }
            }
            if (tbcolvalues->m_old_values)
            {
                column_old = &tbcolvalues->m_old_values[index_column];
                /* 只处理toast, 将toast的值解析出来 */
                if (INFO_COL_IS_TOAST == column_old->m_info)
                {
                    xk_pg_parser_translog_tbcol_value *toast_col = NULL;
                    deal_toast = true;
                    toast_col = heap_get_detoast_column(decodingctx, txn, column_old);
                    if (toast_col)
                    {
                        if (INFO_NOTHING != toast_col->m_info)
                        {
                            elog(RLOG_WARNING, "decode heap record, when decode detoast data, unsupport data type");
                            toast_col->m_info = INFO_COL_MAY_NULL;

                            /* 在这里处理掉toast解析出来的值 */
                            if (toast_col->m_value)
                            {
                                rfree(toast_col->m_value);
                                toast_col->m_valueLen = 0;
                                toast_col->m_value = NULL;
                            }
                        }
                        column_old->m_info = toast_col->m_info;
                        if (column_old->m_value)
                        {
                            rfree(column_old->m_value);
                            column_old->m_value = NULL;
                        }

                        column_old->m_valueLen = toast_col->m_valueLen;

                        if (column_old->m_valueLen > 0)
                        {
                            column_old->m_value = rmalloc0(column_old->m_valueLen + 1);
                            rmemset0(column_old->m_value, 0, 0, column_old->m_valueLen + 1);
                            rmemcpy0(column_old->m_value, 0, toast_col->m_value, column_old->m_valueLen);
                        }
                        heap_free_toast(toast_col);
                    }
                    else
                    {
                        column_old->m_valueLen = 0;
                        if (column_old->m_value)
                        {
                            rfree(column_old->m_value);
                            column_old->m_value = NULL;
                        }
                        column_old->m_info = INFO_COL_MAY_NULL;
                    }
                }

                /* 
                 * 判断是否需要替换
                 * 条件:
                 *      存在 index_value
                 *      且 值为missing
                 *      且 key没有遍历完
                 *      且 key值与列号相同
                 */
                if (index_value
                 && (INFO_COL_MAY_NULL == column_old->m_info)
                 && (index_key < index_key_num)
                 && (index_key_ptr[index_key] == (index_column + 1)))
                {
                    /* 索引自增 */
                    index_key++;

                    /* 拷贝内容, 列名无需拷贝 */
                    column_old->m_coltype = column_new->m_coltype;
                    column_old->m_freeFlag = column_new->m_freeFlag;
                    column_old->m_info = column_new->m_info;
                    column_old->m_valueLen = column_new->m_valueLen;
                    column_old->m_value = rstrdup(column_new->m_value);
                }
                /* 排除null和may null */
                if (column_old->m_valueLen > 0)
                {
                    stmt->len += column_old->m_valueLen;
                }
            }
        }
        /* 释放 */
        if (index_list)
        {
            list_free(index_list);
        }
    }
    stmt->stmt = (void *)trans_return;
    txn->stmtsize += stmt->len;
    decodingctx->trans_cache->totalsize += stmt->len;
    txn->stmts = lappend(txn->stmts, (void*)stmt);
    if (deal_toast)
    {
        TXN_UNSET_TRANS_TOAST(txn->flag);
    }
}

List *decode_heap_multi_insert_save_sysdict_as_insert(List *sysdict,
                                                             xk_pg_parser_translog_tbcolbase *trans_return)
{
    xk_pg_parser_translog_tbcol_nvalues *multi_values =
        (xk_pg_parser_translog_tbcol_nvalues *)trans_return;
    int index_row = 0;

    /* 展开multi insert */
    for (index_row = 0; index_row < multi_values->m_rowCnt; index_row++)
    {
        txn_sysdict *dict = NULL;
        xk_pg_parser_translog_tbcol_values *temp_colvalues = NULL;

        temp_colvalues = rmalloc0(sizeof(xk_pg_parser_translog_tbcol_values));
        if (!temp_colvalues)
        {
            elog(RLOG_ERROR, "oom");
        }
        rmemset0(temp_colvalues, 0, 0, sizeof(xk_pg_parser_translog_tbcol_values));

        /* 设置基础信息 */
        temp_colvalues->m_base.m_dmltype = XK_PG_PARSER_TRANSLOG_DMLTYPE_INSERT;
        temp_colvalues->m_base.m_originid = multi_values->m_base.m_originid;
        temp_colvalues->m_base.m_schemaname = rstrdup(multi_values->m_base.m_schemaname);
        temp_colvalues->m_base.m_tbname = rstrdup(multi_values->m_base.m_tbname);
        temp_colvalues->m_base.m_tabletype = multi_values->m_base.m_tabletype;
        temp_colvalues->m_base.m_type = XK_PG_PARSER_TRANSLOG_RETURN_WITH_DATA;

        /* 设置列值相关信息 */
        temp_colvalues->m_relfilenode = multi_values->m_relfilenode;
        temp_colvalues->m_relid = multi_values->m_relid;
        temp_colvalues->m_valueCnt = multi_values->m_valueCnt;
        temp_colvalues->m_new_values = multi_values->m_rows[index_row].m_new_values;

        /* multiinsert中的列置空 */
        multi_values->m_rows[index_row].m_new_values = NULL;

        /* 保存到系统表链表中 */
        dict = rmalloc0(sizeof(txn_sysdict));
        dict->colvalues = temp_colvalues;
        dict->convert_colvalues = NULL;

        sysdict = lappend(sysdict, dict);
    }

    /* 清理 */
    heap_free_trans_result(trans_return);

    return sysdict;
}

/* 拷贝系统表 */
List *decode_heap_sysdicthis_copy(List *his)
{
    List *result = NULL;
    ListCell *cell = NULL;
    catalogdata *catalog_copy_obj = NULL;

    foreach(cell, his)
    {
        catalogdata *catalog = (catalogdata *)lfirst(cell);
        catalog_copy_obj = catalog_copy(catalog);
        result = lappend(result, (void *)catalog_copy_obj);
    }
    return result;
}
