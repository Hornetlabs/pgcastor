#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/dlist/dlist.h"
#include "utils/varstr/varstr.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/string/stringinfo.h"
#include "utils/algorithm/md5/md5.h"
#include "stmts/txnstmt.h"
#include "stmts/txnstmt_burst.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/cache_sysidcts.h"
#include "cache/txn.h"
#include "catalog/catalog.h"
#include "catalog/class.h"
#include "catalog/index.h"
#include "catalog/type.h"
#include "catalog/attribute.h"
#include "rebuild/rebuild_burst.h"
#include "works/parserwork/wal/decode_heap_util.h"

/* burstcolumn initialization */
rebuild_burstcolumn* rebuild_burstcolumn_init(int colcnt)
{
    rebuild_burstcolumn* burstcolumn = NULL;

    if (0 == colcnt)
    {
        elog(RLOG_WARNING, "rebuild burstcolumn init colcnt is 0");
        return NULL;
    }

    burstcolumn = (rebuild_burstcolumn*)rmalloc0((sizeof(rebuild_burstcolumn) * colcnt));
    if (NULL == burstcolumn)
    {
        elog(RLOG_WARNING, "rebuild burstcolumn init oom");
        return NULL;
    }
    rmemset0(burstcolumn, 0, '\0', (sizeof(rebuild_burstcolumn) * colcnt));

    return burstcolumn;
}

/* burstrow initialization */
rebuild_burstrow* rebuild_burstrow_init(int colcnt)
{
    rebuild_burstrow* burstrow = NULL;

    if (0 == colcnt)
    {
        elog(RLOG_WARNING, "rebuild burstrow init colcnt is 0");
        return NULL;
    }

    burstrow = (rebuild_burstrow*)rmalloc0(sizeof(rebuild_burstrow));
    if (NULL == burstrow)
    {
        elog(RLOG_WARNING, "rebuild burstrow init oom");
        return NULL;
    }
    rmemset0(burstrow, 0, '\0', sizeof(rebuild_burstrow));

    burstrow->flag = REBUILD_BURSTROWFLAG_NOP;
    burstrow->op = REBUILD_BURSTROWTYPE_INVALID;
    burstrow->missingcnt = 0;
    burstrow->row = NULL;
    burstrow->relatedrow = NULL;
    burstrow->missingmapsize = ((colcnt + 7) / 8);
    rmemset1(burstrow->md5, 0, 0, 16);
    burstrow->missingmap = (uint8*)rmalloc0(burstrow->missingmapsize);
    if (NULL == burstrow->missingmap)
    {
        rfree(burstrow);
        elog(RLOG_WARNING, "rebuild burstrow missingmap oom");
        return NULL;
    }
    rmemset0(burstrow->missingmap, 0, 0, burstrow->missingmapsize);

    return burstrow;
}

/* bursttable initialization */
rebuild_bursttable* rebuild_bursttable_init(void)
{
    rebuild_bursttable* bursttable = NULL;

    bursttable = (rebuild_bursttable*)rmalloc0(sizeof(rebuild_bursttable));
    if (NULL == bursttable)
    {
        elog(RLOG_WARNING, "rebuild bursttable init oom");
        return NULL;
    }
    rmemset0(bursttable, 0, '\0', sizeof(rebuild_bursttable));

    bursttable->keycnt = 0;
    bursttable->no = 0;
    bursttable->oid = INVALIDOID;
    bursttable->schema = NULL;
    bursttable->table = NULL;
    bursttable->keys = NULL;

    return bursttable;
}

/* burstnode initialization */
rebuild_burstnode* rebuild_burstnode_init(void)
{
    rebuild_burstnode* burstnode = NULL;

    burstnode = (rebuild_burstnode*)rmalloc0(sizeof(rebuild_burstnode));
    if (NULL == burstnode)
    {
        elog(RLOG_WARNING, "rebuild burstnode init oom");
        return NULL;
    }
    rmemset0(burstnode, 0, '\0', sizeof(rebuild_burstnode));

    burstnode->flag = 0;
    burstnode->type = REBUILD_BURSTNODETYPE_NOP;
    burstnode->stmt = NULL;
    burstnode->dldeleterows = NULL;
    burstnode->dlinsertrows = NULL;
    rmemset1(&burstnode->table, 0, 0, sizeof(rebuild_bursttable));

    return burstnode;
}

/* burst initialization */
rebuild_burst* rebuild_burst_init(void)
{
    rebuild_burst* burst = NULL;

    burst = (rebuild_burst*)rmalloc0(sizeof(rebuild_burst));
    if (NULL == burst)
    {
        elog(RLOG_WARNING, "rebuild burst init oom");
        return NULL;
    }
    rmemset0(burst, 0, '\0', sizeof(rebuild_burst));

    burst->number = 0;
    burst->dlburstnodes = NULL;
    burst->dlbursttable = NULL;

    return burst;
}

/* MD5sum and missingmap */
static bool rebuild_burst_setmd5andmissing(rebuild_burstrow* row, pg_parser_translog_tbcol_value* value)
{
    MD5_CTX                          md5;
    int                              colindex = 0;
    varstr*                          vstr = NULL;
    pg_parser_translog_tbcol_values* colvalues = NULL;

    colvalues = (pg_parser_translog_tbcol_values*)row->row;

    vstr = varstr_init(256);
    if (NULL == vstr)
    {
        elog(RLOG_WARNING, "rebuild burst setmd5adnmissing init varstr oom");
        return false;
    }

    row->missingcnt = 0;
    rmemset0(row->missingmap, 0, 0, row->missingmapsize);
    rmemset1(row->md5, 0, 0, 16);

    for (colindex = 0; colindex < colvalues->m_valueCnt; colindex++)
    {
        if (INFO_COL_IS_NULL == value[colindex].m_info)
        {
            continue;
        }

        if (INFO_COL_MAY_NULL == value[colindex].m_info)
        {
            row->missingcnt++;
            row->missingmap[colindex / 8] |= (1U << (colindex % 8));
            continue;
        }

        varstr_appendbinary(vstr, value[colindex].m_value, value[colindex].m_valueLen);
    }

    if (0 == row->missingcnt)
    {
        MD5Init(&md5);
        MD5Update(&md5, (uint8_t*)vstr->data, vstr->start);
        MD5Final((uint8_t*)row->md5, &md5);
    }

    varstr_free(vstr);

    return true;
}

/* bursttable compare function */
int rebuild_bursttable_cmp(void* s1, void* s2)
{
    rebuild_bursttable* table1 = NULL;
    rebuild_bursttable* table2 = NULL;

    table1 = (rebuild_bursttable*)s1;
    table2 = (rebuild_bursttable*)s2;

    if (table1->oid != table2->oid)
    {
        return 1;
    }

    if (0 != strcmp(table1->schema, table2->schema))
    {
        return 1;
    }

    if (0 != strcmp(table1->table, table2->table))
    {
        return 1;
    }
    return 0;
}

/* burstnode and bursttable compare function */
int rebuild_burstnode_tablecmp(void* s1, void* s2)
{
    rebuild_burstnode*  node = NULL;
    rebuild_bursttable* table = NULL;

    table = (rebuild_bursttable*)s1;
    node = (rebuild_burstnode*)s2;

    if (node->table.no != table->no)
    {
        return 1;
    }

    if (node->table.oid != table->oid)
    {
        return 1;
    }

    if (0 != strcmp(node->table.schema, table->schema))
    {
        return 1;
    }

    if (0 != strcmp(node->table.table, table->table))
    {
        return 1;
    }
    return 0;
}

/* Check column is a constraint column or primary key column */
static bool rebuild_burst_colisconskey(rebuild_bursttable* table, char* colname)
{
    int                  keyindex = 0;
    rebuild_burstcolumn* key = NULL;
    for (keyindex = 0; keyindex < table->keycnt; keyindex++)
    {
        key = &table->keys[keyindex];

        if (0 == strcmp(colname, key->colname))
        {
            return true;
        }
    }
    return false;
}

/* Get typename by typeid */
static char* rebuild_burst_gettypename(List* lattrs, HTAB* htype, Oid typeoid, char* colname)
{
    bool                         find = false;
    int                          typmod = -1;
    ListCell*                    attrlc = NULL;
    pg_sysdict_Form_pg_type      type = NULL;
    pg_sysdict_Form_pg_attribute attr = NULL;
    StringInfoData               result = {0};

    type = (pg_sysdict_Form_pg_type)type_getbyoid(typeoid, htype);
    if (NULL == type)
    {
        elog(RLOG_WARNING, "cache lookup failed for type %u", typeoid);
        return NULL;
    }

    foreach (attrlc, lattrs)
    {
        attr = (pg_sysdict_Form_pg_attribute)lfirst(attrlc);
        if (0 == strcmp(colname, attr->attname.data))
        {
            find = true;
            typmod = attr->atttypmod;
            break;
        }
    }

    if (false == find)
    {
        elog(RLOG_WARNING, "castor rebuild composekey not find attribute attnum %s", colname);
        return NULL;
    }

    initStringInfo(&result);

    /* Get lenth scale precision */
    switch (typeoid)
    {
        case PG_SYSDICT_BITOID:
            if (typmod == -1)
            {
                appendStringInfoString(&result, "bit");
            }
            else
            {
                appendStringInfo(&result, "bit(%d)", typmod);
            }
            break;
        case PG_SYSDICT_VARBITOID:
            if (typmod == -1)
            {
                appendStringInfoString(&result, "varbit");
            }
            else
            {
                appendStringInfo(&result, "varbit(%d)", typmod);
            }
            break;
        case PG_SYSDICT_CHAROID:
        case PG_SYSDICT_BPCHAROID:
            if (typmod == -1)
            {
                appendStringInfoString(&result, "char");
            }
            else
            {
                appendStringInfo(&result, "char(%d)", typmod - VARHDRSZ);
            }
            break;
        case PG_SYSDICT_VARCHAROID:
            if (typmod == -1)
            {
                appendStringInfoString(&result, "varchar");
            }
            else
            {
                appendStringInfo(&result, "varchar(%d)", typmod - VARHDRSZ);
            }
            break;
        case PG_SYSDICT_NUMERICOID:
            if (typmod == -1)
            {
                appendStringInfoString(&result, "numeric");
            }
            else
            {
                appendStringInfo(&result,
                                 "numeric(%d, %d)",
                                 ((typmod - VARHDRSZ) >> 16) & 0xffff,
                                 (typmod - VARHDRSZ) & 0xffff);
            }
            break;
        case PG_SYSDICT_TIMEOID:
            if (typmod == -1)
            {
                appendStringInfoString(&result, "time without time zone");
            }
            else
            {
                appendStringInfo(&result, "time(%d) without time zone", typmod);
            }
            break;
        case PG_SYSDICT_TIMETZOID:
            if (typmod == -1)
            {
                appendStringInfoString(&result, "time with time zone");
            }
            else
            {
                appendStringInfo(&result, "time(%d) with time zone", typmod);
            }
            break;
        case PG_SYSDICT_TIMESTAMPOID:
            if (typmod == -1)
            {
                appendStringInfoString(&result, "timestamp without time zone");
            }
            else
            {
                appendStringInfo(&result, "timestamp(%d) without time zone", typmod);
            }
            break;
        case PG_SYSDICT_TIMESTAMPTZOID:
            if (typmod == -1)
            {
                appendStringInfoString(&result, "timestamp with time zone");
            }
            else
            {
                appendStringInfo(&result, "timestamp(%d) with time zone", typmod);
            }
            break;
        default:
            appendStringInfo(&result, "%s", type->typname.data);
            break;
    }
    return result.data;
}

/* fix missing value */
static bool rebuild_burst_updatematchdata(rebuild_burstrow* insertrow,
                                          rebuild_burstrow* delrow,
                                          rebuild_burstrow* updaterow)
{
    pg_parser_translog_tbcol_values* insert = NULL;
    pg_parser_translog_tbcol_values* update = NULL;
    pg_parser_translog_tbcol_value*  insertvalue = NULL;
    pg_parser_translog_tbcol_value*  nupdatevalue = NULL;
    pg_parser_translog_tbcol_value*  oupdatevalue = NULL;

    /*
     * If there are no missing values in before or after value of the update, return directly */
    if (0 == updaterow->missingcnt && 0 == delrow->missingcnt)
    {
        return true;
    }

    insert = (pg_parser_translog_tbcol_values*)insertrow->row;
    update = (pg_parser_translog_tbcol_values*)updaterow->row;
    insertvalue = insert->m_new_values;
    oupdatevalue = update->m_old_values;
    nupdatevalue = update->m_new_values;

    /* Matched insert completes the update split insert value */
    if (false == pg_parser_trans_matchmissing(nupdatevalue, insertvalue, insert->m_valueCnt))
    {
        elog(RLOG_WARNING, "rebuild burst updatematchdata insert/delete failed");
        return false;
    }

    if (REBUILD_BURSTROWTYPE_UPDATE == insertrow->op)
    {
        if (false == pg_parser_trans_matchmissing(oupdatevalue, insertvalue, insert->m_valueCnt))
        {
            elog(RLOG_WARNING, "rebuild burst updatematchdata insert/update before failed");
            return false;
        }
    }
    return true;
}

/* Check if constraint column values are modified */
static void rebuild_burst_ischangeconskey(rebuild_burstnode* burstnode, rebuild_burstrow* updaterow)
{
    int                              key = 0;
    int                              colindex = 0;
    rebuild_burstcolumn*             column = NULL;
    pg_parser_translog_tbcol_values* update = NULL;
    pg_parser_translog_tbcol_value*  nupdatevalue = NULL;
    pg_parser_translog_tbcol_value*  oupdatevalue = NULL;

    update = (pg_parser_translog_tbcol_values*)updaterow->row;

    nupdatevalue = update->m_new_values;
    oupdatevalue = update->m_old_values;

    column = burstnode->table.keys;

    /* Check if constraint columns are modified based on table.keys */
    for (colindex = 0; colindex < burstnode->table.keycnt; colindex++)
    {
        key = column[colindex].colno;

        /* Found modified constraint column, set flag and return */
        if (0 != strcmp((char*)nupdatevalue[key - 1].m_value, oupdatevalue[key - 1].m_value))
        {
            updaterow->flag = REBUILD_BURSTROWFLAG_CHANGECONSKEY;
            break;
        }
    }
    return;
}

/* Build burstnode table primary key or unique constraint information */
static bool rebuild_composekey(HTAB* hclass, HTAB* hattrs, HTAB* hindex, rebuild_burstnode* pburstnode)
{
    bool                         find = false;
    uint32                       indkey = 0;
    int64                        keycntindex = 0;
    List*                        lindex = NULL;
    List*                        lattrs = NULL;
    ListCell*                    indlc = NULL;
    ListCell*                    attrlc = NULL;
    pg_sysdict_Form_pg_class     class = NULL;
    catalog_index_value*         indvalue = NULL;
    pg_sysdict_Form_pg_index     index = NULL;
    pg_sysdict_Form_pg_attribute attr = NULL;

    /* Get class */
    class = (pg_sysdict_Form_pg_class)class_getbyoid(pburstnode->table.oid, hclass);

    if (NULL == class)
    {
        elog(RLOG_WARNING, "castor rebuild composekey not find class by %lu", pburstnode->table.oid);
        return false;
    }

    /* Get index */
    lindex = (List*)index_getbyoid(pburstnode->table.oid, hindex);

    /* Index not found, set to pbe mode and exit */
    if (NULL == lindex || NULL == lindex->head)
    {
        pburstnode->flag = REBUILD_BURSTNODEFLAG_NOINDEX;
        return true;
    }

    /* Filter replident/primary index, use first index if not found */
    foreach (indlc, lindex)
    {
        find = false;
        indvalue = (catalog_index_value*)lfirst(indlc);

        if (true == indvalue->index->indisreplident)
        {
            find = true;
            index = indvalue->index;
            break;
        }

        if (true == indvalue->index->indisprimary)
        {
            find = true;
            index = indvalue->index;
            break;
        }
    }

    /* Not found, use first index */
    if (false == find)
    {
        indvalue = (catalog_index_value*)linitial(lindex);
        index = indvalue->index;
    }

    pburstnode->table.keys = rebuild_burstcolumn_init(index->indnatts);
    if (NULL == pburstnode->table.keys)
    {
        elog(RLOG_WARNING, "castor rebuild composekey pburstnode table.keys is null ");
        return false;
    }
    pburstnode->table.keycnt = index->indnatts;

    /* Get attributes */
    lattrs = (List*)attribute_getbyoid(pburstnode->table.oid, hattrs);

    if (NULL == lattrs || NULL == lattrs->head)
    {
        elog(RLOG_WARNING, "castor rebuild composekey not find attribute by %lu", pburstnode->table.oid);
        return false;
    }

    /* Fill table.keys*/
    for (keycntindex = 0; keycntindex < index->indnatts; keycntindex++)
    {
        indkey = index->indkey[keycntindex];
        find = false;
        foreach (attrlc, lattrs)
        {
            attr = (pg_sysdict_Form_pg_attribute)lfirst(attrlc);
            if (indkey == attr->attnum)
            {
                find = true;
                pburstnode->table.keys[keycntindex].colno = attr->attnum;
                pburstnode->table.keys[keycntindex].coltype = attr->atttypid;
                pburstnode->table.keys[keycntindex].colname = rstrdup(attr->attname.data);
                break;
            }
        }

        if (false == find)
        {
            elog(RLOG_WARNING, "castor rebuild composekey not find attribute attnum %u", indkey);
            return false;
        }
    }
    pburstnode->flag = REBUILD_BURSTNODEFLAG_INDEX;

    if (lindex)
    {
        list_free(lindex);
    }
    return true;
}

/*
 * Copy tbcolbase
 * Parameters: tbcolbase1 source data
 *             tbcolbase2 target data
 */
static void rebuild_burst_tbcolbasecopy(pg_parser_translog_tbcolbase* tbcolbase1,
                                        pg_parser_translog_tbcolbase* tbcolbase2)
{
    if (NULL == tbcolbase1 || NULL == tbcolbase2)
    {
        return;
    }

    tbcolbase2->m_type = tbcolbase1->m_type;
    tbcolbase2->m_dmltype = tbcolbase1->m_dmltype;
    tbcolbase2->m_originid = tbcolbase1->m_originid;
    tbcolbase2->m_tabletype = tbcolbase1->m_tabletype;
    tbcolbase2->m_schemaname = rstrdup(tbcolbase1->m_schemaname);
    tbcolbase2->m_tbname = rstrdup(tbcolbase1->m_tbname);
    return;
}

/*
 * Copy tbcolbase
 * Parameters: value1 source data
 *
 * Return value: Whether copy succeeded, value2 copy result
 */
static bool rebuild_burst_tbcolvaluecopy(pg_parser_translog_tbcol_value*  value1,
                                         pg_parser_translog_tbcol_value** value2,
                                         uint32                           valuecnt)
{
    int                             len = 0;
    int                             colindex = 0;
    pg_parser_translog_tbcol_value* value = NULL;

    if (NULL == value1)
    {
        elog(RLOG_WARNING, "value1 is null");
        return false;
    }

    len = sizeof(pg_parser_translog_tbcol_value) * valuecnt;
    value = (pg_parser_translog_tbcol_value*)rmalloc0(len);
    if (NULL == value)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return false;
    }
    rmemset0(value, 0, 0, len);

    for (colindex = 0; colindex < valuecnt; colindex++)
    {
        value[colindex].m_freeFlag = value1[colindex].m_freeFlag;
        value[colindex].m_info = value1[colindex].m_info;
        value[colindex].m_coltype = value1[colindex].m_coltype;
        value[colindex].m_valueLen = value1[colindex].m_valueLen;
        value[colindex].m_colName = rstrdup(value1[colindex].m_colName);

        if (0 != value[colindex].m_valueLen)
        {
            value[colindex].m_value = rmalloc0(value[colindex].m_valueLen + 1);
            if (NULL == value[colindex].m_value)
            {
                elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
                return false;
            }
            rmemset0(value[colindex].m_value, 0, 0, value[colindex].m_valueLen + 1);
            rmemcpy0(value[colindex].m_value, 0, value1[colindex].m_value, value1[colindex].m_valueLen);
        }
    }

    *value2 = value;
    return true;
}

/* Get table from list tail */
static void* rebuild_burst_gettable(dlist* dl, void* value, dlistvaluecmp valuecmp)
{
    dlistnode* dlnode = NULL;
    dlistnode* dlnodetmp = NULL;
    if (NULL == dl)
    {
        return NULL;
    }

    for (dlnode = dl->tail; NULL != dlnode; dlnode = dlnodetmp)
    {
        dlnodetmp = dlnode->prev;
        if (0 != valuecmp(value, dlnode->value))
        {
            continue;
        }

        return dlnode->value;
    }

    return NULL;
}

/* Get burst node */
bool rebuild_burst_getnode(HTAB*               hclass,
                           HTAB*               hattrs,
                           HTAB*               hindex,
                           rebuild_burst*      burst,
                           rebuild_burstnode** pburstnode,
                           rebuild_bursttable* bursttable)
{
    rebuild_bursttable* dltable = NULL;
    rebuild_burstnode*  tmpburstnode = NULL;

    dltable = (rebuild_bursttable*)rebuild_burst_gettable(burst->dlbursttable, bursttable, rebuild_bursttable_cmp);
    if (NULL == dltable)
    {
        dltable = rebuild_bursttable_init();
        if (NULL == dltable)
        {
            elog(RLOG_WARNING, "rebuild burst getnode bursttable init failed");
            return false;
        }
        dltable->no = burst->number++;
        dltable->oid = bursttable->oid;
        dltable->schema = rstrdup(bursttable->schema);
        dltable->table = rstrdup(bursttable->table);

        burst->dlbursttable = dlist_put(burst->dlbursttable, dltable);

        tmpburstnode = rebuild_burstnode_init();
        tmpburstnode->table.no = dltable->no;
        tmpburstnode->table.oid = dltable->oid;
        tmpburstnode->table.schema = rstrdup(dltable->schema);
        tmpburstnode->table.table = rstrdup(dltable->table);
    }
    else
    {
        tmpburstnode = (rebuild_burstnode*)dlist_get(burst->dlburstnodes, dltable, rebuild_burstnode_tablecmp);
        if (NULL != tmpburstnode)
        {
            *pburstnode = tmpburstnode;
            return true;
        }

        tmpburstnode = rebuild_burstnode_init();
        tmpburstnode->table.no = burst->number++;
        dltable->no = tmpburstnode->table.no;
        tmpburstnode->table.oid = dltable->oid;
        tmpburstnode->table.schema = rstrdup(dltable->schema);
        tmpburstnode->table.table = rstrdup(dltable->table);
    }

    /* Build burstnode->table.keys index information */
    if (false == rebuild_composekey(hclass, hattrs, hindex, tmpburstnode))
    {
        *pburstnode = NULL;
        rebuild_burstnode_free(tmpburstnode);
        elog(RLOG_WARNING, "rebuild burst getnode composekey failed");
        return false;
    }

    burst->dlburstnodes = dlist_put(burst->dlburstnodes, tmpburstnode);
    *pburstnode = tmpburstnode;

    return true;
}

/*
 * Split update into insert/delete
 * Settings
 * Parameters: burstnode table corresponding node, rows -- update source data
 *
 * Return value: bool true split succeeded, false split failed
 *              delrow split delete statement
 *              insertrow original update statement
 *
 */
bool rebuild_burst_decomposeupdate(rebuild_burstnode* burstnode,
                                   rebuild_burstrow** delrow,
                                   rebuild_burstrow** insertrow,
                                   void*              rows)
{
    rebuild_burstrow*                tmpdelrow = NULL;
    rebuild_burstrow*                tmpinsertrow = NULL;
    pg_parser_translog_tbcol_values* update = NULL;
    pg_parser_translog_tbcol_values* delete = NULL;

    update = (pg_parser_translog_tbcol_values*)rows;

    /* Update old and new value missing values complement each other */
    if (false == pg_parser_trans_matchmissing(update->m_new_values, update->m_old_values, update->m_valueCnt))
    {
        elog(RLOG_WARNING, "rebuild burst decomposeupdate match missing failed");
        return false;
    }

    tmpdelrow = rebuild_burstrow_init(update->m_valueCnt);
    if (NULL == tmpdelrow)
    {
        elog(RLOG_WARNING, "rebuild burst decomposeupdate burstrow init failed");
        return false;
    }

    /* Allocate delete value space */
    delete = (pg_parser_translog_tbcol_values*)rmalloc0(sizeof(pg_parser_translog_tbcol_values));
    if (NULL == delete)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return false;
    }
    rmemset0(delete, 0, '\0', sizeof(pg_parser_translog_tbcol_values));

    /* Copy table information */
    rebuild_burst_tbcolbasecopy(&update->m_base, &delete->m_base);

    delete->m_base.m_dmltype = PG_PARSER_TRANSLOG_DMLTYPE_DELETE;
    delete->m_haspkey = update->m_haspkey;
    delete->m_relfilenode = update->m_relfilenode;
    delete->m_relid = update->m_relid;
    delete->m_tupleCnt = update->m_tupleCnt;
    delete->m_valueCnt = update->m_valueCnt;
    delete->m_new_values = NULL;
    delete->m_tuple = NULL;

    /* Copy before column content */
    if (false == rebuild_burst_tbcolvaluecopy(update->m_old_values, &delete->m_old_values, update->m_valueCnt))
    {
        elog(RLOG_WARNING, "rebuild burst decomposeupdate copy before failed");
        heap_free_trans_result((pg_parser_translog_tbcolbase*)delete);
        return false;
    }
    tmpdelrow->row = delete;
    tmpdelrow->op = REBUILD_BURSTROWTYPE_UPDATE;

    /* Set del to need deletion */
    tmpdelrow->flag = REBUILD_BURSTROWFLAG_REMOVEDELETE;

    if (false == rebuild_burst_setmd5andmissing(tmpdelrow, delete->m_old_values))
    {
        rebuild_burstrow_free(tmpdelrow);
        elog(RLOG_WARNING, "rebuild burst decomposeupdate delete setmd5adnmissing failed");
        return false;
    }

    tmpinsertrow = rebuild_burstrow_init(update->m_valueCnt);
    if (NULL == tmpinsertrow)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return false;
    }

    /* Distinguish insert from update, so set to update */
    tmpinsertrow->row = rows;
    tmpinsertrow->op = REBUILD_BURSTROWTYPE_UPDATE;

    if (false == rebuild_burst_setmd5andmissing(tmpinsertrow, update->m_new_values))
    {
        rebuild_burstrow_free(delrow);
        rebuild_burstrow_free(tmpinsertrow);
        elog(RLOG_WARNING, "rebuild burst decomposeupdate insert setmd5adnmissing failed");
        return false;
    }

    /* Set primary key or unique constraint column changed */
    rebuild_burst_ischangeconskey(burstnode, tmpinsertrow);

    /* Mutually linked, used to determine if delete statement should be saved */
    tmpdelrow->relatedrow = tmpinsertrow;
    tmpinsertrow->relatedrow = tmpdelrow;

    *delrow = tmpdelrow;
    *insertrow = tmpinsertrow;

    return true;
}

/*
 * Merge insert/delete
 * Return true means merge succeeded, return false means merge failed
 */
bool rebuild_burst_mergeinsert(rebuild_burstnode* pburstnode, rebuild_burstrow* insertrow)
{
    dlistnode*        dlnode = NULL;
    rebuild_burstrow* delrow = NULL;

    /* Exit if has missing value or delete list is empty */
    if (insertrow->missingcnt > 0)
    {
        return false;
    }

    if (true == dlist_isnull(pburstnode->dldeleterows))
    {
        return false;
    }

    /* Find delete matching the insert */
    for (dlnode = pburstnode->dldeleterows->tail; NULL != dlnode; dlnode = dlnode->prev)
    {
        delrow = (rebuild_burstrow*)dlnode->value;

        /* Continue if has missing value */
        if (delrow->missingcnt > 0)
        {
            continue;
        }

        /* Found same delete, delete from list, free insert, return true */
        if (0 == memcmp(insertrow->md5, delrow->md5, 16))
        {
            /*
             * Insert/delete merge has multiple cases:
             * 1. insert merges with udelete - when udelete has related row (u 1->2, i 1)
             *     Need to set the related row (uinsert) to normal insert and unchanged primary key
             * 2. When insert and delete and udelete have no related rows - directly merge
             * 3. uinsert merges with delete - when uinsert has related column (udelete) (d 1, u
             * 2->1) Need to set the related column (udelete) flag to normal (execute)
             * 4. uinsert 1 merges with udelete 2 - udelete 1 should execute, uinsert 2 becomes
             * normal insert
             *
             */
            if (REBUILD_BURSTROWTYPE_INSERT == insertrow->op)
            {
                if (REBUILD_BURSTROWTYPE_UPDATE == delrow->op && NULL != delrow->relatedrow)
                {
                    delrow->relatedrow->flag = REBUILD_BURSTROWFLAG_NOP;
                    delrow->relatedrow->op = REBUILD_BURSTROWTYPE_INSERT;
                    delrow->relatedrow->relatedrow = NULL;
                    delrow->relatedrow = NULL;
                }
            }
            else if (REBUILD_BURSTROWTYPE_UPDATE == insertrow->op)
            {
                if (REBUILD_BURSTROWTYPE_DELETE == delrow->op && NULL != insertrow->relatedrow)
                {
                    insertrow->relatedrow->flag = REBUILD_BURSTROWFLAG_NOP;
                    insertrow->relatedrow->relatedrow = NULL;
                    insertrow->relatedrow = NULL;
                }
                else if (REBUILD_BURSTROWTYPE_UPDATE == delrow->op)
                {
                    if (NULL != insertrow->relatedrow)
                    {
                        insertrow->relatedrow->flag = REBUILD_BURSTROWFLAG_NOP;
                        insertrow->relatedrow->relatedrow = NULL;
                        insertrow->relatedrow = NULL;
                    }

                    if (NULL != delrow->relatedrow)
                    {
                        delrow->relatedrow->flag = REBUILD_BURSTROWFLAG_NOP;
                        delrow->relatedrow->op = REBUILD_BURSTROWTYPE_INSERT;
                        delrow->relatedrow->relatedrow = NULL;
                        delrow->relatedrow = NULL;
                    }
                }
            }
            rebuild_burstrow_free(insertrow);
            pburstnode->dldeleterows = dlist_delete(pburstnode->dldeleterows, dlnode, rebuild_burstrow_free);
            return true;
        }
    }

    return false;
}

/*
 * Merge delete/insert
 * Return true means merge succeeded, return false means merge failed
 */
bool rebuild_burst_mergedelete(rebuild_burstnode* pburstnode, rebuild_burstrow* delrow)
{
    int                              keycnt = 0;
    int                              keyindex = 0;
    int                              key = 0;
    dlistnode*                       dlnode = NULL;
    rebuild_burstcolumn*             keys = NULL;
    rebuild_burstrow*                insertrow = NULL;
    pg_parser_translog_tbcol_values* del = NULL;
    pg_parser_translog_tbcol_values* insert = NULL;

    if (true == dlist_isnull(pburstnode->dlinsertrows))
    {
        return false;
    }

    /* Find delete same as insert */
    for (dlnode = pburstnode->dlinsertrows->tail; NULL != dlnode; dlnode = dlnode->prev)
    {
        insertrow = (rebuild_burstrow*)dlnode->value;

        if (REBUILD_BURSTNODEFLAG_NOINDEX == pburstnode->flag)
        {
            /* Found same insert, delete from list, free delete, return true */
            if (0 == memcmp(insertrow->md5, delrow->md5, 16))
            {
                pburstnode->dlinsertrows = dlist_delete(pburstnode->dlinsertrows, dlnode, rebuild_burstrow_free);
                rebuild_burstrow_free(delrow);
                return true;
            }
        }
        else
        {
            bool same = true;
            keycnt = pburstnode->table.keycnt;
            keys = pburstnode->table.keys;
            del = (pg_parser_translog_tbcol_values*)delrow->row;
            insert = (pg_parser_translog_tbcol_values*)insertrow->row;

            /* Compare if constraint column values are the same */
            for (keyindex = 0; keyindex < keycnt; keyindex++)
            {
                key = keys[keyindex].colno;
                if (0 != strcmp(del->m_old_values[key - 1].m_value, insert->m_new_values[key - 1].m_value))
                {
                    same = false;
                    break;
                }
            }

            if (true == same)
            {
                /* Merge succeeded, remove insert, free delrow */
                pburstnode->dlinsertrows = dlist_delete(pburstnode->dlinsertrows, dlnode, rebuild_burstrow_free);
                rebuild_burstrow_free(delrow);
                return true;
            }
        }
    }

    return false;
}

/*
 * Update merge delete/insert
 * Parameters: delrow update split before value
 *
 * Return value: Return true means merge succeeded, return false means merge failed
 *            in_updaterow returns matched insertrow on success, updaterow on failure
 *            error false means execution failed and exit, true means execution succeeded
 */
bool rebuild_burst_updatemergedelete(rebuild_burstnode* burstnode,
                                     rebuild_burstrow*  delrow,
                                     rebuild_burstrow** in_updaterow,
                                     bool*              error)
{
    bool                             same = true;
    int                              keycnt = 0;
    int                              keyindex = 0;
    int                              key = 0;
    dlistnode*                       dlnode = NULL;
    rebuild_burstcolumn*             keys = NULL;
    rebuild_burstrow*                insertrow = NULL;
    rebuild_burstrow*                updaterow = NULL;
    pg_parser_translog_tbcol_values* del = NULL;
    pg_parser_translog_tbcol_values* insert = NULL;
    pg_parser_translog_tbcol_values* update = NULL;
    pg_parser_translog_tbcol_value*  insertvalue = NULL;

    updaterow = *in_updaterow;

    if (true == dlist_isnull(burstnode->dlinsertrows))
    {
        return false;
    }

    update = (pg_parser_translog_tbcol_values*)updaterow->row;

    /* Find delete same as insert */
    for (dlnode = burstnode->dlinsertrows->tail; NULL != dlnode; dlnode = dlnode->prev)
    {
        same = false;
        insertrow = (rebuild_burstrow*)dlnode->value;
        insert = (pg_parser_translog_tbcol_values*)insertrow->row;

        if (REBUILD_BURSTNODEFLAG_NOINDEX == burstnode->flag)
        {
            /* Found same insert, delete from list, free delete, return true */
            if (0 == memcmp(insertrow->md5, delrow->md5, 16))
            {
                same = true;
                break;
            }
        }
        else
        {
            same = true;
            keycnt = burstnode->table.keycnt;
            keys = burstnode->table.keys;
            del = (pg_parser_translog_tbcol_values*)delrow->row;

            /* Compare if constraint column values are the same */
            for (keyindex = 0; keyindex < keycnt; keyindex++)
            {
                key = keys[keyindex].colno;
                if (0 != strcmp(del->m_old_values[key - 1].m_value, insert->m_new_values[key - 1].m_value))
                {
                    same = false;
                    break;
                }
            }

            if (true == same)
            {
                break;
            }
        }
    }

    /* No matching insert found for delete, return directly */
    if (false == same)
    {
        return false;
    }

    /* Complete missing values */
    if (false == rebuild_burst_updatematchdata(insertrow, delrow, updaterow))
    {
        elog(RLOG_WARNING, "rebuild burst updatemergedelete update matchdata failed");
        return false;
    }

    /* Replace insert values with UPDATE after values */
    if (REBUILD_BURSTROWTYPE_INSERT == insertrow->op || REBUILD_BURSTROWTYPE_UPDATE == insertrow->op)
    {
        insert = (pg_parser_translog_tbcol_values*)insertrow->row;
        insertvalue = insert->m_new_values;

        insert->m_new_values = update->m_new_values;
        update->m_new_values = insertvalue;

        /* Only disconnect update->insert related row */
        if (NULL != insertrow->relatedrow)
        {
            insertrow->relatedrow->relatedrow = NULL;
            insertrow->relatedrow = NULL;
        }
    }
    else
    {
        elog(RLOG_WARNING, "rebuild burst updatemergedelete Invalid operation type:%d", insertrow->op);
        *error = true;
        return false;
    }

    /* Calculate MD5 and missing */
    if (false == rebuild_burst_setmd5andmissing(insertrow, insert->m_new_values))
    {
        elog(RLOG_WARNING, "rebuild burst updatemergedelete setmd5adnmissing failed");
        *error = true;
        return false;
    }

    rebuild_burstrow_free(delrow);
    rebuild_burstrow_free(updaterow);

    /* Remove matched insert from the list */
    *in_updaterow = insertrow;
    dlnode->value = NULL;
    burstnode->dlinsertrows = dlist_delete(burstnode->dlinsertrows, dlnode, NULL);

    return true;
}

/* Reorganize txn INSERT content into burst */
static bool rebuild_burst_txn2bursts_insert(rebuild_burst* burst, cache_sysdicts* sysdicts, void* row)
{
    rebuild_bursttable               table = {0};
    rebuild_burstrow*                insertrow = NULL;
    rebuild_burstnode*               burstnode = NULL;
    pg_parser_translog_tbcol_values* insert = NULL;

    insert = (pg_parser_translog_tbcol_values*)row;

    table.oid = insert->m_relid;
    table.schema = insert->m_base.m_schemaname;
    table.table = insert->m_base.m_tbname;

    if (false == rebuild_burst_getnode(sysdicts->by_class,
                                       sysdicts->by_attribute,
                                       sysdicts->by_index,
                                       burst,
                                       &burstnode,
                                       &table))
    {
        elog(RLOG_WARNING, "rebuild burst txn2bursts insert getnode failed");
        return false;
    }

    insertrow = rebuild_burstrow_init(insert->m_valueCnt);
    if (NULL == insertrow)
    {
        elog(RLOG_WARNING, "rebuild burst txn2bursts insert burstrow init failed");
        return false;
    }
    insertrow->row = row;
    insertrow->op = REBUILD_BURSTROWTYPE_INSERT;

    if (false == rebuild_burst_setmd5andmissing(insertrow, insert->m_new_values))
    {
        rebuild_burstrow_free(insertrow);
        elog(RLOG_WARNING, "rebuild burst txn2bursts insert setmd5adnmissing failed");
        return false;
    }

    if (false == rebuild_burst_mergeinsert(burstnode, insertrow))
    {
        burstnode->dlinsertrows = dlist_put(burstnode->dlinsertrows, insertrow);
    }

    return true;
}

/* Reorganize txn DELETE content into burst */
static bool rebuild_burst_txn2bursts_delete(rebuild_burst* burst, cache_sysdicts* sysdicts, void* row)
{
    rebuild_bursttable               table = {0};
    rebuild_burstrow*                delrow = NULL;
    rebuild_burstnode*               burstnode = NULL;
    pg_parser_translog_tbcol_values* delete = NULL;

    delete = (pg_parser_translog_tbcol_values*)row;

    table.oid = delete->m_relid;
    table.schema = delete->m_base.m_schemaname;
    table.table = delete->m_base.m_tbname;

    /* Get burstnode */
    if (false == rebuild_burst_getnode(sysdicts->by_class,
                                       sysdicts->by_attribute,
                                       sysdicts->by_index,
                                       burst,
                                       &burstnode,
                                       &table))
    {
        elog(RLOG_WARNING, "rebuild burst txn2bursts delete getnode failed");
        return false;
    }

    delrow = rebuild_burstrow_init(delete->m_valueCnt);
    if (NULL == delrow)
    {
        elog(RLOG_WARNING, "rebuild burst txn2bursts delete burstrow init failed");
        return false;
    }
    delrow->row = row;
    delrow->op = REBUILD_BURSTROWTYPE_DELETE;

    if (false == rebuild_burst_setmd5andmissing(delrow, delete->m_old_values))
    {
        rebuild_burstrow_free(delrow);
        elog(RLOG_WARNING, "rebuild burst txn2bursts delete setmd5adnmissing failed");
        return false;
    }

    if (false == rebuild_burst_mergedelete(burstnode, delrow))
    {
        burstnode->dldeleterows = dlist_put(burstnode->dldeleterows, delrow);
    }

    return true;
}

/* Reorganize txn MULTIINSERT content into burst */
static bool rebuild_burst_txn2bursts_multiinsert(rebuild_burst* burst, cache_sysdicts* sysdicts, void* row)
{
    int                               rowindx = 0;
    rebuild_bursttable                table = {0};
    rebuild_burstrow*                 insertrow = NULL;
    rebuild_burstnode*                burstnode = NULL;
    pg_parser_translog_tbcol_value*   value = NULL;
    pg_parser_translog_tbcol_values*  insert = NULL;
    pg_parser_translog_tbcol_nvalues* multiinsert = NULL;

    multiinsert = (pg_parser_translog_tbcol_nvalues*)row;

    table.oid = multiinsert->m_relid;
    table.schema = multiinsert->m_base.m_schemaname;
    table.table = multiinsert->m_base.m_tbname;

    if (false == rebuild_burst_getnode(sysdicts->by_class,
                                       sysdicts->by_attribute,
                                       sysdicts->by_index,
                                       burst,
                                       &burstnode,
                                       &table))
    {
        elog(RLOG_WARNING, "rebuild burst txn2bursts multiinsert getnode failed");
        return false;
    }

    for (rowindx = 0; rowindx < multiinsert->m_rowCnt; rowindx++)
    {
        value = multiinsert->m_rows[rowindx].m_new_values;
        /* Split into multiple inserts */
        insert = (pg_parser_translog_tbcol_values*)rmalloc0(sizeof(pg_parser_translog_tbcol_values));
        if (NULL == insert)
        {
            elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
            return false;
        }
        rmemset0(insert, 0, '\0', sizeof(pg_parser_translog_tbcol_values));

        rebuild_burst_tbcolbasecopy(&multiinsert->m_base, &insert->m_base);
        insert->m_base.m_dmltype = PG_PARSER_TRANSLOG_DMLTYPE_INSERT;
        insert->m_haspkey = multiinsert->m_haspkey;
        insert->m_relid = multiinsert->m_relid;
        insert->m_relfilenode = multiinsert->m_relfilenode;
        insert->m_valueCnt = multiinsert->m_valueCnt;
        insert->m_new_values = value;

        insertrow = rebuild_burstrow_init(insert->m_valueCnt);
        if (NULL == insertrow)
        {
            elog(RLOG_WARNING, "rebuild burst txn2bursts multiinsert burstrow init failed");
            return false;
        }
        insertrow->row = insert;
        insertrow->op = REBUILD_BURSTROWTYPE_INSERT;

        if (false == rebuild_burst_setmd5andmissing(insertrow, insert->m_new_values))
        {
            rebuild_burstrow_free(insertrow);
            elog(RLOG_WARNING, "rebuild burst txn2bursts multiinsert setmd5adnmissing failed");
            return false;
        }

        if (false == rebuild_burst_mergeinsert(burstnode, insertrow))
        {
            burstnode->dlinsertrows = dlist_put(burstnode->dlinsertrows, insertrow);
        }
        multiinsert->m_rows[rowindx].m_new_values = NULL;
    }

    return true;
}

/* Reorganize txn UPDATE content into burst */
static bool rebuild_burst_txn2bursts_update(rebuild_burst* burst, cache_sysdicts* sysdicts, void* row)
{
    bool                             error = false;
    bool                             mergeresult = false;
    rebuild_bursttable               table = {0};
    rebuild_burstrow*                delrow = NULL;
    rebuild_burstrow*                insertrow = NULL;
    rebuild_burstnode*               burstnode = NULL;
    pg_parser_translog_tbcol_values* update = NULL;

    update = (pg_parser_translog_tbcol_values*)row;

    table.oid = update->m_relid;
    table.schema = update->m_base.m_schemaname;
    table.table = update->m_base.m_tbname;

    if (false == rebuild_burst_getnode(sysdicts->by_class,
                                       sysdicts->by_attribute,
                                       sysdicts->by_index,
                                       burst,
                                       &burstnode,
                                       &table))
    {
        elog(RLOG_WARNING, "rebuild burst txn2bursts update getnode failed");
        return false;
    }

    /* Decompose into delete and insert */
    if (false == rebuild_burst_decomposeupdate(burstnode, &delrow, &insertrow, row))
    {
        elog(RLOG_WARNING, "rebuild burst txn2bursts update decomposeupdate failed");
        return false;
    }

    /* UPDATE's delete merges with insert */
    mergeresult = rebuild_burst_updatemergedelete(burstnode, delrow, &insertrow, &error);
    if (true == error)
    {
        rebuild_burstrow_free(delrow);
        rebuild_burstrow_free(insertrow);
        elog(RLOG_WARNING, "rebuild burst txn2bursts update mergedelete error");
        return false;
    }

    /* UPDATE's delete merges with insert - if merge fails with error, exit directly */
    if (true == error)
    {
        elog(RLOG_WARNING, "rebuild burst txn2bursts update mergedelete failed");
        return false;
    }

    /* UPDATE's delete merges with insert - if no matching insert found, add delete to list */
    if (false == mergeresult)
    {
        burstnode->dldeleterows = dlist_put(burstnode->dldeleterows, delrow);
    }

    /* UPDATE's insert merges with delete - if merge fails, add to list */
    if (false == rebuild_burst_mergeinsert(burstnode, insertrow))
    {
        burstnode->dlinsertrows = dlist_put(burstnode->dlinsertrows, insertrow);
    }

    return true;
}

/* Reorganize txn content into bursts */
bool rebuild_burst_txn2bursts(rebuild_burst* burst, cache_sysdicts* sysdicts, txn* txn)
{
    bool                          complete = false;
    ListCell*                     lc = NULL;
    ListCell*                     metadatalc = NULL;
    List*                         lststmt = NULL;
    dlistnode*                    dlnode = NULL;
    rebuild_bursttable*           table = NULL;
    rebuild_burstnode*            burstnode = NULL;
    catalogdata*                  catalog_data = NULL;
    catalog_class_value*          class = NULL;
    txnstmt_metadata*             metadatastmt = NULL;
    pg_parser_translog_tbcolbase* tbcolbase = NULL;
    rebuild_bursttable            dltable = {0};

    txnstmt*                      stmtnode = NULL;

    if (NULL == txn->stmts)
    {
        return true;
    }

    /* Reorganize */
    lststmt = txn->stmts;
    txn->stmts = NULL;
    foreach (lc, lststmt)
    {
        stmtnode = (txnstmt*)lfirst(lc);

        if (TXNSTMT_TYPE_DDL == stmtnode->type)
        {
            burst->number++;

            dlnode = (burst->dlbursttable == NULL) ? NULL : burst->dlbursttable->head;

            /* All tables' no increment based on burst->number as boundary */
            for (; NULL != dlnode; dlnode = dlnode->next)
            {
                table = (rebuild_bursttable*)dlnode->value;
                table->no = burst->number++;
            }

            burstnode = rebuild_burstnode_init();
            if (NULL == burstnode)
            {
                elog(RLOG_WARNING, "rebuild burst txn2bursts init burstnode failed");
                return false;
            }

            burstnode->table.no = burst->number++;
            burstnode->type = REBUILD_BURSTNODETYPE_OTHER;
            burstnode->stmt = stmtnode;
            burst->dlburstnodes = dlist_put(burst->dlburstnodes, burstnode);
        }
        else if (TXNSTMT_TYPE_METADATA == stmtnode->type)
        {
            metadatastmt = (txnstmt_metadata*)stmtnode->stmt;

            complete = false;
            metadatalc = metadatastmt->begin;
            while (1)
            {
                /* Apply system catalog */
                catalog_data = (catalogdata*)lfirst(metadatalc);
                cache_sysdicts_txnsysdicthisitem2cache(sysdicts, metadatalc);

                if (CATALOG_TYPE_CLASS == catalog_data->type)
                {
                    cache_sysdicts_clearsysdicthisbyclass(sysdicts, metadatalc);
                    class = (catalog_class_value*)catalog_data->catalog;
                    if (NULL == class)
                    {
                        elog(RLOG_WARNING, "rebuild burst txn2bursts catalog_data->catalog is null");
                        return false;
                    }
                    dltable.oid = class->class->oid;
                    dltable.schema = class->class->nspname.data;
                    dltable.table = class->class->relname.data;

                    table = rebuild_burst_gettable(burst->dlbursttable, &dltable, rebuild_bursttable_cmp);
                    if (NULL == table)
                    {
                        table = rebuild_bursttable_init();
                        if (NULL == table)
                        {
                            elog(RLOG_WARNING, "rebuild burst txn2bursts init bursttable failed");
                            return false;
                        }
                        table->oid = dltable.oid;
                        table->schema = rstrdup(dltable.schema);
                        table->table = rstrdup(dltable.table);
                        burst->dlbursttable = dlist_put(burst->dlbursttable, table);
                    }

                    table->no = burst->number++;
                    burstnode = rebuild_burstnode_init();
                    if (NULL == burstnode)
                    {
                        elog(RLOG_WARNING, "rebuild burst txn2bursts init burstnode failed");
                        return false;
                    }

                    burstnode->table.no = table->no;
                    burstnode->table.oid = class->class->oid;
                    burstnode->table.schema = rstrdup(class->class->nspname.data);
                    burstnode->table.table = rstrdup(class->class->relname.data);
                    if (false ==
                        rebuild_composekey(sysdicts->by_class, sysdicts->by_attribute, sysdicts->by_index, burstnode))
                    {
                        elog(RLOG_WARNING, "rebuild burst txn2bursts composekey failed");
                        return false;
                    }
                    burstnode->type = REBUILD_BURSTNODETYPE_OTHER;
                    burstnode->stmt = stmtnode;
                    burst->dlburstnodes = dlist_put(burst->dlburstnodes, burstnode);
                    /* Skip current node */
                    table->no = burst->number++;
                }
                else if (CATALOG_TYPE_DATABASE == catalog_data->type)
                {
                    catalog_database_value* database = NULL;
                    database = (catalog_database_value*)catalog_data->catalog;
                    if (NULL == database)
                    {
                        elog(RLOG_WARNING, "rebuild burst txn2bursts catalog_data->catalog is null");
                        return false;
                    }

                    burstnode = rebuild_burstnode_init();
                    if (NULL == burstnode)
                    {
                        elog(RLOG_WARNING, "rebuild burst txn2bursts init burstnode failed");
                        return false;
                    }

                    burstnode->table.no = burst->number++;
                    burstnode->type = REBUILD_BURSTNODETYPE_OTHER;
                    burstnode->stmt = stmtnode;
                    burst->dlburstnodes = dlist_put(burst->dlburstnodes, burstnode);
                }

                /* Only one */
                if (metadatalc == metadatastmt->end || true == complete)
                {
                    break;
                }
                /* Check if reached the last one */
                metadatalc = metadatalc->next;
                if (metadatalc == metadatastmt->end)
                {
                    complete = true;
                }
            }
        }
        else if (TXNSTMT_TYPE_DML == stmtnode->type)
        {
            tbcolbase = (pg_parser_translog_tbcolbase*)stmtnode->stmt;
            if (PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT == tbcolbase->m_dmltype)
            {
                if (false == rebuild_burst_txn2bursts_multiinsert(burst, sysdicts, stmtnode->stmt))
                {
                    elog(RLOG_WARNING, "rebuild burst txn2bursts multiinsert failed");
                    return false;
                }
            }
            else if (PG_PARSER_TRANSLOG_DMLTYPE_INSERT == tbcolbase->m_dmltype)
            {
                if (false == rebuild_burst_txn2bursts_insert(burst, sysdicts, stmtnode->stmt))
                {
                    elog(RLOG_WARNING, "rebuild burst txn2bursts insert failed");
                    return false;
                }
                stmtnode->stmt = NULL;
            }
            else if (PG_PARSER_TRANSLOG_DMLTYPE_DELETE == tbcolbase->m_dmltype)
            {
                if (false == rebuild_burst_txn2bursts_delete(burst, sysdicts, stmtnode->stmt))
                {
                    elog(RLOG_WARNING, "rebuild burst txn2bursts delete failed");
                    return false;
                }
                stmtnode->stmt = NULL;
            }
            else if (PG_PARSER_TRANSLOG_DMLTYPE_UPDATE == tbcolbase->m_dmltype)
            {
                if (false == rebuild_burst_txn2bursts_update(burst, sysdicts, stmtnode->stmt))
                {
                    elog(RLOG_WARNING, "rebuild burst txn2bursts update failed");
                    return false;
                }
                stmtnode->stmt = NULL;
            }
            /* Free stmtnode */
            txnstmt_free(stmtnode);
        }
        else
        {
            burstnode = rebuild_burstnode_init();
            if (NULL == burstnode)
            {
                elog(RLOG_WARNING, "rebuild burst txn2bursts init burstnode failed");
                return false;
            }

            burstnode->table.no = burst->number++;
            burstnode->type = REBUILD_BURSTNODETYPE_OTHER;
            burstnode->stmt = stmtnode;
            burst->dlburstnodes = dlist_put(burst->dlburstnodes, burstnode);
        }
        lc->data.ptr_value = NULL;
    }
    list_free(lststmt);

    return true;
}

/* Compare MD5 values for rebuild_burstrow */
static int rebuild_burst_comparemd5(const void* r1, const void* r2)
{
    rebuild_burstrow* row1 = NULL;
    rebuild_burstrow* row2 = NULL;

    row1 = *(rebuild_burstrow**)r1;
    row2 = *(rebuild_burstrow**)r2;

    return memcmp(row1->md5, row2->md5, 16);
}

/* Compare missing values for rebuild_burstrow */
static int rebuild_burst_comparemissing(const void* r1, const void* r2)
{
    rebuild_burstrow* row1 = NULL;
    rebuild_burstrow* row2 = NULL;

    row1 = *(rebuild_burstrow**)r1;
    row2 = *(rebuild_burstrow**)r2;

    /* Sort by missingcnt first */
    if (row1->missingcnt < row2->missingcnt)
    {
        return -1;
    }

    if (row1->missingcnt > row2->missingcnt)
    {
        return 1;
    }

    /* If missingcnt is the same, sort by missingmap */
    return memcmp(row1->missingmap, row2->missingmap, row1->missingmapsize);
}

/* Assemble PBE DELETE statement */
static bool rebuild_burst_assemblepbedelete(rebuild_burstnode* burstnode, txn* txn)
{
    bool                             need_and = false;
    bool                             need_select = true;
    int                              colindex = 0;
    int                              sortindex = 0;
    uint64                           rowcnt = 0;
    uint64                           len = 0;
    uint8*                           md5 = NULL;
    StringInfo                       str = NULL;
    dlistnode*                       dlnode = NULL;
    txnstmt*                         stmt = NULL;
    txnstmt_burst*                   stmtburst = NULL;
    rebuild_burstrow*                delrow = NULL;
    rebuild_burstrow**               sortrow = NULL;
    pg_parser_translog_tbcol_value*  values = NULL;
    pg_parser_translog_tbcol_values* delete = NULL;

    if (true == dlist_isnull(burstnode->dldeleterows))
    {
        return true;
    }

    stmtburst = txnstmt_burst_init();
    if (NULL == stmtburst)
    {
        elog(RLOG_WARNING, "rebuild burst assemblepbedeletes stmtburst init failed");
        return false;
    }
    stmtburst->optype = REBUILD_BURSTROWTYPE_DELETE;

    len = (sizeof(rebuild_burstrow*) * burstnode->dldeleterows->length);

    sortrow = rmalloc0(len);
    if (NULL == sortrow)
    {
        elog(RLOG_WARNING, "rebuild burst assemblepbedeletes sortrow oom");
        rfree(stmtburst);
        return false;
    }
    rmemset0(sortrow, 0, 0, len);

    for (dlnode = burstnode->dldeleterows->head; NULL != dlnode; dlnode = dlnode->next)
    {
        delrow = (rebuild_burstrow*)dlnode->value;
        sortrow[colindex] = delrow;
        colindex++;
    }

    /* Sort by MD5 grouping */
    qsort(sortrow, burstnode->dldeleterows->length, sizeof(rebuild_burstrow*), rebuild_burst_comparemd5);

    str = makeStringInfo();

    appendStringInfo(str, "DELETE FROM \"%s\".\"%s\" WHERE CTID IN (", burstnode->table.schema, burstnode->table.table);

    md5 = sortrow[0]->md5;
    for (sortindex = 0; sortindex < burstnode->dldeleterows->length; sortindex++)
    {
        delrow = sortrow[sortindex];
        delete = (pg_parser_translog_tbcol_values*)delrow->row;
        /* When column is dropped or may be null, do not set the column value */
        if (0 != memcmp(md5, delrow->md5, 16))
        {
            appendStringInfo(str, " LIMIT %lu)", rowcnt);
            appendStringInfo(str, " UNION ALL ");
            md5 = delrow->md5;
            rowcnt = 0;
            need_select = true;
        }

        if (true == need_select)
        {
            appendStringInfo(str,
                             "(SELECT CTID FROM \"%s\".\"%s\" WHERE ",
                             burstnode->table.schema,
                             burstnode->table.table);
            need_and = false;
            for (colindex = 0; colindex < delete->m_valueCnt; colindex++)
            {
                values = &delete->m_old_values[colindex];
                if (INFO_COL_MAY_NULL == values->m_info || INFO_COL_IS_DROPED == values->m_info ||
                    INFO_COL_IS_CUSTOM == values->m_info)
                {
                    continue;
                }

                if (INFO_COL_IS_NULL == values->m_info)
                {
                    if (need_and)
                    {
                        appendStringInfo(str, " AND ");
                    }
                    appendStringInfo(str, "\"%s\" IS NULL ", values->m_colName);
                    need_and = true;
                }
                else if (values->m_info == INFO_NOTHING)
                {
                    char* temp_str = strSpecialCharReplace((char*)values->m_value);
                    if (need_and)
                    {
                        appendStringInfo(str, " AND ");
                    }

                    appendStringInfo(str, "\"%s\" = ", values->m_colName);
                    if (temp_str)
                    {
                        appendStringInfo(str, "'%s'", temp_str);
                        rfree(temp_str);
                    }
                    else
                    {
                        appendStringInfo(str, "'%s'", (char*)values->m_value);
                    }

                    need_and = true;
                }
            }
            need_select = false;
        }
        rowcnt++;
        stmtburst->rows = dlist_put(stmtburst->rows, delete);
        delrow->row = NULL;
    }

    appendStringInfo(str, " LIMIT %lu)", rowcnt);
    appendStringInfo(str, ");");

    /* Initialize txnstmt */
    stmt = txnstmt_init();
    if (NULL == stmt)
    {
        elog(RLOG_WARNING, "rebuild burst assemblepbedeletes txnstmt init failed");
        rfree(stmtburst);
        rfree(sortrow);
        return false;
    }
    stmt->stmt = NULL;
    stmt->type = TXNSTMT_TYPE_BURST;

    stmtburst->batchcmd = (uint8*)str->data;
    stmt->stmt = stmtburst;
    txn->stmts = lappend(txn->stmts, stmt);
    stmt = NULL;
    str->data = NULL;
    deleteStringInfo(str);

    rfree(sortrow);
    return true;
}

/* Assemble PBE INSERT statement (all columns) */
static bool rebuild_burst_assemblepbeinsert(rebuild_burstnode* burstnode, txn* txn)
{
    bool                             need_comma = false;
    bool                             need_colname = true;
    int                              colindex = 0;
    StringInfo                       insertstr = NULL;
    StringInfo                       valuestr = NULL;
    dlistnode*                       dlnode = NULL;
    txnstmt*                         stmt = NULL;
    txnstmt_burst*                   stmtburst = NULL;
    rebuild_burstrow*                insertrow = NULL;
    pg_parser_translog_tbcol_value*  values = NULL;
    pg_parser_translog_tbcol_values* insert = NULL;

    if (true == dlist_isnull(burstnode->dlinsertrows))
    {
        return true;
    }

    stmtburst = txnstmt_burst_init();
    if (NULL == stmtburst)
    {
        elog(RLOG_WARNING, "rebuild burst assemblepbeinsert stmtburst init failed");
        return false;
    }
    stmtburst->optype = REBUILD_BURSTROWTYPE_INSERT;

    insertstr = makeStringInfo();
    valuestr = makeStringInfo();

    appendStringInfo(insertstr, "INSERT INTO \"%s\".\"%s\" (", burstnode->table.schema, burstnode->table.table);

    need_colname = true;
    for (dlnode = burstnode->dlinsertrows->head; dlnode != NULL; dlnode = dlnode->next)
    {
        insertrow = (rebuild_burstrow*)dlnode->value;
        insert = (pg_parser_translog_tbcol_values*)insertrow->row;

        need_comma = false;
        appendStringInfo(valuestr, "(");
        for (colindex = 0; colindex < insert->m_valueCnt; colindex++)
        {
            values = &insert->m_new_values[colindex];
            /* When column is dropped or may be null, do not set the column value */
            if (INFO_COL_MAY_NULL == values->m_info || INFO_COL_IS_DROPED == values->m_info)
            {
                continue;
            }

            if (true == need_colname)
            {
                if (need_comma)
                {
                    appendStringInfo(insertstr, ", ");
                }
                appendStringInfo(insertstr, "\"%s\"", values->m_colName);
            }

            if (INFO_COL_IS_NULL == values->m_info)
            {
                if (need_comma)
                {
                    appendStringInfo(valuestr, ", ");
                }
                appendStringInfo(valuestr, "NULL");
                need_comma = true;
            }
            else
            {
                char* temp_str = strSpecialCharReplace((char*)values->m_value);
                if (need_comma)
                {
                    appendStringInfo(valuestr, ", ");
                }
                if (temp_str)
                {
                    appendStringInfo(valuestr, "'%s'", temp_str);
                    rfree(temp_str);
                }
                else
                {
                    appendStringInfo(valuestr, "'%s'", (char*)values->m_value);
                }
                need_comma = true;
            }
        }
        need_colname = false;
        appendStringInfo(valuestr, ")");

        if (NULL != dlnode->next)
        {
            appendStringInfo(valuestr, ",");
        }

        stmtburst->rows = dlist_put(stmtburst->rows, insert);
        insertrow->row = NULL;
    }

    appendStringInfo(insertstr, ") VALUES ");
    appendStringInfo(valuestr, ";");

    /* Initialize txnstmt */
    stmt = txnstmt_init();
    if (NULL == stmt)
    {
        elog(RLOG_WARNING, "rebuild burst assemblepbeinsert stmt init failed");
        rfree(stmtburst);
        return false;
    }
    stmt->stmt = NULL;
    stmt->type = TXNSTMT_TYPE_BURST;

    appendStringInfo(insertstr, "%s", valuestr->data);

    stmtburst->batchcmd = (uint8*)insertstr->data;
    stmt->stmt = stmtburst;
    txn->stmts = lappend(txn->stmts, stmt);
    stmt = NULL;
    insertstr->data = NULL;
    deleteStringInfo(insertstr);
    deleteStringInfo(valuestr);

    return true;
}

/* Assemble burst DELETE statement (primary key) */
static bool rebuild_burst_assembledelete(rebuild_burstnode* burstnode, txn* txn)
{
    bool                             hasdelete = false;
    bool                             in_comma = false;
    bool                             need_comma = false;
    int                              colindex = 0;
    StringInfo                       str = NULL;
    dlistnode*                       dlnode = NULL;
    txnstmt*                         stmt = NULL;
    txnstmt_burst*                   stmtburst = NULL;
    rebuild_burstrow*                delrow = NULL;
    rebuild_burstcolumn*             column = NULL;
    pg_parser_translog_tbcol_value*  values = NULL;
    pg_parser_translog_tbcol_values* delete = NULL;

    if (true == dlist_isnull(burstnode->dldeleterows))
    {
        return true;
    }

    stmtburst = txnstmt_burst_init();
    if (NULL == stmtburst)
    {
        elog(RLOG_WARNING, "rebuild burst assembledelete stmtburst init failed");
        return false;
    }
    stmtburst->optype = burstnode->type;

    str = makeStringInfo();

    appendStringInfo(str, "DELETE FROM \"%s\".\"%s\" WHERE (", burstnode->table.schema, burstnode->table.table);

    /* Assemble constraint information */
    for (colindex = 0; colindex < burstnode->table.keycnt; colindex++)
    {
        column = &burstnode->table.keys[colindex];
        if (need_comma)
        {
            appendStringInfo(str, ", ");
        }
        appendStringInfo(str, "%s", column->colname);
        need_comma = true;
    }
    appendStringInfo(str, ") IN (");

    in_comma = false;
    /* Assemble values to delete */
    for (dlnode = burstnode->dldeleterows->head; dlnode != NULL; dlnode = dlnode->next)
    {
        delrow = (rebuild_burstrow*)dlnode->value;
        /* Need to remove from assembly */
        if (REBUILD_BURSTROWFLAG_REMOVEDELETE == delrow->flag)
        {
            continue;
        }

        delete = (pg_parser_translog_tbcol_values*)delrow->row;
        if (in_comma)
        {
            appendStringInfo(str, ",");
        }

        need_comma = false;
        appendStringInfo(str, "(");
        for (colindex = 0; colindex < burstnode->table.keycnt; colindex++)
        {
            column = &burstnode->table.keys[colindex];
            values = &delete->m_old_values[column->colno - 1];
            /* When column is dropped or may be null, do not set the column value */
            if (INFO_COL_MAY_NULL == values->m_info || INFO_COL_IS_DROPED == values->m_info ||
                INFO_COL_IS_CUSTOM == values->m_info)
            {
                continue;
            }

            if (INFO_COL_IS_NULL == values->m_info)
            {
                if (need_comma)
                {
                    appendStringInfo(str, ", ");
                }
                appendStringInfo(str, "NULL");
            }
            else
            {
                char* temp_str = strSpecialCharReplace((char*)values->m_value);
                if (need_comma)
                {
                    appendStringInfo(str, ", ");
                }
                if (temp_str)
                {
                    appendStringInfo(str, "'%s'", temp_str);
                    rfree(temp_str);
                }
                else
                {
                    appendStringInfo(str, "'%s'", (char*)values->m_value);
                }
            }
            need_comma = true;
        }
        appendStringInfo(str, ")");
        in_comma = true;

        stmtburst->rows = dlist_put(stmtburst->rows, delete);
        delrow->row = NULL;
        hasdelete = true;
    }

    if (false == hasdelete)
    {
        deleteStringInfo(str);
        txnstmt_burst_free(stmtburst);
        return true;
    }

    appendStringInfo(str, ");");

    /* Generate stmt */
    stmt = txnstmt_init();
    if (NULL == stmt)
    {
        elog(RLOG_WARNING, "rebuild burst assembledelete stmt init failed");
        rfree(stmtburst);
        return false;
    }
    stmt->stmt = NULL;
    stmt->type = TXNSTMT_TYPE_BURST;

    stmtburst->batchcmd = (uint8*)str->data;
    stmt->stmt = stmtburst;
    txn->stmts = lappend(txn->stmts, stmt);
    stmt = NULL;
    str->data = NULL;
    deleteStringInfo(str);

    return true;
}

/* Assemble burst INSERT statement - temp table, all columns, ON CONFLICT */
static bool rebuild_burst_assembleinsert(cache_sysdicts* sysdicts, rebuild_burstnode* burstnode, txn* txn)
{
    bool                             first = true;
    bool                             need_comma = false;
    bool                             conskey_comma = false;
    bool                             missing_comma = false;
    int                              colcnt = 0;
    int                              colindex = 0;
    int                              keyindex = 0;
    int                              missingmapsize = 0;
    int                              missingcnt = 0;
    int                              sortindex = 0;
    uint64                           len = 0;
    uint8*                           missingmap = NULL;
    char*                            type = NULL;
    List*                            lattrs = NULL;
    StringInfo                       conskeystr = NULL;
    StringInfo                       valuestr = NULL;
    StringInfo                       updatestr = NULL;
    StringInfo                       insestrstr = NULL;
    StringInfo                       conflictstr = NULL;
    dlistnode*                       dlnode = NULL;
    txnstmt*                         stmt = NULL;
    txnstmt_burst*                   conskeyburst = NULL;
    txnstmt_burst*                   insertburst = NULL;
    txnstmt_burst*                   conflictburst = NULL;
    rebuild_burstcolumn*             key = NULL;
    rebuild_burstrow**               sortrow = NULL;
    rebuild_burstrow**               tmprow = NULL;
    rebuild_burstrow*                insertrow = NULL;
    rebuild_bursttable*              table = NULL;
    pg_parser_translog_tbcol_value*  values = NULL;
    pg_parser_translog_tbcol_values* insert = NULL;

    if (true == dlist_isnull(burstnode->dlinsertrows))
    {
        return true;
    }

    table = &burstnode->table;

    len = (sizeof(rebuild_burstrow*) * burstnode->dlinsertrows->length);

    sortrow = rmalloc0(len);
    if (NULL == sortrow)
    {
        elog(RLOG_WARNING, "rebuild burst assembleinsert sortrow oom");
        return false;
    }
    rmemset0(sortrow, 0, 0, len);

    /* Get attributes */
    lattrs = (List*)attribute_getbyoid(burstnode->table.oid, sysdicts->by_attribute);

    if (NULL == lattrs || NULL == lattrs->head)
    {
        elog(RLOG_WARNING, "castor burst assembleinsert not find attribute by %lu", burstnode->table.oid);
        rfree(sortrow);
        return false;
    }

    /* Generate sort space, filter constraint/primary key modified data */
    for (dlnode = burstnode->dlinsertrows->head; NULL != dlnode; dlnode = dlnode->next)
    {
        insertrow = (rebuild_burstrow*)dlnode->value;
        insert = (pg_parser_translog_tbcol_values*)insertrow->row;
        /* Assemble constraint modification statement, temp table */
        if (REBUILD_BURSTROWFLAG_CHANGECONSKEY == insertrow->flag)
        {
            if (true == first)
            {
                conskeystr = makeStringInfo();
                insestrstr = makeStringInfo();
                valuestr = makeStringInfo();
                updatestr = makeStringInfo();

                conskeyburst = txnstmt_burst_init();
                if (NULL == conskeyburst)
                {
                    deleteStringInfo(conskeystr);
                    deleteStringInfo(insestrstr);
                    deleteStringInfo(valuestr);
                    deleteStringInfo(updatestr);
                    elog(RLOG_WARNING, "rebuild burst assembleinsert conskeyburst init failed");
                    rfree(sortrow);
                    return false;
                }
                conskeyburst->optype = REBUILD_BURSTROWTYPE_INSERT;

                appendStringInfo(conskeystr,
                                 "CREATE TEMP TABLE IF NOT EXISTS \"%s_burst\" (op text ",
                                 burstnode->table.table);
                appendStringInfo(insestrstr, "INSERT INTO \"%s_burst\" (op", burstnode->table.table);
                appendStringInfo(valuestr, "VALUES ('update' ");
                appendStringInfo(updatestr,
                                 "UPDATE \"%s\".\"%s\" SET ",
                                 burstnode->table.schema,
                                 burstnode->table.table);

                /* Assemble temp table primary key columns */
                for (keyindex = 0; keyindex < table->keycnt; keyindex++)
                {
                    key = &table->keys[keyindex];
                    type = rebuild_burst_gettypename(lattrs, sysdicts->by_type, key->coltype, key->colname);
                    if (NULL == type)
                    {
                        deleteStringInfo(conskeystr);
                        deleteStringInfo(insestrstr);
                        deleteStringInfo(valuestr);
                        deleteStringInfo(updatestr);
                        elog(RLOG_WARNING, "rebuild burst assembleinsert get typenam failed");
                        rfree(sortrow);
                        return false;
                    }
                    appendStringInfo(conskeystr, ", ");
                    appendStringInfo(conskeystr, "\"%s_before\" %s", key->colname, type);

                    appendStringInfo(insestrstr, ", ");
                    appendStringInfo(insestrstr, "\"%s_before\"", key->colname);

                    appendStringInfo(valuestr, ", ");
                    appendStringInfo(valuestr, "'%s'", (char*)insert->m_old_values[key->colno - 1].m_value);
                    rfree(type);
                    type = NULL;
                }

                /* Assemble temp table other columns and missing flag columns */
                need_comma = false;
                for (colindex = 0; colindex < insert->m_valueCnt; colindex++)
                {
                    values = &insert->m_new_values[colindex];
                    if (INFO_COL_IS_DROPED == values->m_info || INFO_COL_IS_CUSTOM == values->m_info)
                    {
                        continue;
                    }
                    type = rebuild_burst_gettypename(lattrs, sysdicts->by_type, values->m_coltype, values->m_colName);
                    if (NULL == type)
                    {
                        deleteStringInfo(conskeystr);
                        deleteStringInfo(insestrstr);
                        deleteStringInfo(valuestr);
                        deleteStringInfo(updatestr);
                        elog(RLOG_WARNING, "rebuild burst assembleinsert get typenam failed");
                        return false;
                    }
                    if (need_comma)
                    {
                        appendStringInfo(updatestr, ", ");
                    }

                    appendStringInfo(conskeystr, ", ");
                    appendStringInfo(conskeystr,
                                     "\"%s\" %s, \"%s_%s_missing\" char",
                                     values->m_colName,
                                     type,
                                     burstnode->table.table,
                                     values->m_colName);
                    rfree(type);
                    type = NULL;

                    appendStringInfo(insestrstr, ", ");
                    appendStringInfo(insestrstr,
                                     "\"%s\", \"%s_%s_missing\"",
                                     values->m_colName,
                                     burstnode->table.table,
                                     values->m_colName);

                    appendStringInfo(valuestr, ", ");
                    if (INFO_COL_MAY_NULL == values->m_info)
                    {
                        appendStringInfo(valuestr, "NULL, 't'");
                    }
                    else if (INFO_COL_IS_NULL == values->m_info)
                    {
                        appendStringInfo(valuestr, "NULL, 'f'");
                    }
                    else
                    {
                        appendStringInfo(valuestr, "'%s', 'f'", (char*)values->m_value);
                    }

                    /* Assemble temp table update statement */
                    if (true == rebuild_burst_colisconskey(table, values->m_colName))
                    {
                        appendStringInfo(updatestr, "\"%s\" = b.\"%s\"", values->m_colName, values->m_colName);
                    }
                    else
                    {
                        appendStringInfo(updatestr,
                                         "\"%s\" = CASE WHEN \"%s_%s_missing\" = 'f' THEN b.\"%s\" "
                                         "ELSE \"%s\".\"%s\".\"%s\" END",
                                         values->m_colName,
                                         burstnode->table.table,
                                         values->m_colName,
                                         values->m_colName,
                                         burstnode->table.schema,
                                         burstnode->table.table,
                                         values->m_colName);
                    }
                    need_comma = true;
                }
                appendStringInfo(conskeystr, ");\nTRUNCATE TABLE \"%s_burst\";\n", burstnode->table.table);
                appendStringInfo(insestrstr, ")");
                appendStringInfo(valuestr, ")");

                /* Assemble temp table UPDATE WHERE condition */
                appendStringInfo(updatestr, " FROM \"%s_burst\" b WHERE", burstnode->table.table);
                for (keyindex = 0; keyindex < table->keycnt; keyindex++)
                {
                    key = &table->keys[keyindex];
                    appendStringInfo(updatestr,
                                     " \"%s\".\"%s\".\"%s\" = b.\"%s_before\" AND",
                                     burstnode->table.schema,
                                     burstnode->table.table,
                                     key->colname,
                                     key->colname);
                }
                appendStringInfo(updatestr, " b.op = 'update';");
                appendStringInfo(conskeystr, "%s %s", insestrstr->data, valuestr->data);

                deleteStringInfo(insestrstr);
                deleteStringInfo(valuestr);
                insestrstr = NULL;
                valuestr = NULL;
                first = false;
                continue;
            }

            /* Assemble temp table update WHERE condition */
            need_comma = false;
            appendStringInfo(conskeystr, ", ('update' ");
            for (colindex = 0; colindex < insert->m_valueCnt; colindex++)
            {
                values = &insert->m_new_values[colindex];
                for (keyindex = 0; keyindex < table->keycnt; keyindex++)
                {
                    key = &table->keys[keyindex];
                    appendStringInfo(conskeystr, ", ");
                    appendStringInfo(conskeystr, "'%s'", (char*)insert->m_old_values[key->colno - 1].m_value);
                    need_comma = true;
                }

                for (colindex = 0; colindex < insert->m_valueCnt; colindex++)
                {
                    values = &insert->m_new_values[colindex];
                    if (INFO_COL_IS_DROPED == values->m_info || INFO_COL_IS_CUSTOM == values->m_info)
                    {
                        continue;
                    }
                    appendStringInfo(conskeystr, ", ");
                    if (INFO_COL_MAY_NULL == values->m_info)
                    {
                        appendStringInfo(conskeystr, "NULL, 't'");
                    }
                    else if (INFO_COL_IS_NULL == values->m_info)
                    {
                        appendStringInfo(conskeystr, "NULL, 'f'");
                    }
                    else
                    {
                        appendStringInfo(conskeystr, "'%s', 'f'", (char*)values->m_value);
                    }
                }
                appendStringInfo(conskeystr, ")");
            }
            conskeyburst->rows = dlist_put(conskeyburst->rows, insertrow->row);
            insertrow->row = NULL;
            continue;
        }
        sortrow[colcnt] = insertrow;
        colcnt++;
    }

    /* Generate stmt using temp table method and add to transaction */
    if (NULL != conskeystr && NULL != updatestr)
    {
        /* Initialize txnstmt */
        stmt = txnstmt_init();
        if (NULL == stmt)
        {
            elog(RLOG_WARNING, "rebuild burst assembleinsert stmt init failed");
            rfree(conskeyburst);
            deleteStringInfo(conskeystr);
            deleteStringInfo(updatestr);
            rfree(sortrow);
            return false;
        }
        stmt->stmt = NULL;
        stmt->type = TXNSTMT_TYPE_BURST;
        appendStringInfo(conskeystr, ";\n %s \n DROP TABLE \"%s_burst\" ", updatestr->data, burstnode->table.table);

        conskeyburst->batchcmd = (uint8*)conskeystr->data;
        conskeystr->data = NULL;
        deleteStringInfo(conskeystr);
        deleteStringInfo(updatestr);
        conskeystr = NULL;
        updatestr = NULL;
        stmt->stmt = conskeyburst;
        txn->stmts = lappend(txn->stmts, (void*)stmt);
        stmt = NULL;
    }

    /* Reallocate sort space based on count after removing constraint/primary key modifications */
    len = (sizeof(rebuild_burstrow*) * colcnt);
    tmprow = rmalloc0(len);
    if (NULL == tmprow)
    {
        return false;
    }
    rmemset0(tmprow, 0, 0, len);
    rmemcpy0(tmprow, 0, sortrow, len);
    rfree(sortrow);
    sortrow = tmprow;
    tmprow = NULL;

    /* Sort by missingmap */
    qsort(sortrow, colcnt, sizeof(rebuild_burstrow*), rebuild_burst_comparemissing);

    /* Set initial missing value */
    missingmapsize = 0;
    missingcnt = -1;
    missingmap = NULL;

    /* Build INSERT statement based on sorted data */
    first = true;
    for (sortindex = 0; sortindex < colcnt; sortindex++)
    {
        insertrow = sortrow[sortindex];
        insert = (pg_parser_translog_tbcol_values*)insertrow->row;

        /* Original insert statement inserted directly */
        if (insertrow->op == REBUILD_BURSTROWTYPE_INSERT)
        {
            if (true == first)
            {
                insestrstr = makeStringInfo();
                valuestr = makeStringInfo();
                appendStringInfo(insestrstr,
                                 "INSERT INTO \"%s\".\"%s\" (",
                                 burstnode->table.schema,
                                 burstnode->table.table);
                appendStringInfo(valuestr, "VALUES (");

                insertburst = txnstmt_burst_init();
                if (NULL == insertburst)
                {
                    elog(RLOG_WARNING, "rebuild burst assembleinsert insertburst init failed");
                    rfree(sortrow);
                    return false;
                }
                insertburst->optype = REBUILD_BURSTROWTYPE_INSERT;

                need_comma = false;
                for (colindex = 0; colindex < insert->m_valueCnt; colindex++)
                {
                    values = &insert->m_new_values[colindex];
                    /* Types not processed */
                    if (INFO_COL_MAY_NULL == values->m_info || INFO_COL_IS_DROPED == values->m_info ||
                        INFO_COL_IS_CUSTOM == values->m_info)
                    {
                        continue;
                    }

                    if (need_comma)
                    {
                        appendStringInfo(insestrstr, ", ");
                        appendStringInfo(valuestr, ", ");
                    }

                    if (INFO_COL_IS_NULL == values->m_info)
                    {
                        appendStringInfo(insestrstr, "\"%s\"", values->m_colName);
                        appendStringInfo(valuestr, "NULL");
                    }
                    else if (values->m_info == INFO_NOTHING)
                    {
                        char* temp_str = strSpecialCharReplace((char*)values->m_value);

                        appendStringInfo(insestrstr, "\"%s\"", values->m_colName);
                        if (temp_str)
                        {
                            appendStringInfo(valuestr, "'%s'", temp_str);
                            rfree(temp_str);
                        }
                        else
                        {
                            appendStringInfo(valuestr, "'%s'", (char*)values->m_value);
                        }
                    }
                    else
                    {
                        rfree(sortrow);
                        deleteStringInfo(insestrstr);
                        deleteStringInfo(valuestr);
                        elog(RLOG_WARNING, "rebuild burst assembleinsert invalid values->m_info");
                        return false;
                    }

                    need_comma = true;
                }
                appendStringInfo(valuestr, ")");
                appendStringInfo(insestrstr, ") %s", valuestr->data);
                deleteStringInfo(valuestr);
                valuestr = NULL;
                first = false;
                insertburst->rows = dlist_put(insertburst->rows, insertrow->row);
                insertrow->row = NULL;
                continue;
            }

            appendStringInfo(insestrstr, ", (");
            need_comma = false;
            for (colindex = 0; colindex < insert->m_valueCnt; colindex++)
            {
                values = &insert->m_new_values[colindex];
                if (INFO_COL_MAY_NULL == values->m_info || INFO_COL_IS_DROPED == values->m_info ||
                    INFO_COL_IS_CUSTOM == values->m_info)
                {
                    continue;
                }

                if (need_comma)
                {
                    appendStringInfo(insestrstr, ", ");
                }

                if (INFO_COL_IS_NULL == values->m_info)
                {
                    appendStringInfo(insestrstr, "NULL");
                }
                else if (values->m_info == INFO_NOTHING)
                {
                    char* temp_str = strSpecialCharReplace((char*)values->m_value);
                    if (temp_str)
                    {
                        appendStringInfo(insestrstr, "'%s'", temp_str);
                        rfree(temp_str);
                    }
                    else
                    {
                        appendStringInfo(insestrstr, "'%s'", (char*)values->m_value);
                    }
                }
                else
                {
                    rfree(sortrow);
                    deleteStringInfo(insestrstr);
                    deleteStringInfo(valuestr);
                    insestrstr = NULL;
                    valuestr = NULL;
                    return false;
                }
                need_comma = true;
            }
            appendStringInfo(insestrstr, ")");

            insertburst->rows = dlist_put(insertburst->rows, insertrow->row);
            insertrow->row = NULL;
            continue;
        }

        /* INSERT from UPDATE inserted via ON CONFLICT */
        if (missingcnt != insertrow->missingcnt || memcmp(missingmap, insertrow->missingmap, missingmapsize))
        {
            /* Missingmap changed, regenerate statement */
            missing_comma = false;
            if (NULL != conflictstr && NULL != valuestr)
            {
                /* Add previous statement to txn */
                stmt = txnstmt_init();
                if (NULL == stmt)
                {
                    deleteStringInfo(valuestr);
                    deleteStringInfo(conflictstr);
                    rfree(sortrow);
                    return false;
                }
                stmt->stmt = NULL;
                stmt->type = TXNSTMT_TYPE_BURST;
                appendStringInfo(conflictstr, " %s", valuestr->data);
                elog(RLOG_WARNING, "burst on  conflict: %s ", conflictstr->data);
                deleteStringInfo(valuestr);
                valuestr = NULL;
                conflictburst->batchcmd = (uint8*)conflictstr->data;
                conflictstr->data = NULL;
                deleteStringInfo(conflictstr);
                conflictstr = NULL;
                stmt->stmt = conflictburst;
                txn->stmts = lappend(txn->stmts, (void*)stmt);
                stmt = NULL;
                conflictburst = NULL;
            }

            /* Assemble INSERT from UPDATE via ON CONFLICT */
            conflictstr = makeStringInfo();
            valuestr = makeStringInfo();
            conflictburst = txnstmt_burst_init();
            if (NULL == conflictburst)
            {
                rfree(sortrow);
                return false;
            }
            conflictburst->optype = REBUILD_BURSTROWTYPE_UPDATE;

            appendStringInfo(conflictstr,
                             "INSERT INTO \"%s\".\"%s\" (",
                             burstnode->table.schema,
                             burstnode->table.table);
            appendStringInfo(valuestr, "ON CONFLICT (");

            /* Assemble ON CONFLICT (id, ...) */
            need_comma = false;
            for (keyindex = 0; keyindex < table->keycnt; keyindex++)
            {
                key = &table->keys[keyindex];
                if (need_comma)
                {
                    appendStringInfo(valuestr, ", ");
                }
                appendStringInfo(valuestr, "\"%s\"", key->colname);
                need_comma = true;
            }
            appendStringInfo(valuestr, ") DO UPDATE SET ");

            need_comma = false;
            /* Assemble column names */
            for (colindex = 0; colindex < insert->m_valueCnt; colindex++)
            {
                values = &insert->m_new_values[colindex];
                if (INFO_COL_IS_DROPED == values->m_info || INFO_COL_IS_CUSTOM == values->m_info ||
                    INFO_COL_MAY_NULL == values->m_info)
                {
                    continue;
                }

                if (need_comma)
                {
                    appendStringInfo(conflictstr, ", ");
                }
                appendStringInfo(conflictstr, "\"%s\"", values->m_colName);

                /* Assemble DO UPDATE SET statement */
                if (false == rebuild_burst_colisconskey(table, values->m_colName))
                {
                    if (conskey_comma)
                    {
                        appendStringInfo(valuestr, ", ");
                    }

                    appendStringInfo(valuestr, "\"%s\" = EXCLUDED.\"%s\" ", values->m_colName, values->m_colName);
                    conskey_comma = true;
                }
                need_comma = true;
            }
            appendStringInfo(conflictstr, ") VALUES ");
            appendStringInfo(valuestr, ";");

            missingmapsize = sortrow[sortindex]->missingmapsize;
            missingcnt = sortrow[sortindex]->missingcnt;
            missingmap = sortrow[sortindex]->missingmap;
        }

        if (missing_comma)
        {
            appendStringInfo(conflictstr, ",");
        }

        appendStringInfo(conflictstr, "(");
        need_comma = false;

        /* Assemble column values */
        for (colindex = 0; colindex < insert->m_valueCnt; colindex++)
        {
            values = &insert->m_new_values[colindex];
            if (INFO_COL_IS_DROPED == values->m_info || INFO_COL_IS_CUSTOM == values->m_info ||
                INFO_COL_MAY_NULL == values->m_info)
            {
                continue;
            }
            if (need_comma)
            {
                appendStringInfo(conflictstr, ", ");
            }

            if (INFO_COL_IS_NULL == values->m_info)
            {
                appendStringInfo(conflictstr, "NULL");
            }
            else
            {
                appendStringInfo(conflictstr, "'%s'", (char*)values->m_value);
            }
            need_comma = true;
        }

        appendStringInfo(conflictstr, ")");

        missing_comma = true;
        conflictburst->rows = dlist_put(conflictburst->rows, insertrow->row);
        insertrow->row = NULL;
    }

    /* Add last statement to txn */
    if (NULL != conflictstr && NULL != valuestr)
    {
        stmt = txnstmt_init();
        if (NULL == stmt)
        {
            rfree(sortrow);
            return false;
        }
        stmt->stmt = NULL;
        stmt->type = TXNSTMT_TYPE_BURST;
        appendStringInfo(conflictstr, " %s", valuestr->data);
        deleteStringInfo(valuestr);
        valuestr = NULL;
        conflictburst->batchcmd = (uint8*)conflictstr->data;
        conflictstr->data = NULL;
        deleteStringInfo(conflictstr);
        stmt->stmt = conflictburst;
        txn->stmts = lappend(txn->stmts, (void*)stmt);
        conflictburst = NULL;
        stmt = NULL;
    }

    if (NULL != insestrstr)
    {
        /* Initialize txnstmt */
        stmt = txnstmt_init();
        if (NULL == stmt)
        {
            rfree(sortrow);
            return false;
        }
        stmt->stmt = NULL;
        stmt->type = TXNSTMT_TYPE_BURST;

        appendStringInfo(insestrstr, ";");
        insertburst->batchcmd = (uint8*)insestrstr->data;
        insestrstr->data = NULL;
        deleteStringInfo(insestrstr);
        stmt->stmt = insertburst;
        txn->stmts = lappend(txn->stmts, (void*)stmt);
        conflictburst = NULL;
        stmt = NULL;
    }

    rfree(sortrow);
    return true;
}

/* Assemble statements from burstnode */
bool rebuild_burst_bursts2stmt(rebuild_burst* burst, cache_sysdicts* sysdicts, txn* txn)
{
    dlistnode*         dlnode = NULL;
    rebuild_burstnode* burstnode = NULL;
    if (true == dlist_isnull(burst->dlburstnodes))
    {
        return true;
    }

    /* Iterate through burstnode */
    for (dlnode = burst->dlburstnodes->head; NULL != dlnode; dlnode = dlnode->next)
    {
        burstnode = (rebuild_burstnode*)dlnode->value;
        /* meta/ddl added directly to transaction */
        if (REBUILD_BURSTNODETYPE_OTHER == burstnode->type)
        {
            txn->stmts = lappend(txn->stmts, burstnode->stmt);
            burstnode->stmt = NULL;
            continue;
        }

        if (REBUILD_BURSTNODEFLAG_NOINDEX == burstnode->flag)
        {
            if (false == rebuild_burst_assemblepbedelete(burstnode, txn))
            {
                elog(RLOG_WARNING, "rebuild burst assemblepbedelete failed");
                return false;
            }

            if (false == rebuild_burst_assemblepbeinsert(burstnode, txn))
            {
                elog(RLOG_WARNING, "rebuild burst assemblepbeinsert failed");
                return false;
            }
        }
        else
        {
            if (false == rebuild_burst_assembledelete(burstnode, txn))
            {
                elog(RLOG_WARNING, "rebuild burst assembledelete failed");
                return false;
            }

            if (false == rebuild_burst_assembleinsert(sysdicts, burstnode, txn))
            {
                elog(RLOG_WARNING, "rebuild burst assembleinsert failed");
                return false;
            }
        }
    }
    return true;
}

/* Free burstcolumn resources */
void rebuild_burstcolumn_free(rebuild_burstcolumn* burstcolumn, int colcnt)
{
    int                 colindex = 0;
    rebuild_burstcolumn column = {'\0'};

    if (NULL == burstcolumn)
    {
        return;
    }

    for (colindex = 0; colindex < colcnt; colindex++)
    {
        column = burstcolumn[colindex];
        if (column.colname)
        {
            rfree(column.colname);
        }
    }

    rfree(burstcolumn);

    return;
}

/* Free burstrow resources */
void rebuild_burstrow_free(void* args)
{
    rebuild_burstrow* burstrow = NULL;

    if (NULL == args)
    {
        return;
    }

    burstrow = (rebuild_burstrow*)args;

    if (burstrow->missingmap)
    {
        rfree(burstrow->missingmap);
    }

    if (burstrow->row)
    {
        heap_free_trans_result((pg_parser_translog_tbcolbase*)burstrow->row);
    }

    rfree(burstrow);
    return;
}

/* Free bursttable resources - does not free bursttable within function */
void rebuild_bursttable_free(void* args)
{
    rebuild_bursttable* bursttable = NULL;

    if (NULL == args)
    {
        return;
    }

    bursttable = (rebuild_bursttable*)args;

    if (bursttable->schema)
    {
        rfree(bursttable->schema);
    }

    if (bursttable->table)
    {
        rfree(bursttable->table);
    }

    if (bursttable->keys)
    {
        rebuild_burstcolumn_free(bursttable->keys, bursttable->keycnt);
    }
    rfree(bursttable);
    return;
}

/* Free burstnode resources */
void rebuild_burstnode_free(void* args)
{
    rebuild_burstnode* burstnode = NULL;

    if (NULL == args)
    {
        return;
    }

    burstnode = (rebuild_burstnode*)args;

    if (burstnode->table.schema)
    {
        rfree(burstnode->table.schema);
    }

    if (burstnode->table.table)
    {
        rfree(burstnode->table.table);
    }

    if (burstnode->table.keys)
    {
        rebuild_burstcolumn_free(burstnode->table.keys, burstnode->table.keycnt);
    }

    dlist_free(burstnode->dldeleterows, rebuild_burstrow_free);
    dlist_free(burstnode->dlinsertrows, rebuild_burstrow_free);

    if (burstnode->stmt)
    {
        txnstmt_free(burstnode->stmt);
    }

    rfree(burstnode);
    return;
}

/* Free burst resources */
void rebuild_burst_free(void* args)
{
    rebuild_burst* burst = NULL;

    if (NULL == args)
    {
        return;
    }

    burst = (rebuild_burst*)args;

    dlist_free(burst->dlbursttable, rebuild_bursttable_free);
    dlist_free(burst->dlburstnodes, rebuild_burstnode_free);

    rfree(burst);
    return;
}
