#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/dlist/dlist.h"
#include "utils/varstr/varstr.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/string/stringinfo.h"
#include "utils/encryption/md5/ripple_encryption_md5.h"
#include "stmts/ripple_txnstmt.h"
#include "stmts/ripple_txnstmt_burst.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_txn.h"
#include "catalog/ripple_catalog.h"
#include "catalog/ripple_class.h"
#include "catalog/ripple_index.h"
#include "catalog/ripple_attribute.h"
#include "rebuild/ripple_rebuild_burst.h"
#include "works/parserwork/wal/ripple_decode_heap_util.h"

/* burstcolumn 初始化 */
ripple_rebuild_burstcolumn* ripple_rebuild_burstcolumn_init(int colcnt)
{
    ripple_rebuild_burstcolumn* burstcolumn = NULL;

    if (0 == colcnt)
    {
        elog(RLOG_WARNING, "rebuild burstcolumn init colcnt is 0");
        return NULL;
    }

    burstcolumn = (ripple_rebuild_burstcolumn*)rmalloc0((sizeof(ripple_rebuild_burstcolumn) * colcnt));
    if (NULL == burstcolumn)
    {
        elog(RLOG_WARNING, "rebuild burstcolumn init oom");
        return NULL;
    }
    rmemset0(burstcolumn, 0, '\0', (sizeof(ripple_rebuild_burstcolumn) * colcnt));

    return burstcolumn;
}

/* burstrow 初始化 */
ripple_rebuild_burstrow* ripple_rebuild_burstrow_init(int colcnt)
{
    ripple_rebuild_burstrow* burstrow = NULL;

    if (0 == colcnt)
    {
        elog(RLOG_WARNING, "rebuild burstrow init colcnt is 0");
        return NULL;
    }

    burstrow = (ripple_rebuild_burstrow*)rmalloc0(sizeof(ripple_rebuild_burstrow));
    if (NULL == burstrow)
    {
        elog(RLOG_WARNING, "rebuild burstrow init oom");
        return NULL;
    }
    rmemset0(burstrow, 0, '\0', sizeof(ripple_rebuild_burstrow));

    burstrow->flag = RIPPLE_REBUILD_BURSTROWFLAG_NOP;
    burstrow->op = RIPPLE_REBUILD_BURSTROWTYPE_INVALID;
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

/* bursttable 初始化 */
ripple_rebuild_bursttable* ripple_rebuild_bursttable_init(void)
{
    ripple_rebuild_bursttable* bursttable = NULL;

    bursttable = (ripple_rebuild_bursttable*)rmalloc0(sizeof(ripple_rebuild_bursttable));
    if (NULL == bursttable)
    {
        elog(RLOG_WARNING, "rebuild bursttable init oom");
        return NULL;
    }
    rmemset0(bursttable, 0, '\0', sizeof(ripple_rebuild_bursttable));

    bursttable->keycnt = 0;
    bursttable->no = 0;
    bursttable->oid = InvalidOid;
    bursttable->schema = NULL;
    bursttable->table = NULL;
    bursttable->keys = NULL;

    return bursttable;
}

/* burstnode 初始化 */
ripple_rebuild_burstnode* ripple_rebuild_burstnode_init(void)
{
    ripple_rebuild_burstnode* burstnode = NULL;

    burstnode = (ripple_rebuild_burstnode*)rmalloc0(sizeof(ripple_rebuild_burstnode));
    if (NULL == burstnode)
    {
        elog(RLOG_WARNING, "rebuild burstnode init oom");
        return NULL;
    }
    rmemset0(burstnode, 0, '\0', sizeof(ripple_rebuild_burstnode));

    burstnode->flag = 0;
    burstnode->type = RIPPLE_REBUILD_BURSTNODETYPE_NOP;
    burstnode->stmt = NULL;
    burstnode->dldeleterows = NULL;
    burstnode->dlinsertrows = NULL;
    rmemset1(&burstnode->table, 0, 0, sizeof(ripple_rebuild_bursttable));

    return burstnode;
}

/* burst 初始化 */
ripple_rebuild_burst* ripple_rebuild_burst_init(void)
{
    ripple_rebuild_burst* burst = NULL;

    burst = (ripple_rebuild_burst*)rmalloc0(sizeof(ripple_rebuild_burst));
    if (NULL == burst)
    {
        elog(RLOG_WARNING, "rebuild burst init oom");
        return NULL;
    }
    rmemset0(burst, 0, '\0', sizeof(ripple_rebuild_burst));
    
    burst->number = 0;
    burst->dlburstnodes = NULL;
    burst->dlbursttable = NULL;

    return burst;
}

/* 计算MD5值和missingmap */
static bool ripple_rebuild_burst_setmd5andmissing(ripple_rebuild_burstrow* row, xk_pg_parser_translog_tbcol_value* value)
{
    MD5_CTX md5;
    int colindex                                        = 0;
    varstr* vstr                                        = NULL;
    xk_pg_parser_translog_tbcol_values* colvalues       = NULL;

    colvalues = (xk_pg_parser_translog_tbcol_values*)row->row;

    vstr = varstr_init(256);
    if (NULL == vstr)
    {
        elog(RLOG_WARNING,"rebuild burst setmd5adnmissing init varstr oom");
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
        MD5Update(&md5, (uint8_t *)vstr->data, vstr->start);
        MD5Final((uint8_t *)row->md5, &md5);
    }

    varstr_free(vstr);

    return true;
}

/* bursttable 比较函数 */
int ripple_rebuild_bursttable_cmp(void* s1, void* s2)
{
    ripple_rebuild_bursttable* table1 = NULL;
    ripple_rebuild_bursttable* table2 = NULL;

    table1 = (ripple_rebuild_bursttable*)s1;
    table2 = (ripple_rebuild_bursttable*)s2;

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

/* burstnode 与bursttable 比较函数 */
int ripple_rebuild_burstnode_tablecmp(void* s1, void* s2)
{
    ripple_rebuild_burstnode* node      = NULL;
    ripple_rebuild_bursttable* table    = NULL;

    table = (ripple_rebuild_bursttable*)s1;
    node = (ripple_rebuild_burstnode*)s2;

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

/* 判断某个列是否为约束列/主键列 */
static bool ripple_rebuild_burst_colisconskey(ripple_rebuild_bursttable *table, char* colname)
{
    int keyindex = 0;
    ripple_rebuild_burstcolumn* key = NULL;
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

/* 根据typeid获取typename */
static char * ripple_rebuild_burst_gettypename(List* lattrs, Oid typeoid, char* colname)
{
    bool find                                   = false;
    int typmod                                  = -1;
    ListCell* attrlc                            = NULL;
    xk_pg_sysdict_Form_pg_attribute attr        = NULL;
    StringInfoData result                       = {0};

    foreach(attrlc, lattrs)
    {
        attr = (xk_pg_sysdict_Form_pg_attribute)lfirst(attrlc);
        if (0 == strcmp(colname, attr->attname.data))
        {
            find = true;
            typmod = attr->atttypmod;
            break;
        }
    }

    if (false == find)
    {
        elog(RLOG_WARNING, "ripple rebuild composekey not find attribute attnum %s", colname);
        return NULL;
    }

    initStringInfo(&result);

    switch (typeoid)
    {
        case XK_PG_SYSDICT_BPCHAROID:
            if (typmod == -1)
                appendStringInfoString(&result, "CHAR");
            else
                appendStringInfo(&result, "CHAR(%d)", typmod - VARHDRSZ);
            break;
        case XK_PG_SYSDICT_TEXTOID:
            appendStringInfo(&result, "TEXT");
            break;
        case XK_PG_SYSDICT_VARCHAROID:
            if (typmod == -1)
                appendStringInfoString(&result, "VARCHAR");
            else
                appendStringInfo(&result, "VARCHAR(%d)", typmod - VARHDRSZ);
            break;
        case XK_PG_SYSDICT_NUMERICOID:
            if (typmod == -1)
                appendStringInfoString(&result, "NUMERIC");
            else
                appendStringInfo(&result, "NUMERIC(%d, %d)",
                                 ((typmod - VARHDRSZ) >> 16) & 0xffff,
                                 (typmod - VARHDRSZ) & 0xffff);
            break;
        case XK_PG_SYSDICT_INT4OID:
            appendStringInfoString(&result, "INTEGER");
            break;
        case XK_PG_SYSDICT_INT2OID:
            appendStringInfoString(&result, "SMALLINT");
            break;
        case XK_PG_SYSDICT_INT8OID:
            appendStringInfoString(&result, "BIGINT");
            break;
        case XK_PG_SYSDICT_FLOAT4OID:
            appendStringInfoString(&result, "REAL");
            break;
        case XK_PG_SYSDICT_FLOAT8OID:
            appendStringInfoString(&result, "DOUBLE");
            break;
        case XK_PG_SYSDICT_BOOLOID:
            appendStringInfoString(&result, "BOOLEAN");
            break;
        case XK_PG_SYSDICT_TIMEOID:
            if (typmod == -1)
                appendStringInfoString(&result, "TIME");
            else
                appendStringInfo(&result, "TIME(%d)", typmod);
            break;
        case XK_PG_SYSDICT_TIMETZOID:
            if (typmod == -1)
                appendStringInfoString(&result, "TIME WITH TIME ZONE");
            else
                appendStringInfo(&result, "TIME(%d) WITH TIME ZONE", typmod);
            break;
        case XK_PG_SYSDICT_TIMESTAMPOID:
            if (typmod == -1)
                appendStringInfoString(&result, "TIMESTAMP");
            else
                appendStringInfo(&result, "TIMESTAMP(%d)", typmod);
            break;
        case XK_PG_SYSDICT_TIMESTAMPTZOID:
            if (typmod == -1)
                appendStringInfoString(&result, "TIMESTAMPWITH TIME ZONE");
            else
                appendStringInfo(&result, "TIMESTAMP(%d) WITH TIME ZONE", typmod);
            break;
        case XK_PG_SYSDICT_DATEOID:
            appendStringInfoString(&result, "DATE");
            break;
        case XK_PG_SYSDICT_XMLOID:
            appendStringInfoString(&result, "XML");
            break;
        case XK_PG_SYSDICT_UUIDOID:
            appendStringInfoString(&result, "UUID");
            break;
        default:
            elog(RLOG_WARNING, "cache lookup failed for type %u", typeoid);
            return NULL;
    }
    return result.data;
}

/* 补全missing值 */
static bool ripple_rebuild_burst_updatematchdata(ripple_rebuild_burstnode* burstnode,
                                                 ripple_rebuild_burstrow* delrow,
                                                 ripple_rebuild_burstrow* updaterow)
{
    bool same                                           = true;
    int keycnt                                          = 0;
    int keyindex                                        = 0;
    int key                                             = 0;
    dlistnode* dlnode                                   = NULL;
    ripple_rebuild_burstcolumn* keys                    = NULL;
    ripple_rebuild_burstrow* insertrow                  = NULL;
    xk_pg_parser_translog_tbcol_values* del             = NULL;
    xk_pg_parser_translog_tbcol_values* insert          = NULL;
    xk_pg_parser_translog_tbcol_values* update          = NULL;
    xk_pg_parser_translog_tbcol_values* delete          = NULL;
    xk_pg_parser_translog_tbcol_value* insertvalue      = NULL;
    xk_pg_parser_translog_tbcol_value* nupdatevalue     = NULL;
    xk_pg_parser_translog_tbcol_value* oupdatevalue     = NULL;
    xk_pg_parser_translog_tbcol_value* deletevalue      = NULL;

    if(true == dlist_isnull(burstnode->dlinsertrows))
    {
        return true;
    }

    /* 没有missing列,返回true */
    if(0 == delrow->missingcnt && 0 == updaterow->missingcnt)
    {
        return true;
    }

    /* 查找与delete相同的insert */
    for (dlnode = burstnode->dlinsertrows->head; NULL != dlnode; dlnode = dlnode->next)
    {
        same = false;
        insertrow = (ripple_rebuild_burstrow*)dlnode->value;

        if (RIPPLE_REBUILD_BURSTNODEFLAG_NOINDEX == burstnode->flag)
        {
            /* 找到相同insert，链表中删除insert、释放delete 返回true */
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
            del = (xk_pg_parser_translog_tbcol_values*)delrow->row;
            insert = (xk_pg_parser_translog_tbcol_values*)insertrow->row;

            /* 比较约束列值是否相同 */
            for (keyindex = 0; keyindex < keycnt; keyindex++)
            {
                key = keys[keyindex].colno;
                if (0 != strcmp(del->m_old_values[key - 1].m_value, insert->m_new_values[key - 1].m_value))
                {
                    same = false;
                    break;
                }
            }

            /* 找到匹配值退出循环 */
            if (true == same)
            {
                break;
            }
        }
    }

    /* 没有找到insert, 不需要补全 */
    if (false == same)
    {
        return true;
    }

    /* 根据查到的insert, 补全delete和update中missing值 */
    insert = (xk_pg_parser_translog_tbcol_values*)insertrow->row;
    update = (xk_pg_parser_translog_tbcol_values*)updaterow->row;
    delete = (xk_pg_parser_translog_tbcol_values*)delrow->row;
    insertvalue = insert->m_new_values;
    oupdatevalue = update->m_old_values;
    nupdatevalue = update->m_new_values;
    deletevalue = delete->m_old_values;

    for (keyindex = 0; keyindex < insert->m_valueCnt; keyindex++)
    {
        if (INFO_COL_MAY_NULL == insertvalue[keyindex].m_info)
        {
            continue;
        }

        if (INFO_COL_MAY_NULL == deletevalue[keyindex].m_info)
        {
            deletevalue[keyindex].m_freeFlag = insertvalue[keyindex].m_freeFlag;
            deletevalue[keyindex].m_info = insertvalue[keyindex].m_info;
            deletevalue[keyindex].m_coltype = insertvalue[keyindex].m_coltype;
            deletevalue[keyindex].m_valueLen = insertvalue[keyindex].m_valueLen;
            rfree(deletevalue[keyindex].m_colName);
            deletevalue[keyindex].m_colName = rstrdup(insertvalue[keyindex].m_colName);
            
            if (0 != deletevalue[keyindex].m_valueLen)
            {
                deletevalue[keyindex].m_value = rmalloc0(deletevalue[keyindex].m_valueLen + 1);
                if (NULL == deletevalue[keyindex].m_value)
                {
                    elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
                    return false;
                }
                rmemset0(deletevalue[keyindex].m_value, 0, 0, deletevalue[keyindex].m_valueLen + 1);
                rmemcpy0(deletevalue[keyindex].m_value, 0, insertvalue[keyindex].m_value, deletevalue[keyindex].m_valueLen);
            }
            else
            {
                deletevalue[keyindex].m_value = NULL;
            }
        }

        if (INFO_COL_MAY_NULL == nupdatevalue[keyindex].m_info)
        {
            nupdatevalue[keyindex].m_freeFlag = insertvalue[keyindex].m_freeFlag;
            nupdatevalue[keyindex].m_info = insertvalue[keyindex].m_info;
            nupdatevalue[keyindex].m_coltype = insertvalue[keyindex].m_coltype;
            nupdatevalue[keyindex].m_valueLen = insertvalue[keyindex].m_valueLen;
            rfree(nupdatevalue[keyindex].m_colName);
            nupdatevalue[keyindex].m_colName = rstrdup(insertvalue[keyindex].m_colName);
            
            if (0 != nupdatevalue[keyindex].m_valueLen)
            {
                nupdatevalue[keyindex].m_value = rmalloc0(nupdatevalue[keyindex].m_valueLen + 1);
                if (NULL == nupdatevalue[keyindex].m_value)
                {
                    elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
                    return false;
                }
                rmemset0(nupdatevalue[keyindex].m_value, 0, 0, nupdatevalue[keyindex].m_valueLen + 1);
                rmemcpy0(nupdatevalue[keyindex].m_value, 0, insertvalue[keyindex].m_value, nupdatevalue[keyindex].m_valueLen);
            }
            else
            {
                nupdatevalue[keyindex].m_value = NULL;
            }
        }

        if (INFO_COL_MAY_NULL == oupdatevalue[keyindex].m_info)
        {
            oupdatevalue[keyindex].m_freeFlag = insertvalue[keyindex].m_freeFlag;
            oupdatevalue[keyindex].m_info = insertvalue[keyindex].m_info;
            oupdatevalue[keyindex].m_coltype = insertvalue[keyindex].m_coltype;
            oupdatevalue[keyindex].m_valueLen = insertvalue[keyindex].m_valueLen;
            rfree(oupdatevalue[keyindex].m_colName);
            oupdatevalue[keyindex].m_colName = rstrdup(insertvalue[keyindex].m_colName);

            if (0 != oupdatevalue[keyindex].m_valueLen)
            {
                oupdatevalue[keyindex].m_value = rmalloc0(insertvalue[keyindex].m_valueLen + 1);
                if (NULL == insertvalue[keyindex].m_value)
                {
                    elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
                    return false;
                }
                rmemset0(oupdatevalue[keyindex].m_value, 0, 0, insertvalue[keyindex].m_valueLen + 1);
                rmemcpy0(oupdatevalue[keyindex].m_value, 0, insertvalue[keyindex].m_value, insertvalue[keyindex].m_valueLen);
            }
            else
            {
                oupdatevalue[keyindex].m_value = NULL;
            }
        }
    }

    return true;
}

/* 判断约束列值是否被修改 */
static void ripple_rebuild_burst_ischangeconskey(ripple_rebuild_burstnode* burstnode,
                                                 ripple_rebuild_burstrow* updaterow)
{
    int key                                             = 0;
    int colindex                                        = 0;
    ripple_rebuild_burstcolumn* column                  = NULL;
    xk_pg_parser_translog_tbcol_values* update          = NULL;
    xk_pg_parser_translog_tbcol_value* nupdatevalue     = NULL;
    xk_pg_parser_translog_tbcol_value* oupdatevalue     = NULL;

    update = (xk_pg_parser_translog_tbcol_values*)updaterow->row;

    nupdatevalue = update->m_new_values;
    oupdatevalue = update->m_old_values;

    column = burstnode->table.keys;

    /* 根据table.keys判断约束列是否被修改 */
    for (colindex = 0; colindex < burstnode->table.keycnt; colindex++)
    {
        key = column[colindex].colno;

        /* 发现被需改的约束列, 设置flage返回 */
        if (0 != strcmp((char*)nupdatevalue[key - 1].m_value, oupdatevalue[key - 1].m_value))
        {
            updaterow->flag = RIPPLE_REBUILD_BURSTROWFLAG_CHANGECONSKEY;
            break;
        }
    }
    return;
}

/* 构建 burstnode table主键或唯一约束信息 */
static bool ripple_rebuild_composekey(HTAB* hclass,
                                      HTAB* hattrs,
                                      HTAB* hindex,
                                      ripple_rebuild_burstnode* pburstnode)
{
    bool find                                   = false;
    uint32 indkey                               = 0;
    int64 keycntindex                           = 0;
    List* lindex                                = NULL;
    List* lattrs                                = NULL;
    ListCell* indlc                             = NULL;
    ListCell* attrlc                            = NULL;
    xk_pg_sysdict_Form_pg_class class           = NULL;
    ripple_catalog_index_value* indvalue        = NULL;
    xk_pg_sysdict_Form_pg_index index           = NULL;
    xk_pg_sysdict_Form_pg_attribute attr        = NULL;

    /* 获取class */
    class = (xk_pg_sysdict_Form_pg_class)ripple_class_getbyoid(pburstnode->table.oid, hclass);

    if (NULL == class)
    {
        elog(RLOG_WARNING, "ripple rebuild composekey not find class by %lu", pburstnode->table.oid);
        return false;
    }

    /* 获取index */
    lindex = (List*)ripple_index_getbyoid(pburstnode->table.oid, hindex);

    /* 未获取index 设置为pbe模式 退出 */
    if (NULL == lindex || NULL == lindex->head)
    {
        pburstnode->flag = RIPPLE_REBUILD_BURSTNODEFLAG_NOINDEX;
        return true;
    }

    /* 筛选replident/primary索引，未找到使用第一个索引 */
    foreach(indlc, lindex)
    {
        find = false;
        indvalue = (ripple_catalog_index_value*)lfirst(indlc);

        if (true == indvalue->ripple_index->indisreplident)
        {
            find = true;
            index = indvalue->ripple_index;
            break;
        }

        if (true == indvalue->ripple_index->indisprimary)
        {
            find = true;
            index = indvalue->ripple_index;
            break;
        }
    }

    /* 未找到使用第一个索引 */
    if (false == find)
    {
        indvalue = (ripple_catalog_index_value*)linitial(lindex);
        index = indvalue->ripple_index;
    }

    pburstnode->table.keys = ripple_rebuild_burstcolumn_init(index->indnatts);
    if (NULL == pburstnode->table.keys)
    {
        elog(RLOG_WARNING, "ripple rebuild composekey pburstnode table.keys is null ");
        return false;
    }
    pburstnode->table.keycnt = index->indnatts;

    /* 获取attribute */
    lattrs = (List*)ripple_attribute_getbyoid(pburstnode->table.oid, hattrs);

    if (NULL == lattrs || NULL == lattrs->head)
    {
        elog(RLOG_WARNING, "ripple rebuild composekey not find attribute by %lu", pburstnode->table.oid);
        return false;
    }
    
    /* 填充 table.keys*/
    for (keycntindex = 0; keycntindex < index->indnatts; keycntindex++)
    {
        indkey = index->indkey[keycntindex];
        find = false;
        foreach(attrlc, lattrs)
        {
            attr = (xk_pg_sysdict_Form_pg_attribute)lfirst(attrlc);
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
            elog(RLOG_WARNING, "ripple rebuild composekey not find attribute attnum %u", indkey);
            return false;
        }
    }
    pburstnode->flag = RIPPLE_REBUILD_BURSTNODEFLAG_INDEX;

    if (lindex)
    {
        list_free(lindex);
    }
    return true;
}

/*
 * 复制tbcolbase
 * 入参: tbcolbase1源数据
 *       tbcolbase2目标数据
*/
static void ripple_rebuild_burst_tbcolbasecopy(xk_pg_parser_translog_tbcolbase* tbcolbase1,
                                               xk_pg_parser_translog_tbcolbase* tbcolbase2)
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
 * 复制tbcolbase
 * 入参: value1源数据
 * 
 * 返回值：是否复制成功，value2复制结果
*/
static bool ripple_rebuild_burst_tbcolvaluecopy(xk_pg_parser_translog_tbcol_value* value1,
                                                xk_pg_parser_translog_tbcol_value** value2,
                                                uint32 valuecnt)
{
    int len                                     = 0;
    int colindex                                = 0;
    xk_pg_parser_translog_tbcol_value* value    = NULL;

    if (NULL == value1)
    {
        elog(RLOG_WARNING, "value1 is null");
        return false;
    }

    len = sizeof(xk_pg_parser_translog_tbcol_value) * valuecnt;
    value = (xk_pg_parser_translog_tbcol_value*)rmalloc0(len);
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

/* 从链表尾获取table */
static void* ripple_rebuild_burst_gettable(dlist* dl, void* value, dlistvaluecmp valuecmp)
{
    dlistnode* dlnode = NULL;
    dlistnode* dlnodetmp = NULL;
    if(NULL == dl)
    {
        return NULL;
    }

    for(dlnode = dl->tail; NULL != dlnode; dlnode = dlnodetmp)
    {
        dlnodetmp = dlnode->prev;
        if(0 != valuecmp(value, dlnode->value))
        {
            continue;
        }

        return dlnode->value;
    }

    return NULL;
}

/* 获取 burst node节点 */
bool ripple_rebuild_burst_getnode(HTAB* hclass,
                                  HTAB* hattrs,
                                  HTAB* hindex,
                                  ripple_rebuild_burst* burst,
                                  ripple_rebuild_burstnode** pburstnode,
                                  ripple_rebuild_bursttable* bursttable)
{
    ripple_rebuild_bursttable* dltable          = NULL;
    ripple_rebuild_burstnode* tmpburstnode      = NULL;

    dltable = (ripple_rebuild_bursttable*)ripple_rebuild_burst_gettable(burst->dlbursttable, bursttable, ripple_rebuild_bursttable_cmp);
    if (NULL == dltable)
    {
        dltable = ripple_rebuild_bursttable_init();
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

        tmpburstnode = ripple_rebuild_burstnode_init();
        tmpburstnode->table.no = dltable->no;
        tmpburstnode->table.oid = dltable->oid;
        tmpburstnode->table.schema = rstrdup(dltable->schema);
        tmpburstnode->table.table = rstrdup(dltable->table);
    }
    else
    {
        tmpburstnode = (ripple_rebuild_burstnode*)dlist_get(burst->dlburstnodes, dltable, ripple_rebuild_burstnode_tablecmp);
        if (NULL != tmpburstnode)
        {
            *pburstnode = tmpburstnode;
            return true;
        }

        tmpburstnode = ripple_rebuild_burstnode_init();
        tmpburstnode->table.no = burst->number++;
        dltable->no = tmpburstnode->table.no;
        tmpburstnode->table.oid = dltable->oid;
        tmpburstnode->table.schema = rstrdup(dltable->schema);
        tmpburstnode->table.table = rstrdup(dltable->table);
    }

    /* 构建 burstnode->table.keys 索引信息 */
    if (false == ripple_rebuild_composekey(hclass, hattrs, hindex, tmpburstnode))
    {
        *pburstnode = NULL;
        ripple_rebuild_burstnode_free(tmpburstnode);
        elog(RLOG_WARNING, "rebuild burst getnode composekey failed");
        return false;
    }

    burst->dlburstnodes = dlist_put(burst->dlburstnodes, tmpburstnode);
    *pburstnode = tmpburstnode;

    return true;
}

/*
 * 拆分 update 为 insert/delete
 * 设置
 * 入参： burstnode table对应node, rows -- update源数据
 * 
 * 返回值说明: bool true拆分成功，false拆分失败
 *            delrow 拆分出的delete语句，
 *            insertrow原update语句，
 * 
*/
bool ripple_rebuild_burst_decomposeupdate(ripple_rebuild_burstnode* burstnode,
                                          ripple_rebuild_burstrow** delrow,
                                          ripple_rebuild_burstrow** insertrow,
                                          void* rows)
{
    int colindex = 0;
    ripple_rebuild_burstrow* tmpdelrow              = NULL;
    ripple_rebuild_burstrow* tmpinsertrow           = NULL;
    xk_pg_parser_translog_tbcol_values* update      = NULL;
    xk_pg_parser_translog_tbcol_values* delete      = NULL;
    xk_pg_parser_translog_tbcol_value* newvalue     = NULL;
    xk_pg_parser_translog_tbcol_value* oldvalue     = NULL;

    update = (xk_pg_parser_translog_tbcol_values*)rows;

    /* update 新旧值missing值互补 */
    for (colindex = 0; colindex < update->m_valueCnt; colindex++)
    {
        newvalue = &update->m_new_values[colindex];
        oldvalue = &update->m_old_values[colindex];

        if (INFO_COL_MAY_NULL == newvalue->m_info
            && INFO_COL_MAY_NULL != oldvalue->m_info)
        {
            newvalue->m_freeFlag = oldvalue->m_freeFlag;
            newvalue->m_info = oldvalue->m_info;
            newvalue->m_coltype = oldvalue->m_coltype;
            newvalue->m_valueLen = oldvalue->m_valueLen;
            rfree(newvalue->m_colName);
            newvalue->m_colName = rstrdup(oldvalue->m_colName);
            
            if (0 != newvalue->m_valueLen)
            {
                newvalue->m_value = rmalloc0(newvalue->m_valueLen + 1);
                if (NULL == newvalue->m_value)
                {
                    elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
                    return false;
                }
                rmemset0(newvalue->m_value, 0, 0, newvalue->m_valueLen + 1);
                rmemcpy0(newvalue->m_value, 0, oldvalue->m_value, newvalue->m_valueLen);
            }
            else
            {
                newvalue->m_value = NULL;
            }
        }

        if (INFO_COL_MAY_NULL != newvalue->m_info
            && INFO_COL_MAY_NULL == oldvalue->m_info)
        {
            oldvalue->m_freeFlag = newvalue->m_freeFlag;
            oldvalue->m_info = newvalue->m_info;
            oldvalue->m_coltype = newvalue->m_coltype;
            oldvalue->m_valueLen = newvalue->m_valueLen;
            rfree(oldvalue->m_colName);
            oldvalue->m_colName = rstrdup(newvalue->m_colName);
            
            if (0 != oldvalue->m_valueLen)
            {
                oldvalue->m_value = rmalloc0(oldvalue->m_valueLen + 1);
                if (NULL == oldvalue->m_value)
                {
                    elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
                    return false;
                }
                rmemset0(oldvalue->m_value, 0, 0, oldvalue->m_valueLen + 1);
                rmemcpy0(oldvalue->m_value, 0, newvalue->m_value, oldvalue->m_valueLen);
            }
            else
            {
                oldvalue->m_value = NULL;
            }
        }
    }

    tmpdelrow = ripple_rebuild_burstrow_init(update->m_valueCnt);
    if (NULL == tmpdelrow)
    {
        elog(RLOG_WARNING, "rebuild burst decomposeupdate burstrow init failed");
        return false;
    }

    /* 申请delete value空间 */
    delete = (xk_pg_parser_translog_tbcol_values*)rmalloc0(sizeof(xk_pg_parser_translog_tbcol_values));
    if(NULL == delete)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return false;
    }
    rmemset0(delete, 0, '\0', sizeof(xk_pg_parser_translog_tbcol_values));

    /* 复制表信息 */
    ripple_rebuild_burst_tbcolbasecopy(&update->m_base, &delete->m_base);

    delete->m_base.m_dmltype = XK_PG_PARSER_TRANSLOG_DMLTYPE_DELETE;
    delete->m_haspkey = update->m_haspkey;
    delete->m_relfilenode = update->m_relfilenode;
    delete->m_relid = update->m_relid;
    delete->m_tupleCnt = update->m_tupleCnt;
    delete->m_valueCnt = update->m_valueCnt;
    delete->m_new_values = NULL;
    delete->m_tuple = NULL;

    /* 复制before列内容 */
    if (false == ripple_rebuild_burst_tbcolvaluecopy(update->m_old_values, &delete->m_old_values, update->m_valueCnt))
    {
        elog(RLOG_WARNING, "rebuild burst decomposeupdate copy before failed");
        heap_free_trans_result((xk_pg_parser_translog_tbcolbase*)delete);
        return false;
    }
    tmpdelrow->row = delete;
    tmpdelrow->op = RIPPLE_REBUILD_BURSTROWTYPE_UPDATE;

    /* 设置del为需要删除 */
    tmpdelrow->flag = RIPPLE_REBUILD_BURSTROWFLAG_REMOVEDELETE;

    if(false == ripple_rebuild_burst_setmd5andmissing(tmpdelrow, delete->m_old_values))
    {
        ripple_rebuild_burstrow_free(tmpdelrow);
        elog(RLOG_WARNING, "rebuild burst decomposeupdate delete setmd5adnmissing failed");
        return false;
    }

    tmpinsertrow = ripple_rebuild_burstrow_init(update->m_valueCnt);
    if (NULL == tmpinsertrow)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return false;
    }

    /* 区分insert和update产生的insert所以设置为update */
    tmpinsertrow->row = rows;
    tmpinsertrow->op = RIPPLE_REBUILD_BURSTROWTYPE_UPDATE;

    if(false == ripple_rebuild_burst_setmd5andmissing(tmpinsertrow, update->m_new_values))
    {
        ripple_rebuild_burstrow_free(delrow);
        ripple_rebuild_burstrow_free(tmpinsertrow);
        elog(RLOG_WARNING, "rebuild burst decomposeupdate insert setmd5adnmissing failed");
        return false;
    }

    /* 设置主键或唯一约束列改变 */
    ripple_rebuild_burst_ischangeconskey(burstnode, tmpinsertrow);

    /* 互相关联 用于判断delete语句是否保存 */
    tmpdelrow->relatedrow = tmpinsertrow;
    tmpinsertrow->relatedrow = tmpdelrow;

    *delrow = tmpdelrow;
    *insertrow = tmpinsertrow;

    return true;
}

/* 
 * 合并 insert/delete 
 * 返回 true 说明合并成功, 返回 false 说明合并失败
*/
bool ripple_rebuild_burst_mergeinsert(ripple_rebuild_burstnode* pburstnode, 
                                      ripple_rebuild_burstrow* insertrow)
{
    dlistnode* dlnode                       = NULL;
    ripple_rebuild_burstrow* delrow         = NULL;

    /* 有missing值或delete链表为空退出 */
    if (insertrow->missingcnt > 0) 
    {
        return false;
    }

    if(true == dlist_isnull(pburstnode->dldeleterows))
    {
        return false;
    }

    /* 查找与insert相同的delete */
    for (dlnode = pburstnode->dldeleterows->tail; NULL != dlnode; dlnode = dlnode->prev)
    {
        delrow = (ripple_rebuild_burstrow*)dlnode->value;

        /* 有missing值继续 */
        if (delrow->missingcnt > 0)
        {
            continue;
        }

        /* 找到相同delete，链表中删除delete、释放insert 返回true */
        if (0 == memcmp(insertrow->md5, delrow->md5, 16))
        {
            pburstnode->dldeleterows = dlist_delete(pburstnode->dldeleterows, dlnode, ripple_rebuild_burstrow_free);
            return true;
        }
    }

    return false;
}

/* 
 * 合并 delete/insert
 * 返回 true 说明合并成功, 返回 false 说明合并失败
*/
bool ripple_rebuild_burst_mergedelete(ripple_rebuild_burstnode* pburstnode, 
                                      ripple_rebuild_burstrow* delrow)
{
    int keycnt                                  = 0;
    int keyindex                                = 0;
    int key                                     = 0;
    dlistnode* dlnode                           = NULL;
    ripple_rebuild_burstcolumn* keys            = NULL;
    ripple_rebuild_burstrow* insertrow          = NULL;
    xk_pg_parser_translog_tbcol_values* del     = NULL;
    xk_pg_parser_translog_tbcol_values* insert  = NULL;

    if(true == dlist_isnull(pburstnode->dlinsertrows))
    {
        return false;
    }

    /* 查找与insert相同的delete */
    for (dlnode = pburstnode->dlinsertrows->tail; NULL != dlnode; dlnode = dlnode->prev)
    {
        insertrow = (ripple_rebuild_burstrow*)dlnode->value;

        if (RIPPLE_REBUILD_BURSTNODEFLAG_NOINDEX == pburstnode->flag)
        {
            /* 找到相同insert，链表中删除insert、释放delete 返回true */
            if (0 == memcmp(insertrow->md5, delrow->md5, 16))
            {
                pburstnode->dlinsertrows= dlist_delete(pburstnode->dlinsertrows, dlnode, ripple_rebuild_burstrow_free);
                ripple_rebuild_burstrow_free(delrow);
                return true;
            }
        }
        else
        {
            bool same = true;
            keycnt = pburstnode->table.keycnt;
            keys = pburstnode->table.keys;
            del = (xk_pg_parser_translog_tbcol_values*)delrow->row;
            insert = (xk_pg_parser_translog_tbcol_values*)insertrow->row;

            /* 比较约束列值是否相同 */
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
                /* 
                 * update产生的insert语句与正常delete可以合并
                 * 需要保存update产生的delete语句
                 * 设置row->flage 为nop，置空delete的relatedrow
                 */
                if (RIPPLE_REBUILD_BURSTROWTYPE_UPDATE == insertrow->op
                    && NULL != insertrow->relatedrow)
                {
                    insertrow->relatedrow->flag = RIPPLE_REBUILD_BURSTROWFLAG_NOP;
                    insertrow->relatedrow->relatedrow = NULL;
                }
                
                /* 合并成功移除insert，释放delrow */
                pburstnode->dlinsertrows = dlist_delete(pburstnode->dlinsertrows, dlnode, ripple_rebuild_burstrow_free);
                ripple_rebuild_burstrow_free(delrow);
                return true;
            }
        }
    }

    return false;
}

/* 
 * update合并 delete/insert
 * 返回 true 说明合并成功, 返回 false 说明合并失败
*/
bool ripple_rebuild_burst_updatemergedelete(ripple_rebuild_burstnode* burstnode,
                                            ripple_rebuild_burstrow* delrow,
                                            ripple_rebuild_burstrow* updaterow)
{
    bool same                                           = true;
    int keycnt                                          = 0;
    int keyindex                                        = 0;
    int key                                             = 0;
    dlistnode* dlnode                                   = NULL;
    ripple_rebuild_burstcolumn* keys                    = NULL;
    ripple_rebuild_burstrow* insertrow                  = NULL;
    xk_pg_parser_translog_tbcol_values* del             = NULL;
    xk_pg_parser_translog_tbcol_values* insert          = NULL;
    xk_pg_parser_translog_tbcol_values* update          = NULL;
    xk_pg_parser_translog_tbcol_value* insertvalue      = NULL;
    xk_pg_parser_translog_tbcol_value* updatevalue      = NULL;

    if(true == dlist_isnull(burstnode->dlinsertrows))
    {
        burstnode->dldeleterows = dlist_put(burstnode->dldeleterows, delrow);
        burstnode->dlinsertrows = dlist_put(burstnode->dlinsertrows, updaterow);
        return true;
    }

    update = (xk_pg_parser_translog_tbcol_values*)updaterow->row;

    /* 查找与insert相同的delete */
    for (dlnode = burstnode->dlinsertrows->tail; NULL != dlnode; dlnode = dlnode->prev)
    {
        same = false;
        insertrow = (ripple_rebuild_burstrow*)dlnode->value;
        insert = (xk_pg_parser_translog_tbcol_values*)insertrow->row;

        if (RIPPLE_REBUILD_BURSTNODEFLAG_NOINDEX == burstnode->flag)
        {
            /* 找到相同insert，链表中删除insert、释放delete 返回true */
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
            del = (xk_pg_parser_translog_tbcol_values*)delrow->row;

            /* 比较约束列值是否相同 */
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

    if (true == same)
    {
        /* 将updatebefore值替换到insert上 */
        if (RIPPLE_REBUILD_BURSTROWTYPE_INSERT == insertrow->op
            || RIPPLE_REBUILD_BURSTROWTYPE_UPDATE == insertrow->op)
        {
            insert = (xk_pg_parser_translog_tbcol_values*)insertrow->row;
            insertvalue = insert->m_new_values;
            updatevalue = update->m_new_values;
            for (keyindex = 0; keyindex < insert->m_valueCnt; keyindex++)
            {
                insertvalue[keyindex].m_freeFlag = updatevalue[keyindex].m_freeFlag;
                insertvalue[keyindex].m_info = updatevalue[keyindex].m_info;
                insertvalue[keyindex].m_valueLen = updatevalue[keyindex].m_valueLen;
                
                if (0 != insertvalue[keyindex].m_valueLen)
                {
                    rfree(insertvalue[keyindex].m_value);
                    insertvalue[keyindex].m_value = updatevalue[keyindex].m_value;
                    updatevalue[keyindex].m_value = NULL;
                }
                else
                {
                    insertvalue[keyindex].m_value = NULL;
                }
            }
        }
        else
        {
            elog(RLOG_WARNING, "rebuild burst updatemergedelete Invalid operation type:%d", insertrow->op);
            return false;
        }

        /* 计算MD5和missing */
        if(false == ripple_rebuild_burst_setmd5andmissing(insertrow, insert->m_new_values))
        {
            elog(RLOG_WARNING, "rebuild burst updatemergedelete setmd5adnmissing failed");
            return false;
        }

        ripple_rebuild_burstrow_free(delrow);
        ripple_rebuild_burstrow_free(updaterow);
        return true;
    }

    burstnode->dldeleterows = dlist_put(burstnode->dldeleterows, delrow);
    burstnode->dlinsertrows = dlist_put(burstnode->dlinsertrows, updaterow);

    return true;
}

/* 对 txn 的insert内容重组为burst */
static bool ripple_rebuild_burst_txn2bursts_insert(ripple_rebuild_burst* burst, 
                                                   ripple_cache_sysdicts* sysdicts,
                                                   void* row)
{
    ripple_rebuild_bursttable table             = {0};
    ripple_rebuild_burstrow* insertrow          = NULL;
    ripple_rebuild_burstnode* burstnode         = NULL;
    xk_pg_parser_translog_tbcol_values* insert  = NULL;

    insert = (xk_pg_parser_translog_tbcol_values*)row;

    table.oid = insert->m_relid;
    table.schema = insert->m_base.m_schemaname;
    table.table = insert->m_base.m_tbname;

    if (false == ripple_rebuild_burst_getnode(sysdicts->by_class,
                                              sysdicts->by_attribute,
                                              sysdicts->by_index,
                                              burst,
                                              &burstnode,
                                              &table))
    {
        elog(RLOG_WARNING, "rebuild burst txn2bursts insert getnode failed");
        return false;
    }

    insertrow = ripple_rebuild_burstrow_init(insert->m_valueCnt);
    if (NULL == insertrow)
    {
        elog(RLOG_WARNING, "rebuild burst txn2bursts insert burstrow init failed");
        return false;
    }
    insertrow->row = row;
    insertrow->op = RIPPLE_REBUILD_BURSTROWTYPE_INSERT;

    if(false == ripple_rebuild_burst_setmd5andmissing(insertrow, insert->m_new_values))
    {
        ripple_rebuild_burstrow_free(insertrow);
        elog(RLOG_WARNING, "rebuild burst txn2bursts insert setmd5adnmissing failed");
        return false;
    }

    if (false == ripple_rebuild_burst_mergeinsert(burstnode, insertrow))
    {
        burstnode->dlinsertrows = dlist_put(burstnode->dlinsertrows, insertrow);
    }

    return true;
}

/* 对 txn 的delete内容重组为burst */
static bool ripple_rebuild_burst_txn2bursts_delete(ripple_rebuild_burst* burst, 
                                                   ripple_cache_sysdicts* sysdicts,
                                                   void* row)
{
    ripple_rebuild_bursttable table             = {0};
    ripple_rebuild_burstrow* delrow             = NULL;
    ripple_rebuild_burstnode* burstnode         = NULL;
    xk_pg_parser_translog_tbcol_values* delete  = NULL;

    delete = (xk_pg_parser_translog_tbcol_values*)row;

    table.oid = delete->m_relid;
    table.schema = delete->m_base.m_schemaname;
    table.table = delete->m_base.m_tbname;

    /* 获取burstnode */
    if (false == ripple_rebuild_burst_getnode(sysdicts->by_class,
                                              sysdicts->by_attribute,
                                              sysdicts->by_index,
                                              burst,
                                              &burstnode,
                                              &table))
    {
        elog(RLOG_WARNING, "rebuild burst txn2bursts delete getnode failed");
        return false;
    }

    delrow = ripple_rebuild_burstrow_init(delete->m_valueCnt);
    if (NULL == delrow)
    {
        elog(RLOG_WARNING, "rebuild burst txn2bursts delete burstrow init failed");
        return false;
    }
    delrow->row = row;
    delrow->op =RIPPLE_REBUILD_BURSTROWTYPE_DELETE;

    if(false == ripple_rebuild_burst_setmd5andmissing(delrow, delete->m_old_values))
    {
        ripple_rebuild_burstrow_free(delrow);
        elog(RLOG_WARNING, "rebuild burst txn2bursts delete setmd5adnmissing failed");
        return false;
    }

    if (false == ripple_rebuild_burst_mergedelete(burstnode, delrow))
    {
        burstnode->dldeleterows = dlist_put(burstnode->dldeleterows, delrow);
    }

    return true;
}

/* 对 txn 的multiinsert内容重组为burst */
static bool ripple_rebuild_burst_txn2bursts_multiinsert(ripple_rebuild_burst* burst, 
                                                        ripple_cache_sysdicts* sysdicts,
                                                        void* row)
{
    int rowindx                                         = 0;
    ripple_rebuild_bursttable table                     = {0};
    ripple_rebuild_burstrow* insertrow                  = NULL;
    ripple_rebuild_burstnode* burstnode                 = NULL;
    xk_pg_parser_translog_tbcol_value* value            = NULL;
    xk_pg_parser_translog_tbcol_values* insert          = NULL;
    xk_pg_parser_translog_tbcol_nvalues* multiinsert    = NULL;

    multiinsert = (xk_pg_parser_translog_tbcol_nvalues*)row;

    table.oid = multiinsert->m_relid;
    table.schema = multiinsert->m_base.m_schemaname;
    table.table = multiinsert->m_base.m_tbname;

    if (false == ripple_rebuild_burst_getnode(sysdicts->by_class,
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
        /* 拆分为多条insert */
        insert = (xk_pg_parser_translog_tbcol_values*)rmalloc0(sizeof(xk_pg_parser_translog_tbcol_values));
        if(NULL == insert)
        {
            elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
            return false;
        }
        rmemset0(insert, 0, '\0', sizeof(xk_pg_parser_translog_tbcol_values));

        ripple_rebuild_burst_tbcolbasecopy(&multiinsert->m_base, &insert->m_base);
        insert->m_base.m_dmltype = XK_PG_PARSER_TRANSLOG_DMLTYPE_INSERT;
        insert->m_haspkey = multiinsert->m_haspkey;
        insert->m_relid = multiinsert->m_relid;
        insert->m_relfilenode = multiinsert->m_relfilenode;
        insert->m_valueCnt = multiinsert->m_valueCnt;
        insert->m_new_values = value;

        insertrow = ripple_rebuild_burstrow_init(insert->m_valueCnt);
        if (NULL == insertrow)
        {
            elog(RLOG_WARNING, "rebuild burst txn2bursts multiinsert burstrow init failed");
            return false;
        }
        insertrow->row = insert;
        insertrow->op = RIPPLE_REBUILD_BURSTROWTYPE_INSERT;

        if(false == ripple_rebuild_burst_setmd5andmissing(insertrow, insert->m_new_values))
        {
            ripple_rebuild_burstrow_free(insertrow);
            elog(RLOG_WARNING, "rebuild burst txn2bursts multiinsert setmd5adnmissing failed");
            return false;
        }

        if (false == ripple_rebuild_burst_mergeinsert(burstnode, insertrow))
        {
            burstnode->dlinsertrows = dlist_put(burstnode->dlinsertrows, insertrow);
        }
        multiinsert->m_rows[rowindx].m_new_values = NULL;
    }

    return true;
}

/* 对 txn 的update内容重组为burst */
static bool ripple_rebuild_burst_txn2bursts_update(ripple_rebuild_burst* burst, 
                                                   ripple_cache_sysdicts* sysdicts,
                                                   void* row)
{
    ripple_rebuild_bursttable table             = {0};
    ripple_rebuild_burstrow* delrow             = NULL;
    ripple_rebuild_burstrow* insertrow          = NULL;
    ripple_rebuild_burstnode* burstnode         = NULL;
    xk_pg_parser_translog_tbcol_values* update  = NULL;

    update = (xk_pg_parser_translog_tbcol_values*)row;

    table.oid = update->m_relid;
    table.schema = update->m_base.m_schemaname;
    table.table = update->m_base.m_tbname;

    if (false == ripple_rebuild_burst_getnode(sysdicts->by_class,
                                              sysdicts->by_attribute,
                                              sysdicts->by_index,
                                              burst,
                                              &burstnode,
                                              &table))
    {
        elog(RLOG_WARNING, "rebuild burst txn2bursts update getnode failed");
        return false;
    }

    /* 拆分为delete和insert */
    if(false == ripple_rebuild_burst_decomposeupdate(burstnode,
                                                     &delrow,
                                                     &insertrow,
                                                     row))
    {
        elog(RLOG_WARNING, "rebuild burst txn2bursts update decomposeupdate failed");
        return false;
    }

    /* 补全missing值 */
    if(false == ripple_rebuild_burst_updatematchdata(burstnode,
                                                     delrow,
                                                     insertrow))
    {
        elog(RLOG_WARNING, "rebuild burst txn2bursts update matchdata failed");
        return false;
    }

    /* 合并update */
    if(false == ripple_rebuild_burst_updatemergedelete(burstnode,
                                                       delrow,
                                                       insertrow))
    {
        elog(RLOG_WARNING, "rebuild burst txn2bursts update mergedelete failed");
        return false;
    }

    return true;
}

/* txn 的内容重组为burst */
bool ripple_rebuild_burst_txn2bursts(ripple_rebuild_burst* burst, ripple_cache_sysdicts* sysdicts, ripple_txn* txn)
{
    bool complete                               = false;
    ListCell* lc                                = NULL;
    ListCell* metadatalc                        = NULL;
    List* lststmt                               = NULL;
    dlistnode* dlnode                           = NULL;
    ripple_rebuild_bursttable* table            = NULL;
    ripple_rebuild_burstnode* burstnode         = NULL;
    ripple_catalogdata *catalogdata             = NULL;
    ripple_catalog_class_value* class           = NULL;
    ripple_txnstmt_metadata* metadatastmt       = NULL;
    xk_pg_parser_translog_tbcolbase* tbcolbase  = NULL;
    ripple_rebuild_bursttable dltable           = {0};

    ripple_txnstmt* stmtnode = NULL;

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

        if (RIPPLE_TXNSTMT_TYPE_DDL == stmtnode->type)
        {
            burst->number ++;

            dlnode = (burst->dlbursttable == NULL) ? NULL : burst->dlbursttable->head;

            /* 所有表的 no 以 burst->number 为基础递增 分界线 */
            for (; NULL != dlnode; dlnode = dlnode->next)
            {
                table = (ripple_rebuild_bursttable*)dlnode->value;
                table->no = burst->number++;
            }

            burstnode = ripple_rebuild_burstnode_init();
            if (NULL == burstnode)
            {
                elog(RLOG_WARNING, "rebuild burst txn2bursts init burstnode failed");
                return false;
            }

            burstnode->table.no = burst->number++;
            burstnode->type = RIPPLE_REBUILD_BURSTNODETYPE_OTHER;
            burstnode->stmt = stmtnode;
            burst->dlburstnodes = dlist_put(burst->dlburstnodes, burstnode);
        }
        else if(RIPPLE_TXNSTMT_TYPE_METADATA == stmtnode->type)
        {
            metadatastmt = (ripple_txnstmt_metadata*)stmtnode->stmt;

            complete = false;
            metadatalc = metadatastmt->begin;
            while(1)
            {
                /* 应用系统表 */
                catalogdata = (ripple_catalogdata*)lfirst(metadatalc);
                ripple_cache_sysdicts_txnsysdicthisitem2cache(sysdicts, metadatalc);

                if (RIPPLE_CATALOG_TYPE_CLASS == catalogdata->type)
                {
                    ripple_cache_sysdicts_clearsysdicthisbyclass(sysdicts, metadatalc);
                    class = (ripple_catalog_class_value*)catalogdata->catalog;
                    if (NULL == class)
                    {
                        elog(RLOG_WARNING, "rebuild burst txn2bursts catalogdata->catalog is null");
                        return false;
                    }
                    dltable.oid = class->ripple_class->oid;
                    dltable.schema = class->ripple_class->nspname.data;
                    dltable.table = class->ripple_class->relname.data;

                    table = ripple_rebuild_burst_gettable(burst->dlbursttable, &dltable, ripple_rebuild_bursttable_cmp);
                    if (NULL == table)
                    {
                        table = ripple_rebuild_bursttable_init();
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
                    burstnode = ripple_rebuild_burstnode_init();
                    if (NULL == burstnode)
                    {
                        elog(RLOG_WARNING, "rebuild burst txn2bursts init burstnode failed");
                        return false;
                    }

                    burstnode->table.no = table->no;
                    burstnode->table.oid = class->ripple_class->oid;
                    burstnode->table.schema = rstrdup(class->ripple_class->nspname.data);
                    burstnode->table.table = rstrdup(class->ripple_class->relname.data);
                    if (false == ripple_rebuild_composekey(sysdicts->by_class,
                                                           sysdicts->by_attribute,
                                                           sysdicts->by_index,
                                                           burstnode))
                    {
                        elog(RLOG_WARNING, "rebuild burst txn2bursts composekey failed");
                        return false;
                    }
                    burstnode->type = RIPPLE_REBUILD_BURSTNODETYPE_OTHER;
                    burstnode->stmt = stmtnode;
                    burst->dlburstnodes = dlist_put(burst->dlburstnodes, burstnode);
                    /* 跳过当前node */
                    table->no = burst->number++;
                }
                else if (RIPPLE_CATALOG_TYPE_DATABASE == catalogdata->type)
                {
                    ripple_catalog_database_value* database = NULL;
                    database = (ripple_catalog_database_value*)catalogdata->catalog;
                    if (NULL == database)
                    {
                        elog(RLOG_WARNING, "rebuild burst txn2bursts catalogdata->catalog is null");
                        return false;
                    }

                    burstnode = ripple_rebuild_burstnode_init();
                    if (NULL == burstnode)
                    {
                        elog(RLOG_WARNING, "rebuild burst txn2bursts init burstnode failed");
                        return false;
                    }

                    burstnode->table.no = burst->number++;
                    burstnode->type = RIPPLE_REBUILD_BURSTNODETYPE_OTHER;
                    burstnode->stmt = stmtnode;
                    burst->dlburstnodes = dlist_put(burst->dlburstnodes, burstnode);
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
        }
        else if(RIPPLE_TXNSTMT_TYPE_DML == stmtnode->type)
        {
            tbcolbase = (xk_pg_parser_translog_tbcolbase *)stmtnode->stmt;
            if(XK_PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT == tbcolbase->m_dmltype)
            {
                if (false == ripple_rebuild_burst_txn2bursts_multiinsert(burst, sysdicts, stmtnode->stmt))
                {
                    elog(RLOG_WARNING, "rebuild burst txn2bursts multiinsert failed");
                    return false;
                }
                
            }
            else if(XK_PG_PARSER_TRANSLOG_DMLTYPE_INSERT == tbcolbase->m_dmltype)
            {
                if (false == ripple_rebuild_burst_txn2bursts_insert(burst, sysdicts, stmtnode->stmt))
                {
                    elog(RLOG_WARNING, "rebuild burst txn2bursts insert failed");
                    return false;
                }
                stmtnode->stmt = NULL;
            }
            else if(XK_PG_PARSER_TRANSLOG_DMLTYPE_DELETE == tbcolbase->m_dmltype)
            {
                if (false == ripple_rebuild_burst_txn2bursts_delete(burst, sysdicts, stmtnode->stmt))
                {
                    elog(RLOG_WARNING, "rebuild burst txn2bursts delete failed");
                    return false;
                }
                stmtnode->stmt = NULL;
            }
            else if(XK_PG_PARSER_TRANSLOG_DMLTYPE_UPDATE == tbcolbase->m_dmltype)
            {
                if (false == ripple_rebuild_burst_txn2bursts_update(burst, sysdicts, stmtnode->stmt))
                {
                    elog(RLOG_WARNING, "rebuild burst txn2bursts update failed");
                    return false;
                }
                stmtnode->stmt = NULL;
            }
            /* stmtnode 释放 */
            ripple_txnstmt_free(stmtnode);
        }
        else 
        {
            burstnode = ripple_rebuild_burstnode_init();
            if (NULL == burstnode)
            {
                elog(RLOG_WARNING, "rebuild burst txn2bursts init burstnode failed");
                return false;
            }

            burstnode->table.no = burst->number++;
            burstnode->type = RIPPLE_REBUILD_BURSTNODETYPE_OTHER;
            burstnode->stmt = stmtnode;
            burst->dlburstnodes = dlist_put(burst->dlburstnodes, burstnode);
        }
        lc->data.ptr_value = NULL;
    }
    list_free(lststmt);

    return true;
}

/* ripple_rebuild_burstrow比较MD5值 */
static int ripple_rebuild_burst_comparemd5(const void *r1, const void *r2)
{
    ripple_rebuild_burstrow* row1 = NULL;
    ripple_rebuild_burstrow* row2 = NULL;

    row1 = *(ripple_rebuild_burstrow** )r1;
    row2 = *(ripple_rebuild_burstrow** )r2;

    return memcmp(row1->md5, row2->md5, 16);
}

/* ripple_rebuild_burstrow比较missing值 */
static int ripple_rebuild_burst_comparemissing(const void *r1, const void *r2)
{
    ripple_rebuild_burstrow* row1 = NULL;
    ripple_rebuild_burstrow* row2 = NULL;

    row1 = *(ripple_rebuild_burstrow** )r1;
    row2 = *(ripple_rebuild_burstrow** )r2;

    /* 先按 missingcnt 排序 */
    if (row1->missingcnt < row2->missingcnt)
    {
        return -1;
    }

    if (row1->missingcnt > row2->missingcnt)
    {
        return 1;
    }

    /*  missingcnt 相同，再按 missingmap 排序 */
    return memcmp(row1->missingmap, row2->missingmap, row1->missingmapsize);
}

/* 拼接pbe delete语句 */
static bool ripple_rebuild_burst_assemblepbedelete(ripple_rebuild_burstnode* burstnode, ripple_txn* txn)
{
    bool need_and                               = false;
    bool need_select                            = true;
    int colindex                                = 0;
    int sortindex                               = 0;
    uint64 rowcnt                               = 0;
    uint64 len                                  = 0;
    uint8* md5                                  = NULL;
    StringInfo str                              = NULL;
    dlistnode* dlnode                           = NULL;
    ripple_txnstmt* stmt                        = NULL;
    ripple_txnstmt_burst* stmtburst             = NULL;
    ripple_rebuild_burstrow* delrow             = NULL;
    ripple_rebuild_burstrow** sortrow           = NULL;
    xk_pg_parser_translog_tbcol_value* values   = NULL;
    xk_pg_parser_translog_tbcol_values* delete  = NULL;

    if (true == dlist_isnull(burstnode->dldeleterows))
    {
        return true;
    }

    stmtburst = ripple_txnstmt_burst_init();
    if (NULL == stmtburst)
    {
        elog(RLOG_WARNING, "rebuild burst assemblepbedeletes stmtburst init failed");
        return false;
    }
    stmtburst->optype = RIPPLE_REBUILD_BURSTROWTYPE_DELETE;

    len = (sizeof(ripple_rebuild_burstrow*) * burstnode->dldeleterows->length);

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
        delrow = (ripple_rebuild_burstrow*)dlnode->value;
        sortrow[colindex] = delrow;
        colindex++;
    }

    /* 根据MD5分组排序 */
    qsort(sortrow, burstnode->dldeleterows->length, sizeof(ripple_rebuild_burstrow*), ripple_rebuild_burst_comparemd5);
    
    str = makeStringInfo();

    appendStringInfo(str, "DELETE FROM \"%s\".\"%s\" WHERE CTID IN (", burstnode->table.schema, burstnode->table.table);

    md5 = sortrow[0]->md5;
    for (sortindex = 0; sortindex < burstnode->dldeleterows->length; sortindex++)
    {
        delrow = sortrow[sortindex];
        delete = (xk_pg_parser_translog_tbcol_values*)delrow->row;
        /* 当列被drop或可能为空时, 不设置该列的值 */
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
            appendStringInfo(str, "(SELECT CTID FROM \"%s\".\"%s\" WHERE ", burstnode->table.schema, burstnode->table.table);
            need_and = false;
            for (colindex = 0; colindex < delete->m_valueCnt; colindex++)
            {
                values = &delete->m_old_values[colindex];
                if (INFO_COL_MAY_NULL == values->m_info 
                    || INFO_COL_IS_DROPED == values->m_info
                    || INFO_COL_IS_CUSTOM == values->m_info)
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
                    char *temp_str = strSpecialCharReplace((char *)values->m_value);
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
                        appendStringInfo(str, "'%s'", (char *)values->m_value);
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

    /* 初始化 ripple_txnstmt */
    stmt = ripple_txnstmt_init();
    if(NULL == stmt)
    {
        elog(RLOG_WARNING, "rebuild burst assemblepbedeletes txnstmt init failed");
        rfree(stmtburst);
        rfree(sortrow);
        return false;
    }
    stmt->stmt = NULL;
    stmt->type = RIPPLE_TXNSTMT_TYPE_BURST;

    stmtburst->batchcmd = (uint8 *)str->data;
    stmt->stmt = stmtburst;
    txn->stmts = lappend(txn->stmts, stmt);
    stmt = NULL;
    str->data = NULL;
    deleteStringInfo(str);

    rfree(sortrow);
    return true;
    
}

/* 拼接pbe insert语句 全列 */
static bool ripple_rebuild_burst_assemblepbeinsert(ripple_rebuild_burstnode* burstnode, ripple_txn* txn)
{
    bool need_comma                             = false;
    bool need_colname                           = true;
    int colindex                                = 0;
    StringInfo insertstr                        = NULL;
    StringInfo valuestr                         = NULL;
    dlistnode* dlnode                           = NULL;
    ripple_txnstmt* stmt                        = NULL;
    ripple_txnstmt_burst* stmtburst             = NULL;
    ripple_rebuild_burstrow* insertrow          = NULL;
    xk_pg_parser_translog_tbcol_value* values   = NULL;
    xk_pg_parser_translog_tbcol_values* insert  = NULL;

    if (true == dlist_isnull(burstnode->dlinsertrows))
    {
        return true;
    }

    stmtburst = ripple_txnstmt_burst_init();
    if (NULL == stmtburst)
    {
        elog(RLOG_WARNING, "rebuild burst assemblepbeinsert stmtburst init failed");
        return false;
    }
    stmtburst->optype = RIPPLE_REBUILD_BURSTROWTYPE_INSERT;

    insertstr = makeStringInfo();
    valuestr = makeStringInfo();

    appendStringInfo(insertstr, "INSERT INTO \"%s\".\"%s\" (", burstnode->table.schema, burstnode->table.table);

    need_colname = true;
    for (dlnode = burstnode->dlinsertrows->head; dlnode != NULL; dlnode = dlnode->next)
    {
        insertrow = (ripple_rebuild_burstrow*)dlnode->value;
        insert = (xk_pg_parser_translog_tbcol_values*)insertrow->row;

        need_comma = false;
        appendStringInfo(valuestr, "(");
        for (colindex = 0; colindex < insert->m_valueCnt; colindex++)
        {
            values = &insert->m_new_values[colindex];
            /* 当列被drop或可能为空时, 不设置该列的值 */
            if (INFO_COL_MAY_NULL == values->m_info 
                || INFO_COL_IS_DROPED == values->m_info)
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
                char *temp_str = strSpecialCharReplace((char *)values->m_value);
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
                    appendStringInfo(valuestr, "'%s'", (char *)values->m_value);
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
    

    /* 初始化 ripple_txnstmt */
    stmt = ripple_txnstmt_init();
    if(NULL == stmt)
    {
        elog(RLOG_WARNING, "rebuild burst assemblepbeinsert stmt init failed");
        rfree(stmtburst);
        return false;
    }
    stmt->stmt = NULL;
    stmt->type = RIPPLE_TXNSTMT_TYPE_BURST;

    appendStringInfo(insertstr, "%s", valuestr->data);

    stmtburst->batchcmd = (uint8 *)insertstr->data;
    stmt->stmt = stmtburst;
    txn->stmts = lappend(txn->stmts, stmt);
    stmt = NULL;
    insertstr->data = NULL;
    deleteStringInfo(insertstr);
    deleteStringInfo(valuestr);

    return true;
}

/* 拼接burst delete语句 主键 */
static bool ripple_rebuild_burst_assembledelete(ripple_rebuild_burstnode* burstnode, ripple_txn* txn)
{
    bool hasdelete                              = false;
    bool in_comma                               = false;
    bool need_comma                             = false;
    int colindex                                = 0;
    StringInfo str                              = NULL;
    dlistnode* dlnode                           = NULL;
    ripple_txnstmt* stmt                        = NULL;
    ripple_txnstmt_burst* stmtburst             = NULL;
    ripple_rebuild_burstrow* delrow             = NULL;
    ripple_rebuild_burstcolumn* column          = NULL;
    xk_pg_parser_translog_tbcol_value* values   = NULL;
    xk_pg_parser_translog_tbcol_values* delete  = NULL;

    if (true == dlist_isnull(burstnode->dldeleterows))
    {
        return true;
    }

    stmtburst = ripple_txnstmt_burst_init();
    if (NULL == stmtburst)
    {
        elog(RLOG_WARNING, "rebuild burst assembledelete stmtburst init failed");
        return false;
    }
    stmtburst->optype = burstnode->type;

    str = makeStringInfo();

    appendStringInfo(str, "DELETE FROM \"%s\".\"%s\" WHERE (", burstnode->table.schema, burstnode->table.table);

    /* 拼接约束信息 */
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
    /* 拼接要删除的值 */
    for (dlnode = burstnode->dldeleterows->head; dlnode != NULL; dlnode = dlnode->next)
    {
        delrow = (ripple_rebuild_burstrow*)dlnode->value;
        /* 需要移除不参与拼接 */
        if (RIPPLE_REBUILD_BURSTROWFLAG_REMOVEDELETE == delrow->flag)
        {
            continue;
        }

        delete = (xk_pg_parser_translog_tbcol_values*)delrow->row;
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
            /* 当列被drop或可能为空时, 不设置该列的值 */
            if (INFO_COL_MAY_NULL == values->m_info
                || INFO_COL_IS_DROPED == values->m_info
                || INFO_COL_IS_CUSTOM == values->m_info)
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
                char *temp_str = strSpecialCharReplace((char *)values->m_value);
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
                    appendStringInfo(str, "'%s'", (char *)values->m_value);
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
        ripple_txnstmt_burst_free(stmtburst);
        return true;
    }
    
    appendStringInfo(str, ");");

    /* 生成stmt */
    stmt = ripple_txnstmt_init();
    if(NULL == stmt)
    {
        elog(RLOG_WARNING, "rebuild burst assembledelete stmt init failed");
        rfree(stmtburst);
        return false;
    }
    stmt->stmt = NULL;
    stmt->type = RIPPLE_TXNSTMT_TYPE_BURST;

    stmtburst->batchcmd = (uint8 *)str->data;
    stmt->stmt = stmtburst;
    txn->stmts = lappend(txn->stmts, stmt);
    stmt = NULL;
    str->data = NULL;
    deleteStringInfo(str);

    return true;
}

/* 拼接burst insert语句 临时表，全列，ON CONFLICT */
static bool ripple_rebuild_burst_assembleinsert(HTAB* hattrs, ripple_rebuild_burstnode* burstnode, ripple_txn* txn)
{
    bool first                                  = true;
    bool need_comma                             = false;
    bool conskey_comma                          = false;
    bool missing_comma                          = false;
    int colcnt                                  = 0;
    int colindex                                = 0;
    int keyindex                                = 0;
    int missingmapsize                          = 0;
    int missingcnt                              = 0;
    int sortindex                               = 0;
    uint64 len                                  = 0;
    uint8* missingmap                           = NULL;
    char* type                                  = NULL;
    List* lattrs                                = NULL;
    StringInfo conskeystr                       = NULL;
    StringInfo valuestr                         = NULL;
    StringInfo updatestr                        = NULL;
    StringInfo insestrstr                       = NULL;
    StringInfo conflictstr                      = NULL;
    dlistnode* dlnode                           = NULL;
    ripple_txnstmt* stmt                        = NULL;
    ripple_txnstmt_burst* conskeyburst          = NULL;
    ripple_txnstmt_burst* insertburst           = NULL;
    ripple_txnstmt_burst* conflictburst         = NULL;
    ripple_rebuild_burstcolumn* key             = NULL;
    ripple_rebuild_burstrow** sortrow           = NULL;
    ripple_rebuild_burstrow** tmprow            = NULL;
    ripple_rebuild_burstrow* insertrow          = NULL;
    ripple_rebuild_bursttable* table            = NULL;
    xk_pg_parser_translog_tbcol_value* values   = NULL;
    xk_pg_parser_translog_tbcol_values* insert  = NULL;

    if (true == dlist_isnull(burstnode->dlinsertrows))
    {
        return true;
    }

    table = &burstnode->table;

    len = (sizeof(ripple_rebuild_burstrow*) * burstnode->dlinsertrows->length);

    sortrow = rmalloc0(len);
    if (NULL == sortrow)
    {
        elog(RLOG_WARNING, "rebuild burst assembleinsert sortrow oom");
        return false;
    }
    rmemset0(sortrow, 0, 0, len);

    /* 获取attribute */
    lattrs = (List*)ripple_attribute_getbyoid(burstnode->table.oid, hattrs);

    if (NULL == lattrs || NULL == lattrs->head)
    {
        elog(RLOG_WARNING, "ripple burst assembleinsert not find attribute by %lu", burstnode->table.oid);
        rfree(sortrow);
        return false;
    }

    /* 生成排序空间，筛选约束/主键修改的数据 */
    for (dlnode = burstnode->dlinsertrows->head; NULL != dlnode; dlnode = dlnode->next)
    {
        insertrow = (ripple_rebuild_burstrow*)dlnode->value;
        insert = (xk_pg_parser_translog_tbcol_values*)insertrow->row;
        /* 拼接约束修改的语句，临时表 */
        if (RIPPLE_REBUILD_BURSTROWFLAG_CHANGECONSKEY == insertrow->flag)
        {
            if (true == first)
            {
                conskeystr = makeStringInfo();
                insestrstr = makeStringInfo();
                valuestr = makeStringInfo();
                updatestr = makeStringInfo();

                conskeyburst = ripple_txnstmt_burst_init();
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
                conskeyburst->optype = RIPPLE_REBUILD_BURSTROWTYPE_INSERT;

                appendStringInfo(conskeystr, "CREATE TEMP TABLE IF NOT EXISTS \"%s_burst\" (op text ", burstnode->table.table);
                appendStringInfo(insestrstr, "INSERT INTO \"%s_burst\" (op",  burstnode->table.table);
                appendStringInfo(valuestr, "VALUES ('update' ");
                appendStringInfo(updatestr, "UPDATE \"%s\".\"%s\" SET ", burstnode->table.schema, burstnode->table.table);

                /* 拼接临时表主键列 */
                for (keyindex = 0; keyindex < table->keycnt; keyindex++)
                {
                    key = &table->keys[keyindex];
                    type = ripple_rebuild_burst_gettypename(lattrs, key->coltype, key->colname);
                    if (NULL == type )
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
                }

                /* 拼接临时表其他列及missing标识列 */
                need_comma = false;
                for (colindex = 0; colindex < insert->m_valueCnt; colindex++)
                {
                    values = &insert->m_new_values[colindex];
                    if (INFO_COL_IS_DROPED == values->m_info
                        || INFO_COL_IS_CUSTOM == values->m_info)
                    {
                        continue;
                    }
                    type = ripple_rebuild_burst_gettypename(lattrs, values->m_coltype, values->m_colName);
                    if (NULL == type )
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
                    appendStringInfo(conskeystr, "\"%s\" %s, \"%s_%s_missing\" char", values->m_colName, type, burstnode->table.table, values->m_colName);

                    appendStringInfo(insestrstr, ", ");
                    appendStringInfo(insestrstr, "\"%s\", \"%s_%s_missing\"", values->m_colName, burstnode->table.table, values->m_colName);

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

                    /* 拼接临时表update语句 */
                    if (true == ripple_rebuild_burst_colisconskey(table, values->m_colName))
                    {
                        appendStringInfo(updatestr, "\"%s\" = b.\"%s\"", values->m_colName, values->m_colName);
                    }
                    else
                    {
                        appendStringInfo(updatestr, "\"%s\" = CASE WHEN \"%s_%s_missing\" = 'f' THEN b.\"%s\" ELSE \"%s\".\"%s\".\"%s\" END", 
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

                /* 拼接临时表update的where条件 */
                appendStringInfo(updatestr, " FROM \"%s_burst\" b WHERE", burstnode->table.table);
                for (keyindex = 0; keyindex < table->keycnt; keyindex++)
                {
                    key = &table->keys[keyindex];
                    appendStringInfo(updatestr, " \"%s\".\"%s\".\"%s\" = b.\"%s_before\" AND", 
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

            /* 拼接临时表内填充的值 */
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
                    if (INFO_COL_IS_DROPED == values->m_info
                        || INFO_COL_IS_CUSTOM == values->m_info)
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

    /* 临时表方式stmt生成加入事务 */
    if (NULL != conskeystr && NULL != updatestr)
    {
        /* 初始化 ripple_txnstmt */
        stmt = ripple_txnstmt_init();
        if(NULL == stmt)
        {
            elog(RLOG_WARNING, "rebuild burst assembleinsert stmt init failed");
            rfree(conskeyburst);
            deleteStringInfo(conskeystr);
            deleteStringInfo(updatestr);
            rfree(sortrow);
            return false;
        }
        stmt->stmt = NULL;
        stmt->type = RIPPLE_TXNSTMT_TYPE_BURST;
        appendStringInfo(conskeystr, ";\n %s \n DROP TABLE \"%s_burst\" ", updatestr->data, burstnode->table.table);

        conskeyburst->batchcmd = (uint8 *)conskeystr->data;
        conskeystr->data = NULL;
        deleteStringInfo(conskeystr);
        deleteStringInfo(updatestr);
        conskeystr = NULL;
        updatestr = NULL;
        stmt->stmt = conskeyburst;
        txn->stmts = lappend(txn->stmts, (void*)stmt);
        stmt = NULL;
    }
    

    /* 根据去除约束/主键修改后的数量重新申请排序空间，去掉多余空间 */
    len = (sizeof(ripple_rebuild_burstrow*) * colcnt);
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

    /* 根据missingmap排序 */
    qsort(sortrow, colcnt, sizeof(ripple_rebuild_burstrow*), ripple_rebuild_burst_comparemissing);

    /* 设置初始missing值 */
    missingmapsize = 0;
    missingcnt = -1;
    missingmap = NULL;

    /* 根据排序好的数据，构建insert语句 */
    first = true;
    for (sortindex = 0; sortindex < colcnt; sortindex++)
    {
        insertrow = sortrow[sortindex];
        insert = (xk_pg_parser_translog_tbcol_values*)insertrow->row;

        /* 原始insert语句直接插入 */
        if (insertrow->op == RIPPLE_REBUILD_BURSTROWTYPE_INSERT)
        {
            if (true == first)
            {
                insestrstr = makeStringInfo();
                valuestr = makeStringInfo();
                appendStringInfo(insestrstr, "INSERT INTO \"%s\".\"%s\" (", burstnode->table.schema, burstnode->table.table);
                appendStringInfo(valuestr, "VALUES (");

                insertburst = ripple_txnstmt_burst_init();
                if (NULL == insertburst)
                {
                    elog(RLOG_WARNING, "rebuild burst assembleinsert insertburst init failed");
                    rfree(sortrow);
                    return false;
                }
                insertburst->optype = RIPPLE_REBUILD_BURSTROWTYPE_INSERT;

                need_comma = false; 
                for (colindex = 0; colindex < insert->m_valueCnt; colindex++)
                {
                    values = &insert->m_new_values[colindex];
                    /* 不处理的类型 */
                    if (INFO_COL_MAY_NULL == values->m_info
                        || INFO_COL_IS_DROPED == values->m_info
                        || INFO_COL_IS_CUSTOM == values->m_info)
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
                        char *temp_str = strSpecialCharReplace((char *)values->m_value);

                        appendStringInfo(insestrstr, "\"%s\"", values->m_colName);
                        if (temp_str)
                        {
                            appendStringInfo(valuestr, "'%s'", temp_str);
                            rfree(temp_str);
                        }
                        else
                        {
                            appendStringInfo(valuestr, "'%s'", (char *)values->m_value);
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
                if (INFO_COL_MAY_NULL == values->m_info 
                    || INFO_COL_IS_DROPED == values->m_info
                    || INFO_COL_IS_CUSTOM == values->m_info)
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
                    char *temp_str = strSpecialCharReplace((char *)values->m_value);
                    if (temp_str)
                    {
                        appendStringInfo(insestrstr, "'%s'", temp_str);
                        rfree(temp_str);
                    }
                    else
                    {
                        appendStringInfo(insestrstr, "'%s'", (char *)values->m_value);
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

        /* update生成的insert on conflict方式插入 */
        if (missingcnt != insertrow->missingcnt
            || memcmp(missingmap, insertrow->missingmap, missingmapsize))
        {
            /* massingmap切换重新生成语句 */
            missing_comma = false;
            if (NULL != conflictstr && NULL != valuestr)
            {
                /* 将上一条语句加入txn */
                stmt = ripple_txnstmt_init();
                if(NULL == stmt)
                {
                    deleteStringInfo(valuestr);
                    deleteStringInfo(conflictstr);
                    rfree(sortrow);
                    return false;
                }
                stmt->stmt = NULL;
                stmt->type = RIPPLE_TXNSTMT_TYPE_BURST;
                appendStringInfo(conflictstr, " %s", valuestr->data);
                elog(RLOG_WARNING, "burst on  conflict: %s ", conflictstr->data);
                deleteStringInfo(valuestr);
                valuestr = NULL;
                conflictburst->batchcmd = (uint8 *)conflictstr->data;
                conflictstr->data = NULL;
                deleteStringInfo(conflictstr);
                conflictstr = NULL;
                stmt->stmt = conflictburst;
                txn->stmts = lappend(txn->stmts, (void*)stmt);
                stmt = NULL;
                conflictburst = NULL;
            }

            /* update生成的insert 以on conflict方式拼接 */
            conflictstr = makeStringInfo();
            valuestr = makeStringInfo();
            conflictburst = ripple_txnstmt_burst_init();
            if (NULL == conflictburst)
            {
                rfree(sortrow);
                return false;
            }
            conflictburst->optype = RIPPLE_REBUILD_BURSTROWTYPE_UPDATE;

            appendStringInfo(conflictstr, "INSERT INTO \"%s\".\"%s\" (", burstnode->table.schema, burstnode->table.table);
            appendStringInfo(valuestr, "ON CONFLICT (");

            /* 拼接ON CONFLICT (id，...) */
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
            /* 拼接列名 */
            for (colindex = 0; colindex < insert->m_valueCnt; colindex++)
            {
                values = &insert->m_new_values[colindex];
                if (INFO_COL_IS_DROPED == values->m_info
                    || INFO_COL_IS_CUSTOM == values->m_info
                    || INFO_COL_MAY_NULL == values->m_info)
                {
                    continue;
                }

                if (need_comma)
                {
                    appendStringInfo(conflictstr, ", ");
                }
                appendStringInfo(conflictstr, "\"%s\"", values->m_colName);

                /* 拼接 DO UPDATE SET 语句 */
                if (false == ripple_rebuild_burst_colisconskey(table, values->m_colName))
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
        
        /* 拼接列值 */
        for (colindex = 0; colindex < insert->m_valueCnt; colindex++)
        {
            values = &insert->m_new_values[colindex];
            if (INFO_COL_IS_DROPED == values->m_info
                || INFO_COL_IS_CUSTOM == values->m_info
                || INFO_COL_MAY_NULL == values->m_info)
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


    /* 将最后一条语句加入txn */
    if (NULL != conflictstr && NULL != valuestr)
    {
        stmt = ripple_txnstmt_init();
        if(NULL == stmt)
        {
            rfree(sortrow);
            return false;
        }
        stmt->stmt = NULL;
        stmt->type = RIPPLE_TXNSTMT_TYPE_BURST;
        appendStringInfo(conflictstr, " %s", valuestr->data);
        deleteStringInfo(valuestr);
        valuestr = NULL;
        conflictburst->batchcmd = (uint8 *)conflictstr->data;
        conflictstr->data = NULL;
        deleteStringInfo(conflictstr);
        stmt->stmt = conflictburst;
        txn->stmts = lappend(txn->stmts, (void*)stmt);
        conflictburst = NULL;
        stmt = NULL;
    }

    if (NULL != insestrstr)
    {
        /* 初始化 ripple_txnstmt */
        stmt = ripple_txnstmt_init();
        if(NULL == stmt)
        {
            rfree(sortrow);
            return false;
        }
        stmt->stmt = NULL;
        stmt->type = RIPPLE_TXNSTMT_TYPE_BURST;
        
        appendStringInfo(insestrstr, ";");
        insertburst->batchcmd = (uint8 *)insestrstr->data;
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

/* burstnode 拼接语句 */
bool ripple_rebuild_burst_bursts2stmt(ripple_rebuild_burst* burst, ripple_cache_sysdicts* sysdicts, ripple_txn* txn)
{
    dlistnode* dlnode                       = NULL;
    ripple_rebuild_burstnode* burstnode     = NULL;
    if (true == dlist_isnull(burst->dlburstnodes))
    {
        return true;
    }

    /* 遍历burstnode */
    for (dlnode = burst->dlburstnodes->head; NULL != dlnode; dlnode = dlnode->next)
    {
        burstnode = (ripple_rebuild_burstnode*)dlnode->value;
        /* meta/ddl直接加入事务 */
        if (RIPPLE_REBUILD_BURSTNODETYPE_OTHER == burstnode->type)
        {
            txn->stmts = lappend(txn->stmts, burstnode->stmt);
            burstnode->stmt = NULL;
            continue;
        }

        if (RIPPLE_REBUILD_BURSTNODEFLAG_NOINDEX == burstnode->flag)
        {
            if (false == ripple_rebuild_burst_assemblepbedelete(burstnode, txn))
            {
                elog(RLOG_WARNING, "rebuild burst assemblepbedelete failed");
                return false;
            }

            if (false == ripple_rebuild_burst_assemblepbeinsert(burstnode, txn))
            {
                elog(RLOG_WARNING, "rebuild burst assemblepbeinsert failed");
                return false;
            }
        }
        else
        {
            if (false == ripple_rebuild_burst_assembledelete(burstnode, txn))
            {
                elog(RLOG_WARNING, "rebuild burst assembledelete failed");
                return false;
            }

            if (false == ripple_rebuild_burst_assembleinsert(sysdicts->by_attribute, burstnode, txn))
            {
                elog(RLOG_WARNING, "rebuild burst assembleinsert failed");
                return false;
            }
        }
    }
    return true;
}

/* burstcolumn 资源释放 */
void ripple_rebuild_burstcolumn_free(ripple_rebuild_burstcolumn* burstcolumn, int colcnt)
{
    int colindex                        = 0;
    ripple_rebuild_burstcolumn column   = {'\0'};

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

/* burstrow 资源释放 */
void ripple_rebuild_burstrow_free(void* args)
{
    ripple_rebuild_burstrow* burstrow = NULL;

    if (NULL == args)
    {
        return;
    }

    burstrow = (ripple_rebuild_burstrow*)args;

    if (burstrow->missingmap)
    {
        rfree(burstrow->missingmap);
    }
    

    if (burstrow->row)
    {
        //todo 释放row
        heap_free_trans_result((xk_pg_parser_translog_tbcolbase*)burstrow->row);
        
    }

    rfree(burstrow);
    return;
}

/* bursttable 资源释放 函数内不是放bursttable */
void ripple_rebuild_bursttable_free(void* args)
{
    ripple_rebuild_bursttable* bursttable = NULL;

    if (NULL == args)
    {
        return;
    }

    bursttable = (ripple_rebuild_bursttable*)args;

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
        ripple_rebuild_burstcolumn_free(bursttable->keys, bursttable->keycnt);
    }
    rfree(bursttable);
    return;
}

/* burstnode 资源释放 */
void ripple_rebuild_burstnode_free(void* args)
{
    ripple_rebuild_burstnode* burstnode = NULL;

    if (NULL == args)
    {
        return;
    }

    burstnode = (ripple_rebuild_burstnode*)args;

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
        ripple_rebuild_burstcolumn_free(burstnode->table.keys, burstnode->table.keycnt);
    }
    
    dlist_free(burstnode->dldeleterows, ripple_rebuild_burstrow_free);
    dlist_free(burstnode->dlinsertrows, ripple_rebuild_burstrow_free);

    if (burstnode->stmt)
    {
        //todo 释放stmt
        ripple_txnstmt_free(burstnode->stmt);
    }

    rfree(burstnode);
    return;
}

/* burst 资源释放 */
void ripple_rebuild_burst_free(void* args)
{
    ripple_rebuild_burst* burst = NULL;

    if (NULL == args)
    {
        return;
    }

    burst = (ripple_rebuild_burst*)args;
    
    dlist_free(burst->dlbursttable, ripple_rebuild_bursttable_free);
    dlist_free(burst->dlburstnodes, ripple_rebuild_burstnode_free);

    rfree(burst);
    return;
}
