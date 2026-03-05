#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/algorithm/crc/crc_check.h"
#include "fastcompare/ripple_fastcompare_chunk.h"
#include "fastcompare/ripple_fastcompare_simpledatachunk.h"
#include "fastcompare/ripple_fastcompare_datacmpresult.h"
#include "fastcompare/ripple_fastcompare_datacompare.h"
#include "fastcompare/ripple_fastcompare_columnvalue.h"
#include "fastcompare/ripple_fastcompare_simplerow.h"
#include "fastcompare/ripple_fastcompare_datacmpresultitem.h"

typedef struct DatacCompareHashEntryItem
{
    uint32  columncrc;  /* 行crc计算结果 */
    List   *privalues;  /* ripple_fastcompare_columnvalue */
} DatacCompareHashEntryItem;

typedef struct DatacCompareHashEntry
{
    uint32  crc;        /* privalues计算而来 */
    List *value_list;   /* DatacCompareHashEntryItem */
} DatacCompareHashEntry;

static DatacCompareHashEntryItem *DatacCompareHashEntryItem_init(void)
{
    DatacCompareHashEntryItem *result = NULL;

    result = rmalloc0(sizeof(DatacCompareHashEntryItem));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(DatacCompareHashEntryItem));
    return result;
}

static void DatacCompareHashEntryItem_clean(DatacCompareHashEntryItem *item)
{
    if (item)
    {
        if (item->privalues)
        {
            ripple_fastcompare_columnvalue_list_clean(item->privalues);
        }
        rfree(item);
    }
}

static List *DatacCompareRowListRemoveAndClean(List *list, ListCell *cell, ListCell *prev)
{
    ripple_fastcompare_simplerow *row = (ripple_fastcompare_simplerow *)lfirst(cell);
    if (row->privalues)
    {
        ripple_fastcompare_columnvalue_list_clean(row->privalues);
    }
    list = list_delete_cell(list, cell, prev);
    rfree(row);
    return list;
}

static List *DatacCompareHashEntryItemListRemoveAndClean(List *list, ListCell *cell)
{
    DatacCompareHashEntryItem *item = (DatacCompareHashEntryItem *)lfirst(cell);
    if (item->privalues)
    {
        ripple_fastcompare_columnvalue_list_clean(item->privalues);
    }
    list = list_delete(list, item);
    rfree(item);
    return list;
}

/* 生成哈希, 并将chunk中的行链表赋值给hash中保存, 原链表置空 */
static HTAB *datachunk2DatacCompareHash(ripple_fastcompare_simpledatachunk *chunk)
{
    HTAB *result = NULL;
    HASHCTL hctl = {'\0'};
    ListCell *cell = NULL;

    hctl.keysize = sizeof(uint32);
    hctl.entrysize = sizeof(DatacCompareHashEntry);
    result = hash_create("datachunk2DatacCompareHash",
                                      1024,
                                     &hctl,
                                      HASH_ELEM | HASH_BLOBS);

    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }

    /* 遍历链表, 写入hash */
    foreach(cell, chunk->data)
    {
        DatacCompareHashEntry *entry = NULL;
        uint32 pkey_crc = 0;
        ripple_fastcompare_simplerow *row = NULL;
        DatacCompareHashEntryItem *item = NULL;
        bool find = false;

        row = (ripple_fastcompare_simplerow *)lfirst(cell);
        pkey_crc = ripple_fastcompare_columnvalue_list_crc(row->privalues);
        entry = hash_search(result, &pkey_crc, HASH_ENTER, &find);

        /* entry初始化 */
        if (!find)
        {
            entry->value_list = NULL;
        }

        /* 40亿分之1的概率碰撞 */
        entry->crc = pkey_crc;
        item = DatacCompareHashEntryItem_init();
        item->privalues = row->privalues;
        item->columncrc = row->crc;
        entry->value_list = lappend(entry->value_list, item);

        /* 原pkey链表置空 */
        row->privalues = NULL;
    }
    return result;
}

/* 将entry从哈希中移除 */
static void datacompareRefCorrHashRemoveAndClean(DatacCompareHashEntry *entry, HTAB *hash)
{
    uint32 key = entry->crc;

    /* 如果存在pkey链表, 清理 */
    if (entry->value_list)
    {
        ListCell *cell = NULL;
        foreach(cell, entry->value_list)
        {
            DatacCompareHashEntryItem *item = (DatacCompareHashEntryItem *)lfirst(cell);
            DatacCompareHashEntryItem_clean(item);
        }
        list_free(entry->value_list);
        entry->value_list = NULL;
    }

    hash_search(hash, &key, HASH_REMOVE, NULL);
}

/* 比较pkey链表的值是否一样 */
static bool datacompareHashComparePkey(List *pkey_ref, List *pkey_corr)
{
    ListCell *cell_ref = NULL;
    ListCell *cell_corr = list_head(pkey_corr);
    foreach(cell_ref, pkey_ref)
    {
        ripple_fastcompare_columnvalue *col_ref = lfirst(cell_ref);
        ripple_fastcompare_columnvalue *col_corr = lfirst(cell_corr);

        if (col_ref->len != col_corr->len)
        {
            return false;
        }
        if (memcmp(col_ref->value, col_corr->value, col_ref->len))
        {
            return false;
        }
        cell_corr = lnext(cell_corr);
    }
    return true;
}

/* 遍历 ref_hash 查找 corr_hash crc校验不通过时保存记录为 update */
static List *datacompareHashCompareList(ripple_fastcompare_datacompare *cmp, HTAB *ref_hash, List *corr_list)
{
    DatacCompareHashEntry *entry = NULL;
    DatacCompareHashEntryItem *item = NULL;
    uint32 pkey_crc = 0;
    ListCell *corr_cell = NULL;
    ListCell *temp_next_cell = NULL;
    ListCell *corr_prev_cell = NULL;

    corr_cell = list_head(corr_list);

    /* 遍历待比较链表 */
    while(corr_cell)
    {
        ripple_fastcompare_simplerow *row = (ripple_fastcompare_simplerow *)lfirst(corr_cell);
        pkey_crc = ripple_fastcompare_columnvalue_list_crc(row->privalues);

        entry = hash_search(ref_hash, &pkey_crc, HASH_FIND, NULL);
        temp_next_cell = lnext(corr_cell);

        /* 根据crc找没找到 */
        if (entry)
        {
            /* 多条, 存在碰撞 */
            ListCell *ref_item_cell = list_head(entry->value_list);
            ListCell *temp_ref_item_cell_next = NULL;

            /* 遍历entry中的链表 */
            while (ref_item_cell)
            {
                item = (DatacCompareHashEntryItem *)lfirst(ref_item_cell);
                temp_ref_item_cell_next = lnext(ref_item_cell);

                /* 比较主键值是否相等 */
                if (datacompareHashComparePkey(item->privalues, row->privalues))
                {
                    /* 相等, 检查crc */
                    if (item->columncrc != row->crc)
                        {
                            /* crc校验不通过 */
                            /* 记录有冲突的数据 */
                            ripple_fastcompare_datacmpresultitem *resitem = NULL;

                            if (!cmp->result)
                            {
                                cmp->result = ripple_fastcompare_datacmpresult_init();
                            }

                            resitem = ripple_fastcompare_datacmpresultitem_init(RIPPLE_FASTCOMPARE_DATACMPRESULTITEM_OP_UPDATE);
                            resitem->privalues = item->privalues;

                            /* 哈希内链表置空 */
                            item->privalues = NULL;
                            cmp->result->checkresult = lappend(cmp->result->checkresult, resitem);
                        }
                    /* 从entry的链表中删除 */
                    entry->value_list = DatacCompareHashEntryItemListRemoveAndClean(entry->value_list, ref_item_cell);
                    /* 从待比较链表中删除 */
                    corr_list = DatacCompareRowListRemoveAndClean(corr_list, corr_cell, corr_prev_cell);
                    corr_cell = NULL;
                    /* 已经找到了, 直接跳出entry的链表循环 */
                    break;
                }
                ref_item_cell = temp_ref_item_cell_next;
            }
            /* 额外判断, 判断entry的list是否为空, 为空删除entry */
            if (entry->value_list == NIL)
            {
                datacompareRefCorrHashRemoveAndClean(entry, ref_hash);
            }
        }
        if (corr_cell)
        {
            corr_prev_cell = corr_cell;
        }
        corr_cell = temp_next_cell;
    }
    return corr_list;
}

/* 遍历哈希, 将数据保存到result里, 操作类型由op决定 */
static void datacompareHashGetCompResult(ripple_fastcompare_datacompare *cmp, HTAB *hash, int op)
{
    HASH_SEQ_STATUS status;
    DatacCompareHashEntry *entry = NULL;

    /* 遍历 */
    hash_seq_init(&status, hash);
    while ((entry = hash_seq_search(&status)) != NULL)
    {
        ListCell *cell = list_head(entry->value_list);
        ListCell *cell_next = NULL;
        while(cell)
        {
            DatacCompareHashEntryItem *item = (DatacCompareHashEntryItem *)lfirst(cell);
            ripple_fastcompare_datacmpresultitem *resitem = NULL;
            cell_next = lnext(cell);
            /* 记录有冲突的数据 */

            if (!cmp->result)
            {
                cmp->result = ripple_fastcompare_datacmpresult_init();
            }

            resitem = ripple_fastcompare_datacmpresultitem_init(op);
            resitem->privalues = item->privalues;

            /* 哈希内链表置空 */
            item->privalues = NULL;

            /* 判断操作类型, 放入到不同的结果集中 */
            if (op == RIPPLE_FASTCOMPARE_DATACMPRESULTITEM_OP_INSERT)
            {
                cmp->result->checkresult = lappend(cmp->result->checkresult, resitem);
            }
            else
            {
                cmp->result->corrresult = lappend(cmp->result->corrresult, resitem);
            }
            cell = cell_next;
        }
        /* 从hash中remove entry并清理 */
        datacompareRefCorrHashRemoveAndClean(entry, hash);
    }
}

/* 遍历链表, 将数据保存到result里, 操作类型由op决定 */
static List *datacompareListGetCompResult(ripple_fastcompare_datacompare *cmp, List *list, int op)
{
    ListCell *cell = NULL;
    ListCell *cell_next = NULL;

    if (!list)
    {
        return NULL;
    }

    cell = list_head(list);

    /* 遍历 */
    while(cell)
    {
        ripple_fastcompare_simplerow *row = (ripple_fastcompare_simplerow *)lfirst(cell);
        ripple_fastcompare_datacmpresultitem *resitem = NULL;

        cell_next = lnext(cell);

        /* 记录有冲突的数据 */

        if (!cmp->result)
        {
            cmp->result = ripple_fastcompare_datacmpresult_init();
        }

        resitem = ripple_fastcompare_datacmpresultitem_init(op);
        resitem->privalues = row->privalues;

        /* 哈希内链表置空 */
        row->privalues = NULL;

        /* 判断操作类型, 放入到不同的结果集中 */
        if (op == RIPPLE_FASTCOMPARE_DATACMPRESULTITEM_OP_INSERT)
        {
            cmp->result->checkresult = lappend(cmp->result->checkresult, resitem);
        }
        else
        {
            cmp->result->corrresult = lappend(cmp->result->corrresult, resitem);
        }

        cell = cell_next;
    }
    list_free_deep(list);
    return NULL;
}

/* 初始化 */
ripple_fastcompare_datacompare *ripple_fastcompare_init_datacompare(void)
{
    ripple_fastcompare_datacompare *result = NULL;

    result = rmalloc0(sizeof(ripple_fastcompare_datacompare));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(ripple_fastcompare_datacompare));

    return result;
}

static void ripple_fastcompare_compare_simple_chunk_easy_mode(ripple_fastcompare_datacompare *cmp)
{
    ripple_fastcompare_simpledatachunk *ref_chunk = NULL;

    ref_chunk = cmp->refchunk;

    /* 第三次遍历, 遍历 corr_chunk->data list, 记录需要delete的数据 */
    ref_chunk->data = datacompareListGetCompResult(cmp, ref_chunk->data, RIPPLE_FASTCOMPARE_DATACMPRESULTITEM_OP_INSERT);
}

bool ripple_fastcompare_compare_simple_chunk(ripple_fastcompare_datacompare *cmp)
{
    ripple_fastcompare_simpledatachunk *ref_chunk = NULL;
    ripple_fastcompare_simpledatachunk *corr_chunk = NULL;
    HTAB *ref_hash = NULL;

    if (!cmp || !cmp->refchunk)
    {
        elog(RLOG_ERROR, "do data compare, point is NULL");
    }

    if (!cmp->corrchunk)
    {
        ripple_fastcompare_compare_simple_chunk_easy_mode(cmp);
        return false;
    }

    ref_chunk = cmp->refchunk;
    corr_chunk = cmp->corrchunk;

    /* 全数据crc校验通过返回真 */
    if (ref_chunk->crc == corr_chunk->crc)
    {
        return true;
    }

    /* 生成哈希 */
    ref_hash = datachunk2DatacCompareHash(ref_chunk);

    /* 第一次遍历, 遍历 ref_hash 查找 corrhash */
    corr_chunk->data = datacompareHashCompareList(cmp, ref_hash, corr_chunk->data);

    /* 第二次遍历, 遍历 ref_hash, 记录需要insert的数据 */
    datacompareHashGetCompResult(cmp, ref_hash, RIPPLE_FASTCOMPARE_DATACMPRESULTITEM_OP_INSERT);

    /* 第三次遍历, 遍历 corr_chunk->data list, 记录需要delete的数据 */
    corr_chunk->data = datacompareListGetCompResult(cmp, corr_chunk->data, RIPPLE_FASTCOMPARE_DATACMPRESULTITEM_OP_DELETE);

    /* 清理hash */
    hash_destroy(ref_hash);

    return false;
}

void ripple_fastcompare_datacompare_set_chunk(ripple_fastcompare_datacompare *cmp, 
                                              ripple_fastcompare_simpledatachunk *refchunk,
                                              ripple_fastcompare_simpledatachunk *corrchunk)
{
    if (cmp->result)
    {
        ripple_fastcompare_datacmpresult_free(cmp->result);
        cmp->result = NULL;
    }

    if (cmp->refchunk)
    {
        ripple_fastcompare_simpledatachunk_clean(cmp->refchunk);
        cmp->refchunk = NULL;
     }

    cmp->refchunk = refchunk;
    cmp->corrchunk = corrchunk;
}

void ripple_fastcompare_datacompare_free(ripple_fastcompare_datacompare *cmp)
{
    if (NULL == cmp)
    {
        return;
    }
    
    if (cmp->result)
    {
        ripple_fastcompare_datacmpresult_free(cmp->result);
    }

    /* cmp->corrchunk是引用, 不释放 */
    // if (cmp->corrchunk)
    // {
    //     ripple_fastcompare_simpledatachunk_clean(cmp->corrchunk);
    // }

    if (cmp->refchunk)
    {
        ripple_fastcompare_simpledatachunk_clean(cmp->refchunk);
    }
    
    rfree(cmp);
}
