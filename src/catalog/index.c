#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/guc/guc.h"
#include "port/file/fd.h"
#include "utils/hash/hash_utils.h"
#include "utils/conn/conn.h"
#include "misc/misc_stat.h"
#include "misc/misc_control.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/cache_sysidcts.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/transcache.h"
#include "catalog/catalog.h"
#include "catalog/index.h"

static void index_dict_index_free(pg_sysdict_Form_pg_index index)
{
    if (index)
    {
        if (index->indkey)
        {
            rfree(index->indkey);
        }
        rfree(index); 
    }
}

static uint32 *index_make_key_from_vector(char *vector_str, int32 natt)
{
    uint32 *result = NULL;
    int index_col = 0;
    char temp_convert[12] = {'\0'};
    char *left = vector_str;
    char *right = left;

    result = rmalloc0(natt * sizeof(int32));
    rmemset0(result, 0, 0, natt * sizeof(int32));

    for (index_col = 0; index_col < natt; index_col++)
    {
        int len = 0;

        right = strstr(left, " ");
        if (right)
        {
            len = right - left;
            right++;
        }
        else
        {
            len = strlen(left);
        }
        memcpy(temp_convert, left, len);
        result[index_col] = atoi(temp_convert);

        if (result[index_col] == 0)
        {
            /* 异常情况, index索引列可能为表达式 */
            //todo liuzihe error
        }

        rmemset1(temp_convert, 0, 0, 12);
        left = right;
    }

    return result;
}

/* 生成系统字典 pg_index */
void index_getfromdb(PGconn *conn, cache_sysdicts* sysdicts)
{
    int index_row;
    int index_col;
    int max_raw;
    bool find;
    char sql_exec[MAX_EXEC_SQL_LEN];
    PGresult* res;
    pg_sysdict_Form_pg_index catalog_index;
    catalog_index_value *idx_val;
    HASHCTL index_hash_ctl;
    catalog_index_hash_entry *index_entry;
    
    /* 初始化变量 */
    index_row = 0;
    index_col = 0;
    max_raw = 0;
    find = false;
    rmemset1(sql_exec, 0, '\0', MAX_EXEC_SQL_LEN);
    res = NULL;
    catalog_index = NULL;
    idx_val = NULL;
    rmemset1(&index_hash_ctl, 0, '\0', sizeof(index_hash_ctl));
    index_entry = NULL;

    /* hash初始化 */
    index_hash_ctl.keysize = sizeof(Oid);
    index_hash_ctl.entrysize = sizeof(catalog_index_hash_entry);
    sysdicts->by_index = hash_create("catalog_sysdict_index", 2048, &index_hash_ctl,
                        HASH_ELEM | HASH_BLOBS);

    /* 排除非普通表的索引 */
    sprintf(sql_exec, "SELECT ind.indrelid, "
                      "ind.indexrelid, "
                      "ind.indisprimary, "
                      "ind.indisreplident, "
                      "ind.indnatts, "
                      "ind.indkey "
                      "FROM pg_index ind "
                      "LEFT JOIN pg_class rel "
                      "ON ind.indrelid = rel.oid "
                      "WHERE ind.indrelid >= 16384 and rel.relkind = 'r' and ind.indisunique = TRUE "
                      "ORDER BY ind.indrelid;");

    res = conn_exec(conn, sql_exec);
    if (NULL == res)
    {
        elog(RLOG_ERROR, "pg_index query failed");
    }

    max_raw = PQntuples(res);
    /* 遍历结果 */
    for (index_row = 0; index_row < max_raw; index_row++)
    {
        index_col = 0;

        catalog_index = (pg_sysdict_Form_pg_index)rmalloc0(sizeof(pg_parser_sysdict_pgindex));
        if(NULL == catalog_index)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(catalog_index, 0, '\0', sizeof(pg_parser_sysdict_pgindex));

        /* indrelid */
        sscanf(PQgetvalue(res, index_row, index_col++), "%u", &catalog_index->indrelid);

        /* indexrelid */
        sscanf(PQgetvalue(res, index_row, index_col++), "%u", &catalog_index->indexrelid);

        /* indisprimary */
        catalog_index->indisprimary = (((char *)PQgetvalue(res, index_row, index_col++))[0]) == 't' ? true : false;

        /* indisreplident */
        catalog_index->indisreplident = (((char *)PQgetvalue(res, index_row, index_col++))[0]) == 't' ? true : false;

        /* indnatts */
        sscanf(PQgetvalue(res, index_row, index_col++), "%d", &catalog_index->indnatts);

        /* indkey */
        catalog_index->indkey = index_make_key_from_vector(PQgetvalue(res, index_row, index_col++), catalog_index->indnatts);

        idx_val = (catalog_index_value *)rmalloc0(sizeof(catalog_index_value));
        if(NULL == idx_val)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(idx_val, 0, '\0', sizeof(catalog_index_value));

        idx_val->oid = catalog_index->indrelid;
        idx_val->index = catalog_index;

        /* 赋值结束, 根据indrelid查找哈希 */
        index_entry = hash_search(sysdicts->by_index, &catalog_index->indrelid, HASH_ENTER, &find);
        if (!find)
        {
            /* 第一次需要初始化 */
            index_entry->oid = catalog_index->indrelid;
            index_entry->index_list = NULL;
        }
        index_entry->index_list = lappend(index_entry->index_list, idx_val);
    }

    /* 结束, 清理 */
    PQclear(res);
}

/* colvalue2index */
catalogdata* index_colvalue2index(void* in_colvalue)
{
    catalogdata* catalog_data = NULL;
    catalog_index_value* index_value = NULL;
    pg_sysdict_Form_pg_index pgindex = NULL;
    pg_parser_translog_tbcol_value* colvalue = NULL;
    uint32_t temp_oid = InvalidOid;
    bool unique = false;
    char *temp_key = NULL;

    colvalue = (pg_parser_translog_tbcol_value*)in_colvalue;

    /* 首先检查 indrelid 和 indisunique */
    temp_oid = (uint32)atoi((char*)((colvalue + 1)->m_value));
    unique = ((char*)((colvalue + 4)->m_value))[0] == 't' ? true : false;

    if ((temp_oid < 16384) || !unique)
    {
        return NULL;
    }

    /* 值转换 */
    catalog_data = (catalogdata*)rmalloc1(sizeof(catalogdata));
    if(NULL == catalog_data)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(catalog_data, 0, '\0', sizeof(catalogdata));

    index_value = (catalog_index_value*)rmalloc1(sizeof(catalog_index_value));
    if(NULL == index_value)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(index_value, 0, '\0', sizeof(catalog_index_value));

    catalog_data->catalog = index_value;
    catalog_data->type = CATALOG_TYPE_INDEX;

    pgindex = (pg_sysdict_Form_pg_index)rmalloc1(sizeof(pg_parser_sysdict_pgindex));
    if(NULL == pgindex)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pgindex, 0, '\0', sizeof(pg_parser_sysdict_pgindex));
    index_value->index = pgindex;

    /* indrelid 1 */
    pgindex->indrelid = temp_oid;
    index_value->oid = pgindex->indrelid;

    /* indexrelid 0 */
    sscanf((char*)((colvalue + 0)->m_value), "%u", &pgindex->indexrelid);

    /* indisprimary 5 */
    pgindex->indisprimary = ((char*)((colvalue + 5)->m_value))[0] == 't' ? true : false;

    /* indisreplident 13 */
    pgindex->indisreplident= ((char*)((colvalue + 13)->m_value))[0] == 't' ? true : false;

    /* indnatts 2 */
    sscanf((char*)((colvalue + 2)->m_value), "%d", &pgindex->indnatts);

    /* indkey 14 */
    temp_key = (char*)((colvalue + 14)->m_value);
    pgindex->indkey = index_make_key_from_vector(temp_key, pgindex->indnatts);

    return catalog_data;
}

static bool check_index_catalog_data_in_list(List *index_list, catalog_index_value* new_index_value)
{
    ListCell *cell = NULL;
    bool find = false;

    if (!index_list)
    {
        return find;
    }

    foreach(cell, index_list)
    {
        catalog_index_value* index_value = (catalog_index_value*)lfirst(cell);
        pg_sysdict_Form_pg_index dict_index = index_value->index;
        if (dict_index->indexrelid == new_index_value->index->indexrelid)
        {
            find = true;
            break;
        }
    }
    return find;
}

static bool update_index_catalog_data_in_lsit(List *index_list, catalog_index_value* new_index_value)
{
    ListCell *cell = NULL;
    bool find = false;

    if (!index_list)
    {
        return find;
    }

    cell = list_head(index_list);

    /* 遍历链表, 查找记录 */
    while (cell)
    {
        ListCell *next_cell = cell->next;
        catalog_index_value* index_value = (catalog_index_value*)lfirst(cell);
        pg_sysdict_Form_pg_index dict_index = index_value->index;

        /* 查找指定的index记录 */
        if (dict_index->indexrelid == new_index_value->index->indexrelid)
        {
            uint32 old_natt = dict_index->indnatts;

            /* 判断键值是否变更 */
            if (old_natt != new_index_value->index->indnatts)
            {
                if (dict_index->indkey)
                {
                    rfree(dict_index->indkey);
                    dict_index->indkey = rmalloc0(sizeof(uint32) * dict_index->indnatts);
                    if(NULL == dict_index->indkey)
                    {
                        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
                    }
                }
                rmemcpy0(dict_index->indkey, 0, new_index_value->index->indkey, sizeof(uint32) * new_index_value->index->indnatts);
            }

            /* 重新赋值 */
            rmemcpy0(dict_index, 0, new_index_value->index, offsetof(pg_parser_sysdict_pgindex, indkey));
            find = true;
            break;
        }
        cell = next_cell;
    }
    return find;
}

/* catalogdata2transcache */
void index_catalogdata2transcache(cache_sysdicts* sysdicts, catalogdata* catalogdata)
{
    bool found = false;
    catalog_index_value* new_index_value = NULL;
    catalog_index_hash_entry* indexInHash = NULL;

    if(NULL == catalogdata || NULL == catalogdata->catalog)
    {
        return;
    }

    new_index_value = (catalog_index_value*)catalogdata->catalog;
    elog(RLOG_DEBUG, "op:%d, newindex, indrelid:%u, primary:%s, isreplident:%s, keynum:%d",
                    catalogdata->op,
                    new_index_value->index->indexrelid,
                    new_index_value->index->indisprimary ? "true" : "false",
                    new_index_value->index->indisreplident ? "true" : "false",
                    new_index_value->index->indnatts);

    if(CATALOG_OP_INSERT == catalogdata->op)
    {
        catalog_index_value* index_value = NULL;
        pg_sysdict_Form_pg_index dict_index = NULL;

        indexInHash = hash_search(sysdicts->by_index, &new_index_value->oid, HASH_ENTER, &found);
        if(false == found)
        {
            indexInHash->index_list = NULL;
        }
        indexInHash->oid = new_index_value->oid;

        if (!check_index_catalog_data_in_list(indexInHash->index_list, new_index_value))
        {
            /* 申请空间，并赋值 */
            dict_index = (pg_sysdict_Form_pg_index)rmalloc0(sizeof(pg_parser_sysdict_pgindex));
            if(NULL == dict_index)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemcpy0(dict_index, 0, new_index_value->index, sizeof(pg_parser_sysdict_pgindex));

            dict_index->indkey = rmalloc0(sizeof(uint32) * dict_index->indnatts);
            if(NULL == dict_index->indkey)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemcpy0(dict_index->indkey, 0, new_index_value->index->indkey, sizeof(uint32) * dict_index->indnatts);

            index_value = (catalog_index_value *)rmalloc0(sizeof(catalog_index_value));
            if(NULL == index_value)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(index_value, 0, 0, sizeof(catalog_index_value));
            index_value->oid = new_index_value->oid;
            index_value->index = dict_index;

            /* 添加到链表中 */
            indexInHash->index_list = lappend(indexInHash->index_list, index_value);
        }
        else
        {
            elog(RLOG_DEBUG, "index r:%u,i:%u find in index hash list, do update",
                                new_index_value->index->indrelid, new_index_value->index->indexrelid);
            update_index_catalog_data_in_lsit(indexInHash->index_list, new_index_value);
        }
    }
    else if(CATALOG_OP_DELETE == catalogdata->op)
    {
        indexInHash = hash_search(sysdicts->by_index, &new_index_value->oid, HASH_FIND, NULL);

        if(NULL != indexInHash)
        {
            if(NULL != indexInHash->index_list)
            {
                ListCell *cell = list_head(indexInHash->index_list);
                ListCell *cell_prev = NULL;
                /* 遍历链表, 查找记录 */
                while (cell)
                {
                    ListCell *next_cell = cell->next;
                    catalog_index_value* index_value = (catalog_index_value*)lfirst(cell);
                    pg_sysdict_Form_pg_index dict_index = index_value->index;

                    /* 查找指定的index记录 */
                    if (dict_index->indexrelid == new_index_value->index->indexrelid)
                    {
                        index_dict_index_free(dict_index);
                        rfree(index_value);
                        indexInHash->index_list = list_delete_cell(indexInHash->index_list, cell, cell_prev);
                        break;
                    }
                    else
                    {
                        cell_prev = cell;
                    }

                    cell = next_cell;
                }

                /* 检查链表是否为空, 为空时从哈希中删除这条记录 */
                if (!indexInHash->index_list)
                {
                    hash_search(sysdicts->by_index, &new_index_value->oid, HASH_REMOVE, NULL);
                }
            }
            else
            {
                elog(RLOG_WARNING, "find index hash info, but list is null");
                hash_search(sysdicts->by_index, &new_index_value->oid, HASH_REMOVE, NULL);
            }
        }
    }
    else if(CATALOG_OP_UPDATE == catalogdata->op)
    {
        indexInHash = hash_search(sysdicts->by_index, &new_index_value->oid, HASH_FIND, &found);
        if(NULL == indexInHash || NULL == (indexInHash->index_list))
        {
            elog(RLOG_WARNING, "index r:%u,i:%u can not fond in index hash",
                                new_index_value->index->indrelid, new_index_value->index->indexrelid);
            return;
        }

        if (!update_index_catalog_data_in_lsit(indexInHash->index_list, new_index_value))
        {
            elog(RLOG_WARNING, "index r:%u,i:%u can not fond in index hash list",
                                new_index_value->index->indrelid, new_index_value->index->indexrelid);
            return;
        }
    }
    else
    {
        elog(RLOG_ERROR, "unknown op:%d", catalogdata->op);
    }
}

void index_catalogdatafree(catalogdata* catalogdata)
{
    catalog_index_value* catalog = NULL;
    if(NULL == catalogdata)
    {
        return;
    }

    /* catalog 内存释放 */
    if(NULL != catalogdata->catalog)
    {
        catalog = (catalog_index_value*)catalogdata->catalog;
        index_dict_index_free(catalog->index);
        rfree(catalogdata->catalog);
    }
    rfree(catalogdata);
}

/* 根据oid获取pg_index链表 */
void* index_getbyoid(Oid oid, HTAB* by_index)
{
    bool found = false;
    catalog_index_hash_entry *indexentry = NULL;
    indexentry = hash_search(by_index, &oid, HASH_FIND, &found);
    if(false == found)
    {
        return NULL;
    }

    /* need free */
    return (void*)list_copy(indexentry->index_list);
}

void indexcache_write(HTAB* indexcache, uint64 *offset, sysdict_header_array* array)
{
    int     fd;
    uint64 page_num = 0;
    uint64 page_offset = PAGE_HEADER_SIZE;
    HASH_SEQ_STATUS status;
    char buffer[FILE_BLK_SIZE];
    pg_sysdict_Form_pg_index index = NULL;
    catalog_index_hash_entry *index_entry = NULL;
    catalog_index_value *index_value = NULL;

    array[CATALOG_TYPE_INDEX - 1].type = CATALOG_TYPE_INDEX;
    array[CATALOG_TYPE_INDEX - 1].offset = *offset;
    page_num = *offset;

    rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
    fd = osal_basic_open_file(SYSDICTS_TMP_FILE,
                        O_RDWR | O_CREAT | BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not create file %s", SYSDICTS_TMP_FILE);
    }

    hash_seq_init(&status, indexcache);
    while ((index_entry = hash_seq_search(&status)) != NULL)
    {
        ListCell *cell = NULL;

        foreach(cell, index_entry->index_list)
        {
            uint32 fix_len = 0;
            uint32 varlena_len = 0;

            index_value = (catalog_index_value*)lfirst(cell);
            index = index_value->index;

            fix_len = offsetof(pg_parser_sysdict_pgindex, indkey);
            varlena_len = sizeof(uint32) * index->indnatts;

            if(page_offset + fix_len + varlena_len > FILE_BLK_SIZE)
            {
                rmemcpy1(buffer, 0, &page_offset, PAGE_HEADER_SIZE);
                if (osal_file_pwrite(fd, buffer, FILE_BLK_SIZE, *offset) != FILE_BLK_SIZE) {
                    elog(RLOG_ERROR, "could not write to file %s", SYSDICTS_TMP_FILE);
                    osal_file_close(fd);
                    return;
                }
                rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
                page_num = *offset + page_offset;
                *offset += FILE_BLK_SIZE;
                page_offset = PAGE_HEADER_SIZE;
            }
            rmemcpy1(buffer, page_offset, index, fix_len);
            page_offset += fix_len;

            rmemcpy1(buffer, page_offset, index->indkey, varlena_len);
            page_offset += varlena_len;
        }
    }

    if (page_offset > PAGE_HEADER_SIZE)
    {
        rmemcpy1(buffer, 0, &page_offset, PAGE_HEADER_SIZE);

        if (osal_file_pwrite(fd, buffer, FILE_BLK_SIZE, *offset) != FILE_BLK_SIZE) {
            elog(RLOG_ERROR, "could not write to file %s", SYSDICTS_TMP_FILE);
            osal_file_close(fd);
            return;
        }
        page_num = page_offset + *offset;
        *offset += FILE_BLK_SIZE;
    }

    if(0 != osal_file_sync(fd))
    {
        elog(RLOG_ERROR, "could not fsync file %s", SYSDICTS_TMP_FILE);
    }

    if(osal_file_close(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", SYSDICTS_TMP_FILE);
    }

    array[CATALOG_TYPE_INDEX - 1].len = page_num;

    return;
}

HTAB* indexcache_load(sysdict_header_array* array)
{
    int r = 0;
    int fd = -1;
    HTAB* indexhtab;
    HASHCTL hash_ctl;
    bool found = false;
    uint32 datasize = 0;
    uint64 offset = 0;
    uint64 fileoffset = 0;

    char buffer[FILE_BLK_SIZE];
    pg_sysdict_Form_pg_index index;
    catalog_index_hash_entry *entry = NULL;

    rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(uint32_t);
    hash_ctl.entrysize = sizeof(catalog_index_value);
    indexhtab = hash_create("catalog_index_value", 2048, &hash_ctl,
                                 HASH_ELEM | HASH_BLOBS);

    if (array[CATALOG_TYPE_INDEX - 1].len == array[CATALOG_TYPE_INDEX - 1].offset)
    {
        return indexhtab;
    }

    fd = osal_basic_open_file(SYSDICTS_FILE,
                        O_RDWR | BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", SYSDICTS_FILE);
    }

    fileoffset = array[CATALOG_TYPE_INDEX - 1].offset;
    while ((r = osal_file_pread(fd, buffer, FILE_BLK_SIZE, fileoffset)) > 0) 
    {
        rmemcpy1(&datasize, 0, buffer, PAGE_HEADER_SIZE);
        offset = PAGE_HEADER_SIZE;

        while (offset < datasize)
        {
            catalog_index_value *index_value = NULL;

            index = (pg_sysdict_Form_pg_index)rmalloc0(sizeof(pg_parser_sysdict_pgindex));
            if(NULL == index)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }

            rmemset0(index, 0, '\0', sizeof(pg_parser_sysdict_pgindex));
            rmemcpy0(index, 0, buffer + offset, offsetof(pg_parser_sysdict_pgindex, indkey));
            offset += offsetof(pg_parser_sysdict_pgindex, indkey);

            index->indkey = rmalloc0(sizeof(uint32) * index->indnatts);
            if(NULL == index->indkey)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }

            rmemset0(index->indkey, 0, '\0', sizeof(uint32) * index->indnatts);
            rmemcpy0(index->indkey, 0, buffer + offset, sizeof(uint32) * index->indnatts);
            offset += sizeof(uint32) * index->indnatts;

            index_value = rmalloc0(sizeof(catalog_index_value));
            if(NULL == index_value)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(index_value, 0, '\0', sizeof(catalog_index_value));
            index_value->oid = index->indrelid;
            index_value->index = index;

            entry = hash_search(indexhtab, &index->indrelid, HASH_ENTER, &found);
            if(!found)
            {
                entry->oid = index->indrelid;
                entry->index_list = NULL;
            }
            entry->index_list = lappend(entry->index_list, index_value);

            if (fileoffset + offset == array[CATALOG_TYPE_INDEX - 1].len)
            {
                if(osal_file_close(fd))
                {
                    elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
                }
                return indexhtab;
            }
        }
        fileoffset += FILE_BLK_SIZE;
    }

    if(osal_file_close(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
    }
    return indexhtab;
}
