#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/guc/guc.h"
#include "port/file/fd.h"
#include "utils/hash/hash_utils.h"
#include "utils/conn/ripple_conn.h"
#include "misc/ripple_misc_stat.h"
#include "misc/ripple_misc_control.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_transcache.h"
#include "catalog/ripple_catalog.h"
#include "catalog/ripple_index.h"

static void ripple_index_dict_index_free(xk_pg_sysdict_Form_pg_index index)
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

static uint32 *ripple_index_make_key_from_vector(char *vector_str, int32 natt)
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
void ripple_index_getfromdb(PGconn *conn, ripple_cache_sysdicts* sysdicts)
{
    int index_row = 0;
    int index_col = 0;
    int max_raw = 0;
    bool find = false;
    char sql_exec[RIPPLE_MAX_EXEC_SQL_LEN] = {'\0'};

    PGresult* res = NULL;

    xk_pg_sysdict_Form_pg_index catalog_index = NULL;
    ripple_catalog_index_value *catalog_index_value = NULL;

    HASHCTL index_hash_ctl = {'\0'};
    ripple_catalog_index_hash_entry *index_entry = NULL;

    /* hash初始化 */
    index_hash_ctl.keysize = sizeof(Oid);
    index_hash_ctl.entrysize = sizeof(ripple_catalog_index_hash_entry);
    sysdicts->by_index = hash_create("ripple_catalog_sysdict_index", 2048, &index_hash_ctl,
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

    res = ripple_conn_exec(conn, sql_exec);
    if (NULL == res)
    {
        elog(RLOG_ERROR, "pg_index query failed");
    }

    max_raw = PQntuples(res);
    /* 遍历结果 */
    for (index_row = 0; index_row < max_raw; index_row++)
    {
        index_col = 0;

        catalog_index = (xk_pg_sysdict_Form_pg_index)rmalloc0(sizeof(xk_pg_parser_sysdict_pgindex));
        if(NULL == catalog_index)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(catalog_index, 0, '\0', sizeof(xk_pg_parser_sysdict_pgindex));

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
        catalog_index->indkey = ripple_index_make_key_from_vector(PQgetvalue(res, index_row, index_col++), catalog_index->indnatts);

        catalog_index_value = (ripple_catalog_index_value *)rmalloc0(sizeof(ripple_catalog_index_value));
        if(NULL == catalog_index_value)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(catalog_index_value, 0, '\0', sizeof(ripple_catalog_index_value));

        catalog_index_value->oid = catalog_index->indrelid;
        catalog_index_value->ripple_index = catalog_index;

        /* 赋值结束, 根据indrelid查找哈希 */
        index_entry = hash_search(sysdicts->by_index, &catalog_index->indrelid, HASH_ENTER, &find);
        if (!find)
        {
            /* 第一次需要初始化 */
            index_entry->oid = catalog_index->indrelid;
            index_entry->ripple_index_list = NULL;
        }
        index_entry->ripple_index_list = lappend(index_entry->ripple_index_list, catalog_index_value);
    }

    /* 结束, 清理 */
    PQclear(res);
}

/* colvalue2index */
ripple_catalogdata* ripple_index_colvalue2index(void* in_colvalue)
{
    ripple_catalogdata* catalogdata = NULL;
    ripple_catalog_index_value* index_value = NULL;
    xk_pg_sysdict_Form_pg_index pgindex = NULL;
    xk_pg_parser_translog_tbcol_value* colvalue = NULL;
    uint32_t temp_oid = InvalidOid;
    bool unique = false;
    char *temp_key = NULL;

    colvalue = (xk_pg_parser_translog_tbcol_value*)in_colvalue;

    /* 首先检查 indrelid 和 indisunique */
    temp_oid = (uint32)atoi((char*)((colvalue + 1)->m_value));
    unique = ((char*)((colvalue + 4)->m_value))[0] == 't' ? true : false;

    if ((temp_oid < 16384) || !unique)
    {
        return NULL;
    }

    /* 值转换 */
    catalogdata = (ripple_catalogdata*)rmalloc1(sizeof(ripple_catalogdata));
    if(NULL == catalogdata)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(catalogdata, 0, '\0', sizeof(ripple_catalogdata));

    index_value = (ripple_catalog_index_value*)rmalloc1(sizeof(ripple_catalog_index_value));
    if(NULL == index_value)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(index_value, 0, '\0', sizeof(ripple_catalog_index_value));

    catalogdata->catalog = index_value;
    catalogdata->type = RIPPLE_CATALOG_TYPE_INDEX;

    pgindex = (xk_pg_sysdict_Form_pg_index)rmalloc1(sizeof(xk_pg_parser_sysdict_pgindex));
    if(NULL == pgindex)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pgindex, 0, '\0', sizeof(xk_pg_parser_sysdict_pgindex));
    index_value->ripple_index = pgindex;

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
    pgindex->indkey = ripple_index_make_key_from_vector(temp_key, pgindex->indnatts);

    return catalogdata;
}

static bool check_index_catalog_data_in_list(List *index_list, ripple_catalog_index_value* new_index_value)
{
    ListCell *cell = NULL;
    bool find = false;

    if (!index_list)
    {
        return find;
    }

    foreach(cell, index_list)
    {
        ripple_catalog_index_value* index_value = (ripple_catalog_index_value*)lfirst(cell);
        xk_pg_sysdict_Form_pg_index dict_index = index_value->ripple_index;
        if (dict_index->indexrelid == new_index_value->ripple_index->indexrelid)
        {
            find = true;
            break;
        }
    }
    return find;
}

static bool update_index_catalog_data_in_lsit(List *index_list, ripple_catalog_index_value* new_index_value)
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
        ripple_catalog_index_value* index_value = (ripple_catalog_index_value*)lfirst(cell);
        xk_pg_sysdict_Form_pg_index dict_index = index_value->ripple_index;

        /* 查找指定的index记录 */
        if (dict_index->indexrelid == new_index_value->ripple_index->indexrelid)
        {
            uint32 old_natt = dict_index->indnatts;

            /* 判断键值是否变更 */
            if (old_natt != new_index_value->ripple_index->indnatts)
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
                rmemcpy0(dict_index->indkey, 0, new_index_value->ripple_index->indkey, sizeof(uint32) * new_index_value->ripple_index->indnatts);
            }

            /* 重新赋值 */
            rmemcpy0(dict_index, 0, new_index_value->ripple_index, offsetof(xk_pg_parser_sysdict_pgindex, indkey));
            find = true;
            break;
        }
        cell = next_cell;
    }
    return find;
}

/* catalogdata2transcache */
void ripple_index_catalogdata2transcache(ripple_cache_sysdicts* sysdicts, ripple_catalogdata* catalogdata)
{
    bool found = false;
    ripple_catalog_index_value* new_index_value = NULL;
    ripple_catalog_index_hash_entry* indexInHash = NULL;

    if(NULL == catalogdata || NULL == catalogdata->catalog)
    {
        return;
    }

    new_index_value = (ripple_catalog_index_value*)catalogdata->catalog;
    elog(RLOG_DEBUG, "op:%d, newindex, indrelid:%u, primary:%s, isreplident:%s, keynum:%d",
                    catalogdata->op,
                    new_index_value->ripple_index->indexrelid,
                    new_index_value->ripple_index->indisprimary ? "true" : "false",
                    new_index_value->ripple_index->indisreplident ? "true" : "false",
                    new_index_value->ripple_index->indnatts);

    if(RIPPLE_CATALOG_OP_INSERT == catalogdata->op)
    {
        ripple_catalog_index_value* index_value = NULL;
        xk_pg_sysdict_Form_pg_index dict_index = NULL;

        indexInHash = hash_search(sysdicts->by_index, &new_index_value->oid, HASH_ENTER, &found);
        if(false == found)
        {
            indexInHash->ripple_index_list = NULL;
        }
        indexInHash->oid = new_index_value->oid;

        if (!check_index_catalog_data_in_list(indexInHash->ripple_index_list, new_index_value))
        {
            /* 申请空间，并赋值 */
            dict_index = (xk_pg_sysdict_Form_pg_index)rmalloc0(sizeof(xk_pg_parser_sysdict_pgindex));
            if(NULL == dict_index)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemcpy0(dict_index, 0, new_index_value->ripple_index, sizeof(xk_pg_parser_sysdict_pgindex));

            dict_index->indkey = rmalloc0(sizeof(uint32) * dict_index->indnatts);
            if(NULL == dict_index->indkey)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemcpy0(dict_index->indkey, 0, new_index_value->ripple_index->indkey, sizeof(uint32) * dict_index->indnatts);

            index_value = (ripple_catalog_index_value *)rmalloc0(sizeof(ripple_catalog_index_value));
            if(NULL == index_value)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(index_value, 0, 0, sizeof(ripple_catalog_index_value));
            index_value->oid = new_index_value->oid;
            index_value->ripple_index = dict_index;

            /* 添加到链表中 */
            indexInHash->ripple_index_list = lappend(indexInHash->ripple_index_list, index_value);
        }
        else
        {
            elog(RLOG_DEBUG, "index r:%u,i:%u find in index hash list, do update",
                                new_index_value->ripple_index->indrelid, new_index_value->ripple_index->indexrelid);
            update_index_catalog_data_in_lsit(indexInHash->ripple_index_list, new_index_value);
        }
    }
    else if(RIPPLE_CATALOG_OP_DELETE == catalogdata->op)
    {
        indexInHash = hash_search(sysdicts->by_index, &new_index_value->oid, HASH_FIND, NULL);

        if(NULL != indexInHash)
        {
            if(NULL != indexInHash->ripple_index_list)
            {
                ListCell *cell = list_head(indexInHash->ripple_index_list);
                ListCell *cell_prev = NULL;
                /* 遍历链表, 查找记录 */
                while (cell)
                {
                    ListCell *next_cell = cell->next;
                    ripple_catalog_index_value* index_value = (ripple_catalog_index_value*)lfirst(cell);
                    xk_pg_sysdict_Form_pg_index dict_index = index_value->ripple_index;

                    /* 查找指定的index记录 */
                    if (dict_index->indexrelid == new_index_value->ripple_index->indexrelid)
                    {
                        ripple_index_dict_index_free(dict_index);
                        rfree(index_value);
                        indexInHash->ripple_index_list = list_delete_cell(indexInHash->ripple_index_list, cell, cell_prev);
                        break;
                    }
                    else
                    {
                        cell_prev = cell;
                    }

                    cell = next_cell;
                }

                /* 检查链表是否为空, 为空时从哈希中删除这条记录 */
                if (!indexInHash->ripple_index_list)
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
    else if(RIPPLE_CATALOG_OP_UPDATE == catalogdata->op)
    {
        indexInHash = hash_search(sysdicts->by_index, &new_index_value->oid, HASH_FIND, &found);
        if(NULL == indexInHash || NULL == (indexInHash->ripple_index_list))
        {
            elog(RLOG_WARNING, "index r:%u,i:%u can not fond in index hash",
                                new_index_value->ripple_index->indrelid, new_index_value->ripple_index->indexrelid);
            return;
        }

        if (!update_index_catalog_data_in_lsit(indexInHash->ripple_index_list, new_index_value))
        {
            elog(RLOG_WARNING, "index r:%u,i:%u can not fond in index hash list",
                                new_index_value->ripple_index->indrelid, new_index_value->ripple_index->indexrelid);
            return;
        }
    }
    else
    {
        elog(RLOG_ERROR, "unknown op:%d", catalogdata->op);
    }
}

void ripple_index_catalogdatafree(ripple_catalogdata* catalogdata)
{
    ripple_catalog_index_value* catalog = NULL;
    if(NULL == catalogdata)
    {
        return;
    }

    /* catalog 内存释放 */
    if(NULL != catalogdata->catalog)
    {
        catalog = (ripple_catalog_index_value*)catalogdata->catalog;
        ripple_index_dict_index_free(catalog->ripple_index);
        rfree(catalogdata->catalog);
    }
    rfree(catalogdata);
}

/* 根据oid获取pg_index链表 */
void* ripple_index_getbyoid(Oid oid, HTAB* by_index)
{
    bool found = false;
    ripple_catalog_index_hash_entry *indexentry = NULL;
    indexentry = hash_search(by_index, &oid, HASH_FIND, &found);
    if(false == found)
    {
        return NULL;
    }

    /* need free */
    return (void*)list_copy(indexentry->ripple_index_list);
}

void ripple_indexcache_write(HTAB* indexcache, uint64 *offset, ripple_sysdict_header_array* array)
{
    int     fd;
    uint64 page_num = 0;
    uint64 page_offset = RIPPLE_PAGE_HEADER_SIZE;
    HASH_SEQ_STATUS status;
    char buffer[RIPPLE_FILE_BLK_SIZE];
    xk_pg_sysdict_Form_pg_index index = NULL;
    ripple_catalog_index_hash_entry *index_entry = NULL;
    ripple_catalog_index_value *index_value = NULL;

    array[RIPPLE_CATALOG_TYPE_INDEX - 1].type = RIPPLE_CATALOG_TYPE_INDEX;
    array[RIPPLE_CATALOG_TYPE_INDEX - 1].offset = *offset;
    page_num = *offset;

    rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);
    fd = BasicOpenFile(RIPPLE_SYSDICTS_TMP_FILE,
                        O_RDWR | O_CREAT | RIPPLE_BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not create file %s", RIPPLE_SYSDICTS_TMP_FILE);
    }

    hash_seq_init(&status, indexcache);
    while ((index_entry = hash_seq_search(&status)) != NULL)
    {
        ListCell *cell = NULL;

        foreach(cell, index_entry->ripple_index_list)
        {
            uint32 fix_len = 0;
            uint32 varlena_len = 0;

            index_value = (ripple_catalog_index_value*)lfirst(cell);
            index = index_value->ripple_index;

            fix_len = offsetof(xk_pg_parser_sysdict_pgindex, indkey);
            varlena_len = sizeof(uint32) * index->indnatts;

            if(page_offset + fix_len + varlena_len > RIPPLE_FILE_BLK_SIZE)
            {
                rmemcpy1(buffer, 0, &page_offset, RIPPLE_PAGE_HEADER_SIZE);
                if (FilePWrite(fd, buffer, RIPPLE_FILE_BLK_SIZE, *offset) != RIPPLE_FILE_BLK_SIZE) {
                    elog(RLOG_ERROR, "could not write to file %s", RIPPLE_SYSDICTS_TMP_FILE);
                    FileClose(fd);
                    return;
                }
                rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);
                page_num = *offset + page_offset;
                *offset += RIPPLE_FILE_BLK_SIZE;
                page_offset = RIPPLE_PAGE_HEADER_SIZE;
            }
            rmemcpy1(buffer, page_offset, index, fix_len);
            page_offset += fix_len;

            rmemcpy1(buffer, page_offset, index->indkey, varlena_len);
            page_offset += varlena_len;
        }
    }

    if (page_offset > RIPPLE_PAGE_HEADER_SIZE)
    {
        rmemcpy1(buffer, 0, &page_offset, RIPPLE_PAGE_HEADER_SIZE);

        if (FilePWrite(fd, buffer, RIPPLE_FILE_BLK_SIZE, *offset) != RIPPLE_FILE_BLK_SIZE) {
            elog(RLOG_ERROR, "could not write to file %s", RIPPLE_SYSDICTS_TMP_FILE);
            FileClose(fd);
            return;
        }
        page_num = page_offset + *offset;
        *offset += RIPPLE_FILE_BLK_SIZE;
    }

    if(0 != FileSync(fd))
    {
        elog(RLOG_ERROR, "could not fsync file %s", RIPPLE_SYSDICTS_TMP_FILE);
    }

    if(FileClose(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_TMP_FILE);
    }

    array[RIPPLE_CATALOG_TYPE_INDEX - 1].len = page_num;

    return;
}

HTAB* ripple_indexcache_load(ripple_sysdict_header_array* array)
{
    int r = 0;
    int fd = -1;
    HTAB* indexhtab;
    HASHCTL hash_ctl;
    bool found = false;
    uint32 datasize = 0;
    uint64 offset = 0;
    uint64 fileoffset = 0;

    char buffer[RIPPLE_FILE_BLK_SIZE];
    xk_pg_sysdict_Form_pg_index index;
    ripple_catalog_index_hash_entry *entry = NULL;

    rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(uint32_t);
    hash_ctl.entrysize = sizeof(ripple_catalog_index_value);
    indexhtab = hash_create("ripple_catalog_index_value", 2048, &hash_ctl,
                                 HASH_ELEM | HASH_BLOBS);

    if (array[RIPPLE_CATALOG_TYPE_INDEX - 1].len == array[RIPPLE_CATALOG_TYPE_INDEX - 1].offset)
    {
        return indexhtab;
    }

    fd = BasicOpenFile(RIPPLE_SYSDICTS_FILE,
                        O_RDWR | RIPPLE_BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", RIPPLE_SYSDICTS_FILE);
    }

    fileoffset = array[RIPPLE_CATALOG_TYPE_INDEX - 1].offset;
    while ((r = FilePRead(fd, buffer, RIPPLE_FILE_BLK_SIZE, fileoffset)) > 0) 
    {
        rmemcpy1(&datasize, 0, buffer, RIPPLE_PAGE_HEADER_SIZE);
        offset = RIPPLE_PAGE_HEADER_SIZE;

        while (offset < datasize)
        {
            ripple_catalog_index_value *index_value = NULL;

            index = (xk_pg_sysdict_Form_pg_index)rmalloc0(sizeof(xk_pg_parser_sysdict_pgindex));
            if(NULL == index)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }

            rmemset0(index, 0, '\0', sizeof(xk_pg_parser_sysdict_pgindex));
            rmemcpy0(index, 0, buffer + offset, offsetof(xk_pg_parser_sysdict_pgindex, indkey));
            offset += offsetof(xk_pg_parser_sysdict_pgindex, indkey);

            index->indkey = rmalloc0(sizeof(uint32) * index->indnatts);
            if(NULL == index->indkey)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }

            rmemset0(index->indkey, 0, '\0', sizeof(uint32) * index->indnatts);
            rmemcpy0(index->indkey, 0, buffer + offset, sizeof(uint32) * index->indnatts);
            offset += sizeof(uint32) * index->indnatts;

            index_value = rmalloc0(sizeof(ripple_catalog_index_value));
            if(NULL == index_value)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(index_value, 0, '\0', sizeof(ripple_catalog_index_value));
            index_value->oid = index->indrelid;
            index_value->ripple_index = index;

            entry = hash_search(indexhtab, &index->indrelid, HASH_ENTER, &found);
            if(!found)
            {
                entry->oid = index->indrelid;
                entry->ripple_index_list = NULL;
            }
            entry->ripple_index_list = lappend(entry->ripple_index_list, index_value);

            if (fileoffset + offset == array[RIPPLE_CATALOG_TYPE_INDEX - 1].len)
            {
                if(FileClose(fd))
                {
                    elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_FILE);
                }
                return indexhtab;
            }
        }
        fileoffset += RIPPLE_FILE_BLK_SIZE;
    }

    if(FileClose(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_FILE);
    }
    return indexhtab;
}
