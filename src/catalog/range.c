#include "app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/guc/guc.h"
#include "utils/hash/hash_utils.h"
#include "utils/conn/conn.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/cache_sysidcts.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/transcache.h"
#include "catalog/catalog.h"
#include "catalog/range.h"

void range_getfromdb(PGconn *conn, cache_sysdicts* sysdicts)
{
	int i, j;
	HASHCTL hash_ctl;
	bool found = false;
	PGresult *res  = NULL;
	catalog_range_value *entry = NULL;
	xk_pg_sysdict_Form_pg_range range = NULL;
	const char *query = "SELECT rel.rngtypid,rel.rngsubtype FROM pg_range rel;";

	rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(catalog_range_value);
	sysdicts->by_range = hash_create("catalog_sysdicts_range", 2048, &hash_ctl,
										HASH_ELEM | HASH_BLOBS);

	res = conn_exec(conn, query);
	if (NULL == res)
	{
		elog(RLOG_ERROR, "pg_range query failed");
	}

	// 打印行数据
	for (i = 0; i < PQntuples(res); i++) 
	{
		range = (xk_pg_sysdict_Form_pg_range)rmalloc0(sizeof(xk_pg_parser_sysdict_pgrange));
		if(NULL == range)
		{
			elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
		}
		rmemset0(range, 0, '\0', sizeof(xk_pg_parser_sysdict_pgrange));
		j=0;
		sscanf(PQgetvalue(res, i, j++), "%u", &range->rngtypid);
		sscanf(PQgetvalue(res, i, j++), "%u", &range->rngsubtype);

		entry = hash_search(sysdicts->by_range, &range->rngtypid, HASH_ENTER, &found);
		if(found)
		{
			elog(RLOG_ERROR, "range_oid:%u already exist in by_range", entry->range->rngtypid);
		}
		entry->rngtypid = range->rngtypid;
		entry->range = range;
	}
	PQclear(res);
	return;
}

void rangedata_write(List* range, uint64 *offset, sysdict_header_array* array)
{
	int	 fd;
	uint64 page_num = 0;
	uint64 page_offset = 0;
	ListCell*	cell = NULL;
	char buffer[FILE_BLK_SIZE];
	xk_pg_sysdict_Form_pg_range riplerange;
	
	array->type = CATALOG_TYPE_RANGE;
	array->offset = *offset;
	page_num = *offset;

	rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
	fd = osal_basic_open_file(SYSDICTS_FILE,
						O_RDWR | O_CREAT | BINARY);

	if (fd < 0)
	{
		elog(RLOG_ERROR, "could not create file %s", SYSDICTS_FILE);
	}

	foreach(cell, range)
	{
		riplerange = (xk_pg_sysdict_Form_pg_range) lfirst(cell);
		if(page_offset + sizeof(xk_pg_parser_sysdict_pgrange) > FILE_BLK_SIZE)
		{
			if (osal_file_pwrite(fd, buffer, FILE_BLK_SIZE, *offset) != FILE_BLK_SIZE) {
				elog(RLOG_ERROR, "could not write to file %s", SYSDICTS_FILE);
				osal_file_close(fd);
				return;
			}
			rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
			page_num = *offset + page_offset;
			*offset += FILE_BLK_SIZE;
			page_offset = 0;
		}
		rmemcpy1(buffer, page_offset, riplerange, sizeof(xk_pg_parser_sysdict_pgrange));
		page_offset += sizeof(xk_pg_parser_sysdict_pgrange);
	}

	if (page_offset > 0) {
		if (osal_file_pwrite(fd, buffer, FILE_BLK_SIZE, *offset) != FILE_BLK_SIZE) {
			elog(RLOG_ERROR, "could not write to file %s", SYSDICTS_FILE);
			osal_file_close(fd);
			return;
		}
		page_num = page_offset + *offset;
		*offset += FILE_BLK_SIZE;
	}

	if(0 != osal_file_sync(fd))
	{
		elog(RLOG_ERROR, "could not fsync file %s", SYSDICTS_FILE);
	}

	if(osal_file_close(fd))
	{
		elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
	}

	array->len = page_num;

}

HTAB* rangecache_load(sysdict_header_array* array)
{
    int r = 0;
    int fd = -1;
    HTAB* rangehtab;
    HASHCTL hash_ctl;
    bool found = false;
    uint64 fileoffset = 0;
    char buffer[FILE_BLK_SIZE];
    xk_pg_sysdict_Form_pg_range range;
    catalog_range_value *entry = NULL;

    rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(uint32_t);
    hash_ctl.entrysize = sizeof(catalog_range_value);
    rangehtab = hash_create("catalog_range_value", 2048, &hash_ctl,
                                 HASH_ELEM | HASH_BLOBS);

    if (array[CATALOG_TYPE_RANGE - 1].len == array[CATALOG_TYPE_RANGE - 1].offset)
    {
        return rangehtab;
    }

    fd = osal_basic_open_file(SYSDICTS_FILE,
                        O_RDWR | BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", SYSDICTS_FILE);
    }

    fileoffset = array[CATALOG_TYPE_RANGE - 1].offset;
    while ((r = osal_file_pread(fd, buffer, FILE_BLK_SIZE, fileoffset)) > 0) 
    {
        uint64 offset = 0;

        while (offset + sizeof(xk_pg_parser_sysdict_pgrange) < FILE_BLK_SIZE)
        {
            range = (xk_pg_sysdict_Form_pg_range)rmalloc1(sizeof(xk_pg_parser_sysdict_pgrange));
            if(NULL == range)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(range, 0, '\0', sizeof(xk_pg_parser_sysdict_pgrange));
            rmemcpy0(range, 0, buffer + offset, sizeof(xk_pg_parser_sysdict_pgrange));
            entry = hash_search(rangehtab, &range->rngtypid, HASH_ENTER, &found);
            if(found)
            {
                elog(RLOG_ERROR, "range_oid:%u already exist in by_range", entry->range->rngtypid);
            }
            entry->rngtypid = range->rngtypid;
            entry->range = range;
            offset += sizeof(xk_pg_parser_sysdict_pgrange);
            if (fileoffset + offset == array[CATALOG_TYPE_RANGE - 1].len)
            {
                if(osal_file_close(fd))
                {
                    elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
                }
                return rangehtab;
            }
        }
        fileoffset += FILE_BLK_SIZE;
    }

    if(osal_file_close(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
    }

    return rangehtab;

}

/* colvalue2range */
catalogdata* range_colvalue2range(void* in_colvalue)
{
    catalogdata* catalog_data = NULL;
    catalog_range_value* rangevalue = NULL;
    xk_pg_sysdict_Form_pg_range pgrange = NULL;
    xk_pg_parser_translog_tbcol_value* colvalue = NULL;

    colvalue = (xk_pg_parser_translog_tbcol_value*)in_colvalue;

    /* 值转换 */
    catalog_data = (catalogdata*)rmalloc0(sizeof(catalogdata));
    if(NULL == catalog_data)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(catalog_data, 0, '\0', sizeof(catalogdata));

    rangevalue = (catalog_range_value*)rmalloc0(sizeof(catalog_range_value));
    if(NULL == rangevalue)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(rangevalue, 0, '\0', sizeof(catalog_range_value));
    catalog_data->catalog = rangevalue;
    catalog_data->type = CATALOG_TYPE_RANGE;

    pgrange = (xk_pg_sysdict_Form_pg_range)rmalloc1(sizeof(xk_pg_parser_sysdict_pgrange));
    if(NULL == pgrange)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pgrange, 0, '\0', sizeof(xk_pg_parser_sysdict_pgrange));
    rangevalue->range = pgrange;

    /* rngsubtype 1 */
    sscanf((char*)((colvalue + 1)->m_value), "%u", &pgrange->rngsubtype);

    /* rngtypid 0 */
    sscanf((char*)((colvalue + 0)->m_value), "%u", &pgrange->rngtypid);
    rangevalue->rngtypid = pgrange->rngtypid;

    return catalog_data;
}


/* catalogdata2transcache */
void range_catalogdata2transcache(cache_sysdicts* sysdicts, catalogdata* catalogdata)
{
    bool found = false;
    catalog_range_value* newcatalog = NULL;
    catalog_range_value* catalogInHash = NULL;

    if(NULL == catalogdata || NULL == catalogdata->catalog)
    {
        return;
    }

    newcatalog = (catalog_range_value*)catalogdata->catalog;
    if(CATALOG_OP_INSERT == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_range, &newcatalog->rngtypid, HASH_ENTER, &found);
        if(true == found)
        {
            elog(RLOG_WARNING, "by_range hash duplicate oid, %u, %u",
                                catalogInHash->range->rngtypid,
                                catalogInHash->range->rngsubtype);

            if(NULL != catalogInHash->range)
            {
                rfree(catalogInHash->range);
            }
        }
        catalogInHash->rngtypid = newcatalog->rngtypid;
		catalogInHash->range = (xk_pg_sysdict_Form_pg_range)rmalloc1(sizeof(xk_pg_parser_sysdict_pgrange));
		if(NULL == catalogInHash->range)
		{
			elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
		}
        rmemcpy0(catalogInHash->range, 0, newcatalog->range, sizeof(xk_pg_parser_sysdict_pgrange));
    }
    else if(CATALOG_OP_DELETE == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_range, &newcatalog->rngtypid, HASH_REMOVE, &found);
        if(NULL != catalogInHash)
        {
            if(NULL != catalogInHash->range)
            {
                rfree(catalogInHash->range);
            }
        }
    }
    else if(CATALOG_OP_UPDATE == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_range, &newcatalog->rngtypid, HASH_FIND, &found);
        if(NULL == catalogInHash)
        {
            elog(RLOG_WARNING, "by_range hash duplicate oid, %u, %u",
                                newcatalog->range->rngtypid,
                                newcatalog->range->rngsubtype);
			return;
        }
        rfree(catalogInHash->range);

		catalogInHash->range = (xk_pg_sysdict_Form_pg_range)rmalloc1(sizeof(xk_pg_parser_sysdict_pgrange));
		if(NULL == catalogInHash->range)
		{
			elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
		}
        rmemcpy0(catalogInHash->range, 0, newcatalog->range, sizeof(xk_pg_parser_sysdict_pgrange));
    }
    else
    {
        elog(RLOG_ERROR, "unknown op:%d", catalogdata->op);
    }
}


void rangecache_write(HTAB* rangecache, uint64 *offset, sysdict_header_array* array)
{
	int	 fd;
	uint64 page_num = 0;
	uint64 page_offset = 0;
	HASH_SEQ_STATUS status;
	char buffer[FILE_BLK_SIZE];
	catalog_range_value *entry = NULL;
	xk_pg_sysdict_Form_pg_range riplerange = NULL;
	
	array[CATALOG_TYPE_RANGE - 1].type = CATALOG_TYPE_RANGE;
	array[CATALOG_TYPE_RANGE - 1].offset = *offset;
	page_num = *offset;

	rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
	fd = osal_basic_open_file(SYSDICTS_TMP_FILE,
						O_RDWR | O_CREAT | BINARY);

	if (fd < 0)
	{
		elog(RLOG_ERROR, "could not create file %s", SYSDICTS_TMP_FILE);
	}

	hash_seq_init(&status,rangecache);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		riplerange = entry->range;

		if(page_offset + sizeof(xk_pg_parser_sysdict_pgrange) > FILE_BLK_SIZE)
		{
			if (osal_file_pwrite(fd, buffer, FILE_BLK_SIZE, *offset) != FILE_BLK_SIZE) {
				elog(RLOG_ERROR, "could not write to file %s", SYSDICTS_TMP_FILE);
				osal_file_close(fd);
				return;
			}
			rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
			page_num = *offset + page_offset;
			*offset += FILE_BLK_SIZE;
			page_offset = 0;
		}
		rmemcpy1(buffer, page_offset, riplerange, sizeof(xk_pg_parser_sysdict_pgrange));
		page_offset += sizeof(xk_pg_parser_sysdict_pgrange);
	}
	if (page_offset > 0) {
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

	array[CATALOG_TYPE_RANGE - 1].len = page_num;

}

void range_catalogdatafree(catalogdata* catalogdata)
{
    catalog_range_value* catalog = NULL;
    if(NULL == catalogdata)
    {
        return;
    }

    if(NULL == catalogdata->catalog)
    {
        rfree(catalogdata);
        return;
    }

    /* catalog 内存释放 */
    catalog = (catalog_range_value*)catalogdata->catalog;
    if(NULL != catalog->range)
    {
        rfree(catalog->range);
    }
    rfree(catalogdata->catalog);
    rfree(catalogdata);
}
