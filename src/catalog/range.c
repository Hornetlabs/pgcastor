#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/guc/guc.h"
#include "utils/hash/hash_utils.h"
#include "utils/conn/ripple_conn.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_transcache.h"
#include "catalog/ripple_catalog.h"
#include "catalog/ripple_range.h"

void ripple_range_getfromdb(PGconn *conn, ripple_cache_sysdicts* sysdicts)
{
	int i, j;
	HASHCTL hash_ctl;
	bool found = false;
	PGresult *res  = NULL;
	ripple_catalog_range_value *entry = NULL;
	xk_pg_sysdict_Form_pg_range ripple_range = NULL;
	const char *query = "SELECT rel.rngtypid,rel.rngsubtype FROM pg_range rel;";

	rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(ripple_catalog_range_value);
	sysdicts->by_range = hash_create("ripple_catalog_sysdicts_range", 2048, &hash_ctl,
										HASH_ELEM | HASH_BLOBS);

	res = ripple_conn_exec(conn, query);
	if (NULL == res)
	{
		elog(RLOG_ERROR, "pg_range query failed");
	}

	// 打印行数据
	for (i = 0; i < PQntuples(res); i++) 
	{
		ripple_range = (xk_pg_sysdict_Form_pg_range)rmalloc0(sizeof(xk_pg_parser_sysdict_pgrange));
		if(NULL == ripple_range)
		{
			elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
		}
		rmemset0(ripple_range, 0, '\0', sizeof(xk_pg_parser_sysdict_pgrange));
		j=0;
		sscanf(PQgetvalue(res, i, j++), "%u", &ripple_range->rngtypid);
		sscanf(PQgetvalue(res, i, j++), "%u", &ripple_range->rngsubtype);

		entry = hash_search(sysdicts->by_range, &ripple_range->rngtypid, HASH_ENTER, &found);
		if(found)
		{
			elog(RLOG_ERROR, "range_oid:%u already exist in by_range", entry->ripple_range->rngtypid);
		}
		entry->rngtypid = ripple_range->rngtypid;
		entry->ripple_range = ripple_range;
	}
	PQclear(res);
	return;
}

void ripple_rangedata_write(List* ripple_range, uint64 *offset, ripple_sysdict_header_array* array)
{
	int	 fd;
	uint64 page_num = 0;
	uint64 page_offset = 0;
	ListCell*	cell = NULL;
	char buffer[RIPPLE_FILE_BLK_SIZE];
	xk_pg_sysdict_Form_pg_range riplerange;
	
	array->type = RIPPLE_CATALOG_TYPE_RANGE;
	array->offset = *offset;
	page_num = *offset;

	rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);
	fd = BasicOpenFile(RIPPLE_SYSDICTS_FILE,
						O_RDWR | O_CREAT | RIPPLE_BINARY);

	if (fd < 0)
	{
		elog(RLOG_ERROR, "could not create file %s", RIPPLE_SYSDICTS_FILE);
	}

	foreach(cell, ripple_range)
	{
		riplerange = (xk_pg_sysdict_Form_pg_range) lfirst(cell);
		if(page_offset + sizeof(xk_pg_parser_sysdict_pgrange) > RIPPLE_FILE_BLK_SIZE)
		{
			if (FilePWrite(fd, buffer, RIPPLE_FILE_BLK_SIZE, *offset) != RIPPLE_FILE_BLK_SIZE) {
				elog(RLOG_ERROR, "could not write to file %s", RIPPLE_SYSDICTS_FILE);
				FileClose(fd);
				return;
			}
			rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);
			page_num = *offset + page_offset;
			*offset += RIPPLE_FILE_BLK_SIZE;
			page_offset = 0;
		}
		rmemcpy1(buffer, page_offset, riplerange, sizeof(xk_pg_parser_sysdict_pgrange));
		page_offset += sizeof(xk_pg_parser_sysdict_pgrange);
	}

	if (page_offset > 0) {
		if (FilePWrite(fd, buffer, RIPPLE_FILE_BLK_SIZE, *offset) != RIPPLE_FILE_BLK_SIZE) {
			elog(RLOG_ERROR, "could not write to file %s", RIPPLE_SYSDICTS_FILE);
			FileClose(fd);
			return;
		}
		page_num = page_offset + *offset;
		*offset += RIPPLE_FILE_BLK_SIZE;
	}

	if(0 != FileSync(fd))
	{
		elog(RLOG_ERROR, "could not fsync file %s", RIPPLE_SYSDICTS_FILE);
	}

	if(FileClose(fd))
	{
		elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_FILE);
	}

	array->len = page_num;

}

HTAB* ripple_rangecache_load(ripple_sysdict_header_array* array)
{
    int r = 0;
    int fd = -1;
    HTAB* rangehtab;
    HASHCTL hash_ctl;
    bool found = false;
    uint64 fileoffset = 0;
    char buffer[RIPPLE_FILE_BLK_SIZE];
    xk_pg_sysdict_Form_pg_range range;
    ripple_catalog_range_value *entry = NULL;

    rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(uint32_t);
    hash_ctl.entrysize = sizeof(ripple_catalog_range_value);
    rangehtab = hash_create("ripple_catalog_range_value", 2048, &hash_ctl,
                                 HASH_ELEM | HASH_BLOBS);

    if (array[RIPPLE_CATALOG_TYPE_RANGE - 1].len == array[RIPPLE_CATALOG_TYPE_RANGE - 1].offset)
    {
        return rangehtab;
    }

    fd = BasicOpenFile(RIPPLE_SYSDICTS_FILE,
                        O_RDWR | RIPPLE_BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", RIPPLE_SYSDICTS_FILE);
    }

    fileoffset = array[RIPPLE_CATALOG_TYPE_RANGE - 1].offset;
    while ((r = FilePRead(fd, buffer, RIPPLE_FILE_BLK_SIZE, fileoffset)) > 0) 
    {
        uint64 offset = 0;

        while (offset + sizeof(xk_pg_parser_sysdict_pgrange) < RIPPLE_FILE_BLK_SIZE)
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
                elog(RLOG_ERROR, "range_oid:%u already exist in by_range", entry->ripple_range->rngtypid);
            }
            entry->rngtypid = range->rngtypid;
            entry->ripple_range = range;
            offset += sizeof(xk_pg_parser_sysdict_pgrange);
            if (fileoffset + offset == array[RIPPLE_CATALOG_TYPE_RANGE - 1].len)
            {
                if(FileClose(fd))
                {
                    elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_FILE);
                }
                return rangehtab;
            }
        }
        fileoffset += RIPPLE_FILE_BLK_SIZE;
    }

    if(FileClose(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_FILE);
    }

    return rangehtab;

}

/* colvalue2range */
ripple_catalogdata* ripple_range_colvalue2range(void* in_colvalue)
{
    ripple_catalogdata* catalogdata = NULL;
    ripple_catalog_range_value* rangevalue = NULL;
    xk_pg_sysdict_Form_pg_range pgrange = NULL;
    xk_pg_parser_translog_tbcol_value* colvalue = NULL;

    colvalue = (xk_pg_parser_translog_tbcol_value*)in_colvalue;

    /* 值转换 */
    catalogdata = (ripple_catalogdata*)rmalloc0(sizeof(ripple_catalogdata));
    if(NULL == catalogdata)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(catalogdata, 0, '\0', sizeof(ripple_catalogdata));

    rangevalue = (ripple_catalog_range_value*)rmalloc0(sizeof(ripple_catalog_range_value));
    if(NULL == rangevalue)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(rangevalue, 0, '\0', sizeof(ripple_catalog_range_value));
    catalogdata->catalog = rangevalue;
    catalogdata->type = RIPPLE_CATALOG_TYPE_RANGE;

    pgrange = (xk_pg_sysdict_Form_pg_range)rmalloc1(sizeof(xk_pg_parser_sysdict_pgrange));
    if(NULL == pgrange)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pgrange, 0, '\0', sizeof(xk_pg_parser_sysdict_pgrange));
    rangevalue->ripple_range = pgrange;

    /* rngsubtype 1 */
    sscanf((char*)((colvalue + 1)->m_value), "%u", &pgrange->rngsubtype);

    /* rngtypid 0 */
    sscanf((char*)((colvalue + 0)->m_value), "%u", &pgrange->rngtypid);
    rangevalue->rngtypid = pgrange->rngtypid;

    return catalogdata;
}


/* catalogdata2transcache */
void ripple_range_catalogdata2transcache(ripple_cache_sysdicts* sysdicts, ripple_catalogdata* catalogdata)
{
    bool found = false;
    ripple_catalog_range_value* newcatalog = NULL;
    ripple_catalog_range_value* catalogInHash = NULL;

    if(NULL == catalogdata || NULL == catalogdata->catalog)
    {
        return;
    }

    newcatalog = (ripple_catalog_range_value*)catalogdata->catalog;
    if(RIPPLE_CATALOG_OP_INSERT == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_range, &newcatalog->rngtypid, HASH_ENTER, &found);
        if(true == found)
        {
            elog(RLOG_WARNING, "by_range hash duplicate oid, %u, %u",
                                catalogInHash->ripple_range->rngtypid,
                                catalogInHash->ripple_range->rngsubtype);

            if(NULL != catalogInHash->ripple_range)
            {
                rfree(catalogInHash->ripple_range);
            }
        }
        catalogInHash->rngtypid = newcatalog->rngtypid;
		catalogInHash->ripple_range = (xk_pg_sysdict_Form_pg_range)rmalloc1(sizeof(xk_pg_parser_sysdict_pgrange));
		if(NULL == catalogInHash->ripple_range)
		{
			elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
		}
        rmemcpy0(catalogInHash->ripple_range, 0, newcatalog->ripple_range, sizeof(xk_pg_parser_sysdict_pgrange));
    }
    else if(RIPPLE_CATALOG_OP_DELETE == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_range, &newcatalog->rngtypid, HASH_REMOVE, &found);
        if(NULL != catalogInHash)
        {
            if(NULL != catalogInHash->ripple_range)
            {
                rfree(catalogInHash->ripple_range);
            }
        }
    }
    else if(RIPPLE_CATALOG_OP_UPDATE == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_range, &newcatalog->rngtypid, HASH_FIND, &found);
        if(NULL == catalogInHash)
        {
            elog(RLOG_WARNING, "by_range hash duplicate oid, %u, %u",
                                newcatalog->ripple_range->rngtypid,
                                newcatalog->ripple_range->rngsubtype);
			return;
        }
        rfree(catalogInHash->ripple_range);

		catalogInHash->ripple_range = (xk_pg_sysdict_Form_pg_range)rmalloc1(sizeof(xk_pg_parser_sysdict_pgrange));
		if(NULL == catalogInHash->ripple_range)
		{
			elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
		}
        rmemcpy0(catalogInHash->ripple_range, 0, newcatalog->ripple_range, sizeof(xk_pg_parser_sysdict_pgrange));
    }
    else
    {
        elog(RLOG_ERROR, "unknown op:%d", catalogdata->op);
    }
}


void ripple_rangecache_write(HTAB* rangecache, uint64 *offset, ripple_sysdict_header_array* array)
{
	int	 fd;
	uint64 page_num = 0;
	uint64 page_offset = 0;
	HASH_SEQ_STATUS status;
	char buffer[RIPPLE_FILE_BLK_SIZE];
	ripple_catalog_range_value *entry = NULL;
	xk_pg_sysdict_Form_pg_range riplerange = NULL;
	
	array[RIPPLE_CATALOG_TYPE_RANGE - 1].type = RIPPLE_CATALOG_TYPE_RANGE;
	array[RIPPLE_CATALOG_TYPE_RANGE - 1].offset = *offset;
	page_num = *offset;

	rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);
	fd = BasicOpenFile(RIPPLE_SYSDICTS_TMP_FILE,
						O_RDWR | O_CREAT | RIPPLE_BINARY);

	if (fd < 0)
	{
		elog(RLOG_ERROR, "could not create file %s", RIPPLE_SYSDICTS_TMP_FILE);
	}

	hash_seq_init(&status,rangecache);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		riplerange = entry->ripple_range;

		if(page_offset + sizeof(xk_pg_parser_sysdict_pgrange) > RIPPLE_FILE_BLK_SIZE)
		{
			if (FilePWrite(fd, buffer, RIPPLE_FILE_BLK_SIZE, *offset) != RIPPLE_FILE_BLK_SIZE) {
				elog(RLOG_ERROR, "could not write to file %s", RIPPLE_SYSDICTS_TMP_FILE);
				FileClose(fd);
				return;
			}
			rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);
			page_num = *offset + page_offset;
			*offset += RIPPLE_FILE_BLK_SIZE;
			page_offset = 0;
		}
		rmemcpy1(buffer, page_offset, riplerange, sizeof(xk_pg_parser_sysdict_pgrange));
		page_offset += sizeof(xk_pg_parser_sysdict_pgrange);
	}
	if (page_offset > 0) {
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

	array[RIPPLE_CATALOG_TYPE_RANGE - 1].len = page_num;

}

void ripple_range_catalogdatafree(ripple_catalogdata* catalogdata)
{
    ripple_catalog_range_value* catalog = NULL;
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
    catalog = (ripple_catalog_range_value*)catalogdata->catalog;
    if(NULL != catalog->ripple_range)
    {
        rfree(catalog->ripple_range);
    }
    rfree(catalogdata->catalog);
    rfree(catalogdata);
}
