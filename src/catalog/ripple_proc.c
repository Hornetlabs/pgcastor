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
#include "catalog/ripple_proc.h"


void ripple_proc_getfromdb(PGconn *conn, ripple_cache_sysdicts* sysdicts)
{
	int	 i, j;
	HASHCTL hash_ctl;
	bool found = false;
	PGresult *res  = NULL;
	ripple_catalog_proc_value *entry = NULL;
	xk_pg_sysdict_Form_pg_proc ripple_proc = NULL;
	const char *query = "SELECT rel.oid,rel.proname, rel.pronamespace,rel.pronargs, rel.proargtypes FROM pg_proc rel;";
	
	rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(ripple_catalog_proc_value);
	sysdicts->by_proc = hash_create("ripple_catalog_sysdicts_proc", 2048, &hash_ctl,
								 HASH_ELEM | HASH_BLOBS);

	res = ripple_conn_exec(conn, query);
	if (NULL == res)
	{
		elog(RLOG_ERROR, "pg_proc query failed");
	}


	// 打印行数据
	for (i = 0; i < PQntuples(res); i++) 
	{
		ripple_proc = (xk_pg_sysdict_Form_pg_proc)rmalloc0(sizeof(xk_pg_parser_sysdict_pgproc));
		if(NULL == ripple_proc)
		{
			elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
		}
		rmemset0(ripple_proc, 0, '\0', sizeof(xk_pg_parser_sysdict_pgproc));
		j=0;
		sscanf(PQgetvalue(res, i, j++), "%u", &ripple_proc->oid);
		strcpy(ripple_proc->proname.data ,PQgetvalue(res, i, j++));
		sscanf(PQgetvalue(res, i, j++), "%u", &ripple_proc->pronamespace);
		sscanf(PQgetvalue(res, i, j++), "%hd", &ripple_proc->pronargs);

		entry = hash_search(sysdicts->by_proc, &ripple_proc->oid, HASH_ENTER, &found);
		if(found)
		{
			elog(RLOG_ERROR, "proc_oid:%u already exist in by_proc", entry->ripple_proc->oid);
		}
		entry->oid = ripple_proc->oid;
		entry->ripple_proc = ripple_proc;
	}
	PQclear(res);
	return;
}

void ripple_procdata_write(List* ripple_proc, uint64 *offset, ripple_sysdict_header_array* array)
{
	int	 fd;
	uint64 page_num = 0;
	uint64 page_offset = 0;
	ListCell*	cell = NULL;
	char buffer[RIPPLE_FILE_BLK_SIZE];
	xk_pg_sysdict_Form_pg_proc proc;

	array->type = RIPPLE_CATALOG_TYPE_PROC;
	array->offset = *offset;
	page_num = *offset;

	rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);
	fd = BasicOpenFile(RIPPLE_SYSDICTS_FILE,
						O_RDWR | O_CREAT | RIPPLE_BINARY);

	if (fd < 0)
	{
		elog(RLOG_ERROR, "could not create file %s", RIPPLE_SYSDICTS_FILE);
	}

	foreach(cell, ripple_proc)
	{
		proc = (xk_pg_sysdict_Form_pg_proc) lfirst(cell);
		if(page_offset + sizeof(xk_pg_parser_sysdict_pgproc) > RIPPLE_FILE_BLK_SIZE)
		{
			if (FilePWrite(fd, buffer, RIPPLE_FILE_BLK_SIZE, *offset) != RIPPLE_FILE_BLK_SIZE) {
				elog(RLOG_ERROR, "could not write to file %s", RIPPLE_SYSDICTS_FILE);
				close(fd);
				return;
			}
			rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);
			page_num = *offset + page_offset;
			*offset += RIPPLE_FILE_BLK_SIZE;
			page_offset = 0;
		}
		rmemcpy1(buffer, page_offset, proc, sizeof(xk_pg_parser_sysdict_pgproc));
		page_offset += sizeof(xk_pg_parser_sysdict_pgproc);
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
		elog(RLOG_ERROR, "could not FileClose file %s", RIPPLE_SYSDICTS_FILE);
	}

	array->len = page_num;
}

HTAB* ripple_proccache_load(ripple_sysdict_header_array* array)
{
    int r = 0;
    int fd = -1;
    HTAB* prochtab;
    HASHCTL hash_ctl;
    bool found = false;
    uint64 fileoffset = 0;
    char buffer[RIPPLE_FILE_BLK_SIZE];
    xk_pg_sysdict_Form_pg_proc proc;
    ripple_catalog_proc_value *entry = NULL;

    rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(uint32_t);
    hash_ctl.entrysize = sizeof(ripple_catalog_proc_value);
    prochtab = hash_create("ripple_catalog_proc_value", 2048, &hash_ctl,
                                 HASH_ELEM | HASH_BLOBS);

    if (array[RIPPLE_CATALOG_TYPE_PROC - 1].len == array[RIPPLE_CATALOG_TYPE_PROC - 1].offset)
    {
        return prochtab;
    }

    fd = BasicOpenFile(RIPPLE_SYSDICTS_FILE,
                        O_RDWR | RIPPLE_BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", RIPPLE_SYSDICTS_FILE);
    }

    fileoffset = array[RIPPLE_CATALOG_TYPE_PROC - 1].offset;
    while ((r = FilePRead(fd, buffer, RIPPLE_FILE_BLK_SIZE, fileoffset)) > 0) 
    {
        uint64 offset = 0;

        while (offset + sizeof(xk_pg_parser_sysdict_pgproc) < RIPPLE_FILE_BLK_SIZE)
        {
            proc = (xk_pg_sysdict_Form_pg_proc)rmalloc1(sizeof(xk_pg_parser_sysdict_pgproc));
            if(NULL == proc)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(proc, 0, '\0', sizeof(xk_pg_parser_sysdict_pgproc));
            rmemcpy0(proc, 0, buffer + offset, sizeof(xk_pg_parser_sysdict_pgproc));
            entry = hash_search(prochtab, &proc->oid, HASH_ENTER, &found);
            if(found)
            {
                elog(RLOG_ERROR, "proc_oid:%u already exist in by_proc", entry->ripple_proc->oid);
            }
            entry->oid = proc->oid;
            entry->ripple_proc = proc;
            offset += sizeof(xk_pg_parser_sysdict_pgproc);
            if (fileoffset + offset == array[RIPPLE_CATALOG_TYPE_PROC - 1].len)
            {
                if(FileClose(fd))
                {
                    elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_FILE);
                }
                return prochtab;
            }
        }
        fileoffset += RIPPLE_FILE_BLK_SIZE;
    }

    if(FileClose(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_FILE);
    }

    return prochtab;
}

/* colvalue2proc */
ripple_catalogdata* ripple_proc_colvalue2proc(void* in_colvalue)
{
    ripple_catalogdata* catalogdata = NULL;
    ripple_catalog_proc_value* procvalue = NULL;
    xk_pg_sysdict_Form_pg_proc pgproc = NULL;
    xk_pg_parser_translog_tbcol_value* colvalue = NULL;

    colvalue = (xk_pg_parser_translog_tbcol_value*)in_colvalue;

    /* 值转换 */
    catalogdata = (ripple_catalogdata*)rmalloc0(sizeof(ripple_catalogdata));
    if(NULL == catalogdata)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(catalogdata, 0, '\0', sizeof(ripple_catalogdata));

    procvalue = (ripple_catalog_proc_value*)rmalloc0(sizeof(ripple_catalog_proc_value));
    if(NULL == procvalue)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(procvalue, 0, '\0', sizeof(ripple_catalog_proc_value));
    catalogdata->catalog = procvalue;
    catalogdata->type = RIPPLE_CATALOG_TYPE_PROC;

    pgproc = (xk_pg_sysdict_Form_pg_proc)rmalloc1(sizeof(xk_pg_parser_sysdict_pgproc));
    if(NULL == pgproc)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pgproc, 0, '\0', sizeof(xk_pg_parser_sysdict_pgproc));
    procvalue->ripple_proc = pgproc;

    /* oid 0 */
    sscanf((char*)((colvalue + 0)->m_value), "%u", &pgproc->oid);
    procvalue->oid = pgproc->oid;

    /* proname 1 */
    rmemcpy1(pgproc->proname.data, 0, (char*)((colvalue + 1)->m_value), (colvalue + 1)->m_valueLen);

    /* pronamespace 2 */
    sscanf((char*)((colvalue + 2)->m_value), "%u", &pgproc->pronamespace);

    /* pronargs 16 */
    sscanf((char*)((colvalue + 16)->m_value), "%hd", &pgproc->pronargs);

    return catalogdata;
}

/* catalogdata2transcache */
void ripple_proc_catalogdata2transcache(ripple_cache_sysdicts* sysdicts, ripple_catalogdata* catalogdata)
{
    bool found = false;
    ripple_catalog_proc_value* newcatalog = NULL;
    ripple_catalog_proc_value* catalogInHash = NULL;

    if(NULL == catalogdata || NULL == catalogdata->catalog)
    {
        return;
    }

    newcatalog = (ripple_catalog_proc_value*)catalogdata->catalog;
    if(RIPPLE_CATALOG_OP_INSERT == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_proc, &newcatalog->oid, HASH_ENTER, &found);
        if(true == found)
        {
            elog(RLOG_WARNING, "by_proc hash duplicate oid, %u, %s",
                                catalogInHash->ripple_proc->oid,
                                catalogInHash->ripple_proc->proname.data);

            if(NULL != catalogInHash->ripple_proc)
            {
                rfree(catalogInHash->ripple_proc);
            }
        }
        catalogInHash->oid = newcatalog->oid;

		catalogInHash->ripple_proc = (xk_pg_sysdict_Form_pg_proc)rmalloc1(sizeof(xk_pg_parser_sysdict_pgproc));
		if(NULL == catalogInHash->ripple_proc)
		{
			elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
		}
		rmemcpy0(catalogInHash->ripple_proc, 0, newcatalog->ripple_proc, sizeof(xk_pg_parser_sysdict_pgproc));
    }
    else if(RIPPLE_CATALOG_OP_DELETE == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_proc, &newcatalog->oid, HASH_REMOVE, &found);
        if(NULL != catalogInHash)
        {
            if(NULL != catalogInHash->ripple_proc)
            {
                rfree(catalogInHash->ripple_proc);
            }
        }
    }
    else if(RIPPLE_CATALOG_OP_UPDATE == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_proc, &newcatalog->oid, HASH_FIND, &found);
        if(NULL == catalogInHash)
        {
            elog(RLOG_WARNING, "by_proc hash not found, %u, %s",
                                newcatalog->ripple_proc->oid,
                                newcatalog->ripple_proc->proname.data);
			return;
        }
        rfree(catalogInHash->ripple_proc);

		catalogInHash->ripple_proc = (xk_pg_sysdict_Form_pg_proc)rmalloc1(sizeof(xk_pg_parser_sysdict_pgproc));
		if(NULL == catalogInHash->ripple_proc)
		{
			elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
		}
		rmemcpy0(catalogInHash->ripple_proc, 0, newcatalog->ripple_proc, sizeof(xk_pg_parser_sysdict_pgproc));
    }
    else
    {
        elog(RLOG_ERROR, "unknown op:%d", catalogdata->op);
    }
}

void ripple_proccache_write(HTAB* proccache, uint64 *offset, ripple_sysdict_header_array* array)
{
	int	 fd;
	uint64 page_num = 0;
	uint64 page_offset = 0;
	HASH_SEQ_STATUS status;
	char buffer[RIPPLE_FILE_BLK_SIZE];
	ripple_catalog_proc_value *entry = NULL;
	xk_pg_sysdict_Form_pg_proc proc = NULL;

	array[RIPPLE_CATALOG_TYPE_PROC - 1].type = RIPPLE_CATALOG_TYPE_PROC;
	array[RIPPLE_CATALOG_TYPE_PROC - 1].offset = *offset;
	page_num = *offset;

	rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);
	fd = BasicOpenFile(RIPPLE_SYSDICTS_TMP_FILE,
						O_RDWR | O_CREAT | RIPPLE_BINARY);

	if (fd < 0)
	{
		elog(RLOG_ERROR, "could not create file %s", RIPPLE_SYSDICTS_TMP_FILE);
	}

	hash_seq_init(&status,proccache);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		proc = entry->ripple_proc;

		if(page_offset + sizeof(xk_pg_parser_sysdict_pgproc) > RIPPLE_FILE_BLK_SIZE)
		{
			if (FilePWrite(fd, buffer, RIPPLE_FILE_BLK_SIZE, *offset) != RIPPLE_FILE_BLK_SIZE) {
				elog(RLOG_ERROR, "could not write to file %s", RIPPLE_SYSDICTS_TMP_FILE);
				close(fd);
				return;
			}
			rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);
			page_num = *offset + page_offset;
			*offset += RIPPLE_FILE_BLK_SIZE;
			page_offset = 0;
		}
		rmemcpy1(buffer, page_offset, proc, sizeof(xk_pg_parser_sysdict_pgproc));
		page_offset += sizeof(xk_pg_parser_sysdict_pgproc);
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
	array[RIPPLE_CATALOG_TYPE_PROC - 1].len = page_num;
}

void ripple_proc_catalogdatafree(ripple_catalogdata* catalogdata)
{
    ripple_catalog_proc_value* catalog = NULL;
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
    catalog = (ripple_catalog_proc_value*)catalogdata->catalog;
    if(NULL != catalog->ripple_proc)
    {
        rfree(catalog->ripple_proc);
    }
    rfree(catalogdata->catalog);
    rfree(catalogdata);
}
