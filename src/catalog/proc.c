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
#include "catalog/proc.h"


void proc_getfromdb(PGconn *conn, cache_sysdicts* sysdicts)
{
	int	 i, j;
	HASHCTL hash_ctl;
	bool found = false;
	PGresult *res  = NULL;
	catalog_proc_value *entry = NULL;
	xk_pg_sysdict_Form_pg_proc proc = NULL;
	const char *query = "SELECT rel.oid,rel.proname, rel.pronamespace,rel.pronargs, rel.proargtypes FROM pg_proc rel;";
	
	rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(catalog_proc_value);
	sysdicts->by_proc = hash_create("catalog_sysdicts_proc", 2048, &hash_ctl,
								 HASH_ELEM | HASH_BLOBS);

	res = conn_exec(conn, query);
	if (NULL == res)
	{
		elog(RLOG_ERROR, "pg_proc query failed");
	}


	// 打印行数据
	for (i = 0; i < PQntuples(res); i++) 
	{
		proc = (xk_pg_sysdict_Form_pg_proc)rmalloc0(sizeof(xk_pg_parser_sysdict_pgproc));
		if(NULL == proc)
		{
			elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
		}
		rmemset0(proc, 0, '\0', sizeof(xk_pg_parser_sysdict_pgproc));
		j=0;
		sscanf(PQgetvalue(res, i, j++), "%u", &proc->oid);
		strcpy(proc->proname.data ,PQgetvalue(res, i, j++));
		sscanf(PQgetvalue(res, i, j++), "%u", &proc->pronamespace);
		sscanf(PQgetvalue(res, i, j++), "%hd", &proc->pronargs);

		entry = hash_search(sysdicts->by_proc, &proc->oid, HASH_ENTER, &found);
		if(found)
		{
			elog(RLOG_ERROR, "proc_oid:%u already exist in by_proc", entry->proc->oid);
		}
		entry->oid = proc->oid;
		entry->proc = proc;
	}
	PQclear(res);
	return;
}

void procdata_write(List* proc, uint64 *offset, sysdict_header_array* array)
{
	int	 fd;
	uint64 page_num = 0;
	uint64 page_offset = 0;
	ListCell*	cell = NULL;
	char buffer[FILE_BLK_SIZE];
	xk_pg_sysdict_Form_pg_proc proc_data;

	array->type = CATALOG_TYPE_PROC;
	array->offset = *offset;
	page_num = *offset;

	rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
	fd = osal_basic_open_file(SYSDICTS_FILE,
				O_RDWR | O_CREAT | BINARY);

	if (fd < 0)
	{
		elog(RLOG_ERROR, "could not create file %s", SYSDICTS_FILE);
	}

	foreach(cell, proc)
	{
		proc_data = (xk_pg_sysdict_Form_pg_proc) lfirst(cell);
		if(page_offset + sizeof(xk_pg_parser_sysdict_pgproc) > FILE_BLK_SIZE)
		{
			if (osal_file_pwrite(fd, buffer, FILE_BLK_SIZE, *offset) != FILE_BLK_SIZE) {
				elog(RLOG_ERROR, "could not write to file %s", SYSDICTS_FILE);
				close(fd);
				return;
			}
			rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
			page_num = *offset + page_offset;
			*offset += FILE_BLK_SIZE;
			page_offset = 0;
		}
		rmemcpy1(buffer, page_offset, proc_data, sizeof(xk_pg_parser_sysdict_pgproc));
		page_offset += sizeof(xk_pg_parser_sysdict_pgproc);
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
		elog(RLOG_ERROR, "could not osal_file_close file %s", SYSDICTS_FILE);
	}

	array->len = page_num;
}

HTAB* proccache_load(sysdict_header_array* array)
{
    int r = 0;
    int fd = -1;
    HTAB* prochtab;
    HASHCTL hash_ctl;
    bool found = false;
    uint64 fileoffset = 0;
    char buffer[FILE_BLK_SIZE];
    xk_pg_sysdict_Form_pg_proc proc;
    catalog_proc_value *entry = NULL;

    rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(uint32_t);
    hash_ctl.entrysize = sizeof(catalog_proc_value);
    prochtab = hash_create("catalog_proc_value", 2048, &hash_ctl,
                                 HASH_ELEM | HASH_BLOBS);

    if (array[CATALOG_TYPE_PROC - 1].len == array[CATALOG_TYPE_PROC - 1].offset)
    {
        return prochtab;
    }

    fd = osal_basic_open_file(SYSDICTS_FILE,
                        O_RDWR | BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", SYSDICTS_FILE);
    }

    fileoffset = array[CATALOG_TYPE_PROC - 1].offset;
    while ((r = osal_file_pread(fd, buffer, FILE_BLK_SIZE, fileoffset)) > 0) 
    {
        uint64 offset = 0;

        while (offset + sizeof(xk_pg_parser_sysdict_pgproc) < FILE_BLK_SIZE)
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
                elog(RLOG_ERROR, "proc_oid:%u already exist in by_proc", entry->proc->oid);
            }
            entry->oid = proc->oid;
            entry->proc = proc;
            offset += sizeof(xk_pg_parser_sysdict_pgproc);
            if (fileoffset + offset == array[CATALOG_TYPE_PROC - 1].len)
            {
                if(osal_file_close(fd))
                {
                    elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
                }
                return prochtab;
            }
        }
        fileoffset += FILE_BLK_SIZE;
    }

    if(osal_file_close(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
    }

    return prochtab;
}

/* colvalue2proc */
catalogdata* proc_colvalue2proc(void* in_colvalue)
{
    catalogdata* catalog_data = NULL;
    catalog_proc_value* procvalue = NULL;
    xk_pg_sysdict_Form_pg_proc pgproc = NULL;
    xk_pg_parser_translog_tbcol_value* colvalue = NULL;

    colvalue = (xk_pg_parser_translog_tbcol_value*)in_colvalue;

    /* 值转换 */
    catalog_data = (catalogdata*)rmalloc0(sizeof(catalogdata));
    if(NULL == catalog_data)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(catalog_data, 0, '\0', sizeof(catalogdata));

    procvalue = (catalog_proc_value*)rmalloc0(sizeof(catalog_proc_value));
    if(NULL == procvalue)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(procvalue, 0, '\0', sizeof(catalog_proc_value));
    catalog_data->catalog = procvalue;
    catalog_data->type = CATALOG_TYPE_PROC;

    pgproc = (xk_pg_sysdict_Form_pg_proc)rmalloc1(sizeof(xk_pg_parser_sysdict_pgproc));
    if(NULL == pgproc)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pgproc, 0, '\0', sizeof(xk_pg_parser_sysdict_pgproc));
    procvalue->proc = pgproc;

    /* oid 0 */
    sscanf((char*)((colvalue + 0)->m_value), "%u", &pgproc->oid);
    procvalue->oid = pgproc->oid;

    /* proname 1 */
    rmemcpy1(pgproc->proname.data, 0, (char*)((colvalue + 1)->m_value), (colvalue + 1)->m_valueLen);

    /* pronamespace 2 */
    sscanf((char*)((colvalue + 2)->m_value), "%u", &pgproc->pronamespace);

    /* pronargs 16 */
    sscanf((char*)((colvalue + 16)->m_value), "%hd", &pgproc->pronargs);

    return catalog_data;
}

/* catalogdata2transcache */
void proc_catalogdata2transcache(cache_sysdicts* sysdicts, catalogdata* catalogdata)
{
    bool found = false;
    catalog_proc_value* newcatalog = NULL;
    catalog_proc_value* catalogInHash = NULL;

    if(NULL == catalogdata || NULL == catalogdata->catalog)
    {
        return;
    }

    newcatalog = (catalog_proc_value*)catalogdata->catalog;
    if(CATALOG_OP_INSERT == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_proc, &newcatalog->oid, HASH_ENTER, &found);
        if(true == found)
        {
            elog(RLOG_WARNING, "by_proc hash duplicate oid, %u, %s",
                                catalogInHash->proc->oid,
                                catalogInHash->proc->proname.data);

            if(NULL != catalogInHash->proc)
            {
                rfree(catalogInHash->proc);
            }
        }
        catalogInHash->oid = newcatalog->oid;

		catalogInHash->proc = (xk_pg_sysdict_Form_pg_proc)rmalloc1(sizeof(xk_pg_parser_sysdict_pgproc));
		if(NULL == catalogInHash->proc)
		{
			elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
		}
		rmemcpy0(catalogInHash->proc, 0, newcatalog->proc, sizeof(xk_pg_parser_sysdict_pgproc));
    }
    else if(CATALOG_OP_DELETE == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_proc, &newcatalog->oid, HASH_REMOVE, &found);
        if(NULL != catalogInHash)
        {
            if(NULL != catalogInHash->proc)
            {
                rfree(catalogInHash->proc);
            }
        }
    }
    else if(CATALOG_OP_UPDATE == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_proc, &newcatalog->oid, HASH_FIND, &found);
        if(NULL == catalogInHash)
        {
            elog(RLOG_WARNING, "by_proc hash not found, %u, %s",
                                newcatalog->proc->oid,
                                newcatalog->proc->proname.data);
			return;
        }
        rfree(catalogInHash->proc);

		catalogInHash->proc = (xk_pg_sysdict_Form_pg_proc)rmalloc1(sizeof(xk_pg_parser_sysdict_pgproc));
		if(NULL == catalogInHash->proc)
		{
			elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
		}
		rmemcpy0(catalogInHash->proc, 0, newcatalog->proc, sizeof(xk_pg_parser_sysdict_pgproc));
    }
    else
    {
        elog(RLOG_ERROR, "unknown op:%d", catalogdata->op);
    }
}

void proccache_write(HTAB* proccache, uint64 *offset, sysdict_header_array* array)
{
	int	 fd;
	uint64 page_num = 0;
	uint64 page_offset = 0;
	HASH_SEQ_STATUS status;
	char buffer[FILE_BLK_SIZE];
	catalog_proc_value *entry = NULL;
	xk_pg_sysdict_Form_pg_proc proc = NULL;

	array[CATALOG_TYPE_PROC - 1].type = CATALOG_TYPE_PROC;
	array[CATALOG_TYPE_PROC - 1].offset = *offset;
	page_num = *offset;

	rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
	fd = osal_basic_open_file(SYSDICTS_TMP_FILE,
						O_RDWR | O_CREAT | BINARY);

	if (fd < 0)
	{
		elog(RLOG_ERROR, "could not create file %s", SYSDICTS_TMP_FILE);
	}

	hash_seq_init(&status,proccache);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		proc = entry->proc;

		if(page_offset + sizeof(xk_pg_parser_sysdict_pgproc) > FILE_BLK_SIZE)
		{
			if (osal_file_pwrite(fd, buffer, FILE_BLK_SIZE, *offset) != FILE_BLK_SIZE) {
				elog(RLOG_ERROR, "could not write to file %s", SYSDICTS_TMP_FILE);
				close(fd);
				return;
			}
			rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
			page_num = *offset + page_offset;
			*offset += FILE_BLK_SIZE;
			page_offset = 0;
		}
		rmemcpy1(buffer, page_offset, proc, sizeof(xk_pg_parser_sysdict_pgproc));
		page_offset += sizeof(xk_pg_parser_sysdict_pgproc);
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
	array[CATALOG_TYPE_PROC - 1].len = page_num;
}

void proc_catalogdatafree(catalogdata* catalogdata)
{
    catalog_proc_value* catalog = NULL;
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
    catalog = (catalog_proc_value*)catalogdata->catalog;
    if(NULL != catalog->proc)
    {
        rfree(catalog->proc);
    }
    rfree(catalogdata->catalog);
    rfree(catalogdata);
}
