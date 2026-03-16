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
#include "catalog/ripple_namespace.h"


void ripple_namespace_getfromdb(PGconn *conn, ripple_cache_sysdicts* sysdicts)
{
	int i, j;
	HASHCTL hash_ctl;
	bool found = false;
	PGresult *res  = NULL;
	ripple_catalog_namespace_value *entry = NULL;
	xk_pg_sysdict_Form_pg_namespace ripple_namespace = NULL;
	const char *query = "SELECT rel.oid,rel.nspname FROM pg_namespace rel;";

	rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(uint32_t);
	hash_ctl.entrysize = sizeof(ripple_catalog_namespace_value);
	sysdicts->by_namespace = hash_create("ripple_catalog_sysdicts_namespace", 2048, &hash_ctl,
										HASH_ELEM | HASH_BLOBS);

	res = ripple_conn_exec(conn, query);
	if (NULL == res)
	{
		elog(RLOG_ERROR, "pg_namespace query failed");
	}

	// 打印行数据
	for (i = 0; i < PQntuples(res); i++) 
	{
		ripple_namespace = (xk_pg_sysdict_Form_pg_namespace)rmalloc0(sizeof(xk_pg_parser_sysdict_pgnamespace));
		if(NULL == ripple_namespace)
		{
			elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
		}
		rmemset0(ripple_namespace, 0, '\0', sizeof(xk_pg_parser_sysdict_pgnamespace));
		j=0;
		sscanf(PQgetvalue(res, i, j++), "%u", &ripple_namespace->oid);
		strcpy(ripple_namespace->nspname.data, PQgetvalue(res, i, j++));
		
		entry = hash_search(sysdicts->by_namespace, &ripple_namespace->oid, HASH_ENTER, &found);
		if(found)
		{
			elog(RLOG_ERROR, "namespace_oid:%u already exist in by_namespace", entry->ripple_namespace->oid);
		}
		entry->oid = ripple_namespace->oid;
		entry->ripple_namespace = ripple_namespace;
	}

	PQclear(res);
	return;
}

void ripple_namespacedata_write(List* ripple_namespace, uint64 *offset, ripple_sysdict_header_array* array)
{
	int	 fd;
	uint64 page_num = 0;
	uint64 page_offset = 0;
	ListCell*	cell = NULL;
	char buffer[RIPPLE_FILE_BLK_SIZE];
	xk_pg_sysdict_Form_pg_namespace riplenamespace;

	array->type = RIPPLE_CATALOG_TYPE_NAMESPACE;
	array->offset = *offset;
	page_num = *offset;
	
	rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);
	fd = BasicOpenFile(RIPPLE_SYSDICTS_FILE,
						O_RDWR | O_CREAT | RIPPLE_BINARY);

	if (fd < 0)
	{
		elog(RLOG_ERROR, "could not create file %s", RIPPLE_SYSDICTS_FILE);
	}
	
	foreach(cell, ripple_namespace)
	{
		riplenamespace = (xk_pg_sysdict_Form_pg_namespace) lfirst(cell);

		if(page_offset + sizeof(xk_pg_parser_sysdict_pgnamespace) > RIPPLE_FILE_BLK_SIZE)
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
		rmemcpy1(buffer, page_offset, riplenamespace, sizeof(xk_pg_parser_sysdict_pgnamespace));
		page_offset += sizeof(xk_pg_parser_sysdict_pgnamespace);
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

HTAB* ripple_namespacecache_load(ripple_sysdict_header_array* array)
{
    int r = 0;
    int fd = -1;
    HTAB* namespacehtab;
    HASHCTL hash_ctl;
    bool found = false;
    uint64 fileoffset = 0;
    char buffer[RIPPLE_FILE_BLK_SIZE];
    xk_pg_sysdict_Form_pg_namespace namespace;
    ripple_catalog_namespace_value *entry = NULL;

    rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(uint32_t);
    hash_ctl.entrysize = sizeof(ripple_catalog_namespace_value);
    namespacehtab = hash_create("ripple_catalog_namespace_value", 2048, &hash_ctl,
                                 HASH_ELEM | HASH_BLOBS);

    if (array[RIPPLE_CATALOG_TYPE_NAMESPACE - 1].len == array[RIPPLE_CATALOG_TYPE_NAMESPACE - 1].offset)
    {
        return namespacehtab;
    }

    fd = BasicOpenFile(RIPPLE_SYSDICTS_FILE,
                        O_RDWR | RIPPLE_BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", RIPPLE_SYSDICTS_FILE);
    }

    fileoffset = array[RIPPLE_CATALOG_TYPE_NAMESPACE - 1].offset;
    while ((r = FilePRead(fd, buffer, RIPPLE_FILE_BLK_SIZE, fileoffset)) > 0) 
    {
        uint64 offset = 0;

        while (offset + sizeof(xk_pg_parser_sysdict_pgnamespace) < RIPPLE_FILE_BLK_SIZE)
        {
            namespace = (xk_pg_sysdict_Form_pg_namespace)rmalloc1(sizeof(xk_pg_parser_sysdict_pgnamespace));
            if(NULL == namespace)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(namespace, 0, '\0', sizeof(xk_pg_parser_sysdict_pgnamespace));
            rmemcpy0(namespace, 0, buffer + offset, sizeof(xk_pg_parser_sysdict_pgnamespace));
            entry = hash_search(namespacehtab, &namespace->oid, HASH_ENTER, &found);
            if(found)
            {
                elog(RLOG_ERROR, "namespace_oid:%u already exist in by_namespace", entry->ripple_namespace->oid);
            }
            entry->oid = namespace->oid;
            entry->ripple_namespace = namespace;
            offset += sizeof(xk_pg_parser_sysdict_pgnamespace);
            if (fileoffset + offset == array[RIPPLE_CATALOG_TYPE_NAMESPACE - 1].len)
            {
                if(FileClose(fd))
                {
                    elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_FILE);
                }
                return namespacehtab;
            }
        }
        fileoffset += RIPPLE_FILE_BLK_SIZE;
    }

    if(FileClose(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_FILE);
    }

    return namespacehtab;
}

/* colvalue2namespace */
ripple_catalogdata* ripple_namespace_colvalue2namespace(void* in_colvalue)
{
    ripple_catalogdata* catalogdata = NULL;
    ripple_catalog_namespace_value* namespacevalue = NULL;
    xk_pg_sysdict_Form_pg_namespace pgnamespace = NULL;
    xk_pg_parser_translog_tbcol_value* colvalue = NULL;

    colvalue = (xk_pg_parser_translog_tbcol_value*)in_colvalue;

    /* 值转换 */
    catalogdata = (ripple_catalogdata*)rmalloc1(sizeof(ripple_catalogdata));
    if(NULL == catalogdata)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(catalogdata, 0, '\0', sizeof(ripple_catalogdata));

    namespacevalue = (ripple_catalog_namespace_value*)rmalloc0(sizeof(ripple_catalog_namespace_value));
    if(NULL == namespacevalue)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(namespacevalue, 0, '\0', sizeof(ripple_catalog_namespace_value));
    catalogdata->catalog = namespacevalue;
    catalogdata->type = RIPPLE_CATALOG_TYPE_NAMESPACE;

    pgnamespace = (xk_pg_sysdict_Form_pg_namespace)rmalloc1(sizeof(xk_pg_parser_sysdict_pgnamespace));
    if(NULL == pgnamespace)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pgnamespace, 0, '\0', sizeof(xk_pg_parser_sysdict_pgnamespace));
    namespacevalue->ripple_namespace = pgnamespace;

    /* nspname 1 */
    rmemcpy1(pgnamespace->nspname.data, 0, (char*)((colvalue + 1)->m_value), (colvalue + 1)->m_valueLen);

    /* oid 0 */
    sscanf((char*)((colvalue + 0)->m_value), "%u", &pgnamespace->oid);
    namespacevalue->oid = pgnamespace->oid;

    return catalogdata;
}

/* catalogdata2transcache */
void ripple_namespace_catalogdata2transcache(ripple_cache_sysdicts* sysdicts, ripple_catalogdata* catalogdata)
{
    bool found = false;
    ripple_catalog_namespace_value* newcatalog = NULL;
    ripple_catalog_namespace_value* catalogInHash = NULL;
	xk_pg_sysdict_Form_pg_namespace duppgnsp = NULL;

    if(NULL == catalogdata || NULL == catalogdata->catalog)
    {
        return;
    }

    newcatalog = (ripple_catalog_namespace_value*)catalogdata->catalog;
    if(RIPPLE_CATALOG_OP_INSERT == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_namespace, &newcatalog->oid, HASH_ENTER, &found);
        if(true == found)
        {
            elog(RLOG_WARNING, "by_namespace hash duplicate oid, %u, %s",
                                catalogInHash->ripple_namespace->oid,
                                catalogInHash->ripple_namespace->nspname.data);

            if(NULL != catalogInHash->ripple_namespace)
            {
                rfree(catalogInHash->ripple_namespace);
            }
        }

		duppgnsp = (xk_pg_sysdict_Form_pg_namespace)rmalloc1(sizeof(xk_pg_parser_sysdict_pgnamespace));
		if(NULL == duppgnsp)
		{
			elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
		}
		rmemcpy0(duppgnsp, 0, newcatalog->ripple_namespace, sizeof(xk_pg_parser_sysdict_pgnamespace));

        catalogInHash->oid = newcatalog->oid;
        catalogInHash->ripple_namespace = duppgnsp;
    }
    else if(RIPPLE_CATALOG_OP_DELETE == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_namespace, &newcatalog->oid, HASH_REMOVE, &found);
        if(NULL != catalogInHash)
        {
            if(NULL != catalogInHash->ripple_namespace)
            {
                rfree(catalogInHash->ripple_namespace);
            }
        }
    }
    else if(RIPPLE_CATALOG_OP_UPDATE == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_namespace, &newcatalog->oid, HASH_FIND, &found);
        if(NULL == catalogInHash)
        {
            elog(RLOG_WARNING, "namespace %s,%u can not fond in namespace hash",
                                newcatalog->ripple_namespace->oid,
                                newcatalog->ripple_namespace->nspname.data);
			return;
        }
        rfree(catalogInHash->ripple_namespace);

		duppgnsp = (xk_pg_sysdict_Form_pg_namespace)rmalloc1(sizeof(xk_pg_parser_sysdict_pgnamespace));
		if(NULL == duppgnsp)
		{
			elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
		}
		rmemcpy0(duppgnsp, 0, newcatalog->ripple_namespace, sizeof(xk_pg_parser_sysdict_pgnamespace));

        catalogInHash->ripple_namespace = duppgnsp;
    }
    else
    {
        elog(RLOG_ERROR, "unknown op:%d", catalogdata->op);
    }
}

void ripple_namespacecache_write(HTAB* namespacecache, uint64 *offset, ripple_sysdict_header_array* array)
{
	int	 fd;
	uint64 page_num = 0;
	uint64 page_offset = 0;
	HASH_SEQ_STATUS status;
	char buffer[RIPPLE_FILE_BLK_SIZE];
	ripple_catalog_namespace_value *entry = NULL;
	xk_pg_sysdict_Form_pg_namespace riplenamespace = NULL;

	array[RIPPLE_CATALOG_TYPE_NAMESPACE - 1].type = RIPPLE_CATALOG_TYPE_NAMESPACE;
	array[RIPPLE_CATALOG_TYPE_NAMESPACE - 1].offset = *offset;
	page_num = *offset;

	rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);
	fd = BasicOpenFile(RIPPLE_SYSDICTS_TMP_FILE,
						O_RDWR | O_CREAT | RIPPLE_BINARY);

	if (fd < 0)
	{
		elog(RLOG_ERROR, "could not create file %s", RIPPLE_SYSDICTS_TMP_FILE);
	}
	
	hash_seq_init(&status,namespacecache);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		riplenamespace = entry->ripple_namespace;

		if(page_offset + sizeof(xk_pg_parser_sysdict_pgnamespace) > RIPPLE_FILE_BLK_SIZE)
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
		rmemcpy1(buffer, page_offset, riplenamespace, sizeof(xk_pg_parser_sysdict_pgnamespace));
		page_offset += sizeof(xk_pg_parser_sysdict_pgnamespace);
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
	array[RIPPLE_CATALOG_TYPE_NAMESPACE - 1].len = page_num;
}

void ripple_namespace_catalogdatafree(ripple_catalogdata* catalogdata)
{
    ripple_catalog_namespace_value* catalog = NULL;
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
    catalog = (ripple_catalog_namespace_value*)catalogdata->catalog;
    if(NULL != catalog->ripple_namespace)
    {
        rfree(catalog->ripple_namespace);
    }
    rfree(catalogdata->catalog);
    rfree(catalogdata);
}

/* 根据oid获取pg_namespace 数据 */
void* ripple_namespace_getbyoid(Oid oid, HTAB* by_namespace)
{
    bool found = false;
    ripple_catalog_namespace_value *nspentry = NULL;
    nspentry = hash_search(by_namespace, &oid, HASH_FIND, &found);
    if(false == found)
    {
        return NULL;
    }
    return (void*)nspentry->ripple_namespace;
}
