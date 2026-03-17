#include "app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "utils/conn/conn.h"
#include "utils/hash/hash_utils.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/cache_sysidcts.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/transcache.h"
#include "catalog/catalog.h"
#include "catalog/operator.h"

void operator_getfromdb(PGconn *conn, cache_sysdicts* sysdicts)
{
	int i, j; 
	PGresult *res = NULL;
	HASHCTL hash_ctl;
	bool found = false;
	catalog_operator_value *entry = NULL;
	xk_pg_sysdict_Form_pg_operator operator = NULL;
	const char *query = "SELECT rel.oid,rel.oprname FROM pg_operator rel;";

	rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(catalog_operator_value);
	sysdicts->by_operator = hash_create("catalog_sysdicts_operator", 2048, &hash_ctl,
								 HASH_ELEM | HASH_BLOBS);

	res = conn_exec(conn, query);
	if (NULL == res)
	{
		elog(RLOG_ERROR, "pg_operator query failed");
	}

	// 打印行数据
	for (i = 0; i < PQntuples(res); i++) 
	{
		operator = (xk_pg_sysdict_Form_pg_operator)rmalloc0(sizeof(xk_pg_parser_sysdict_pgoperator));
		if(NULL == operator)
		{
			elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
		}
		rmemset0(operator, 0, '\0', sizeof(xk_pg_parser_sysdict_pgoperator));
		j=0;
		sscanf(PQgetvalue(res, i, j++), "%u", &operator->oid);
		strcpy(operator->oprname.data ,PQgetvalue(res, i, j++));

		entry = hash_search(sysdicts->by_operator, &operator->oid, HASH_ENTER, &found);
		if(found)
		{
			elog(RLOG_ERROR, "operator_oid:%u already exist in by_operator", entry->operator->oid);
		}
		entry->oid = operator->oid;
		entry->operator = operator;

	}
	PQclear(res);
	return;
}


void operatordata_write(List* operator_list, uint64 *offset, sysdict_header_array* array)
{
	int	 fd;
	uint64 page_num = 0;
	uint64 page_offset = 0;
	ListCell*	cell = NULL;
	char buffer[FILE_BLK_SIZE];
	xk_pg_sysdict_Form_pg_operator rippleoperator;
	
	array->type = CATALOG_TYPE_OPERATOR;
	array->offset = *offset;
	page_num = *offset;

	rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
	fd = osal_basic_open_file(SYSDICTS_FILE,
						O_RDWR | O_CREAT | BINARY);

	if (fd < 0)
	{
		elog(RLOG_ERROR, "could not create file %s", SYSDICTS_FILE);
	}

	foreach(cell, operator_list)
	{
		rippleoperator = (xk_pg_sysdict_Form_pg_operator) lfirst(cell);

		if(page_offset + sizeof(xk_pg_parser_sysdict_pgoperator) > FILE_BLK_SIZE)
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
		rmemcpy1(buffer, page_offset, rippleoperator, sizeof(xk_pg_parser_sysdict_pgoperator));
		page_offset += sizeof(xk_pg_parser_sysdict_pgoperator);
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

HTAB* operatorcache_load(sysdict_header_array* array)
{
    int r = 0;
    int fd = -1;
    HTAB* operatorhtab;
    HASHCTL hash_ctl;
    bool found = false;
    uint64 fileoffset = 0;
    char buffer[FILE_BLK_SIZE];
    xk_pg_sysdict_Form_pg_operator operator;
    catalog_operator_value *entry = NULL;

    rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(uint32_t);
    hash_ctl.entrysize = sizeof(catalog_operator_value);
    operatorhtab = hash_create("catalog_operator_value", 2048, &hash_ctl,
                                 HASH_ELEM | HASH_BLOBS);

    if (array[CATALOG_TYPE_OPERATOR - 1].len == array[CATALOG_TYPE_OPERATOR - 1].offset)
    {
        return operatorhtab;
    }

    fd = osal_basic_open_file(SYSDICTS_FILE,
                        O_RDWR | BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", SYSDICTS_FILE);
    }

    fileoffset = array[CATALOG_TYPE_OPERATOR - 1].offset;
    while ((r = osal_file_pread(fd, buffer, FILE_BLK_SIZE, fileoffset)) > 0) 
    {
        uint64 offset = 0;

        while (offset + sizeof(xk_pg_parser_sysdict_pgoperator) < FILE_BLK_SIZE)
        {
            operator = (xk_pg_sysdict_Form_pg_operator)rmalloc1(sizeof(xk_pg_parser_sysdict_pgoperator));
            if(NULL == operator)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(operator, 0, '\0', sizeof(xk_pg_parser_sysdict_pgoperator));
            rmemcpy0(operator, 0, buffer + offset, sizeof(xk_pg_parser_sysdict_pgoperator));
            entry = hash_search(operatorhtab, &operator->oid, HASH_ENTER, &found);
            if(found)
            {
                elog(RLOG_ERROR, "operator_oid:%u already exist in by_operator", entry->operator->oid);
            }
            entry->oid = operator->oid;
            entry->operator = operator;
            offset += sizeof(xk_pg_parser_sysdict_pgoperator);
            if (fileoffset + offset == array[CATALOG_TYPE_OPERATOR - 1].len)
            {
                if(osal_file_close(fd))
                {
                    elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
                }
                return operatorhtab;
            }
        }
        fileoffset += FILE_BLK_SIZE;
    }

    if(osal_file_close(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
    }

    return operatorhtab;
}

void operatorcache_write(HTAB* operatorcache, uint64 *offset, sysdict_header_array* array)
{
	int	 fd;
	uint64 page_num = 0;
	uint64 page_offset = 0;
	HASH_SEQ_STATUS status;
	char buffer[FILE_BLK_SIZE];
	catalog_operator_value *entry = NULL;
	xk_pg_sysdict_Form_pg_operator operator = NULL;
	
	array[CATALOG_TYPE_OPERATOR - 1].type = CATALOG_TYPE_OPERATOR;
	array[CATALOG_TYPE_OPERATOR - 1].offset = *offset;
	page_num = *offset;

	rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
	fd = osal_basic_open_file(SYSDICTS_TMP_FILE,
						O_RDWR | O_CREAT | BINARY);

	if (fd < 0)
	{
		elog(RLOG_ERROR, "could not create file %s", SYSDICTS_TMP_FILE);
	}

	hash_seq_init(&status,operatorcache);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		operator = entry->operator;

		if(page_offset + sizeof(xk_pg_parser_sysdict_pgoperator) > FILE_BLK_SIZE)
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
		rmemcpy1(buffer, page_offset, operator, sizeof(xk_pg_parser_sysdict_pgoperator));
		page_offset += sizeof(xk_pg_parser_sysdict_pgoperator);
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
	array[CATALOG_TYPE_OPERATOR - 1].len = page_num;
}
