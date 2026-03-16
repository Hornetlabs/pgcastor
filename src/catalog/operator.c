#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "utils/conn/ripple_conn.h"
#include "utils/hash/hash_utils.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_transcache.h"
#include "catalog/ripple_catalog.h"
#include "catalog/ripple_operator.h"

void ripple_operator_getfromdb(PGconn *conn, ripple_cache_sysdicts* sysdicts)
{
	int i, j; 
	PGresult *res = NULL;
	HASHCTL hash_ctl;
	bool found = false;
	ripple_catalog_operator_value *entry = NULL;
	xk_pg_sysdict_Form_pg_operator ripple_operator = NULL;
	const char *query = "SELECT rel.oid,rel.oprname FROM pg_operator rel;";

	rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(ripple_catalog_operator_value);
	sysdicts->by_operator = hash_create("ripple_catalog_sysdicts_operator", 2048, &hash_ctl,
								 HASH_ELEM | HASH_BLOBS);

	res = ripple_conn_exec(conn, query);
	if (NULL == res)
	{
		elog(RLOG_ERROR, "pg_operator query failed");
	}

	// 打印行数据
	for (i = 0; i < PQntuples(res); i++) 
	{
		ripple_operator = (xk_pg_sysdict_Form_pg_operator)rmalloc0(sizeof(xk_pg_parser_sysdict_pgoperator));
		if(NULL == ripple_operator)
		{
			elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
		}
		rmemset0(ripple_operator, 0, '\0', sizeof(xk_pg_parser_sysdict_pgoperator));
		j=0;
		sscanf(PQgetvalue(res, i, j++), "%u", &ripple_operator->oid);
		strcpy(ripple_operator->oprname.data ,PQgetvalue(res, i, j++));

		entry = hash_search(sysdicts->by_operator, &ripple_operator->oid, HASH_ENTER, &found);
		if(found)
		{
			elog(RLOG_ERROR, "operator_oid:%u already exist in by_operator", entry->ripple_operator->oid);
		}
		entry->oid = ripple_operator->oid;
		entry->ripple_operator = ripple_operator;

	}
	PQclear(res);
	return;
}


void ripple_operatordata_write(List* operator_list, uint64 *offset, ripple_sysdict_header_array* array)
{
	int	 fd;
	uint64 page_num = 0;
	uint64 page_offset = 0;
	ListCell*	cell = NULL;
	char buffer[RIPPLE_FILE_BLK_SIZE];
	xk_pg_sysdict_Form_pg_operator rippleoperator;
	
	array->type = RIPPLE_CATALOG_TYPE_OPERATOR;
	array->offset = *offset;
	page_num = *offset;

	rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);
	fd = BasicOpenFile(RIPPLE_SYSDICTS_FILE,
						O_RDWR | O_CREAT | RIPPLE_BINARY);

	if (fd < 0)
	{
		elog(RLOG_ERROR, "could not create file %s", RIPPLE_SYSDICTS_FILE);
	}

	foreach(cell, operator_list)
	{
		rippleoperator = (xk_pg_sysdict_Form_pg_operator) lfirst(cell);

		if(page_offset + sizeof(xk_pg_parser_sysdict_pgoperator) > RIPPLE_FILE_BLK_SIZE)
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
		rmemcpy1(buffer, page_offset, rippleoperator, sizeof(xk_pg_parser_sysdict_pgoperator));
		page_offset += sizeof(xk_pg_parser_sysdict_pgoperator);
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

HTAB* ripple_operatorcache_load(ripple_sysdict_header_array* array)
{
    int r = 0;
    int fd = -1;
    HTAB* operatorhtab;
    HASHCTL hash_ctl;
    bool found = false;
    uint64 fileoffset = 0;
    char buffer[RIPPLE_FILE_BLK_SIZE];
    xk_pg_sysdict_Form_pg_operator operator;
    ripple_catalog_operator_value *entry = NULL;

    rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(uint32_t);
    hash_ctl.entrysize = sizeof(ripple_catalog_operator_value);
    operatorhtab = hash_create("ripple_catalog_operator_value", 2048, &hash_ctl,
                                 HASH_ELEM | HASH_BLOBS);

    if (array[RIPPLE_CATALOG_TYPE_OPERATOR - 1].len == array[RIPPLE_CATALOG_TYPE_OPERATOR - 1].offset)
    {
        return operatorhtab;
    }

    fd = BasicOpenFile(RIPPLE_SYSDICTS_FILE,
                        O_RDWR | RIPPLE_BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", RIPPLE_SYSDICTS_FILE);
    }

    fileoffset = array[RIPPLE_CATALOG_TYPE_OPERATOR - 1].offset;
    while ((r = FilePRead(fd, buffer, RIPPLE_FILE_BLK_SIZE, fileoffset)) > 0) 
    {
        uint64 offset = 0;

        while (offset + sizeof(xk_pg_parser_sysdict_pgoperator) < RIPPLE_FILE_BLK_SIZE)
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
                elog(RLOG_ERROR, "operator_oid:%u already exist in by_operator", entry->ripple_operator->oid);
            }
            entry->oid = operator->oid;
            entry->ripple_operator = operator;
            offset += sizeof(xk_pg_parser_sysdict_pgoperator);
            if (fileoffset + offset == array[RIPPLE_CATALOG_TYPE_OPERATOR - 1].len)
            {
                if(FileClose(fd))
                {
                    elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_FILE);
                }
                return operatorhtab;
            }
        }
        fileoffset += RIPPLE_FILE_BLK_SIZE;
    }

    if(FileClose(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_FILE);
    }

    return operatorhtab;
}

void ripple_operatorcache_write(HTAB* operatorcache, uint64 *offset, ripple_sysdict_header_array* array)
{
	int	 fd;
	uint64 page_num = 0;
	uint64 page_offset = 0;
	HASH_SEQ_STATUS status;
	char buffer[RIPPLE_FILE_BLK_SIZE];
	ripple_catalog_operator_value *entry = NULL;
	xk_pg_sysdict_Form_pg_operator operator = NULL;
	
	array[RIPPLE_CATALOG_TYPE_OPERATOR - 1].type = RIPPLE_CATALOG_TYPE_OPERATOR;
	array[RIPPLE_CATALOG_TYPE_OPERATOR - 1].offset = *offset;
	page_num = *offset;

	rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);
	fd = BasicOpenFile(RIPPLE_SYSDICTS_TMP_FILE,
						O_RDWR | O_CREAT | RIPPLE_BINARY);

	if (fd < 0)
	{
		elog(RLOG_ERROR, "could not create file %s", RIPPLE_SYSDICTS_TMP_FILE);
	}

	hash_seq_init(&status,operatorcache);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		operator = entry->ripple_operator;

		if(page_offset + sizeof(xk_pg_parser_sysdict_pgoperator) > RIPPLE_FILE_BLK_SIZE)
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
		rmemcpy1(buffer, page_offset, operator, sizeof(xk_pg_parser_sysdict_pgoperator));
		page_offset += sizeof(xk_pg_parser_sysdict_pgoperator);
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
	array[RIPPLE_CATALOG_TYPE_OPERATOR - 1].len = page_num;
}
