#include "ripple_app_incl.h"
#include "splitwork/wal/ripple_wal_define.h"
#include "cache/ripple_recordcache.h"
#include "splitwork/wal/ripple_split_walwork.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_errnodef.h"
#include "common/xk_pg_parser_translog.h"
#include "port/file/fd.h"
#include "utils/list/list_func.h"


#define USE_TEST 1

/* ---------------------tuple begin--------------------- */
#if 0
typedef struct pagecache
{
	uint32_t relfilenode;
	uint32_t pageno;
	char page[8192];
}pagecache;
#endif

#define pstrdup strdup
#define pg_strdup strdup
#define pfree free

#define WALWORK_MEM_ALLOC malloc
#define WALWORK_MEM_FREE free

typedef struct tupcache
{
	uint32_t relfilenode;
	uint32_t pageno;
	uint32_t itemoff;
	uint32_t len;
	char    *data;
}tupcache;
static List *tuplecache_list = NULL;

/* ---------------------tuple end--------------------- */

/* ---------------------external bgin--------------------- */

typedef struct chunkcache
{
	uint32_t chunkid;
	uint32_t chunk_len;
	uint32_t chunk_seq;
	char    *chunkdata;
}chunkcache;
static List *chunkcache_list = NULL;
/* ---------------------external end--------------------- */

typedef union
{
	struct						/* Normal varlena (4-byte length) */
	{
		uint32		va_header;
		char		va_data[FLEXIBLE_ARRAY_MEMBER];
	}			va_4byte;
	struct						/* Compressed-in-line format */
	{
		uint32		va_header;
		uint32		va_rawsize; /* Original data size (excludes header) */
		char		va_data[FLEXIBLE_ARRAY_MEMBER]; /* Compressed data */
	}			va_compressed;
} varattrib_4b;


/* 临时添加, 移植pg原版后删除 */
typedef struct xk_pg_parser_xl_running_xacts
{
    int             xcnt;               /* # of xact ids in xids[] */
    int             subxcnt;            /* # of subxact ids in xids[] */
    bool            subxid_overflow;    /* snapshot overflowed, subxids missing */
    uint32_t        nextXid;            /* xid from ShmemVariableCache->nextFullXid */
    uint32_t        oldestRunningXid;   /* *not* oldestXmin */
    uint32_t        latestCompletedXid; /* so we can set xmax */

    uint32_t        xids[FLEXIBLE_ARRAY_MEMBER];
} xk_pg_parser_xl_running_xacts;

static xk_pg_parser_translog_translog2col *temp_free_trans_result = NULL;
static uint64_t	start_lsn = 0;
static bool		no_pre = false;
//static bool		no_waldump = false;
static bool		no_trans_display = false;

#define WAL_LEVEL XK_PG_PARSER_WALLEVEL_REPLICA

//static const char *progname;
char *conninfo_kingbase = "host=localhost port=54321 dbname=kingbase password=Hello@123";
//char *conninfo_highgo = "host=localhost port=5866 dbname=highgo user=sysdba password=Hello@123";
char *conninfo_highgo = "host=127.0.0.1 port=5866 dbname=highgo user=sysdba password=78521@Liu123";
char *conninfo_uxdb = "host=124.128.223.82 port=50017 dbname=UXDB user=UXDB password=Qwer@1234.";
char *conninfo_default = "host=localhost port=5432 dbname=postgres";
static uint32_t dbtype = XK_DATABASE_TYPE_POSTGRESQL;
//static bool need_decrypt = false;

#define page_32K (uint32_t)32768
#define page_16K (uint32_t)16384
#define page_8K  (uint32_t)8192
static bool enable_tuple_debug = false;
static uint8_t enable_trans_debug = 0;
static uint32_t global_pagesize = page_8K;
static bool if_trans_ddl = true;
static bool display_ddl_trans = true;
//static int	WalSegSz;
static char dbversion[128] = {'\0'};

//static bool do_parser = false; 
static List *sysdict_list = NULL;
static bool inddl = false;


static void print_node_ddl(void* value, PGconn *conn, uint32_t relid, int local);
static xk_pg_parser_translog_pre_base *parse_record(XLogRecord *record);
static xk_pg_parser_translog_tbcolbase *trans_insert_record(xk_pg_parser_translog_pre_base *pre_base, XLogRecord *record, bool *needfree);
static xk_pg_parser_translog_tbcolbase *trans_delete_record(xk_pg_parser_translog_pre_base *pre_base, XLogRecord *record, bool *needfree);
static xk_pg_parser_translog_tbcolbase *trans_update_record(xk_pg_parser_translog_pre_base *pre_base, XLogRecord *record, bool *needfree);
static void insert_trans_display(xk_pg_parser_translog_tbcol_values *trans_base);
static void multi_insert_trans_display(xk_pg_parser_translog_tbcol_nvalues *trans_base);
static void delete_trans_display(xk_pg_parser_translog_tbcol_values *trans_base);
static void update_trans_display(xk_pg_parser_translog_tbcol_values *trans_base);
static Oid get_oid_by_felfilenode(PGconn *conn, uint32_t relfilenode);
static xk_pg_parser_sysdicts *get_sysdict_by_oid(PGconn *conn, Oid oid);
static xk_pg_parser_sysdict_pgclass *get_class_sysdict(PGconn *conn, Oid oid, int *natts, Oid *nspid);
static xk_pg_parser_sysdict_pgnamespace *get_namespace_sysdict(PGconn *conn, Oid nspid);
static xk_pg_parser_sysdict_pgattributes *get_attribute(PGconn *conn, Oid oid, Oid nspid, int attnum);
static xk_pg_parser_sysdict_pgtype *get_type(PGconn *conn, Oid typid);
static xk_pg_parser_sysdict_pgrange *get_range(PGconn *conn, Oid typid);
static List *get_enum_list(PGconn *conn, Oid typid, List *enum_list);
static xk_pg_parser_sysdict_pgproc *get_proc(PGconn *conn, Oid outputid);
static bool check_display(xk_pg_parser_translog_tbcol_values *trans);
static void get_all_sysdict(PGconn *conn,
							Oid search_oid,
							List **class_list,
							List **namespace_list,
							List **attributes_list,
							List **type_list,
							List **range_list,
							List **enum_list,
							List **proc_list,
							Oid typid_cache[100],
							int *typ_cache_num);
static char *get_relname_by_oid(PGconn *conn, uint32_t oid);
//static void storage_page(xk_pg_parser_translog_page *pages);
//static void get_pages(xk_pg_parser_translog_translog2col *trans_data,
//					  xk_pg_parser_translog_pre_heap *heap_pre);
//static pagecache *get_page_from_cache(uint32_t relfilenode, uint32_t pageno);
static void storage_tuple(uint32_t relfilenode, xk_pg_parser_translog_tuplecache *tuple, uint32_t count);
static void get_tuples(xk_pg_parser_translog_translog2col *trans_data,
					  xk_pg_parser_translog_pre_heap *heap_pre);
static tupcache *get_tuple_from_cache(uint32_t relfilenode, uint32_t pageno, uint32_t itemoff);
//static OffsetNumber PageAddItemExtended(Page page,
//											  Item item,
//											  Size size,
//											  OffsetNumber offsetNumber,
//											  int flags);
//static OffsetNumber PageApplyItem(Page page,
//											  Item item,
//											  Size size,
//											  OffsetNumber offsetNumber,
//											  int flags);
static uint8_t check_catalog(Oid oid);
static xk_pg_parser_translog_ddlstmt *trans_ddl(List *list);
static void display_ddl(xk_pg_parser_translog_ddlstmt *ddl_trans);
static char *get_namespace_name_by_oid(PGconn *conn, uint32_t oid);
//#define PageAddItem(page, item, size, offsetNumber, overwrite, is_heap)
//	PageAddItemExtended(page, item, size, offsetNumber,
//						((overwrite) ? PAI_OVERWRITE : 0) |
//						((is_heap) ? PAI_IS_HEAP : 0))

static void storage_catalog(xk_pg_parser_translog_tbcolbase *trans_return);


static void *palloc(size_t size)
{
	return malloc(size);
}

static void *palloc0(size_t size)
{
	void *result = malloc(size);
	memset(result, 0, size);
	return result;
}

static void trans_main(XLogRecord *record)
{
	/* 预解析parse_record */
	xk_pg_parser_translog_pre_base *pre_base = parse_record(record);
	xk_pg_parser_translog_tbcolbase *trans_base = NULL;
	xk_pg_parser_translog_tbcol_values *trans_value = NULL;
	xk_pg_parser_translog_tbcol_nvalues *trans_nvalue = NULL;
	//xk_pg_parser_translog_pre_image_tuple *trans_tuple = NULL;
	bool need_free = true;
	/* 二次解析trans_insert_record */
	if (pre_base->m_type == XK_PG_PARSER_TRANSLOG_HEAP_INSERT)
	{
		trans_base = trans_insert_record(pre_base, record, &need_free);
		trans_value = (xk_pg_parser_translog_tbcol_values *)trans_base;
		if (trans_value->m_base.m_type & XK_PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE)
			storage_tuple(trans_value->m_relfilenode, trans_value->m_tuple, trans_value->m_tupleCnt);
		if (!no_trans_display && check_display(trans_value))
			insert_trans_display(trans_value);
		if (need_free)
			xk_pg_parser_trans_TransRecord_free(NULL, trans_base);
		xk_pg_parser_trans_TransRecord_free(temp_free_trans_result, NULL);
		temp_free_trans_result = NULL;
	}
	else if (pre_base->m_type == XK_PG_PARSER_TRANSLOG_HEAP_DELETE)
	{
		trans_base = trans_delete_record(pre_base, record, &need_free);
		trans_value = (xk_pg_parser_translog_tbcol_values *)trans_base;
		if (trans_value->m_base.m_type & XK_PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE)
			storage_tuple(trans_value->m_relfilenode, trans_value->m_tuple, trans_value->m_tupleCnt);
		if (!no_trans_display && check_display(trans_value))
			delete_trans_display(trans_value);
		if (need_free)
			xk_pg_parser_trans_TransRecord_free(NULL, trans_base);
		xk_pg_parser_trans_TransRecord_free(temp_free_trans_result, NULL);
		temp_free_trans_result = NULL;
	}
	else if (pre_base->m_type == XK_PG_PARSER_TRANSLOG_HEAP_UPDATE
			 || pre_base->m_type == XK_PG_PARSER_TRANSLOG_HEAP_HOT_UPDATE)
	{
		trans_base = trans_update_record(pre_base, record, &need_free);
		trans_value = (xk_pg_parser_translog_tbcol_values *)trans_base;
		if (trans_value->m_base.m_type & XK_PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE)
			storage_tuple(trans_value->m_relfilenode, trans_value->m_tuple, trans_value->m_tupleCnt);
		if (!no_trans_display && check_display(trans_value))
			update_trans_display(trans_value);
		if (need_free)
			xk_pg_parser_trans_TransRecord_free(NULL, trans_base);
		xk_pg_parser_trans_TransRecord_free(temp_free_trans_result, NULL);
		temp_free_trans_result = NULL;
	}
	else if (pre_base->m_type == XK_PG_PARSER_TRANSLOG_FPW_TUPLE)
	{
		xk_pg_parser_translog_pre_image_tuple *image = (xk_pg_parser_translog_pre_image_tuple *)pre_base;
		storage_tuple(image->m_relfilenode, image->m_tuples, image->m_tuplecnt);
	}
	else if (pre_base->m_type == XK_PG_PARSER_TRANSLOG_HEAP2_MULTI_INSERT)
	{
		trans_base = trans_insert_record(pre_base, record, &need_free);
		trans_nvalue = (xk_pg_parser_translog_tbcol_nvalues *)trans_base;
		if (trans_nvalue->m_base.m_type & XK_PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE)
			storage_tuple(trans_nvalue->m_relfilenode, trans_nvalue->m_tuple, trans_nvalue->m_tupleCnt);
		if (!no_trans_display)
			multi_insert_trans_display(trans_nvalue);
		if (need_free)
			xk_pg_parser_trans_TransRecord_Minsert_free(NULL, trans_base);
		xk_pg_parser_trans_TransRecord_Minsert_free(temp_free_trans_result, NULL);
		temp_free_trans_result = NULL;
	}
	xk_pg_parser_trans_preTrans_free(pre_base);
}


static xk_pg_parser_translog_pre_base *parse_record(XLogRecord *record)
{
#ifdef USE_TEST
	xk_pg_parser_translog_pre *pre_parser_test = palloc0(sizeof(xk_pg_parser_translog_pre));
	xk_pg_parser_translog_pre_base * return_base = NULL;
	int32_t xk_errno = 0;
	xk_pg_parser_translog_pre_heap * heap = NULL;
	xk_pg_parser_translog_pre_trans * trans = NULL;

	//char *msg = palloc(1024);
	pre_parser_test->m_debugLevel = enable_trans_debug;
	pre_parser_test->m_pagesize = global_pagesize;
	pre_parser_test->m_walLevel = WAL_LEVEL;
	pre_parser_test->m_record = palloc0(record->xl_tot_len);
	memcpy(pre_parser_test->m_record, record, record->xl_tot_len);
	//pre_parser_test->m_record = (uint8_t *)record;
	pre_parser_test->m_dbtype = dbtype;
	pre_parser_test->m_dbversion = dbversion;

	if (!xk_pg_parser_trans_preTrans(pre_parser_test, &return_base, &xk_errno))
	{
		printf("ERROR IN PRE TRANS\n");
		printf("errcode: %x, msg: %s", xk_errno, xk_pg_parser_errno_getErrInfo(xk_errno));
		exit(1);
	}
	if (return_base->m_type == XK_PG_PARSER_TRANSLOG_XACT_COMMIT
		&& inddl)
	{
		if (if_trans_ddl)
		{
			xk_pg_parser_translog_ddlstmt *ddl_trans = NULL;
			ddl_trans = trans_ddl(sysdict_list);
			display_ddl(ddl_trans);
			inddl = false;
		}
		if (chunkcache_list)
		{
			list_free_deep(chunkcache_list);
			chunkcache_list = NULL;
		}

	}
	if (return_base->m_type == XK_PG_PARSER_TRANSLOG_XACT_ABORT
		&& inddl)
	{
		if (if_trans_ddl)
		{
			list_free_deep(sysdict_list);
			sysdict_list = NULL;
			inddl = false;
		}
		if (chunkcache_list)
		{
			list_free_deep(chunkcache_list);
			chunkcache_list = NULL;
		}

	}
	if (!no_pre)
	{
		if (return_base)
		{
			printf("-------------------------------\n");
			printf("V V V V V V V V V V V V V V V V\n");
			printf("success get pre parser data!\n");
			switch (return_base->m_type)
			{
				case (uint8_t)0x00:
					printf("needn't parser record\n");
					break;
				case (uint8_t)0x01:
				case (uint8_t)0x02:
				case (uint8_t)0x03:
				case (uint8_t)0x04:
				case (uint8_t)0x05:
					heap = (xk_pg_parser_translog_pre_heap *)return_base;
					printf("\n\nheap record\n");
					printf("m_base\n");
					printf("     |-->m_type: %u\n", heap->m_base.m_type);
					printf("m_needtuple: %u\n", heap->m_needtuple);
					printf("m_originid: %u\n", heap->m_base.m_originid);
					printf("m_tuplecnt: %u\n", heap->m_tuplecnts);
					printf("m_transid: %u\n", heap->m_transid);
					printf("m_relfilenode: %u\n", heap->m_relfilenode);
					if (heap->m_tuplecnts == 0)
						printf("m_tuples: NULL\n");
					else
					{
						printf("m_tuples: \n");
						printf("        |--block[%u]---item offset[%u]\n", heap->m_pagenos, heap->m_tupitemoff);
					}
					break;
				case (uint8_t)0x06:
				case (uint8_t)0x07:
				{
					trans = (xk_pg_parser_translog_pre_trans *)return_base;

					printf("\n\ntrans record\n");
					printf("m_base\n");
					printf("     |-->m_type: %u\n", trans->m_base.m_type);
					printf("m_status: %u ", trans->m_status);
					if (1 == trans->m_status)
						printf("ABORT\n");
					else if (2 == trans->m_status)
						printf("COMMIT\n");
					if (trans->m_time)
						printf("m_time: %s\n", trans->m_time);
					else
						printf("m_time: NULL\n");
					break;
				}
				case (uint8_t)0x08:
					printf("\n\nswitch record\n");
					break;
				case (uint8_t)0x09:
				case (uint8_t)0x0A:
					printf("\n\ncheckpoint record\n");
					printf("next trans id : %lu\n", 
						((xk_pg_parser_translog_pre_transchkp*)return_base)->m_nextid);
					break;
				case (uint8_t) 0x0B:
					printf("\n\nget FPW data, storage tyuple\n");
					break;
				case (uint8_t) 0x0C:
				{
					//xk_pg_parser_translog_pre_relmap *relmap = 
					//	(xk_pg_parser_translog_pre_relmap *) return_base;
					printf("\n\nget relmap\n");
					break;
				}
				case (uint8_t) 0x0D:
				{
					int index_xid = 0;
					xk_pg_parser_translog_pre_running_xact * xacts =
						(xk_pg_parser_translog_pre_running_xact *) return_base;
					xk_pg_parser_xl_running_xacts *xact =
						(xk_pg_parser_xl_running_xacts *) xacts->m_standby;
					printf("\n\nrunnig xact\n");
					printf("latestCompletedXid: %u\n", xact->latestCompletedXid);
					printf("nextXid: %u\n", xact->nextXid);
					printf("oldestRunningXid: %u\n", xact->oldestRunningXid);
					printf("subxcnt: %d\n", xact->subxcnt);
					printf("xcnt: %d\n", xact->xcnt);
					for (index_xid = 0; index_xid < xact->xcnt; index_xid++)
						printf("xid[%d]: %u\n", index_xid, xact->xids[index_xid]);
					break;
				}
				case (uint8_t) 0x0E:
				{
					printf("\n\nxlog recovery\n");
					break;
				}
				case (uint8_t) 0x0F:
				case (uint8_t) 0x10:
				{
					trans = (xk_pg_parser_translog_pre_trans *)return_base;
					printf("\n\ntrans record\n");
					printf("m_base\n");
					printf("     |-->m_type: %u\n", trans->m_base.m_type);
					printf("m_status: %u ", trans->m_status);
					if (1 == trans->m_status)
						printf("ABORT PREPARE\n");
					else if (2 == trans->m_status)
						printf("COMMIT PREPARE\n");
					if (trans->m_time)
						printf("m_time: %s\n", trans->m_time);
					else
						printf("m_time: NULL\n");
					break;
				}
				case (uint8_t) 0x11:
				{
					printf("ASSIGNMENT\n");
					break;
				}
				case (uint8_t) 0x12:
				{
					printf("PREPARE\n");
					break;
				}
				case (uint8_t) 0x13:
				{
					printf("TRUNCATE\n");
					break;
				}
				case (uint8_t) 0x14:
				{
					printf("INPLACE\n");
					break;
				}
				case (uint8_t) 0x15:
				{
					printf("CONFIRM\n");
					break;
				}

			}
			printf("A A A A A A A A A A A A A A A A\n");
			printf("-------------------------------\n");
			printf("\n\n");
		}
		else
		{
			printf("-------------------------------\n");
			printf("V V V V V V V V V V V V V V V V\n");
			printf("can't get data failed!\n");
			printf("A A A A A A A A A A A A A A A A\n");
			printf("-------------------------------\n");
			printf("\n\n");
		}
	}
	//xk_pg_parser_trans_preTrans_free(pre_parser_test);
	return return_base;
#else
	return true;
#endif
}
static void exit_nicely(PGconn *conn)
{
    PQfinish(conn);
    exit(1);
}

static void do_name_copy(xk_pg_parser_translog_tbcolbase *trans_return)
{
	char *temp_str = NULL;
	int index_nvalues = 0;
	int index_values = 0;
	temp_str = trans_return->m_tbname;
	trans_return->m_tbname = pg_strdup(temp_str);
	temp_str = trans_return->m_schemaname;
	trans_return->m_schemaname = pg_strdup(temp_str);
	if (trans_return->m_dmltype == XK_PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT)
	{
		xk_pg_parser_translog_tbcol_nvalues *dml_result = (xk_pg_parser_translog_tbcol_nvalues *)trans_return;
		for (index_nvalues = 0; index_nvalues < dml_result->m_rowCnt; index_nvalues++)
		{
			for (index_values = 0; index_values < dml_result->m_valueCnt; index_values++)
			{
				if (dml_result->m_rows[index_nvalues].m_new_values)
				{
					temp_str = dml_result->m_rows[index_nvalues].m_new_values[index_values].m_colName;
					dml_result->m_rows[index_nvalues].m_new_values[index_values].m_colName = pg_strdup(temp_str);
				}
			}
		}
	}
	else
	{
		xk_pg_parser_translog_tbcol_values *dml_result = (xk_pg_parser_translog_tbcol_values *)trans_return;
		for (index_values = 0; index_values < dml_result->m_valueCnt; index_values++)
		{
			if (dml_result->m_new_values)
			{
				if (dml_result->m_new_values[index_values].m_info != 7)
				{
					temp_str = dml_result->m_new_values[index_values].m_colName;
					dml_result->m_new_values[index_values].m_colName = pg_strdup(temp_str);
				}
				
			}
			if (dml_result->m_old_values)
			{
				if (dml_result->m_old_values[index_values].m_info != 7)
				{
					temp_str = dml_result->m_old_values[index_values].m_colName;
					dml_result->m_old_values[index_values].m_colName = pg_strdup(temp_str);
				}
			}
		}
	}
}

static bool check_external(PGconn *conn, uint32_t oid)
{
	PGresult   *res;
	char sql_exec[1024] = {'\0'};
	bool result = false;
	char *temp = NULL;
	if (dbtype == XK_DATABASE_TYPE_UXDB)
		sprintf(sql_exec, "select relnamespace from ux_class where oid = %u;", oid);
	else
		sprintf(sql_exec, "select relnamespace from pg_class where oid = %u;", oid);
	res = PQexec(conn, sql_exec);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "failed in get nspname: %s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}
	temp = PQgetvalue(res, 0, 0);
	if (!temp)
	{
		fprintf(stderr, "failed in check_external: %u", oid);
		PQclear(res);
		exit_nicely(conn);
	}
	if (!strcmp(temp, "99"))
		result = true;
	PQclear(res);
	return result;
}

static xk_pg_parser_translog_tbcolbase *trans_insert_record(xk_pg_parser_translog_pre_base *pre_base, XLogRecord *record, bool *need_free)
{
	xk_pg_parser_translog_translog2col *trans_data = palloc(sizeof(xk_pg_parser_translog_translog2col));
	xk_pg_parser_translog_tbcolbase *trans_return = NULL;
	xk_pg_parser_translog_pre_heap *heap_pre = (xk_pg_parser_translog_pre_heap*)pre_base;
	Oid oid = 0;
	int32_t err_num = 0;

	const char *conninfo;
	PGconn	 *conn;
	//PGresult   *res;
	bool		isexternal;

	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}
	oid = get_oid_by_felfilenode(conn, heap_pre->m_relfilenode);
	trans_data->m_iscatalog = check_catalog(oid);
	isexternal = check_external(conn, oid);
	if (if_trans_ddl)
	{
		if (!trans_data->m_iscatalog && inddl)
		{
			xk_pg_parser_translog_ddlstmt *ddl_trans = NULL;
			ddl_trans = trans_ddl(sysdict_list);
			display_ddl(ddl_trans);
			inddl = false;
		}
	}


	trans_data->m_tuplecnt = 0;
	trans_data->m_tuples = NULL;
	trans_data->m_pagesize = global_pagesize;
	trans_data->m_record = palloc0(record->xl_tot_len);
	memcpy(trans_data->m_record, record, record->xl_tot_len);
	//trans_data->m_record = (uint8_t *)record;
	trans_data->m_dbtype = dbtype;
	trans_data->m_dbversion = dbversion;
	trans_data->m_debugLevel = enable_trans_debug;

	trans_data->m_convert = palloc0(sizeof(xk_pg_parser_translog_convertinfo));
	trans_data->m_convert->m_dbcharset = pstrdup("UTF8");
	trans_data->m_convert->m_tartgetcharset = pstrdup("UTF8");
	trans_data->m_convert->m_tzname = pstrdup("Asia/Shanghai");
	trans_data->m_convert->m_monetary = pstrdup("en_US.UTF-8");
	trans_data->m_convert->m_numeric = pstrdup("en_US.UTF-8");
	trans_data->m_walLevel = WAL_LEVEL;
	trans_data->m_sysdicts = get_sysdict_by_oid(conn, oid);

	if (!xk_pg_parser_trans_TransRecord(trans_data, &trans_return, &err_num))
	{
		printf("error in trans!\n");
		printf("errcode: %x, msg: %s", err_num, xk_pg_parser_errno_getErrInfo(err_num));
		exit(1);
	}
	do_name_copy(trans_return);
	if (trans_data->m_iscatalog)
	{
		storage_catalog(trans_return);
		*need_free = false;
	}
	else
		*need_free = true;
	if (isexternal)
	{
		xk_pg_parser_translog_tbcol_values *insert = (xk_pg_parser_translog_tbcol_values *)trans_return;
		chunkcache *chunk = palloc0(sizeof(chunkcache));
		chunk->chunk_len = insert->m_new_values[2].m_valueLen;
		chunk->chunk_seq = strtoul((char*)insert->m_new_values[1].m_value, NULL, 10);
		chunk->chunkid = strtoul((char*)insert->m_new_values[0].m_value, NULL, 10);
		chunk->chunkdata = palloc0(chunk->chunk_len);
		memcpy(chunk->chunkdata, insert->m_new_values[2].m_value, chunk->chunk_len);
		free(insert->m_new_values[2].m_value);
		insert->m_new_values[2].m_value = NULL;
		chunkcache_list = lappend(chunkcache_list, chunk);
	}
	PQfinish(conn);
	temp_free_trans_result = trans_data;
	return trans_return;
}

static xk_pg_parser_translog_tbcolbase *trans_delete_record(xk_pg_parser_translog_pre_base *pre_base, XLogRecord *record, bool *need_free)
{
	xk_pg_parser_translog_translog2col *trans_data = palloc(sizeof(xk_pg_parser_translog_translog2col));
	xk_pg_parser_translog_tbcolbase *trans_return = NULL;
	xk_pg_parser_translog_pre_heap *heap_pre = (xk_pg_parser_translog_pre_heap*)pre_base;
	Oid oid = 0;
	int32_t err_num = 0;
	char *temp_name = NULL;

	const char *conninfo;
	PGconn	 *conn;
//	PGresult   *res;
	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}
	oid = get_oid_by_felfilenode(conn, heap_pre->m_relfilenode);
	temp_name = get_relname_by_oid(conn, oid);
	trans_data->m_iscatalog = check_catalog(oid);
	if (heap_pre->m_needtuple && strncmp(temp_name, "pg_toast", 8))
	{
		if (trans_data->m_iscatalog || WAL_LEVEL == XK_PG_PARSER_WALLEVEL_REPLICA)
		{
			trans_data->m_tuplecnt = heap_pre->m_tuplecnts;
			trans_data->m_tuples = palloc0(sizeof(xk_pg_parser_translog_tuplecache) * trans_data->m_tuplecnt);
			get_tuples(trans_data, heap_pre);
			trans_data->m_pagesize = global_pagesize;
			trans_data->m_record = palloc0(record->xl_tot_len);
			memcpy(trans_data->m_record, record, record->xl_tot_len);
			//trans_data->m_record = (uint8_t *)record;
			trans_data->m_dbtype = dbtype;
			trans_data->m_dbversion = dbversion;
			trans_data->m_debugLevel = enable_trans_debug;
		}
		else
		{
			trans_data->m_tuplecnt = 0;
			trans_data->m_tuples = NULL;
			trans_data->m_pagesize = global_pagesize;
			trans_data->m_record = palloc0(record->xl_tot_len);
			memcpy(trans_data->m_record, record, record->xl_tot_len);
			//trans_data->m_record = (uint8_t *)record;
			trans_data->m_dbtype = dbtype;
			trans_data->m_dbversion = dbversion;
			trans_data->m_debugLevel = enable_trans_debug;
		}
	}
	else
	{

		trans_data->m_tuplecnt = 0;
		trans_data->m_tuples = NULL;
		trans_data->m_pagesize = global_pagesize;
		trans_data->m_record = palloc0(record->xl_tot_len);
		memcpy(trans_data->m_record, record, record->xl_tot_len);
		//trans_data->m_record = (uint8_t *)record;
		trans_data->m_dbtype = dbtype;
		trans_data->m_dbversion = dbversion;
		trans_data->m_debugLevel = enable_trans_debug;
	}
	if (if_trans_ddl)
	{
		if (!trans_data->m_iscatalog && inddl)
		{
			xk_pg_parser_translog_ddlstmt *ddl_trans = NULL;
			ddl_trans = trans_ddl(sysdict_list);
			display_ddl(ddl_trans);
			inddl = false;
		}
	}

	trans_data->m_convert = palloc0(sizeof(xk_pg_parser_translog_convertinfo));
	trans_data->m_convert->m_dbcharset = pstrdup("UTF8");
	trans_data->m_convert->m_tartgetcharset = pstrdup("UTF8");
	trans_data->m_convert->m_tzname = pstrdup("Asia/Shanghai");
	trans_data->m_convert->m_monetary = pstrdup("en_US.UTF-8");
	trans_data->m_convert->m_numeric = pstrdup("en_US.UTF-8");
	trans_data->m_walLevel = WAL_LEVEL;
	trans_data->m_sysdicts = get_sysdict_by_oid(conn, oid);

	if (!xk_pg_parser_trans_TransRecord(trans_data, &trans_return, &err_num))
	{
		printf("error in trans!\n");
		printf("errcode: %x, msg: %s", err_num, xk_pg_parser_errno_getErrInfo(err_num));
		exit(1);
	}
	do_name_copy(trans_return);
	if (trans_data->m_iscatalog)
	{
		storage_catalog(trans_return);
		*need_free = false;
	}
	else
		*need_free = true;
	PQfinish(conn);
	temp_free_trans_result = trans_data;
	return trans_return;
}

static xk_pg_parser_translog_tbcolbase *trans_update_record(xk_pg_parser_translog_pre_base *pre_base, XLogRecord *record, bool *need_free)
{
	xk_pg_parser_translog_translog2col *trans_data = palloc0(sizeof(xk_pg_parser_translog_translog2col));
	xk_pg_parser_translog_tbcolbase *trans_return = NULL;
	xk_pg_parser_translog_pre_heap *heap_pre = (xk_pg_parser_translog_pre_heap*)pre_base;
	Oid oid = 0;
	int32_t err_num = 0;

	const char *conninfo;
	PGconn	 *conn;
//	PGresult   *res;

	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}
	oid = get_oid_by_felfilenode(conn, heap_pre->m_relfilenode);
	trans_data->m_iscatalog = check_catalog(oid);
	if (heap_pre->m_needtuple)
	{
		if (trans_data->m_iscatalog || WAL_LEVEL == XK_PG_PARSER_WALLEVEL_REPLICA)
		{
			trans_data->m_tuplecnt = heap_pre->m_tuplecnts;
			trans_data->m_tuples = palloc0(sizeof(xk_pg_parser_translog_tuplecache) * trans_data->m_tuplecnt);
			get_tuples(trans_data, heap_pre);
			trans_data->m_pagesize = global_pagesize;
			trans_data->m_record = palloc0(record->xl_tot_len);
			memcpy(trans_data->m_record, record, record->xl_tot_len);
			//trans_data->m_record = (uint8_t *)record;
			trans_data->m_dbtype = dbtype;
			trans_data->m_dbversion = dbversion;
			trans_data->m_debugLevel = enable_trans_debug;
		}
		else
		{
			trans_data->m_tuplecnt = 0;
			trans_data->m_tuples = NULL;
			trans_data->m_pagesize = global_pagesize;
			trans_data->m_record = palloc0(record->xl_tot_len);
			memcpy(trans_data->m_record, record, record->xl_tot_len);
			//trans_data->m_record = (uint8_t *)record;
			trans_data->m_dbtype = dbtype;
			trans_data->m_dbversion = dbversion;
			trans_data->m_debugLevel = enable_trans_debug;
		}
	}
	else
	{
		trans_data->m_tuplecnt = 0;
		trans_data->m_tuples = NULL;
		trans_data->m_pagesize = global_pagesize;
		trans_data->m_record = palloc0(record->xl_tot_len);
		memcpy(trans_data->m_record, record, record->xl_tot_len);
		//trans_data->m_record = (uint8_t *)record;
		trans_data->m_dbtype = dbtype;
		trans_data->m_dbversion = dbversion;
		trans_data->m_debugLevel = enable_trans_debug;
	}

	if (if_trans_ddl)
	{
		if (!trans_data->m_iscatalog && inddl)
		{
			xk_pg_parser_translog_ddlstmt *ddl_trans = NULL;
			ddl_trans = trans_ddl(sysdict_list);
			display_ddl(ddl_trans);
			inddl = false;
		}
	}

	trans_data->m_convert = palloc0(sizeof(xk_pg_parser_translog_convertinfo));
	trans_data->m_convert->m_dbcharset = pstrdup("UTF8");
	trans_data->m_convert->m_tartgetcharset = pstrdup("UTF8");
	trans_data->m_convert->m_tzname = pstrdup("Asia/Shanghai");
	trans_data->m_convert->m_monetary = pstrdup("en_US.UTF-8");
	trans_data->m_convert->m_numeric = pstrdup("en_US.UTF-8");
	trans_data->m_walLevel = WAL_LEVEL;
	trans_data->m_sysdicts = get_sysdict_by_oid(conn, oid);

	if (!xk_pg_parser_trans_TransRecord(trans_data, &trans_return, &err_num))
	{
		printf("error in trans!\n");
		printf("errcode: %x, msg: %s", err_num, xk_pg_parser_errno_getErrInfo(err_num));
		exit(1);
	}
	do_name_copy(trans_return);
	if (trans_data->m_iscatalog)
	{
		storage_catalog(trans_return);
		*need_free = false;
	}
	else
		*need_free = true;
	PQfinish(conn);
	temp_free_trans_result = trans_data;
	return trans_return;
}

static uint8_t check_catalog(Oid oid)
{
	if (oid < 16384)
		return (uint8_t) 1;
	else
		return (uint8_t) 0;
}

static Oid get_oid_by_felfilenode(PGconn *conn, uint32_t relfilenode)
{
	Oid result = 0;
	PGresult   *res;
	char sql_exec[256] = {'\0'};
	char *result_char = NULL;
	char *temp = NULL;
	int oid_tablespace_num = 0;
	int index_tablespace_num = 0;
	uint32_t *tablesp_oid = NULL;
	if (dbtype == XK_DATABASE_TYPE_UXDB)
		sprintf(sql_exec, "select oid from ux_tablespace;");
	else
		sprintf(sql_exec, "select oid from pg_tablespace;");
	res = PQexec(conn, sql_exec);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "failed in select: %s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}
	oid_tablespace_num = PQntuples(res);
	tablesp_oid = palloc0(sizeof(uint32_t) * (oid_tablespace_num + 1));
	for (index_tablespace_num = 0; index_tablespace_num < oid_tablespace_num; index_tablespace_num++)
	{
		tablesp_oid[index_tablespace_num] = strtoul(PQgetvalue(res, index_tablespace_num, 0), NULL, 10);
	}
	tablesp_oid[oid_tablespace_num] = 0;
	PQclear(res);

	for (index_tablespace_num = 0; index_tablespace_num < oid_tablespace_num + 1; index_tablespace_num++)
	{
		if (dbtype == XK_DATABASE_TYPE_UXDB)
			sprintf(sql_exec, "select ux_filenode_relation::oid from ux_filenode_relation(%d,%u);", tablesp_oid[index_tablespace_num], relfilenode);
		else
			sprintf(sql_exec, "select pg_filenode_relation::oid from pg_filenode_relation(%d,%u);", tablesp_oid[index_tablespace_num], relfilenode);
		res = PQexec(conn, sql_exec);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			fprintf(stderr, "failed in select: %s", PQerrorMessage(conn));
			PQclear(res);
			exit_nicely(conn);
		}
		temp = PQgetvalue(res, 0, 0);
		if (temp == NULL)
		{
			fprintf(stderr, "failed in get_oid_by_felfilenode, relfilenode[%u]", relfilenode);
			PQclear(res);
			exit_nicely(conn);
		}
		result_char = pstrdup(temp);
		PQclear(res);
		result = strtoul(result_char, NULL, 10);
		if (result != 0)
		{
			pfree(tablesp_oid);
			return result;
		}
	}
	fprintf(stderr, "failed in get_oid_by_felfilenode, relfilenode[%u], get oid is 0", relfilenode);
	pfree(tablesp_oid);
	exit_nicely(conn);
	/* make complier happy */
	return 0;
}

static xk_pg_parser_sysdict_pgclass *get_class_sysdict(PGconn *conn, Oid oid, int *natts, Oid *nspid)
{
	PGresult   *res;
//	uint32_t relfilenode = 0;
	char * temp_char = NULL;
	char sql_exec[1024] = {'\0'};
	xk_pg_parser_sysdict_pgclass *pgclass = palloc0(sizeof(xk_pg_parser_sysdict_pgclass));
	if (dbtype == XK_DATABASE_TYPE_UXDB)
		sprintf(sql_exec, "select ux_relation_filenode(%u)", oid);
	else
		sprintf(sql_exec, "select pg_relation_filenode(%u)", oid);
	res = PQexec(conn, sql_exec);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "failed in get relfilenode: %s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}
	pgclass->relfilenode = strtoul(PQgetvalue(res, 0, 0), NULL, 10);
	PQclear(res);

	memset(sql_exec, 0, 1024);
	if (dbtype == XK_DATABASE_TYPE_UXDB)
		sprintf(sql_exec, "select relname,relnamespace,reltype,"
		"relkind,relnatts,reltoastrelid from ux_class where oid = %u", oid);
	else
		sprintf(sql_exec, "select relname,relnamespace,reltype,"
						  "relkind,relnatts,reltoastrelid from pg_class where oid = %u", oid);
	res = PQexec(conn, sql_exec);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		if (dbtype == XK_DATABASE_TYPE_UXDB)
			fprintf(stderr, "failed in get ux_class: %s", PQerrorMessage(conn));
		else
			fprintf(stderr, "failed in get pg_class: %s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}
	pgclass->oid = oid;
	strcpy(pgclass->relname.data, PQgetvalue(res, 0, 0));
	pgclass->relnamespace = strtoul(PQgetvalue(res, 0, 1), NULL, 10);
	pgclass->reltype = strtoul(PQgetvalue(res, 0, 2), NULL, 10);
	temp_char = PQgetvalue(res, 0, 3);
	pgclass->relkind = temp_char[0];
	pgclass->relnatts = strtoul(PQgetvalue(res, 0, 4), NULL, 10);
	pgclass->reltoastrelid = strtoul(PQgetvalue(res, 0, 5), NULL, 10);
	PQclear(res);
	*natts = (int)pgclass->relnatts;
	*nspid = pgclass->relnamespace;
	return pgclass;
}

static xk_pg_parser_sysdict_pgnamespace *get_namespace_sysdict(PGconn *conn, Oid nspid)
{
	PGresult   *res;
//	uint32_t relfilenode = 0;
//	char * temp_char = NULL;
	char sql_exec[1024] = {'\0'};
	xk_pg_parser_sysdict_pgnamespace *pgnsp = palloc0(sizeof(xk_pg_parser_sysdict_pgnamespace));

	memset(sql_exec, 0, 1024);
	if (dbtype == XK_DATABASE_TYPE_UXDB)
		sprintf(sql_exec, "select nspname from ux_namespace where oid = %u", nspid);
	else
		sprintf(sql_exec, "select nspname from pg_namespace where oid = %u", nspid);
	res = PQexec(conn, sql_exec);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		if (dbtype == XK_DATABASE_TYPE_UXDB)
			fprintf(stderr, "failed in get ux_namespace: %s", PQerrorMessage(conn));
		else
			fprintf(stderr, "failed in get pg_namespace: %s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}
	pgnsp->oid = nspid;
	strcpy(pgnsp->nspname.data, PQgetvalue(res, 0, 0));
	PQclear(res);
	return pgnsp;
}

static xk_pg_parser_sysdict_pgattributes *get_attribute(PGconn *conn, Oid oid, Oid nspid, int attnum)
{
	PGresult   *res;
//	uint32_t relfilenode = 0;
	char * temp_char = NULL;
	char sql_exec[1024] = {'\0'};
	xk_pg_parser_sysdict_pgattributes *attribute = palloc0(sizeof(xk_pg_parser_sysdict_pgattributes));

	if (dbtype == XK_DATABASE_TYPE_UXDB)
		sprintf(sql_exec, "select attname,atttypid,attstattarget,attlen,attnum,"
						  "attndims,attcacheoff,atttypmod,attbyval,attstorage,attalign,"
						  "attnotnull,atthasdef,atthasmissing,attidentity,attgenerated,"
						  "attisdropped,attislocal,attinhcount,attcollation from ux_attribute "
						  "where attrelid = %u and attnum = %d", oid, attnum);
	else
		sprintf(sql_exec, "select attname,atttypid,attstattarget,attlen,attnum,"
						  "attndims,attcacheoff,atttypmod,attbyval,attstorage,attalign,"
						  "attnotnull,atthasdef,atthasmissing,attidentity,attgenerated,"
						  "attisdropped,attislocal,attinhcount,attcollation from pg_attribute "
						  "where attrelid = %u and attnum = %d", oid, attnum);
	res = PQexec(conn, sql_exec);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		if (dbtype == XK_DATABASE_TYPE_UXDB)
			fprintf(stderr, "failed in get ux_attribute: %s", PQerrorMessage(conn));
		else
			fprintf(stderr, "failed in get pg_attribute: %s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}
	attribute->attrelid = oid;
	strcpy(attribute->attname.data, PQgetvalue(res, 0, 0));
	attribute->atttypid = strtoul(PQgetvalue(res, 0, 1), NULL, 10);
	attribute->attstattarget = atoi(PQgetvalue(res, 0, 2));
	attribute->attlen = atoi(PQgetvalue(res, 0, 3));
	attribute->attnum = atoi(PQgetvalue(res, 0, 4));
	attribute->attndims = atoi(PQgetvalue(res, 0, 5));
	attribute->attcacheoff = atoi(PQgetvalue(res, 0, 6));
	attribute->atttypmod = atoi(PQgetvalue(res, 0, 7));
	temp_char = PQgetvalue(res, 0, 8);
	attribute->attbyval = temp_char[0] == 't' ? true : false;
	temp_char = PQgetvalue(res, 0, 9);
	attribute->attstorage = temp_char[0];
	temp_char = PQgetvalue(res, 0, 10);
	attribute->attalign = temp_char[0];
	temp_char = PQgetvalue(res, 0, 11);
	attribute->attnotnull = temp_char[0] == 't' ? true : false;
	temp_char = PQgetvalue(res, 0, 12);
	attribute->atthasdef = temp_char[0] == 't' ? true : false;
	temp_char = PQgetvalue(res, 0, 13);
	attribute->atthasmissing = temp_char[0] == 't' ? true : false;
	temp_char = PQgetvalue(res, 0, 14);
	attribute->attidentity = temp_char[0];
	temp_char = PQgetvalue(res, 0, 15);
	attribute->attgenerated = temp_char[0];
	temp_char = PQgetvalue(res, 0, 16);
	attribute->attisdropped = temp_char[0] == 't' ? true : false;
	temp_char = PQgetvalue(res, 0, 17);
	attribute->attislocal = temp_char[0] == 't' ? true : false;
	attribute->attinhcount = atoi(PQgetvalue(res, 0, 18));
	attribute->attcollation = strtoul(PQgetvalue(res, 0, 19), NULL, 10);

	PQclear(res);
	return attribute;
}

static xk_pg_parser_sysdict_pgtype *get_type(PGconn *conn, Oid typid)
{
	PGresult   *res;
//	uint32_t relfilenode = 0;
	char * temp_char = NULL;
	char sql_exec[1024] = {'\0'};
	xk_pg_parser_sysdict_pgtype *pgtype = palloc0(sizeof(xk_pg_parser_sysdict_pgtype));



	if (dbtype == XK_DATABASE_TYPE_UXDB)
		sprintf(sql_exec, "select typname,typlen,typbyval,typtype,"
						"typdelim,typelem,typoutput::oid,typrelid,typalign,typstorage "
						"from ux_type where oid = %u", typid);
	else
		sprintf(sql_exec, "select typname,typlen,typbyval,typtype,"
						"typdelim,typelem,typoutput::oid,typrelid,typalign,typstorage "
						"from pg_type where oid = %u", typid);
	res = PQexec(conn, sql_exec);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		if (dbtype == XK_DATABASE_TYPE_UXDB)
			fprintf(stderr, "failed in get ux_type: %s", PQerrorMessage(conn));
		else
			fprintf(stderr, "failed in get pg_type: %s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}

	pgtype->oid = typid;
	strcpy(pgtype->typname.data, PQgetvalue(res, 0, 0));
	pgtype->typlen = atoi(PQgetvalue(res, 0, 1));
	temp_char = PQgetvalue(res, 0, 2);
	pgtype->typbyval = temp_char[0] == 't' ? true : false;
	temp_char = PQgetvalue(res, 0, 3);
	pgtype->typtype = temp_char[0];
	temp_char = PQgetvalue(res, 0, 4);
	pgtype->typdelim = temp_char[0];
	pgtype->typelem = strtoul(PQgetvalue(res, 0, 5), NULL, 10);
	pgtype->typoutput = strtoul(PQgetvalue(res, 0, 6), NULL, 10);
	pgtype->typrelid = strtoul(PQgetvalue(res, 0, 7), NULL, 10);
	temp_char = PQgetvalue(res, 0, 8);
	pgtype->typalign = temp_char[0];
	temp_char = PQgetvalue(res, 0, 9);
	pgtype->typstorage = temp_char[0];

	PQclear(res);
	return pgtype;
}

static xk_pg_parser_sysdict_pgrange *get_range(PGconn *conn, Oid typid)
{
	PGresult   *res;
//	uint32_t relfilenode = 0;
//	char * temp_char = NULL;
	char sql_exec[1024] = {'\0'};
	xk_pg_parser_sysdict_pgrange *pgrange = palloc0(sizeof(xk_pg_parser_sysdict_pgrange));
	if (dbtype == XK_DATABASE_TYPE_UXDB)
		sprintf(sql_exec, "select rngsubtype from ux_range where rngtypid = %u", typid);
	else
		sprintf(sql_exec, "select rngsubtype from pg_range where rngtypid = %u", typid);
	res = PQexec(conn, sql_exec);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "failed in get pg_range: %s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}

	pgrange->rngtypid = typid;
	pgrange->rngsubtype = strtoul(PQgetvalue(res, 0, 0), NULL, 10);

	PQclear(res);
	return pgrange;
}

static List *get_enum_list(PGconn *conn, Oid typid, List *enum_list)
{
	PGresult   *res;
//	uint32_t relfilenode = 0;
//	char * temp_char = NULL;
	char sql_exec[1024] = {'\0'};
	int	 tuple_num = 0;
	int i = 0;
	if (dbtype == XK_DATABASE_TYPE_UXDB)
		sprintf(sql_exec, "select oid, enumlabel from ux_enum where enumtypid = %u", typid);
	else
		sprintf(sql_exec, "select oid, enumlabel from pg_enum where enumtypid = %u", typid);
	res = PQexec(conn, sql_exec);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "failed in get pg_enum_list: %s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}
	tuple_num = PQntuples(res);
	for (i = 0; i < tuple_num; i++)
	{
		xk_pg_parser_sysdict_pgenum *pgenum = palloc0(sizeof(xk_pg_parser_sysdict_pgenum));
		pgenum->oid = strtoul(PQgetvalue(res, i, 0), NULL, 10);
		strcpy(pgenum->enumlabel.data, PQgetvalue(res, i, 1));
		pgenum->enumtypid = typid;
		enum_list = lappend(enum_list, pgenum);
	}

	PQclear(res);
	return enum_list;
}

static xk_pg_parser_sysdict_pgproc *get_proc(PGconn *conn, Oid outputid)
{
	PGresult   *res;
//	uint32_t relfilenode = 0;
//	char * temp_char = NULL;
	char sql_exec[1024] = {'\0'};
	xk_pg_parser_sysdict_pgproc *pgproc = palloc0(sizeof(xk_pg_parser_sysdict_pgproc));

	if (dbtype == XK_DATABASE_TYPE_UXDB)
		sprintf(sql_exec, "select proname,pronamespace,pronargs from ux_proc where oid = %u",
																					outputid);
	else
		sprintf(sql_exec, "select proname,pronamespace,pronargs from pg_proc where oid = %u",
																					outputid);
	res = PQexec(conn, sql_exec);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		if (dbtype == XK_DATABASE_TYPE_UXDB)
			fprintf(stderr, "failed in get ux_proc: %s", PQerrorMessage(conn));
		else
			fprintf(stderr, "failed in get pg_proc: %s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}

	pgproc->oid = outputid;
	strcpy(pgproc->proname.data, PQgetvalue(res, 0, 0));
	pgproc->pronamespace = strtoul(PQgetvalue(res, 0, 1), NULL, 10);
	pgproc->pronargs = atoi(PQgetvalue(res, 0, 2));

	PQclear(res);
	return pgproc;
}

static void get_all_sysdict(PGconn *conn,
							Oid search_oid,
							List **class_list,
							List **namespace_list,
							List **attributes_list,
							List **type_list,
							List **range_list,
							List **enum_list,
							List **proc_list,
							Oid typid_cache[100],
							int *typ_cache_num)
{
	Oid nspid = 0;
	int natts = 0;
	int i = 0;
	ListCell *cell = NULL;
	xk_pg_parser_sysdict_pgattributes *att = NULL;
	xk_pg_parser_sysdict_pgtype *typ = NULL;
	Oid att_typid = 0;
	bool have_type_cache = false;
	List *temp_class_list = NULL;
	List *temp_namespace_list = NULL;
	List *temp_attributes_list = NULL;
	List *temp_type_list = NULL;
	List *temp_range_list = NULL;
	List *temp_enum_list = NULL;
	List *temp_proc_list = NULL;

	/* 查找pg_class */
	temp_class_list = lappend(temp_class_list, get_class_sysdict(conn, search_oid, &natts, &nspid));
	/* 查找pg_namespace */
	temp_namespace_list = lappend(temp_namespace_list, get_namespace_sysdict(conn, nspid));
	/* 查找pg_attribute */
	for (i = 0; i < natts; i++)
	{
		temp_attributes_list = lappend(temp_attributes_list, get_attribute(conn, search_oid, nspid, i + 1));
	}
	/* 查找pg_type */
	foreach(cell, temp_attributes_list)
	{
		att = (xk_pg_parser_sysdict_pgattributes*)lfirst(cell);
		if (att->attisdropped)
			continue;
		att_typid = att->atttypid;
		have_type_cache = false;
		for (i = 0; i < 100; i++)
		{
			if (typid_cache[i] == 0)
				break;
			if (typid_cache[i] == att_typid)
			{
				have_type_cache = true;
				break;
			}
		}
		if (!have_type_cache)
		{
			temp_type_list = lappend(temp_type_list, get_type(conn, att_typid));
			typid_cache[*typ_cache_num] = att_typid;
			*typ_cache_num += 1;
		}
	}
	/* 如果pg_type的typelem不为0, 需要将其加入到type链表中 */
	foreach(cell, temp_type_list)
	{
		typ = (xk_pg_parser_sysdict_pgtype*)lfirst(cell);
		have_type_cache = false;
		if (!typ->typelem)
			continue;
		for (i = 0; i < 100; i++)
		{
			if (typid_cache[i] == 0)
				break;
			if (typid_cache[i] == typ->typelem)
			{
				have_type_cache = true;
				break;
			}
		}
		if (!have_type_cache)
		{
			temp_type_list = lappend(temp_type_list, get_type(conn, typ->typelem));
			typid_cache[*typ_cache_num] = typ->typelem;
			*typ_cache_num += 1;
		}
	}
	
	cell = NULL;
	/* 查找pg_proc, pg_enum, pg_range */
	foreach(cell, temp_type_list)
	{
		typ = (xk_pg_parser_sysdict_pgtype*)lfirst(cell);
		if (typ->typtype == 'r')
		{
			xk_pg_parser_sysdict_pgrange *temp_range = get_range(conn, typ->oid);
			have_type_cache = false;
			for (i = 0; i < 100; i++)
			{
				if (typid_cache[i] == 0)
					break;
				if (typid_cache[i] == temp_range->rngsubtype)
				{
					have_type_cache = true;
					break;
				}
			}
			if (!have_type_cache)
			{
				temp_type_list = lappend(temp_type_list, get_type(conn, temp_range->rngsubtype));
				typid_cache[*typ_cache_num] = temp_range->rngsubtype;
				*typ_cache_num += 1;
			}
			temp_range_list = lappend(temp_range_list, temp_range);
		}
		else if (typ->typtype == 'e')
			temp_enum_list = get_enum_list(conn, typ->oid, temp_enum_list);

		temp_proc_list = lappend(temp_proc_list, get_proc(conn, typ->typoutput));
	}

	/* 将临时链表附加到总链表中 */
	if (temp_class_list)
	{
		cell = NULL;
		foreach(cell, temp_class_list)
			*class_list = lappend(*class_list, lfirst(cell));
		list_free(temp_class_list);
	}

	if (temp_attributes_list)
	{
		cell = NULL;
		foreach(cell, temp_attributes_list)
			*attributes_list = lappend(*attributes_list, lfirst(cell));
		list_free(temp_attributes_list);
	}

	if (temp_namespace_list)
	{
		cell = NULL;
		foreach(cell, temp_namespace_list)
			*namespace_list = lappend(*namespace_list, lfirst(cell));
		list_free(temp_namespace_list);
	}

	if (temp_type_list)
	{
		cell = NULL;
		foreach(cell, temp_type_list)
			*type_list = lappend(*type_list, lfirst(cell));
	}

	if (temp_range_list)
	{
		cell = NULL;
		foreach(cell, temp_range_list)
			*range_list = lappend(*range_list, lfirst(cell));
		list_free(temp_range_list);
	}

	if (temp_enum_list)
	{
		cell = NULL;
		foreach(cell, temp_enum_list)
			*enum_list = lappend(*enum_list, lfirst(cell));
		list_free(temp_enum_list);
	}

	if (temp_proc_list)
	{
		cell = NULL;
		foreach(cell, temp_proc_list)
			*proc_list = lappend(*proc_list, lfirst(cell));
		list_free(temp_proc_list);
	}

	if (temp_type_list)
	{
		cell = NULL;
		typ = NULL;
		foreach(cell, temp_type_list)
		{
			typ = (xk_pg_parser_sysdict_pgtype*)lfirst(cell);
			if (typ->typrelid != 0 && typ->typtype == 'c')
			{
				/* 递归获取子类型系统表 */
				get_all_sysdict(conn,
								typ->typrelid,
								class_list,
								namespace_list,
								attributes_list,
								type_list,
								range_list,
								enum_list,
								proc_list,
								typid_cache,
								typ_cache_num);
			}
		}
		list_free(temp_type_list);
	}

}

static xk_pg_parser_sysdicts *get_sysdict_by_oid(PGconn *conn, Oid oid)
{
	xk_pg_parser_sysdicts *result = NULL;
	List *class_list = NULL,
		 *attributes_list = NULL,
		 *namespace_list = NULL,
		 *type_list = NULL,
		 *range_list = NULL,
		 *enum_list = NULL,
		 *proc_list = NULL;
	ListCell *cell = NULL;
//	Oid search_oid = oid;
//	Oid nspid = 0;
//	int natts = 0;

//	xk_pg_parser_sysdict_pgattributes *att = NULL;
//	xk_pg_parser_sysdict_pgtype *typ = NULL;
//	Oid att_typid = 0;
	/* 简单情况下, 直接使用一个数组做缓存 */
	Oid typid_cache[100] = {'\0'};
//	bool have_type_cache = false;
//	int		 nFields;
	int		 i = 0,
			 typ_cache_num = 0;

	result = palloc0(sizeof(xk_pg_parser_sysdicts));
	get_all_sysdict(conn,
					oid,
					&class_list,
					&namespace_list,
					&attributes_list,
					&type_list,
					&range_list,
					&enum_list,
					&proc_list,
					typid_cache,
					&typ_cache_num);

	/* 组装结构体 */
	if (class_list)
	{
		result->m_pg_class.m_count = class_list->length;
		result->m_pg_class.m_pg_class = palloc0(sizeof(xk_pg_parser_sysdict_pgclass)
													   * class_list->length);
		cell = NULL;
		i = 0;
		foreach(cell, class_list)
			memcpy(&result->m_pg_class.m_pg_class[i++], lfirst(cell),
													  sizeof(xk_pg_parser_sysdict_pgclass));
		list_free_deep(class_list);
	}

	if (attributes_list)
	{
		result->m_pg_attribute.m_count = attributes_list->length;
		result->m_pg_attribute.m_pg_attributes = palloc0(sizeof(xk_pg_parser_sysdict_pgattributes)
													   * attributes_list->length);
		cell = NULL;
		i = 0;
		foreach(cell, attributes_list)
			memcpy(&result->m_pg_attribute.m_pg_attributes[i++], lfirst(cell),
													  sizeof(xk_pg_parser_sysdict_pgattributes));
		list_free_deep(attributes_list);
	}

	if (namespace_list)
	{
		result->m_pg_namespace.m_count = namespace_list->length;
		result->m_pg_namespace.m_pg_namespace = palloc0(sizeof(xk_pg_parser_sysdict_pgnamespace)
													   * namespace_list->length);
		cell = NULL;
		i = 0;
		foreach(cell, namespace_list)
			memcpy(&result->m_pg_namespace.m_pg_namespace[i++], lfirst(cell),
													  sizeof(xk_pg_parser_sysdict_pgnamespace));
		list_free_deep(namespace_list);
	}

	if (type_list)
	{
		result->m_pg_type.m_count = type_list->length;
		result->m_pg_type.m_pg_type = palloc0(sizeof(xk_pg_parser_sysdict_pgtype)
													   * type_list->length);
		cell = NULL;
		i = 0;
		foreach(cell, type_list)
			memcpy(&result->m_pg_type.m_pg_type[i++], lfirst(cell),
													  sizeof(xk_pg_parser_sysdict_pgtype));
		list_free_deep(type_list);
	}

	if (range_list)
	{
		result->m_pg_range.m_count = range_list->length;
		result->m_pg_range.m_pg_range = palloc0(sizeof(xk_pg_parser_sysdict_pgrange)
													   * range_list->length);
		cell = NULL;
		i = 0;
		foreach(cell, range_list)
			memcpy(&result->m_pg_range.m_pg_range[i++], lfirst(cell),
													  sizeof(xk_pg_parser_sysdict_pgrange));
		list_free_deep(range_list);
	}

	if (enum_list)
	{
		result->m_pg_enum.m_count = enum_list->length;
		result->m_pg_enum.m_pg_enum = palloc0(sizeof(xk_pg_parser_sysdict_pgenum)
													   * enum_list->length);
		cell = NULL;
		i = 0;
		foreach(cell, enum_list)
			memcpy(&result->m_pg_enum.m_pg_enum[i++], lfirst(cell),
													  sizeof(xk_pg_parser_sysdict_pgenum));
		list_free_deep(enum_list);
	}

	if (proc_list)
	{
		result->m_pg_proc.m_count = proc_list->length;
		result->m_pg_proc.m_pg_proc = palloc0(sizeof(xk_pg_parser_sysdict_pgproc)
													   * proc_list->length);
		cell = NULL;
		i = 0;
		foreach(cell, proc_list)
			memcpy(&result->m_pg_proc.m_pg_proc[i++], lfirst(cell),
													  sizeof(xk_pg_parser_sysdict_pgproc));
		list_free_deep(proc_list);
	}

	return result;
}

static char *get_output_name_by_typid(PGconn *conn, uint32_t oid)
{
	PGresult   *res;
	char sql_exec[1024] = {'\0'};
	char *result;
	char *temp = NULL;
	if (dbtype == XK_DATABASE_TYPE_UXDB)
		sprintf(sql_exec, "select typoutput from ux_type where oid = %u;", oid);
	else
		sprintf(sql_exec, "select typoutput from pg_type where oid = %u;", oid);
	res = PQexec(conn, sql_exec);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "failed in get_output_name_by_typid: %s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}
	temp = PQgetvalue(res, 0, 0);
	if (!temp)
	{
		fprintf(stderr, "failed in get_output_name_by_typid: %u", oid);
		PQclear(res);
		exit_nicely(conn);
	}
	result = pstrdup(temp);
	PQclear(res);
	return result;
}

#define XK_DATABASE_PG14 "pg14"
#define XK_DATABASE_PG15 "pg15"
#define XK_DATABASE_PG16 "pg16"

static char *get_chunkdata_from_list(uint32_t chunkid, uint32_t *chunk_len, xk_pg_parser_translog_tbcol_valuetype_external *ext)
{
	int i = 0;
	ListCell *cell = NULL;
	chunkcache * tempchunk = NULL;
	char *result = NULL;
	char *point;
	int len = 0;
	/* 默认是顺序放好的 */
	foreach(cell, chunkcache_list)
	{
		tempchunk = (chunkcache *)lfirst(cell);
		if (tempchunk->chunkid == chunkid)
		{
//			if (i == 0)
//				len += XK_PG_PARSER_VARSIZE_ANY(tempchunk->chunkdata);
//			else
				len += XK_PG_PARSER_VARSIZE_ANY_EXHDR(tempchunk->chunkdata);
			i++;
		}
	}
	len += VARHDRSZ;
	printf("len:%d\n", len);
	if (dbtype == XK_DATABASE_TYPE_POSTGRESQL && (!strcmp(dbversion, XK_DATABASE_PG14) || !strcmp(dbversion, XK_DATABASE_PG15) || !strcmp(dbversion, XK_DATABASE_PG16)))
	{
		printf("compress method: %s\n", (ext->m_extsize >> 30) == 0 ? "pglz" : ((ext->m_extsize >> 30) == 1) ? "lz4" : "WRONG METHOD");
		printf("m_extsize:%d\n", ext->m_extsize & ((1U << 30) - 1));

	}	
	else
	{
		printf("compress method: pglz\n");
		printf("m_extsize:%d\n", ext->m_extsize);
	}

	printf("m_rawsize:%d\n", ext->m_rawsize);
	result = palloc0(len);
	*chunk_len = len;
	point = result;

	printf("\n\nlen:%d, extsize:%d\n\n", len, ext->m_extsize);

	i = 0;
	foreach(cell, chunkcache_list)
	{
		tempchunk = (chunkcache *)lfirst(cell);
		if (tempchunk->chunkid == chunkid)
		{
			if (i == 0)
			{
				if (dbtype == XK_DATABASE_TYPE_POSTGRESQL && (!strcmp(dbversion, XK_DATABASE_PG14) || !strcmp(dbversion, XK_DATABASE_PG15) || !strcmp(dbversion, XK_DATABASE_PG16)))
				{
					if ((((ext)->m_extsize & ((1U << 30) - 1)) < (ext)->m_rawsize - ((int32_t) sizeof(int32_t))))
						(((varattrib_4b *) (result))->va_4byte.va_header = (((uint32) (len)) << 2) | 0x02);
					else
						(((varattrib_4b *) (result))->va_4byte.va_header = (((uint32) (len)) << 2));
				}
				else
				{
					if (XK_PG_PARSER_VARATT_EXTERNAL_IS_COMPRESSED(ext))
						(((varattrib_4b *) (result))->va_4byte.va_header = (((uint32) (len)) << 2) | 0x02);
					else
						(((varattrib_4b *) (result))->va_4byte.va_header = (((uint32) (len)) << 2));
				}
				point += VARHDRSZ;
				memcpy(point, tempchunk->chunkdata + (XK_PG_PARSER_VARSIZE_ANY(tempchunk->chunkdata) 
					   - XK_PG_PARSER_VARSIZE_ANY_EXHDR(tempchunk->chunkdata)), 
					   XK_PG_PARSER_VARSIZE_ANY_EXHDR(tempchunk->chunkdata));
				point += XK_PG_PARSER_VARSIZE_ANY_EXHDR(tempchunk->chunkdata);
			}

			else
			{
				memcpy(point, tempchunk->chunkdata + (XK_PG_PARSER_VARSIZE_ANY(tempchunk->chunkdata) 
					   - XK_PG_PARSER_VARSIZE_ANY_EXHDR(tempchunk->chunkdata)), 
					   XK_PG_PARSER_VARSIZE_ANY_EXHDR(tempchunk->chunkdata));
				point += XK_PG_PARSER_VARSIZE_ANY_EXHDR(tempchunk->chunkdata);
			}
			i++;
		}
	}
	return result;
}

static xk_pg_parser_translog_tbcol_value *get_de_toast_column(xk_pg_parser_translog_tbcol_value *col)
{
	const char *conninfo;
	PGconn	 *conn;
//	PGresult   *res;
//	char sql_exec[1024] = {'\0'};
//	char *temp = NULL;
	int ereno = 0;
	xk_pg_parser_translog_tbcol_value *result = NULL;
	xk_pg_parser_translog_convertinfo convert = {'\0'};
	xk_pg_parser_translog_external xk_pg_parser_exdata = {'\0'};
	xk_pg_parser_translog_tbcol_valuetype_external *ext = (xk_pg_parser_translog_tbcol_valuetype_external *)col->m_value;

	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}

	convert.m_dbcharset = pstrdup("UTF8");
	convert.m_tartgetcharset = pstrdup("UTF8");
	convert.m_tzname = pstrdup("Asia/Shanghai");
	convert.m_monetary = pstrdup("en_US.UTF-8");
	convert.m_numeric = pstrdup("en_US.UTF-8");

	xk_pg_parser_exdata.m_colName = col->m_colName;
	xk_pg_parser_exdata.m_dbtype = dbtype;
	xk_pg_parser_exdata.m_dbversion = dbversion;
	xk_pg_parser_exdata.m_convertInfo = &convert;
	xk_pg_parser_exdata.m_typeid = col->m_coltype;
	xk_pg_parser_exdata.m_typout = get_output_name_by_typid(conn, xk_pg_parser_exdata.m_typeid);
	xk_pg_parser_exdata.m_chunkdata = get_chunkdata_from_list(ext->m_valueid, &xk_pg_parser_exdata.m_datalen, ext);


	if (!xk_pg_parser_trans_external_trans(&xk_pg_parser_exdata, &result, &ereno))
	{
		fprintf(stderr, "failed detoast external %s",
		PQerrorMessage(conn));
		exit_nicely(conn);
	}
	PQfinish(conn);
	return result;
}
static void print_node(void* value)
{
	xk_pg_parser_nodetree *node = (xk_pg_parser_nodetree *)value;
	while (node)
	{
		switch (node->m_node_type)
		{
			case XK_PG_PARSER_NODETYPE_VAR:
			{
				xk_pg_parser_node_var *node_var = (xk_pg_parser_node_var *)node->m_node;
				printf(">%d<", node_var->m_attno);
				break;
			}
			case XK_PG_PARSER_NODETYPE_CONST:
			{
				xk_pg_parser_node_const *node_const = (xk_pg_parser_node_const *)node->m_node;
				if (node_const->m_char_value)
					printf("%s", node_const->m_char_value);
				break;
			}
			case XK_PG_PARSER_NODETYPE_FUNC:
			{
				xk_pg_parser_node_func *node_func = (xk_pg_parser_node_func *)node->m_node;
				if (node_func->m_funcname)
					printf("%s", node_func->m_funcname);
				else
					printf(">%d<", node_func->m_funcid);
				break;
			}
			case XK_PG_PARSER_NODETYPE_OP:
			{
				xk_pg_parser_node_op *node_op = (xk_pg_parser_node_op *)node->m_node;
				if (node_op->m_opname)
					printf("%s", node_op->m_opname);
				else
					printf(">%d<", node_op->m_opid);
				break;
			}
			case XK_PG_PARSER_NODETYPE_CHAR:
			{
				printf("%s", (char*)node->m_node);
				break;
			}
		}
		node = node->m_next;
	}
}
static void insert_trans_display(xk_pg_parser_translog_tbcol_values *trans_base)
{
	int i = 0;
	printf(">>>>>>>>>>>>>>>>>>>>begin trans insert<<<<<<<<<<<<<<<<<<<<\n");
	printf("DML TYPE INSERT:\n");
	printf("table name: %s.%s\n", trans_base->m_base.m_schemaname, trans_base->m_base.m_tbname);
	for (i = 0; i < trans_base->m_valueCnt; i++)
	{
		if (trans_base->m_new_values[i].m_info == INFO_COL_IS_NULL)
			printf("|----column[%d]: [%s]  values[>NULL<]\n", i, trans_base->m_new_values[i].m_colName);
		else if (trans_base->m_new_values[i].m_info == INFO_COL_MAY_NULL)
		{
			printf("|----column[%d]: [%s]  values[>MISSING<]\n", i, trans_base->m_new_values[i].m_colName);
		}
		else if (trans_base->m_new_values[i].m_info == INFO_COL_IS_BYTEA)
		{
			printf("|----column[%d]: [%s]  values[>BYTEA DATA<]\n", i, trans_base->m_new_values[i].m_colName);
		}
		else if (trans_base->m_new_values[i].m_info == INFO_COL_IS_TOAST)
		{
//			int count = 0;
			char *out = NULL;
			xk_pg_parser_translog_tbcol_value *detoast = NULL;
			detoast = get_de_toast_column(&trans_base->m_new_values[i]);
			out = (char*)detoast->m_value;
			printf("|----column[%d]: [%s]  values[%s]\n", i, detoast->m_colName, out);
			xk_pg_parser_trans_external_free(NULL, detoast);
		}
		else if (trans_base->m_new_values[i].m_info == INFO_COL_IS_CUSTOM)
		{
			printf("|----column[%d]: [%s]  values[>CUSTOM<]\n", i, trans_base->m_new_values[i].m_colName);
		}
		else if (trans_base->m_new_values[i].m_info == INFO_COL_IS_NODE)
		{
			printf("|----column[%d]: [%s]  values[", i, trans_base->m_new_values[i].m_colName);
			print_node(trans_base->m_new_values[i].m_value);
			printf("]\n");
		}
		else if (trans_base->m_new_values[i].m_info == INFO_COL_IS_DROPED)
		{
			//do nothing
		}
		else
			printf("|----column[%d]: [%s]  values[%s]\n", i, trans_base->m_new_values[i].m_colName,
												  (char *)trans_base->m_new_values[i].m_value);
	//xk_pg_parser_free_value_ext(&trans_base->m_new_values[i]);
	}
	printf(">>>>>>>>>>>>>>>>>>>> end trans insert <<<<<<<<<<<<<<<<<<<<\n\n\n");
}

static void multi_insert_trans_display(xk_pg_parser_translog_tbcol_nvalues *trans_base)
{
	int i = 0;
	int j = 0;
	printf(">>>>>>>>>>>>>>>>>>>>begin trans multi insert<<<<<<<<<<<<<<<<<<<<\n");
	printf("DML TYPE INSERT:\n");
	printf("table name: %s.%s\n", trans_base->m_base.m_schemaname, trans_base->m_base.m_tbname);
	for (i = 0; i < trans_base->m_rowCnt; i++)
	{
		printf("|-----row %d\n", i);
		for (j = 0; j < trans_base->m_valueCnt; j++)
		{
			if (trans_base->m_rows[i].m_new_values[j].m_info == INFO_COL_IS_NULL)
				printf("|     |----column[%d]: [%s]  values[>NULL<]\n", j, trans_base->m_rows[i].m_new_values[j].m_colName);
			else if (trans_base->m_rows[i].m_new_values[j].m_info == INFO_COL_IS_TOAST)
			{
//				int count = 0;
				char *out = NULL;
				xk_pg_parser_translog_tbcol_value *detoast = NULL;
				detoast = get_de_toast_column(&trans_base->m_rows[i].m_new_values[j]);
				out = (char*)detoast->m_value;
				printf("|     |----column[%d]: [%s]  values[%s]\n", i, detoast->m_colName, out);
				xk_pg_parser_trans_external_free(NULL, detoast);
			}
			else
				printf("|     |----column[%d]: [%s]  values[%s]\n", j, trans_base->m_rows[i].m_new_values[j].m_colName,
													  (char *)trans_base->m_rows[i].m_new_values[j].m_value);
		}
		printf("|\n");
	}
	printf(">>>>>>>>>>>>>>>>>>>> end trans multi insert <<<<<<<<<<<<<<<<<<<<\n\n\n");
}

static void delete_trans_display(xk_pg_parser_translog_tbcol_values *trans_base)
{
	int i = 0;
	printf(">>>>>>>>>>>>>>>>>>>>begin trans delete<<<<<<<<<<<<<<<<<<<<\n");
	printf("DML TYPE DELETE:\n");
	printf("table name: %s.%s\n", trans_base->m_base.m_schemaname, trans_base->m_base.m_tbname);
	for (i = 0; i < trans_base->m_valueCnt; i++)
	{
		if (trans_base->m_old_values[i].m_info == INFO_COL_IS_NULL)
			printf("|----column[%d]: [%s]  values[>NULL<]\n", i, trans_base->m_old_values[i].m_colName);
		else if (trans_base->m_old_values[i].m_info == INFO_COL_IS_DROPED
		 		|| trans_base->m_old_values[i].m_info == INFO_COL_MAY_NULL)
		{
			printf("|----column[%d]: [%s]  values[>MISSING<]\n", i, trans_base->m_old_values[i].m_colName);
		}
		else if (trans_base->m_old_values[i].m_info == INFO_COL_IS_BYTEA)
		{
			printf("|----column[%d]: [%s]  values[>BYTEA DATA<]\n", i, trans_base->m_old_values[i].m_colName);
		}
		else if (trans_base->m_old_values[i].m_info == INFO_COL_IS_TOAST)
		{
			//xk_pg_parser_translog_tbcol_value *detoast = NULL;
			//detoast = get_de_toast_column(&trans_base->m_old_values[i]);	
			//printf("|----column[%d]: [%s]  values[%s]\n", i, detoast->m_colName, (char*)detoast->m_value);
			printf("|----column[%d]: [%s]  values[>EXTERNAL DATA<]\n", i, trans_base->m_old_values[i].m_colName);
		}
		else if (trans_base->m_old_values[i].m_info == INFO_COL_IS_CUSTOM)
			printf("|----old column[%d]: [%s]  values[>CUSTOM<]\n", i, trans_base->m_old_values[i].m_colName);
		else if (trans_base->m_old_values[i].m_info == INFO_COL_IS_NODE)
			printf("|----old column[%d]: [%s]  values[>NODE<]\n", i, trans_base->m_old_values[i].m_colName);
		else
			printf("|----column[%d]: [%s]  values[%s]\n", i, trans_base->m_old_values[i].m_colName,
												  (char *)trans_base->m_old_values[i].m_value);
	}
	printf(">>>>>>>>>>>>>>>>>>>> end trans delete <<<<<<<<<<<<<<<<<<<<\n\n\n");
}

static void update_trans_display(xk_pg_parser_translog_tbcol_values *trans_base)
{
	int i = 0;
	printf(">>>>>>>>>>>>>>>>>>>>begin trans update<<<<<<<<<<<<<<<<<<<<\n");
	printf("DML TYPE UPDATE:\n");
	printf("table name: %s.%s\n", trans_base->m_base.m_schemaname, trans_base->m_base.m_tbname);
	for (i = 0; i < trans_base->m_valueCnt; i++)
	{
		if (trans_base->m_old_values[i].m_info == INFO_COL_IS_NULL)
			printf("|----old column[%d]: [%s]  values[>NULL<]\n", i, trans_base->m_old_values[i].m_colName);
		else if (trans_base->m_old_values[i].m_info == INFO_COL_IS_TOAST
			  || trans_base->m_old_values[i].m_info == INFO_COL_IS_CUSTOM
			  || trans_base->m_old_values[i].m_info == INFO_COL_IS_ARRAY
			  || trans_base->m_old_values[i].m_info == INFO_COL_MAY_NULL
			  || trans_base->m_old_values[i].m_info == INFO_COL_IS_DROPED
			  || trans_base->m_old_values[i].m_info == INFO_COL_IS_BYTEA
			  || trans_base->m_old_values[i].m_info == INFO_COL_IS_NODE)
			printf("|----old column[%d]: [%s]  values[>CAN'T DISPLAY<]\n", i, trans_base->m_old_values[i].m_colName);
		else
			printf("|----old column[%d]: [%s]  values[%s]\n", i, trans_base->m_old_values[i].m_colName,
												  (char *)trans_base->m_old_values[i].m_value);
	}
	printf("|\n|\n");
	for (i = 0; i < trans_base->m_valueCnt; i++)
	{
		if (trans_base->m_new_values[i].m_info == INFO_COL_IS_NULL)
			printf("|----new column[%d]: [%s]  values[>NULL<]\n", i, trans_base->m_new_values[i].m_colName);
		else if (trans_base->m_new_values[i].m_info == INFO_COL_IS_CUSTOM
			  || trans_base->m_new_values[i].m_info == INFO_COL_IS_ARRAY
			  || trans_base->m_new_values[i].m_info == INFO_COL_MAY_NULL
			  || trans_base->m_new_values[i].m_info == INFO_COL_IS_DROPED
			  || trans_base->m_new_values[i].m_info == INFO_COL_IS_BYTEA
			  || trans_base->m_new_values[i].m_info == INFO_COL_IS_NODE)
			printf("|----new column[%d]: [%s]  values[>CAN'T DISPLAY<]\n", i, trans_base->m_new_values[i].m_colName);
		else if (trans_base->m_new_values[i].m_info == INFO_COL_IS_TOAST)
		{
//			int count = 0;
			char *out = NULL;
			xk_pg_parser_translog_tbcol_value *detoast = NULL;
			detoast = get_de_toast_column(&trans_base->m_new_values[i]);
			out = (char*)detoast->m_value;
			printf("|----column[%d]: [%s]  values[%s]\n", i, detoast->m_colName, out);
			xk_pg_parser_trans_external_free(NULL, detoast);
		}
		else
			printf("|----new column[%d]: [%s]  values[%s]\n", i, trans_base->m_new_values[i].m_colName,
												  (char *)trans_base->m_new_values[i].m_value);
	}
	printf(">>>>>>>>>>>>>>>>>>>> end trans update <<<<<<<<<<<<<<<<<<<<\n\n\n");
}


static void storage_tuple(uint32_t relfilenode, xk_pg_parser_translog_tuplecache *tuple, uint32_t count)
{
	tupcache *tuple_storage = NULL;
	int i = 0;

	for (i = 0; i < count; i++)
	{
		tuple_storage = get_tuple_from_cache(relfilenode, tuple[i].m_pageno, tuple[i].m_itemoffnum);
		if (!tuple_storage)
		{
			if (enable_tuple_debug)
				printf("storage tuple, page relfilenode: [%u], no[%u],"
					   "tuple offset:[%u]\n", relfilenode,
											  tuple[i].m_pageno,
											  tuple[i].m_itemoffnum);
			tuple_storage = palloc0(sizeof(tupcache));
			tuple_storage->relfilenode = relfilenode;
			tuple_storage->pageno = tuple[i].m_pageno;
			tuple_storage->len = tuple[i].m_tuplelen;
			tuple_storage->itemoff = tuple[i].m_itemoffnum;
			tuple_storage->data = palloc0(tuple_storage->len);
			memcpy(tuple_storage->data, tuple[i].m_tupledata, tuple_storage->len);
			tuplecache_list =  lappend(tuplecache_list, tuple_storage);
		}
		else
		{
			if (enable_tuple_debug)
				printf("already have cache, restore, page relfilenode: [%u], no[%u],"
					   "tuple offset:[%u]\n", relfilenode,
											  tuple[i].m_pageno,
											  tuple[i].m_itemoffnum);
			tuple_storage->len = tuple[i].m_tuplelen;
			pfree(tuple_storage->data);
			tuple_storage->data = palloc0(tuple_storage->len);
			memcpy(tuple_storage->data, tuple[i].m_tupledata, tuple_storage->len);
		}

	}
}

static tupcache *get_tuple_from_cache(uint32_t relfilenode, uint32_t pageno, uint32_t itemoff)
{
	tupcache *result = NULL,
			  *temp_tup = NULL;
	ListCell *cell = NULL;
	foreach(cell, tuplecache_list)
	{
		temp_tup = (tupcache *)lfirst(cell);
		if (temp_tup->pageno == pageno
			&& temp_tup->relfilenode == relfilenode
			&& temp_tup->itemoff == itemoff)
		{
			result = temp_tup;
			return result;
		}
	}
	return NULL;
}

static void get_tuples(xk_pg_parser_translog_translog2col *trans_data,
					  xk_pg_parser_translog_pre_heap *heap_pre)
{
	tupcache *tuple_temp = NULL;
	uint32_t pageno = heap_pre->m_pagenos;
	uint32_t relfilenode = heap_pre->m_relfilenode;
	uint32_t itemoff = heap_pre->m_tupitemoff;
	tuple_temp = get_tuple_from_cache(relfilenode, pageno, itemoff);
	if (tuple_temp == NULL)
	{
		printf("can't get page cache, relfilenode: %u, no: %u, itemoff:%u", relfilenode, pageno, itemoff);
		exit(1);
	}
	trans_data->m_tuples->m_pageno = pageno;
	trans_data->m_tuples->m_tupledata = palloc0(tuple_temp->len);
	memcpy(trans_data->m_tuples->m_tupledata, tuple_temp->data, tuple_temp->len);
	trans_data->m_tuples->m_itemoffnum = itemoff;
	trans_data->m_tuples->m_tuplelen = tuple_temp->len;
}
#if 0
static void storage_page(xk_pg_parser_translog_page *pages)
{
	pagecache *page = palloc0(sizeof(pagecache));
	PageHeader phdr = NULL;
	printf("storage page, relfilenode: [%u], no[%u]\n", pages->m_relfilenode, pages->m_pageno);
	page->pageno = pages->m_pageno;
	page->relfilenode = pages->m_relfilenode;
	memcpy(page->page, pages->m_page, 8192);
	phdr = (PageHeader) page->page;
	printf("page lower: [%d], upper:[%d]\n", phdr->pd_lower, phdr->pd_upper);
	free(pages->m_page);
	pages->m_page = NULL;
	pagecache_list = lappend(pagecache_list, page);
}

static void get_pages(xk_pg_parser_translog_translog2col *trans_data,
					  xk_pg_parser_translog_pre_heap *heap_pre)
{
	pagecache *temp_page_cache = NULL;
	int i = 0;
	for (i = 0; i < heap_pre->m_pagecnt; i ++)
	{
		uint32_t pageno = heap_pre->m_pagenos[i];
		uint32_t relfilenode = heap_pre->m_relfilenode;
		temp_page_cache = get_page_from_cache(relfilenode, pageno);
		if (temp_page_cache == NULL)
		{
			printf("can't get page cache, relfilenode: %u, no: %u", relfilenode, pageno);
			exit(1);
		}
		trans_data->m_pages[i].m_pageno = pageno;
		trans_data->m_pages[i].m_relfilenode = relfilenode;
		trans_data->m_pages[i].m_page = (uint8_t *)temp_page_cache->page;
	}
}

static pagecache *get_page_from_cache(uint32_t relfilenode, uint32_t pageno)
{
	pagecache *result = NULL,
			  *temp_page = NULL;
	ListCell *cell = NULL;
	foreach(cell, pagecache_list)
	{
		temp_page = (pagecache *)lfirst(cell);
		if (temp_page->pageno == pageno
			&& temp_page->relfilenode == relfilenode)
		{
			result = temp_page;
			break;
		}
	}
	return result;
}

#define VALGRIND_CHECK_MEM_IS_DEFINED(addr, size)			do {} while (0)
/* 这里去除了很多的检查项, 因为我们不处理回收, 检查已经由pg做过一遍了, 这里只需要应用 */
static OffsetNumber
PageAddItemExtended(Page page,
					Item item,
					Size size,
					OffsetNumber offsetNumber,
					int flags)
{
	PageHeader	phdr = (PageHeader) page;
	Size		alignedSize;
	int			lower;
	int			upper;
	ItemId		itemId;
	OffsetNumber limit;
	bool		needshuffle = false;

	/*
	 * Be wary about corrupted page pointers
	 */
	if (phdr->pd_lower < SizeOfPageHeaderData ||
		phdr->pd_lower > phdr->pd_upper ||
		phdr->pd_upper > phdr->pd_special ||
		phdr->pd_special > BLCKSZ)
		{
			printf("error1\n");
			exit(1);
		}

	/*
	 * Select offsetNumber to place the new item at
	 */
	limit = OffsetNumberNext(PageGetMaxOffsetNumber(page));
	printf("limit[%u]\n", limit);
	/* was offsetNumber passed in? */
	if (OffsetNumberIsValid(offsetNumber))
	{
		/* yes, check it */
		if ((flags & PAI_OVERWRITE) != 0)
		{
			if (offsetNumber < limit)
			{
				itemId = PageGetItemId(phdr, offsetNumber);
				if (ItemIdIsUsed(itemId) || ItemIdHasStorage(itemId))
				{
					printf("will not overwrite a used ItemId\n");
					exit(1);
				}
			}
		}
		else
		{
			if (offsetNumber < limit)
				needshuffle = true; /* need to move existing linp's */
		}
	}
	else
	{
		/* offsetNumber was not passed in, so find a free slot */
		/* if no free slot, we'll put it at limit (1st open slot) */
		if (PageHasFreeLinePointers(phdr))
		{
			/*
			 * Look for "recyclable" (unused) ItemId.  We check for no storage
			 * as well, just to be paranoid --- unused items should never have
			 * storage.
			 */
			for (offsetNumber = 1; offsetNumber < limit; offsetNumber++)
			{
				itemId = PageGetItemId(phdr, offsetNumber);
				if (!ItemIdIsUsed(itemId) && !ItemIdHasStorage(itemId))
					break;
			}
			if (offsetNumber >= limit)
			{
				/* the hint is wrong, so reset it */
				PageClearHasFreeLinePointers(phdr);
			}
		}
		else
		{
			/* don't bother searching if hint says there's no free slot */
			offsetNumber = limit;
		}
	}

	/* Reject placing items beyond the first unused line pointer */
	if (offsetNumber > limit)
	{
		printf("specified item offset is too large\n");
		exit(1);
	}

	/* Reject placing items beyond heap boundary, if heap */
	if ((flags & PAI_IS_HEAP) != 0 && offsetNumber > MaxHeapTuplesPerPage)
	{
		printf("can't put more than MaxHeapTuplesPerPage items in a heap page\n");
		exit(1);
	}

	/*
	 * Compute new lower and upper pointers for page, see if it'll fit.
	 *
	 * Note: do arithmetic as signed ints, to avoid mistakes if, say,
	 * alignedSize > pd_upper.
	 */
	if (offsetNumber == limit || needshuffle)
		lower = phdr->pd_lower + sizeof(ItemIdData);
	else
		lower = phdr->pd_lower;

	alignedSize = MAXALIGN(size);

	upper = (int) phdr->pd_upper - (int) alignedSize;
	printf("origin lower:%d, upper:%d\n", phdr->pd_lower, phdr->pd_upper);
	printf("offset number [%d], size[%u][%u], lower:%d, upper:%d\n", offsetNumber, size, alignedSize, lower, upper);

	if (lower > upper)
	{
		printf("lower > upper!\n");
		exit(1);
	};

	/*
	 * OK to insert the item.  First, shuffle the existing pointers if needed.
	 */
	itemId = PageGetItemId(phdr, offsetNumber);

	if (needshuffle)
		memmove(itemId + 1, itemId,
				(limit - offsetNumber) * sizeof(ItemIdData));

	/* set the line pointer */
	ItemIdSetNormal(itemId, upper, size);

	/*
	 * Items normally contain no uninitialized bytes.  Core bufpage consumers
	 * conform, but this is not a necessary coding rule; a new index AM could
	 * opt to depart from it.  However, data type input functions and other
	 * C-language functions that synthesize datums should initialize all
	 * bytes; datumIsEqual() relies on this.  Testing here, along with the
	 * similar check in printtup(), helps to catch such mistakes.
	 *
	 * Values of the "name" type retrieved via index-only scans may contain
	 * uninitialized bytes; see comment in btrescan().  Valgrind will report
	 * this as an error, but it is safe to ignore.
	 */
	VALGRIND_CHECK_MEM_IS_DEFINED(item, size);

	/* copy the item's data onto the page */
	memcpy((char *) page + upper, item, size);

	/* adjust page header */
	phdr->pd_lower = (LocationIndex) lower;
	phdr->pd_upper = (LocationIndex) upper;

	return offsetNumber;
}
static OffsetNumber PageApplyItem(Page page,
										Item item,
										Size size,
										OffsetNumber offsetNumber,
										int flags)
{
	PageHeader	phdr = (PageHeader) page;
	Size		alignedSize;
	int			lower;
	int			upper;
	ItemId		itemId;
	bool		overwrite = false;
	bool		normal_add = false;
	/*
	 * Select offsetNumber to place the new item at
	 */

	/* was offsetNumber passed in? */
	if (OffsetNumberIsValid(offsetNumber))
	{
		/* don't check, just overwrite */
		itemId = PageGetItemId(phdr, offsetNumber);
		if (ItemIdIsUsed(itemId) || ItemIdHasStorage(itemId))
		{
			overwrite = true;
		}
	}
	else
	{
		printf("add item have invalid offset number\n");
		exit(1);
	}

	if (overwrite)
	{
		uint32_t old_off = itemId->lp_off;
		if (itemId->lp_len < size)
		{
			printf("can't rewrite item, bescause old len < new len\n");
			exit(1);
		}
		printf("old item off[%d], len[%d]\n", itemId->lp_off, itemId->lp_len);
		ItemIdSetNormal(itemId, old_off, size);
		printf("new item off[%d], len[%d]\n", itemId->lp_off, size);
		memcpy((char *) page + old_off, item, size);
	}
	else
	{
		//if (offsetNumber != 1)
		//{
		//	ItemId old_item = PageGetItemId(phdr, offsetNumber - 1);
		//	if (ItemIdIsUsed(old_item) || ItemIdHasStorage(old_item))
		//	{
		//		uint32_t off = 0;
		//		alignedSize = MAXALIGN(size);
		//		off = old_item->lp_off - alignedSize;
		//		upper = (int) off;
		//		lower = phdr->pd_lower + sizeof(ItemIdData);
		//	}
		//	else
		//		normal_add = true;
//
		//	alignedSize = MAXALIGN(size);
		//	
		//}
		//if (offsetNumber == 1 || normal_add)
		//{
			lower = phdr->pd_lower + sizeof(ItemIdData);
			alignedSize = MAXALIGN(size);
			upper = (int) phdr->pd_upper - (int) alignedSize;
		//}

		printf("offset number [%d], size[%u][%u], lower:%d, upper:%d\n", offsetNumber, size, alignedSize, lower, upper);
		/*
		 * OK to insert the item.  First, shuffle the existing pointers if needed.
		 */
		itemId = PageGetItemId(phdr, offsetNumber);

		/* set the line pointer */
		ItemIdSetNormal(itemId, upper, size);

		/* copy the item's data onto the page */
		memcpy((char *) page + upper, item, size);

		/* adjust page header */
		phdr->pd_lower = (LocationIndex) lower;
		phdr->pd_upper = (LocationIndex) upper;
	}
	return offsetNumber;
}
#endif
static xk_pg_parser_translog_ddlstmt *trans_ddl(List *list)
{
	ListCell *cell = NULL;
	xk_pg_parser_translog_systb2ddl *ddl = palloc0(sizeof(xk_pg_parser_translog_systb2ddl));
	xk_pg_parser_translog_systb2dll_record *ddl_record_head = NULL;
	xk_pg_parser_translog_systb2dll_record *ddl_record_tail = NULL;
	xk_pg_parser_translog_tbcol_values *dml_trans = NULL;
	xk_pg_parser_translog_ddlstmt *result = NULL;
	int32_t errnos = 0;
	int num = 1;
	xk_pg_parser_translog_convertinfo convert = {'\0'};
	convert.m_dbcharset = pstrdup("UTF8");
	convert.m_tartgetcharset = pstrdup("UTF8");
	convert.m_tzname = pstrdup("Asia/Shanghai");
	convert.m_monetary = pstrdup("en_US.UTF-8");
	convert.m_numeric = pstrdup("en_US.UTF-8");
	ddl->m_convert = &convert;

	foreach(cell, list)
	{
		dml_trans = (xk_pg_parser_translog_tbcol_values *)lfirst(cell);
		if(dml_trans->m_base.m_dmltype == XK_PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT)
		{
			xk_pg_parser_translog_tbcol_nvalues *dml_trans_multi = (xk_pg_parser_translog_tbcol_nvalues *)dml_trans;
			int i = 0;
			for (i = 0; i < dml_trans_multi->m_rowCnt; i++)
			{
				xk_pg_parser_translog_systb2dll_record *ddl_record_current = palloc0(sizeof(xk_pg_parser_translog_systb2dll_record));
				xk_pg_parser_translog_tbcol_values *dml_multichange = palloc0(sizeof(xk_pg_parser_translog_tbcol_values));
				dml_multichange->m_base.m_dmltype = XK_PG_PARSER_TRANSLOG_DMLTYPE_INSERT;
				dml_multichange->m_base.m_originid = dml_trans_multi->m_base.m_originid;
				dml_multichange->m_base.m_schemaname = dml_trans_multi->m_base.m_schemaname;
				dml_multichange->m_base.m_tabletype = dml_trans_multi->m_base.m_tabletype;
				dml_multichange->m_base.m_tbname = dml_trans_multi->m_base.m_tbname;
				dml_multichange->m_base.m_type = dml_trans_multi->m_base.m_type;
				dml_multichange->m_new_values = dml_trans_multi->m_rows[i].m_new_values;
				dml_multichange->m_relfilenode = dml_trans_multi->m_relfilenode;
				dml_multichange->m_relid = dml_trans_multi->m_relid;
				dml_multichange->m_valueCnt = dml_trans_multi->m_valueCnt;
				ddl_record_current->m_record = dml_multichange;
				if (!ddl_record_tail)
					ddl_record_head = ddl_record_tail = ddl_record_current;
				else
				{
					ddl_record_tail->m_next = ddl_record_current;
					ddl_record_tail = ddl_record_tail->m_next;
				}
			}
		}
		else
		{
			xk_pg_parser_translog_systb2dll_record *ddl_record_current = palloc0(sizeof(xk_pg_parser_translog_systb2dll_record));
			ddl_record_current->m_record = dml_trans;
			if (!ddl_record_tail)
				ddl_record_head = ddl_record_tail = ddl_record_current;
			else
			{
				ddl_record_tail->m_next = ddl_record_current;
				ddl_record_tail = ddl_record_tail->m_next;
			}
		}
	}
	ddl->m_debugLevel = enable_trans_debug;
	ddl->m_record = ddl_record_head;
	ddl->m_dbtype = dbtype;
	ddl->m_dbversion = dbversion;
	while (ddl_record_head && enable_trans_debug)
	{
		printf("ddl_list:\n");
		printf("[%d]: ", num++);
		switch (ddl_record_head->m_record->m_base.m_dmltype)
		{
			case XK_PG_PARSER_TRANSLOG_DMLTYPE_INSERT:
				printf("INSERT  ");
				break;
			case XK_PG_PARSER_TRANSLOG_DMLTYPE_DELETE:
				printf("DELETE  ");
				break;
			case XK_PG_PARSER_TRANSLOG_DMLTYPE_UPDATE:
				printf("UPDATE  ");
				break;
			case XK_PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT:
				printf("MINSERT ");
				break;
		}
		printf(">%s<\n", ddl_record_head->m_record->m_base.m_tbname);
		ddl_record_head = ddl_record_head->m_next;
	}
	if (!xk_pg_parser_trans_DDLtrans(ddl, &result, &errnos))
	{
		printf("can't trans ddl!\n");
		printf("error:%s\n", xk_pg_parser_errno_getErrInfo(errnos));
		exit(1);
	}
	//xk_pg_parser_trans_ddl_free(ddl, NULL);
	return result;
}

static char *get_typename_by_oid(PGconn *conn, uint32_t oid)
{
	PGresult   *res;
	char sql_exec[1024] = {'\0'};
	char *result;
//	char *check_catalog = NULL;
	char *temp = NULL;
	if (dbtype == XK_DATABASE_TYPE_UXDB)
		sprintf(sql_exec, "select typname from ux_type where oid = %u;", oid);
	else
		sprintf(sql_exec, "select typname from pg_type where oid = %u;", oid);
	res = PQexec(conn, sql_exec);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "failed in get typname: %s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}
	temp = PQgetvalue(res, 0, 0);
	if (!temp)
	{
		fprintf(stderr, "failed in get_typename_by_oid, oid[%u]", oid);
		PQclear(res);
		exit_nicely(conn);
	}
	result = pstrdup(temp);
	PQclear(res);
	return result;
}

static char *get_namespace_name_by_oid(PGconn *conn, uint32_t oid)
{
	PGresult   *res;
	char sql_exec[1024] = {'\0'};
	char *result;
	char *temp = NULL;
	if (dbtype == XK_DATABASE_TYPE_UXDB)
		sprintf(sql_exec, "select nspname from ux_namespace where oid = %u;", oid);
	else
		sprintf(sql_exec, "select nspname from pg_namespace where oid = %u;", oid);
	res = PQexec(conn, sql_exec);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "failed in get nspname: %s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}
	temp = PQgetvalue(res, 0, 0);
	if (!temp)
	{
		fprintf(stderr, "failed in get_namespace_name_by_oid: %u", oid);
		PQclear(res);
		exit_nicely(conn);
	}
	result = pstrdup(temp);
	PQclear(res);
	return result;
}

static char *get_namespace_name_by_reloid(PGconn *conn, uint32_t oid)
{
	PGresult   *res;
	char sql_exec[1024] = {'\0'};
	char *result;
	char *temp = NULL;
	if (dbtype == XK_DATABASE_TYPE_UXDB)
		sprintf(sql_exec, "select nspname from ux_namespace where oid = (select relnamespace from ux_class where oid = %u);", oid);
	else
		sprintf(sql_exec, "select nspname from pg_namespace where oid = (select relnamespace from pg_class where oid = %u);", oid);
	res = PQexec(conn, sql_exec);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "failed in get nspname: %s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}
	temp = PQgetvalue(res, 0, 0);
	if (!temp)
	{
		fprintf(stderr, "failed in get_namespace_name_by_reloid: %u", oid);
		PQclear(res);
		exit_nicely(conn);
	}
	result = pstrdup(temp);
	PQclear(res);
	return result;
}

static char *get_relname_by_oid(PGconn *conn, uint32_t oid)
{
	PGresult   *res;
	char sql_exec[1024] = {'\0'};
	char *result;
	char *temp = NULL;
	char cache[20] = {'\0'};
	if (dbtype == XK_DATABASE_TYPE_UXDB)
		sprintf(sql_exec, "select relname from ux_class where oid = %u;", oid);
	else
		sprintf(sql_exec, "select relname from pg_class where oid = %u;", oid);
	res = PQexec(conn, sql_exec);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "failed in get nspname: %s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}
	if (PQntuples(res) == 0)
	{
		sprintf(cache, "%u", oid);
		result = pstrdup(cache);
	}
	else
	{
		temp = PQgetvalue(res, 0, 0);
		if (!temp)
		{
			sprintf(cache, "%u", oid);
			result = pstrdup(cache);
		}
		else
			result = pstrdup(temp);
	}
	PQclear(res);
	return result;
}

static char *get_index_typname_by_oid(PGconn *conn, uint32_t oid)
{
	PGresult   *res;
	char sql_exec[1024] = {'\0'};
	char *result;
	char *temp = NULL;
	if (dbtype == XK_DATABASE_TYPE_UXDB)
		sprintf(sql_exec, "select amname from ux_am where oid = %u;", oid);
	else
		sprintf(sql_exec, "select amname from pg_am where oid = %u;", oid);
	res = PQexec(conn, sql_exec);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "failed in get nspname: %s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}
	temp = PQgetvalue(res, 0, 0);
	if (!temp)
	{
		fprintf(stderr, "failed in get_index_typname_by_oid: %u", oid);
		PQclear(res);
		exit_nicely(conn);
	}
	result = pstrdup(temp);
	PQclear(res);
	return result;
}

static char *get_colname_by_oid(PGconn *conn, uint32_t oid, uint16 attnum)
{
	PGresult   *res;
	char sql_exec[1024] = {'\0'};
	char *result;
	char *temp = NULL;
	if (dbtype == XK_DATABASE_TYPE_UXDB)
		sprintf(sql_exec, "select attname from ux_attribute where attrelid = %u and attnum = %u;", oid, attnum);
	else
		sprintf(sql_exec, "select attname from pg_attribute where attrelid = %u and attnum = %u;", oid, attnum);
	res = PQexec(conn, sql_exec);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "failed in get nspname: %s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}
	temp = PQgetvalue(res, 0, 0);
	if (!temp)
	{
		fprintf(stderr, "failed in get_colname_by_oid: %u", oid);
		PQclear(res);
		exit_nicely(conn);
	}
	result = pstrdup(temp);
	PQclear(res);
	return result;
}

static char *get_opclassname_by_oid(PGconn *conn, uint32_t oid)
{
	PGresult   *res;
	char sql_exec[1024] = {'\0'};
	char *result;
	char *temp = NULL;
	if (dbtype == XK_DATABASE_TYPE_UXDB)
		sprintf(sql_exec, "select opcname from ux_opclass where oid = %u;", oid);
	else
		sprintf(sql_exec, "select opcname from pg_opclass where oid = %u;", oid);
	res = PQexec(conn, sql_exec);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "failed in get nspname: %s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}
	temp = PQgetvalue(res, 0, 0);
	if (!temp)
	{
		fprintf(stderr, "failed in get_opclassname_by_oid: %u", oid);
		PQclear(res);
		exit_nicely(conn);
	}
	result = pstrdup(temp);
	PQclear(res);
	return result;
}

static void display_create_table(xk_pg_parser_translog_ddlstmt *ddl_trans)
{
	xk_pg_parser_translog_ddlstmt_createtable *table = (xk_pg_parser_translog_ddlstmt_createtable *)
														ddl_trans->m_ddlstmt;
	const char *conninfo;
	PGconn	 *conn;
//	PGresult   *res;
//	char sql_exec[1024] = {'\0'};
	char *nspname = NULL;
	int i = 0;

	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}
	nspname = get_namespace_name_by_oid(conn, table->m_nspoid);

	printf("DDL TYPE: CREATE TABLE\n");
	printf("ddl: create ");
	if (table->m_logtype == XK_PG_PARSER_DDL_TABLE_LOG_TEMP)
		printf("temp ");
	else if (table->m_logtype == XK_PG_PARSER_DDL_TABLE_LOG_UNLOGGED)
		printf("unlogged ");
	else
	{
		//do nothing
	}
	printf("table %s.%s ", nspname, table->m_tabname);
	if (table->m_tableflag == XK_PG_PARSER_DDL_TABLE_FLAG_EMPTY)
	{
		printf("()");
	}
	else
	{
		if (table->m_tabletype == XK_PG_PARSER_DDL_TABLE_TYPE_NORMAL
		|| table->m_tabletype == XK_PG_PARSER_DDL_TABLE_TYPE_PARTITION)
		{
			printf("(");
			for (i = 0; i < table->m_colcnt; i ++)
			{
				char *typname = get_typename_by_oid(conn, table->m_cols[i].m_coltypid);
				if (table->m_cols[i].m_flag & XK_PG_PARSER_DDL_COLUMN_NOTNULL)
					printf("%s %s not null", table->m_cols[i].m_colname, typname);
				else
				{
					if (table->m_cols[i].m_length > 0)
						printf("%s %s(%d)", table->m_cols[i].m_colname, typname, table->m_cols[i].m_length);
					else if (table->m_cols[i].m_precision > 0 && table->m_cols[i].m_scale < 0)
						printf("%s %s(%d)", table->m_cols[i].m_colname, typname, table->m_cols[i].m_precision);
					else if (table->m_cols[i].m_precision > 0 && table->m_cols[i].m_scale >= 0)
						printf("%s %s(%d, %d)", table->m_cols[i].m_colname,
												typname,
												table->m_cols[i].m_precision,
												table->m_cols[i].m_scale);
					else
						printf("%s %s", table->m_cols[i].m_colname, typname);
				}
				if (table->m_cols[i].m_default)
				{
					printf(" default(");
					print_node_ddl(table->m_cols[i].m_default, conn, table->m_relid, 0);
					printf(")");
				}

				if (i != table->m_colcnt - 1)
					printf(", ");
				pfree(typname);
			}
			printf(")");
		}
	}
	if (table->m_inherits_cnt > 0)
	{
		char * temp_relname = NULL;
		printf(" inherits (");
		for (i = 0; i < table->m_inherits_cnt; i++)
		{
			temp_relname = get_relname_by_oid(conn, table->m_inherits[i]);
			printf("%s", temp_relname);
			if (i != table->m_inherits_cnt - 1)
				printf(", ");
		}
		printf(")");
	}
	if (table->m_tabletype == XK_PG_PARSER_DDL_TABLE_TYPE_PARTITION_SUB
	 || table->m_tabletype == XK_PG_PARSER_DDL_TABLE_TYPE_PARTITION_BOTH)
	{
		char *relname = get_relname_by_oid(conn, table->m_partitionof->m_partitionof_table_oid);
		printf(" partition of %s ", relname);
		print_node_ddl(table->m_partitionof->m_partitionof_node, conn, table->m_relid, 0);
	}
	if (table->m_tabletype == XK_PG_PARSER_DDL_TABLE_TYPE_PARTITION
	 || table->m_tabletype == XK_PG_PARSER_DDL_TABLE_TYPE_PARTITION_BOTH)
	{
		int count = 1;
		printf(" partition by ");
		if (table->m_partitionby->m_partition_type == XK_PG_PARSER_DDL_PARTITION_TABLE_HASH)
			printf("hash ");
		else if (table->m_partitionby->m_partition_type == XK_PG_PARSER_DDL_PARTITION_TABLE_RANGE)
			printf("range ");
		else if (table->m_partitionby->m_partition_type == XK_PG_PARSER_DDL_PARTITION_TABLE_LIST)
			printf("list ");
		printf("(");
		for (i = 0; i < table->m_partitionby->m_column_num; i++)
		{
			if (table->m_partitionby->m_column[i] != 0)
				printf("%s", table->m_cols[table->m_partitionby->m_column[i] - 1].m_colname);
			else
				print_node_ddl(table->m_partitionby->m_colnode, conn, table->m_relid, count++);
			if (i != table->m_partitionby->m_column_num - 1)
				printf(", ");
		}
		printf(")");
	}
	printf(";\n");
	pfree(nspname);
	PQfinish(conn);
}

static void display_create_namespace(xk_pg_parser_translog_ddlstmt *ddl_trans)
{
	xk_pg_parser_translog_ddlstmt_valuebase *schema = (xk_pg_parser_translog_ddlstmt_valuebase *)
														ddl_trans->m_ddlstmt;
	const char *conninfo;
	PGconn	 *conn;
//	PGresult   *res;
//	char sql_exec[1024] = {'\0'};
//	int i = 0;

	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}

	printf("DDL TYPE: CREATE NAMESPACE\n");
	printf("ddl: create schema %s;\n", schema->m_value);

	PQfinish(conn);
}

static void display_drop_namespace(xk_pg_parser_translog_ddlstmt *ddl_trans)
{
	xk_pg_parser_translog_ddlstmt_valuebase *schema = (xk_pg_parser_translog_ddlstmt_valuebase *)
														ddl_trans->m_ddlstmt;
	const char *conninfo;
	PGconn	 *conn;
//	PGresult   *res;
//	char sql_exec[1024] = {'\0'};
//	int i = 0;

	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}

	printf("DDL TYPE: DROP NAMESPACE\n");
	printf("ddl: drop schema %s;\n", schema->m_value);

	PQfinish(conn);
}

static void display_create_index(xk_pg_parser_translog_ddlstmt *ddl_trans)
{
	xk_pg_parser_translog_ddlstmt_index *index = (xk_pg_parser_translog_ddlstmt_index *)
														ddl_trans->m_ddlstmt;
	const char *conninfo;
	PGconn	 *conn;
//	PGresult   *res;
//	char sql_exec[1024] = {'\0'};
	int i = 0;
	char *relname = NULL;
	char *indextype = NULL;
	int count = 1;

	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}
	relname = get_relname_by_oid(conn, index->m_relid);
	indextype = get_index_typname_by_oid(conn, index->m_indtype);
	printf("DDL TYPE: CREATE INDEX\n");
	printf("ddl: create");
	if (index->m_option & XK_PG_PARSER_DDL_INDEX_UNIQUE)
		printf(" unique");
	printf(" index %s on %s using %s (", index->m_indname, relname, indextype);
	for (i = 0; i < index->m_colcnt; i++)
	{
		if (index->m_column[i] > 0)
			printf("%s", index->m_includecols[index->m_column[i] - 1].m_colname);
		else
		{
			print_node_ddl(index->m_colnode, conn, index->m_relid, count++);
		}
		if (i < index->m_colcnt - 1)
			printf(", ");
	}
	printf(");\n");
	pfree(relname);
	pfree(indextype);
	PQfinish(conn);
}

static void display_create_sequence(xk_pg_parser_translog_ddlstmt *ddl_trans)
{
	xk_pg_parser_translog_ddlstmt_sequence *seq = (xk_pg_parser_translog_ddlstmt_sequence *)
														ddl_trans->m_ddlstmt;
	const char *conninfo;
	PGconn	 *conn;
//	PGresult   *res;
//	char sql_exec[1024] = {'\0'};
	char *typname = NULL;
//	int i = 0;

	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}
	typname = get_typename_by_oid(conn, seq->m_seqtypid);
	printf("DDL TYPE: CREATE SEQUENCE\n");
	printf("ddl: create sequence %s as %s ", seq->m_seqname, typname);
	printf("increment by %lu ", seq->m_seqincrement);
	printf("minvalue %lu ", seq->m_seqmin);
	printf("maxvalue %lu ", seq->m_seqmax);
	printf("start with %lu ", seq->m_seqstart);
	printf("cache %lu ", seq->m_seqcache);
	if (seq->m_seqcycle)
		printf("cycle");
	else
		printf("no cycle");
	printf(";\n");
	pfree(typname);
	PQfinish(conn);
}

static void display_create_type(xk_pg_parser_translog_ddlstmt *ddl_trans)
{
	xk_pg_parser_translog_ddlstmt_type *typ = (xk_pg_parser_translog_ddlstmt_type *)
														ddl_trans->m_ddlstmt;
	const char *conninfo;
	PGconn	 *conn;
//	PGresult   *res;
//	char sql_exec[1024] = {'\0'};
	char *nspname = NULL;
	char *typname = NULL;
	int i = 0;

	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}
	nspname = get_namespace_name_by_oid(conn, typ->m_typnspid);
	printf("DDL TYPE: CREATE TYPE\n");
	if (typ->m_typtype == XK_PG_PARSER_TRANSLOG_DDLSTMT_TYPE_TYPTYPE_DOMAIN)
	{
		typname = get_typename_by_oid(conn, *((uint32_t *)typ->m_typptr));
		printf("ddl: create domain %s.%s as %s;\n", nspname, typ->m_type_name, typname);
		pfree(typname);
	}
	else
	{
		printf("ddl: create type %s.%s as ", nspname, typ->m_type_name);
		if (typ->m_typtype == XK_PG_PARSER_TRANSLOG_DDLSTMT_TYPE_TYPTYPE_COMPOSITE)
		{
			xk_pg_parser_translog_ddlstmt_typcol *typcol = NULL;
			printf("(");
			for (i = 0; i < typ->m_typvalcnt; i++)
			{
				typcol = (xk_pg_parser_translog_ddlstmt_typcol *)(typ->m_typptr);
				typname = get_typename_by_oid(conn, typcol[i].m_coltypid);
				printf("%s %s", typcol[i].m_colname, typname);
				pfree(typname);
				if (i < typ->m_typvalcnt - 1)
					printf(", ");
			}
			printf(")");
		}
		else if(typ->m_typtype == XK_PG_PARSER_TRANSLOG_DDLSTMT_TYPE_TYPTYPE_RANGE)
		{
			xk_pg_parser_translog_ddlstmt_typrange *typcol = NULL;
			char *opclassname = NULL;
			typcol = (xk_pg_parser_translog_ddlstmt_typrange *)(typ->m_typptr);
			opclassname = get_opclassname_by_oid(conn, typcol->m_subtype_opclass);
			typname = get_typename_by_oid(conn, typcol->m_subtype);
			printf("range (");
			printf("subtype = %s, ", typname);
			printf("subtype_opclass = %s)", opclassname);
			pfree(typname);
			pfree(opclassname);
		}
		else if(typ->m_typtype == XK_PG_PARSER_TRANSLOG_DDLSTMT_TYPE_TYPTYPE_ENUM)
		{
			xk_pg_parser_translog_ddlstmt_valuebase *typcol = NULL;
			printf("enum (");
			for (i = 0; i < typ->m_typvalcnt; i++)
			{
				typcol = (xk_pg_parser_translog_ddlstmt_valuebase *)(typ->m_typptr);
				printf("'%s'", typcol[i].m_value);
				if (i < typ->m_typvalcnt - 1)
					printf(", ");
			}
			printf(")");
		}
		printf(";\n");
	}
	pfree(nspname);
	PQfinish(conn);
}

static void display_drop_table(xk_pg_parser_translog_ddlstmt *ddl_trans)
{
	xk_pg_parser_translog_ddlstmt_drop_base *table = (xk_pg_parser_translog_ddlstmt_drop_base *)
														ddl_trans->m_ddlstmt;
	const char *conninfo;
	PGconn	 *conn;
//	PGresult   *res;
//	char sql_exec[1024] = {'\0'};
	char *nspname = NULL;
//	int i = 0;

	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}
	nspname = get_namespace_name_by_oid(conn, table->m_namespace_oid);

	printf("DDL TYPE: DROP TABLE\n");
	printf("ddl: drop table %s.%s;\n", nspname, table->m_name);
	printf("          |---> %u\n", table->m_relid);
	pfree(nspname);
	PQfinish(conn);
}
static void display_drop_index(xk_pg_parser_translog_ddlstmt *ddl_trans)
{
	xk_pg_parser_translog_ddlstmt_drop_base *index = (xk_pg_parser_translog_ddlstmt_drop_base *)
														ddl_trans->m_ddlstmt;
	const char *conninfo;
	PGconn	 *conn;
//	PGresult   *res;
//	char sql_exec[1024] = {'\0'};
	char *nspname = NULL;
//	int i = 0;

	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}
	nspname = get_namespace_name_by_oid(conn, index->m_namespace_oid);

	printf("DDL TYPE: DROP INDEX\n");
	printf("ddl: drop index %s.%s;\n", nspname, index->m_name);
	printf("          |---> %u\n", index->m_relid);
	pfree(nspname);
	PQfinish(conn);
}

static void display_drop_sequence(xk_pg_parser_translog_ddlstmt *ddl_trans)
{
	xk_pg_parser_translog_ddlstmt_drop_base *seq = (xk_pg_parser_translog_ddlstmt_drop_base *)
														ddl_trans->m_ddlstmt;
	const char *conninfo;
	PGconn	 *conn;
//	PGresult   *res;
//	char sql_exec[1024] = {'\0'};
	char *nspname = NULL;
//	int i = 0;

	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}
	nspname = get_namespace_name_by_oid(conn, seq->m_namespace_oid);

	printf("DDL TYPE: DROP SEQUENCE\n");
	printf("ddl: drop sequence %s.%s;\n", nspname, seq->m_name);
	pfree(nspname);
	PQfinish(conn);
}

static void display_drop_type(xk_pg_parser_translog_ddlstmt *ddl_trans)
{
	xk_pg_parser_translog_ddlstmt_drop_base *type = (xk_pg_parser_translog_ddlstmt_drop_base *)
														ddl_trans->m_ddlstmt;
	const char *conninfo;
	PGconn	 *conn;
//	PGresult   *res;
//	char sql_exec[1024] = {'\0'};
	char *nspname = NULL;
//	int i = 0;

	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}
	nspname = get_namespace_name_by_oid(conn, type->m_namespace_oid);

	printf("DDL TYPE: DROP TYPE\n");
	printf("ddl: drop type %s.%s;\n", nspname, type->m_name);
	pfree(nspname);
	PQfinish(conn);
}

static void display_truncate_table(xk_pg_parser_translog_ddlstmt *ddl_trans)
{
	xk_pg_parser_translog_ddlstmt_drop_base *table = (xk_pg_parser_translog_ddlstmt_drop_base *)
														ddl_trans->m_ddlstmt;
	const char *conninfo;
	PGconn	 *conn;
//	PGresult   *res;
//	char sql_exec[1024] = {'\0'};
	char *nspname = NULL;
//	int i = 0;

	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}
	nspname = get_namespace_name_by_oid(conn, table->m_namespace_oid);

	printf("DDL TYPE: TRUNCATE TABLE\n");
	printf("ddl: truncate table %s.%s;\n", nspname, table->m_name);
	pfree(nspname);
	PQfinish(conn);
}

static void display_reindex(xk_pg_parser_translog_ddlstmt *ddl_trans)
{
	xk_pg_parser_translog_ddlstmt_drop_base *index = (xk_pg_parser_translog_ddlstmt_drop_base *)
														ddl_trans->m_ddlstmt;
	const char *conninfo;
	PGconn	 *conn;
//	PGresult   *res;
//	char sql_exec[1024] = {'\0'};
	char *nspname = NULL;
//	int i = 0;

	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}
	nspname = get_namespace_name_by_oid(conn, index->m_namespace_oid);

	printf("DDL TYPE: REINDEX INDEX\n");
	printf("ddl: reindex index %s.%s;\n", nspname, index->m_name);
	pfree(nspname);
	PQfinish(conn);
}

static void display_alter_table_drop_constraint(xk_pg_parser_translog_ddlstmt *ddl_trans)
{
	xk_pg_parser_translog_ddlstmt_drop_constraint *cons = (xk_pg_parser_translog_ddlstmt_drop_constraint *)
														ddl_trans->m_ddlstmt;
	const char *conninfo;
	PGconn	 *conn;
//	PGresult   *res;
//	char sql_exec[1024] = {'\0'};
	char *nspname = NULL;
	char *relname = NULL;
//	int i = 0;

	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}
	nspname = get_namespace_name_by_oid(conn, cons->m_namespace_oid);
	relname = get_relname_by_oid(conn, cons->m_relid);
	printf("DDL TYPE: ALTER TABLE\n");
	printf("ddl: alter table %s.%s drop constraint %s;\n", nspname, relname, cons->m_consname);
	pfree(nspname);
	pfree(relname);
	PQfinish(conn);
}

static void display_alter_table_add_column(xk_pg_parser_translog_ddlstmt *ddl_trans)
{
	xk_pg_parser_translog_ddlstmt_addcolumn *add = (xk_pg_parser_translog_ddlstmt_addcolumn *)
														ddl_trans->m_ddlstmt;
	const char *conninfo;
	PGconn	 *conn;
//	PGresult   *res;
//	char sql_exec[1024] = {'\0'};
	char *nspname = NULL;
	char *typname = NULL;
//	int i = 0;

	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}
	nspname = get_namespace_name_by_oid(conn, add->m_relnamespace);
	typname = get_typename_by_oid(conn, add->m_addcolumn->m_coltypid);
	printf("DDL TYPE: ALTER TABLE\n");
	printf("ddl: alter table %s.%s add column %s %s ", nspname,
													  add->m_relname,
													  add->m_addcolumn->m_colname,
													  typname);
	if (add->m_addcolumn->m_flag & XK_PG_PARSER_DDL_COLUMN_NOTNULL)
		printf("not null");
	printf(";\n");
	pfree(nspname);
	pfree(typname);
	PQfinish(conn);
}
static void display_alter_table_rename_column(xk_pg_parser_translog_ddlstmt *ddl_trans)
{
	xk_pg_parser_translog_ddlstmt_altercolumn *rename = (xk_pg_parser_translog_ddlstmt_altercolumn *)
														ddl_trans->m_ddlstmt;
	const char *conninfo;
	PGconn	 *conn;
//	PGresult   *res;
//	char sql_exec[1024] = {'\0'};
	char *nspname = NULL;
	char *relname = NULL;
//	int i = 0;

	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}
	relname = get_relname_by_oid(conn, rename->m_relid);
	nspname = get_namespace_name_by_reloid(conn, rename->m_relid);
	printf("DDL TYPE: ALTER TABLE\n");
	printf("ddl: alter table %s.%s alter column %s rename to %s;\n", nspname,
													  relname,
													  rename->m_colname,
													  rename->m_colname_new);
	pfree(relname);
	pfree(nspname);
	PQfinish(conn);
}

static void display_alter_table_alter_column_type(xk_pg_parser_translog_ddlstmt *ddl_trans)
{
	xk_pg_parser_translog_ddlstmt_altercolumn *alter = (xk_pg_parser_translog_ddlstmt_altercolumn *)
														ddl_trans->m_ddlstmt;
	const char *conninfo;
	PGconn	 *conn;
//	PGresult   *res;
//	char sql_exec[1024] = {'\0'};
	char *relname = NULL;
	char *nspname = NULL;
	char *typname = NULL;
//	int i = 0;

	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}
	relname = get_relname_by_oid(conn, alter->m_relid);
	nspname = get_namespace_name_by_reloid(conn, alter->m_relid);
	typname = get_typename_by_oid(conn, alter->m_type_new);
	printf("DDL TYPE: ALTER TABLE\n");
	printf("ddl: alter table %s.%s alter column %s type %s", nspname,
													  relname,
													  alter->m_colname,
													  typname);
	if (alter->m_length > 0)
		printf("(%d)", alter->m_length);
	else if (alter->m_precision > 0 && alter->m_scale < 0)
		printf("(%d)", alter->m_precision);
	else if (alter->m_precision > 0 && alter->m_scale >= 0)
		printf("(%d, %d)", alter->m_precision, alter->m_scale);
	else
		printf(";\n");
	pfree(nspname);
	pfree(typname);
	PQfinish(conn);
}

static void display_alter_table_alter_column_null_set(xk_pg_parser_translog_ddlstmt *ddl_trans)
{
	xk_pg_parser_translog_ddlstmt_altercolumn *alter = (xk_pg_parser_translog_ddlstmt_altercolumn *)
														ddl_trans->m_ddlstmt;
	const char *conninfo;
	PGconn	 *conn;
//	PGresult   *res;
//	char sql_exec[1024] = {'\0'};
	char *nspname = NULL;
	char *relname = NULL;
//	int i = 0;

	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}
	relname = get_relname_by_oid(conn, alter->m_relid);
	nspname = get_namespace_name_by_reloid(conn, alter->m_relid);
	printf("DDL TYPE: ALTER TABLE\n");
	printf("ddl: alter table %s.%s alter column %s ", nspname,
													  relname,
													  alter->m_colname);
	if (alter->m_notnull == true)
		printf("set not null;\n");
	else
		printf("drop not null;\n");
	pfree(nspname);
	pfree(relname);
	PQfinish(conn);
}

static void print_node_ddl(void* value, PGconn *conn, uint32_t relid, int local)
{
	xk_pg_parser_nodetree *node = (xk_pg_parser_nodetree *)value;
	PGresult   *res;
	char sql_exec[1024] = {'\0'};
	char *temp = NULL;
	int current_local = 1;
	bool location = false;

	while (node)
	{
		if (node->m_node_type == XK_PG_PARSER_NODETYPE_SEPARATOR)
			current_local += 1;
		if (current_local == local)
			location = true;
		else
			location = false;
		if (!location && local)
		{
			node = node->m_next;
			continue;
		}

		switch (node->m_node_type)
		{
			case XK_PG_PARSER_NODETYPE_VAR:
			{
				xk_pg_parser_node_var *node_var = (xk_pg_parser_node_var *)node->m_node;
				if (dbtype == XK_DATABASE_TYPE_UXDB)
					sprintf(sql_exec, "select attname from ux_attribute "
									  "where attrelid = %u and attnum = %d;", relid, node_var->m_attno);
				else
					sprintf(sql_exec, "select attname from pg_attribute "
									  "where attrelid = %u and attnum = %d;", relid, node_var->m_attno);
				res = PQexec(conn, sql_exec);
				if (PQresultStatus(res) != PGRES_TUPLES_OK)
				{
					fprintf(stderr, "failed in print_node_ddl: %s", PQerrorMessage(conn));
					PQclear(res);
					exit_nicely(conn);
				}
				temp = PQgetvalue(res, 0, 0);
				if (!temp)
				{
					fprintf(stderr, "failed in print_node_ddl VAR: table:%u, num:%d", relid, node_var->m_attno);
					PQclear(res);
					exit_nicely(conn);
				}
				printf("%s", temp);
				PQclear(res);
				break;
			}
			case XK_PG_PARSER_NODETYPE_CONST:
			{
				xk_pg_parser_node_const *node_const = (xk_pg_parser_node_const *)node->m_node;
				if (node_const->m_char_value)
				{
					if (node_const->m_typid == 2205)
					{
						if (dbtype == XK_DATABASE_TYPE_UXDB)
							sprintf(sql_exec, "select relname from ux_class "
											"where oid = %s;", node_const->m_char_value);
						else
							sprintf(sql_exec, "select relname from pg_class "
											"where oid = %s;", node_const->m_char_value);
						res = PQexec(conn, sql_exec);
						if (PQresultStatus(res) != PGRES_TUPLES_OK)
						{
							fprintf(stderr, "failed in print_node_ddl: %s", PQerrorMessage(conn));
							PQclear(res);
							exit_nicely(conn);
						}
						temp = PQgetvalue(res, 0, 0);
						if (!temp)
						{
							fprintf(stderr, "failed in print_node_ddl CONST: table:%u, CONST", relid);
							PQclear(res);
							exit_nicely(conn);
						}
						printf("\'%s\'::", temp);
						temp = NULL;
						memset(sql_exec, 0, 1024);
						PQclear(res);
					}
					else
						printf("\'%s\'::", node_const->m_char_value);
					if (dbtype == XK_DATABASE_TYPE_UXDB)
						sprintf(sql_exec, "select typname from ux_type "
										  "where oid = %u;", node_const->m_typid);
					else
						sprintf(sql_exec, "select typname from pg_type "
										  "where oid = %u;", node_const->m_typid);
					res = PQexec(conn, sql_exec);
					if (PQresultStatus(res) != PGRES_TUPLES_OK)
					{
						fprintf(stderr, "failed in print_node_ddl: %s", PQerrorMessage(conn));
						PQclear(res);
						exit_nicely(conn);
					}
					temp = PQgetvalue(res, 0, 0);
					if (!temp)
					{
						fprintf(stderr, "failed in print_node_ddl CONST: table:%u, CONST", relid);
						PQclear(res);
						exit_nicely(conn);
					}
					printf("%s", temp);
					PQclear(res);
				}
				else
					printf("[can't convert]");
				break;
			}
			case XK_PG_PARSER_NODETYPE_FUNC:
			{
				xk_pg_parser_node_func *node_func = (xk_pg_parser_node_func *)node->m_node;
				if (node_func->m_funcname)
					printf("%s", node_func->m_funcname);
				else
				{
					if (dbtype == XK_DATABASE_TYPE_UXDB)
						sprintf(sql_exec, "select proname from ux_proc "
										  "where oid = %u;", node_func->m_funcid);
					else
						sprintf(sql_exec, "select proname from pg_proc "
										  "where oid = %u;", node_func->m_funcid);
					res = PQexec(conn, sql_exec);
					if (PQresultStatus(res) != PGRES_TUPLES_OK)
					{
						fprintf(stderr, "failed in print_node_ddl: %s", PQerrorMessage(conn));
						PQclear(res);
						exit_nicely(conn);
					}
					temp = PQgetvalue(res, 0, 0);
					if (!temp)
					{
						fprintf(stderr, "failed in print_node_ddl FUNC: %u", node_func->m_funcid);
						PQclear(res);
						exit_nicely(conn);
					}
					printf("%s", temp);
					PQclear(res);
				}
				break;
			}
			case XK_PG_PARSER_NODETYPE_OP:
			{
				xk_pg_parser_node_op *node_op = (xk_pg_parser_node_op *)node->m_node;
				if (node_op->m_opname)
					printf("%s", node_op->m_opname);
				else
				{
					if (dbtype == XK_DATABASE_TYPE_UXDB)
						sprintf(sql_exec, "select oprname from ux_operator "
										  "where oid = %u;", node_op->m_opid);
					else
						sprintf(sql_exec, "select oprname from pg_operator "
										  "where oid = %u;", node_op->m_opid);
					res = PQexec(conn, sql_exec);
					if (PQresultStatus(res) != PGRES_TUPLES_OK)
					{
						fprintf(stderr, "failed in print_node_ddl: %s", PQerrorMessage(conn));
						PQclear(res);
						exit_nicely(conn);
					}
					temp = PQgetvalue(res, 0, 0);
					if (!temp)
					{
						fprintf(stderr, "failed in print_node_ddl OP: %u", node_op->m_opid);
						PQclear(res);
						exit_nicely(conn);
					}
					printf("%s", temp);
					PQclear(res);
				}
				break;
			}
			case XK_PG_PARSER_NODETYPE_CHAR:
			{
				printf("%s", (char*)node->m_node);
				break;
			}
			case XK_PG_PARSER_NODETYPE_TYPE:
			{
				xk_pg_parser_node_type * node_type = (xk_pg_parser_node_type *)node->m_node;
				char *typname = get_typename_by_oid(conn, node_type->m_typeid);
				printf("%s", typname);
				break;
			}
			case XK_PG_PARSER_NODETYPE_SEPARATOR:
				break;
		}
		node = node->m_next;
	}
}

static void display_alter_table_alter_column_default(xk_pg_parser_translog_ddlstmt *ddl_trans)
{
	xk_pg_parser_translog_ddlstmt_default *alter = (xk_pg_parser_translog_ddlstmt_default *)
														ddl_trans->m_ddlstmt;
	const char *conninfo;
	PGconn	 *conn;
//	PGresult   *res;
//	char sql_exec[1024] = {'\0'};
	char *nspname = NULL;
	char *relname = NULL;
//	int i = 0;

	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}
	relname = get_relname_by_oid(conn, alter->m_relid);
	nspname = get_namespace_name_by_reloid(conn, alter->m_relid);
	printf("DDL TYPE: ALTER TABLE\n");
	printf("ddl: alter table %s.%s alter column %s set default(", nspname,
													  relname,
													  alter->m_colname);
	print_node_ddl(alter->m_default_node, conn, alter->m_relid, 0);
	printf(")\n");
	pfree(nspname);
	pfree(relname);
	PQfinish(conn);
}

static void display_alter_table_alter_column_drop_default(xk_pg_parser_translog_ddlstmt *ddl_trans)
{
	xk_pg_parser_translog_ddlstmt_default *alter = (xk_pg_parser_translog_ddlstmt_default *)
														ddl_trans->m_ddlstmt;
	const char *conninfo;
	PGconn	 *conn;
//	PGresult   *res;
//	char sql_exec[1024] = {'\0'};
	char *nspname = NULL;
	char *relname = NULL;
//	int i = 0;

	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}
	relname = get_relname_by_oid(conn, alter->m_relid);
	nspname = get_namespace_name_by_reloid(conn, alter->m_relid);
	printf("DDL TYPE: ALTER TABLE\n");
	printf("ddl: alter table %s.%s alter column %s drop default;\n", nspname,
													  relname,
													  alter->m_colname);
	pfree(nspname);
	pfree(relname);
	PQfinish(conn);
}

static void display_alter_table_alter_column_drop_column(xk_pg_parser_translog_ddlstmt *ddl_trans)
{
	xk_pg_parser_translog_ddlstmt_altercolumn *drop = (xk_pg_parser_translog_ddlstmt_altercolumn *)
														ddl_trans->m_ddlstmt;
	const char *conninfo;
	PGconn	 *conn;
//	PGresult   *res;
//	char sql_exec[1024] = {'\0'};
	char *nspname = NULL;
	char *relname = NULL;
//	int i = 0;

	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}
	relname = get_relname_by_oid(conn, drop->m_relid);
	nspname = get_namespace_name_by_reloid(conn, drop->m_relid);
	printf("DDL TYPE: ALTER TABLE\n");
	printf("ddl: alter table %s.%s drop column %s;\n", nspname,
													  relname,
													  drop->m_colname);
	pfree(nspname);
	pfree(relname);
	PQfinish(conn);
}

static void display_alter_table_alter_column_alter_schema(xk_pg_parser_translog_ddlstmt *ddl_trans)
{
	xk_pg_parser_translog_ddlstmt_altertable *table = (xk_pg_parser_translog_ddlstmt_altertable *)
														ddl_trans->m_ddlstmt;
	const char *conninfo;
	PGconn	 *conn;
//	PGresult   *res;
//	char sql_exec[1024] = {'\0'};
	char *nspname_old = NULL;
	char *nspname_new = NULL;
//	int i = 0;

	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}
	nspname_old = get_namespace_name_by_oid(conn, table->m_relnamespaceid);
	nspname_new = get_namespace_name_by_oid(conn, table->m_relnamespaceid_new);
	printf("DDL TYPE: ALTER TABLE\n");
	printf("ddl: alter table %s.%s set schema %s;\n", nspname_old,
													  table->m_relname,
													  nspname_new);
	pfree(nspname_old);
	pfree(nspname_new);
	PQfinish(conn);
}

static void display_alter_table_set_logged(xk_pg_parser_translog_ddlstmt *ddl_trans)
{
	xk_pg_parser_translog_ddlstmt_setlog *setlog = (xk_pg_parser_translog_ddlstmt_setlog *)
														ddl_trans->m_ddlstmt;
	const char *conninfo;
	PGconn	 *conn;
//	PGresult   *res;
//	char sql_exec[1024] = {'\0'};
	char *nspname = NULL;
//	int i = 0;

	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}
	nspname = get_namespace_name_by_oid(conn, setlog->m_relnamespace);
	printf("DDL TYPE: ALTER TABLE\n");
	printf("ddl: alter table %s.%s set logged;\n", nspname,
													  setlog->m_relname);
	pfree(nspname);
	PQfinish(conn);
}

static void display_alter_table_set_unlogged(xk_pg_parser_translog_ddlstmt *ddl_trans)
{
	xk_pg_parser_translog_ddlstmt_setlog *setlog = (xk_pg_parser_translog_ddlstmt_setlog *)
														ddl_trans->m_ddlstmt;
	const char *conninfo;
	PGconn	 *conn;
//	PGresult   *res;
//	char sql_exec[1024] = {'\0'};
	char *nspname = NULL;
//	int i = 0;

	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}
	nspname = get_namespace_name_by_oid(conn, setlog->m_relnamespace);
	printf("DDL TYPE: ALTER TABLE\n");
	printf("ddl: alter table %s.%s set unlogged;\n", nspname,
													  setlog->m_relname);
	pfree(nspname);
	PQfinish(conn);
}

static void display_alter_table_rename(xk_pg_parser_translog_ddlstmt *ddl_trans)
{
	xk_pg_parser_translog_ddlstmt_altertable *table = (xk_pg_parser_translog_ddlstmt_altertable *)
														ddl_trans->m_ddlstmt;
	const char *conninfo;
	PGconn	 *conn;
//	PGresult   *res;
//	char sql_exec[1024] = {'\0'};
	char *nspname = NULL;
//	int i = 0;

	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}
	nspname = get_namespace_name_by_oid(conn, table->m_relnamespaceid);

	printf("DDL TYPE: ALTER TABLE\n");
	printf("ddl: alter table %s.%s rename to %s;\n", nspname, table->m_relname, table->m_relname_new);
	pfree(nspname);
	PQfinish(conn);
}


static void display_alter_table_add_constraint(xk_pg_parser_translog_ddlstmt *ddl_trans)
{
	xk_pg_parser_translog_ddlstmt_tbconstraint *table = (xk_pg_parser_translog_ddlstmt_tbconstraint *)
														ddl_trans->m_ddlstmt;
	const char *conninfo;
	PGconn	 *conn;
//	PGresult   *res;
//	char sql_exec[1024] = {'\0'};
	char *nspname = NULL;
	char *relname = NULL;
	int i = 0;

	conninfo = (char *)conninfo_default;
	/* 建立到数据库的一个连接 */
	conn = PQconnectdb(conninfo);
	/* 检查看连接是否成功建立 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}
	nspname = get_namespace_name_by_oid(conn, table->m_consnspoid);
	relname = get_relname_by_oid(conn, table->m_relid);
	printf("DDL TYPE: ALTER TABLE ADD CONSTRAINT\n");
	printf("ddl: alter table %s.%s add constraint %s ", nspname, relname, table->m_consname);
	if (table->m_type == XK_PG_PARSER_DDL_CONSTRAINT_PRIMARYKEY)
	{
		xk_pg_parser_translog_ddlstmt_tbconstraint_key *pkey = 
		(xk_pg_parser_translog_ddlstmt_tbconstraint_key *)table->m_constraint_stmt;

		printf("primary key (");
		for (i = 0; i < pkey->m_colcnt; i++)
		{
			printf("%s", pkey->m_concols[i].m_colname);
			if (i < pkey->m_colcnt - 1)
			printf(", ");
		}
		printf(");\n");
	}
	else if (table->m_type == XK_PG_PARSER_DDL_CONSTRAINT_UNIQUE)
	{
		xk_pg_parser_translog_ddlstmt_tbconstraint_key *ukey = 
		(xk_pg_parser_translog_ddlstmt_tbconstraint_key *)table->m_constraint_stmt;

		printf("unique (");
		for (i = 0; i < ukey->m_colcnt; i++)
		{
			printf("%s", ukey->m_concols[i].m_colname);
			if (i < ukey->m_colcnt - 1)
			printf(", ");
		}
		printf(");\n");
	}
	else if (table->m_type == XK_PG_PARSER_DDL_CONSTRAINT_FOREIGNKEY)
	{
		xk_pg_parser_translog_ddlstmt_tbconstraint_fkey *fkey = 
		(xk_pg_parser_translog_ddlstmt_tbconstraint_fkey *)table->m_constraint_stmt;

		printf("foreign key (");
		for (i = 0; i < fkey->m_colcnt; i++)
		{
			char *tempcolname = get_colname_by_oid(conn, table->m_relid, fkey->m_concols_position[i]);
			printf("%s", tempcolname);
			if (i < fkey->m_colcnt - 1)
			printf(", ");
			pfree(tempcolname);
		}
		printf(") ");
		pfree(relname);
		pfree(nspname);
		relname = get_relname_by_oid(conn, fkey->m_consfkeyid);
		nspname = get_namespace_name_by_reloid(conn, fkey->m_consfkeyid);
		printf(" references %s.%s(", nspname, relname);
		for (i = 0; i < fkey->m_colcnt; i++)
		{
			char *tempcolname = get_colname_by_oid(conn, table->m_relid, fkey->m_fkeycols_position[i]);
			printf("%s", tempcolname);
			if (i < fkey->m_colcnt - 1)
			printf(", ");
			pfree(tempcolname);
		}
		printf(");\n");
	}
	else if (table->m_type == XK_PG_PARSER_DDL_CONSTRAINT_CHECK)
	{
		xk_pg_parser_translog_ddlstmt_tbconstraint_check *check = 
		(xk_pg_parser_translog_ddlstmt_tbconstraint_check *)table->m_constraint_stmt;

		printf("check (");
		print_node_ddl(check->m_check_node, conn, table->m_relid, 0);
		printf(");\n");
	}
	pfree(nspname);
	pfree(relname);
	PQfinish(conn);
}

static void free_dml_trans_from_list(void)
{
	ListCell *cell = NULL;
	List *list = sysdict_list;
	xk_pg_parser_translog_tbcol_values *dml_trans = NULL;
	foreach(cell, list)
	{
		dml_trans = (xk_pg_parser_translog_tbcol_values *)lfirst(cell);
		if(dml_trans->m_base.m_dmltype == XK_PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT)
		{

			xk_pg_parser_trans_TransRecord_Minsert_free(NULL, (xk_pg_parser_translog_tbcolbase *)dml_trans);
		}
		else
		{
			xk_pg_parser_trans_TransRecord_free(NULL, (xk_pg_parser_translog_tbcolbase *)dml_trans);
		}
	}
}

static void display_ddl(xk_pg_parser_translog_ddlstmt *ddl_trans)
{
	xk_pg_parser_translog_ddlstmt *current_ddl = ddl_trans;
	printf(">>>>>>>>>>>>>>>>>>>>trans ddl done!<<<<<<<<<<<<<<<<<<<<\n");
	if (!current_ddl)
	{
		printf("can't get any ddl!\n");
		free_dml_trans_from_list();
		list_free(sysdict_list);
		sysdict_list = NULL;
		return;
	}
	while (current_ddl)
	{
		if (current_ddl->m_base.m_ddltype == XK_PG_PARSER_DDLTYPE_CREATE)
		{
			switch (current_ddl->m_base.m_ddlinfo)
			{
				case XK_PG_PARSER_DDLINFO_CREATE_TABLE:
					display_create_table(current_ddl);
					break;
				case XK_PG_PARSER_DDLINFO_CREATE_INDEX:
					display_create_index(current_ddl);
					break;
				case XK_PG_PARSER_DDLINFO_CREATE_SEQUENCE:
					display_create_sequence(current_ddl);
					break;
				case XK_PG_PARSER_DDLINFO_CREATE_TYPE:
					display_create_type(current_ddl);
					break;
				case XK_PG_PARSER_DDLINFO_CREATE_NAMESPACE:
					display_create_namespace(current_ddl);
					break;
			}
		}
		else if (current_ddl->m_base.m_ddltype == XK_PG_PARSER_DDLTYPE_DROP)
		{
			switch (current_ddl->m_base.m_ddlinfo)
			{
				case XK_PG_PARSER_DDLINFO_DROP_TABLE:
					display_drop_table(current_ddl);
					break;
				case XK_PG_PARSER_DDLINFO_DROP_INDEX:
					display_drop_index(current_ddl);
					break;
				case XK_PG_PARSER_DDLINFO_DROP_SEQUENCE:
					display_drop_sequence(current_ddl);
					break;
				case XK_PG_PARSER_DDLINFO_DROP_TYPE:
					display_drop_type(current_ddl);
					break;
				case XK_PG_PARSER_DDLINFO_DROP_NAMESPACE:
					display_drop_namespace(current_ddl);
					break;

			}
		}
		else if (current_ddl->m_base.m_ddltype == XK_PG_PARSER_DDLTYPE_ALTER)
		{
			switch (current_ddl->m_base.m_ddlinfo)
			{
				case XK_PG_PARSER_DDLINFO_ALTER_TABLE_ADD_CONSTRAINT:
					display_alter_table_add_constraint(current_ddl);
					break;
				case XK_PG_PARSER_DDLINFO_ALTER_TABLE_RENAME:
					display_alter_table_rename(current_ddl);
					break;
				case XK_PG_PARSER_DDLINFO_ALTER_TABLE_DROP_CONSTRAINT:
					display_alter_table_drop_constraint(current_ddl);
					break;
				case XK_PG_PARSER_DDLINFO_ALTER_TABLE_ADD_COLUMN:
					display_alter_table_add_column(current_ddl);
					break;
				case XK_PG_PARSER_DDLINFO_ALTER_COLUMN_RENAME:
					display_alter_table_rename_column(current_ddl);
					break;
				case XK_PG_PARSER_DDLINFO_ALTER_COLUMN_TYPE:
					display_alter_table_alter_column_type(current_ddl);
					break;
				case XK_PG_PARSER_DDLINFO_ALTER_COLUMN_NOTNULL:
					display_alter_table_alter_column_null_set(current_ddl);
					break;
				case XK_PG_PARSER_DDLINFO_ALTER_COLUMN_NULL:
					display_alter_table_alter_column_null_set(current_ddl);
					break;
				case XK_PG_PARSER_DDLINFO_ALTER_COLUMN_DEFAULT:
					display_alter_table_alter_column_default(current_ddl);
					break;
				case XK_PG_PARSER_DDLINFO_ALTER_COLUMN_DROP_DEFAULT:
					display_alter_table_alter_column_drop_default(current_ddl);
					break;
				case XK_PG_PARSER_DDLINFO_ALTER_TABLE_DROP_COLUMN:
					display_alter_table_alter_column_drop_column(current_ddl);
					break;
				case XK_PG_PARSER_DDLINFO_ALTER_TABLE_NAMESPACE:
					display_alter_table_alter_column_alter_schema(current_ddl);
					break;
				case XK_PG_PARSER_DDLINFO_ALTER_TABLE_SET_LOGGED:
					display_alter_table_set_logged(current_ddl);
					break;
				case XK_PG_PARSER_DDLINFO_ALTER_TABLE_SET_UNLOGGED:
					display_alter_table_set_unlogged(current_ddl);
					break;
			}
		}
		else if (current_ddl->m_base.m_ddltype == XK_PG_PARSER_DDLTYPE_SPECIAL)
		{
			switch (current_ddl->m_base.m_ddlinfo)
			{
				case XK_PG_PARSER_DDLINFO_TRUNCATE:
					display_truncate_table(current_ddl);
					break;
				case XK_PG_PARSER_DDLINFO_REINDEX:
					display_reindex(current_ddl);
					break;

			}
		}
		current_ddl = current_ddl->m_next;
	}
	xk_pg_parser_trans_ddl_free(NULL, ddl_trans);
	free_dml_trans_from_list();
	list_free(sysdict_list);
	sysdict_list = NULL;
	printf("\n\n\n\n");
}

static bool check_display(xk_pg_parser_translog_tbcol_values *trans)
{
	if (!strncmp("UX_", trans->m_base.m_tbname, 3))
		return false;
	else if (!display_ddl_trans)
	{
		if (!strncmp("pg_", trans->m_base.m_tbname, 3) || !strncmp("ux_", trans->m_base.m_tbname, 3))
			return false;
		else if (!strcmp("pg_catalog", trans->m_base.m_schemaname))
			return false;
	}
	return true;
}

static void storage_catalog(xk_pg_parser_translog_tbcolbase *trans_return)
{
	//char *lappend_data = NULL;
	//if (trans_return->m_dmltype == XK_PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT)
	//{
	//	lappend_data = palloc0(sizeof(xk_pg_parser_translog_tbcol_nvalues));
	//	memcpy(lappend_data, trans_return, sizeof(xk_pg_parser_translog_tbcol_nvalues));
	//}
	//else
	//{
	//	lappend_data = palloc0(sizeof(xk_pg_parser_translog_tbcol_values));
	//	memcpy(lappend_data, trans_return, sizeof(xk_pg_parser_translog_tbcol_values));
	//}
	sysdict_list = lappend(sysdict_list, trans_return);
	inddl = true;
}

//------------------------------------------------------------------------

#define WALSEGSZ (uint32)16777216
#define XLOG_BLCKSZ 8192

#define TIMELINE 1

typedef struct ripple_splitwal_incompleteRecord
{
    uint32      len;
    uint32      incomplete_len;
    char       *record;
}ripple_splitwal_incompleteRecord;

typedef struct ripple_splitwal_pageBuffer
{
    uint32              size;
    XLogRecPtr          startptr;
    char               *buf;
    ripple_splitwal_incompleteRecord   *incomplete;
}ripple_splitwal_pageBuffer;

typedef struct ripple_splitwal_WalReadCtl
{
    bool        wait;
    int         fd;
    TimeLineID  timeline;
    char       *inpath;
    XLogRecPtr  startptr;
    uint32      blcksz;
    uint32      walsz;
} ripple_splitwal_WalReadCtl;


static void printTime(void)
{

    struct timespec time_start = { 0, 0 };
    //获取当前时间
    clock_gettime(0, &time_start);
    printf("时间 %lus,%lu ns\n", time_start.tv_sec,time_start.tv_nsec);
}

static void wal_usleep(long microsec)
{
    if (microsec > 0)
    {
        struct timeval delay;
        delay.tv_sec = microsec / 1000000L;
        delay.tv_usec = microsec % 1000000L;
        (void) select(0, NULL, NULL, NULL, &delay);
    }
}

static int open_file_in_directory(const char *directory, const char *fname)
{
    int     fd = -1;
    char    fpath[MAXPGPATH];

    Assert(directory != NULL);

    snprintf(fpath, MAXPGPATH, "%s/%s", directory, fname);
    fd = open(fpath, O_RDONLY | RIPPLE_BINARY, 0);

    if (fd < 0 && errno != ENOENT)
        elog(RLOG_ERROR, "could not open file \"%s\": %s", fname, strerror(errno));
    return fd;
}

/*
 * Read count bytes from a segment file in the specified directory, for the
 * given timeline, containing the specified record pointer; store the data in
 * the passed buffer.
 */
static void WalWorkRead(ripple_splitwal_WalReadCtl *private, const char *directory, TimeLineID timeline_id,
                        XLogRecPtr startptr, uint32 WalSegSz, ripple_splitwal_pageBuffer *pbuff, Size count)
{
    int         sendFile = private->fd;
    char       *p;
    char       *buf = pbuff->buf;
    XLogRecPtr  recptr;
    Size        nbytes;

    static XLogSegNo sendSegNo = 0;
    static uint32 sendOff = 0;

    pbuff->startptr = startptr;

    p = buf;
    recptr = startptr;
    nbytes = count;

    while (nbytes > 0)
    {
        uint32        startoff;
        int            segbytes;
        int            readbytes;

        startoff = XLogSegmentOffset(recptr, WalSegSz);

        if (sendFile < 0 || !XLByteInSeg(recptr, sendSegNo, WalSegSz))
        {
            char        fname[MAXFNAMELEN];
            int            tries;

            /* Switch to another logfile segment */
            if (sendFile >= 0)
                close(sendFile);

            XLByteToSeg(recptr, sendSegNo, WalSegSz);

            XLogFileName(fname, timeline_id, sendSegNo, WalSegSz);

            if (sendFile < 0)
            {
                /*
                * In follow mode there is a short period of time after the server
                * has written the end of the previous file before the new file is
                * available. So we loop for 5 seconds looking for the file to
                * appear before giving up.
                */
                for (tries = 0; tries < 10; tries++)
                {
                    sendFile = open_file_in_directory(directory, fname);
                    if (sendFile >= 0)
                        break;
                    if (errno == ENOENT)
                    {
                        int            save_errno = errno;

                        /* File not there yet, try again */
                        wal_usleep(500 * 1000);

                        errno = save_errno;
                        continue;
                    }
                    /* Any other error, fall through and fail */
                    break;
                }
            }

            if (sendFile < 0)
                elog(RLOG_ERROR, "could not find file \"%s\": %s", fname, strerror(errno));
            sendOff = 0;
        }

        /* Need to seek in the file? */
        if (sendOff != startoff)
        {
            if (lseek(sendFile, (off_t) startoff, SEEK_SET) < 0)
            {
                int            err = errno;
                char        fname[MAXPGPATH];

                XLogFileName(fname, timeline_id, sendSegNo, WalSegSz);

                elog(RLOG_ERROR, "could not seek in log file %s to offset %u: %s",
                            fname, startoff, strerror(err));
            }
            sendOff = startoff;
        }

        /* How many bytes are within this segment? */
        if (nbytes > (WalSegSz - startoff))
            segbytes = WalSegSz - startoff;
        else
            segbytes = nbytes;

        readbytes = FileRead(sendFile, p, segbytes);
        if (readbytes <= 0)
        {
            int            err = errno;
            char        fname[MAXPGPATH];
            int            save_errno = errno;

            XLogFileName(fname, timeline_id, sendSegNo, WalSegSz);
            errno = save_errno;

            if (readbytes < 0)
                elog(RLOG_ERROR, "could not read from log file %s, offset %u, length %d: %s",
                            fname, sendOff, segbytes, strerror(err));
            else if (readbytes == 0)
                elog(RLOG_ERROR, "could not read from log file %s, offset %u: read %d of %zu",
                            fname, sendOff, readbytes, (Size) segbytes);
        }

        /* Update state for read */
        recptr += readbytes;

        sendOff += readbytes;
        nbytes -= readbytes;
        p += readbytes;
    }
    private->fd = sendFile;
}

static int WalWorkReadPage(ripple_splitwal_WalReadCtl *private, ripple_splitwal_pageBuffer *buff)
{
    int            count = private->blcksz;

    WalWorkRead(private, private->inpath, private->timeline, XlogGetPagePtr(private->startptr, count),
                     private->walsz, buff, count);
    if (((buff->startptr + buff->size) % private->walsz) == 0)
    {
        close(private->fd);
        private->fd = -1;
    }

    return count;
}

static void initWalReadCtl(ripple_splitwal_WalReadCtl *readCtl)
{
    //todo guc param
    //readCtl->inpath = guc_getConfigOption("wal_directory");
    //readCtl->blcksz = guc_getConfigOption("wal_block_size");
    //readCtl->startptr = ;
    //readCtl->timeline = ;
    char *temp_path = "/home/liu/pg/pgsql/data/pg_wal";

    readCtl->inpath = WALWORK_MEM_ALLOC(strlen(temp_path) + 1);
    memset(readCtl->inpath, 0, strlen(temp_path) + 1);
    strcpy(readCtl->inpath, temp_path);

    readCtl->fd = -1;
    readCtl->wait = false;
    readCtl->blcksz = XLOG_BLCKSZ;
    readCtl->startptr = start_lsn;
    readCtl->timeline = TIMELINE;
    readCtl->walsz = WALSEGSZ;
}

static void initPageBuffer(ripple_splitwal_WalReadCtl *readCtl, ripple_splitwal_pageBuffer *pageBuf)
{
    pageBuf->buf = WALWORK_MEM_ALLOC(readCtl->blcksz);
    pageBuf->size = readCtl->blcksz;
    pageBuf->incomplete = NULL;
    pageBuf->startptr = InvalidXLogRecPtr;
}

static void cleanPageBuffer(ripple_splitwal_pageBuffer *pageBuf)
{
    memset(pageBuf->buf, 0, pageBuf->size);
    pageBuf->startptr = InvalidXLogRecPtr;
}

/* 清理工作不会删除目标自身 */
static void destoryWalReadCtl(ripple_splitwal_WalReadCtl *readCtl)
{
    if (readCtl)
    {
        if (readCtl->inpath)
            WALWORK_MEM_FREE(readCtl->inpath);
    }
}

/* 清理工作不会删除目标自身 */
static void destoryPageBuffer(ripple_splitwal_pageBuffer *pageBuf)
{
    if (pageBuf)
    {
        if (pageBuf->buf)
            WALWORK_MEM_FREE(pageBuf->buf);
        if (pageBuf->incomplete)
        {
            elog(RLOG_WARNING, "when walwork exit, wal buffer already have incomplete record");
            WALWORK_MEM_FREE(pageBuf->incomplete);
        }
    }
}

static void destoryIncompleteRecord(ripple_splitwal_incompleteRecord *iRecord)
{
    if (iRecord)
    {
        if (iRecord->record)
            WALWORK_MEM_FREE(iRecord->record);
        WALWORK_MEM_FREE(iRecord);
    }
}

typedef struct totalLen
{
    uint32 len;
}totalLen;

#define updateCurrentPtr currentPtr = buff->startptr + offset

static ripple_recordcacheentry *splitRecordFromBuffer(ripple_splitwal_WalReadCtl *rctl, ripple_splitwal_pageBuffer *buff)
{
    XLogRecPtr              currentPtr = rctl->startptr;
    uint32                  offset = rctl->startptr - buff->startptr;
    char                   *currentBuf = buff->buf + offset;
    ripple_recordcacheentry   *result = WALWORK_MEM_ALLOC(sizeof(ripple_recordcacheentry));

    memset(result, 0, sizeof(ripple_recordcacheentry));

    while (offset < buff->size)
    {
        /* 标志着从blockheader开始 */
        if (IsXlogPageBegin(currentPtr, buff->size))
        {
            XLogPageHeader phdr = (XLogPageHeader) (currentBuf);
            /* 判断是否为wal文件开始 */
            if (IsXLogSegmentBegin(currentPtr, rctl->walsz))
                offset += SizeOfXLogLongPHD;
            else
                offset += SizeOfXLogShortPHD;

            updateCurrentPtr;

            /* 第一条record是否完整, 存在即不完整 */
            if (buff->incomplete)
            {
                uint32      temp_offset = buff->incomplete->len;
                char       *temp_record_ptr = (char *)buff->incomplete->record;
                uint32      realSize = MAXALIGN(phdr->xlp_rem_len);
                uint32      freeSpace = buff->size - offset;
                realSize = (realSize <= freeSpace) ? realSize : freeSpace;

                temp_offset -= buff->incomplete->incomplete_len;
                buff->incomplete->incomplete_len -= realSize;

                /* 组装 */
                memcpy(temp_record_ptr + temp_offset, buff->buf + offset, realSize);

                if (buff->incomplete->incomplete_len < 0)
                    elog(RLOG_ERROR, "split record, but get invalid incomplete record");

                if (buff->incomplete->incomplete_len == 0)
                {
                    /* 已经是完整record, 附加到结果中 */
                    ripple_cacherecord *recordEntry = WALWORK_MEM_ALLOC(sizeof(ripple_cacherecord));
                    recordEntry->startlsn = currentPtr;
                    recordEntry->rec = WALWORK_MEM_ALLOC(buff->incomplete->len);
                    memset(recordEntry->rec, 0, buff->incomplete->len);
                    memcpy(recordEntry->rec, buff->incomplete->record, buff->incomplete->len);

                    result->records = lappend(result->records, recordEntry);

                    /* 释放 */
                    destoryIncompleteRecord(buff->incomplete);
                    buff->incomplete = NULL;
                }

                offset += realSize;
                updateCurrentPtr;
            }
        }
        else /* 从record开始 */
        {
            /* 读取4字节的长度信息 */
            totalLen   *total_len = (totalLen *) (buff->buf + offset);

            /* 首先判断长度是否为0 */
            if (total_len->len == 0)
            {
                /* 可能WAL数据还未写入, 或者发生了switch xlog, 或者时间线变更 */
                if (result->records)
                {
                    /* 存在已有record */
                    ripple_cacherecord *recordEntry = llast(result->records);
                    XLogRecord *record = (XLogRecord *) recordEntry->rec;

                    /* 判断最后一条record是否为switch */
                    if (IsSwitchXlog(record->xl_rmid, record->xl_info))
                    {
                        offset = buff->size;
                        updateCurrentPtr;
                        break;
                    }
                }

                /* 在这里, 暂时认为追到了最新点 */
                if (!rctl->wait)
                    rctl->wait = true;

                break;
            }

            /* 判断是否跨block */
            if (offset + total_len->len > buff->size)
            {
                /* 跨block, 初始化incomplete record */
                ripple_splitwal_incompleteRecord *irecord = WALWORK_MEM_ALLOC(sizeof(ripple_splitwal_incompleteRecord));
                memset(irecord, 0, sizeof(ripple_splitwal_incompleteRecord));

                irecord->len = MAXALIGN(total_len->len);

                irecord->record = WALWORK_MEM_ALLOC(irecord->len);
                memset(irecord->record, 0, irecord->len);
                memcpy(irecord->record, buff->buf + offset, buff->size - offset);

                irecord->incomplete_len = irecord->len - (buff->size - offset);
                buff->incomplete = irecord;

                offset = buff->size;
                updateCurrentPtr;
            }
            else
            {
                /* 不跨block, 正常解析 */
                uint32               realSize = MAXALIGN(total_len->len);
                ripple_cacherecord *recordEntry = WALWORK_MEM_ALLOC(sizeof(ripple_cacherecord));
                recordEntry->startlsn = currentPtr;
                recordEntry->rec = WALWORK_MEM_ALLOC(realSize);
                memset(recordEntry->rec, 0, realSize);
                memcpy(recordEntry->rec, buff->buf + offset, realSize);

                result->records = lappend(result->records, recordEntry);

                offset += realSize;
                updateCurrentPtr;
            }
        }
    }

    /* 更新已经划分完的record lsn */
    rctl->startptr = currentPtr;

    if (result->records)
    {
        rctl->wait = false;
        result->num = result->records->length;
        result->startLSN = ((ripple_cacherecord*) lfirst(list_head(result->records)))->startlsn;
        result->endLSN = ((ripple_cacherecord*) lfirst(list_tail(result->records)))->startlsn;
        return result;
    }
    else
    {
        /* 如果有跨越了一个block的record, 这里是可以发生的 */
        WALWORK_MEM_FREE(result);
        return NULL;
    }
}

static void* splitRecordMain(void)
{
    ripple_splitwal_WalReadCtl           readCtl = {'\0'};
    ripple_splitwal_pageBuffer           pageBuf = {'\0'};
    uint32               waitCount = 0;

    /* TODO 初始化线程需要的信息 */
    initWalReadCtl(&readCtl);
    initPageBuffer(&readCtl, &pageBuf);
    printTime();
    while(1)
    {
        ripple_recordcacheentry   *recordEntry = NULL;

        /* 根据已知信息获取wal文件的一个block的数据 */
        WalWorkReadPage(&readCtl, &pageBuf);
        recordEntry = splitRecordFromBuffer(&readCtl, &pageBuf);
        if (recordEntry)
        {
            /* 放入缓存 */
            if (true)
            {
                ListCell *cell = NULL;
                foreach(cell, recordEntry->records)
                {
                    ripple_cacherecord *record_entry = (ripple_cacherecord *)lfirst(cell);
                    XLogRecord *record = (XLogRecord *)record_entry->rec;
					trans_main(record);
                }
            }
        }

        /* 已经将record放入了缓存, 清理pageBuf中不再需要的页, 不完整的record不能被清理 */
        cleanPageBuffer(&pageBuf);

        if (readCtl.wait)
        {
            waitCount++;
            printTime();
            printf("readCtl->startptr: %X/%X\n", (uint32) (readCtl.startptr >> 32), (uint32) readCtl.startptr);
            /* wait 500ms */
            wal_usleep(500 * 1000);
        }
        else
            waitCount = 0;

        if (waitCount >= 10)
        {
            //tryUpdateTimeLine(&readCtl);
            waitCount = 0;
        }


        //ripple_recordcache_add(InvalidXLogRecPtr, 8192, 0, NULL);
    }
    destoryWalReadCtl(&readCtl);
    destoryPageBuffer(&pageBuf);
    return NULL;
}
//----------------------------------------------------------------------------

int main(int argc, char **argv)
{
	uint32_t xlogid = 0;
	uint32_t xrecoff = 0;
	if (argc != 6)
	{
		printf("param error!\n");
		printf("use: test_parser [start_lsn] [display_pre_parser] [display_trans] [enable_trans_ddl] [display_ddl]\n");
		printf("like: test_parser 0/5C6BC410 t t t t\n");
		return 0;
	}
	if (sscanf(argv[1], "%X/%X", &xlogid, &xrecoff) != 2)
	{
		printf("start lsn error!\n");
		return 0;
	}
	start_lsn = (uint64) xlogid << 32 | xrecoff;

	no_pre = *(argv[2]) == 'f' ? true : false;
	no_trans_display = *(argv[3]) == 'f' ? true : false;
	if_trans_ddl = *(argv[4]) == 't' ? true : false;
	display_ddl_trans = *(argv[5]) == 't' ? true : false;
	splitRecordMain();
	return 0;
}
