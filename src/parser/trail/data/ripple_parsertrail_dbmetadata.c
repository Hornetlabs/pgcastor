#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "stmts/ripple_txnstmt.h"
#include "stmts/ripple_txnstmt_metadata.h"
#include "catalog/ripple_catalog.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "parser/trail/ripple_parsertrail.h"
#include "parser/trail/data/ripple_parsertrail_dbmetadata.h"


/* 向数据库hash里面增加数据 */
static bool ripple_parsertrail_dbmetadata2hash(ripple_parsertrail* parsertrail,
                                                ripple_ff_dbmetadata* ffdbmd)
{
    /*
     * 存在则不处理，不存在则增加
     */
    bool nadd = false;
    bool found = false;
    ripple_catalog_database_value *dbentry = NULL;
    ripple_catalog_datname2oid_value* dbnameentry = NULL;
    xk_pg_parser_NameData dbname = {{'\0'}};
    HASHCTL hctl = { 0 };
    ripple_txn *cur_txn = parsertrail->lasttxn;
    bool add_txn = false;
    ripple_txnstmt *stmt = NULL;
    ripple_txnstmt_metadata *metadata = NULL;
    ListCell *sys_begin = NULL;

    /*
     * 在 hash 表中查看是否含有，不含有则加入
     */
    if(NULL == parsertrail->transcache->sysdicts->by_database)
    {
        /* 创建 hash */
        nadd = true;
        rmemset1(&hctl, 0, 0, sizeof(hctl));
        hctl.keysize = sizeof(Oid);
        hctl.entrysize = sizeof(ripple_catalog_database_value);
        parsertrail->transcache->sysdicts->by_database = hash_create("decodehdatabase",
                                                        256,
                                                        &hctl,
                                                        HASH_ELEM | HASH_BLOBS);
    }

    dbentry = hash_search(parsertrail->transcache->sysdicts->by_database, &ffdbmd->oid, HASH_ENTER, &found);
    if(false == found)
    {
        /* 添加 */
        dbentry->oid = ffdbmd->oid;
        dbentry->ripple_database = (xk_pg_sysdict_Form_pg_database)rmalloc1(sizeof(xk_pg_parser_sysdict_pgdatabase));
        if(NULL == dbentry->ripple_database)
        {
            elog(RLOG_WARNING, "out of memory");
            return false;
        }
        rmemset0(dbentry->ripple_database, 0, '\0', sizeof(xk_pg_parser_sysdict_pgdatabase));
        dbentry->ripple_database->oid = ffdbmd->oid;
        rmemcpy1(dbentry->ripple_database->datname.data, 0, ffdbmd->dbname, strlen(ffdbmd->dbname));
    }

    if(false == nadd)
    {
        return true;
    }

    if(NULL == parsertrail->transcache->sysdicts->by_datname2oid)
    {
        rmemset1(&hctl, 0, 0, sizeof(hctl));
        hctl.keysize = sizeof(xk_pg_parser_NameData);
        hctl.entrysize = sizeof(ripple_catalog_datname2oid_value);
        parsertrail->transcache->sysdicts->by_datname2oid = hash_create("decodehdbname2oid",
                                                        256,
                                                        &hctl,
                                                        HASH_ELEM | HASH_BLOBS);
    }

    rmemcpy1(dbname.data, 0, ffdbmd->dbname, strlen(ffdbmd->dbname));
    dbnameentry = hash_search(parsertrail->transcache->sysdicts->by_datname2oid, dbname.data, HASH_ENTER, &found);
    if(false == found)
    {
        /* 添加 */
        dbnameentry->oid = ffdbmd->oid;
        rmemcpy1(dbnameentry->datname.data, 0, ffdbmd->dbname, strlen(ffdbmd->dbname));
    }

    /* 判断是否在一个事务内, 如果不在事务内, 为其分配一个新事物 */
    if (cur_txn == NULL)
    {
        /* 不在事务内, 创建一个新的txn */
        add_txn = true;
        cur_txn = ripple_txn_init(RIPPLE_FROZEN_TXNID, InvalidXLogRecPtr, InvalidXLogRecPtr);
        cur_txn->type = RIPPLE_TXN_TYPE_METADATA;
    }

    /* 拼装sysdictHis */
    if (ffdbmd)
    {
        /* pg_database */
        ripple_catalogdata *catalog_db = rmalloc0(sizeof(ripple_catalogdata));
        ripple_catalog_database_value *db_value = rmalloc0(sizeof(ripple_catalog_database_value));

        rmemset0(catalog_db, 0, 0, sizeof(ripple_catalogdata));
        rmemset0(db_value, 0, 0, sizeof(ripple_catalog_database_value));

        /* database catalogdata赋值 */
        catalog_db->op = RIPPLE_CATALOG_OP_INSERT;
        catalog_db->type = RIPPLE_CATALOG_TYPE_DATABASE;

        /* db_value赋值 */
        db_value->ripple_database = rmalloc0(sizeof(xk_pg_parser_sysdict_pgdatabase));
        rmemset0(db_value->ripple_database, 0, 0, sizeof(xk_pg_parser_sysdict_pgdatabase));
        db_value->oid = ffdbmd->oid;

        /* 
         * database结构体赋值
         * 
         * oid
         * datname
         */
        db_value->ripple_database->oid = ffdbmd->oid;
        strcpy(db_value->ripple_database->datname.data, ffdbmd->dbname);

        catalog_db->catalog = (void *) db_value;

        /* database 组装完毕, 附加到链表中 */
        cur_txn->sysdictHis = lappend(cur_txn->sysdictHis, catalog_db);
        sys_begin = list_tail(cur_txn->sysdictHis);
    }

    /* 创建METADATA stmt */
    /* 添加stmt, 作为系统表段标识 */
    stmt = ripple_txnstmt_init();
    if(NULL == stmt)
    {
        elog(RLOG_WARNING, "init txnstmt error");
        return false;
    }
    stmt->type = RIPPLE_TXNSTMT_TYPE_METADATA;

    metadata = ripple_txnstmt_metadata_init();
    if(NULL == metadata)
    {
        elog(RLOG_WARNING, "init metastmt error");
        return false;
    }
    stmt->stmt = (void *)metadata;
    metadata->begin = sys_begin;
    metadata->end = list_tail(cur_txn->sysdictHis);
    cur_txn->stmts = lappend(cur_txn->stmts, stmt);

    /* 如果是创建的新事物, 在这里将事务添加到缓存中 */
    if (add_txn)
    {
        parsertrail->dtxns = dlist_put(parsertrail->dtxns, cur_txn);
        if(NULL == parsertrail->dtxns)
        {
            elog(RLOG_WARNING, "parser trail database metadata add txn to dlist error");
            return false;
        }
        parsertrail->lasttxn = NULL;
    }
    return true;
}


/*
 * 数据库信息应用
 *  此处为独立的处理逻辑，函数流程到此说明前期的处理已经完成，在这只需要应用即可
*/
bool ripple_parsertrail_dbmetadataapply(ripple_parsertrail* parsertrail, void* data)
{
    bool found = false;
    ripple_ff_dbmetadata* dbmetadata = NULL;
    ripple_fftrail_privdata* privdata = NULL;
    ripple_fftrail_database_deserialentry* deserialentry = NULL;

    dbmetadata = (ripple_ff_dbmetadata*)data;

    /* 将数据库信息应用 */
    privdata = parsertrail->ffsmgrstate->fdata->ffdata;

    deserialentry = hash_search(privdata->databases, &dbmetadata->dbmdno, HASH_ENTER, &found);
    if(true == found)
    {
        elog(RLOG_ERROR, "in version 1.0, privdata->databases shoulde be empty");
    }

    /* 加入 */
    deserialentry->no = dbmetadata->dbmdno;
    deserialentry->oid = dbmetadata->oid;
    rmemcpy1(deserialentry->database, 0, dbmetadata->dbname, strlen(dbmetadata->dbname));

    /* 加入到链表中 */
    privdata->dbentrys = lappend(privdata->dbentrys, deserialentry);

    if(false == ripple_parsertrail_dbmetadata2hash(parsertrail, dbmetadata))
    {
        elog(RLOG_WARNING, "parser trail dbmetadata error");
        return false;
    }

    /* 日志级别为 debug */
    if(RLOG_DEBUG == g_loglevel)
    {
        /* 输出调试日志 */
        elog(RLOG_DEBUG, "----------Trail MetaDB Begin----------------");
        elog(RLOG_DEBUG, "dbmdno:           %u",    dbmetadata->header.dbmdno);
        elog(RLOG_DEBUG, "tbmdno:           %u",    dbmetadata->header.tbmdno);
        elog(RLOG_DEBUG, "transid:          %lu",   dbmetadata->header.transid);
        elog(RLOG_DEBUG, "transind:         %u",    dbmetadata->header.transind);
        elog(RLOG_DEBUG, "totallength:      %lu",   dbmetadata->header.totallength);
        elog(RLOG_DEBUG, "reclength:        %u",    dbmetadata->header.reclength);
        elog(RLOG_DEBUG, "reccount:         %u",    dbmetadata->header.reccount);
        elog(RLOG_DEBUG, "formattype:       %u",    dbmetadata->header.formattype);
        elog(RLOG_DEBUG, "subtype:          %u",    dbmetadata->header.subtype);
        elog(RLOG_DEBUG, "dbmdno:           %u",    dbmetadata->dbmdno);
        elog(RLOG_DEBUG, "oid:              %u",    dbmetadata->oid);
        elog(RLOG_DEBUG, "dbname:           %s",    dbmetadata->dbname);
        elog(RLOG_DEBUG, "charset:          %s",    dbmetadata->charset);
        elog(RLOG_DEBUG, "timezone:         %s",    dbmetadata->timezone);
        elog(RLOG_DEBUG, "money:            %s",    dbmetadata->money);
        elog(RLOG_DEBUG, "----------Trail MetaDB   End----------------");
    }
    return true;
}

/* 清理工作 */
void ripple_parsertrail_dbmetadataclean(ripple_parsertrail* parsertrail, void* data)
{
    ripple_ff_dbmetadata* dbmetadata = NULL;

    if (!data)
    {
        return;
    }

    dbmetadata = (ripple_ff_dbmetadata*)data;

    RIPPLE_UNUSED(parsertrail);

    /* 内存释放 */
    if(NULL != dbmetadata->dbname)
    {
        rfree(dbmetadata->dbname);
    }

    if(NULL != dbmetadata->charset)
    {
        rfree(dbmetadata->charset);
    }

    if(NULL != dbmetadata->timezone)
    {
        rfree(dbmetadata->timezone);
    }

    if(NULL != dbmetadata->money)
    {
        rfree(dbmetadata->money);
    }
    rfree(dbmetadata);
}
