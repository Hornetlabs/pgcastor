#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/thread/ripple_thread.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/hash/hash_search.h"
#include "utils/regex/ripple_regex.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "loadrecords/ripple_record.h"
#include "queue/ripple_queue.h"
#include "threads/ripple_threads.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "net/netpacket/ripple_netpacket.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "utils/mpage/mpage.h"
#include "queue/ripple_queue.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadpagefromfile.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadwalrecords.h"
#include "cache/ripple_fpwcache.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "misc/ripple_misc_stat.h"
#include "misc/ripple_misc_control.h"
#include "queue/ripple_queue.h"
#include "snapshot/ripple_snapshot.h"
#include "refresh/ripple_refresh_tables.h"
#include "stmts/ripple_txnstmt_refresh.h"
#include "stmts/ripple_txnstmt_onlinerefresh.h"
#include "works/parserwork/wal/ripple_onlinerefresh.h"
#include "onlinerefresh/capture/ripple_onlinerefresh_capture.h"
#include "works/splitwork/wal/ripple_splitwork_wal.h"
#include "works/splitwork/wal/ripple_wal_define.h"
#include "works/parserwork/wal/ripple_rewind.h"
#include "works/parserwork/wal/ripple_parserwork_decode.h"
#include "works/parserwork/wal/ripple_decode_checkpoint.h"
#include "onlinerefresh/capture/parserwal/ripple_onlinerefresh_capture_parser.h"
#include "metric/capture/ripple_metric_capture.h"
#include "strategy/ripple_filter_dataset.h"
#include "onlinerefresh/capture/parserwal/ripple_onlinerefresh_capture_decode.h"
#include "works/parserwork/wal/ripple_parserwork_wal.h"


static ripple_decodingcontext *ripple_onlinerefresh_captureparser_decodingctxinit(void)
{
    ripple_decodingcontext*  decodingctx = NULL;
    HASHCTL hash_ctl;
    if(NULL != decodingctx)
    {
        return decodingctx;
    }

    decodingctx = (ripple_decodingcontext*)rmalloc1(sizeof(ripple_decodingcontext));
    if(NULL == decodingctx)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(decodingctx, 0, '\0', sizeof(ripple_decodingcontext));
    decodingctx->decode_record = NULL;
    decodingctx->stat = RIPPLE_DECODINGCONTEXT_INIT;
    decodingctx->filterbigtrans = false;

     /* 在磁盘中加载 BASE 信息 */
    ripple_misc_stat_loaddecode((void*)&decodingctx->base);
    decodingctx->decode_record = NULL;

    decodingctx->transcache = (ripple_transcache*)rmalloc0(sizeof(ripple_transcache));
    if(NULL == decodingctx->transcache)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(decodingctx->transcache, 0, '\0', sizeof(ripple_transcache));
    decodingctx->transcache->tableincludes = ripple_filter_dataset_inittableinclude(decodingctx->transcache->tableincludes);
    decodingctx->transcache->tableexcludes = ripple_filter_dataset_inittableexclude(decodingctx->transcache->tableexcludes);
    decodingctx->transcache->addtablepattern = ripple_filter_dataset_initaddtablepattern(decodingctx->transcache->addtablepattern);

    /* 添加解析库需要的信息 */
    decodingctx->walpre.m_dbtype = g_idbtype;
    decodingctx->walpre.m_dbversion = guc_getConfigOption(RIPPLE_CFG_KEY_DBVERION);
    decodingctx->walpre.m_debugLevel = 0;
    decodingctx->walpre.m_pagesize = g_blocksize;
    decodingctx->walpre.m_walLevel = XK_PG_PARSER_WALLEVEL_LOGICAL;
    decodingctx->walpre.m_record = NULL;

    /* 初始化链表结构 */
    decodingctx->transcache->transdlist = (ripple_txn_dlist*)rmalloc0(sizeof(ripple_txn_dlist));
    if(NULL == decodingctx->transcache->transdlist)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(decodingctx->transcache->transdlist, 0, '\0', sizeof(ripple_txn_dlist));
    decodingctx->transcache->transdlist->head = NULL;
    decodingctx->transcache->transdlist->tail = NULL;

    /* 初始化capture_buffer 并转换为字节大小 */
    decodingctx->transcache->capture_buffer = RIPPLE_MB2BYTE(guc_getConfigOptionInt(RIPPLE_CFG_KEY_CAPTURE_BUFFER));

    /* 初始化事务HASH表 */
    rmemset1(&hash_ctl, 0, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(FullTransactionId);
    hash_ctl.entrysize = sizeof(ripple_txn);
    decodingctx->transcache->by_txns = hash_create("transaction hash", 8192, &hash_ctl,
                                                        HASH_ELEM | HASH_BLOBS);
    /* 初始化sysdicts */
    decodingctx->transcache->sysdicts = (ripple_cache_sysdicts*)rmalloc0(sizeof(ripple_cache_sysdicts));
    if(NULL == decodingctx->transcache->sysdicts)
    {
        elog(RLOG_ERROR, "out of memeory");
    }
    rmemset0(decodingctx->transcache->sysdicts, 0, '\0', sizeof(ripple_cache_sysdicts));

    /* rewind初始化 */
    decodingctx->rewind = (ripple_rewind*)rmalloc0(sizeof(ripple_rewind));
    if(NULL == decodingctx->rewind)
    {
        elog(RLOG_ERROR, "out of memeory");
    }
    rmemset0(decodingctx->rewind, 0, '\0', sizeof(ripple_rewind));

    /* fpw 初始化 */
    decodingctx->transcache->by_fpwtuples = ripple_fpwcache_init(decodingctx->transcache);

    return decodingctx;
}

/* 生成过滤集, 仅包含等待增量工作的表 */
bool ripple_onlinerefresh_captureparser_datasetinit(ripple_decodingcontext *ctx, ripple_onlinerefresh_capture* onlinerefresh)
{
    ripple_refresh_tables  *tables = onlinerefresh->tables;
    ripple_refresh_table   *table_node = NULL;
    HASHCTL hashCtl_o2d = {'\0'};
    ripple_filter_oid2datasetnode *temp_o2d_entry = NULL;

    /* 创建oid2dataset hash */
    hashCtl_o2d.keysize = sizeof(Oid);
    hashCtl_o2d.entrysize = sizeof(ripple_filter_oid2datasetnode);

    ctx->transcache->hsyncdataset = hash_create("online refresh filter_o2d_htab",
                                       256,
                                      &hashCtl_o2d,
                                       HASH_ELEM | HASH_BLOBS);

    table_node = tables->tables;

    while (table_node)
    {
        temp_o2d_entry = hash_search(ctx->transcache->hsyncdataset, &table_node->oid, HASH_ENTER, NULL);
        if (!temp_o2d_entry)
        {
            elog(RLOG_WARNING, "can't insert hsyncdataset hash");
            return false;
        }
        temp_o2d_entry->oid = table_node->oid;
        strcpy(temp_o2d_entry->dataset.schema, table_node->schema);
        strcpy(temp_o2d_entry->dataset.table, table_node->table);

        table_node = table_node->next;
    }

    return true;
}


ripple_onlinerefresh_captureparser *ripple_onlinerefresh_captureparser_init(void)
{
    ripple_onlinerefresh_captureparser *result = NULL;

    result = rmalloc0(sizeof(ripple_onlinerefresh_captureparser));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    result = rmemset0(result, 0, 0, sizeof(ripple_onlinerefresh_captureparser));
    result->decodingctx = ripple_onlinerefresh_captureparser_decodingctxinit();

    return result;
}

/*
 * 设置解析器需要的基础信息
 *  1、字符集/时区/源字符集/目标字符集
 *  2、加载系统字典
 *  3、构建checkpoint信息
*/
void ripple_onlinerefresh_captureparser_loadmetadata(ripple_onlinerefresh_captureparser* olcparser)
{
    Oid dbid = InvalidOid;
    ripple_decodingcontext* decodingctx = NULL;

    decodingctx = olcparser->decodingctx;

    /* 在磁盘中加载 BASE 信息 */
    ripple_misc_stat_loaddecode((void*)&decodingctx->base);

    decodingctx->database = ripple_misc_controldata_database_get(NULL);
    decodingctx->monetary = rstrdup(ripple_misc_controldata_monetary_get());
    decodingctx->numeric = rstrdup(ripple_misc_controldata_numeric_get());
    decodingctx->tzname = rstrdup(ripple_misc_controldata_timezone_get());
    decodingctx->orgdbcharset = rstrdup(ripple_misc_controldata_orgencoding_get());
    decodingctx->tgtdbcharset = ripple_misc_controldata_dstencoding_get();

    /*加载字典表*/
    dbid = ripple_misc_controldata_database_get(NULL);
    ripple_cache_sysdictsload((void**)&decodingctx->transcache->sysdicts);

    decodingctx->transcache->htxnfilterdataset = ripple_filter_dataset_txnfilterload(decodingctx->transcache->sysdicts->by_namespace,
                                                                                     decodingctx->transcache->sysdicts->by_class);

    decodingctx->transcache->sysdicts->by_relfilenode = ripple_cache_sysdicts_buildrelfilenode2oid(dbid,
                                                                                    (void*)decodingctx->transcache->sysdicts);
    /* checkpoint 节点初始化 */
    ripple_decode_chkpt_init(decodingctx, decodingctx->base.redolsn);

    elog(RLOG_INFO, "capture onlinerefresh parser from, restartlsn %X/%X, last commit lsn:%X/%X, fileid:%lu, offset:%u, curtlid:%u",
                (uint32)(decodingctx->base.restartlsn>>32), (uint32)decodingctx->base.restartlsn,
                (uint32)(decodingctx->base.confirmedlsn>>32), (uint32)decodingctx->base.confirmedlsn,
                decodingctx->base.fileid, decodingctx->base.fileoffset, decodingctx->base.curtlid);
}

/* 遍历解析 record */
static void ripple_onlinerefresh_captureparser_parser(ripple_decodingcontext* decodingctx, dlist* record_dlist)
{
    ripple_record* record = NULL;
    ripple_onlinerefresh_capture* olcapture = (ripple_onlinerefresh_capture *)decodingctx->privdata;
    dlistnode* dlnode = NULL;

    dlnode = record_dlist->head;

    while (dlnode)
    {
        record = (ripple_record*)dlnode->value;

        /* 解析 record */
        decodingctx->decode_record = record;

        /* 调用解析函数 */
        g_parserecno++;
        ripple_parserwork_waldecode_onlinerefresh(decodingctx);
        decodingctx->decode_record = NULL;

        if (ripple_onlinerefresh_capture_xids_isnull(olcapture))
        {
            break;
        }
        dlnode = dlnode->next;
    }
}

void *ripple_onlinerefresh_captureparser_main(void* args)
{
    int timeout = 0;
    ripple_thrnode* thrnode = NULL;
    ripple_decodingcontext* decodingctx = NULL;
    ripple_onlinerefresh_capture *olcapture = NULL;
    ripple_onlinerefresh_captureparser * parser_task = NULL;
    dlist* record_dlist = NULL;

    thrnode = (ripple_thrnode*)args;

    parser_task = (ripple_onlinerefresh_captureparser*)thrnode->data;
    decodingctx = parser_task->decodingctx;
    olcapture = (ripple_onlinerefresh_capture *)decodingctx->privdata;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "onlinerefresh capture parser stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
        return NULL;
    }
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    while(1)
    {
        record_dlist = NULL;

        /* 首先判断是否接收到退出信号 */
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 查看是否解析完成 */
        if(true == ripple_onlinerefresh_capture_xids_isnull(olcapture))
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 获取数据 */
        record_dlist = ripple_queue_get(decodingctx->recordqueue, &timeout);
        if(NULL == record_dlist)
        {
            if(RIPPLE_ERROR_TIMEOUT == timeout)
            {
                continue;
            }
            elog(RLOG_WARNING, "onlinerefresh capture get records error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        /* 根据 entry 的内容获取数据 */
        ripple_onlinerefresh_captureparser_parser(decodingctx, record_dlist);

        /* record 双向链表 内存释放 */
        dlist_free(record_dlist, (dlistvaluefree )ripple_record_free);
    }
    ripple_pthread_exit(NULL);
    return NULL;
}

void ripple_onlinerefresh_captureparser_free(void* args)
{
    ripple_onlinerefresh_captureparser *olparserwal = NULL;

    olparserwal = (ripple_onlinerefresh_captureparser*)args;
    if (olparserwal)
    {
        if (olparserwal->decodingctx)
        {
            ripple_parserwork_wal_destroy(olparserwal->decodingctx);
        }
        rfree(olparserwal);
    }
}

