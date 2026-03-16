#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/string/stringinfo.h"
#include "threads/ripple_threads.h"
#include "queue/ripple_queue.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_transcache.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "loadrecords/ripple_record.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "refresh/ripple_refresh_table_syncstats.h"
#include "refresh/integrate/ripple_refresh_integrate.h"
#include "stmts/ripple_txnstmt.h"
#include "parser/trail/ripple_parsertrail.h"
#include "increment/integrate/parser/ripple_increment_integrateparsertrail.h"

/* 由于onlinerefresh会等待refresh结束
 * 如果存量未完全发送且capture重启出现reset，导致onlinerefresh无法退出
 * 改为parse接收到onlinrefreshabandon时创建abandon文件
 * 管理线程检测到abandon文件时退出
 */
static bool ripple_increment_integrateparsertrail_writeonlinerefreshabandon(ripple_txn* txn)
{
    int fd = 0;
    List* luuid = NULL;
    ListCell* lc = NULL;
    char* traildir = NULL;
    StringInfo path = NULL;
    ripple_uuid_t* uuid = NULL;
    ripple_txnstmt* rstmt = NULL;

    if (NULL == txn || NULL == txn->stmts)
    {
        return false;
    }
    path = makeStringInfo();
    traildir = guc_getConfigOption(RIPPLE_CFG_KEY_TRAIL_DIR);
    rstmt = (ripple_txnstmt*)(lfirst(list_head(txn->stmts)));

    luuid = (List*) rstmt->stmt;

    foreach(lc, luuid)
    {
        resetStringInfo(path);
        uuid = (ripple_uuid_t*)lfirst(lc);

        appendStringInfo(path, "%s/%s/%s", traildir, RIPPLE_REFRESH_ONLINEREFRESH, uuid2string(uuid));

        MakeDir(path->data);

        appendStringInfo(path, "/%s", RIPPLE_ONLINEREFRESHABANDON_DAT);

        /* 打开文件 */
        fd = BasicOpenFile(path->data, O_RDWR | O_CREAT| RIPPLE_BINARY);
        if (fd  < 0)
        {
            elog(RLOG_WARNING, "open file %s error %s", path->data, strerror(errno));
            deleteStringInfo(path);
            return false;
        }

        FileClose(fd);
    }
    deleteStringInfo(path);
    return true;

}

/* 初始化 */
ripple_increment_integrateparsertrail* ripple_increment_integrateparsertrail_init(void)
{
    int             mbytes = 0;
    uint64          bytes = 0;
    HASHCTL         hctl;
    ripple_increment_integrateparsertrail* integrateparsertrail = NULL;
    /* 初始化进程需要的信息 */
    integrateparsertrail = (ripple_increment_integrateparsertrail*)rmalloc1(sizeof(ripple_increment_integrateparsertrail));
    if(NULL == integrateparsertrail)
    {
        elog(RLOG_WARNING, "out of memory");
    }
    rmemset0(integrateparsertrail, 0, '\0', sizeof(ripple_increment_integrateparsertrail));
    integrateparsertrail->parsertrail.ffsmgrstate = NULL;
    integrateparsertrail->parsertrail.records = NULL;

    /* 初始化反序列化接口 */
    integrateparsertrail->parsertrail.ffsmgrstate = (ripple_ffsmgr_state*)rmalloc1(sizeof(ripple_ffsmgr_state));
    if(NULL == integrateparsertrail->parsertrail.ffsmgrstate)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(integrateparsertrail->parsertrail.ffsmgrstate, 0, '\0', sizeof(ripple_ffsmgr_state));
    integrateparsertrail->parsertrail.ffsmgrstate->status = RIPPLE_FFSMGR_STATUS_NOP;
    integrateparsertrail->parsertrail.ffsmgrstate->bufid = 0;
    integrateparsertrail->parsertrail.ffsmgrstate->compatibility = guc_getConfigOptionInt(RIPPLE_CFG_KEY_COMPATIBILITY);
    integrateparsertrail->parsertrail.ffsmgrstate->privdata = (void*)integrateparsertrail;
    integrateparsertrail->parsertrail.ffsmgrstate->callback.getrecords = NULL;
    integrateparsertrail->parsertrail.ffsmgrstate->fdata = NULL;        /* fdata->extradata ListCell, 当前在处理的cell */
                                                                        /* fdata->ffdata trail 文件对应的库/表结构      */
    integrateparsertrail->parsertrail.records = NULL;

    /* 换算文件的大小 */
    mbytes = guc_getConfigOptionInt(RIPPLE_CFG_KEY_TRAIL_MAX_SIZE);
    bytes = RIPPLE_MB2BYTE(mbytes);
    integrateparsertrail->parsertrail.ffsmgrstate->maxbufid = (bytes/RIPPLE_FILE_BUFFER_SIZE);

    /* 调用初始化接口 */
    ripple_ffsmgr_init(RIPPLE_FFSMG_IF_TYPE_TRAIL, integrateparsertrail->parsertrail.ffsmgrstate);
    integrateparsertrail->parsertrail.ffsmgrstate->ffsmgr->ffsmgr_init(RIPPLE_FFSMGR_IF_OPTYPE_DESERIAL, integrateparsertrail->parsertrail.ffsmgrstate);

    /* 初始化 txncache */
    integrateparsertrail->parsertrail.transcache = rmalloc1(sizeof(ripple_transcache));
    if(NULL == integrateparsertrail->parsertrail.transcache)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(integrateparsertrail->parsertrail.transcache, 0, '\0', sizeof(ripple_transcache));

    /* 创建 hash */
    rmemset1(&hctl, 0, 0, sizeof(hctl));
    hctl.keysize = sizeof(FullTransactionId);
    hctl.entrysize = sizeof(ripple_txn);
    integrateparsertrail->parsertrail.transcache->by_txns = hash_create("integrate_trail_txn_hash",
                                                       2048,
                                                      &hctl,
                                                       HASH_ELEM | HASH_BLOBS);

    /* 初始化 sysdicts */
    integrateparsertrail->parsertrail.transcache->sysdicts = rmalloc0(sizeof(ripple_cache_sysdicts));
    if(NULL == integrateparsertrail->parsertrail.transcache->sysdicts)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(integrateparsertrail->parsertrail.transcache->sysdicts, 0, '\0', sizeof(ripple_cache_sysdicts));
    integrateparsertrail->state = RIPPLE_INTEGRATE_STATUS_PARSER_INIT;

    return integrateparsertrail;
}


/*-------------------------refresh事务处理 begin------------------*/

static bool ripple_increment_integrateparsertrail_refresh(ripple_increment_integrateparsertrail* parser, ripple_txn* txn)
{
    struct stat st;
    char file[RIPPLE_MAXPATH]                       = {'\0'};
    ripple_txnstmt* txnstmt                         = NULL;
    ripple_refresh_tables* refreshtables            = NULL;
    ripple_refresh_tables* refreshtblinstmt         = NULL;
    ripple_refresh_table_syncstats* tablesyncstats  = NULL;
    ripple_refresh_integrate* rintegrate            = NULL;

    /* 有refresh状态文件不生成refresh任务 */
    sprintf(file, "%s/%s", RIPPLE_REFRESH_REFRESH, RIPPLE_REFRESH_STATUS);

    /* 校验文件是否存在，存在返回true */
    if(0 == stat(file, &st))
    {
        return true;
    }

    if(0 == list_length(txn->stmts))
    {
        elog(RLOG_WARNING, "refresh stmts is null");
        return false;
    }

    /* 获取 refresh stmt */
    txnstmt = (ripple_txnstmt*)lfirst(list_head(txn->stmts));
    refreshtblinstmt = (ripple_refresh_tables*)txnstmt->stmt;

    refreshtables = ripple_refresh_tables_copy(refreshtblinstmt);
    tablesyncstats = ripple_refresh_table_syncstats_init();
    ripple_refresh_table_syncstats_tablesyncing_set(refreshtables, tablesyncstats);
    ripple_refresh_table_syncstats_tablesyncall_set(refreshtables, tablesyncstats);

    /* 初始化refresh mgr线程的相关结构 */
    rintegrate = ripple_refresh_integrate_init();
    if(NULL == rintegrate)
    {
        elog(RLOG_WARNING, "integrate refresh init refresh error");
        return false;
    }
    rintegrate->stat = RIPPLE_REFRESH_INTEGRATE_STAT_INIT;

    /* 设置type */
    rintegrate->sync_stats = tablesyncstats;
    parser->callback.integratestate_addrefresh(parser->privdata, (void*)rintegrate);
    ripple_refresh_freetables(refreshtables);
    return true;
}

/*-------------------------refresh事务处理   end------------------*/

static bool ripple_increment_integrateparsertrail_txns2queue(ripple_increment_integrateparsertrail* parser)
{
    bool bret = true;
    ripple_txn* txn = NULL;
    dlistnode* dlnode = NULL;

    for(dlnode = parser->parsertrail.dtxns->head; NULL != dlnode; dlnode = parser->parsertrail.dtxns->head)
    {
        parser->parsertrail.dtxns->head = dlnode->next;
        txn = (ripple_txn*)dlnode->value;
        dlnode->value = NULL;

        if(RIPPLE_TXN_TYPE_NORMAL == txn->type
           || RIPPLE_TXN_TYPE_SHIFTFILE == txn->type
           || RIPPLE_TXN_TYPE_ONLINEREFRESH_DATASET == txn->type
           || RIPPLE_TXN_TYPE_ABANDON == txn->type
           || RIPPLE_TXN_TYPE_METADATA == txn->type)
        {
            /* 普通事务 */
            ;
        }
        else if(RIPPLE_TXN_TYPE_ONLINEREFRESH_BEGIN == txn->type
                || RIPPLE_TXN_TYPE_ONLINEREFRESH_END == txn->type
                || RIPPLE_TXN_TYPE_ONLINEREFRESH_DATASET == txn->type)
        {
            /* onlinerefresh 事务 */
            ;
        }
        else if(RIPPLE_TXN_TYPE_REFRESH == txn->type)
        {
            bret = ripple_increment_integrateparsertrail_refresh(parser, txn);
            if(false == bret)
            {
                elog(RLOG_WARNING, "increment integrate parser refresh txn error");
                break;
            }
        }
        else if(RIPPLE_TXN_TYPE_BIGTXN_BEGIN == txn->type
                || RIPPLE_TXN_TYPE_BIGTXN_END_COMMIT == txn->type 
                || RIPPLE_TXN_TYPE_BIGTXN_END_ABORT == txn->type)
        {
            /* 大事务 */
            ;
        }
        else if(RIPPLE_TXN_TYPE_RESET == txn->type)
        {
            /* reset 清理哈希中事务*/
            HASH_SEQ_STATUS status;
            ripple_txn* entry = NULL;
            hash_seq_init(&status, parser->parsertrail.transcache->by_txns);
            while ((entry = hash_seq_search(&status)) != NULL)
            {
                ripple_txn_free(entry);
                hash_search(parser->parsertrail.transcache->by_txns, &entry->xid, HASH_REMOVE, NULL);
            }
        }
        else if(RIPPLE_TXN_TYPE_ONLINEREFRESH_ABANDON == txn->type)
        {
            bret = ripple_increment_integrateparsertrail_writeonlinerefreshabandon(txn);
            ripple_txn_freevoid((void*)txn);
            txn = NULL;
            dlnode->value = NULL;
        }
        else
        {
            elog(RLOG_WARNING, "increment integrate unknown txn flag %u error", txn->flag);
            bret = false;
        }

        if(false == bret)
        {
            elog(RLOG_WARNING, "increment integrate parser txn 2 queue error");
            break;
        }

        if (NULL != txn)
        {
            parser->callback.setmetricloadlsn(parser->privdata, txn->confirm.wal.lsn);
            parser->callback.setmetricloadtimestamp(parser->privdata, (TimestampTz)txn->endtimestamp);
        }

        ripple_cache_txn_add(parser->parsertrail.parser2txn, txn);
        dlnode->value = NULL;
        dlist_node_free(dlnode, NULL);
    }

    return bret;
}


/* 解析 trail 文件主函数 */
void* ripple_increment_integrateparsertrail_main(void *args)
{
    ripple_thrnode* thrnode                                     = NULL;
    ripple_increment_integrateparsertrail* intgrparsertrail     = NULL;

    thrnode = (ripple_thrnode*)args;

    intgrparsertrail = (ripple_increment_integrateparsertrail*)thrnode->data;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "increment integrate parser trail exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    /* 进入工作 */
    while(1)
    {
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            /* 序列化/落盘 */
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 获取数据, 超时退出 */
        intgrparsertrail->parsertrail.records = ripple_queue_get(intgrparsertrail->recordscache, NULL);
        if(true == dlist_isnull(intgrparsertrail->parsertrail.records))
        {
            /* 需要退出，等待 thrnode->stat 变为 TERM 后退出*/
            if(RIPPLE_THRNODE_STAT_TERM != thrnode->stat)
            {
                /* 睡眠 10 毫秒 */
                usleep(10000);
                continue;
            }
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        /* 遍历 entry 解析 */
        if(false == ripple_parsertrail_parser(&intgrparsertrail->parsertrail))
        {
            elog(RLOG_WARNING, "integrate increment parser parser error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        /* 事务处理 */
        if(true == dlist_isnull(intgrparsertrail->parsertrail.dtxns))
        {
            continue;
        }

        if(false == ripple_increment_integrateparsertrail_txns2queue(intgrparsertrail))
        {
            elog(RLOG_WARNING, "increment integrate add txn 2 queue error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }
    }

    ripple_pthread_exit(NULL);
    return NULL;
}

/* 释放 */
void ripple_increment_integrateparsertrail_free(ripple_increment_integrateparsertrail* parsertrail)
{
    if(NULL == parsertrail)
    {
        return;
    }

    ripple_parsertrail_free((ripple_parsertrail*)parsertrail);

    parsertrail->privdata = NULL;
    parsertrail->recordscache = NULL;

    rfree(parsertrail);
    parsertrail = NULL;
}
