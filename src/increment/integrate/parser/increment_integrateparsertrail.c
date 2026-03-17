#include "app_incl.h"
#include "port/file/fd.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/uuid.h"
#include "utils/string/stringinfo.h"
#include "threads/threads.h"
#include "queue/queue.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/cache_sysidcts.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/transcache.h"
#include "storage/ff_detail.h"
#include "storage/file_buffer.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "loadrecords/record.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "refresh/refresh_table_syncstats.h"
#include "refresh/integrate/refresh_integrate.h"
#include "stmts/txnstmt.h"
#include "parser/trail/parsertrail.h"
#include "increment/integrate/parser/increment_integrateparsertrail.h"

/* 由于onlinerefresh会等待refresh结束
 * 如果存量未完全发送且capture重启出现reset，导致onlinerefresh无法退出
 * 改为parse接收到onlinrefreshabandon时创建abandon文件
 * 管理线程检测到abandon文件时退出
 */
static bool increment_integrateparsertrail_writeonlinerefreshabandon(txn* txn)
{
    int fd = 0;
    List* luuid = NULL;
    ListCell* lc = NULL;
    char* traildir = NULL;
    StringInfo path = NULL;
    uuid_t* uuid = NULL;
    txnstmt* rstmt = NULL;

    if (NULL == txn || NULL == txn->stmts)
    {
        return false;
    }
    path = makeStringInfo();
    traildir = guc_getConfigOption(CFG_KEY_TRAIL_DIR);
    rstmt = (txnstmt*)(lfirst(list_head(txn->stmts)));

    luuid = (List*) rstmt->stmt;

    foreach(lc, luuid)
    {
        resetStringInfo(path);
        uuid = (uuid_t*)lfirst(lc);

        appendStringInfo(path, "%s/%s/%s", traildir, REFRESH_ONLINEREFRESH, uuid2string(uuid));

        osal_make_dir(path->data);

        appendStringInfo(path, "/%s", ONLINEREFRESHABANDON_DAT);

        /* 打开文件 */
        fd = osal_basic_open_file(path->data, O_RDWR | O_CREAT| BINARY);
        if (fd  < 0)
        {
            elog(RLOG_WARNING, "open file %s error %s", path->data, strerror(errno));
            deleteStringInfo(path);
            return false;
        }

        osal_file_close(fd);
    }
    deleteStringInfo(path);
    return true;

}

/* 初始化 */
increment_integrateparsertrail* increment_integrateparsertrail_init(void)
{
    int             mbytes = 0;
    uint64          bytes = 0;
    HASHCTL         hctl;
    increment_integrateparsertrail* integrateparsertrail = NULL;
    /* 初始化进程需要的信息 */
    integrateparsertrail = (increment_integrateparsertrail*)rmalloc1(sizeof(increment_integrateparsertrail));
    if(NULL == integrateparsertrail)
    {
        elog(RLOG_WARNING, "out of memory");
    }
    rmemset0(integrateparsertrail, 0, '\0', sizeof(increment_integrateparsertrail));
    integrateparsertrail->parsertrail.ffsmgrstate = NULL;
    integrateparsertrail->parsertrail.records = NULL;

    /* 初始化反序列化接口 */
    integrateparsertrail->parsertrail.ffsmgrstate = (ffsmgr_state*)rmalloc1(sizeof(ffsmgr_state));
    if(NULL == integrateparsertrail->parsertrail.ffsmgrstate)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(integrateparsertrail->parsertrail.ffsmgrstate, 0, '\0', sizeof(ffsmgr_state));
    integrateparsertrail->parsertrail.ffsmgrstate->status = FFSMGR_STATUS_NOP;
    integrateparsertrail->parsertrail.ffsmgrstate->bufid = 0;
    integrateparsertrail->parsertrail.ffsmgrstate->compatibility = guc_getConfigOptionInt(CFG_KEY_COMPATIBILITY);
    integrateparsertrail->parsertrail.ffsmgrstate->privdata = (void*)integrateparsertrail;
    integrateparsertrail->parsertrail.ffsmgrstate->callback.getrecords = NULL;
    integrateparsertrail->parsertrail.ffsmgrstate->fdata = NULL;        /* fdata->extradata ListCell, 当前在处理的cell */
                                                                        /* fdata->ffdata trail 文件对应的库/表结构      */
    integrateparsertrail->parsertrail.records = NULL;

    /* 换算文件的大小 */
    mbytes = guc_getConfigOptionInt(CFG_KEY_TRAIL_MAX_SIZE);
    bytes = MB2BYTE(mbytes);
    integrateparsertrail->parsertrail.ffsmgrstate->maxbufid = (bytes/FILE_BUFFER_SIZE);

    /* 调用初始化接口 */
    ffsmgr_init(FFSMG_IF_TYPE_TRAIL, integrateparsertrail->parsertrail.ffsmgrstate);
    integrateparsertrail->parsertrail.ffsmgrstate->ffsmgr->ffsmgr_init(FFSMGR_IF_OPTYPE_DESERIAL, integrateparsertrail->parsertrail.ffsmgrstate);

    /* 初始化 txncache */
    integrateparsertrail->parsertrail.transcache = rmalloc1(sizeof(transcache));
    if(NULL == integrateparsertrail->parsertrail.transcache)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(integrateparsertrail->parsertrail.transcache, 0, '\0', sizeof(transcache));

    /* 创建 hash */
    rmemset1(&hctl, 0, 0, sizeof(hctl));
    hctl.keysize = sizeof(FullTransactionId);
    hctl.entrysize = sizeof(txn);
    integrateparsertrail->parsertrail.transcache->by_txns = hash_create("integrate_trail_txn_hash",
                                                       2048,
                                                      &hctl,
                                                       HASH_ELEM | HASH_BLOBS);

    /* 初始化 sysdicts */
    integrateparsertrail->parsertrail.transcache->sysdicts = rmalloc0(sizeof(cache_sysdicts));
    if(NULL == integrateparsertrail->parsertrail.transcache->sysdicts)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(integrateparsertrail->parsertrail.transcache->sysdicts, 0, '\0', sizeof(cache_sysdicts));
    integrateparsertrail->state = INTEGRATE_STATUS_PARSER_INIT;

    return integrateparsertrail;
}


/*-------------------------refresh事务处理 begin------------------*/

static bool increment_integrateparsertrail_refresh(increment_integrateparsertrail* parser, txn* txn_obj)
{
    struct stat st;
    char file[MAXPATH]                       = {'\0'};
    txnstmt* txn_stmt                         = NULL;
    refresh_tables* refreshtables            = NULL;
    refresh_tables* refreshtblinstmt         = NULL;
    refresh_table_syncstats* tablesyncstats  = NULL;
    refresh_integrate* rintegrate            = NULL;

    /* 有refresh状态文件不生成refresh任务 */
    sprintf(file, "%s/%s", REFRESH_REFRESH, REFRESH_STATUS);

    /* 校验文件是否存在，存在返回true */
    if(0 == stat(file, &st))
    {
        return true;
    }

    if(0 == list_length(txn_obj->stmts))
    {
        elog(RLOG_WARNING, "refresh stmts is null");
        return false;
    }

    /* 获取 refresh stmt */
    txn_stmt = (txnstmt*)lfirst(list_head(txn_obj->stmts));
    refreshtblinstmt = (refresh_tables*)txn_stmt->stmt;

    refreshtables = refresh_tables_copy(refreshtblinstmt);
    tablesyncstats = refresh_table_syncstats_init();
    refresh_table_syncstats_tablesyncing_set(refreshtables, tablesyncstats);
    refresh_table_syncstats_tablesyncall_set(refreshtables, tablesyncstats);

    /* 初始化refresh mgr线程的相关结构 */
    rintegrate = refresh_integrate_init();
    if(NULL == rintegrate)
    {
        elog(RLOG_WARNING, "integrate refresh init refresh error");
        return false;
    }
    rintegrate->stat = REFRESH_INTEGRATE_STAT_INIT;

    /* 设置type */
    rintegrate->sync_stats = tablesyncstats;
    parser->callback.integratestate_addrefresh(parser->privdata, (void*)rintegrate);
    refresh_freetables(refreshtables);
    return true;
}

/*-------------------------refresh事务处理   end------------------*/

static bool increment_integrateparsertrail_txns2queue(increment_integrateparsertrail* parser)
{
    bool bret = true;
    txn* txn_obj = NULL;
    dlistnode* dlnode = NULL;

    for(dlnode = parser->parsertrail.dtxns->head; NULL != dlnode; dlnode = parser->parsertrail.dtxns->head)
    {
        parser->parsertrail.dtxns->head = dlnode->next;
        txn_obj = (txn*)dlnode->value;
        dlnode->value = NULL;

        if(TXN_TYPE_NORMAL == txn_obj->type
           || TXN_TYPE_SHIFTFILE == txn_obj->type
           || TXN_TYPE_ONLINEREFRESH_DATASET == txn_obj->type
           || TXN_TYPE_ABANDON == txn_obj->type
           || TXN_TYPE_METADATA == txn_obj->type)
        {
            /* 普通事务 */
            ;
        }
        else if(TXN_TYPE_ONLINEREFRESH_BEGIN == txn_obj->type
                || TXN_TYPE_ONLINEREFRESH_END == txn_obj->type
                || TXN_TYPE_ONLINEREFRESH_DATASET == txn_obj->type)
        {
            /* onlinerefresh 事务 */
            ;
        }
        else if(TXN_TYPE_REFRESH == txn_obj->type)
        {
            bret = increment_integrateparsertrail_refresh(parser, txn_obj);
            if(false == bret)
            {
                elog(RLOG_WARNING, "increment integrate parser refresh txn error");
                break;
            }
        }
        else if(TXN_TYPE_BIGTXN_BEGIN == txn_obj->type
                || TXN_TYPE_BIGTXN_END_COMMIT == txn_obj->type 
                || TXN_TYPE_BIGTXN_END_ABORT == txn_obj->type)
        {
            /* 大事务 */
            ;
        }
        else if(TXN_TYPE_RESET == txn_obj->type)
        {
            /* reset 清理哈希中事务*/
            HASH_SEQ_STATUS status;
            txn* entry = NULL;
            hash_seq_init(&status, parser->parsertrail.transcache->by_txns);
            while ((entry = hash_seq_search(&status)) != NULL)
            {
                txn_free(entry);
                hash_search(parser->parsertrail.transcache->by_txns, &entry->xid, HASH_REMOVE, NULL);
            }
        }
        else if(TXN_TYPE_ONLINEREFRESH_ABANDON == txn_obj->type)
        {
            bret = increment_integrateparsertrail_writeonlinerefreshabandon(txn_obj);
            txn_freevoid((void*)txn_obj);
            txn_obj = NULL;
            dlnode->value = NULL;
        }
        else
        {
            elog(RLOG_WARNING, "increment integrate unknown txn flag %u error", txn_obj->flag);
            bret = false;
        }

        if(false == bret)
        {
            elog(RLOG_WARNING, "increment integrate parser txn 2 queue error");
            break;
        }

        if (NULL != txn_obj)
        {
            parser->callback.setmetricloadlsn(parser->privdata, txn_obj->confirm.wal.lsn);
            parser->callback.setmetricloadtimestamp(parser->privdata, (TimestampTz)txn_obj->endtimestamp);
        }

        cache_txn_add(parser->parsertrail.parser2txn, txn_obj);
        dlnode->value = NULL;
        dlist_node_free(dlnode, NULL);
    }

    return bret;
}


/* 解析 trail 文件主函数 */
void* increment_integrateparsertrail_main(void *args)
{
    thrnode* thr_node                                     = NULL;
    increment_integrateparsertrail* intgrparsertrail     = NULL;

    thr_node = (thrnode*)args;

    intgrparsertrail = (increment_integrateparsertrail*)thr_node->data;

    /* 查看状态 */
    if(THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING, "increment integrate parser trail exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thr_node->stat = THRNODE_STAT_WORK;

    /* 进入工作 */
    while(1)
    {
        if(THRNODE_STAT_TERM == thr_node->stat)
        {
            /* 序列化/落盘 */
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* 获取数据, 超时退出 */
        intgrparsertrail->parsertrail.records = queue_get(intgrparsertrail->recordscache, NULL);
        if(true == dlist_isnull(intgrparsertrail->parsertrail.records))
        {
            /* 需要退出，等待 thr_node->stat 变为 TERM 后退出*/
            if(THRNODE_STAT_TERM != thr_node->stat)
            {
                /* 睡眠 10 毫秒 */
                usleep(10000);
                continue;
            }
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        /* 遍历 entry 解析 */
        if(false == parsertrail_parser(&intgrparsertrail->parsertrail))
        {
            elog(RLOG_WARNING, "integrate increment parser parser error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        /* 事务处理 */
        if(true == dlist_isnull(intgrparsertrail->parsertrail.dtxns))
        {
            continue;
        }

        if(false == increment_integrateparsertrail_txns2queue(intgrparsertrail))
        {
            elog(RLOG_WARNING, "increment integrate add txn 2 queue error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }
    }

    pthread_exit(NULL);
    return NULL;
}

/* 释放 */
void increment_integrateparsertrail_free(increment_integrateparsertrail* parser_trail)
{
    if(NULL == parser_trail)
    {
        return;
    }

    parsertrail_free((parsertrail*)parser_trail);

    parser_trail->privdata = NULL;
    parser_trail->recordscache = NULL;

    rfree(parser_trail);
    parser_trail = NULL;
}
