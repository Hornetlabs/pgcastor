#include "ripple_app_incl.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/hash/hash_search.h"
#include "threads/ripple_threads.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "queue/ripple_queue.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "filetransfer/ripple_filetransfer.h"
#include "filetransfer/ripple_filetransfer_ftp.h"
#include "task/ripple_task_slot.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "refresh/ripple_refresh_table_syncstats.h"
#include "refresh/pump/ripple_refresh_pump.h"
#include "stmts/ripple_txnstmt.h"
#include "stmts/ripple_txnstmt_onlinerefresh.h"
#include "onlinerefresh/ripple_onlinerefresh_persist.h"
#include "onlinerefresh/pump/ripple_onlinerefesh_pump.h"
#include "parser/trail/ripple_parsertrail.h"
#include "increment/pump/parser/ripple_increment_pumpparsertrail.h"

/* 重启collector卡在反序列化中时，parser获取状态 */
static int ripple_increment_pumpparsertrail_getparserstate(void* privdata)
{
    ripple_increment_pumpparsertrail* pumpparsertrail = NULL;

    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "parsertrail getparserstate exception, privdata point is NULL");
    }

    pumpparsertrail = (ripple_increment_pumpparsertrail*)privdata;

    return pumpparsertrail->state;
}

static bool ripple_increment_pumpparsertrail_check_work_state(ripple_increment_pumpparsertrail* parsertrail)
{
    if (parsertrail->state == RIPPLE_PUMP_STATUS_PARSER_WORKING)
    {
        return true;
    }

    if (parsertrail->state == RIPPLE_PUMP_STATUS_PARSER_INIT)
    {
        /* 清理record cache */
        ripple_queue_clear(parsertrail->recordscache, dlist_freevoid);
        ripple_parsertrail_reset(&parsertrail->parsertrail);
        parsertrail->parsertrail.ffsmgrstate->callback.getparserstate = ripple_increment_pumpparsertrail_getparserstate;
        parsertrail->parsertrail.ffsmgrstate->callback.getrecords = NULL;
        ripple_increment_pumpparsertrail_state_set(parsertrail, RIPPLE_PUMP_STATUS_PARSER_READY);
        parsertrail->callback.serialstate_state_set(parsertrail->privdata, RIPPLE_PUMP_STATUS_SERIAL_INIT);
    }
    else if (parsertrail->state == RIPPLE_PUMP_STATUS_PARSER_WORK)
    {
        parsertrail->callback.splittrail_state_set(parsertrail->privdata, RIPPLE_PUMP_STATUS_SPLIT_WORK);
        ripple_increment_pumpparsertrail_state_set(parsertrail, RIPPLE_PUMP_STATUS_PARSER_WORKING);
        return true;
    }
    return false;
}

/* 设置 state 的值 */
void ripple_increment_pumpparsertrail_state_set(ripple_increment_pumpparsertrail* parsertrail,int state)
{
    parsertrail->state = state;
}

/* 初始化pumpparsertrail */
ripple_increment_pumpparsertrail* ripple_increment_pumpparsertrail_init(void)
{
    int     mbytes = 0;
    uint64  bytes = 0;
    HASHCTL hctl;
    ripple_increment_pumpparsertrail* pumpparsertrail = NULL;

    pumpparsertrail = (ripple_increment_pumpparsertrail*)rmalloc0(sizeof(ripple_increment_pumpparsertrail));
    if(NULL == pumpparsertrail)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(pumpparsertrail, 0, '\0', sizeof(ripple_increment_pumpparsertrail));

    /* 初始化反序列化接口 */
    /* 初始化落盘文件接口 */
    pumpparsertrail->parsertrail.ffsmgrstate = (ripple_ffsmgr_state*)rmalloc0(sizeof(ripple_ffsmgr_state));
    if(NULL == pumpparsertrail->parsertrail.ffsmgrstate)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(pumpparsertrail->parsertrail.ffsmgrstate, 0, '\0', sizeof(ripple_ffsmgr_state));
    pumpparsertrail->parsertrail.ffsmgrstate->status = RIPPLE_FFSMGR_STATUS_NOP;
    pumpparsertrail->parsertrail.ffsmgrstate->bufid = 0;
    pumpparsertrail->parsertrail.ffsmgrstate->compatibility = guc_getConfigOptionInt(RIPPLE_CFG_KEY_COMPATIBILITY);
    pumpparsertrail->parsertrail.ffsmgrstate->privdata = (void*)pumpparsertrail;
    pumpparsertrail->parsertrail.ffsmgrstate->callback.getrecords = NULL;
    pumpparsertrail->parsertrail.ffsmgrstate->fdata = NULL;      /* fdata->extradata ListCell, 当前在处理的cell */
                                                    /* fdata->ffdata trail 文件对应的库/表结构      */
    pumpparsertrail->parsertrail.records = NULL;

    /* 换算文件的大小 */
    mbytes = guc_getConfigOptionInt(RIPPLE_CFG_KEY_TRAIL_MAX_SIZE);
    bytes = RIPPLE_MB2BYTE(mbytes);
    pumpparsertrail->parsertrail.ffsmgrstate->maxbufid = (bytes/RIPPLE_FILE_BUFFER_SIZE);

    /* 调用初始化接口 */
    ripple_ffsmgr_init(RIPPLE_FFSMG_IF_TYPE_TRAIL, pumpparsertrail->parsertrail.ffsmgrstate);
    pumpparsertrail->parsertrail.ffsmgrstate->ffsmgr->ffsmgr_init(RIPPLE_FFSMGR_IF_OPTYPE_DESERIAL, pumpparsertrail->parsertrail.ffsmgrstate);

    /* 初始化 txncache */
    pumpparsertrail->parsertrail.transcache = rmalloc1(sizeof(ripple_transcache));
    if(NULL == pumpparsertrail->parsertrail.transcache)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(pumpparsertrail->parsertrail.transcache, 0, '\0', sizeof(ripple_transcache));

    /* 创建 hash */
    rmemset1(&hctl, 0, 0, sizeof(hctl));
    hctl.keysize = sizeof(TransactionId);
    hctl.entrysize = sizeof(ripple_txn);
    pumpparsertrail->parsertrail.transcache->by_txns = hash_create("increment_pumpparsertrail_txn_hash",
                                                       2048,
                                                      &hctl,
                                                       HASH_ELEM | HASH_BLOBS);

    /* 初始化 sysdicts */
    pumpparsertrail->parsertrail.transcache->sysdicts = rmalloc0(sizeof(ripple_cache_sysdicts));
    if(NULL == pumpparsertrail->parsertrail.transcache->sysdicts)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(pumpparsertrail->parsertrail.transcache->sysdicts, 0, '\0', sizeof(ripple_cache_sysdicts));

    ripple_increment_pumpparsertrail_state_set(pumpparsertrail, RIPPLE_PUMP_STATUS_PARSER_RESET);

    return pumpparsertrail;
}

/*-------------------------大事务处理 begin-----------------------*/
/* 大事务--- begin 处理 */
static bool ripple_increment_pumpparsertrail_bigtxnbegin(ripple_increment_pumpparsertrail* parser, ripple_txn* txn)
{
    ripple_recpos pos = {{'0'}};
    /* 添加大事务节点 */
    pos.trail.fileid = txn->segno;
    pos.trail.offset = txn->end.trail.offset;
    parser->callback.bigtxn_add(parser->privdata, txn->xid, &pos);
    return true;
}

/* 大事务---end 处理 */
static bool ripple_increment_pumpparsertrail_bigtxnend(ripple_increment_pumpparsertrail* parser, ripple_txn* txn)
{
    ripple_recpos pos = {{'0'}};
    pos.trail.fileid = txn->segno;
    pos.trail.offset = txn->end.trail.offset;
    parser->callback.bigtxn_end(parser->privdata, txn->xid, &pos);
    return true;
}

/* 大事务---放弃未接受到end的大事务 */
static bool ripple_increment_pumpparsertrail_bigtxnsetabandon(ripple_increment_pumpparsertrail* parser)
{
    return parser->callback.bigtxn_setabandon(parser->privdata);
}

/*-------------------------大事务处理   end-----------------------*/

/*-------------------------onlinerefresh事务处理 begin------------*/
static bool ripple_increment_pumpparsertrail_onlinerefreshbegin(ripple_increment_pumpparsertrail* parser, ripple_txn* txn)
{
    ripple_txnstmt* txnstmt = NULL;
    ripple_refresh_tables* tables                       = NULL;
    ripple_refresh_table_syncstats* tablesyncstats      = NULL;
    ripple_txnstmt_onlinerefresh* onlinerefresh         = NULL;
    ripple_onlinerefresh_pump *pumpolrefresh            = NULL;

    if(0 == list_length(txn->stmts))
    {
        elog(RLOG_WARNING, "pump onlinerefesh begin stmts is null");
        return false;
    }

    /* 获取 onlinerefresh 节点 */
    txnstmt = (ripple_txnstmt*)lfirst(list_head(txn->stmts));
    onlinerefresh = (ripple_txnstmt_onlinerefresh*)txnstmt->stmt;

    /* 注册onlinerefresh节点 */
    tables = ripple_refresh_tables_copy(onlinerefresh->refreshtables);
    tablesyncstats = ripple_refresh_table_syncstats_init();
    if(NULL == tablesyncstats)
    {
        elog(RLOG_WARNING, "pump onlinerefres init table syncstat error");
        return false;
    }
    ripple_refresh_table_syncstats_tablesyncing_set(tables, tablesyncstats);
    ripple_refresh_table_syncstats_tablesyncall_set(tables, tablesyncstats);

    pumpolrefresh = ripple_onlinerefresh_pump_init();
    if(NULL == pumpolrefresh)
    {
        elog(RLOG_WARNING, "pump onlinerefres init onlinerefresh error");
        return false;
    }

    pumpolrefresh->begin.trail.fileid = txn->segno;
    pumpolrefresh->begin.trail.offset = txn->end.trail.offset;

    pumpolrefresh->increment = onlinerefresh->increment;
    rmemcpy1(pumpolrefresh->no.data, 0, onlinerefresh->no->data, RIPPLE_UUID_LEN);
    pumpolrefresh->stat = RIPPLE_ONLINEREFRESH_PUMP_INIT;

    /* 网闸处理,获取网闸队列并设置 */
    pumpolrefresh->filetransfernode = (ripple_queue*)parser->callback.filetransfernode_get(parser->privdata);

    /* 添加 onlinerefresh 节点 */
    pumpolrefresh->tablesyncstats = tablesyncstats;
    parser->callback.addonlinerefresh(parser->privdata, pumpolrefresh);
    ripple_refresh_freetables(tables);
    return true;
}

/* onlinerefresh--- begin 处理 */
static bool ripple_increment_pumpparsertrail_onlinerefreshend(ripple_increment_pumpparsertrail* parser, ripple_txn* txn)
{
    ripple_recpos pos = {{'0'}};
    ripple_txnstmt* stmt = NULL;
    /* 添加大事务节点 */
    pos.trail.fileid = txn->segno;
    pos.trail.offset = txn->end.trail.offset;
    stmt = (ripple_txnstmt*)lfirst(list_head(txn->stmts));
    parser->callback.onlinerefresh_end(parser->privdata, stmt->stmt, &pos);
    return true;
}

/* onlinerefresh---放弃未接受到end的大事务 */
static bool ripple_increment_pumpparsertrail_onlinerefreshsetabandon(ripple_increment_pumpparsertrail* parser)
{
    bool result = false;
    List* uuid_list = NULL;
    ripple_txnstmt* stmt = NULL;
    ripple_txn* txn = NULL;

    result = parser->callback.onlinerefresh_setabandon(parser->privdata, (void**)&uuid_list);
    if (NULL == uuid_list)
    {
        return true;
    }

    txn = ripple_txn_init(RIPPLE_FROZEN_TXNID, InvalidXLogRecPtr, RIPPLE_MAX_LSN);
    stmt = ripple_txnstmt_init();
    stmt->stmt = uuid_list;
    stmt->type = RIPPLE_TXNSTMT_TYPE_ONLINEREFRESHABANDON;

    txn->stmts = lappend(txn->stmts, stmt);
    ripple_txn_addcommit(txn);
    ripple_cache_txn_add(parser->parsertrail.parser2txn, txn);

    return result;
}

/*-------------------------onlinerefresh事务处理   end------------*/

/*-------------------------refresh事务处理 begin------------------*/
static bool ripple_increment_pumpparsertrail_refresh(ripple_increment_pumpparsertrail* parser, ripple_txn* txn)
{
    ripple_txnstmt* txnstmt = NULL;
    ripple_refresh_tables* refreshtables = NULL;
    ripple_refresh_tables* refreshtblinstmt = NULL;
    ripple_refresh_table_syncstats* tablesyncstats = NULL;
    ripple_refresh_pump* rpump = NULL;

    if(0 == list_length(txn->stmts))
    {
        elog(RLOG_WARNING, "pump refresh stmts is null");
        return false;
    }

    /* 
     * refresh 启动之处有2处
     *  1、通过解析 trail 文件获取到 refresh
     *  2、在 1 的过程中, 因为网络的原因导致 refresh 中断, 此时在连接上collector后会根据状态文件启动 refresh
     *      但是在此过程中, 因为解析是从文件的头开始的, 所以可能会再次 走到了 1 处, 所以会有 isrefreshdown 的判断
     * 
     * TODO, 感觉此处的 isrefreshdown 可以去掉, 因为 collector 在接收到 refresh 后会切换文件.......
     */
    if (false == parser->callback.isrefreshdown(parser->privdata))
    {
        return true;
    }

    /* 获取 refresh stmt */
    txnstmt = (ripple_txnstmt*)lfirst(list_head(txn->stmts));
    refreshtblinstmt = (ripple_refresh_tables*)txnstmt->stmt;

    refreshtables = ripple_refresh_tables_copy(refreshtblinstmt);
    tablesyncstats = ripple_refresh_table_syncstats_init();
    ripple_refresh_table_syncstats_tablesyncing_set(refreshtables, tablesyncstats);
    ripple_refresh_table_syncstats_tablesyncall_set(refreshtables, tablesyncstats);

    /* 初始化refresh mgr线程的相关结构 */
    rpump = ripple_refresh_pump_init();
    if(NULL == rpump)
    {
        elog(RLOG_WARNING, "pump refresh init refresh error");
        return false;
    }
    rpump->stat = RIPPLE_REFRESH_PUMP_STAT_INIT;
    rpump->sync_stats = tablesyncstats;

    parser->callback.addrefresh(parser->privdata, (void*)rpump);

    /* 生成refresh分片数文件并加入到队列中(网闸相关) */
    ripple_filetransfer_pumprefresh(rpump, refreshtables);

    ripple_refresh_freetables(refreshtables);
    return true;
}

/*-------------------------refresh事务处理   end------------------*/

static bool ripple_increment_pumpparsertrail_txns2queue(ripple_increment_pumpparsertrail* parser)
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
           || RIPPLE_TXN_TYPE_ONLINEREFRESH_ABANDON == txn->type
           || RIPPLE_TXN_TYPE_ABANDON == txn->type
           || RIPPLE_TXN_TYPE_METADATA == txn->type)
        {
            /* 普通事务 */
            ;
        }
        else if(RIPPLE_TXN_TYPE_BIGTXN_BEGIN == txn->type)
        {
            bret = ripple_increment_pumpparsertrail_bigtxnbegin(parser, txn);
        }
        else if(RIPPLE_TXN_TYPE_BIGTXN_END_COMMIT == txn->type || RIPPLE_TXN_TYPE_BIGTXN_END_ABORT == txn->type)
        {
            bret = ripple_increment_pumpparsertrail_bigtxnend(parser, txn);
        }
        else if(RIPPLE_TXN_TYPE_ONLINEREFRESH_BEGIN == txn->type)
        {
            bret = ripple_increment_pumpparsertrail_onlinerefreshbegin(parser, txn);
        }
        else if(RIPPLE_TXN_TYPE_ONLINEREFRESH_END == txn->type)
        {
            bret = ripple_increment_pumpparsertrail_onlinerefreshend(parser, txn);
        }
        else if(RIPPLE_TXN_TYPE_REFRESH == txn->type)
        {
            /* refresh 注册 */
            bret = ripple_increment_pumpparsertrail_refresh(parser, txn);
            if(false == bret)
            {
                bret = false;
                elog(RLOG_WARNING, "increment pump parser refresh txn error");
                break;
            }
        }
        else if (RIPPLE_TXN_TYPE_RESET == txn->type)
        {
            /* reset处理未接受到end的大事务 */
            HASH_SEQ_STATUS status;
            ripple_txn* entry = NULL;
            hash_seq_init(&status, parser->parsertrail.transcache->by_txns);
            while ((entry = hash_seq_search(&status)) != NULL)
            {
                ripple_txn_free(entry);
                hash_search(parser->parsertrail.transcache->by_txns, &entry->xid, HASH_REMOVE, NULL);
            }
            bret = ripple_increment_pumpparsertrail_bigtxnsetabandon(parser);
            bret = (true == ripple_increment_pumpparsertrail_onlinerefreshsetabandon(parser)) ? bret : false;
            ripple_txn_freevoid((void*)txn);
            txn = NULL;
        }
        else
        {
            bret = false;
            elog(RLOG_WARNING, "increment pump unknown txn flag %u error", txn->flag);
        }

        if(false == bret)
        {
            bret = false;
            elog(RLOG_WARNING, "increment pump parser txn 2 queue error");
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

void* ripple_increment_pumpparsertrail_main(void* args)
{
    int timeout = 0;
    ripple_thrnode* thrnode = NULL;
    ripple_parsertrail* parsertrail = NULL;
    ripple_increment_pumpparsertrail* pumpparsertrail = NULL;

    thrnode = (ripple_thrnode*)args;

    pumpparsertrail = (ripple_increment_pumpparsertrail*)thrnode->data;

    parsertrail = (ripple_parsertrail*)pumpparsertrail;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "pump increment parser stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    while(1)
    {
        if (RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        if (!ripple_increment_pumpparsertrail_check_work_state(pumpparsertrail))
        {
            /* 睡眠 10 毫秒 */
            usleep(10000);
            continue;
        }

        /* 获取数据, 超时退出 */
        parsertrail->records = ripple_queue_get(pumpparsertrail->recordscache, &timeout);
        if(NULL == parsertrail->records)
        {
            /* 需要退出，等待 worknode->status 变为 RIPPLE_WORK_STATUS_TERM 后退出*/
            if(RIPPLE_ERROR_TIMEOUT == timeout)
            {
                continue;
            }

            elog(RLOG_WARNING, "pump increment parser get records from queue error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        if(false == ripple_parsertrail_parser(parsertrail))
        {
            elog(RLOG_WARNING, "pump increment parser parser error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        /* 事务处理 */
        if(true == dlist_isnull(parsertrail->dtxns))
        {
            continue;
        }

        if(false == ripple_increment_pumpparsertrail_txns2queue(pumpparsertrail))
        {
            elog(RLOG_WARNING, "increment pump add txn 2 queue error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }
    }

    return NULL;
}

void ripple_increment_pumpparsertrail_free(ripple_increment_pumpparsertrail* parsertrail)
{
    if(NULL == parsertrail)
    {
        return;
    }

    ripple_parsertrail_free((ripple_parsertrail*)parsertrail);

    parsertrail->privdata = NULL;
    parsertrail->parsertrail.parser2txn = NULL;
    parsertrail->recordscache = NULL;

    rfree(parsertrail);
    parsertrail = NULL;
    elog(RLOG_INFO, "ripple_increment_pumpparsertrail_free");

}