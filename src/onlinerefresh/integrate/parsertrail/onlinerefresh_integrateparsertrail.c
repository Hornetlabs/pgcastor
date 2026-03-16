#include "ripple_app_incl.h"
#include "port/thread/ripple_thread.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "queue/ripple_queue.h"
#include "threads/ripple_threads.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "parser/trail/ripple_parsertrail.h"
#include "increment/integrate/parser/ripple_increment_integrateparsertrail.h"
#include "onlinerefresh/integrate/parsertrail/ripple_onlinerefresh_integrateparsertrail.h"
 

/* 逻辑读取主线程 */
ripple_onlinerefresh_integrateparsertrail* ripple_onlinerefresh_integrateparsertrail_init(void)
{
    ripple_onlinerefresh_integrateparsertrail* traildecodecxt = NULL;

    traildecodecxt = (ripple_onlinerefresh_integrateparsertrail*)rmalloc0(sizeof(ripple_onlinerefresh_integrateparsertrail));
    if(NULL == traildecodecxt)
    {
        elog(RLOG_WARNING, "onlinerefresh integrateparsertrail malloc out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(traildecodecxt, 0, 0, sizeof(ripple_onlinerefresh_integrateparsertrail));

    traildecodecxt->decodingctx = ripple_increment_integrateparsertrail_init();
    return traildecodecxt;
}

/*
 * false    处理出错
 * true     处理成功
 * 
 * 入参:
 * bexit    true 可以退出了
 *          false 还不可以退出
*/
static bool ripple_onlinerefresh_integrateparsertrail_txns2queue(ripple_increment_integrateparsertrail* parser, bool* bexit)
{
    ripple_txn* txn = NULL;
    dlistnode* dlnode = NULL;

    for(dlnode = parser->parsertrail.dtxns->head; NULL != dlnode; dlnode = parser->parsertrail.dtxns->head)
    {
        parser->parsertrail.dtxns->head = dlnode->next;
        txn = (ripple_txn*)dlnode->value;
        dlnode->value = NULL;

        if(RIPPLE_TXN_FLAG_NORMAL == txn->flag)
        {
            /* 普通事务 */
            ripple_cache_txn_add(parser->parsertrail.parser2txn, txn);
            dlnode->value = NULL;
            dlist_node_free(dlnode, NULL);
            continue;
        }
        else if(RIPPLE_TXN_ISBIGTXN(txn->flag))
        {
            elog(RLOG_WARNING, "in integrate onlinerefresh increment parse bigtxn, logical error");
            return false;
        }
        else if(RIPPLE_TXN_ISONLINEREFRESHTXN(txn->flag))
        {
            /* 增量事务 */
            if( RIPPLE_TXN_TYPE_ONLINEREFRESH_INC_END == txn->type)
            {
                /* 增量处理完成了, 可以退出了 */
                ripple_cache_txn_add(parser->parsertrail.parser2txn, txn);
                dlnode->value = NULL;
                dlist_node_free(dlnode, NULL);
                *bexit = true;
                return true;
            }
            else
            {
                elog(RLOG_WARNING, "in integrate onlinerefresh increment unknown txn flag:%u", txn->flag);
                return false;
            }
        }
        else
        {
            elog(RLOG_WARNING, "in integrate onlinerefresh increment unknown txn flag:%u", txn->flag);
            return false;
        }
    }

    return true;
}

/* 解析 trail 文件主函数 */
void *ripple_onlinerefresh_integrateparsertrail_main(void* args)
{
    bool bexit                                                  = false;
    int timeout                                                 = 0;
    ripple_thrnode* thrnode                                     = NULL;
    ripple_parsertrail* parsertrail                             = NULL;
    ripple_increment_integrateparsertrail* traildecodecxt       = NULL;
    ripple_onlinerefresh_integrateparsertrail* oliparsertrail   = NULL;
 
    thrnode = (ripple_thrnode*)args;

    oliparsertrail = (ripple_onlinerefresh_integrateparsertrail*)thrnode->data;

    traildecodecxt = oliparsertrail->decodingctx;
    parsertrail = (ripple_parsertrail*)traildecodecxt;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "onlinerefresh integrate parser trail stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    /* 进入工作 */
    while(1)
    {
        /* 首先判断是否接收到退出信号 */
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 获取数据, 超时退出 */
        traildecodecxt->parsertrail.records = ripple_queue_get(traildecodecxt->recordscache, &timeout);
        if(true == dlist_isnull(traildecodecxt->parsertrail.records))
        {
            if(RIPPLE_ERROR_TIMEOUT == timeout)
            {
                continue;
            }
            elog(RLOG_WARNING, "onlinerefresh integrate get records error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        if(false == ripple_parsertrail_parser(parsertrail))
        {
            elog(RLOG_WARNING, "onlinerefresh integrate increment parse records error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        /* 事务处理 */
        if(true == dlist_isnull(parsertrail->dtxns))
        {
            continue;
        }

        if(false == ripple_onlinerefresh_integrateparsertrail_txns2queue(traildecodecxt, &bexit))
        {
            elog(RLOG_WARNING, "onlinerefresh integrate increment add txn 2 queue error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        /* 解析到了 onlinerefresh end, 那么线程可以退出了 */
        if(true == bexit)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }
    }

    ripple_pthread_exit(NULL);
    return NULL;
}

void ripple_onlinerefresh_integrateparsertrail_free(void *args)
{
    ripple_onlinerefresh_integrateparsertrail* traildecodecxt = NULL;

    traildecodecxt = (ripple_onlinerefresh_integrateparsertrail*)args;

    if (NULL == traildecodecxt)
    {
        return;
    }

    if (traildecodecxt->decodingctx)
    {
        ripple_increment_integrateparsertrail_free(traildecodecxt->decodingctx);
    }

    rfree(traildecodecxt);
}
