#include "app_incl.h"
#include "port/thread/thread.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "queue/queue.h"
#include "threads/threads.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "parser/trail/parsertrail.h"
#include "increment/integrate/parser/increment_integrateparsertrail.h"
#include "onlinerefresh/integrate/parsertrail/onlinerefresh_integrateparsertrail.h"
 

/* 逻辑读取主线程 */
onlinerefresh_integrateparsertrail* onlinerefresh_integrateparsertrail_init(void)
{
    onlinerefresh_integrateparsertrail* traildecodecxt = NULL;

    traildecodecxt = (onlinerefresh_integrateparsertrail*)rmalloc0(sizeof(onlinerefresh_integrateparsertrail));
    if(NULL == traildecodecxt)
    {
        elog(RLOG_WARNING, "onlinerefresh integrateparsertrail malloc out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(traildecodecxt, 0, 0, sizeof(onlinerefresh_integrateparsertrail));

    traildecodecxt->decodingctx = increment_integrateparsertrail_init();
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
static bool onlinerefresh_integrateparsertrail_txns2queue(increment_integrateparsertrail* parser, bool* bexit)
{
    txn* txn_ptr = NULL;
    dlistnode* dlnode = NULL;

    for(dlnode = parser->parsertrail.dtxns->head; NULL != dlnode; dlnode = parser->parsertrail.dtxns->head)
    {
        parser->parsertrail.dtxns->head = dlnode->next;
        txn_ptr = (txn*)dlnode->value;
        dlnode->value = NULL;

        if(TXN_FLAG_NORMAL == txn_ptr->flag)
        {
            /* 普通事务 */
            cache_txn_add(parser->parsertrail.parser2txn, txn_ptr);
            dlnode->value = NULL;
            dlist_node_free(dlnode, NULL);
            continue;
        }
        else if(TXN_ISBIGTXN(txn_ptr->flag))
        {
            elog(RLOG_WARNING, "in integrate onlinerefresh increment parse bigtxn, logical error");
            return false;
        }
        else if(TXN_ISONLINEREFRESHTXN(txn_ptr->flag))
        {
            /* 增量事务 */
            if( TXN_TYPE_ONLINEREFRESH_INC_END == txn_ptr->type)
            {
                /* 增量处理完成了, 可以退出了 */
                cache_txn_add(parser->parsertrail.parser2txn, txn_ptr);
                dlnode->value = NULL;
                dlist_node_free(dlnode, NULL);
                *bexit = true;
                return true;
            }
            else
            {
                elog(RLOG_WARNING, "in integrate onlinerefresh increment unknown txn flag:%u", txn_ptr->flag);
                return false;
            }
        }
        else
        {
            elog(RLOG_WARNING, "in integrate onlinerefresh increment unknown txn flag:%u", txn_ptr->flag);
            return false;
        }
    }

    return true;
}

/* 解析 trail 文件主函数 */
void *onlinerefresh_integrateparsertrail_main(void* args)
{
    bool bexit                                                  = false;
    int timeout                                                 = 0;
    thrnode* thr_node                                     = NULL;
    parsertrail* parser_trail                             = NULL;
    increment_integrateparsertrail* traildecodecxt       = NULL;
    onlinerefresh_integrateparsertrail* oliparsertrail   = NULL;
 
    thr_node = (thrnode*)args;

    oliparsertrail = (onlinerefresh_integrateparsertrail*)thr_node->data;

    traildecodecxt = oliparsertrail->decodingctx;
    parser_trail = (parsertrail*)traildecodecxt;

    /* 查看状态 */
    if(THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING, "onlinerefresh integrate parser trail stat exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
        return NULL;
    }

    /* 设置为工作状态 */
    thr_node->stat = THRNODE_STAT_WORK;

    /* 进入工作 */
    while(1)
    {
        /* 首先判断是否接收到退出信号 */
        if(THRNODE_STAT_TERM == thr_node->stat)
        {
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* 获取数据, 超时退出 */
        traildecodecxt->parsertrail.records = queue_get(traildecodecxt->recordscache, &timeout);
        if(true == dlist_isnull(traildecodecxt->parsertrail.records))
        {
            if(ERROR_TIMEOUT == timeout)
            {
                continue;
            }
            elog(RLOG_WARNING, "onlinerefresh integrate get records error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        if(false == parsertrail_parser(parser_trail))
        {
            elog(RLOG_WARNING, "onlinerefresh integrate increment parse records error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        /* 事务处理 */
        if(true == dlist_isnull(parser_trail->dtxns))
        {
            continue;
        }

        if(false == onlinerefresh_integrateparsertrail_txns2queue(traildecodecxt, &bexit))
        {
            elog(RLOG_WARNING, "onlinerefresh integrate increment add txn 2 queue error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        /* 解析到了 onlinerefresh end, 那么线程可以退出了 */
        if(true == bexit)
        {
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }
    }

    pthread_exit(NULL);
    return NULL;
}

void onlinerefresh_integrateparsertrail_free(void *args)
{
    onlinerefresh_integrateparsertrail* traildecodecxt = NULL;

    traildecodecxt = (onlinerefresh_integrateparsertrail*)args;

    if (NULL == traildecodecxt)
    {
        return;
    }

    if (traildecodecxt->decodingctx)
    {
        increment_integrateparsertrail_free(traildecodecxt->decodingctx);
    }

    rfree(traildecodecxt);
}
