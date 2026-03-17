#include "app_incl.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "threads/threads.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "stmts/txnstmt.h"
#include "stmts/txnstmt_updaterewind.h"
#include "rebuild/rebuild.h"
#include "onlinerefresh/integrate/rebuild/onlinerefresh_integraterebuild.h"

/* 只用于更新解析到的位置 */
static bool onlinerefresh_integraterebuild_updaterewindstmt_set(onlinerefresh_integraterebuild* rebuild_obj, txn* txn)
{
    txnstmt* stmtnode = NULL;
    txnstmt_updaterewind* updaterewind = NULL;

    /* 申请空间 */
    stmtnode = txnstmt_init();
    if(NULL == stmtnode)
    {
        return false;
    }
    rmemset0(stmtnode, 0, '\0', sizeof(txnstmt));

    updaterewind = txnstmt_updaterewind_init();
    if (NULL == updaterewind)
    {
        rfree(stmtnode);
        return false;
    }

    updaterewind->rewind.trail.fileid = txn->segno;
    updaterewind->rewind.trail.offset = txn->end.trail.offset;

    stmtnode->type = TXNSTMT_TYPE_UPDATEREWIND;
    stmtnode->stmt = (void*)updaterewind;

    txn->stmts = lappend(txn->stmts, stmtnode);

    return true;
}

/* 初始化 */
onlinerefresh_integraterebuild* onlinerefresh_integraterebuild_init(void)
{
    char* burst                                     = NULL;
    onlinerefresh_integraterebuild* rebuild_obj  = NULL;

    rebuild_obj = rmalloc0(sizeof(onlinerefresh_integraterebuild));
    if(NULL == rebuild_obj)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(rebuild_obj, 0, '\0', sizeof(onlinerefresh_integraterebuild));
    rebuild_reset(&rebuild_obj->rebuild);
    rebuild_obj->parser2rebuild = NULL;
    rebuild_obj->rebuild2sync = NULL;
    rebuild_obj->mergetxn = (0 == guc_getConfigOptionInt(CFG_KEY_MERGETXN)) ? false : true;
    rebuild_obj->txbundlesize = guc_getConfigOptionInt(CFG_KEY_TXBUNDLESIZE);
    /* 设置integrate_method */
    burst = guc_getConfigOption(CFG_KEY_INTEGRATE_METHOD);
    if (burst != NULL && '\0' != burst[0] && 0 == strcmp(burst, "burst"))
    {
        rebuild_obj->burst = true;
    }
    return rebuild_obj;
}

/* 工作 */
void* onlinerefresh_integraterebuild_main(void *args)
{
    int timeout                                             = 0;
    int txbundlesize                                        = 0;
    txn* txns                                        = NULL;
    txn* ntxn                                        = NULL;
    txn* txnnode                                     = NULL;
    thrnode* thr_node                                 = NULL;
    onlinerefresh_integraterebuild* rebuild_obj          = NULL;

    thr_node = (thrnode*)args;

    rebuild_obj = (onlinerefresh_integraterebuild*)thr_node->data;

    /* 查看状态 */
    if(THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING, "onlinerefresh integrate rebuild stat exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
        return NULL;
    }

    /* 设置为工作状态 */
    thr_node->stat = THRNODE_STAT_WORK;

    while(1)
    {
        /* 首先判断是否接收到退出信号 */
        if(THRNODE_STAT_TERM == thr_node->stat)
        {
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* 在缓存中获取数据 */
        txns = cache_txn_getbatch(rebuild_obj->parser2rebuild, &timeout);
        if(NULL == txns)
        {
            /* 超时继续执行 */
            if(ERROR_TIMEOUT == timeout)
            {
                continue;
            }

            /* 处理失败, 退出 */
            elog(RLOG_WARNING, "onlinerefrese integrate rebuild cache_txn_getbatch error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        /* 遍历 txns 并重组语句 */
        for(txnnode = txns; NULL != txnnode; txnnode = txns)
        {
            /* special 用于标记指定事务: refresh/onlinerefreshbegin/onlinerefreshend */
            txns = txnnode->cachenext;

            /* 事务类型为增量结束，直接加入缓存保证状态表内容正确 */
            if (TXN_TYPE_ONLINEREFRESH_INC_END == txnnode->type)
            {
                if (NULL != ntxn)
                {
                    onlinerefresh_integraterebuild_updaterewindstmt_set(rebuild_obj, ntxn);
                    cache_txn_add(rebuild_obj->rebuild2sync, ntxn);
                    ntxn = NULL;
                    txbundlesize = 0;
                }
                /* 将事务放入到缓存中 */
                onlinerefresh_integraterebuild_updaterewindstmt_set(rebuild_obj, txnnode);
                cache_txn_add(rebuild_obj->rebuild2sync, txnnode);
                break;
            }

            if (true == rebuild_obj->burst)
            {
                /* burst重组 */
                if(false == rebuild_txnburst(&rebuild_obj->rebuild, txnnode))
                {
                    elog(RLOG_WARNING, "onlinerefrese integrate rebuild_txnburst error");
                    thr_node->stat = THRNODE_STAT_ABORT;
                    break;
                }
            }
            else
            {
                /* 重组 */
                if(false == rebuild_prepared(&rebuild_obj->rebuild, txnnode))
                {
                    elog(RLOG_WARNING, "onlinerefrese integrate rebuild_prepared error");
                    thr_node->stat = THRNODE_STAT_ABORT;
                    break;
                }
            }

            if(NULL == txnnode->stmts)
            {
                txn_free(txnnode);
                rfree(txnnode);
                continue;
            }

            if (false == rebuild_obj->mergetxn)
            {
                /* 将事务放入到缓存中 */
                onlinerefresh_integraterebuild_updaterewindstmt_set(rebuild_obj, txnnode);
                cache_txn_add(rebuild_obj->rebuild2sync, txnnode);
                continue;
            }

            if (txnnode->stmts->length > rebuild_obj->txbundlesize)
            {
                if (NULL != ntxn)
                {
                    onlinerefresh_integraterebuild_updaterewindstmt_set(rebuild_obj, ntxn);
                    cache_txn_add(rebuild_obj->rebuild2sync, ntxn);
                    ntxn = NULL;
                    txbundlesize = 0;
                }
                onlinerefresh_integraterebuild_updaterewindstmt_set(rebuild_obj, txnnode);
                cache_txn_add(rebuild_obj->rebuild2sync, txnnode);
                continue;
            }

            /* 合并事务处理 */
            /* 若为空,那么申请空间 */
            if(NULL == ntxn)
            {
                ntxn = (txn*)rmalloc0(sizeof(txn));
                if(NULL == ntxn)
                {
                    elog(RLOG_WARNING, "onlinerefrese integrate rebuild_obj malloc txn out of memory, %s", strerror(errno));
                    thr_node->stat = THRNODE_STAT_ABORT;
                    break;
                }
                rmemset0(ntxn, 0, '\0', sizeof(txn));
            }

            /* 复制事务信息 */
            ntxn->xid = txnnode->xid;
            ntxn->flag = txnnode->flag;
            ntxn->segno = txnnode->segno;
            ntxn->debugno = txnnode->debugno;
            ntxn->start = txnnode->start;
            ntxn->end = txnnode->end;
            ntxn->redo = txnnode->redo;
            ntxn->restart = txnnode->restart;
            ntxn->confirm = txnnode->confirm;
            txbundlesize += txnnode->stmts->length;

            /* 将 stmts 加入到新事务中 */
            ntxn->stmts = list_concat(ntxn->stmts, txnnode->stmts);
            if (ntxn->stmts != txnnode->stmts)
            {
                rfree(txnnode->stmts);
            }
            txnnode->stmts = NULL;

            /* entry 释放 */
            txn_free(txnnode);
            rfree(txnnode);

            /* 最后一个事务或超出合并事务的大小 */
            if(NULL != txns && NULL != txns->stmts
                && ((txbundlesize + txns->stmts->length) < rebuild_obj->txbundlesize))
            {
                continue;
            }
            onlinerefresh_integraterebuild_updaterewindstmt_set(rebuild_obj, ntxn);
            cache_txn_add(rebuild_obj->rebuild2sync, ntxn);
            ntxn = NULL;
            txbundlesize = 0;
        }
    }

    pthread_exit(NULL);
    return NULL;
}

void onlinerefresh_integraterebuild_free(void *args)
{
    onlinerefresh_integraterebuild* rebuild_obj = NULL;

    rebuild_obj = (onlinerefresh_integraterebuild*)args;

    if (NULL == rebuild_obj)
    {
        return;
    }

    rebuild_obj->parser2rebuild = NULL;
    rebuild_obj->rebuild2sync = NULL;
    
    rebuild_destroy(&rebuild_obj->rebuild);

    rfree(rebuild_obj);
}
