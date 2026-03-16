#include "ripple_app_incl.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "threads/ripple_threads.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "stmts/ripple_txnstmt.h"
#include "stmts/ripple_txnstmt_updaterewind.h"
#include "rebuild/ripple_rebuild.h"
#include "onlinerefresh/integrate/rebuild/ripple_onlinerefresh_integraterebuild.h"

/* 只用于更新解析到的位置 */
static bool ripple_onlinerefresh_integraterebuild_updaterewindstmt_set(ripple_onlinerefresh_integraterebuild* rebuild, ripple_txn* txn)
{
    ripple_txnstmt* stmtnode = NULL;
    ripple_txnstmt_updaterewind* updaterewind = NULL;

    /* 申请空间 */
    stmtnode = ripple_txnstmt_init();
    if(NULL == stmtnode)
    {
        return false;
    }
    rmemset0(stmtnode, 0, '\0', sizeof(ripple_txnstmt));

    updaterewind = ripple_txnstmt_updaterewind_init();
    if (NULL == updaterewind)
    {
        rfree(stmtnode);
        return false;
    }

    updaterewind->rewind.trail.fileid = txn->segno;
    updaterewind->rewind.trail.offset = txn->end.trail.offset;

    stmtnode->type = RIPPLE_TXNSTMT_TYPE_UPDATEREWIND;
    stmtnode->stmt = (void*)updaterewind;

    txn->stmts = lappend(txn->stmts, stmtnode);

    return true;
}

/* 初始化 */
ripple_onlinerefresh_integraterebuild* ripple_onlinerefresh_integraterebuild_init(void)
{
    char* burst                                     = NULL;
    ripple_onlinerefresh_integraterebuild* rebuild  = NULL;

    rebuild = rmalloc0(sizeof(ripple_onlinerefresh_integraterebuild));
    if(NULL == rebuild)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(rebuild, 0, '\0', sizeof(ripple_onlinerefresh_integraterebuild));
    ripple_rebuild_reset((ripple_rebuild*)rebuild);
    rebuild->parser2rebuild = NULL;
    rebuild->rebuild2sync = NULL;
    rebuild->mergetxn = (0 == guc_getConfigOptionInt(RIPPLE_CFG_KEY_MERGETXN)) ? false : true;
    rebuild->txbundlesize = guc_getConfigOptionInt(RIPPLE_CFG_KEY_TXBUNDLESIZE);
    /* 设置integrate_method */
    burst = guc_getConfigOption(RIPPLE_CFG_KEY_INTEGRATE_METHOD);
    if (burst != NULL && '\0' != burst[0] && 0 == strcmp(burst, "burst"))
    {
        rebuild->burst = true;
    }
    return rebuild;
}

/* 工作 */
void* ripple_onlinerefresh_integraterebuild_main(void *args)
{
    int timeout                                             = 0;
    int txbundlesize                                        = 0;
    ripple_txn* txns                                        = NULL;
    ripple_txn* ntxn                                        = NULL;
    ripple_txn* txnnode                                     = NULL;
    ripple_thrnode* thrnode                                 = NULL;
    ripple_onlinerefresh_integraterebuild* rebuild          = NULL;

    thrnode = (ripple_thrnode*)args;

    rebuild = (ripple_onlinerefresh_integraterebuild*)thrnode->data;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "onlinerefresh integrate rebuild stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    while(1)
    {
        /* 首先判断是否接收到退出信号 */
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 在缓存中获取数据 */
        txns = ripple_cache_txn_getbatch(rebuild->parser2rebuild, &timeout);
        if(NULL == txns)
        {
            /* 超时继续执行 */
            if(RIPPLE_ERROR_TIMEOUT == timeout)
            {
                continue;
            }

            /* 处理失败, 退出 */
            elog(RLOG_WARNING, "onlinerefrese integrate rebuild cache_txn_getbatch error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        /* 遍历 txns 并重组语句 */
        for(txnnode = txns; NULL != txnnode; txnnode = txns)
        {
            /* special 用于标记指定事务: refresh/onlinerefreshbegin/onlinerefreshend */
            txns = txnnode->cachenext;

            /* 事务类型为增量结束，直接加入缓存保证状态表内容正确 */
            if (RIPPLE_TXN_TYPE_ONLINEREFRESH_INC_END == txnnode->type)
            {
                if (NULL != ntxn)
                {
                    ripple_onlinerefresh_integraterebuild_updaterewindstmt_set(rebuild, ntxn);
                    ripple_cache_txn_add(rebuild->rebuild2sync, ntxn);
                    ntxn = NULL;
                    txbundlesize = 0;
                }
                /* 将事务放入到缓存中 */
                ripple_onlinerefresh_integraterebuild_updaterewindstmt_set(rebuild, txnnode);
                ripple_cache_txn_add(rebuild->rebuild2sync, txnnode);
                break;
            }

            if (true == rebuild->burst)
            {
                /* burst重组 */
                if(false == ripple_rebuild_txnburst((ripple_rebuild*)rebuild, txnnode))
                {
                    elog(RLOG_WARNING, "onlinerefrese integrate rebuild_txnburst error");
                    thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                    break;
                }
            }
            else
            {
                /* 重组 */
                if(false == ripple_rebuild_prepared((ripple_rebuild*)rebuild, txnnode))
                {
                    elog(RLOG_WARNING, "onlinerefrese integrate rebuild_prepared error");
                    thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                    break;
                }
            }

            if(NULL == txnnode->stmts)
            {
                ripple_txn_free(txnnode);
                rfree(txnnode);
                continue;
            }

            if (false == rebuild->mergetxn)
            {
                /* 将事务放入到缓存中 */
                ripple_onlinerefresh_integraterebuild_updaterewindstmt_set(rebuild, txnnode);
                ripple_cache_txn_add(rebuild->rebuild2sync, txnnode);
                continue;
            }

            if (txnnode->stmts->length > rebuild->txbundlesize)
            {
                if (NULL != ntxn)
                {
                    ripple_onlinerefresh_integraterebuild_updaterewindstmt_set(rebuild, ntxn);
                    ripple_cache_txn_add(rebuild->rebuild2sync, ntxn);
                    ntxn = NULL;
                    txbundlesize = 0;
                }
                ripple_onlinerefresh_integraterebuild_updaterewindstmt_set(rebuild, txnnode);
                ripple_cache_txn_add(rebuild->rebuild2sync, txnnode);
                continue;
            }

            /* 合并事务处理 */
            /* 若为空,那么申请空间 */
            if(NULL == ntxn)
            {
                ntxn = (ripple_txn*)rmalloc0(sizeof(ripple_txn));
                if(NULL == ntxn)
                {
                    elog(RLOG_WARNING, "onlinerefrese integrate rebuild malloc txn out of memory, %s", strerror(errno));
                    thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                    break;
                }
                rmemset0(ntxn, 0, '\0', sizeof(ripple_txn));
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
            ripple_txn_free(txnnode);
            rfree(txnnode);

            /* 最后一个事务或超出合并事务的大小 */
            if(NULL != txns && NULL != txns->stmts
                && ((txbundlesize + txns->stmts->length) < rebuild->txbundlesize))
            {
                continue;
            }
            ripple_onlinerefresh_integraterebuild_updaterewindstmt_set(rebuild, ntxn);
            ripple_cache_txn_add(rebuild->rebuild2sync, ntxn);
            ntxn = NULL;
            txbundlesize = 0;
        }
    }

    ripple_pthread_exit(NULL);
    return NULL;
}

void ripple_onlinerefresh_integraterebuild_free(void *args)
{
    ripple_onlinerefresh_integraterebuild* rebuild = NULL;

    rebuild = (ripple_onlinerefresh_integraterebuild*)args;

    if (NULL == rebuild)
    {
        return;
    }

    rebuild->parser2rebuild = NULL;
    rebuild->rebuild2sync = NULL;
    
    ripple_rebuild_destroy((ripple_rebuild *)rebuild);

    rfree(rebuild);
}
