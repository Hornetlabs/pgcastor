#include "ripple_app_incl.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "threads/ripple_threads.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "stmts/ripple_txnstmt.h"
#include "rebuild/ripple_rebuild.h"
#include "refresh/ripple_refresh_tables.h"
#include "onlinerefresh/integrate/dataset/ripple_onlinerefresh_integratedataset.h"
#include "onlinerefresh/integrate/dataset/ripple_onlinerefresh_integratefilterdataset.h"
#include "bigtransaction/integrate/rebuild/ripple_bigtxn_integraterebuild.h"

/* 初始化 */
ripple_bigtxn_integraterebuild* ripple_bigtxn_integraterebuild_init(void)
{
    char* burst                                 = NULL;
    ripple_bigtxn_integraterebuild* rebuild     = NULL;

    rebuild = rmalloc0(sizeof(ripple_bigtxn_integraterebuild));
    if(NULL == rebuild)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(rebuild, 0, '\0', sizeof(ripple_bigtxn_integraterebuild));
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
void *ripple_bigtxn_integraterebuild_main(void* args)
{
    int txbundlesize                                                = 0;
    int timeout                                                     = 0;
    bool find                                                       = false;
    Oid relid                                                       = InvalidOid;
    ripple_txn* txns                                                = NULL;
    ripple_txn* ntxn                                                = NULL;
    ripple_txn* txnnode                                             = NULL;
    ListCell* filterlc                                              = NULL;
    ripple_thrnode* thrnode                                         = NULL;
    ripple_txnstmt* stmtnode                                        = NULL;
    ripple_bigtxn_integraterebuild* rebuild                         = NULL;
    xk_pg_parser_translog_tbcol_values* values                      = NULL;
    xk_pg_parser_translog_tbcolbase* tbcolbase                      = NULL;
    xk_pg_parser_translog_tbcol_nvalues* nvalues                    = NULL;
    ripple_onlinerefresh_integratefilterdataset* filterdatasetentry = NULL;


    thrnode = (ripple_thrnode*)args;

    rebuild = (ripple_bigtxn_integraterebuild*)thrnode->data;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "integrate bigtxn rebuild stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    while(1)
    {
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            /* 序列化/落盘 */
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 在缓存中获取数据 */
        txns = ripple_cache_txn_getbatch(rebuild->parser2rebuild, &timeout);
        if(NULL == txns)
        {
            if(RIPPLE_ERROR_TIMEOUT == timeout)
            {
                /* 睡眠 10 毫秒 */
                usleep(10000);
                continue;
            }

             /* 出错了 */
            elog(RLOG_WARNING, "ripple_cache_txn_getbatch error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        /* 遍历 txns 并重组语句 */
        for(txnnode = txns; NULL != txnnode; txnnode = txns)
        {
            txns = txnnode->cachenext;

            if (NULL != rebuild->onlinerefreshdataset
                && !dlist_isnull(rebuild->onlinerefreshdataset->onlinerefresh))
            {
                /* 过滤onlinerefresh中应用过的数据 */
                List* tmpstmt = NULL;
                stmtnode = NULL;
                find = false;
                foreach(filterlc, txnnode->stmts)
                {
                    stmtnode = (ripple_txnstmt*)lfirst(filterlc);
                    if (stmtnode->type != RIPPLE_TXNSTMT_TYPE_DML)
                    {
                        tmpstmt = lappend(tmpstmt, stmtnode);
                        continue;
                    }

                    tbcolbase = (xk_pg_parser_translog_tbcolbase *)stmtnode->stmt;
            
                    if(XK_PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT == tbcolbase->m_dmltype)
                    {
                        nvalues = (xk_pg_parser_translog_tbcol_nvalues *)stmtnode->stmt;
                        relid = nvalues->m_relid;
                    }
                    else
                    {
                        values = (xk_pg_parser_translog_tbcol_values *)stmtnode->stmt;
                        relid = values->m_relid;
                    }

                    filterdatasetentry = hash_search(rebuild->honlinerefreshfilterdataset, &relid, HASH_FIND, &find);
                    if(false == find)
                    {
                        tmpstmt = lappend(tmpstmt, stmtnode);
                        continue;
                    }

                    if(txnnode->xid < filterdatasetentry->txid)
                    {
                        ripple_txnstmt_free(stmtnode);
                        continue;
                    }

                    tmpstmt = lappend(tmpstmt, stmtnode);
                    continue;
                }
                list_free(txnnode->stmts);
                txnnode->stmts = tmpstmt;
                tmpstmt = NULL;
            }

            if (true == rebuild->burst)
            {
                /* burst重组 */
                if(false == ripple_rebuild_txnburst((ripple_rebuild*)rebuild, txnnode))
                {
                    elog(RLOG_WARNING, "ripple_bigtxn_integraterebuild_txnburst error");
                    thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                    break;
                }
            }
            else
            {
                /* 重组 */
                if(false == ripple_rebuild_prepared((ripple_rebuild*)rebuild, txnnode))
                {
                    elog(RLOG_WARNING, "ripple_bigtxn_integraterebuild_prepared error");
                    thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                    break;
                }
            }

            if (true == txnnode->commit)
            {
                if (ntxn)
                {
                    ripple_cache_txn_add(rebuild->rebuild2sync, ntxn);
                    ntxn = NULL;
                    txbundlesize = 0;
                }
                ripple_cache_txn_add(rebuild->rebuild2sync, txnnode);
                continue;
            }
            

            if(NULL == txnnode->stmts)
            {
                ripple_txn_free(txnnode);
                rfree(txnnode);
                continue;
            }

            if (false == rebuild->mergetxn)
            {
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
                    elog(RLOG_WARNING, "ntxn out of memory, %s", strerror(errno));
                    thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                    goto ripple_bigtxn_integraterebuild_main_error;
                }
                rmemset0(ntxn, 0, '\0', sizeof(ripple_txn));
            }

            /* 复制事务信息 */
            ntxn->flag = txnnode->flag;
            ntxn->xid = txnnode->xid;
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
            if(NULL != txns 
                && ((txbundlesize + txns->stmts->length) < rebuild->txbundlesize))
            {
                continue;
            }
            ripple_cache_txn_add(rebuild->rebuild2sync, ntxn);
            ntxn = NULL;
            txbundlesize = 0;
        }
    }
ripple_bigtxn_integraterebuild_main_error:
    ripple_pthread_exit(NULL);
    return NULL;
}

void ripple_bigtxn_integraterebuild_free(void *args)
{
    ripple_bigtxn_integraterebuild* rebuild = NULL;

    rebuild = (ripple_bigtxn_integraterebuild*)args;

    if (NULL == rebuild)
    {
        return;
    }

    rebuild->parser2rebuild = NULL;
    rebuild->rebuild2sync = NULL;
    
    ripple_rebuild_destroy((ripple_rebuild *)rebuild);

    rfree(rebuild);
}
