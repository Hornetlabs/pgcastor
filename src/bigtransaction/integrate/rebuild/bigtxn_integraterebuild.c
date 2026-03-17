#include "app_incl.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/uuid/uuid.h"
#include "utils/hash/hash_search.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "threads/threads.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "stmts/txnstmt.h"
#include "rebuild/rebuild.h"
#include "refresh/refresh_tables.h"
#include "onlinerefresh/integrate/dataset/onlinerefresh_integratedataset.h"
#include "onlinerefresh/integrate/dataset/onlinerefresh_integratefilterdataset.h"
#include "bigtransaction/integrate/rebuild/bigtxn_integraterebuild.h"

/* 初始化 */
bigtxn_integraterebuild* bigtxn_integraterebuild_init(void)
{
    char* burst                                 = NULL;
    bigtxn_integraterebuild* rebuild_obj     = NULL;

    rebuild_obj = rmalloc0(sizeof(bigtxn_integraterebuild));
    if(NULL == rebuild_obj)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(rebuild_obj, 0, '\0', sizeof(bigtxn_integraterebuild));
    rebuild_reset((rebuild*)rebuild_obj);
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
void *bigtxn_integraterebuild_main(void* args)
{
    int txbundlesize                                                = 0;
    int timeout                                                     = 0;
    bool find                                                       = false;
    Oid relid                                                       = InvalidOid;
    txn* txns                                                = NULL;
    txn* ntxn                                                = NULL;
    txn* txnnode                                             = NULL;
    ListCell* filterlc                                              = NULL;
    thrnode* thr_node                                         = NULL;
    txnstmt* stmtnode                                        = NULL;
    bigtxn_integraterebuild* rebuild_obj                         = NULL;
    pg_parser_translog_tbcol_values* values                      = NULL;
    pg_parser_translog_tbcolbase* tbcolbase                      = NULL;
    pg_parser_translog_tbcol_nvalues* nvalues                    = NULL;
    onlinerefresh_integratefilterdataset* filterdatasetentry = NULL;


    thr_node = (thrnode*)args;
    rebuild_obj = (bigtxn_integraterebuild*)thr_node->data;

    /* 查看状态 */
    if(THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING, "integrate bigtxn rebuild stat exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thr_node->stat = THRNODE_STAT_WORK;

    while(1)
    {
        if(THRNODE_STAT_TERM == thr_node->stat)
        {
            /* 序列化/落盘 */
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* 在缓存中获取数据 */
        txns = cache_txn_getbatch(rebuild_obj->parser2rebuild, &timeout);
        if(NULL == txns)
        {
            if(ERROR_TIMEOUT == timeout)
            {
                /* 睡眠 10 毫秒 */
                usleep(10000);
                continue;
            }

             /* 出错了 */
            elog(RLOG_WARNING, "cache_txn_getbatch error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        /* 遍历 txns 并重组语句 */
        for(txnnode = txns; NULL != txnnode; txnnode = txns)
        {
            txns = txnnode->cachenext;

            if (NULL != rebuild_obj->onlinerefreshdataset
                && !dlist_isnull(rebuild_obj->onlinerefreshdataset->onlinerefresh))
            {
                /* 过滤onlinerefresh中应用过的数据 */
                List* tmpstmt = NULL;
                stmtnode = NULL;
                find = false;
                foreach(filterlc, txnnode->stmts)
                {
                    stmtnode = (txnstmt*)lfirst(filterlc);
                    if (stmtnode->type != TXNSTMT_TYPE_DML)
                    {
                        tmpstmt = lappend(tmpstmt, stmtnode);
                        continue;
                    }

                    tbcolbase = (pg_parser_translog_tbcolbase *)stmtnode->stmt;
            
                    if(PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT == tbcolbase->m_dmltype)
                    {
                        nvalues = (pg_parser_translog_tbcol_nvalues *)stmtnode->stmt;
                        relid = nvalues->m_relid;
                    }
                    else
                    {
                        values = (pg_parser_translog_tbcol_values *)stmtnode->stmt;
                        relid = values->m_relid;
                    }

                    filterdatasetentry = hash_search(rebuild_obj->honlinerefreshfilterdataset, &relid, HASH_FIND, &find);
                    if(false == find)
                    {
                        tmpstmt = lappend(tmpstmt, stmtnode);
                        continue;
                    }

                    if(txnnode->xid < filterdatasetentry->txid)
                    {
                        txnstmt_free(stmtnode);
                        continue;
                    }

                    tmpstmt = lappend(tmpstmt, stmtnode);
                    continue;
                }
                list_free(txnnode->stmts);
                txnnode->stmts = tmpstmt;
                tmpstmt = NULL;
            }

            if (true == rebuild_obj->burst)
            {
                /* burst重组 */
                if(false == rebuild_txnburst((rebuild*)rebuild_obj, txnnode))
                {
                    elog(RLOG_WARNING, "bigtxn_integraterebuild_txnburst error");
                    thr_node->stat = THRNODE_STAT_ABORT;
                    break;
                }
            }
            else
            {
                /* 重组 */
                if(false == rebuild_prepared((rebuild*)rebuild_obj, txnnode))
                {
                    elog(RLOG_WARNING, "bigtxn_integraterebuild_prepared error");
                    thr_node->stat = THRNODE_STAT_ABORT;
                    break;
                }
            }

            if (true == txnnode->commit)
            {
                if (ntxn)
                {
                    cache_txn_add(rebuild_obj->rebuild2sync, ntxn);
                    ntxn = NULL;
                    txbundlesize = 0;
                }
                cache_txn_add(rebuild_obj->rebuild2sync, txnnode);
                continue;
            }
            

            if(NULL == txnnode->stmts)
            {
                txn_free(txnnode);
                rfree(txnnode);
                continue;
            }

            if (false == rebuild_obj->mergetxn)
            {
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
                    elog(RLOG_WARNING, "ntxn out of memory, %s", strerror(errno));
                    thr_node->stat = THRNODE_STAT_ABORT;
                    goto bigtxn_integraterebuild_main_error;
                }
                rmemset0(ntxn, 0, '\0', sizeof(txn));
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
            txn_free(txnnode);
            rfree(txnnode);

            /* 最后一个事务或超出合并事务的大小 */
            if(NULL != txns 
                && ((txbundlesize + txns->stmts->length) < rebuild_obj->txbundlesize))
            {
                continue;
            }
            cache_txn_add(rebuild_obj->rebuild2sync, ntxn);
            ntxn = NULL;
            txbundlesize = 0;
        }
    }
bigtxn_integraterebuild_main_error:
    pthread_exit(NULL);
    return NULL;
}

void bigtxn_integraterebuild_free(void *args)
{
    bigtxn_integraterebuild* rebuild_obj = NULL;

    rebuild_obj = (bigtxn_integraterebuild*)args;

    if (NULL == rebuild_obj)
    {
        return;
    }

    rebuild_obj->parser2rebuild = NULL;
    rebuild_obj->rebuild2sync = NULL;
    
    rebuild_destroy((rebuild *)rebuild_obj);

    rfree(rebuild_obj);
}
