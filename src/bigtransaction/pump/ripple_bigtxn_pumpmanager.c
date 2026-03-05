#include "ripple_app_incl.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/mpage/mpage.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "threads/ripple_threads.h"
#include "queue/ripple_queue.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ffsmgr.h"
#include "cache/ripple_transcache.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadtrailrecords.h"
#include "parser/trail/ripple_parsertrail.h"
#include "serial/ripple_serial.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netiomp/ripple_netiomp_poll.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netclient.h"
#include "filetransfer/ripple_filetransfer.h"
#include "increment/pump/split/ripple_increment_pumpsplittrail.h"
#include "increment/pump/parser/ripple_increment_pumpparsertrail.h"
#include "increment/pump/serial/ripple_increment_pumpserial.h"
#include "increment/pump/net/ripple_increment_pumpnet.h"
#include "bigtransaction/persist/ripple_bigtxn_persist.h"
#include "bigtransaction/pump/parser/ripple_bigtxn_pumpparsertrail.h"
#include "bigtransaction/pump/split/ripple_bigtxn_pumpsplittrail.h"
#include "bigtransaction/pump/serial/ripple_bigtxn_pumpserial.h"
#include "bigtransaction/pump/net/ripple_bigtxn_pumpnet.h"
#include "bigtransaction/pump/ripple_bigtxn_pumpmanager.h"

/* 大事务设置管理线程状态为 INPROCESS */
static void ripple_bigtxn_pumpmanager_setinprocess(void* privdata)
{
    ripple_bigtxn_pumpmanager* bigtxnmgr = NULL;

    bigtxnmgr = (ripple_bigtxn_pumpmanager*)privdata;

    if (NULL == bigtxnmgr)
    {
        elog(RLOG_ERROR, "bigtxn pump stat set inprocess exception, privdata point is NULL");
    }

    elog(RLOG_WARNING, "bigtxn pump stat set inprocess :%lu ", bigtxnmgr->xid);

    bigtxnmgr->stat = RIPPLE_BIGTXN_PUMPMANAGER_STAT_INPROCESS;

    return;
}

/* 大事务设置管理线程状态为 RESET */
static void ripple_bigtxn_pumpmanager_setreset(void* privdata)
{
    ripple_bigtxn_pumpmanager* bigtxnmgr = NULL;

    bigtxnmgr = (ripple_bigtxn_pumpmanager*)privdata;

    if (NULL == bigtxnmgr)
    {
        elog(RLOG_ERROR, "bigtxn pump stat set reset exception, privdata point is NULL");
    }

    if (RIPPLE_BIGTXN_PUMPMANAGER_STAT_DONE > bigtxnmgr->stat)
    {
        bigtxnmgr->stat = RIPPLE_BIGTXN_PUMPMANAGER_STAT_RESET;
    }

    return;
}

/* 大事务判断管理线程状态是否为 RESET */
static bool ripple_bigtxn_pumpmanager_isreset(void* privdata)
{
    bool result                             = false;
    ripple_bigtxn_pumpmanager* bigtxnmgr    = NULL;

    bigtxnmgr = (ripple_bigtxn_pumpmanager*)privdata;

    if (NULL == bigtxnmgr)
    {
        elog(RLOG_ERROR, "bigtxn pump stat set reset exception, privdata point is NULL");
    }

    if (RIPPLE_BIGTXN_PUMPMANAGER_STAT_RESET == bigtxnmgr->stat)
    {
        result = true;
    }

    return result;
}

/* 清理 txn2filebuffers/parser2txns/recordscache缓存 */
static void ripple_bigtxn_pumpmanager_cacheclean(ripple_bigtxn_pumpmanager* bigtxnmgr)
{
    riple_file_buffer_clean_waitflush(bigtxnmgr->txn2filebuffers);
    ripple_cache_txn_clean(bigtxnmgr->parser2txns);
    ripple_queue_clear(bigtxnmgr->recordscache, dlist_freevoid);
}

/* 启动网络发送工作线程 */
static bool ripple_bigtxn_pumpmanager_startincnetjob(ripple_bigtxn_pumpmanager* bigtxnmgr)
{
    ripple_bigtxn_pumpnet* bigtxnnet                   = NULL;

    /*---------------------发送线程 begin------------------*/
    bigtxnnet = ripple_bigtxn_pumpnet_init();
    if(NULL == bigtxnnet)
    {
        elog(RLOG_WARNING, "bigtxn pump net init error");
        return false;
    }
    bigtxnnet->xid = bigtxnmgr->xid;
    bigtxnnet->clientstate->txn2filebuffer = bigtxnmgr->txn2filebuffers;
    bigtxnnet->clientstate->privdata = (void*)bigtxnmgr;
    bigtxnnet->clientstate->callback.bigtxn_mgrstat_setinprocess = ripple_bigtxn_pumpmanager_setinprocess;
    bigtxnnet->clientstate->callback.bigtxn_mgrstat_setreset = ripple_bigtxn_pumpmanager_setreset;
    bigtxnnet->clientstate->callback.bigtxn_mgrstat_isreset = ripple_bigtxn_pumpmanager_isreset;
    bigtxnnet->clientstate->callback.splittrail_statefileid_set = NULL;
    bigtxnnet->clientstate->callback.serialstate_state_set = NULL;
    bigtxnnet->clientstate->callback.setmetricsendlsn = NULL;
    bigtxnnet->clientstate->callback.setmetricsendtimestamp = NULL;
    bigtxnnet->clientstate->callback.setmetricsendtrailno = NULL;
    bigtxnnet->clientstate->callback.setmetricsendtrailstart = NULL;
    bigtxnnet->clientstate->callback.pumpstate_addrefresh = NULL;
    bigtxnnet->clientstate->callback.pumpstate_isrefreshdown = NULL;
    bigtxnnet->clientstate->callback.pumpnet_filetransfernode_add = NULL;

    /*---------------------发送线程   end------------------*/

    /* 启动发送线程 */
    if(false == ripple_threads_addjobthread(bigtxnmgr->thrsmgr->parents,
                                            RIPPLE_THRNODE_IDENTITY_PUMP_BIGTXN_NETCLIENT,
                                            bigtxnmgr->thrsmgr->submgrref.no,
                                            (void*)bigtxnnet,
                                            ripple_bigtxn_pumpnet_free,
                                            NULL,
                                            ripple_bigtxn_pumpnet_main))
    {
        elog(RLOG_WARNING, "bigtxn pump start net job error");
        return false;
    }

    return true;
}

/* pump端 添加filetransfernode */
static void ripple_bigtxn_pumpmanager_filegap_add(void* privdata, void* filegap)
{
    ripple_bigtxn_pumpmanager* bigtxnmgr = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "bigtxn pump filegap add exception, privdata point is NULL");
    }

    bigtxnmgr = (ripple_bigtxn_pumpmanager*)privdata;

    if (NULL == bigtxnmgr->filegap)
    {
        elog(RLOG_ERROR, "bigtxn pump filegap add exception, filegap point is NULL");
    }
    ripple_filetransfer_node_add(bigtxnmgr->filegap, filegap);
    return;
}

/* 启动除net外增量工作线程 回调函数待重启功能实现后设置*/
static bool ripple_bigtxn_pumpmanager_startincrementjob(ripple_bigtxn_pumpmanager* bigtxnmgr,
                                                        ripple_thrnode** parserthr,
                                                        ripple_thrnode** serialthr,
                                                        ripple_thrnode** loadrecordthr)
{
    ListCell* lc                                        = NULL;
    ripple_thrref* thrref                               = NULL;
    ripple_bigtxn_pumpparsertrail* bigtxnparser         = NULL;
    ripple_bigtxn_pumpserial* bigtxnserial              = NULL;
    ripple_bigtxn_pumpsplittrail* bigtxnloadrecord      = NULL;

    /*---------------------序列化线程 begin----------------*/
    bigtxnserial = ripple_bigtxn_pumpserial_init();
    if(NULL == bigtxnserial)
    {
        elog(RLOG_WARNING, "bigtxn pump serial init error");
        return false;
    }
    bigtxnserial->serialstate->privdata = (void*)bigtxnmgr;
    bigtxnserial->serialstate->parser2serialtxns = bigtxnmgr->parser2txns;
    bigtxnserial->serialstate->base.txn2filebuffer = bigtxnmgr->txn2filebuffers;

    /* 回调函数置空 */
    bigtxnserial->serialstate->callback.clientstat_state_set = NULL;
    bigtxnserial->serialstate->callback.parserstat_state_set = NULL;
    bigtxnserial->serialstate->callback.networkclientstate_cfileid_get = NULL;
    /*---------------------序列化线程   end----------------*/

    /*---------------------解析器线程 begin----------------*/
    /* 
     * 解析器初始化
     * parserwal回调设置
    */
    bigtxnparser = ripple_bigtxn_pumpparsertrail_init();
    if(NULL == bigtxnparser)
    {
        elog(RLOG_WARNING, "bigtxn pump parsertrail init error");
        return false;
    }
    bigtxnparser->pumpparsertrail->privdata = (void*)bigtxnmgr;
    bigtxnparser->pumpparsertrail->recordscache = bigtxnmgr->recordscache;
    bigtxnparser->pumpparsertrail->parsertrail.parser2txn = bigtxnmgr->parser2txns;
    /*---------------------解析器线程 end----------------*/

    /*---------------------trail拆分线程 begin--------------*/
    bigtxnloadrecord = ripple_bigtxn_pumpsplittrail_init(bigtxnmgr->xid);
    if(NULL == bigtxnloadrecord)
    {
        elog(RLOG_WARNING, "bigtxn pump init splittrail error");
        return false;
    }
    bigtxnloadrecord->splittrailctx->privdata = (void*)bigtxnmgr;
    bigtxnloadrecord->splittrailctx->recordscache = bigtxnmgr->recordscache;
    bigtxnloadrecord->splittrailctx->callback.pumpstate_filetransfernode_add = ripple_bigtxn_pumpmanager_filegap_add;
    /*---------------------trail拆分线程   end--------------*/

    /*
     * 启动各线程
     */
    /* 序列化线程 */
    if(false == ripple_threads_addjobthread(bigtxnmgr->thrsmgr->parents,
                                            RIPPLE_THRNODE_IDENTITY_PUMP_BIGTXN_SERIAL,
                                            bigtxnmgr->thrsmgr->submgrref.no,
                                            (void*)bigtxnserial,
                                            ripple_bigtxn_pumpserial_free,
                                            NULL,
                                            ripple_bigtxn_pumpserial_main))
    {
        elog(RLOG_WARNING, "bigtxn pump start serial job error");
        return false;
    }

    /* 注册解析器线程 */
    if(false == ripple_threads_addjobthread(bigtxnmgr->thrsmgr->parents,
                                            RIPPLE_THRNODE_IDENTITY_PUMP_BIGTXN_PARSER,
                                            bigtxnmgr->thrsmgr->submgrref.no,
                                            (void*)bigtxnparser,
                                            ripple_bigtxn_pumpparsertrail_free,
                                            NULL,
                                            ripple_bigtxn_pumpparsertrail_main))
    {
        elog(RLOG_WARNING, "bigtxn pump start parsertrail job error");
        return false;
    }

    /* 注册 loadrecords线程 */
    if(false == ripple_threads_addjobthread(bigtxnmgr->thrsmgr->parents,
                                            RIPPLE_THRNODE_IDENTITY_PUMP_BIGTXN_LOADRECORD,
                                            bigtxnmgr->thrsmgr->submgrref.no,
                                            (void*)bigtxnloadrecord,
                                            ripple_bigtxn_pumpsplittrail_free,
                                            NULL,
                                            ripple_bigtxn_pumpsplittrail_main))
    {
        elog(RLOG_WARNING, "bigtxn pump start splittrail job error");
        return false;
    }

    /* 预先获取到各线程, 方面后续的逻辑判断 */
    /* 获取 loadrecord 线程 */
    lc = bigtxnmgr->thrsmgr->childthrrefs->head;
    thrref = (ripple_thrref*)lfirst(lc);
    *loadrecordthr = ripple_threads_getthrnodebyno(bigtxnmgr->thrsmgr->parents, thrref->no);
    if(NULL == *loadrecordthr)
    {
        elog(RLOG_WARNING, "bigtxn pumpmanager can not get load record thread by no:%lu", thrref->no);
        return false;
    }

    /* 获取 parser 线程 */
    lc = lc->next;
    thrref = (ripple_thrref*)lfirst(lc);
    *parserthr = ripple_threads_getthrnodebyno(bigtxnmgr->thrsmgr->parents, thrref->no);
    if(NULL == *parserthr)
    {
        elog(RLOG_WARNING, "bigtxn pumpmanager can not get parser thread by no:%lu", thrref->no);
        return false;
    }

    /* 获取 serail 线程 */
    lc = lc->next;
    thrref = (ripple_thrref*)lfirst(lc);
    *serialthr = ripple_threads_getthrnodebyno(bigtxnmgr->thrsmgr->parents, thrref->no);
    if(NULL == *serialthr)
    {
        elog(RLOG_WARNING, "bigtxn pumpmanager can not get serail thread by no:%lu", thrref->no);
        return false;
    }
    return true;
}

/* 
 * 对比 recpos
 * dst > src    : 1
 * dst == src   : 0
 * dst < src    : -1
 */
int ripple_bigtxn_pumpmanager_compare_recpos(ripple_recpos* src, ripple_recpos* dst)
{
    /* 对比 fileid */
    if (dst->trail.fileid > src->trail.fileid)
    {
        return 1;
    }
    else if (dst->trail.fileid == src->trail.fileid)
    {
        /* fileid 相同 对比offset */
        if (dst->trail.offset > src->trail.offset)
        {
            return 1;
        }
        else if (dst->trail.offset == src->trail.offset)
        {
            return 0;
        }
    }

    /* dst 的 fileid < src fileid 或相同时 dst offset < src offset */
    return -1;
}

void ripple_bigtxn_pumpmanager_set_begin(ripple_bigtxn_pumpmanager *bigtxnmgr, ripple_recpos *pos)
{
    bigtxnmgr->begin.trail.fileid = pos->trail.fileid;
    bigtxnmgr->begin.trail.offset = pos->trail.offset;
}

void ripple_bigtxn_pumpmanager_set_end(ripple_bigtxn_pumpmanager *bigtxnmgr, ripple_recpos *pos)
{
    bigtxnmgr->end.trail.fileid = pos->trail.fileid;
    bigtxnmgr->end.trail.offset = pos->trail.offset;
}

void ripple_bigtxn_pumpmanager_set_stat(ripple_bigtxn_pumpmanager *bigtxnmgr, int stat)
{
    bigtxnmgr->stat = stat;
}

dlist *ripple_bigtxn_pumpmanager_persist2pumpmanager(ripple_bigtxn_persist *persist, ripple_queue* gap)
{
    dlist *result = NULL;
    dlistnode *dnode = NULL;

    if (!persist || true == dlist_isnull(persist->dpersistnodes))
    {
        return NULL;
    }

    /* 遍历persist */
    for (dnode = persist->dpersistnodes->head; NULL != dnode; dnode = dnode->next)
    {
        ripple_bigtxn_persistnode *persistnode = (ripple_bigtxn_persistnode *)dnode->value;
        ripple_bigtxn_pumpmanager *bigtxnmgr = NULL;

        /* 已完成的和放弃掉的不再处理 */
        if (RIPPLE_BIGTXN_PERSISTNODE_STAT_DONE == persistnode->stat
            || RIPPLE_BIGTXN_PERSISTNODE_STAT_ABANDON == persistnode->stat)
        {
            continue;
        }

        /* 包含了对txn的创建 */
        /* 构建pumpmanager并初始化 设置xid 和 begin*/
        bigtxnmgr = ripple_bigtxn_pumpmanager_init();
        bigtxnmgr->filegap = gap;
        ripple_bigtxn_pumpmanager_set_begin(bigtxnmgr, &persistnode->begin);
        bigtxnmgr->xid = persistnode->xid;
        bigtxnmgr->stat = RIPPLE_BIGTXN_PUMPMANAGER_STAT_INIT;
        result = dlist_put(result, bigtxnmgr);
    }
    return result;
}

/* 初始化 */
ripple_bigtxn_pumpmanager *ripple_bigtxn_pumpmanager_init(void)
{
    ripple_bigtxn_pumpmanager *bigtxnmgr = NULL;

    bigtxnmgr = rmalloc0(sizeof(ripple_bigtxn_pumpmanager));
    if (!bigtxnmgr)
    {
        elog(RLOG_WARNING, "pump bigtxn pumpmanager oom %s", strerror(errno));
        return NULL;
    }
    rmemset0(bigtxnmgr, 0, 0, sizeof(ripple_bigtxn_pumpmanager));

    bigtxnmgr->stat = RIPPLE_BIGTXN_PUMPMANAGER_STAT_NOP;
    bigtxnmgr->thrsmgr = NULL;
    bigtxnmgr->recordscache = ripple_queue_init();
    bigtxnmgr->parser2txns = ripple_cache_txn_init();
    bigtxnmgr->txn2filebuffers = ripple_file_buffer_init();

    return bigtxnmgr;
}

/* 大事务管理线程主函数 */
/* 管理线程状态转换
 * 1.申请ripple_bigtxn_pumpmanager空间加入队列中设置为 INIT
 * 2.遍历队列启动管理线程设置为 STARTING , 此状态下管理线程等待启动net线程
 * 3.当net线程接收到身份验证后设置管理线程为 INPROCESS ，该状态下启动loadrecords/parser/serial线程
 *   并设置管理状态为 WAITDONE 
 * 4.WAITDONE状态下等待解析线程退出 --> 设置loadrecords 线程TERM-->等待net线程退出 --> 设置 serial 线程TERM
 *  --> 等待loadrecords、serial线程退出
 * 5.所有job线程退出设置状态为 DONE ，退出管理线程
 * 
 * 特殊处理：解析到reset
 * 1.解析线程接收到reset事务后回调设置管理线程状态为 ABANDON ，该状态下设置loadrecords/parser/serial/net线程状态为TERM
 *  --> 等待loadrecords/parser/serial/net退出 --> 设置管理线程状态为 ABANDONED
 * 
 * 网络故障：
 * 1.网络出现故障，net线程会设置大事务管理线程状态为 RESET，NET线程会等待管理线程状态不为 RESET
 * 2.大事务管理线程在 RESET 状态下设置 loadrecords/parser/serial 为 TERM --> 清理 txn2filebuffers/parser2txns/recordscache缓存
 *   -->等待loadrecords/parser/serial退出 --> 清理 txn2filebuffers/parser2txns/recordscache缓存 --> 设置管理线程状态为IDENTITY
 * 3.管理线程状态为IDENTITY下，等待网络连接成功接收到身份验证后由net线程设置管理线程状态为 INPROCESS 
 *   --> 管理线程启动loadrecords/parser/serial线程 --> 等待退出
*/

void* ripple_bigtxn_pumpmanager_main(void* args)
{
    int jobcnt                                  = 0;
    ListCell* lc                                = NULL;
    ripple_thrref* thrref                       = NULL;
    ripple_thrnode* thrnode                     = NULL;
    ripple_thrnode* bigtxnnetthrnode            = NULL;
    ripple_thrnode* bigtxnserialthrnode         = NULL;
    ripple_thrnode* bigtxnparsertrailthrnode    = NULL;
    ripple_thrnode* bigtxnloadrecthrnode        = NULL;
    ripple_bigtxn_pumpmanager *bigtxnmgr        = NULL;

    thrnode = (ripple_thrnode*)args;

    bigtxnmgr = (ripple_bigtxn_pumpmanager*)thrnode->data;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "pump bigtxn manager stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        bigtxnmgr->stat = RIPPLE_BIGTXN_PUMPMANAGER_STAT_DONE;
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    if(false == ripple_bigtxn_pumpmanager_startincnetjob(bigtxnmgr))
    {
        elog(RLOG_WARNING, "bigtxn pump start net job thread error");
        bigtxnmgr->stat = RIPPLE_BIGTXN_PUMPMANAGER_STAT_DONE;
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

    /* 获取 loadrecord 线程 */
    lc = bigtxnmgr->thrsmgr->childthrrefs->head;
    thrref = (ripple_thrref*)lfirst(lc);
    bigtxnnetthrnode = ripple_threads_getthrnodebyno(bigtxnmgr->thrsmgr->parents, thrref->no);
    if(NULL == bigtxnnetthrnode)
    {
        elog(RLOG_WARNING, "bigtxn pumpmanager can not get load net thread by no:%lu", thrref->no);
        bigtxnmgr->stat = RIPPLE_BIGTXN_PUMPMANAGER_STAT_DONE;
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

    /* 主循环 */
    while (1)
    {
        usleep(50000);
        /* 打开文件 */
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            /* 序列化/落盘 */
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 将放弃的事务的子线程退出 */
        if (true == bigtxnmgr->abandon)
        {
            /* 还未启动loadrecords/parser/serial就遇到reset */
            if(NULL == bigtxnparsertrailthrnode
                && NULL == bigtxnloadrecthrnode
                && NULL == bigtxnserialthrnode)
            {
                if (RIPPLE_THRNODE_STAT_WORK > bigtxnnetthrnode->stat)
                {
                    continue;
                }

                /* 设置 net 线程退出 */
                if(RIPPLE_THRNODE_STAT_TERM > bigtxnnetthrnode->stat)
                {
                    bigtxnnetthrnode->stat = RIPPLE_THRNODE_STAT_TERM;
                    continue;
                }
                if(RIPPLE_THRNODE_STAT_EXITED != bigtxnnetthrnode->stat)
                {
                    /* net 线程未退出, 等待 */
                    continue;
                }
                bigtxnnetthrnode->stat = RIPPLE_THRNODE_STAT_FREE;

                /* 设置本线程退出 */
                thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
                bigtxnmgr->stat = RIPPLE_BIGTXN_PUMPMANAGER_STAT_ABANDONED;
                break;
            }
            /* 设置 loadrecords 线程退出 */
            if(RIPPLE_THRNODE_STAT_TERM > bigtxnloadrecthrnode->stat)
            {
                bigtxnloadrecthrnode->stat = RIPPLE_THRNODE_STAT_TERM;
                continue;
            }
            if(RIPPLE_THRNODE_STAT_EXITED != bigtxnloadrecthrnode->stat)
            {
                /* loadrecords线程未退出, 等待 */
                continue;
            }

            /* 设置 parsertrail 线程退出 */
            if(RIPPLE_THRNODE_STAT_TERM > bigtxnparsertrailthrnode->stat)
            {
                bigtxnparsertrailthrnode->stat = RIPPLE_THRNODE_STAT_TERM;
                continue;
            }
            if(RIPPLE_THRNODE_STAT_EXITED != bigtxnparsertrailthrnode->stat)
            {
                /* parsertrail线程未退出, 等待 */
                continue;
            }

             /* 设置 序列化 线程退出 */
            if(RIPPLE_THRNODE_STAT_TERM > bigtxnserialthrnode->stat)
            {
                bigtxnserialthrnode->stat = RIPPLE_THRNODE_STAT_TERM;
                continue;
            }
            if(RIPPLE_THRNODE_STAT_EXITED != bigtxnserialthrnode->stat)
            {
                /* 序列化 线程未退出, 等待 */
                continue;
            }

            /* 设置 net 线程退出 */
            if(RIPPLE_THRNODE_STAT_TERM > bigtxnnetthrnode->stat)
            {
                bigtxnnetthrnode->stat = RIPPLE_THRNODE_STAT_TERM;
                continue;
            }
            if(RIPPLE_THRNODE_STAT_EXITED != bigtxnnetthrnode->stat)
            {
                /* net 线程未退出, 等待 */
                continue;
            }

            bigtxnserialthrnode->stat = RIPPLE_THRNODE_STAT_FREE;
            bigtxnparsertrailthrnode->stat = RIPPLE_THRNODE_STAT_FREE;
            bigtxnloadrecthrnode->stat = RIPPLE_THRNODE_STAT_FREE;
            bigtxnnetthrnode->stat = RIPPLE_THRNODE_STAT_FREE;

            bigtxnserialthrnode = NULL;
            bigtxnparsertrailthrnode = NULL;
            bigtxnloadrecthrnode = NULL;
            bigtxnnetthrnode = NULL;

            /* 设置本线程退出 */
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            bigtxnmgr->stat = RIPPLE_BIGTXN_PUMPMANAGER_STAT_ABANDONED;
            break;
        }

        /* 等待net线程启动 */
        if(RIPPLE_BIGTXN_PUMPMANAGER_STAT_STARTING == bigtxnmgr->stat)
        {
            /* 查看是否已经启动成功 */
            jobcnt = 0;
            if(false == ripple_threads_countsubmgrjobthredsabovework(bigtxnmgr->thrsmgr->parents,
                                                                     bigtxnmgr->thrsmgr->childthrrefs,
                                                                     &jobcnt))
            {
                elog(RLOG_WARNING, "pump bigtxn count job thread above work stat error");
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                break;
            }

            if(jobcnt != bigtxnmgr->thrsmgr->childthrrefs->length)
            {
                continue;
            }
            continue;
        }
        else if(RIPPLE_BIGTXN_PUMPMANAGER_STAT_IDENTITY == bigtxnmgr->stat)
        {
            /* 等待网络接收身份验证后设置为 INPROCESS*/
            continue;
        }
        else if (RIPPLE_BIGTXN_PUMPMANAGER_STAT_INPROCESS == bigtxnmgr->stat)
        {
            /* 启动loadrecords/parser/serial */
            if(false == ripple_bigtxn_pumpmanager_startincrementjob(bigtxnmgr,
                                                                    &bigtxnparsertrailthrnode,
                                                                    &bigtxnserialthrnode,
                                                                    &bigtxnloadrecthrnode))
            {
                elog(RLOG_WARNING, "bigtxn pump start job thread error");
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                break;
            }

            /* 保证设置设置为其它状态时不会被覆盖 */
            if (RIPPLE_BIGTXN_PUMPMANAGER_STAT_INPROCESS == bigtxnmgr->stat)
            {
                bigtxnmgr->stat = RIPPLE_BIGTXN_PUMPMANAGER_STAT_WAITINGDONE;
            }
            continue;
        }
        else if (RIPPLE_BIGTXN_PUMPMANAGER_STAT_RESET == bigtxnmgr->stat)
        {
            if(NULL == bigtxnparsertrailthrnode
                && NULL == bigtxnloadrecthrnode
                && NULL == bigtxnserialthrnode)
            {
                bigtxnmgr->stat = RIPPLE_BIGTXN_PUMPMANAGER_STAT_IDENTITY;
                continue;
            }
            /* 设置 loadrecords 线程退出 */
            if(RIPPLE_THRNODE_STAT_TERM > bigtxnparsertrailthrnode->stat)
            {
                bigtxnparsertrailthrnode->stat = RIPPLE_THRNODE_STAT_TERM;
                continue;
            }

            if(RIPPLE_THRNODE_STAT_TERM > bigtxnloadrecthrnode->stat)
            {
                bigtxnloadrecthrnode->stat = RIPPLE_THRNODE_STAT_TERM;
                continue;
            }

            if(RIPPLE_THRNODE_STAT_TERM > bigtxnserialthrnode->stat)
            {
                bigtxnserialthrnode->stat = RIPPLE_THRNODE_STAT_TERM;
                continue;
            }

            ripple_bigtxn_pumpmanager_cacheclean(bigtxnmgr);

            if(RIPPLE_THRNODE_STAT_EXITED != bigtxnparsertrailthrnode->stat
                || RIPPLE_THRNODE_STAT_EXITED != bigtxnloadrecthrnode->stat
                || RIPPLE_THRNODE_STAT_EXITED != bigtxnserialthrnode->stat)
            {
                /* 解析线程未退出, 等待 */
                continue;
            }

            bigtxnloadrecthrnode->stat = RIPPLE_THRNODE_STAT_FREE;
            bigtxnparsertrailthrnode->stat = RIPPLE_THRNODE_STAT_FREE;
            bigtxnserialthrnode->stat = RIPPLE_THRNODE_STAT_FREE;

            bigtxnserialthrnode = NULL;
            bigtxnparsertrailthrnode = NULL;
            bigtxnloadrecthrnode = NULL;
            ripple_bigtxn_pumpmanager_cacheclean(bigtxnmgr);
            bigtxnmgr->stat = RIPPLE_BIGTXN_PUMPMANAGER_STAT_IDENTITY;
            continue;
        }
        else if (RIPPLE_BIGTXN_PUMPMANAGER_STAT_WAITINGDONE == bigtxnmgr->stat)
        {
            if(RIPPLE_THRNODE_STAT_EXITED != bigtxnparsertrailthrnode->stat)
            {
                /* 解析线程未退出, 等待 */
                continue;
            }

            /* 设置 loadrecords 线程退出 */
            if(RIPPLE_THRNODE_STAT_TERM > bigtxnloadrecthrnode->stat)
            {
                bigtxnloadrecthrnode->stat = RIPPLE_THRNODE_STAT_TERM;
                continue;
            }

            if(RIPPLE_THRNODE_STAT_EXITED != bigtxnnetthrnode->stat)
            {
                /* net线程未退出, 等待 */
                continue;
            }

            /* 设置 serial 线程退出 */
            if(RIPPLE_THRNODE_STAT_TERM > bigtxnserialthrnode->stat)
            {
                bigtxnserialthrnode->stat = RIPPLE_THRNODE_STAT_TERM;
                continue;
            }

            if(RIPPLE_THRNODE_STAT_EXITED != bigtxnserialthrnode->stat
                || RIPPLE_THRNODE_STAT_EXITED != bigtxnloadrecthrnode->stat)
            {
                /* loadrecords、serial线程未退出, 等待 */
                continue;
            }

            /* 所有线程都已经退出, 管理线程可以退出了 */
            bigtxnserialthrnode->stat = RIPPLE_THRNODE_STAT_FREE;
            bigtxnparsertrailthrnode->stat = RIPPLE_THRNODE_STAT_FREE;
            bigtxnloadrecthrnode->stat = RIPPLE_THRNODE_STAT_FREE;
            bigtxnnetthrnode->stat = RIPPLE_THRNODE_STAT_FREE;

            bigtxnserialthrnode = NULL;
            bigtxnparsertrailthrnode = NULL;
            bigtxnloadrecthrnode = NULL;
            bigtxnnetthrnode = NULL;
            
            /* 设置本线程退出 */
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            bigtxnmgr->stat = RIPPLE_BIGTXN_PUMPMANAGER_STAT_DONE;
            break;
        }
    }

    ripple_pthread_exit(NULL);
    return NULL;
}

/* 网闸添加删除大事务目录任务 */
void ripple_bigtxn_pumpmanager_gapdeletedir_add(ripple_bigtxn_pumpmanager *bigtxnmgr)
{
    /*
     * 服务器上的文件由 loadrecords 线程获取, 文件清理也应该由 loadrecords 处理
     */
    char* url                                   = NULL;
    char* cdata                                 = NULL;
    ripple_filetransfer_cleanpath* cleanpath    = NULL;

    /* url不配置不下载文件 */
    url = guc_getConfigOption(RIPPLE_CFG_KEY_FTPURL);
    if (url == NULL || url[0] == '\0')
    {
        return;
    }
    cdata = guc_getConfigOption(RIPPLE_CFG_KEY_TRAIL_DIR);

    /* 创建filetransfer节点加入队列 */
    cleanpath = ripple_filetransfer_cleanpath_init();
    cleanpath->base.type = RIPPLE_FILETRANSFERNODE_TYPE_DELETEDIR;
    snprintf(cleanpath->base.localpath, RIPPLE_MAXPATH, "%s/%s/%lu", cdata, RIPPLE_STORAGE_BIG_TRANSACTION_DIR, bigtxnmgr->xid);
    snprintf(cleanpath->base.localdir, RIPPLE_MAXPATH, "%s/%s/%lu", cdata, RIPPLE_STORAGE_BIG_TRANSACTION_DIR, bigtxnmgr->xid);
    snprintf(cleanpath->prefixpath, RIPPLE_MAXPATH, "%s/%s/%lu", guc_getConfigOption(RIPPLE_CFG_KEY_JOBNAME), RIPPLE_STORAGE_BIG_TRANSACTION_DIR, bigtxnmgr->xid);
    ripple_filetransfer_node_add(bigtxnmgr->filegap, (void*)cleanpath);
    cleanpath = NULL;
    return;
}

void ripple_bigtxn_pumpmanager_destory(void* args)
{
    ripple_bigtxn_pumpmanager *bigtxnmgr = NULL; 

    bigtxnmgr = (ripple_bigtxn_pumpmanager *)args;

    if (NULL == bigtxnmgr)
    {
        return;
    }

    ripple_queue_destroy(bigtxnmgr->recordscache, dlist_freevoid);

    if (bigtxnmgr->parser2txns)
    {
        ripple_cache_txn_destroy(bigtxnmgr->parser2txns);
    }

    if (bigtxnmgr->txn2filebuffers)
    {
        ripple_file_buffer_destroy(bigtxnmgr->txn2filebuffers);
    }

    rfree(bigtxnmgr);
}

