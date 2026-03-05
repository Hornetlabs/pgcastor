#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/mpage/mpage.h"
#include "threads/ripple_threads.h"
#include "queue/ripple_queue.h"
#include "task/ripple_task_slot.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_transcache.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netclient.h"
#include "metric/pump/ripple_statework_pump.h"
#include "works/dyworks/ripple_dyworks.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "refresh/ripple_refresh_table_syncstats.h"
#include "refresh/pump/ripple_refresh_pump.h"
#include "onlinerefresh/ripple_onlinerefresh_persist.h"
#include "onlinerefresh/pump/ripple_onlinerefesh_pump.h"
#include "serial/ripple_serial.h"
#include "parser/trail/ripple_parsertrail.h"
#include "filetransfer/ripple_filetransfer.h"
#include "filetransfer/ripple_filetransfer_ftp.h"
#include "filetransfer/pump/ripple_filetransfer_pump.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadtrailrecords.h"
#include "increment/pump/net/ripple_increment_pumpnet.h"
#include "increment/pump/serial/ripple_increment_pumpserial.h"
#include "increment/pump/split/ripple_increment_pumpsplittrail.h"
#include "increment/pump/parser/ripple_increment_pumpparsertrail.h"
#include "bigtransaction/persist/ripple_bigtxn_persist.h"
#include "bigtransaction/pump/ripple_bigtxn_pumpmanager.h"
#include "bigtransaction/ripple_bigtxn.h"
#include "increment/pump/ripple_increment_pump.h"

/* pump端 解析线程添加refresh */
static void ripple_increment_pumpstate_addrefresh(void* privdata, void* refresh)
{
    int iret = 0;
    ripple_increment_pump* pumpstate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "pump refresh add exception, privdata point is NULL");
    }

    pumpstate = (ripple_increment_pump*)privdata;

    iret = ripple_thread_lock(&pumpstate->refreshlock);
    if(0 != iret)
    {
        elog(RLOG_ERROR, "get lock error:%s", strerror(errno));
    }

    pumpstate->refresh = lappend(pumpstate->refresh, refresh);

    ripple_thread_unlock(&pumpstate->refreshlock);

    return;
}

/* pump端 refresh是否结束 */
static bool ripple_increment_pumpstate_isrefreshdown(void* privdata)
{
    int iret = 0;
    ripple_increment_pump* pumpstate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "pump refre isdown exception, privdata point is NULL");
    }

    pumpstate = (ripple_increment_pump*)privdata;

    iret = ripple_thread_lock(&pumpstate->refreshlock);
    if(0 != iret)
    {
        elog(RLOG_ERROR, "get lock error:%s", strerror(errno));
    }

    if (NULL == pumpstate->refresh
        || 0 == pumpstate->refresh->length)
    {
        ripple_thread_unlock(&pumpstate->refreshlock);
        return true;
    }

    ripple_thread_unlock(&pumpstate->refreshlock);
    return false;
}

/* pump端 设置pumpstate加载 capture trail 文件编号 */
static void ripple_increment_pumpstate_loadtrailno_set(void* privdata, uint64 loadtrailno)
{
    ripple_increment_pump* pumpstate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "pump metric loadtrailno set exception, privdata point is NULL");
    }

    pumpstate = (ripple_increment_pump*)privdata;

    if (NULL == pumpstate->pumpstate)
    {
        elog(RLOG_ERROR, "pump metric loadtrailno set exception, pumpstate point is NULL");
    }

    pumpstate->pumpstate->loadtrailno = loadtrailno;

    return;
}

/* pump端 设置pumpstate加载 capture trail 文件内的偏移 */
static void ripple_increment_pumpstate_loadtrailstart_set(void* privdata, uint64 loadtrailstart)
{
    ripple_increment_pump* pumpstate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "pump metric loadtrailstart set exception, privdata point is NULL");
    }

    pumpstate = (ripple_increment_pump*)privdata;

    if (NULL == pumpstate->pumpstate)
    {
        elog(RLOG_ERROR, "pump metric loadtrailstart set exception, pumpstate point is NULL");
    }

    pumpstate->pumpstate->loadtrailstart = loadtrailstart;

    return;
}

/* pump端 设置pumpstate重组后事务的 lsn */
static void ripple_increment_pumpstate_loadlsn_set(void* privdata, XLogRecPtr loadlsn)
{
    ripple_increment_pump* pumpstate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "pump metric loadlsn set exception, privdata point is NULL");
    }

    pumpstate = (ripple_increment_pump*)privdata;

    if (NULL == pumpstate->pumpstate)
    {
        elog(RLOG_ERROR, "pump metric loadlsn set exception, pumpstate point is NULL");
    }

    if (InvalidXLogRecPtr != loadlsn)
    {
        pumpstate->pumpstate->loadlsn = loadlsn;
    }

    return;
}

/* pump端 设置pumpstate重组后事务的提交的时间戳 */
static void ripple_increment_pumpstate_loadtimestamp_set(void* privdata, TimestampTz loadtimestamp)
{
    ripple_increment_pump* pumpstate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "pump metric loadtimestamp set exception, privdata point is NULL");
    }

    pumpstate = (ripple_increment_pump*)privdata;

    if (NULL == pumpstate->pumpstate)
    {
        elog(RLOG_ERROR, "pump metric loadtimestamp set exception, pumpstate point is NULL");
    }

    if (0 != loadtimestamp)
    {
        pumpstate->pumpstate->loadtimestamp = loadtimestamp;
    }

    return;
}

/* pump端 设置pumpstate发送出去的 lsn */
static void ripple_increment_pumpstate_sendlsn_set(void* privdata, XLogRecPtr sendlsn)
{
    ripple_increment_pump* pumpstate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "pump metric sendlsn set exception, privdata point is NULL");
    }

    pumpstate = (ripple_increment_pump*)privdata;

    if (NULL == pumpstate->pumpstate)
    {
        elog(RLOG_ERROR, "pump metric sendlsn set exception, pumpstate point is NULL");
    }

    pumpstate->pumpstate->sendlsn = sendlsn;

    return;
}

/* pump端 设置pumpstate的发送到 collector 的 trail 编号 */
static void ripple_increment_pumpstate_sendtrailno_set(void* privdata, uint64 sendtrailno)
{
    ripple_increment_pump* pumpstate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "pump metric sendtrailno set exception, privdata point is NULL");
    }

    pumpstate = (ripple_increment_pump*)privdata;

    if (NULL == pumpstate->pumpstate)
    {
        elog(RLOG_ERROR, "pump metric sendtrailno set exception, pumpstate point is NULL");
    }

    pumpstate->pumpstate->sendtrailno = sendtrailno;

    return;
}

/* pump端 设置pumpstate已发送的 trail 文件内的偏移 */
static void ripple_increment_pumpstate_sendtrailstart_set(void* privdata, uint64 sendtrailstart)
{
    ripple_increment_pump* pumpstate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "pump metric sendtrailstart set exception, privdata point is NULL");
    }

    pumpstate = (ripple_increment_pump*)privdata;

    if (NULL == pumpstate->pumpstate)
    {
        elog(RLOG_ERROR, "pump metric sendtrailstart set exception, pumpstate point is NULL");
    }

    pumpstate->pumpstate->sendtrailstart = sendtrailstart;

    return;
}

/* pump端 设置pumpstate已发送的 trail 文件内的偏移 */
static void ripple_increment_pumpstate_sendtimestamp_set(void* privdata, TimestampTz sendtimestamp)
{
    ripple_increment_pump* pumpstate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "pump metric sendtimestamp set exception, privdata point is NULL");
    }

    pumpstate = (ripple_increment_pump*)privdata;

    if (NULL == pumpstate->pumpstate)
    {
        elog(RLOG_ERROR, "pump metric sendtimestamp set exception, pumpstate point is NULL");
    }
    if (0 != sendtimestamp)
    {
        pumpstate->pumpstate->sendtimestamp = sendtimestamp;
    }

    return;
}

/* pump端 添加filetransfernode */
static void ripple_pumpstat_filetransfernode_add(void* privdata, void* filetransfernode)
{
    ripple_increment_pump* pumpstate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "pump filetransfernode add exception, privdata point is NULL");
    }

    pumpstate = (ripple_increment_pump*)privdata;

    if (NULL == pumpstate->filetransfernode)
    {
        elog(RLOG_ERROR, "pump filetransfernode add exception, filetransfernode point is NULL");
    }

    ripple_filetransfer_node_add((void*)pumpstate->filetransfernode, filetransfernode);

    return;
}

/* pump端 获取filetransfernode队列 */
static void* ripple_pumpstat_filetransfernode_get(void* privdata)
{
    ripple_increment_pump* pumpstate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "pump filetransfernode add exception, privdata point is NULL");
    }

    pumpstate = (ripple_increment_pump*)privdata;

    if (NULL == pumpstate->filetransfernode)
    {
        elog(RLOG_ERROR, "pump filetransfernode add exception, filetransfernode point is NULL");
    }

    return (void*)pumpstate->filetransfernode;
}

/* 新增大事务 */
static void  ripple_increment_pump_addbigtxn(void* privdata,
                                             FullTransactionId xid,
                                             ripple_recpos* pos)
{
    int iret                                    = 0;
    ripple_bigtxn_persist *persist              = NULL;
    ripple_increment_pump* pumpstate            = NULL;

    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "pump bigtxn pumpmanager addbigtxn exception, privdata point is NULL");
    }

    pumpstate = (ripple_increment_pump*)privdata;

    if (NULL == pumpstate->txnpersist)
    {
        elog(RLOG_ERROR, "pump bigtxn pumpmanager addbigtxn exception, txnpersist point is NULL");
    }

    persist = pumpstate->txnpersist;

ripple_pump_bigtxn_pumpmanager_addbigtxn_retry:
    /* 获取锁, 这里只用了一个互斥锁, 可能会造成锁等待(现阶段不会发生) */
    iret = ripple_thread_lock(&pumpstate->bigtxnlock);
    if(0 != iret)
    {
        elog(RLOG_ERROR, "get lock error:%s", strerror(errno));
    }

    /* 只有在 pos >= rewind时才做处理 */
    if (ripple_bigtxn_pumpmanager_compare_recpos(&persist->rewind, pos) >= 0)
    {
        bool exist = false;
        dlistnode *dnode = NULL;
        dlistnode* dlnodenext                   = NULL;
        ripple_bigtxn_persistnode *persistnode  = NULL;

        /* 确保存在 */
        dnode = persist->dpersistnodes ? persist->dpersistnodes->head : NULL;

        /* 遍历persist, 检查是否有相同事务 */
        for (; dnode; dnode = dlnodenext)
        {
            dlnodenext = dnode->next;
            persistnode = (ripple_bigtxn_persistnode *)dnode->value;

            /* 是否存在事务号相同的大事务 */
            if (xid == persistnode->xid)
            {
                /* 存在 */
                exist = true;

                /* 检查pos是否和记录中相同 */
                if (ripple_bigtxn_pumpmanager_compare_recpos(&persistnode->begin, pos) > 0)
                {
                    /* 位置不同, 发生了切换, 检查状态 */
                    if (persistnode->stat == RIPPLE_BIGTXN_PERSISTNODE_STAT_ABANDON)
                    {
                        /* 已放弃, 删除节点, 继续遍历 */
                        dlist_delete(persist->dpersistnodes, dnode, ripple_bigtxn_persistnode_free);
                        persist->count -= 1;
                        continue;
                    }
                    else
                    {
                        /* 释放锁, 等待50ms后重试 */
                        ripple_thread_unlock(&pumpstate->bigtxnlock);
                        usleep(50000);
                        goto ripple_pump_bigtxn_pumpmanager_addbigtxn_retry;
                    }
                }
                else
                {
                    break;
                }
            }
        }

        /* 遍历结束, 判断是否存在, 只有不存在时才需要新增 */
        if (!exist)
        {
            ripple_bigtxn_persistnode *pernode = NULL;
            ripple_bigtxn_pumpmanager *pumpmanager = NULL;

            /* 构建persist node并初始化 */
            pernode = ripple_bigtxn_persist_node_init();
            ripple_bigtxn_persistnode_set_begin(pernode, pos);
            ripple_bigtxn_persistnode_set_xid(pernode, xid);
            ripple_bigtxn_persistnode_set_stat_init(pernode);

            if(true == dlist_isnull(persist->dpersistnodes))
            {
                persist->rewind.trail.fileid = pos->trail.fileid;
                persist->rewind.trail.offset = pos->trail.offset;
            }
            persist->dpersistnodes = dlist_put(persist->dpersistnodes, pernode);
            persist->count += 1;

            /* 构建pumpmanager并初始化 设置xid 和 begin*/
            pumpmanager = ripple_bigtxn_pumpmanager_init();
            ripple_bigtxn_pumpmanager_set_begin(pumpmanager, pos);
            pumpmanager->xid = xid;
            pumpmanager->filegap = pumpstate->filetransfernode;

            pumpmanager->stat = RIPPLE_BIGTXN_PUMPMANAGER_STAT_INIT;

            pumpstate->bigtxn = dlist_put(pumpstate->bigtxn, pumpmanager);

            /* persists落盘 */
            if (!ripple_bigtxn_write_persist(pumpstate->txnpersist))
            {
                /* 错误处理 */
                elog(RLOG_ERROR, "bigtxn pump flush persist error");
            }
        }
    }

    /* 设置完毕 解锁 */
    ripple_thread_unlock(&pumpstate->bigtxnlock);
}

/* 大事务---设置指定事务结束位置 */
static bool ripple_increment_pump_bigtxnendpos_set(void* privdata,
                                                    FullTransactionId xid,
                                                    ripple_recpos* pos)
{
    bool find                               = false;
    int iret                                = 0;
    dlistnode *dnode_persist                = NULL;
    dlistnode *dnode_pump                   = NULL;
    ripple_bigtxn_persist *persist          = NULL;
    ripple_increment_pump* pumpstate        = NULL;
    ripple_bigtxn_pumpmanager *bigtxnmgr    = NULL;
    ripple_bigtxn_persistnode *persistnode  = NULL;

    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "pump bigtxnmanager bigtxnendpos set exception, privdata point is NULL");
    }

    pumpstate = (ripple_increment_pump*)privdata;

    /* 获取锁, 这里只用了一个互斥锁, 可能会造成锁等待(现阶段不会发生) */
    iret = ripple_thread_lock(&pumpstate->bigtxnlock);
    if(0 != iret)
    {
        elog(RLOG_ERROR, "get lock error:%s", strerror(errno));
    }

    if (NULL == pumpstate->txnpersist)
    {
        ripple_thread_unlock(&pumpstate->bigtxnlock);
        elog(RLOG_ERROR, "pump bigtxnmanager bigtxnendpos set exception, txnpersist point is NULL");
    }

    /* 确保存在 */
    if (true == dlist_isnull(pumpstate->txnpersist->dpersistnodes)
        || true == dlist_isnull(pumpstate->bigtxn))
    {
        ripple_thread_unlock(&pumpstate->bigtxnlock);
        elog(RLOG_WARNING, "set bigtxn end pos, node or persist is NULL");
        return false;
    }
    persist = pumpstate->txnpersist;

    /* 遍历persist node */
    for (dnode_persist = persist->dpersistnodes->head; dnode_persist; dnode_persist = dnode_persist->next)
    {
        persistnode = (ripple_bigtxn_persistnode *)dnode_persist->value;

        /* 是否存在事务号相同的大事务 */
        if (persistnode->xid == xid)
        {
            /* 设置end信息 */
            find = true;
            ripple_bigtxn_persistnode_set_end(persistnode, pos);
        }
    }

    /* 遍历pumpnode */
    for (dnode_pump = pumpstate->bigtxn->head; dnode_pump; dnode_pump = dnode_pump->next)
    {
        bigtxnmgr = (ripple_bigtxn_pumpmanager *)dnode_pump->value;
        /* 是否存在事务号相同的大事务 */
        if (bigtxnmgr->xid == xid)
        {
            /* 设置end信息 */
            ripple_bigtxn_pumpmanager_set_end(bigtxnmgr, pos);
        }
    }

    if (!find)
    {
        elog(RLOG_WARNING, "can't find persist by xid: %lu", xid);
        ripple_thread_unlock(&pumpstate->bigtxnlock);
        return false;
    }

    /* 设置完毕 解锁 */
    ripple_thread_unlock(&pumpstate->bigtxnlock);
    return true;
}

/* onlinerefresh---设置指定uuid的结束位置 */
static bool ripple_increment_pump_onlinerefresh_setendpos(void* privdata,
                                                          void* in_uuid,
                                                          ripple_recpos* pos)
{
    bool find                                       = false;
    int iret                                        = 0;
    ripple_uuid_t* uuid                             = NULL;
    dlistnode *dnode_persist                        = NULL;
    dlistnode *dnode_pump                           = NULL;
    ripple_increment_pump* pumpstate                = NULL;
    ripple_onlinerefresh_pump* olrmgr               = NULL;
    ripple_onlinerefresh_persist *persist           = NULL;
    ripple_onlinerefresh_persistnode *persistnode   = NULL;

    if (NULL == privdata || NULL == in_uuid)
    {
        elog(RLOG_ERROR, "pump onlinerefresh endpos set exception, privdata point is NULL");
    }

    pumpstate = (ripple_increment_pump*)privdata;

    /* 获取锁, 这里只用了一个互斥锁, 可能会造成锁等待(现阶段不会发生) */
    iret = ripple_thread_lock(&pumpstate->onlinerefreshlock);
    if(0 != iret)
    {
        elog(RLOG_ERROR, "get lock error:%s", strerror(errno));
    }

    if (NULL == pumpstate->olrpersist)
    {
        ripple_thread_unlock(&pumpstate->onlinerefreshlock);
        elog(RLOG_ERROR, "pump onlinerefresh endpos set exception, txnpersist point is NULL");
    }

    persist = pumpstate->olrpersist;
    uuid = (ripple_uuid_t*)in_uuid;

    /* 不存在onlinerefresh不做处理 */
    if (true == dlist_isnull(persist->dpersistnodes) || true == dlist_isnull(pumpstate->onlinerefresh))
    {
        elog(RLOG_INFO, "set onlinerefresh end pos, node or persist is NULL");
        ripple_thread_unlock(&pumpstate->onlinerefreshlock);
        return true;
    }

    /* 遍历persist node */
    for (dnode_persist = persist->dpersistnodes->head; dnode_persist; dnode_persist = dnode_persist->next)
    {
        persistnode = (ripple_onlinerefresh_persistnode *)dnode_persist->value;

        /* 是否存在uuid相同的onlinerefresh */
        if (0 == memcmp(persistnode->uuid.data, uuid->data, RIPPLE_UUID_LEN))
        {
            /* 设置end信息 */
            find = true;
            ripple_onlinerefresh_persistnode_endset(persistnode, *pos);
        }
    }

    /* 遍历pumpnode */
    for (dnode_pump = pumpstate->onlinerefresh->head; dnode_pump; dnode_pump = dnode_pump->next)
    {
        olrmgr = (ripple_onlinerefresh_pump *)dnode_pump->value;

        /* 是否存在uuid相同的onlinerefresh */
        if (0 == memcmp(olrmgr->no.data, uuid->data, RIPPLE_UUID_LEN))
        {
            /* 设置end信息 */
            ripple_onlinerefresh_pumpmanager_setend(olrmgr, pos);
        }
    }

    if (!find)
    {
        char* cuuid = NULL;
        cuuid = uuid2string(uuid);
        elog(RLOG_WARNING, "can't find persist by uuid: %s", cuuid);
        rfree(cuuid);
        ripple_thread_unlock(&pumpstate->onlinerefreshlock);
        return false;
    }

    /* 设置完毕 解锁 */
    ripple_thread_unlock(&pumpstate->onlinerefreshlock);
    return true;
}

/* 设置未接受到end的事务状态为放弃 */
static bool ripple_increment_pump_bigtxnsetabandon(void* privdata)
{
    int iret                                = 0;
    dlistnode *dnode_pump                   = NULL;
    ripple_bigtxn_pumpmanager *bigtxnmgr    = NULL;
    ripple_increment_pump* pumpstate        = NULL;

    if (NULL == privdata)
    {
        elog(RLOG_WARNING, "pump bigtxnmanager bigtxnendpos set exception, privdata point is NULL");
        return false;
    }

    pumpstate = (ripple_increment_pump*)privdata;

    /* 不存在大事务不做处理 */
    if (true == dlist_isnull(pumpstate->bigtxn))
    {
        elog(RLOG_INFO, "bigtxn setabandon, bigtxnmgr is NULL");
        return true;
    }

    /* 获取锁, 这里只用了一个互斥锁, 可能会造成锁等待(现阶段不会发生) */
    iret = ripple_thread_lock(&pumpstate->bigtxnlock);
    if(0 != iret)
    {
        elog(RLOG_WARNING, "get lock error:%s", strerror(errno));
        return false;
    }

    /* 遍历pumpnode */
    for (dnode_pump = pumpstate->bigtxn->head; dnode_pump; dnode_pump = dnode_pump->next)
    {
        bigtxnmgr = (ripple_bigtxn_pumpmanager *)dnode_pump->value;
        if (0 == bigtxnmgr->end.trail.fileid
            && 0 == bigtxnmgr->end.trail.offset)
        {
            elog(RLOG_DEBUG, "pump bigtxnsetabandon:%lu", bigtxnmgr->xid);
            bigtxnmgr->abandon = true;
        }
    }

    /* 设置完毕 解锁 */
    ripple_thread_unlock(&pumpstate->bigtxnlock);
    return true;
}

/* 设置未接受到end的事务状态为放弃 */
static bool ripple_increment_pump_onlinerefreshsetabandon(void* privdata, void** list)
{
    int iret                                = 0;
    List* luuid                             = NULL;
    ripple_uuid_t* uuid                     = NULL;
    dlistnode *dnode_pump                   = NULL;
    ripple_onlinerefresh_pump *olrmgr       = NULL;
    ripple_increment_pump* pumpstate        = NULL;

    if (NULL == privdata)
    {
        elog(RLOG_WARNING, "pump onlinerefresh setabandon exception, privdata point is NULL");
        return false;
    }

    pumpstate = (ripple_increment_pump*)privdata;

    /* 不存在大事务不做处理 */
    if (true == dlist_isnull(pumpstate->onlinerefresh))
    {
        elog(RLOG_INFO, "pump onlinerefresh setabandon, onlinerefresh is NULL");
        return true;
    }

    /* 获取锁, 这里只用了一个互斥锁, 可能会造成锁等待(现阶段不会发生) */
    iret = ripple_thread_lock(&pumpstate->onlinerefreshlock);
    if(0 != iret)
    {
        elog(RLOG_WARNING, "get lock error:%s", strerror(errno));
        return false;
    }

    /* 遍历pumpnode */
    for (dnode_pump = pumpstate->onlinerefresh->head; dnode_pump; dnode_pump = dnode_pump->next)
    {
        olrmgr = (ripple_onlinerefresh_pump *)dnode_pump->value;
        if (0 == olrmgr->end.trail.fileid
            && 0 == olrmgr->end.trail.offset)
        {
            elog(RLOG_DEBUG, "pump onlinerefresh abandon:%s", uuid2string(&olrmgr->no));
            olrmgr->abandon = true;
            uuid = ripple_uuid_copy(&olrmgr->no);
            luuid = lappend(luuid, uuid);
        }
    }

    /* 设置完毕 解锁 */
    ripple_thread_unlock(&pumpstate->onlinerefreshlock);
    *list = (void*)luuid;
    return true;
}

/* 初始化 */
ripple_increment_pump* ripple_increment_pump_init(void)
{
    ripple_increment_pump* pumpstate = NULL;
    pumpstate = (ripple_increment_pump*)rmalloc1(sizeof(ripple_increment_pump));
    if (NULL == pumpstate)
    {
        elog(RLOG_WARNING, "pump init error");
        return NULL;
    }
    rmemset0(pumpstate, 0, '\0', sizeof(ripple_increment_pump));

    /* loadrecords--->解析器 */
    pumpstate->recordscache = ripple_queue_init();
    if(NULL == pumpstate->recordscache)
    {
        elog(RLOG_WARNING, "pump init error");
        return NULL;
    }

    /* 解析器--->序列化 */
    pumpstate->parser2serialtxns = ripple_cache_txn_init();
    if(NULL == pumpstate->parser2serialtxns)
    {
        elog(RLOG_WARNING, "pump init error");
        return NULL;
    }

    /* 序列化--->flush */
    pumpstate->txn2filebuffer = ripple_file_buffer_init();
    if(NULL == pumpstate->txn2filebuffer)
    {
        elog(RLOG_WARNING, "pump init error");
        return NULL;
    }

    /* 网闸队列 */
    pumpstate->filetransfernode = ripple_queue_init();
    if(NULL == pumpstate->filetransfernode)
    {
        elog(RLOG_WARNING, "pump init error");
        return NULL;
    }

    /* 线程管理结构 */
    pumpstate->threads = ripple_threads_init();
    if(NULL == pumpstate->threads)
    {
        elog(RLOG_WARNING, "pump init error");
        return NULL;
    }

    /*----------------指标线程初始化 begin------------------------*/
    pumpstate->pumpstate = ripple_state_pump_init();
    ripple_thread_mutex_init(&pumpstate->onlinerefreshlock, NULL);
    ripple_thread_mutex_init(&pumpstate->refreshlock, NULL);
    ripple_thread_mutex_init(&pumpstate->bigtxnlock, NULL);

    /*----------------指标线程初始化   end------------------------*/

    /*----------------网闸结构初始化 begin------------------------*/
    pumpstate->ftptransfer = ripple_filetransfer_pump_init();
    pumpstate->ftptransfer->filetransfernode = pumpstate->filetransfernode;

    /*----------------网闸结构初始化   end------------------------*/

    /*----------------加载 records 结构初始化 begin---------------*/
    pumpstate->splittrailctx = ripple_increment_pumpsplittrail_init();
    pumpstate->splittrailctx->privdata = (void*)pumpstate;
    pumpstate->splittrailctx->recordscache = pumpstate->recordscache;
    pumpstate->splittrailctx->callback.parsertrail_state_set = ripple_increment_pumpstate_parsertrail_state_set;
    pumpstate->splittrailctx->callback.setmetricloadtrailstart = ripple_increment_pumpstate_loadtrailstart_set;
    pumpstate->splittrailctx->callback.pumpstate_filetransfernode_add = ripple_pumpstat_filetransfernode_add;

    /*----------------加载 records 结构初始化   end---------------*/

    /*----------------解析器结构初始化 begin----------------------*/
    pumpstate->pumpparsertrail = ripple_increment_pumpparsertrail_init();
    pumpstate->pumpparsertrail->privdata = (void*)pumpstate;
    pumpstate->pumpparsertrail->recordscache = pumpstate->recordscache;
    pumpstate->pumpparsertrail->parsertrail.parser2txn = pumpstate->parser2serialtxns;
    pumpstate->pumpparsertrail->callback.serialstate_state_set = ripple_increment_pumpstate_serialtrail_state_set;
    pumpstate->pumpparsertrail->callback.splittrail_state_set = ripple_increment_pumpstate_splittrail_state_set;
    pumpstate->pumpparsertrail->callback.addonlinerefresh = ripple_increment_pumpstate_addonlinerefresh;
    pumpstate->pumpparsertrail->callback.addrefresh = ripple_increment_pumpstate_addrefresh;
    pumpstate->pumpparsertrail->callback.isrefreshdown = ripple_increment_pumpstate_isrefreshdown;
    pumpstate->pumpparsertrail->callback.filetransfernode_add = ripple_pumpstat_filetransfernode_add;
    pumpstate->pumpparsertrail->callback.filetransfernode_get = ripple_pumpstat_filetransfernode_get;
    pumpstate->pumpparsertrail->callback.bigtxn_add = ripple_increment_pump_addbigtxn;
    pumpstate->pumpparsertrail->callback.bigtxn_end = ripple_increment_pump_bigtxnendpos_set;
    pumpstate->pumpparsertrail->callback.bigtxn_setabandon = ripple_increment_pump_bigtxnsetabandon;
    pumpstate->pumpparsertrail->callback.onlinerefresh_setabandon = ripple_increment_pump_onlinerefreshsetabandon;
    pumpstate->pumpparsertrail->callback.onlinerefresh_end = ripple_increment_pump_onlinerefresh_setendpos;
    pumpstate->pumpparsertrail->callback.setmetricloadlsn = ripple_increment_pumpstate_loadlsn_set;
    pumpstate->pumpparsertrail->callback.setmetricloadtimestamp = ripple_increment_pumpstate_loadtimestamp_set;

    /*----------------解析器结构初始化 end----------------------*/

    /*----------------连接collectro 结构初始化 begin------------*/
    pumpstate->clientstate = ripple_increment_pumpnet_init();
    pumpstate->clientstate->privdata = (void*)pumpstate;
    pumpstate->clientstate->txn2filebuffer = pumpstate->txn2filebuffer;
    pumpstate->clientstate->callback.splittrail_statefileid_set = ripple_increment_pumpstate_splittrail_statefileid_set;
    pumpstate->clientstate->callback.serialstate_state_set = ripple_increment_pumpstate_serialtrail_state_set;
    pumpstate->clientstate->callback.setmetricsendlsn = ripple_increment_pumpstate_sendlsn_set;
    pumpstate->clientstate->callback.setmetricsendtrailno = ripple_increment_pumpstate_sendtrailno_set;
    pumpstate->clientstate->callback.setmetricloadtrailno = ripple_increment_pumpstate_loadtrailno_set;
    pumpstate->clientstate->callback.setmetricsendtrailstart = ripple_increment_pumpstate_sendtrailstart_set;
    pumpstate->clientstate->callback.setmetricsendtimestamp = ripple_increment_pumpstate_sendtimestamp_set;
    pumpstate->clientstate->callback.pumpstate_addrefresh = ripple_increment_pumpstate_addrefresh;
    pumpstate->clientstate->callback.pumpstate_isrefreshdown = ripple_increment_pumpstate_isrefreshdown;
    pumpstate->clientstate->callback.pumpnet_filetransfernode_add = ripple_pumpstat_filetransfernode_add;

    /*----------------连接collectro 结构初始化   end------------*/

    /*----------------序列化结构初始化 begin----------------------*/
    pumpstate->serialstate = ripple_increment_pumpserialstate_init();
    pumpstate->serialstate->privdata = (void*)pumpstate;
    pumpstate->serialstate->parser2serialtxns = pumpstate->parser2serialtxns;
    pumpstate->serialstate->base.txn2filebuffer = pumpstate->txn2filebuffer;
    pumpstate->serialstate->callback.clientstat_state_set = ripple_increment_pumpstate_networkclient_state_set;
    pumpstate->serialstate->callback.parserstat_state_set = ripple_increment_pumpstate_parsertrail_state_set;
    pumpstate->serialstate->callback.networkclientstate_cfileid_get = ripple_increment_pumpstate_networkclient_cfileid_get;

    /*----------------序列化结构初始化   end----------------------*/

    return pumpstate;

}

/* networkclient设置splittrail拆分的fileid和状态 */
void ripple_increment_pumpstate_splittrail_statefileid_set(void* privdata, int state, uint64 fileid)
{
    ripple_recpos recpos;
    ripple_increment_pump* pumpstate = NULL;
    if (NULL == privdata)
    {
        /* never come here */
        elog(RLOG_ERROR, "Pump splittrail statefileid set exception, privdata point is NULL");
    }

    pumpstate = (ripple_increment_pump*)privdata;

    if (NULL == pumpstate->splittrailctx)
    {
        /* never come here */
        elog(RLOG_ERROR, "Pump splittrail statefileid set  exception, splittrailctx point is NULL");
    }

    pumpstate->splittrailctx->state = state;
    recpos.trail.fileid = fileid;
    recpos.trail.offset = 0;
    ripple_increment_pumpsplittrail_reset_position(pumpstate->splittrailctx, &recpos);

    elog(RLOG_DEBUG, "Pump split state change, fileid: %lu", fileid);
    return;
}

/* parserwork_trail设置splittrail拆分的状态 */
void ripple_increment_pumpstate_splittrail_state_set(void* privdata, int state)
{
    ripple_increment_pump* pumpstate = NULL;
    if (NULL == privdata)
    {
        /* never come here */
        elog(RLOG_ERROR, "Pump splittrail state exception, privdata point is NULL");
    }

    pumpstate = (ripple_increment_pump*)privdata;

    if (NULL == pumpstate->splittrailctx)
    {
        /* never come here */
        elog(RLOG_ERROR, "Pump splittrail state exception, splittrailctx point is NULL");
    }

    pumpstate->splittrailctx->state = state;

    return;
}

/* 设置解析线程状态 */
void ripple_increment_pumpstate_parsertrail_state_set(void* privdata, int state)
{
    ripple_increment_pump* pumpstate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "Pump parsertrail state exception, privdata point is NULL");
    }

    pumpstate = (ripple_increment_pump*)privdata;

    if (NULL == pumpstate->pumpparsertrail)
    {
        elog(RLOG_ERROR, "Pump parsertrail state exception, pumpparsertrail point is NULL");
    }

    pumpstate->pumpparsertrail->state = state;
}

/* 设置序列化状态 */
void ripple_increment_pumpstate_serialtrail_state_set(void* privdata, int state)
{
    ripple_increment_pump* pumpstate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "Pump serialtrail state exception, privdata point is NULL");
    }

    pumpstate = (ripple_increment_pump*)privdata;

    if (NULL == pumpstate->serialstate)
    {
        elog(RLOG_ERROR, "Pump serialtrail state exception, serialstate point is NULL");
    }

    pumpstate->serialstate->state = state;
}

/* 设置网络端状态 */
void ripple_increment_pumpstate_networkclient_state_set(void* privdata, int state)
{
    ripple_increment_pump* pumpstate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "Pump networkclient state exception, privdata point is NULL");
    }

    pumpstate = (ripple_increment_pump*)privdata;

    if (NULL == pumpstate->clientstate)
    {
        elog(RLOG_ERROR, "Pump networkclient state exception, networkclient point is NULL");
    }

    pumpstate->clientstate->state = state;
}

/* 获取网络端cfileid */
uint64 ripple_increment_pumpstate_networkclient_cfileid_get(void* privdata)
{
    ripple_increment_pump* pumpstate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "Pump networkclient cfileid exception, privdata point is NULL");
    }

    pumpstate = (ripple_increment_pump*)privdata;

    if (NULL == pumpstate->clientstate)
    {
        elog(RLOG_ERROR, "Pump networkclient cfileid exception, networkclient point is NULL");
    }

    return pumpstate->clientstate->crecpos.trail.fileid;
}

void ripple_increment_pumpstate_addonlinerefresh(void* privdata, void* onlinerefresh)
{
    bool exist                                      = false;
    dlistnode *dnode                                = NULL;
    dlistnode* dlnodenext                           = NULL;
    ripple_increment_pump* pumpstate                = NULL;
    ripple_onlinerefresh_pump* olrnode              = NULL;
    ripple_onlinerefresh_persist* persist           = NULL;
    ripple_onlinerefresh_persistnode* persistnode   = NULL;

    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "Pump addonlinerefresh exception, privdata point is NULL");
    }

    pumpstate = (ripple_increment_pump*)privdata;
    olrnode = (ripple_onlinerefresh_pump*)onlinerefresh;

    ripple_thread_lock(&pumpstate->onlinerefreshlock);

    if (NULL == pumpstate->olrpersist)
    {
        elog(RLOG_ERROR, "Pump addonlinerefresh exception, olrpersist point is NULL");
    }

    persist = pumpstate->olrpersist;

    if (ripple_bigtxn_pumpmanager_compare_recpos(&persist->rewind, &olrnode->begin) < 0)
    {
        ripple_onlinerefresh_pump_free(onlinerefresh);
        ripple_thread_unlock(&pumpstate->onlinerefreshlock);
        return;
    }

ripple_increment_pumpstate_addonlinerefresh_retry:
    dnode = persist->dpersistnodes ? persist->dpersistnodes->head : NULL;

    /* 遍历persist, 检查是否有相同事务 */
    for (; dnode; dnode = dlnodenext)
    {
        dlnodenext = dnode->next;
        persistnode = (ripple_onlinerefresh_persistnode *)dnode->value;

        if (0 == memcmp(olrnode->no.data, persistnode->uuid.data, RIPPLE_UUID_LEN))
        {
            exist = true;
            if (ripple_bigtxn_pumpmanager_compare_recpos(&persist->rewind, &olrnode->begin) > 0)
            {
                if (RIPPLE_ONLINEREFRESH_PERSISTNODE_STAT_ABANDON == persistnode->stat)
                {
                    /* 已放弃, 删除节点, 继续遍历 */
                    dlist_delete(persist->dpersistnodes, dnode, ripple_onlinerefresh_persistnode_free);
                    continue;
                }
                else
                {
                    ripple_thread_unlock(&pumpstate->onlinerefreshlock);
                    usleep(50000);
                    goto ripple_increment_pumpstate_addonlinerefresh_retry;
                }
            }
            else
            {
                ripple_onlinerefresh_pump_free(onlinerefresh);
                break;
            }
        }

    }

    if (false == exist)
    {
        /* 构建persist node并初始化 */
        persistnode = ripple_onlinerefresh_persistnode_init();
        ripple_onlinerefresh_persistnode_statset(persistnode, RIPPLE_ONLINEREFRESH_PERSISTNODE_STAT_INIT);
        ripple_onlinerefresh_persistnode_uuidset(persistnode, &olrnode->no);
        ripple_onlinerefresh_persistnode_incrementset(persistnode, olrnode->increment);
        ripple_onlinerefresh_persistnode_beginset(persistnode, olrnode->begin);

        persistnode->refreshtbs = olrnode->tablesyncstats;
        if(true == dlist_isnull(persist->dpersistnodes))
        {
            persist->rewind.trail.fileid = olrnode->begin.trail.fileid;
            persist->rewind.trail.offset = olrnode->begin.trail.offset;
        }

        pumpstate->olrpersist->dpersistnodes = dlist_put(pumpstate->olrpersist->dpersistnodes, persistnode);

        /* persists落盘 */
        if (!ripple_onlinerefresh_persist_write(pumpstate->olrpersist))
        {
            /* 错误处理 */
            elog(RLOG_ERROR, "onlinerefresh pump flush persist error");
        }
        pumpstate->onlinerefresh = dlist_put(pumpstate->onlinerefresh, onlinerefresh);
    }

    ripple_thread_unlock(&pumpstate->onlinerefreshlock);
}

/*------------refresh 管理 begin-------------------------*/
/* 启动 refresh */
bool ripple_increment_pump_startrefresh(ripple_increment_pump* incpump)
{
    int iret = 0;
    ListCell* lc = NULL;
    ripple_refresh_pump* rpump = NULL;

    iret = ripple_thread_lock(&incpump->refreshlock);
    if(0 != iret)
    {
        elog(RLOG_WARNING, "get lock error:%s", strerror(errno));
        return false;
    }

    /* 遍历 refresh 节点并启动 */
    foreach(lc, incpump->refresh)
    {
        rpump = (ripple_refresh_pump*)lfirst(lc);
        if (RIPPLE_REFRESH_PUMP_STAT_INIT != rpump->stat)
        {
            continue;
        }

        /* 启动 refresh */
        /* 设置为启动中 */
        rpump->stat = RIPPLE_REFRESH_PUMP_STAT_STARTING;
        if(false == ripple_threads_addsubmanger(incpump->threads,
                                                RIPPLE_THRNODE_IDENTITY_PUMP_REFRESH_MGR,
                                                incpump->persistno,
                                                &rpump->thrsmgr,
                                                (void*)rpump,
                                                NULL,
                                                NULL,
                                                ripple_refresh_pump_main))
        {
            elog(RLOG_WARNING, "start refresh manager failed");
            ripple_thread_unlock(&incpump->refreshlock);
            return false;
        }
    }

    ripple_thread_unlock(&incpump->refreshlock);
    return true;
}

/* 回收 refresh 节点 */
bool ripple_increment_pump_tryjoinonrefresh(ripple_increment_pump* incpump)
{
    int iret = 0;
    List* nl = NULL;
    ListCell* lc = NULL;
    ripple_refresh_pump* rpump = NULL;

    iret = ripple_thread_lock(&incpump->refreshlock);
    if(0 != iret)
    {
        elog(RLOG_WARNING, "get lock error:%s", strerror(errno));
        return false;
    }

    foreach(lc, incpump->refresh)
    {
        rpump = (ripple_refresh_pump*)lfirst(lc);
        if (RIPPLE_REFRESH_PUMP_STAT_DONE != rpump->stat)
        {
            nl = lappend(nl, rpump);
            continue;
        }

        /* 
         * refresh 已经做完了
         *  1、网闸   清理掉服务器上的文件
         *  2、资源释放
         */
        ripple_increment_pumpsplittrail_deletedir_add(incpump->splittrailctx);
        ripple_refresh_pump_free(rpump);
    }

    list_free(incpump->refresh);
    incpump->refresh = nl;
    ripple_thread_unlock(&incpump->refreshlock);
    return true;
}

/*------------refresh 管理   end-------------------------*/

/*------------onlinerefresh 管理 begin-------------------*/
/* 启动 onlinerefresh 管理线程 */
bool ripple_increment_pump_startonlinerefresh(ripple_increment_pump* incpump)
{
    dlistnode* dlnode = NULL;
    ripple_onlinerefresh_pump* olrpump = NULL;

    if(true == dlist_isnull(incpump->onlinerefresh))
    {
        return true;
    }

    ripple_thread_lock(&incpump->onlinerefreshlock);

    /* 遍历 refresh 节点并启动 */
    for(dlnode = incpump->onlinerefresh->head; NULL != dlnode; dlnode = dlnode->next)
    {
        olrpump = (ripple_onlinerefresh_pump*)dlnode->value;
        if (RIPPLE_ONLINEREFRESH_PUMP_INIT != olrpump->stat)
        {
            continue;
        }

        olrpump->stat = RIPPLE_ONLINEREFRESH_PUMP_WAITSTART;

        /* 启动 onlinerefresh manager 线程 */
        if(false == ripple_threads_addsubmanger(incpump->threads,
                                                RIPPLE_THRNODE_IDENTITY_PUMP_ONLINEREFRESH_INC_MGR,
                                                incpump->persistno,
                                                &olrpump->thrsmgr,
                                                (void*)olrpump,
                                                NULL,
                                                NULL,
                                                ripple_onlinerefresh_pump_main))
        {
            elog(RLOG_WARNING, "pump start onlinerefresh manager failed");
            ripple_thread_unlock(&incpump->refreshlock);
            return false;
        }
    }

    ripple_thread_unlock(&incpump->onlinerefreshlock);
    return true;
}


/* 回收 onlinerefresh 节点 */
bool ripple_increment_pump_tryjoinononlinerefresh(ripple_increment_pump* incpump)
{
    dlistnode* dlnode                   = NULL;
    dlistnode* dlnodenext               = NULL;
    ripple_onlinerefresh_pump* olrpump  = NULL;

    if(true == dlist_isnull(incpump->onlinerefresh))
    {
        return true;
    }

    ripple_thread_lock(&incpump->onlinerefreshlock);
    for(dlnode = incpump->onlinerefresh->head; NULL != dlnode; dlnode = dlnodenext)
    {
        dlnodenext = dlnode->next;
        olrpump = (ripple_onlinerefresh_pump*)dlnode->value;
        if (RIPPLE_ONLINEREFRESH_PUMP_DONE == olrpump->stat)
        {
            ripple_onlinerefresh_persist_statesetbyuuid(incpump->olrpersist, 
                                                        &olrpump->no, 
                                                        RIPPLE_ONLINEREFRESH_PERSISTNODE_STAT_DONE);
        }
        else if (RIPPLE_ONLINEREFRESH_PUMP_ABANDONED == olrpump->stat)
        {
            ripple_onlinerefresh_persist_statesetbyuuid(incpump->olrpersist, 
                                                        &olrpump->no, 
                                                        RIPPLE_ONLINEREFRESH_PERSISTNODE_STAT_ABANDON);
        }
        else
        {
            continue;
        }

        ripple_onlinerefresh_persist_removerefreshtbsbyuuid(incpump->olrpersist, &olrpump->no);

        ripple_onlinerefresh_persist_electionrewindbyuuid(incpump->olrpersist, &olrpump->no);

        /* 落盘 */
        ripple_onlinerefresh_persist_write(incpump->olrpersist);

        incpump->onlinerefresh = dlist_delete(incpump->onlinerefresh, dlnode, ripple_onlinerefresh_pump_free);
    }

    ripple_thread_unlock(&incpump->onlinerefreshlock);
    return true;
}

/*------------onlinerefresh 管理   end-------------------*/

/*------------bigtxn 管理 begin-------------------*/
/* 启动 bigtxn 管理线程 */
bool ripple_increment_pump_startbigtxn(ripple_increment_pump* incpump)
{
    dlistnode* dlnode = NULL;
    ripple_bigtxn_pumpmanager* bigtxnpump = NULL;

    if(true == dlist_isnull(incpump->bigtxn))
    {
        return true;
    }

    ripple_thread_lock(&incpump->bigtxnlock);

    /* 遍历 refresh 节点并启动 */
    for(dlnode = incpump->bigtxn->head; NULL != dlnode; dlnode = dlnode->next)
    {
        bigtxnpump = (ripple_bigtxn_pumpmanager*)dlnode->value;
        if (RIPPLE_BIGTXN_PUMPMANAGER_STAT_INIT != bigtxnpump->stat)
        {
            continue;
        }

        bigtxnpump->stat = RIPPLE_BIGTXN_PUMPMANAGER_STAT_STARTING;

        /* 启动 bigtxn manager 线程 */
        if(false == ripple_threads_addsubmanger(incpump->threads,
                                                RIPPLE_THRNODE_IDENTITY_PUMP_BIGTXN_MGR,
                                                incpump->persistno,
                                                &bigtxnpump->thrsmgr,
                                                (void*)bigtxnpump,
                                                NULL,
                                                NULL,
                                                ripple_bigtxn_pumpmanager_main))
        {
            elog(RLOG_WARNING, "pump start bigtxn manager failed");
            ripple_thread_unlock(&incpump->bigtxnlock);
            return false;
        }
    }

    ripple_thread_unlock(&incpump->bigtxnlock);
    return true;
}

/* 回收 bigtxn 节点 */
bool ripple_increment_pump_tryjoinonbigtxn(ripple_increment_pump* incpump)
{
    char path[MAXPGPATH]                    = {'\0'};
    dlistnode* dlnode                       = NULL;
    dlistnode* dlnodenext                   = NULL;
    ripple_bigtxn_pumpmanager* bigtxnpump   = NULL;

    if(true == dlist_isnull(incpump->bigtxn))
    {
        return true;
    }

    ripple_thread_lock(&incpump->bigtxnlock);
    for(dlnode = incpump->bigtxn->head; NULL != dlnode; dlnode = dlnodenext)
    {
        dlnodenext = dlnode->next;
        bigtxnpump = (ripple_bigtxn_pumpmanager*)dlnode->value;

        if (RIPPLE_BIGTXN_PUMPMANAGER_STAT_DONE == bigtxnpump->stat)
        {
            ripple_bigtxn_persist_set_state_by_xid(incpump->txnpersist, bigtxnpump->xid,
                                                   RIPPLE_BIGTXN_PERSISTNODE_STAT_DONE);
        }
        else if (RIPPLE_BIGTXN_PUMPMANAGER_STAT_ABANDONED == bigtxnpump->stat)
        {
            ripple_bigtxn_persist_set_state_by_xid(incpump->txnpersist, bigtxnpump->xid,
                                                   RIPPLE_BIGTXN_PERSISTNODE_STAT_ABANDON);
        }
        else
        {
            continue;
        }

        /* 先换算 rewind 点 */
        ripple_bigtxn_persist_electionrewindbyxid(incpump->txnpersist, bigtxnpump->xid);

        /* 落盘 */
        ripple_bigtxn_write_persist(incpump->txnpersist);

        ripple_bigtxn_pumpmanager_gapdeletedir_add(bigtxnpump);

        /* 删除xid对应大事务文件夹 */
        rmemset1(path, 0, '\0', MAXPGPATH);
        snprintf(path, MAXPGPATH, "%s/%s/%lu", guc_getConfigOption(RIPPLE_CFG_KEY_TRAIL_DIR), RIPPLE_STORAGE_BIG_TRANSACTION_DIR, bigtxnpump->xid);
        RemoveDir(path);

        incpump->bigtxn = dlist_delete(incpump->bigtxn, dlnode, ripple_bigtxn_pumpmanager_destory);
    }

    ripple_thread_unlock(&incpump->bigtxnlock);
    return true;
}

/*------------bigtxn 管理   end-------------------*/


void ripple_increment_pumpstate_destroy(ripple_increment_pump* pumpstate)
{
    ListCell* lc = NULL;
    if (NULL == pumpstate)
    {
        return;
    }

    ripple_increment_pumpsplittrail_free(pumpstate->splittrailctx);

    ripple_increment_pumpparsertrail_free(pumpstate->pumpparsertrail);

    ripple_increment_pumpserial_destroy(pumpstate->serialstate);

    ripple_filetransfer_pump_free(pumpstate->ftptransfer);

    ripple_increment_pumpnet_destroy(pumpstate->clientstate);

    ripple_file_buffer_destroy(pumpstate->txn2filebuffer);

    ripple_cache_txn_destroy(pumpstate->parser2serialtxns);

    ripple_queue_destroy(pumpstate->recordscache, dlist_freevoid);

    ripple_queue_destroy(pumpstate->filetransfernode, ripple_filetransfer_queuefree);

    ripple_thread_mutex_destroy(&pumpstate->onlinerefreshlock);

    ripple_state_pump_destroy(pumpstate->pumpstate);

    dlist_free(pumpstate->onlinerefresh, ripple_onlinerefresh_pump_free);

    dlist_free(pumpstate->bigtxn, ripple_bigtxn_pumpmanager_destory);

    ripple_bigtxn_persist_free(pumpstate->txnpersist);

    ripple_onlinerefresh_persist_free(pumpstate->olrpersist);

    ripple_thread_mutex_destroy(&pumpstate->refreshlock);

    foreach(lc, pumpstate->refresh)
    {
        ripple_refresh_pump_free((ripple_refresh_pump*)lfirst(lc));
    }
    list_free(pumpstate->refresh);

    ripple_threads_free(pumpstate->threads);
    rfree(pumpstate);
    pumpstate = NULL;
}
