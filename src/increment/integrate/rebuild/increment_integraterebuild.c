#include "app_incl.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/uuid.h"
#include "utils/string/stringinfo.h"
#include "queue/queue.h"
#include "task/task_slot.h"
#include "threads/threads.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "stmts/txnstmt.h"
#include "stmts/txnstmt_prepared.h"
#include "rebuild/rebuild.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "refresh/refresh_table_syncstats.h"
#include "stmts/txnstmt_onlinerefresh.h"
#include "stmts/txnstmt_updaterewind.h"
#include "stmts/txnstmt_updatesynctable.h"
#include "onlinerefresh/integrate/dataset/onlinerefresh_integratedataset.h"
#include "onlinerefresh/integrate/dataset/onlinerefresh_integratefilterdataset.h"
#include "onlinerefresh/onlinerefresh_persist.h"
#include "bigtransaction/persist/bigtxn_persist.h"
#include "bigtransaction/integrate/bigtxn_integratemanager.h"
#include "onlinerefresh/integrate/onlinerefresh_integrate.h"
#include "increment/integrate/rebuild/increment_integraterebuild.h"

/* 初始化 */
increment_integraterebuild* increment_integraterebuild_init(void)
{
    char* burst = NULL;
    increment_integraterebuild* rebuild_obj = rmalloc0(sizeof(increment_integraterebuild));
    if(NULL == rebuild_obj)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(rebuild_obj, 0, '\0', sizeof(increment_integraterebuild));
    rebuild_reset((rebuild*)rebuild_obj);
    rebuild_obj->parser2rebuild = NULL;
    rebuild_obj->rebuild2sync = NULL;
    rebuild_obj->filterlsn = InvalidXLogRecPtr;
    rebuild_obj->stat = INCREMENT_INTEGRATEREBUILD_STAT_NOP;
    rebuild_obj->mergetxn = (0 == guc_getConfigOptionInt(CFG_KEY_MERGETXN)) ? false : true;
    rebuild_obj->txbundlesize = guc_getConfigOptionInt(CFG_KEY_TXBUNDLESIZE);

    /* 设置integrate_method */
    burst = guc_getConfigOption(CFG_KEY_INTEGRATE_METHOD);
    if (burst != NULL && '\0' != burst[0] && 0 == strcmp(burst, "burst"))
    {
        rebuild_obj->burst = true;
    }

    rebuild_obj->txnpersist = bigtxn_persist_init();

    rebuild_obj->honlinerefreshfilterdataset = onlinerefresh_integratefilterdataset_init();

    rebuild_obj->onlinerefreshdataset = onlinerefresh_integratedataset_init();
    return rebuild_obj;
}

static bool increment_integraterebuild_canwork(increment_integraterebuild* rebuild)
{
    if(rebuild->stat == INCREMENT_INTEGRATEREBUILD_STAT_WORK)
    {
        return true;
    }
    else if(rebuild->stat == INCREMENT_INTEGRATEREBUILD_STAT_READY)
    {
        rebuild->stat = INCREMENT_INTEGRATEREBUILD_STAT_WORK;
        return true;
    }
    else if(rebuild->stat == INCREMENT_INTEGRATEREBUILD_STAT_WAITTERM)
    {
        cache_txn_clean(rebuild->parser2rebuild);
        return false;
    }
    return false;
}

static txn* increment_integraterebuild_updatesynctabletxn_set(increment_integraterebuild* rebuild_obj, txn* txn_obj)
{
    txn* cur_txn = NULL;
    txnstmt* stmtnode = NULL;
    txnstmt_updatesynctable* updatesynctable = NULL;

    cur_txn = txn_init(txn_obj->xid, txn_obj->start.wal.lsn, InvalidXLogRecPtr);
    if(NULL == cur_txn)
    {
        elog(RLOG_WARNING, "cur_txn out of memory, %s", strerror(errno));
        return NULL;
    }

    /* 申请空间 */
    stmtnode = txnstmt_init();
    if(NULL == stmtnode)
    {
        rfree(cur_txn);
        elog(RLOG_WARNING, "stmtnode out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(stmtnode, 0, '\0', sizeof(txnstmt));

    updatesynctable =  txnstmt_updatesynctable_init();
    if (NULL == updatesynctable)
    {
        rfree(cur_txn);
        rfree(stmtnode);
        return NULL;
    }

    stmtnode->type = TXNSTMT_TYPE_UPDATESYNCTABLE;
    stmtnode->stmt = (void*)updatesynctable;

    cur_txn->stmts = lappend(cur_txn->stmts, stmtnode);
    cur_txn->confirm.wal.lsn = txn_obj->confirm.wal.lsn;
    cur_txn->end.trail.offset = txn_obj->end.trail.offset;
    cur_txn->endtimestamp = txn_obj->endtimestamp;
    cur_txn->segno = txn_obj->segno;
    cur_txn->xid = txn_obj->xid;
    return cur_txn;
}

static bool increment_integraterebuild_updaterewindstmt_set(increment_integraterebuild* rebuild_obj, txn* txn_obj)
{
    recpos pos = {{'\0'}};
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

    pos.trail.fileid = txn_obj->segno;
    pos.trail.offset = txn_obj->end.trail.offset;
    bigtxn_persist_electionrewind(rebuild_obj->txnpersist, &pos);

    updaterewind->rewind = rebuild_obj->txnpersist->rewind;

    stmtnode->type = TXNSTMT_TYPE_UPDATEREWIND;
    stmtnode->stmt = (void*)updaterewind;

    txn_obj->stmts = lappend(txn_obj->stmts, stmtnode);

    return true;
}

/* 清理放弃掉的onlinerefresh对应过滤集 */
static void increment_integraterebuild_delonlinerefresdataset(increment_integraterebuild* rebuild_obj, thrnode* thr_node, void* abandon)
{
    List* luuid                                         = NULL;
    ListCell* lc                                        = NULL;
    uuid_t* uuid                                 = NULL;
    onlinerefresh_integratedatasetnode *endnode  = NULL;

    if (NULL == abandon)
    {
        return;
    }
    
    luuid = (List*)abandon;

    foreach(lc, luuid)
    {

        uuid = (uuid_t*)lfirst(lc);

        endnode = onlinerefresh_integratedataset_number_get(rebuild_obj->onlinerefreshdataset, uuid->data);
        while (true)
        {
            if(THRNODE_STAT_TERM == thr_node->stat)
            {
                return;
            }

            /* 等待onlinerefresh结束 */
            if (rebuild_obj->callback.isonlinerefreshdone(rebuild_obj->privdata, uuid->data))
            {
                onlinerefresh_persist_statesetbyuuid(rebuild_obj->olpersist, 
                                                            &endnode->onlinerefreshno, 
                                                            ONLINEREFRESH_PERSISTNODE_STAT_DONE);
                /* onlinerefresh结束清理persist的存量表 */
                onlinerefresh_persist_removerefreshtbsbyuuid(rebuild_obj->olpersist, &endnode->onlinerefreshno);
                onlinerefresh_persist_electionrewindbyuuid(rebuild_obj->olpersist, &endnode->onlinerefreshno);
                onlinerefresh_integratefilterdataset_delete(rebuild_obj->honlinerefreshfilterdataset, endnode->refreshtables, endnode->txid);
                onlinerefresh_integratedataset_delete(rebuild_obj->onlinerefreshdataset, uuid->data);
                break;
            }
            usleep(50000);
        }
    }
    return;
}
/* 新增大事务 */
static void  increment_integraterebuild_addbigtxnpersist(increment_integraterebuild* rebuild_obj,
                                                                txn* txn_obj)
{
    bool exist                                  = false;
    recpos pos                           = {{'\0'}};
    dlistnode *dnode                            = NULL;
    dlistnode* dlnodenext                       = NULL;
    bigtxn_persist *persist              = NULL;
    bigtxn_persistnode *persistnode      = NULL;

    persist = rebuild_obj->txnpersist;

    /* 确保存在 */
    dnode = persist->dpersistnodes ? persist->dpersistnodes->head : NULL;

    pos.trail.fileid = txn_obj->segno;
    pos.trail.offset = txn_obj->end.trail.offset;

    /* 遍历persist, 检查是否有相同事务 */
    for (; dnode; dnode = dlnodenext)
    {
        dlnodenext = dnode->next;
        persistnode = (bigtxn_persistnode *)dnode->value;

        /* 是否存在事务号相同的大事务 */
        if (txn_obj->xid == persistnode->xid)
        {
            /* 存在 */
            exist = true;
            bigtxn_persistnode_set_begin(persistnode, &pos);
            bigtxn_persistnode_set_xid(persistnode, txn_obj->xid);
        }
    }

    /* 遍历结束, 判断是否存在, 只有不存在时才需要新增 */
    if (!exist)
    {
        bigtxn_persistnode *pernode = NULL;

        /* 构建persist node并初始化 */
        pernode = bigtxn_persist_node_init();
        bigtxn_persistnode_set_begin(pernode, &pos);
        bigtxn_persistnode_set_xid(pernode, txn_obj->xid);
        pernode->stat = BIGTXN_PERSISTNODE_STAT_INPROCESS;
        persist->dpersistnodes = dlist_put(persist->dpersistnodes, pernode);
        persist->count += 1;
    }
}

/* 新增onlinerefresh persist */
static void  increment_integraterebuild_addonlinerefreshpersist(increment_integraterebuild* rebuild_obj,
                                                                       onlinerefresh_integrate *olrintegrate)
{
    bool exist                                  = false;
    dlistnode *dnode                            = NULL;
    dlistnode* dlnodenext                       = NULL;
    onlinerefresh_persist *persist              = NULL;
    onlinerefresh_persistnode *persistnode      = NULL;

    persist = rebuild_obj->olpersist;

    /* 确保存在 */
    dnode = persist->dpersistnodes ? persist->dpersistnodes->head : NULL;

    /* 遍历persist, 检查是否有相同事务 */
    for (; dnode; dnode = dlnodenext)
    {
        dlnodenext = dnode->next;
        persistnode = (onlinerefresh_persistnode *)dnode->value;

        /* 是否存在事务号相同的大事务 */
        if (0 == memcmp(olrintegrate->no.data, persistnode->uuid.data, UUID_LEN))
        {
            /* 存在 */
            exist = true;
            onlinerefresh_persistnode_beginset(persistnode, olrintegrate->begin);
        }
    }

    /* 遍历结束, 判断是否存在, 只有不存在时才需要新增 */
    if (!exist)
    {
        onlinerefresh_persistnode *pernode = NULL;

        /* 构建persist node并初始化 */
        pernode = onlinerefresh_persistnode_init();
        onlinerefresh_persistnode_statset(pernode, ONLINEREFRESH_PERSISTNODE_STAT_INIT);
        onlinerefresh_persistnode_uuidset(pernode, &olrintegrate->no);
        onlinerefresh_persistnode_incrementset(pernode, olrintegrate->increment);
        onlinerefresh_persistnode_beginset(pernode, olrintegrate->begin);
        onlinerefresh_persistnode_txidset(pernode, olrintegrate->txid);

        pernode->refreshtbs = olrintegrate->tablesyncstats;
        if(true == dlist_isnull(persist->dpersistnodes))
        {
            persist->rewind.trail.fileid = olrintegrate->begin.trail.fileid;
            persist->rewind.trail.offset = olrintegrate->begin.trail.offset;
        }
        persist->dpersistnodes = dlist_put(persist->dpersistnodes, pernode);
    }
    onlinerefresh_persist_write(persist);
}

static bool increment_integraterebuild_isspecialtxn(txn* txn_obj)
{
    if(NULL == txn_obj->stmts)
    {
        return true;
    }

    /* refresh */
    if (TXN_TYPE_METADATA < txn_obj->type)
    {
        return true;
    }
    else
    {
        return false;
    }
}

/*
 * 根据rebuild->burst选择重组事务方式
 * 
 * 返回值：true成功，false失败
*/
static bool increment_integraterebuild_rebuildtxn(increment_integraterebuild* rebuild_obj,
                                                         txn *txn_obj)
{
    if (true == rebuild_obj->burst)
    {
        /* 重组 */
        if(false == rebuild_txnburst((rebuild*)rebuild_obj, txn_obj))
        {
            elog(RLOG_WARNING, "increment_integraterebuild_txnburst error");
            return false;
        }
    }
    else
    {
        /* 重组 */
        if(false == rebuild_prepared((rebuild*)rebuild_obj, txn_obj))
        {
            elog(RLOG_WARNING, "increment_integraterebuild_prepared error");
            return false;
        }
    }

    return true;
}

/*
 * integate rebuild应用系统表，应用时先清理index和attribute
*/
static void increment_integraterebuild_transcatalog2transcache(increment_integraterebuild* rebuild_obj,
                                                                      txn* txn_obj)
{
    bool equal                              = false;
    ListCell* stmtlc                        = NULL;
    ListCell* catraloglc                    = NULL;
    txnstmt* stmtnode                = NULL;
    txnstmt_metadata* metadatastmt   = NULL;

    if(NULL == txn_obj->stmts)
    {
        return;
    }

    foreach(stmtlc, txn_obj->stmts)
    {
        stmtnode = (txnstmt*)lfirst(stmtlc);

        if (TXNSTMT_TYPE_METADATA == stmtnode->type)
        {
            metadatastmt = (txnstmt_metadata*)stmtnode->stmt;
            catraloglc = metadatastmt->begin;
            /* 根基class->oid清理之前的index和attribute */
            cache_sysdicts_clearsysdicthisbyclass(rebuild_obj->rebuild.sysdicts, catraloglc);

            while(1)
            {
                /* 应用到系统表中 */
                cache_sysdicts_txnsysdicthisitem2cache(rebuild_obj->rebuild.sysdicts, (void*)catraloglc);

                /* 只有一个 */
                if(catraloglc == metadatastmt->end
                   || true == equal)
                {
                    break;
                }

                /* 校验是否到达最后一个 */
                catraloglc = catraloglc->next;
                if(catraloglc == metadatastmt->end)
                {
                    equal = true;
                }
            }
        }
    }
    return;
}

/* 只有在大事务异常退出和term信号返回false */
static bool increment_integraterebuild_specialtxn(increment_integraterebuild* rebuild_obj,
                                                         thrnode* thr_node,
                                                         txn* txn_obj,
                                                         txn** ntxn,
                                                         int* txbundlesize)
{
    recpos pos = {{'\0'}};
    txn* cur_txn = NULL;
    txnstmt* stmtnode = NULL;
    txnstmt_onlinerefresh* onlinerefresh = NULL;
    onlinerefresh_integratedatasetnode *datasetnode = NULL;
    onlinerefresh_integratedatasetnode *endnode = NULL;

    if(NULL == txn_obj->stmts)
    {
        return true;
    }

    stmtnode = (txnstmt*)lfirst(list_head(txn_obj->stmts));

    /* refresh */
    if (TXNSTMT_TYPE_REFRESH == stmtnode->type)
    {
        cur_txn = NULL;
        while (true)
        {
            if(THRNODE_STAT_TERM == thr_node->stat)
            {
                return false;
            }

            if (true == rebuild_obj->callback.isrefreshdown(rebuild_obj->privdata))
            {
                break;
            }
            usleep(50000);
        }
        /* 更新状态表信息不包含rewind信息 */
        cur_txn = increment_integraterebuild_updatesynctabletxn_set(rebuild_obj, txn_obj);
        if (cur_txn)
        {
            cur_txn->segno = 1;
            cur_txn->end.trail.offset = 0;
            /* 事务中不存储rewind信息，以stmt将计算后的rewind传递到应用线程更新rewind信息 */
            increment_integraterebuild_updaterewindstmt_set(rebuild_obj, cur_txn);
            cache_txn_add(rebuild_obj->rebuild2sync, cur_txn);
            cur_txn = NULL;
        }
        
    }
    else if (TXNSTMT_TYPE_ONLINEREFRESH_BEGIN == stmtnode->type)
    {
        if (NULL != *ntxn)
        {
            if(false == increment_integraterebuild_rebuildtxn(rebuild_obj, *ntxn))
            {
                elog(RLOG_WARNING, "increment integraterebuild specialtxn rebuildtxn error");
                return false;
            }
            /* 事务中不存储rewind信息，以stmt将计算后的rewind传递到应用线程更新rewind信息 */
            increment_integraterebuild_updaterewindstmt_set(rebuild_obj, *ntxn);
            cache_txn_add(rebuild_obj->rebuild2sync, *ntxn);
            *ntxn = NULL;
            *txbundlesize = 0;
        }
        //转换类型
        onlinerefresh = (txnstmt_onlinerefresh*)stmtnode->stmt;

        /* 等待缓存内事务应用完成 */
        while(true)
        {
            if(THRNODE_STAT_TERM == thr_node->stat)
            {
                return false;
            }

            if (txn_obj->segno == rebuild_obj->olpersist->rewind.trail.fileid
                && txn_obj->end.trail.offset == rebuild_obj->olpersist->rewind.trail.offset)
            {
                break;
            }

            /* 缓存应用完成且sync线程空闲 */
            if (cache_txn_isnull(rebuild_obj->rebuild2sync)
                && true == rebuild_obj->callback.issyncidle(rebuild_obj->privdata))
            {
                refresh_tables* tables = NULL;
                refresh_table_syncstats* tablesyncstats = NULL;
                onlinerefresh_integrate *integrateolrefresh = NULL;

                /* 注册onlinerefresh节点 */
                tables = refresh_tables_copy(onlinerefresh->refreshtables);

                tablesyncstats = refresh_table_syncstats_init();
                refresh_table_syncstats_tablesyncing_set(tables, tablesyncstats);
                refresh_table_syncstats_tablesyncall_set(tables, tablesyncstats);

                integrateolrefresh = onlinerefresh_integrate_init(onlinerefresh->increment);

                integrateolrefresh->stat = ONLINEREFRESH_INTEGRATE_INIT;
                rmemcpy1(integrateolrefresh->no.data, 0, onlinerefresh->no->data, UUID_LEN);
                integrateolrefresh->increment = onlinerefresh->increment;
                integrateolrefresh->tablesyncstats = tablesyncstats;
                integrateolrefresh->txid = onlinerefresh->txid;
                integrateolrefresh->begin.trail.fileid = txn_obj->segno;
                integrateolrefresh->begin.trail.offset = txn_obj->end.trail.offset;

                increment_integraterebuild_addonlinerefreshpersist(rebuild_obj, integrateolrefresh);
                rebuild_obj->callback.addonlinerefresh(rebuild_obj->privdata, integrateolrefresh);

                refresh_freetables(tables);
                break;
            }
            usleep(50000);
        }

        while (true)
        {
            if(THRNODE_STAT_TERM == thr_node->stat)
            {
                return false;
            }
            /* 等待onlinerefresh存量结束 */
            if (true == rebuild_obj->callback.isolrrefreshdone(rebuild_obj->privdata, onlinerefresh->no->data))
            {
                break;
            }
            usleep(50000);
        }

        /* 创建onlinerefresh过滤数据集 */
        onlinerefresh_integratefilterdataset_add(rebuild_obj->honlinerefreshfilterdataset, onlinerefresh->refreshtables, onlinerefresh->txid);
        datasetnode = onlinerefresh_integratedatasetnode_init();
        onlinerefresh_integratedatasetnode_no_set(datasetnode, onlinerefresh->no->data);
        onlinerefresh_integratedatasetnode_txid_set(datasetnode, onlinerefresh->txid);
        onlinerefresh_integratedatasetnode_refreshtables_set(datasetnode, onlinerefresh->refreshtables);
        onlinerefresh_integratedataset_add(rebuild_obj->onlinerefreshdataset, datasetnode);
    }
    else if (TXNSTMT_TYPE_ONLINEREFRESH_END == stmtnode->type)
    {
        if (NULL != *ntxn)
        {
            if(false == increment_integraterebuild_rebuildtxn(rebuild_obj, *ntxn))
            {
                elog(RLOG_WARNING, "increment integraterebuild specialtxn rebuildtxn error");
                return false;
            }
            cache_txn_add(rebuild_obj->rebuild2sync, *ntxn);
            *ntxn = NULL;
            *txbundlesize = 0;
        }
        elog(RLOG_DEBUG, "get TXNSTMT_TYPE_ONLINEREFRESH_END");
        /* 获取onlinerefresh编号 */
        endnode = onlinerefresh_integratedataset_number_get(rebuild_obj->onlinerefreshdataset, stmtnode->stmt);

        while (true)
        {
            if(THRNODE_STAT_TERM == thr_node->stat)
            {
                return false;
            }
            /* 等待onlinerefresh结束 */
            if (rebuild_obj->callback.isonlinerefreshdone(rebuild_obj->privdata, endnode->onlinerefreshno.data))
            {
                onlinerefresh_persist_statesetbyuuid(rebuild_obj->olpersist, 
                                                            &endnode->onlinerefreshno, 
                                                            ONLINEREFRESH_PERSISTNODE_STAT_DONE);
                /* onlinerefresh结束清理persist的存量表 */
                onlinerefresh_persist_removerefreshtbsbyuuid(rebuild_obj->olpersist, &endnode->onlinerefreshno);
                onlinerefresh_persist_electionrewindbyuuid(rebuild_obj->olpersist, &endnode->onlinerefreshno);
                onlinerefresh_integratefilterdataset_delete(rebuild_obj->honlinerefreshfilterdataset, endnode->refreshtables, endnode->txid);
                onlinerefresh_integratedataset_delete(rebuild_obj->onlinerefreshdataset, stmtnode->stmt);
                rebuild_obj->callback.setonlinerefreshfree(rebuild_obj->privdata, stmtnode->stmt);
                break;
            }
            usleep(50000);
        }

        cur_txn = increment_integraterebuild_updatesynctabletxn_set(rebuild_obj, txn_obj);
        if (cur_txn)
        {
            /* 事务中不存储rewind信息，以stmt将计算后的rewind传递到应用线程更新rewind信息 */
            increment_integraterebuild_updaterewindstmt_set(rebuild_obj, cur_txn);
            cache_txn_add(rebuild_obj->rebuild2sync, cur_txn);
            cur_txn = NULL;
        }
    }
    else if (TXNSTMT_TYPE_RESET == stmtnode->type)
    {
        if (NULL != *ntxn)
        {
            if(false == increment_integraterebuild_rebuildtxn(rebuild_obj, *ntxn))
            {
                elog(RLOG_WARNING, "increment integraterebuild specialtxn rebuildtxn error");
                return false;
            }
            /* 事务中不存储rewind信息，以stmt将计算后的rewind传递到应用线程更新rewind信息 */
            increment_integraterebuild_updaterewindstmt_set(rebuild_obj, *ntxn);
            cache_txn_add(rebuild_obj->rebuild2sync, *ntxn);
            *ntxn = NULL;
            *txbundlesize = 0;
        }
        bigtxn_integratepersist_cleannotdone(rebuild_obj->txnpersist);

    }
    else if (TXNSTMT_TYPE_ABANDON == stmtnode->type)
    {
        bigtxn_persist_removebyxid(rebuild_obj->txnpersist, txn_obj->xid);
        pos.trail.fileid = txn_obj->segno;
        pos.trail.offset = txn_obj->end.trail.offset;
        bigtxn_persist_electionrewind(rebuild_obj->txnpersist, &pos);
    }
    else if (TXNSTMT_TYPE_BIGTXN_BEGIN == stmtnode->type)
    {
        //todo,删除persist，大事务接收到end启动，不完成不会更新rewind点，只要不结束必然会启动大事务
        increment_integraterebuild_addbigtxnpersist(rebuild_obj, txn_obj);
    }
    else if (TXNSTMT_TYPE_BIGTXN_END == stmtnode->type)
    {
        bigtxn_end_stmt *end_stmt = NULL;
        bigtxn_integratemanager *bigtxn = NULL;
        if (NULL != *ntxn)
        {
            if(false == increment_integraterebuild_rebuildtxn(rebuild_obj, *ntxn))
            {
                elog(RLOG_WARNING, "increment integraterebuild specialtxn rebuildtxn error");
                return false;
            }
            /* 事务中不存储rewind信息，以stmt将计算后的rewind传递到应用线程更新rewind信息 */
            increment_integraterebuild_updaterewindstmt_set(rebuild_obj, *ntxn);
            cache_txn_add(rebuild_obj->rebuild2sync, *ntxn);
            *ntxn = NULL;
            *txbundlesize = 0;
        }
        end_stmt = (bigtxn_end_stmt *)stmtnode->stmt;
        /* 等待缓存内事务应用完成 */
        while(true)
        {
            if(THRNODE_STAT_TERM == thr_node->stat)
            {
                return false;
            }

            /* 缓存应用完成且sync线程空闲 */
            if (cache_txn_isnull(rebuild_obj->rebuild2sync)
                && true == rebuild_obj->callback.issyncidle(rebuild_obj->privdata))
            {
                if (true == end_stmt->commit)
                {
                    bigtxn = bigtxn_integratemanager_init();
                    bigtxn->xid = end_stmt->xid;
                    /* 复制onlinerefresh过滤集用于大事务内部过滤 */
                    bigtxn->honlinerefreshfilterdataset = onlinerefresh_integratefilterdataset_copy(rebuild_obj->honlinerefreshfilterdataset);
                    if (false == dlist_isnull(rebuild_obj->onlinerefreshdataset->onlinerefresh))
                    {
                        bigtxn->onlinerefreshdataset = onlinerefresh_integratedataset_copy(rebuild_obj->onlinerefreshdataset);
                    }
                    bigtxn_integratemanager_stat_set(bigtxn, BIGTXN_INTEGRATEMANAGER_STAT_INIT);
                    rebuild_obj->callback.addbigtxn(rebuild_obj->privdata, (void*)bigtxn);
                }
                break;
            }
            usleep(50000);
        }

        if (true == end_stmt->commit)
        {
            while (true)
            {
                if(THRNODE_STAT_TERM == thr_node->stat)
                {
                    return false;
                }

                if (true == rebuild_obj->callback.isbigtxnsigterm(rebuild_obj->privdata, end_stmt->xid))
                {
                    return false;
                }

                if (true == rebuild_obj->callback.isbigtxndown(rebuild_obj->privdata, end_stmt->xid))
                {
                    break;
                }
                usleep(50000);
            }
        }

        bigtxn_persist_removebyxid(rebuild_obj->txnpersist, end_stmt->xid);

        cur_txn = increment_integraterebuild_updatesynctabletxn_set(rebuild_obj, txn_obj);
        if (cur_txn)
        {
            cur_txn->xid = end_stmt->xid;
            /* 事务中不存储rewind信息，以stmt将计算后的rewind传递到应用线程更新rewind信息 */
            increment_integraterebuild_updaterewindstmt_set(rebuild_obj, cur_txn);
            cache_txn_add(rebuild_obj->rebuild2sync, cur_txn);
            cur_txn = NULL;
        }
    }
    else if (TXNSTMT_TYPE_ONLINEREFRESHABANDON == stmtnode->type)
    {
        increment_integraterebuild_delonlinerefresdataset(rebuild_obj, thr_node, stmtnode->stmt);
    }

    return true;
}


/* 工作 */
void* increment_integraterebuild_main(void* args)
{
    bool find                                                           = false;
    int timeout                                                         = 0;
    int txbundlesize                                                    = 0;
    Oid relid                                                           = InvalidOid;
    ListCell* filterlc                                                  = NULL;
    txn* txns                                                    = NULL;
    txn* ntxn                                                    = NULL;
    txn* txnnode                                                 = NULL;
    thrnode* thr_node                                             = NULL;
    txnstmt* stmtnode                                            = NULL;
    increment_integraterebuild* rebuild_obj                          = NULL;
    xk_pg_parser_translog_tbcol_values* values                          = NULL;
    xk_pg_parser_translog_tbcolbase* tbcolbase                          = NULL;
    xk_pg_parser_translog_tbcol_nvalues* nvalues                        = NULL;
    onlinerefresh_integratedatasetnode *node                     = NULL;
    onlinerefresh_integratefilterdataset* filterdatasetentry     = NULL;

    thr_node = (thrnode*)args;
    /* 入参转换 */
    rebuild_obj = (increment_integraterebuild* )thr_node->data;

    /* 查看状态 */
    if(THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING, "increment integrate rebuild exception, expected state is THRNODE_STAT_STARTING");
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
            cache_txn_clean(rebuild_obj->parser2rebuild);
            break;
        }

        if(false == increment_integraterebuild_canwork(rebuild_obj))
        {
            usleep(50000);
            continue;
        }

        /* 在缓存中获取数据 */
        txns = cache_txn_getbatch(rebuild_obj->parser2rebuild, &timeout);
        if(NULL == txns)
        {
            /* 超时,再次获取 */
            if(ERROR_TIMEOUT == timeout)
            {
                usleep(10000);
                continue;
            }

            elog(RLOG_WARNING, "get file buffer error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        /* 遍历 txns 并重组语句 */
        for(txnnode = txns; NULL != txnnode; txnnode = txns)
        {
            /* special 用于标记指定事务: refresh/onlinerefreshbegin/onlinerefreshend */
            txns = txnnode->cachenext;

            /* 过滤事务 */
            if(txnnode->confirm.wal.lsn <= rebuild_obj->filterlsn)
            {
                /* 应用系统表数据 */
                increment_integraterebuild_transcatalog2transcache(rebuild_obj, txnnode);
                txn_free(txnnode);
                rfree(txnnode);
                continue;
            }
            if (MAX_LSN > txnnode->confirm.wal.lsn)
            {
                rebuild_obj->filterlsn = txnnode->confirm.wal.lsn;
            }

            /* 用于处理指定事务: refresh/onlinerefreshbegin/onlinerefreshend */
            if (true == increment_integraterebuild_isspecialtxn(txnnode))
            {

                if(false == increment_integraterebuild_specialtxn(rebuild_obj,
                                                                        thr_node,
                                                                        txnnode,
                                                                        &ntxn,
                                                                        &txbundlesize))
                {
                    /* term退出或其他独立线程退出，返回上层while循环等待term */
                    rebuild_obj->stat = INCREMENT_INTEGRATEREBUILD_STAT_WAITTERM;
                    elog(RLOG_WARNING, "Received term or other independent thread exit");
                    break;
                }

                if (txnnode)
                {
                    txn_free(txnnode);
                    rfree(txnnode);
                }
                continue;
            }

            if (false == dlist_isnull(rebuild_obj->onlinerefreshdataset->onlinerefresh))
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

                    node = onlinerefresh_integratedataset_txid_get(rebuild_obj->onlinerefreshdataset, filterdatasetentry->txid);
                    while(false == rebuild_obj->callback.isonlinerefreshdone(rebuild_obj->privdata, node->onlinerefreshno.data))
                    {
                        if(THRNODE_STAT_TERM == thr_node->stat)
                        {
                            thr_node->stat = THRNODE_STAT_EXIT;
                            pthread_exit(NULL);
                            return NULL;
                        }
                        usleep(50000);
                        continue;
                    }

                    tmpstmt = lappend(tmpstmt, stmtnode);
                    continue;
                }
                list_free(txnnode->stmts);
                txnnode->stmts = tmpstmt;
                tmpstmt = NULL;

                /* 无需处理 todo更新状态表 */
                if(NULL == txnnode->stmts)
                {
                    txn* cur_txn = NULL;
                    cur_txn = increment_integraterebuild_updatesynctabletxn_set(rebuild_obj, txnnode);
                    if (cur_txn)
                    {
                        /* 事务中不存储rewind信息，以stmt将计算后的rewind传递到应用线程更新rewind信息 */
                        increment_integraterebuild_updaterewindstmt_set(rebuild_obj, cur_txn);
                        cache_txn_add(rebuild_obj->rebuild2sync, cur_txn);
                        cur_txn = NULL;
                    }
                    else
                    {
                        elog(RLOG_WARNING, "increment_integraterebuild_updatesynctabletxn_set error");
                        thr_node->stat = THRNODE_STAT_ABORT;
                        break;
                    }
                    txn_free(txnnode);
                    rfree(txnnode);
                    continue;
                }
            }

            if(NULL == txnnode->stmts)
            {
                txn_free(txnnode);
                rfree(txnnode);
                continue;
            }

            /* 不执行事务合并 */
            if (false == rebuild_obj->mergetxn)
            {
                /* 将事务放入到缓存中 */
                if (txnnode->end.wal.lsn != InvalidXLogRecPtr)
                {
                    rebuild_obj->filterlsn = txnnode->end.wal.lsn;
                }

                if(false == increment_integraterebuild_rebuildtxn(rebuild_obj, txnnode))
                {
                    thr_node->stat = THRNODE_STAT_ABORT;
                    break;
                }

                /* 事务中不存储rewind信息，以stmt将计算后的rewind传递到应用线程更新rewind信息 */
                increment_integraterebuild_updaterewindstmt_set(rebuild_obj, txnnode);
                cache_txn_add(rebuild_obj->rebuild2sync, txnnode);
                continue;
            }

            if (txnnode->stmts->length > rebuild_obj->txbundlesize)
            {
                if (NULL != ntxn)
                {
                    if(false == increment_integraterebuild_rebuildtxn(rebuild_obj, ntxn))
                    {
                        thr_node->stat = THRNODE_STAT_ABORT;
                        break;
                    }
                    /* 事务中不存储rewind信息，以stmt将计算后的rewind传递到应用线程更新rewind信息 */
                    increment_integraterebuild_updaterewindstmt_set(rebuild_obj, ntxn);
                    cache_txn_add(rebuild_obj->rebuild2sync, ntxn);
                    ntxn = NULL;
                    txbundlesize = 0;
                }

                if(false == increment_integraterebuild_rebuildtxn(rebuild_obj, txnnode))
                {
                    thr_node->stat = THRNODE_STAT_ABORT;
                    break;
                }

                /* 事务中不存储rewind信息，以stmt将计算后的rewind传递到应用线程更新rewind信息 */
                increment_integraterebuild_updaterewindstmt_set(rebuild_obj, txnnode);
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
                    elog(RLOG_WARNING, "integrate rebuild txn init out of memory, %s", strerror(errno));
                    thr_node->stat = THRNODE_STAT_ABORT;
                    break;
                }
                rmemset0(ntxn, 0, '\0', sizeof(txn));
            }

            /* 复制事务信息 */
            ntxn->xid = txnnode->xid;
            ntxn->segno = txnnode->segno;
            ntxn->debugno = txnnode->debugno;
            ntxn->start = txnnode->start;
            ntxn->end = txnnode->end;
            ntxn->redo = txnnode->redo;
            ntxn->restart = txnnode->restart;
            ntxn->confirm = txnnode->confirm;
            ntxn->endtimestamp = txnnode->endtimestamp;
            txbundlesize += txnnode->stmts->length;

            /* 将 sysdictHis 加入到新事务中 */
            if (NULL != ntxn->sysdictHis || NULL != txnnode->sysdictHis)
            {
                ntxn->sysdictHis = list_concat(ntxn->sysdictHis, txnnode->sysdictHis);
                if (ntxn->sysdictHis != txnnode->sysdictHis
                    && NULL != txnnode->sysdictHis)
                {
                    rfree(txnnode->sysdictHis);
                }
                txnnode->sysdictHis = NULL;
            }

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

            if(false == increment_integraterebuild_rebuildtxn(rebuild_obj, ntxn))
            {
                thr_node->stat = THRNODE_STAT_ABORT;
                break;
            }

            /* 事务中不存储rewind信息，以stmt将计算后的rewind传递到应用线程更新rewind信息 */
            increment_integraterebuild_updaterewindstmt_set(rebuild_obj, ntxn);
            cache_txn_add(rebuild_obj->rebuild2sync, ntxn);
            ntxn = NULL;
            txbundlesize = 0;
        }
    }

    pthread_exit(NULL);
    return NULL;
}

void increment_integraterebuild_free(increment_integraterebuild* rebuild_obj)
{
    if (NULL == rebuild_obj)
    {
        return;
    }

    rebuild_obj->parser2rebuild = NULL;
    rebuild_obj->rebuild2sync = NULL;
    
    rebuild_destroy((rebuild *)rebuild_obj);

    if (rebuild_obj->honlinerefreshfilterdataset)
    {
        hash_destroy(rebuild_obj->honlinerefreshfilterdataset);
    }

    if (rebuild_obj->onlinerefreshdataset)
    {
        dlist_free(rebuild_obj->onlinerefreshdataset->onlinerefresh, onlinerefresh_integratedataset_free);
        rfree(rebuild_obj->onlinerefreshdataset);
    }

    if (rebuild_obj->olpersist)
    {
        onlinerefresh_persist_free(rebuild_obj->olpersist);
    }
    

    if (rebuild_obj->txnpersist)
    {
        bigtxn_persist_free(rebuild_obj->txnpersist);
    }

    rfree(rebuild_obj);
}
