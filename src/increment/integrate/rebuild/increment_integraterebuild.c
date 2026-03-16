#include "ripple_app_incl.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/string/stringinfo.h"
#include "queue/ripple_queue.h"
#include "task/ripple_task_slot.h"
#include "threads/ripple_threads.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "stmts/ripple_txnstmt.h"
#include "stmts/ripple_txnstmt_prepared.h"
#include "rebuild/ripple_rebuild.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "refresh/ripple_refresh_table_syncstats.h"
#include "stmts/ripple_txnstmt_onlinerefresh.h"
#include "stmts/ripple_txnstmt_updaterewind.h"
#include "stmts/ripple_txnstmt_updatesynctable.h"
#include "onlinerefresh/integrate/dataset/ripple_onlinerefresh_integratedataset.h"
#include "onlinerefresh/integrate/dataset/ripple_onlinerefresh_integratefilterdataset.h"
#include "onlinerefresh/ripple_onlinerefresh_persist.h"
#include "bigtransaction/persist/ripple_bigtxn_persist.h"
#include "bigtransaction/integrate/ripple_bigtxn_integratemanager.h"
#include "onlinerefresh/integrate/ripple_onlinerefresh_integrate.h"
#include "increment/integrate/rebuild/ripple_increment_integraterebuild.h"

/* 初始化 */
ripple_increment_integraterebuild* ripple_increment_integraterebuild_init(void)
{
    char* burst = NULL;
    ripple_increment_integraterebuild* rebuild = rmalloc0(sizeof(ripple_increment_integraterebuild));
    if(NULL == rebuild)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(rebuild, 0, '\0', sizeof(ripple_increment_integraterebuild));
    ripple_rebuild_reset((ripple_rebuild*)rebuild);
    rebuild->parser2rebuild = NULL;
    rebuild->rebuild2sync = NULL;
    rebuild->filterlsn = InvalidXLogRecPtr;
    rebuild->stat = RIPPLE_INCREMENT_INTEGRATEREBUILD_STAT_NOP;
    rebuild->mergetxn = (0 == guc_getConfigOptionInt(RIPPLE_CFG_KEY_MERGETXN)) ? false : true;
    rebuild->txbundlesize = guc_getConfigOptionInt(RIPPLE_CFG_KEY_TXBUNDLESIZE);

    /* 设置integrate_method */
    burst = guc_getConfigOption(RIPPLE_CFG_KEY_INTEGRATE_METHOD);
    if (burst != NULL && '\0' != burst[0] && 0 == strcmp(burst, "burst"))
    {
        rebuild->burst = true;
    }

    rebuild->txnpersist = ripple_bigtxn_persist_init();

    rebuild->honlinerefreshfilterdataset = ripple_onlinerefresh_integratefilterdataset_init();

    rebuild->onlinerefreshdataset = ripple_onlinerefresh_integratedataset_init();
    return rebuild;
}

static bool ripple_increment_integraterebuild_canwork(ripple_increment_integraterebuild* rebuild)
{
    if(rebuild->stat == RIPPLE_INCREMENT_INTEGRATEREBUILD_STAT_WORK)
    {
        return true;
    }
    else if(rebuild->stat == RIPPLE_INCREMENT_INTEGRATEREBUILD_STAT_READY)
    {
        rebuild->stat = RIPPLE_INCREMENT_INTEGRATEREBUILD_STAT_WORK;
        return true;
    }
    else if(rebuild->stat == RIPPLE_INCREMENT_INTEGRATEREBUILD_STAT_WAITTERM)
    {
        ripple_cache_txn_clean(rebuild->parser2rebuild);
        return false;
    }
    return false;
}

static ripple_txn* ripple_increment_integraterebuild_updatesynctabletxn_set(ripple_increment_integraterebuild* rebuild, ripple_txn* txn)
{
    ripple_txn* cur_txn = NULL;
    ripple_txnstmt* stmtnode = NULL;
    ripple_txnstmt_updatesynctable* updatesynctable = NULL;
    cur_txn = ripple_txn_init(txn->xid, txn->start.wal.lsn, InvalidXLogRecPtr);
    if(NULL == cur_txn)
    {
        elog(RLOG_WARNING, "cur_txn out of memory, %s", strerror(errno));
        return NULL;
    }

    /* 申请空间 */
    stmtnode = ripple_txnstmt_init();
    if(NULL == stmtnode)
    {
        rfree(cur_txn);
        elog(RLOG_WARNING, "stmtnode out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(stmtnode, 0, '\0', sizeof(ripple_txnstmt));

    updatesynctable =  ripple_txnstmt_updatesynctable_init();
    if (NULL == updatesynctable)
    {
        rfree(cur_txn);
        rfree(stmtnode);
        return NULL;
    }

    stmtnode->type = RIPPLE_TXNSTMT_TYPE_UPDATESYNCTABLE;
    stmtnode->stmt = (void*)updatesynctable;

    cur_txn->stmts = lappend(cur_txn->stmts, stmtnode);
    cur_txn->confirm.wal.lsn = txn->confirm.wal.lsn;
    cur_txn->end.trail.offset = txn->end.trail.offset;
    cur_txn->endtimestamp = txn->endtimestamp;
    cur_txn->segno = txn->segno;
    cur_txn->xid = txn->xid;
    return cur_txn;
}

static bool ripple_increment_integraterebuild_updaterewindstmt_set(ripple_increment_integraterebuild* rebuild, ripple_txn* txn)
{
    ripple_recpos pos = {{'\0'}};
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

    pos.trail.fileid = txn->segno;
    pos.trail.offset = txn->end.trail.offset;
    ripple_bigtxn_persist_electionrewind(rebuild->txnpersist, &pos);

    updaterewind->rewind = rebuild->txnpersist->rewind;

    stmtnode->type = RIPPLE_TXNSTMT_TYPE_UPDATEREWIND;
    stmtnode->stmt = (void*)updaterewind;

    txn->stmts = lappend(txn->stmts, stmtnode);

    return true;
}

/* 清理放弃掉的onlinerefresh对应过滤集 */
static void ripple_increment_integraterebuild_delonlinerefresdataset(ripple_increment_integraterebuild* rebuild, ripple_thrnode* thrnode, void* abandon)
{
    List* luuid                                         = NULL;
    ListCell* lc                                        = NULL;
    ripple_uuid_t* uuid                                 = NULL;
    ripple_onlinerefresh_integratedatasetnode *endnode  = NULL;

    if (NULL == abandon)
    {
        return;
    }
    
    luuid = (List*)abandon;

    foreach(lc, luuid)
    {

        uuid = (ripple_uuid_t*)lfirst(lc);

        endnode = ripple_onlinerefresh_integratedataset_number_get(rebuild->onlinerefreshdataset, uuid->data);
        while (true)
        {
            if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
            {
                return;
            }

            /* 等待onlinerefresh结束 */
            if (rebuild->callback.isonlinerefreshdone(rebuild->privdata, uuid->data))
            {
                ripple_onlinerefresh_persist_statesetbyuuid(rebuild->olpersist, 
                                                            &endnode->onlinerefreshno, 
                                                            RIPPLE_ONLINEREFRESH_PERSISTNODE_STAT_DONE);
                /* onlinerefresh结束清理persist的存量表 */
                ripple_onlinerefresh_persist_removerefreshtbsbyuuid(rebuild->olpersist, &endnode->onlinerefreshno);
                ripple_onlinerefresh_persist_electionrewindbyuuid(rebuild->olpersist, &endnode->onlinerefreshno);
                ripple_onlinerefresh_integratefilterdataset_delete(rebuild->honlinerefreshfilterdataset, endnode->refreshtables, endnode->txid);
                ripple_onlinerefresh_integratedataset_delete(rebuild->onlinerefreshdataset, uuid->data);
                break;
            }
            usleep(50000);
        }
    }
    return;
}
/* 新增大事务 */
static void  ripple_increment_integraterebuild_addbigtxnpersist(ripple_increment_integraterebuild* rebuild,
                                                                ripple_txn* txn)
{
    bool exist                                  = false;
    ripple_recpos pos                           = {{'\0'}};
    dlistnode *dnode                            = NULL;
    dlistnode* dlnodenext                       = NULL;
    ripple_bigtxn_persist *persist              = NULL;
    ripple_bigtxn_persistnode *persistnode      = NULL;

    persist = rebuild->txnpersist;

    /* 确保存在 */
    dnode = persist->dpersistnodes ? persist->dpersistnodes->head : NULL;

    pos.trail.fileid = txn->segno;
    pos.trail.offset = txn->end.trail.offset;

    /* 遍历persist, 检查是否有相同事务 */
    for (; dnode; dnode = dlnodenext)
    {
        dlnodenext = dnode->next;
        persistnode = (ripple_bigtxn_persistnode *)dnode->value;

        /* 是否存在事务号相同的大事务 */
        if (txn->xid == persistnode->xid)
        {
            /* 存在 */
            exist = true;
            ripple_bigtxn_persistnode_set_begin(persistnode, &pos);
            ripple_bigtxn_persistnode_set_xid(persistnode, txn->xid);
        }
    }

    /* 遍历结束, 判断是否存在, 只有不存在时才需要新增 */
    if (!exist)
    {
        ripple_bigtxn_persistnode *pernode = NULL;

        /* 构建persist node并初始化 */
        pernode = ripple_bigtxn_persist_node_init();
        ripple_bigtxn_persistnode_set_begin(pernode, &pos);
        ripple_bigtxn_persistnode_set_xid(pernode, txn->xid);
        pernode->stat = RIPPLE_BIGTXN_PERSISTNODE_STAT_INPROCESS;
        persist->dpersistnodes = dlist_put(persist->dpersistnodes, pernode);
        persist->count += 1;
    }
}

/* 新增onlinerefresh persist */
static void  ripple_increment_integraterebuild_addonlinerefreshpersist(ripple_increment_integraterebuild* rebuild,
                                                                       ripple_onlinerefresh_integrate *olrintegrate)
{
    bool exist                                  = false;
    dlistnode *dnode                            = NULL;
    dlistnode* dlnodenext                       = NULL;
    ripple_onlinerefresh_persist *persist              = NULL;
    ripple_onlinerefresh_persistnode *persistnode      = NULL;

    persist = rebuild->olpersist;

    /* 确保存在 */
    dnode = persist->dpersistnodes ? persist->dpersistnodes->head : NULL;

    /* 遍历persist, 检查是否有相同事务 */
    for (; dnode; dnode = dlnodenext)
    {
        dlnodenext = dnode->next;
        persistnode = (ripple_onlinerefresh_persistnode *)dnode->value;

        /* 是否存在事务号相同的大事务 */
        if (0 == memcmp(olrintegrate->no.data, persistnode->uuid.data, RIPPLE_UUID_LEN))
        {
            /* 存在 */
            exist = true;
            ripple_onlinerefresh_persistnode_beginset(persistnode, olrintegrate->begin);
        }
    }

    /* 遍历结束, 判断是否存在, 只有不存在时才需要新增 */
    if (!exist)
    {
        ripple_onlinerefresh_persistnode *pernode = NULL;

        /* 构建persist node并初始化 */
        pernode = ripple_onlinerefresh_persistnode_init();
        ripple_onlinerefresh_persistnode_statset(pernode, RIPPLE_ONLINEREFRESH_PERSISTNODE_STAT_INIT);
        ripple_onlinerefresh_persistnode_uuidset(pernode, &olrintegrate->no);
        ripple_onlinerefresh_persistnode_incrementset(pernode, olrintegrate->increment);
        ripple_onlinerefresh_persistnode_beginset(pernode, olrintegrate->begin);
        ripple_onlinerefresh_persistnode_txidset(pernode, olrintegrate->txid);

        pernode->refreshtbs = olrintegrate->tablesyncstats;
        if(true == dlist_isnull(persist->dpersistnodes))
        {
            persist->rewind.trail.fileid = olrintegrate->begin.trail.fileid;
            persist->rewind.trail.offset = olrintegrate->begin.trail.offset;
        }
        persist->dpersistnodes = dlist_put(persist->dpersistnodes, pernode);
    }
    ripple_onlinerefresh_persist_write(persist);
}

static bool ripple_increment_integraterebuild_isspecialtxn(ripple_txn* txn)
{
    if(NULL == txn->stmts)
    {
        return true;
    }

    /* refresh */
    if (RIPPLE_TXN_TYPE_METADATA < txn->type)
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
static bool ripple_increment_integraterebuild_rebuildtxn(ripple_increment_integraterebuild* rebuild,
                                                         ripple_txn *txn)
{
    if (true == rebuild->burst)
    {
        /* 重组 */
        if(false == ripple_rebuild_txnburst((ripple_rebuild*)rebuild, txn))
        {
            elog(RLOG_WARNING, "ripple_increment_integraterebuild_txnburst error");
            return false;
        }
    }
    else
    {
        /* 重组 */
        if(false == ripple_rebuild_prepared((ripple_rebuild*)rebuild, txn))
        {
            elog(RLOG_WARNING, "ripple_increment_integraterebuild_prepared error");
            return false;
        }
    }

    return true;
}

/*
 * integate rebuild应用系统表，应用时先清理index和attribute
*/
static void ripple_increment_integraterebuild_transcatalog2transcache(ripple_increment_integraterebuild* rebuild,
                                                                      ripple_txn* txn)
{
    bool equal                              = false;
    ListCell* stmtlc                        = NULL;
    ListCell* catraloglc                    = NULL;
    ripple_txnstmt* stmtnode                = NULL;
    ripple_txnstmt_metadata* metadatastmt   = NULL;

    if(NULL == txn->stmts)
    {
        return;
    }

    foreach(stmtlc, txn->stmts)
    {
        stmtnode = (ripple_txnstmt*)lfirst(stmtlc);

        if (RIPPLE_TXNSTMT_TYPE_METADATA == stmtnode->type)
        {
            metadatastmt = (ripple_txnstmt_metadata*)stmtnode->stmt;
            catraloglc = metadatastmt->begin;
            /* 根基class->oid清理之前的index和attribute */
            ripple_cache_sysdicts_clearsysdicthisbyclass(rebuild->rebuild.sysdicts, catraloglc);

            while(1)
            {
                /* 应用到系统表中 */
                ripple_cache_sysdicts_txnsysdicthisitem2cache(rebuild->rebuild.sysdicts, (void*)catraloglc);

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
static bool ripple_increment_integraterebuild_specialtxn(ripple_increment_integraterebuild* rebuild,
                                                         ripple_thrnode* thrnode,
                                                         ripple_txn* txn,
                                                         ripple_txn** ntxn,
                                                         int* txbundlesize)
{
    ripple_recpos pos = {{'\0'}};
    ripple_txn* cur_txn = NULL;
    ripple_txnstmt* stmtnode = NULL;
    ripple_txnstmt_onlinerefresh* onlinerefresh = NULL;
    ripple_onlinerefresh_integratedatasetnode *datasetnode = NULL;
    ripple_onlinerefresh_integratedatasetnode *endnode = NULL;

    if(NULL == txn->stmts)
    {
        return true;
    }

    stmtnode = (ripple_txnstmt*)lfirst(list_head(txn->stmts));

    /* refresh */
    if (RIPPLE_TXNSTMT_TYPE_REFRESH == stmtnode->type)
    {
        cur_txn = NULL;
        while (true)
        {
            if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
            {
                return false;
            }

            if (true == rebuild->callback.isrefreshdown(rebuild->privdata))
            {
                break;
            }
            usleep(50000);
        }
        /* 更新状态表信息不包含rewind信息 */
        cur_txn = ripple_increment_integraterebuild_updatesynctabletxn_set(rebuild, txn);
        if (cur_txn)
        {
            cur_txn->segno = 1;
            cur_txn->end.trail.offset = 0;
            /* 事务中不存储rewind信息，以stmt将计算后的rewind传递到应用线程更新rewind信息 */
            ripple_increment_integraterebuild_updaterewindstmt_set(rebuild, cur_txn);
            ripple_cache_txn_add(rebuild->rebuild2sync, cur_txn);
            cur_txn = NULL;
        }
        
    }
    else if (RIPPLE_TXNSTMT_TYPE_ONLINEREFRESH_BEGIN == stmtnode->type)
    {
        if (NULL != *ntxn)
        {
            if(false == ripple_increment_integraterebuild_rebuildtxn(rebuild, *ntxn))
            {
                elog(RLOG_WARNING, "increment integraterebuild specialtxn rebuildtxn error");
                return false;
            }
            /* 事务中不存储rewind信息，以stmt将计算后的rewind传递到应用线程更新rewind信息 */
            ripple_increment_integraterebuild_updaterewindstmt_set(rebuild, *ntxn);
            ripple_cache_txn_add(rebuild->rebuild2sync, *ntxn);
            *ntxn = NULL;
            *txbundlesize = 0;
        }
        //转换类型
        onlinerefresh = (ripple_txnstmt_onlinerefresh*)stmtnode->stmt;

        /* 等待缓存内事务应用完成 */
        while(true)
        {
            if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
            {
                return false;
            }

            if (txn->segno == rebuild->olpersist->rewind.trail.fileid
                && txn->end.trail.offset == rebuild->olpersist->rewind.trail.offset)
            {
                break;
            }

            /* 缓存应用完成且sync线程空闲 */
            if (ripple_cache_txn_isnull(rebuild->rebuild2sync)
                && true == rebuild->callback.issyncidle(rebuild->privdata))
            {
                ripple_refresh_tables* tables = NULL;
                ripple_refresh_table_syncstats* tablesyncstats = NULL;
                ripple_onlinerefresh_integrate *integrateolrefresh = NULL;

                /* 注册onlinerefresh节点 */
                tables = ripple_refresh_tables_copy(onlinerefresh->refreshtables);

                tablesyncstats = ripple_refresh_table_syncstats_init();
                ripple_refresh_table_syncstats_tablesyncing_set(tables, tablesyncstats);
                ripple_refresh_table_syncstats_tablesyncall_set(tables, tablesyncstats);

                integrateolrefresh = ripple_onlinerefresh_integrate_init(onlinerefresh->increment);

                integrateolrefresh->stat = RIPPLE_ONLINEREFRESH_INTEGRATE_INIT;
                rmemcpy1(integrateolrefresh->no.data, 0, onlinerefresh->no->data, RIPPLE_UUID_LEN);
                integrateolrefresh->increment = onlinerefresh->increment;
                integrateolrefresh->tablesyncstats = tablesyncstats;
                integrateolrefresh->txid = onlinerefresh->txid;
                integrateolrefresh->begin.trail.fileid = txn->segno;
                integrateolrefresh->begin.trail.offset = txn->end.trail.offset;

                ripple_increment_integraterebuild_addonlinerefreshpersist(rebuild, integrateolrefresh);
                rebuild->callback.addonlinerefresh(rebuild->privdata, integrateolrefresh);

                ripple_refresh_freetables(tables);
                break;
            }
            usleep(50000);
        }

        while (true)
        {
            if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
            {
                return false;
            }
            /* 等待onlinerefresh存量结束 */
            if (true == rebuild->callback.isolrrefreshdone(rebuild->privdata, onlinerefresh->no->data))
            {
                break;
            }
            usleep(50000);
        }

        /* 创建onlinerefresh过滤数据集 */
        ripple_onlinerefresh_integratefilterdataset_add(rebuild->honlinerefreshfilterdataset, onlinerefresh->refreshtables, onlinerefresh->txid);
        datasetnode = ripple_onlinerefresh_integratedatasetnode_init();
        ripple_onlinerefresh_integratedatasetnode_no_set(datasetnode, onlinerefresh->no->data);
        ripple_onlinerefresh_integratedatasetnode_txid_set(datasetnode, onlinerefresh->txid);
        ripple_onlinerefresh_integratedatasetnode_refreshtables_set(datasetnode, onlinerefresh->refreshtables);
        ripple_onlinerefresh_integratedataset_add(rebuild->onlinerefreshdataset, datasetnode);
    }
    else if (RIPPLE_TXNSTMT_TYPE_ONLINEREFRESH_END == stmtnode->type)
    {
        if (NULL != *ntxn)
        {
            if(false == ripple_increment_integraterebuild_rebuildtxn(rebuild, *ntxn))
            {
                elog(RLOG_WARNING, "increment integraterebuild specialtxn rebuildtxn error");
                return false;
            }
            ripple_cache_txn_add(rebuild->rebuild2sync, *ntxn);
            *ntxn = NULL;
            *txbundlesize = 0;
        }
        elog(RLOG_DEBUG, "get RIPPLE_TXNSTMT_TYPE_ONLINEREFRESH_END");
        /* 获取onlinerefresh编号 */
        endnode = ripple_onlinerefresh_integratedataset_number_get(rebuild->onlinerefreshdataset, stmtnode->stmt);

        while (true)
        {
            if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
            {
                return false;
            }
            /* 等待onlinerefresh结束 */
            if (rebuild->callback.isonlinerefreshdone(rebuild->privdata, endnode->onlinerefreshno.data))
            {
                ripple_onlinerefresh_persist_statesetbyuuid(rebuild->olpersist, 
                                                            &endnode->onlinerefreshno, 
                                                            RIPPLE_ONLINEREFRESH_PERSISTNODE_STAT_DONE);
                /* onlinerefresh结束清理persist的存量表 */
                ripple_onlinerefresh_persist_removerefreshtbsbyuuid(rebuild->olpersist, &endnode->onlinerefreshno);
                ripple_onlinerefresh_persist_electionrewindbyuuid(rebuild->olpersist, &endnode->onlinerefreshno);
                ripple_onlinerefresh_integratefilterdataset_delete(rebuild->honlinerefreshfilterdataset, endnode->refreshtables, endnode->txid);
                ripple_onlinerefresh_integratedataset_delete(rebuild->onlinerefreshdataset, stmtnode->stmt);
                rebuild->callback.setonlinerefreshfree(rebuild->privdata, stmtnode->stmt);
                break;
            }
            usleep(50000);
        }

        cur_txn = ripple_increment_integraterebuild_updatesynctabletxn_set(rebuild, txn);
        if (cur_txn)
        {
            /* 事务中不存储rewind信息，以stmt将计算后的rewind传递到应用线程更新rewind信息 */
            ripple_increment_integraterebuild_updaterewindstmt_set(rebuild, cur_txn);
            ripple_cache_txn_add(rebuild->rebuild2sync, cur_txn);
            cur_txn = NULL;
        }
    }
    else if (RIPPLE_TXNSTMT_TYPE_RESET == stmtnode->type)
    {
        if (NULL != *ntxn)
        {
            if(false == ripple_increment_integraterebuild_rebuildtxn(rebuild, *ntxn))
            {
                elog(RLOG_WARNING, "increment integraterebuild specialtxn rebuildtxn error");
                return false;
            }
            /* 事务中不存储rewind信息，以stmt将计算后的rewind传递到应用线程更新rewind信息 */
            ripple_increment_integraterebuild_updaterewindstmt_set(rebuild, *ntxn);
            ripple_cache_txn_add(rebuild->rebuild2sync, *ntxn);
            *ntxn = NULL;
            *txbundlesize = 0;
        }
        ripple_bigtxn_integratepersist_cleannotdone(rebuild->txnpersist);

    }
    else if (RIPPLE_TXNSTMT_TYPE_ABANDON == stmtnode->type)
    {
        ripple_bigtxn_persist_removebyxid(rebuild->txnpersist, txn->xid);
        pos.trail.fileid = txn->segno;
        pos.trail.offset = txn->end.trail.offset;
        ripple_bigtxn_persist_electionrewind(rebuild->txnpersist, &pos);
    }
    else if (RIPPLE_TXNSTMT_TYPE_BIGTXN_BEGIN == stmtnode->type)
    {
        //todo,删除persist，大事务接收到end启动，不完成不会更新rewind点，只要不结束必然会启动大事务
        ripple_increment_integraterebuild_addbigtxnpersist(rebuild, txn);
    }
    else if (RIPPLE_TXNSTMT_TYPE_BIGTXN_END == stmtnode->type)
    {
        ripple_bigtxn_end_stmt *end_stmt = NULL;
        ripple_bigtxn_integratemanager *bigtxn = NULL;
        if (NULL != *ntxn)
        {
            if(false == ripple_increment_integraterebuild_rebuildtxn(rebuild, *ntxn))
            {
                elog(RLOG_WARNING, "increment integraterebuild specialtxn rebuildtxn error");
                return false;
            }
            /* 事务中不存储rewind信息，以stmt将计算后的rewind传递到应用线程更新rewind信息 */
            ripple_increment_integraterebuild_updaterewindstmt_set(rebuild, *ntxn);
            ripple_cache_txn_add(rebuild->rebuild2sync, *ntxn);
            *ntxn = NULL;
            *txbundlesize = 0;
        }
        end_stmt = (ripple_bigtxn_end_stmt *)stmtnode->stmt;
        /* 等待缓存内事务应用完成 */
        while(true)
        {
            if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
            {
                return false;
            }

            /* 缓存应用完成且sync线程空闲 */
            if (ripple_cache_txn_isnull(rebuild->rebuild2sync)
                && true == rebuild->callback.issyncidle(rebuild->privdata))
            {
                if (true == end_stmt->commit)
                {
                    bigtxn = ripple_bigtxn_integratemanager_init();
                    bigtxn->xid = end_stmt->xid;
                    /* 复制onlinerefresh过滤集用于大事务内部过滤 */
                    bigtxn->honlinerefreshfilterdataset = ripple_onlinerefresh_integratefilterdataset_copy(rebuild->honlinerefreshfilterdataset);
                    if (false == dlist_isnull(rebuild->onlinerefreshdataset->onlinerefresh))
                    {
                        bigtxn->onlinerefreshdataset = ripple_onlinerefresh_integratedataset_copy(rebuild->onlinerefreshdataset);
                    }
                    ripple_bigtxn_integratemanager_stat_set(bigtxn, RIPPLE_BIGTXN_INTEGRATEMANAGER_STAT_INIT);
                    rebuild->callback.addbigtxn(rebuild->privdata, (void*)bigtxn);
                }
                break;
            }
            usleep(50000);
        }

        if (true == end_stmt->commit)
        {
            while (true)
            {
                if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
                {
                    return false;
                }

                if (true == rebuild->callback.isbigtxnsigterm(rebuild->privdata, end_stmt->xid))
                {
                    return false;
                }

                if (true == rebuild->callback.isbigtxndown(rebuild->privdata, end_stmt->xid))
                {
                    break;
                }
                usleep(50000);
            }
        }

        ripple_bigtxn_persist_removebyxid(rebuild->txnpersist, end_stmt->xid);

        cur_txn = ripple_increment_integraterebuild_updatesynctabletxn_set(rebuild, txn);
        if (cur_txn)
        {
            cur_txn->xid = end_stmt->xid;
            /* 事务中不存储rewind信息，以stmt将计算后的rewind传递到应用线程更新rewind信息 */
            ripple_increment_integraterebuild_updaterewindstmt_set(rebuild, cur_txn);
            ripple_cache_txn_add(rebuild->rebuild2sync, cur_txn);
            cur_txn = NULL;
        }
    }
    else if (RIPPLE_TXNSTMT_TYPE_ONLINEREFRESHABANDON == stmtnode->type)
    {
        ripple_increment_integraterebuild_delonlinerefresdataset(rebuild, thrnode, stmtnode->stmt);
    }

    return true;
}


/* 工作 */
void* ripple_increment_integraterebuild_main(void* args)
{
    bool find                                                           = false;
    int timeout                                                         = 0;
    int txbundlesize                                                    = 0;
    Oid relid                                                           = InvalidOid;
    ListCell* filterlc                                                  = NULL;
    ripple_txn* txns                                                    = NULL;
    ripple_txn* ntxn                                                    = NULL;
    ripple_txn* txnnode                                                 = NULL;
    ripple_thrnode* thrnode                                             = NULL;
    ripple_txnstmt* stmtnode                                            = NULL;
    ripple_increment_integraterebuild* rebuild                          = NULL;
    xk_pg_parser_translog_tbcol_values* values                          = NULL;
    xk_pg_parser_translog_tbcolbase* tbcolbase                          = NULL;
    xk_pg_parser_translog_tbcol_nvalues* nvalues                        = NULL;
    ripple_onlinerefresh_integratedatasetnode *node                     = NULL;
    ripple_onlinerefresh_integratefilterdataset* filterdatasetentry     = NULL;

    thrnode = (ripple_thrnode*)args;
    /* 入参转换 */
    rebuild = (ripple_increment_integraterebuild* )thrnode->data;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "increment integrate rebuild exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
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
            ripple_cache_txn_clean(rebuild->parser2rebuild);
            break;
        }

        if(false == ripple_increment_integraterebuild_canwork(rebuild))
        {
            usleep(50000);
            continue;
        }

        /* 在缓存中获取数据 */
        txns = ripple_cache_txn_getbatch(rebuild->parser2rebuild, &timeout);
        if(NULL == txns)
        {
            /* 超时,再次获取 */
            if(RIPPLE_ERROR_TIMEOUT == timeout)
            {
                usleep(10000);
                continue;
            }

            elog(RLOG_WARNING, "get file buffer error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        /* 遍历 txns 并重组语句 */
        for(txnnode = txns; NULL != txnnode; txnnode = txns)
        {
            /* special 用于标记指定事务: refresh/onlinerefreshbegin/onlinerefreshend */
            txns = txnnode->cachenext;

            /* 过滤事务 */
            if(txnnode->confirm.wal.lsn <= rebuild->filterlsn)
            {
                /* 应用系统表数据 */
                ripple_increment_integraterebuild_transcatalog2transcache(rebuild, txnnode);
                ripple_txn_free(txnnode);
                rfree(txnnode);
                continue;
            }
            if (RIPPLE_MAX_LSN > txnnode->confirm.wal.lsn)
            {
                rebuild->filterlsn = txnnode->confirm.wal.lsn;
            }

            /* 用于处理指定事务: refresh/onlinerefreshbegin/onlinerefreshend */
            if (true == ripple_increment_integraterebuild_isspecialtxn(txnnode))
            {

                if(false == ripple_increment_integraterebuild_specialtxn(rebuild,
                                                                        thrnode,
                                                                        txnnode,
                                                                        &ntxn,
                                                                        &txbundlesize))
                {
                    /* term退出或其他独立线程退出，返回上层while循环等待term */
                    rebuild->stat = RIPPLE_INCREMENT_INTEGRATEREBUILD_STAT_WAITTERM;
                    elog(RLOG_WARNING, "Received term or other independent thread exit");
                    break;
                }

                if (txnnode)
                {
                    ripple_txn_free(txnnode);
                    rfree(txnnode);
                }
                continue;
            }

            if (false == dlist_isnull(rebuild->onlinerefreshdataset->onlinerefresh))
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

                    node = ripple_onlinerefresh_integratedataset_txid_get(rebuild->onlinerefreshdataset, filterdatasetentry->txid);
                    while(false == rebuild->callback.isonlinerefreshdone(rebuild->privdata, node->onlinerefreshno.data))
                    {
                        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
                        {
                            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
                            ripple_pthread_exit(NULL);
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
                    ripple_txn* cur_txn = NULL;
                    cur_txn = ripple_increment_integraterebuild_updatesynctabletxn_set(rebuild, txnnode);
                    if (cur_txn)
                    {
                        /* 事务中不存储rewind信息，以stmt将计算后的rewind传递到应用线程更新rewind信息 */
                        ripple_increment_integraterebuild_updaterewindstmt_set(rebuild, cur_txn);
                        ripple_cache_txn_add(rebuild->rebuild2sync, cur_txn);
                        cur_txn = NULL;
                    }
                    else
                    {
                        elog(RLOG_WARNING, "ripple_increment_integraterebuild_updatesynctabletxn_set error");
                        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                        break;
                    }
                    ripple_txn_free(txnnode);
                    rfree(txnnode);
                    continue;
                }
            }

            if(NULL == txnnode->stmts)
            {
                ripple_txn_free(txnnode);
                rfree(txnnode);
                continue;
            }

            /* 不执行事务合并 */
            if (false == rebuild->mergetxn)
            {
                /* 将事务放入到缓存中 */
                if (txnnode->end.wal.lsn != InvalidXLogRecPtr)
                {
                    rebuild->filterlsn = txnnode->end.wal.lsn;
                }

                if(false == ripple_increment_integraterebuild_rebuildtxn(rebuild, txnnode))
                {
                    thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                    break;
                }

                /* 事务中不存储rewind信息，以stmt将计算后的rewind传递到应用线程更新rewind信息 */
                ripple_increment_integraterebuild_updaterewindstmt_set(rebuild, txnnode);
                ripple_cache_txn_add(rebuild->rebuild2sync, txnnode);
                continue;
            }

            if (txnnode->stmts->length > rebuild->txbundlesize)
            {
                if (NULL != ntxn)
                {
                    if(false == ripple_increment_integraterebuild_rebuildtxn(rebuild, ntxn))
                    {
                        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                        break;
                    }
                    /* 事务中不存储rewind信息，以stmt将计算后的rewind传递到应用线程更新rewind信息 */
                    ripple_increment_integraterebuild_updaterewindstmt_set(rebuild, ntxn);
                    ripple_cache_txn_add(rebuild->rebuild2sync, ntxn);
                    ntxn = NULL;
                    txbundlesize = 0;
                }

                if(false == ripple_increment_integraterebuild_rebuildtxn(rebuild, txnnode))
                {
                    thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                    break;
                }

                /* 事务中不存储rewind信息，以stmt将计算后的rewind传递到应用线程更新rewind信息 */
                ripple_increment_integraterebuild_updaterewindstmt_set(rebuild, txnnode);
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
                    elog(RLOG_WARNING, "integrate rebuild txn init out of memory, %s", strerror(errno));
                    thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                    break;
                }
                rmemset0(ntxn, 0, '\0', sizeof(ripple_txn));
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
            ripple_txn_free(txnnode);
            rfree(txnnode);

            /* 最后一个事务或超出合并事务的大小 */
            if(NULL != txns && NULL != txns->stmts
                && ((txbundlesize + txns->stmts->length) < rebuild->txbundlesize))
            {
                continue;
            }

            if(false == ripple_increment_integraterebuild_rebuildtxn(rebuild, ntxn))
            {
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                break;
            }

            /* 事务中不存储rewind信息，以stmt将计算后的rewind传递到应用线程更新rewind信息 */
            ripple_increment_integraterebuild_updaterewindstmt_set(rebuild, ntxn);
            ripple_cache_txn_add(rebuild->rebuild2sync, ntxn);
            ntxn = NULL;
            txbundlesize = 0;
        }
    }

    ripple_pthread_exit(NULL);
    return NULL;
}

void ripple_increment_integraterebuild_free(ripple_increment_integraterebuild* rebuild)
{
    if (NULL == rebuild)
    {
        return;
    }

    rebuild->parser2rebuild = NULL;
    rebuild->rebuild2sync = NULL;
    
    ripple_rebuild_destroy((ripple_rebuild *)rebuild);

    if (rebuild->honlinerefreshfilterdataset)
    {
        hash_destroy(rebuild->honlinerefreshfilterdataset);
    }

    if (rebuild->onlinerefreshdataset)
    {
        dlist_free(rebuild->onlinerefreshdataset->onlinerefresh, ripple_onlinerefresh_integratedataset_free);
        rfree(rebuild->onlinerefreshdataset);
    }

    if (rebuild->olpersist)
    {
        ripple_onlinerefresh_persist_free(rebuild->olpersist);
    }
    

    if (rebuild->txnpersist)
    {
        ripple_bigtxn_persist_free(rebuild->txnpersist);
    }

    rfree(rebuild);
}
