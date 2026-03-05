#include "ripple_app_incl.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "threads/ripple_threads.h"
#include "queue/ripple_queue.h"
#include "storage/ripple_file_buffer.h"
#include "net/netmsg/ripple_netmsg.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netiomp/ripple_netiomp_poll.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netclient.h"
#include "net/netmsg/ripple_netmsg_c2pidentity.h"
#include "metric/pump/ripple_statework_pump.h"
#include "increment/pump/net/ripple_increment_pumpnet.h"
#include "filetransfer/ripple_filetransfer.h"
#include "works/network/ripple_refresh_pumpshardingnet.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "refresh/ripple_refresh_table_syncstats.h"
#include "refresh/pump/ripple_refresh_pump.h"

/* refresh故障恢复生成下载任务 */
static void ripple_netmsg_c2pidentity_refreshrecovery_dowdload(ripple_increment_pumpnetstate* increment_clientstate, ripple_refresh_table_syncstats* tables)
{
    int index_stat = 0;
    char* url = NULL;
    ripple_filetransfer_refresh *refresh = NULL;
    ripple_refresh_table_syncstat* current_table = NULL;
    ripple_filetransfer_refreshshards *refreshshards = NULL;

    url = guc_getConfigOption(RIPPLE_CFG_KEY_FTPURL);

    if (url == NULL || url[0] == '\0')
    {
        return;
    }

    if (!tables || !tables->tablesyncing)
    {
        return;
    }

    ripple_refresh_table_syncstats_lock(tables);
    current_table = tables->tablesyncing;

    /* 遍历complete目录生成任务 */
    while (current_table)
    {
        if (current_table->tablestat == RIPPLE_REFRESH_TABLE_STAT_DONE)
        {
            current_table = current_table->next;
            continue;
        }

        if (0 == current_table->cnt)
        {
            refreshshards = ripple_filetransfer_refreshshards_init();
            ripple_filetransfer_download_refreshshards_set(refreshshards, current_table->schema, current_table->table);
            increment_clientstate->callback.pumpnet_filetransfernode_add(increment_clientstate->privdata, (void*)refreshshards);
            refreshshards = NULL;
            current_table = current_table->next;
            continue;
        }

        /* 表已经处理过，直接生成分片任务 */
        for (index_stat = 0; index_stat < current_table->cnt; index_stat++)
        {
            if (current_table->stat[index_stat] == RIPPLE_REFRESH_TABLE_SYNCS_SHARD_STAT_DONE)
            {
                continue;
            }

            refresh = ripple_filetransfer_refresh_init();
            ripple_filetransfer_refresh_set(refresh, current_table->schema, current_table->table, current_table->cnt, index_stat + 1);
            sprintf(refresh->base.localdir, "%s/%s/%s_%s", guc_getConfigOption(RIPPLE_CFG_KEY_TRAIL_DIR),
                                                           RIPPLE_REFRESH_REFRESH,
                                                           refresh->schema,
                                                           refresh->table);
            sprintf(refresh->base.localpath, "%s/%s/%s_%s/%s/%s_%s_%u_%u", 
                                                guc_getConfigOption(RIPPLE_CFG_KEY_TRAIL_DIR),
                                                RIPPLE_REFRESH_REFRESH,
                                                refresh->schema,
                                                refresh->table,
                                                RIPPLE_REFRESH_PARTIAL,
                                                refresh->schema,
                                                refresh->table,
                                                refresh->shards,
                                                refresh->shardnum);
            sprintf(refresh->prefixpath, "%s/%s/%s_%s/%s", guc_getConfigOption(RIPPLE_CFG_KEY_JOBNAME),
                                                           RIPPLE_REFRESH_REFRESH,
                                                           refresh->schema,
                                                           refresh->table,
                                                           RIPPLE_REFRESH_COMPLETE);
            increment_clientstate->callback.pumpnet_filetransfernode_add(increment_clientstate->privdata, (void*)refresh);
            refresh = NULL;
        }
        current_table = current_table->next;
    }
    ripple_refresh_table_syncstats_unlock(tables);
}

static bool ripple_netmsg_c2pidentity_restartrefresh(ripple_increment_pumpnetstate *increment_clientstate)
{
    ripple_refresh_tables* rtables                  = NULL;
    ripple_refresh_table_syncstats* tablesyncstats  = NULL;
    ripple_refresh_pump *rpump                      = NULL;
    char refresh_path[MAXPGPATH]                    = { '\0' };

    /* fileid > 0并且有refresh在执行时不重启 */
    if (0 == increment_clientstate->recpos.trail.fileid
        || false == increment_clientstate->callback.pumpstate_isrefreshdown(increment_clientstate->privdata))
    {
        return true;
    }

    sprintf(refresh_path, "%s/%s", guc_getConfigOption(RIPPLE_CFG_KEY_TRAIL_DIR),
                                    RIPPLE_REFRESH_REFRESH);

    /* 从文件夹获取refresh表，没有表返回空退出 */
    rtables = ripple_refresh_tables_gen_from_file(refresh_path);
    if (NULL == rtables || 0 == rtables->cnt)
    {
        ripple_refresh_freetables(rtables);
        return true;
    }

    tablesyncstats = ripple_refresh_table_syncstats_init();

    ripple_refresh_table_syncstats_tablesyncing_setfromfile(rtables, tablesyncstats, refresh_path);
    ripple_refresh_table_syncstats_tablesyncall_setfromfile(rtables, tablesyncstats, refresh_path);

    ripple_netmsg_c2pidentity_refreshrecovery_dowdload(increment_clientstate, tablesyncstats);

    /* 初始化refresh mgr线程的相关结构 */
    rpump = ripple_refresh_pump_init();
    if (NULL == rpump)
    {
        elog(RLOG_WARNING, "init refresh pump error");
        return false;
    }
    ripple_refresh_pump_setstatinit(rpump);
    ripple_refresh_pump_setsynctablestat(rpump, tablesyncstats);

    /* 添加refresh pump */
    increment_clientstate->callback.pumpstate_addrefresh(increment_clientstate->privdata, (void*)rpump);
   
    ripple_refresh_freetables(rtables);
    return true;
}

/* 
 * 接收来自collector的identity结果
 *  pump 处理
 */
bool ripple_netmsg_c2pidentity(void* privdata, uint8* msg)
{
    /*
     * 更新地址信息
     */
    uint8* uptr = NULL;
    int8 identitytype = 0;
    uint32 msglen = 0;
    uint32 msgtype = RIPPLE_NETMSG_TYPE_NOP;
    ripple_increment_pumpnetstate* increment_clientstate = NULL;
    ripple_refresh_pumpshardingnetstate* sharding_clientstate = NULL;

    uptr = msg;
    msgtype = RIPPLE_CONCAT(get, 32bit)(&uptr);
    if(RIPPLE_NETMSG_TYPE_C2P_IDENTITY != msgtype)
    {
        elog(RLOG_WARNING, "get collector response, hope msgtype %u, but got %u", 
                            RIPPLE_NETMSG_TYPE_C2P_IDENTITY,
                            msgtype);
        return false;
    }
    msglen = RIPPLE_CONCAT(get, 32bit)(&uptr);

    if(RIPPLE_NETMSG_TYPE_C2P_IDENTITY_SIZE != msglen)
    {
        elog(RLOG_WARNING, " get RIPPLE_NETMSG_TYPE_C2P_IDENTITY msg, hope msglen 25, but got:%u", msglen);
        return false;
    }

    /* 获取数据 */
    identitytype = RIPPLE_CONCAT(get, 8bit)(&uptr);

    if (RIPPLE_NETIDENTITY_TYPE_PUMPINCREAMENT == identitytype)
    {
        increment_clientstate = (ripple_increment_pumpnetstate*)privdata;
        increment_clientstate->recpos.trail.fileid = RIPPLE_CONCAT(get, 64bit)(&uptr);
        increment_clientstate->recpos.trail.offset = 0;
        increment_clientstate->crecpos.trail.fileid = RIPPLE_CONCAT(get, 64bit)(&uptr);
        increment_clientstate->crecpos.trail.offset = 0;

        increment_clientstate->callback.setmetricloadtrailno(increment_clientstate->privdata, increment_clientstate->recpos.trail.fileid);

        /* 重新设置读取的起点,并根据 fileid 和 foffset 过滤 */
        elog(RLOG_INFO, "get identity increment from collector:%lu, %lu, cfileid:%lu",
                                                    increment_clientstate->recpos.trail.fileid,
                                                    increment_clientstate->recpos.trail.offset,
                                                    increment_clientstate->crecpos.trail.fileid);
        increment_clientstate->state = RIPPLE_INCREMENT_PUMPNETSTATE_STATE_IDENTITY;

        /* 故障恢复 */
        if (false == ripple_netmsg_c2pidentity_restartrefresh(increment_clientstate))
        {
            elog(RLOG_WARNING, "refresh restart error");
            return false;
        }

        /* 尝试清理缓存 */
        riple_file_buffer_clean_waitflush(increment_clientstate->txn2filebuffer);
        increment_clientstate->callback.splittrail_statefileid_set(increment_clientstate->privdata,
                                                                   RIPPLE_PUMP_STATUS_SPLIT_INIT,
                                                                   increment_clientstate->recpos.trail.fileid);
    }
    else if (RIPPLE_NETIDENTITY_TYPE_ONLINEREFRESH_INC == identitytype)
    {
        increment_clientstate = (ripple_increment_pumpnetstate*)privdata;
        increment_clientstate->recpos.trail.fileid = RIPPLE_CONCAT(get, 64bit)(&uptr);
        increment_clientstate->recpos.trail.offset = 0;
        increment_clientstate->crecpos.trail.fileid = RIPPLE_CONCAT(get, 64bit)(&uptr);
        increment_clientstate->crecpos.trail.offset = 0;
        /* 重新设置读取的起点,并根据 fileid 和 foffset 过滤 */
        elog(RLOG_INFO, "onlinerefreshinc get identity increment from collector:%lu, %lu, cfileid:%lu",
                                                    increment_clientstate->recpos.trail.fileid,
                                                    increment_clientstate->recpos.trail.offset,
                                                    increment_clientstate->crecpos.trail.fileid);
        increment_clientstate->state = RIPPLE_INCREMENT_PUMPNETSTATE_STATE_IDENTITY;
    }
    else if (RIPPLE_NETIDENTITY_TYPE_PUMPREFRESHARDING == identitytype
             || RIPPLE_NETIDENTITY_TYPE_ONLINEREFRESH_SHARDING == identitytype)
    {
        sharding_clientstate = (ripple_refresh_pumpshardingnetstate*)privdata;
        /* 接收到身份验证开始获取数据 */
        sharding_clientstate->state = RIPPLE_REFRESH_PUMPSHARDINGNETSTATE_STATE_IDLE;
    }
    else if (RIPPLE_NETIDENTITY_TYPE_BIGTRANSACTION == identitytype)
    {
        increment_clientstate = (ripple_increment_pumpnetstate*)privdata;

        /* 重新设置读取的起点,并根据 fileid 和 foffset 过滤 */
        elog(RLOG_INFO, "bigtxn get identity bigtxn from collector:%lu, %lu, cfileid:%lu",
                                                    increment_clientstate->recpos.trail.fileid,
                                                    increment_clientstate->recpos.trail.offset,
                                                    increment_clientstate->crecpos.trail.fileid);
        increment_clientstate->state = RIPPLE_REFRESH_PUMPSHARDINGNETSTATE_STATE_IDLE;

        /* 设置大事务管理线程状态为 inprocess 用于启动增量线程*/
        increment_clientstate->callback.bigtxn_mgrstat_setinprocess(increment_clientstate->privdata);
    }
    else
    {
        elog(RLOG_WARNING, "Unsupported identity type:%d,",identitytype);
        return false;
    }

    return true;
}



