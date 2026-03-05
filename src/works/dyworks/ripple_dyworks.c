#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/thread/ripple_thread.h"
#include "port/net/ripple_net.h"
#include "net/netiomp/ripple_netiomp.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/hash/hash_search.h"
#include "misc/ripple_misc_stat.h"
#include "queue/ripple_queue.h"
#include "snapshot/ripple_snapshot.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "task/ripple_task_slot.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "utils/mpage/mpage.h"
#include "queue/ripple_queue.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadpagefromfile.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadwalrecords.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "serial/ripple_serial.h"
#include "utils/mpage/mpage.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadtrailrecords.h"
#include "parser/trail/ripple_parsertrail.h"
#include "net/netmsg/ripple_netmsg.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netiomp/ripple_netiomp_poll.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netclient.h"
#include "works/dyworks/ripple_dyworks.h"
#include "refresh/ripple_refresh_tables.h"
#include "works/dyworks/taskwork/ripple_taskwork.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "refresh/ripple_refresh_table_syncstats.h"
#include "stmts/ripple_txnstmt_refresh.h"
#include "stmts/ripple_txnstmt_onlinerefresh.h"
#include "works/parserwork/wal/ripple_onlinerefresh.h"
#include "works/parserwork/wal/ripple_rewind.h"
#include "works/parserwork/wal/ripple_parserwork_decode.h"
#include "works/splitwork/wal/ripple_splitwork_wal.h"
#include "fastcompare/ripple_fastcompare_chunk.h"
#include "fastcompare/ripple_fastcompare_tablecomparecatalog.h"
#include "fastcompare/ripple_fastcompare_tableslicetask.h"
#include "fastcompare/ripple_fastcompare_tableslice.h"
#include "fastcompare/ripple_fastcompare_simpledatachunk.h"
#include "fastcompare/ripple_fastcompare_datacmpresult.h"
#include "fastcompare/ripple_fastcompare_datacompare.h"
#include "fastcompare/ripple_fastcompare_columnvalue.h"
#include "fastcompare/ripple_fastcompare_beginchunk.h"
#include "fastcompare/ripple_fastcompare_beginslice.h"
#include "fastcompare/ripple_fastcompare_tablecorrectslice.h"
#include "fastcompare/ripple_fastcompare_tableslicecorrectmanager.h"
#include "bigtransaction/persist/ripple_bigtxn_persist.h"
#include "bigtransaction/ripple_bigtxn.h"
#include "increment/pump/split/ripple_increment_pumpsplittrail.h"
#include "increment/pump/parser/ripple_increment_pumpparsertrail.h"
#include "increment/pump/serial/ripple_increment_pumpserial.h"
#include "increment/pump/net/ripple_increment_pumpnet.h"

typedef void (*dyworksnodefree)();
typedef void* (*dyworkthread)(void* args);

typedef struct RIPPLE_DYWORKS_TYP2OPT
{
    ripple_dythread_type                type;
    char*                               desc;
    dyworkthread                        main;
    dyworksnodefree                     free;
} ripple_dyworks_typ2opt;

static ripple_dyworks*          m_dyworks = NULL;

static ripple_dyworks_typ2opt    m_dyworksopt[] =
{
    {
        RIPPLE_DYTHREAD_TYPE_NOP,
        "NOP",
        NULL,
        NULL
    },
    {
        RIPPLE_DYTHREAD_TYPE_NETSVR,
        "COLLECTOR NET SVR",
        NULL,
        NULL
    },
    {
        RIPPLE_DYTHREAD_TYPE_REFRESHMGR,
        "REFRESH THREAD MANAGER",
        NULL,
        NULL
    },
    {
        RIPPLE_DYTHREAD_TYPE_REFRESHMONITOR,
        "REFRESH WORK MONITOR",
        NULL,
        NULL
    },
    {
        RIPPLE_DYTHREAD_TYPE_WORK,
        "REFRESH WORK",
        ripple_taskwork_main,
        ripple_taskwork_free
    },
    {
        RIPPLE_DYTHREAD_TYPE_ONLINEREFRESH_CAPTURE_PARSERWAL,
        "ONLINE REFRESH CAPTURE PARSERWAL WORK",
        NULL,
        NULL,
    },
    {
        RIPPLE_DYTHREAD_TYPE_ONLINEREFRESH_CAPTURE_SPLITWAL,
        "ONLINE REFRESH CAPTURE SPLITWAL WORK",
        ripple_onlinerefresh_captureloadrecord_main,
        ripple_onlinerefresh_captureloadrecord_free,
    },
    {
        RIPPLE_DYTHREAD_TYPE_ONLINEREFRESH_CAPTURE_WRITE,
        "ONLINE REFRESH CAPTURE WRITE WORK",
        NULL,
        NULL,
    },
    {
        RIPPLE_DYTHREAD_TYPE_ONLINEREFRESH_CAPTURE_SERIAL,
        "ONLINE REFRESH CAPTURE SERIAL WORK",
        NULL,
        NULL,
    },
    {
        RIPPLE_DYTHREAD_TYPE_ONLINEREFRESH_CAPTURE,
        "ONLINE REFRESH CAPTURE WORK",
        NULL,
        NULL
    },
    {
        RIPPLE_DYTHREAD_TYPE_PUMP_ONLINEREFRESH_MANAGE,
        "CAPTURE ONLINEREFRESH MANAGE",
        NULL,
        NULL
    },
    {
        RIPPLE_DYTHREAD_TYPE_INTEGRATE_ONLINEREFRESH_MANAGE,
        "INTEGRATE ONLINEREFRESH MANAGE",
        NULL,
        NULL
    },
    {
        RIPPLE_DYTHREAD_TYPE_FASTCMPTABLESLICECORRECT,
        "Fast Compare TableSliceCorrect",
        riple_fastcompare_tableslicecorrectmanager_main,
        riple_fastcompare_tableslicecorrectmanager_free
    },
    {
        RIPPLE_DYTHREAD_TYPE_FASTCMPTABLESLICE,
        "Fast Compare TableSlice",
        ripple_fastcompare_tableslicetask_main,
        ripple_fastcompare_tableslicetask_free
    },
    {
        RIPPLE_DYTHREAD_TYPE_PUMP_BIGTXN_SPLITTRAIL,
        "Bigtransaction Pump Split",
        NULL,
        NULL
    },
    {
        RIPPLE_DYTHREAD_TYPE_PUMP_BIGTXN_PARSERTRAIL,
        "Bigtransaction Pump Parser",
        NULL,
        NULL
    },
    {
        RIPPLE_DYTHREAD_TYPE_PUMP_BIGTXN_SERIAL,
        "Bigtransaction Pump Serial",
        NULL,
        NULL
    },
    {
        RIPPLE_DYTHREAD_TYPE_PUMP_BIGTXN_CLIENT,
        "Bigtransaction Pump Client",
        NULL,
        NULL
    }
};


/* 
 * 检查待启动队列中是否有内容，有内容则启动
 *  1、分别检测待启动队列中是否有内容
 *  2、启动线程，并加入到运行队列中
 */
static void ripple_dyworks_maystart(void)
{
    int index = 0;
    ripple_dyworks_node* dyworksnode = NULL;
    for(index = 0; index < RIPPLE_DYTHREAD_TYPE_MAX; index++)
    {
        /* 加锁 */
        ripple_thread_lock(&m_dyworks->nodeswait[index].lock);
        if(NULL == m_dyworks->nodeswait[index].head)
        {
            ripple_thread_unlock(&m_dyworks->nodeswait[index].lock);
            continue;
        }

        /* 遍历启动 */
        for(dyworksnode = m_dyworks->nodeswait[index].head; NULL != dyworksnode; dyworksnode = m_dyworks->nodeswait[index].head)
        {
            if(NULL == dyworksnode->next)
            {
                m_dyworks->nodeswait[index].head = NULL;
                m_dyworks->nodeswait[index].tail = NULL;
            }
            else
            {
                m_dyworks->nodeswait[index].head = dyworksnode->next;
                dyworksnode->next->prev = NULL;
            }

            /* 将线程加入到启动线程中并启动 */
            /* 加锁，保证唯一性 */
            dyworksnode->next = NULL;
            dyworksnode->prev = NULL;
            dyworksnode->status = RIPPLE_WORK_STATUS_INIT;
            if(NULL == m_dyworks->nodesrunning[index].head)
            {
                m_dyworks->nodesrunning[index].head = dyworksnode;
            }
            else
            {
                dyworksnode->prev = m_dyworks->nodesrunning[index].tail;
                dyworksnode->prev->next = dyworksnode;
            }
            m_dyworks->nodesrunning[index].tail = dyworksnode;

            /* 启动工作线程 */
            ripple_thread_create(&dyworksnode->id, NULL, m_dyworksopt[index].main, dyworksnode);
        }

        ripple_thread_unlock(&m_dyworks->nodeswait[index].lock);
    }
    return;
}

/*
 * 检测是否有线程退出
*/
static void ripple_dyworks_maystop(void)
{
    int iret = 0;
    int index = 0;
    ripple_dyworks_node* dyworksnode = NULL;
    ripple_dyworks_node* currdyworksnode = NULL;
    for(index = 0; index < RIPPLE_DYTHREAD_TYPE_MAX; index++)
    {
        if(NULL == m_dyworks->nodesrunning[index].head)
        {
            continue;
        }

        /* 遍历启动 */
        for(dyworksnode = m_dyworks->nodesrunning[index].head; NULL != dyworksnode; dyworksnode = currdyworksnode)
        {
            currdyworksnode = dyworksnode->next;
            
            iret = ripple_thread_tryjoin_np(dyworksnode->id, NULL);
            if(EBUSY == iret || EINTR == iret)
            {
                /* 正常运行 */
                continue;
            }
            else if(0 == iret)
            {
                /* 检测线程是否为异常退出，若为异常退出，那么设置为 g_gotsigterm */
                if(RIPPLE_WORK_STATUS_EXIT != dyworksnode->status)
                {
                    elog(RLOG_WARNING, "pthread:%lu abnormal, xsynch exit", dyworksnode->id);
                    g_gotsigterm = true;
                }

                /* 资源释放 */
                if(NULL == dyworksnode->next)
                {
                    /* 尾部 */
                    if(m_dyworks->nodesrunning[index].head == dyworksnode)
                    {
                        /* 头部 */
                        m_dyworks->nodesrunning[index].head = NULL;
                        m_dyworks->nodesrunning[index].tail = NULL;
                    }
                    else
                    {
                        m_dyworks->nodesrunning[index].tail->prev->next = NULL;
                        m_dyworks->nodesrunning[index].tail = m_dyworks->nodesrunning[index].tail->prev;
                    }
                }
                else
                {
                    if(m_dyworks->nodesrunning[index].head == dyworksnode)
                    {
                        m_dyworks->nodesrunning[index].head = dyworksnode->next;
                        dyworksnode->next->prev = NULL;
                    }
                    else
                    {
                        dyworksnode->next->prev = dyworksnode->prev;
                        dyworksnode->prev->next = dyworksnode->next;
                    }
                }
                dyworksnode->next = NULL;
                dyworksnode->prev = NULL;

                m_dyworksopt[index].free(dyworksnode->data);
                rfree(dyworksnode);
            }
            else
            {
                /* 线程异常退出, ripple进程退出 */
                elog(RLOG_ERROR, "never come here, pthread:%lu, %s", dyworksnode->id, strerror(errno));
            }
        }
    }
}

/* 
 * 设置线程的退出信号
 */
void ripple_dyworks_setterm(void)
{
    int index = 0;
    ripple_dyworks_node* dyworksnode = NULL;

    /* 只关注 running 队列 */
    for(index = 0; index < RIPPLE_DYTHREAD_TYPE_MAX; index++)
    {
        if(NULL == m_dyworks->nodesrunning[index].head)
        {
            continue;
        }

        /* 遍历启动 */
        for(dyworksnode = m_dyworks->nodesrunning[index].head; NULL != dyworksnode; dyworksnode = dyworksnode->next)
        {
            if(RIPPLE_WORK_STATUS_EXIT == dyworksnode->status)
            {
                continue;
            }

            dyworksnode->status = RIPPLE_WORK_STATUS_TERM;
        }
    }
}

/*
 * 查看线程是否完全退出
*/
bool ripple_dyworks_canexit(void)
{
    int index = 0;

    if(NULL == m_dyworks)
    {
        return true;
    }

    /* 只关注 running 队列 */
    for(index = 0; index < RIPPLE_DYTHREAD_TYPE_MAX; index++)
    {
        if(NULL != m_dyworks->nodesrunning[index].head)
        {
            return false;
        }
    }

    return true;
}

/* 添加线程到待启动队列 */
bool ripple_dyworks_register(ripple_dythread_type type, void* privdata)
{
    /* 
     * 获取锁，并根据 type 的类型不同申请不同的空间
     */
    ripple_dyworks_node* dyworksnode = NULL;
    dyworksnode = (ripple_dyworks_node*)rmalloc0(sizeof(ripple_dyworks_node));
    if(NULL == dyworksnode)
    {
        return false;
    }
    rmemset0(dyworksnode, 0, '\0', sizeof(ripple_dyworks_node));
    dyworksnode->data = privdata;
    dyworksnode->next = NULL;
    dyworksnode->prev = NULL;
    dyworksnode->status = RIPPLE_WORK_STATUS_WAITSTART;

    /* 获取锁，并放入到待启动列表中 */
    ripple_thread_lock(&m_dyworks->nodeswait[type].lock);
    if(NULL == m_dyworks->nodeswait[type].head)
    {
        m_dyworks->nodeswait[type].head = dyworksnode;
    }
    else
    {
        dyworksnode->prev = m_dyworks->nodeswait[type].tail;
        dyworksnode->prev->next = dyworksnode;
    }
    m_dyworks->nodeswait[type].tail = dyworksnode;
    ripple_thread_unlock(&m_dyworks->nodeswait[type].lock);
    return true;
}

/*
 * 回收退出的线程
 *  1、查看是否有需要启动的线程
 *  2、查看是否有退出的线程
 */
void ripple_dyworks_trydestroy(void)
{
    /* 查看是否有待启动的线程 */
    if(false == g_gotsigterm)
    {
        ripple_dyworks_maystart();
    }

    /* 查看是否有退出的线程 */
    ripple_dyworks_maystop();
}

/* 初始化动态线程管理 */
void ripple_dyworks_init(void)
{
    int index = 0;
    /* 创建多个子线程工作 */
    m_dyworks = (ripple_dyworks*)rmalloc1(sizeof(ripple_dyworks));
    if(NULL == m_dyworks)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(m_dyworks, 0, '\0', sizeof(ripple_dyworks));

    /* 申请空间 */
    m_dyworks->nodesrunning = (ripple_dyworks_nodes*)rmalloc1((sizeof(ripple_dyworks_nodes) * RIPPLE_DYTHREAD_TYPE_MAX));
    if(NULL == m_dyworks->nodesrunning)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(m_dyworks->nodesrunning, 0, '\0', (sizeof(ripple_dyworks_nodes) * RIPPLE_DYTHREAD_TYPE_MAX));
    /* 内容初始化 */
    for(index = 0; index < RIPPLE_DYTHREAD_TYPE_MAX; index++)
    {
        m_dyworks->nodesrunning[index].head = NULL;
        m_dyworks->nodesrunning[index].tail = NULL;
        m_dyworks->nodesrunning[index].type = index;
    }

    /* 申请空间 */
    m_dyworks->nodeswait = (ripple_dyworks_nodes*)rmalloc1((sizeof(ripple_dyworks_nodes) * RIPPLE_DYTHREAD_TYPE_MAX));
    if(NULL == m_dyworks->nodeswait)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(m_dyworks->nodeswait, 0, '\0', (sizeof(ripple_dyworks_nodes) * RIPPLE_DYTHREAD_TYPE_MAX));

    /* 内容初始化 */
    for(index = 0; index < RIPPLE_DYTHREAD_TYPE_MAX; index++)
    {
        m_dyworks->nodeswait[index].head = NULL;
        m_dyworks->nodeswait[index].tail = NULL;
        m_dyworks->nodeswait[index].type = index;
        ripple_thread_mutex_init(&m_dyworks->nodeswait[index].lock, NULL);
    }
}

void ripple_dyworks_destroy(int status, void* args)
{
    if(NULL == m_dyworks)
    {
        return;
    }

    if(NULL != m_dyworks->nodeswait)
    {
        rfree(m_dyworks->nodeswait);
    }

    if(NULL != m_dyworks->nodesrunning)
    {
        rfree(m_dyworks->nodesrunning);
    }

    rfree(m_dyworks);
    m_dyworks = NULL;
}



