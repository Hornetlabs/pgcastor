#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/string/stringinfo.h"
#include "utils/path/ripple_path.h"
#include "queue/ripple_queue.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netiomp/ripple_netiomp_poll.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netserver.h"
#include "net/ripple_netclient.h"
#include "task/ripple_task_slot.h"
#include "works/dyworks/ripple_dyworks.h"
#include "fastcompare/ripple_fastcompare_chunk.h"
#include "fastcompare/ripple_fastcompare_tablecomparecatalog.h"
#include "fastcompare/ripple_fastcompare_tablesmanager.h"
#include "fastcompare/ripple_fastcompare_tableslicetask.h"

static void
help()
{
    printf("Usage:\n  fastcompareclient [OPTION]\n");
    printf("Options:\n");
    printf("  -f config.cfg        config file\n");
    printf("  \"schema.table schema.table\"   \n");
}

static void ripple_fastcompare_slots_init(ripple_fastcompare_tablesmanger* mgr)
{
    int index_slots = 0;
    ripple_task_slots *slots = ripple_taskslots_init();

    mgr->slots = slots;
    slots->cnt = mgr->parallelcnt;
    slots->task_slots = ripple_taskslot_init(slots->cnt);

    for (index_slots = 0; index_slots < slots->cnt; index_slots++)
    {
        ripple_task_slot *slot = &slots->task_slots[index_slots];
        ripple_fastcompare_tableslicetask *shard = NULL;

        /* 分配空间和初始化 */
        shard = ripple_fastcompare_tableslicetask_init();

        shard->catalog = mgr->catalog;
        shard->slicequeue = mgr->queue;

        slot->stat = RIPPLE_TASKSLOT_INIT;
        slot->task = (ripple_task *)shard;
    }
}

/* fastcompare client 处理 */
int main(int argc, char** argv)
{
    int index = 0;
    bool gen_queue = false;

    List* cmptables = NULL;
    const char*     loglevel    = NULL;
    ripple_fastcompare_tablesmanger* tabelsmanger = NULL;

    if(1 < argc)
    {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
        {
            help();
            exit(0);
        }

        /* 检查个数 */
        if(4 > argc)
        {
            help();
            exit(0);
        }
        
        if(0 != strcmp(argv[1], "-f"))
        {
            help();
            exit(0);
        }
    }
    else
    {
        help();
        exit(0);
    }

    if (argc > 3)
    {
        int index_guc = 0;
        for (index_guc = 3; index_guc < argc; index_guc++)
        {
            cmptables = lappend(cmptables, rstrdup(argv[index_guc]));
        }
    }

    /* 设置程序的类型 */
    g_proctype = RIPPLE_PROC_TYPE_FASTCMPCLIENT;

    /* 获取绝对路径 */
    rmemcpy1(g_cfgpath, 0, argv[2], strlen(argv[2]));
    ripple_path_canonicalize_path(g_cfgpath);
    for(index = strlen(g_cfgpath); index > 0; index--)
    {
        if(g_cfgpath[index - 1] != '/')
        {
            g_cfgpath[index - 1] = '\0';
            continue;
        }
        break;
    }

    /* 参数解析 */
    guc_loadcfg(argv[2], false);

    /* 查看解析内容是否正确 */
    guc_debug();

    /* 设置 日志级别 */
    loglevel = guc_getConfigOption(RIPPLE_CFG_KEY_LOG_LEVEL);
    if(NULL == loglevel)
    {
        elog(RLOG_ERROR, "unrecognized configuration parameter:%s", loglevel);
    }

    elog_seteloglevel(loglevel);

    /* 获取主线程号 */
    g_mainthrid = pthread_self();

    /* 初始化 manger 管理函数 */
    /* 动态线程初始化 */
    ripple_dyworks_init();

    /* 初始化管理结构 */
    tabelsmanger = ripple_fastcompare_tablesmanger_init();
    if(NULL == tabelsmanger)
    {
        elog(RLOG_ERROR, "table manager init error");
    }

    /* 加载待比较表 */
    if (!ripple_fastcompare_tablesmanger_load_compare_tables(tabelsmanger, cmptables))
    {
        elog(RLOG_INFO, "don't have compare tables, exit");
        return 0;
    }

    /* 根据内核和并行 */
    /* 获取并行数 */
    tabelsmanger->parallelcnt = guc_getConfigOptionInt(RIPPLE_CFG_KEY_FASTCMP_MAX_PARALLEL_WORKS);

    /* 加载系统表 */
    tabelsmanger->catalog = ripple_fastcompare_tablecomparecatalog_init();

    /* 初始化队列 */
    tabelsmanger->queue = ripple_queue_init();

    /* 初始化slots */
    ripple_fastcompare_slots_init(tabelsmanger);

    while (true)
    {
        bool still_working = false;
        int index_slots = 0;

        /* 首先判断是否接收到退出信号 */
        if(true == g_gotsigterm)
        {
            break;
        }

        /* 检查存量子线程状态 */
        for (index_slots = 0; index_slots < tabelsmanger->slots->cnt; index_slots++)
        {
            ripple_task_slot *temp_slot = &tabelsmanger->slots->task_slots[index_slots];
            if (temp_slot->stat == RIPPLE_TASKSLOT_WORK)
            {
                still_working = true;
            }
            else if (temp_slot->stat == RIPPLE_TASKSLOT_IDLE)
            {
                still_working = true;
                if (ripple_queue_isnull(tabelsmanger->queue))
                {
                    ripple_taskslot_stat_setterm(temp_slot);
                }
            }
            else if (temp_slot->stat == RIPPLE_TASKSLOT_INIT)
            {
                still_working = true;
                temp_slot->stat = RIPPLE_TASKSLOT_IDLE;
                if (!ripple_dyworks_register(RIPPLE_DYTHREAD_TYPE_FASTCMPTABLESLICE, (void *)temp_slot))
                {
                    elog(RLOG_WARNING, "start refresh worker failed");
                }
            }
            else if (temp_slot->stat == RIPPLE_TASKSLOT_TERM)
            {
                still_working = true;
            }

            /* 只有所有work线程状态为exit时mgr才退出 */
        }

        /* 首先判断是否存在任务和待同步表 */
        if (gen_queue && ripple_queue_isnull(tabelsmanger->queue))
        {
            /* 检查work线程状态 */
            if (!still_working)
            {
                /* 全部线程结束, 退出 */
                break;
            }
        }

        /* 线程处理 */
        ripple_dyworks_trydestroy();

        if (!gen_queue)
        {
            /* 按照表生成队列 */
            ripple_fastcompare_tablesmanager_slice_table(tabelsmanger);
            gen_queue = true;
        }

        usleep(50000);
    }

    return 0;
}

