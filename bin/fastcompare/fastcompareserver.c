#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/path/ripple_path.h"
#include "utils/hash/hash_search.h"
#include "utils/daemon/ripple_process.h"
#include "signal/ripple_signal.h"
#include "works/dyworks/ripple_dyworks.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/ripple_netserver.h"
#include "fastcompare/ripple_fastcompare_tablecomparecatalog.h"
#include "fastcompare/ripple_fastcompare_tablescorrectmanager.h"

static void
help()
{
    printf("Usage:\n  fastcompareserver [OPTION]\n");
    printf("Options:\n");
    printf("  -f config.cfg        config file\n");
}

/*
 * 程序主入口
*/
int main(int argc, char **argv)
{
    int index = 0;
    const char*     loglevel    = NULL;
    ripple_fastcompare_tablescorrectmanager* corrmanager = NULL;

    if(1 < argc)
    {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
        {
            help();
            exit(0);
        }

        /* 检查个数 */
        if(3 != argc)
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

    /* 设置程序的类型 */
    g_proctype = RIPPLE_PROC_TYPE_FASTCMPSVR;

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

    /* 设置后台运行 */
    ripple_makedaemon();

    /* 
     * 启动工作线程
     */
    /* 设置信号处理函数 */
    ripple_signal_init();

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

    /* 解除信号屏蔽 */
    ripple_singal_setmask();
    
    /* 创建监听 */
    corrmanager = ripple_fastcompare_tablescorrectmanager_init();
    if(NULL == corrmanager)
    {
        elog(RLOG_ERROR, "table correct manager init error");
    }

    while(1)
    {
        if(true == g_gotsigterm)
        {
            /* 设置所有的动态线程状态为需要退出 */
            ripple_dyworks_setterm();
        }

        /* 回收动态线程的资源 */
        /* 线程回收处理 */
        ripple_dyworks_trydestroy();

        /* 如果是 sigterm 状态 */
        if(true == g_gotsigterm)
        {
            /* 退出 */
            if(false == ripple_dyworks_canexit())
            {
                continue;
            }
            usleep(50000);
            ripple_fastcompare_tablescorrectmanager_free(corrmanager);
            break;
        }

        /* 监听，接受连接，注册启动 */
        if(false == ripple_netserver_desc(&corrmanager->base))
        {
            /* 出错,那么退出 */
            g_gotsigterm = true;
            elog(RLOG_WARNING, "accept error");
        }
    }
    return 0;
}
