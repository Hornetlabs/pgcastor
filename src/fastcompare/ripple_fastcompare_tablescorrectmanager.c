#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "port/net/ripple_net.h"
#include "utils/mem/ripple_mem.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "queue/ripple_queue.h"
#include "task/ripple_task_slot.h"
#include "net/netmsg/ripple_netmsg.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netiomp/ripple_netiomp_poll.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netserver.h"
#include "net/ripple_netclient.h"
#include "works/dyworks/ripple_dyworks.h"
#include "fastcompare/ripple_fastcompare_chunk.h"
#include "fastcompare/ripple_fastcompare_tablecomparecatalog.h"
#include "fastcompare/ripple_fastcompare_simpledatachunk.h"
#include "fastcompare/ripple_fastcompare_datacmpresult.h"
#include "fastcompare/ripple_fastcompare_datacompare.h"
#include "fastcompare/ripple_fastcompare_beginchunk.h"
#include "fastcompare/ripple_fastcompare_beginslice.h"
#include "fastcompare/ripple_fastcompare_tableslicetask.h"
#include "fastcompare/ripple_fastcompare_tablecorrectslice.h"
#include "fastcompare/ripple_fastcompare_tablescorrectmanager.h"
#include "fastcompare/ripple_fastcompare_tableslicecorrectmanager.h"

/* 回调处理 */
static bool ripple_fastcompare_tablescorrectmanager_registertablecorrectslice(void* tablecorrect, rsocket sock)
{
    /* 初始化 TableSliceCorrectManager 并注册启动 */
    ripple_fastcompare_tablescorrectmanager* tablecorrectmanager = NULL;
    ripple_fastcompare_tableslicecorrectmanager* tableslicecorrmgr = NULL;

    /* TODO 注册启动 */
    tablecorrectmanager = (ripple_fastcompare_tablescorrectmanager*)tablecorrect;

    tableslicecorrmgr = riple_fastcompare_tableslicecorrectmanager_init();
    if(NULL == tableslicecorrmgr)
    {
        return false;
    }
    tableslicecorrmgr->base.fd = sock;
    tableslicecorrmgr->catalog = tablecorrectmanager->catalog;

    /* 注册动态线程 */
    if(false == ripple_dyworks_register(RIPPLE_DYTHREAD_TYPE_FASTCMPTABLESLICECORRECT, (void*)tableslicecorrmgr))
    {
        elog(RLOG_WARNING, "netclient->fd:%d", tableslicecorrmgr->base.fd);
        ripple_close(tableslicecorrmgr->base.fd);
        tableslicecorrmgr->base.fd = -1;
        elog(RLOG_WARNING, "register fast compare table slice correct error, %s", strerror(errno));
        return false;
    }

    return true;
}

/* 资源回收 */
void ripple_fastcompare_tablescorrectmanager_free(ripple_fastcompare_tablescorrectmanager* tablecorrect)
{
    if(NULL == tablecorrect)
    {
        return;
    }
    
    if (tablecorrect->catalog)
    {
        ripple_fastcompare_tablecomparecatalog_destroy(tablecorrect->catalog);
    }

    /* 资源回收 */
    ripple_netserver_free(&tablecorrect->base);

    rfree(tablecorrect);
}

/* 初始化并监听端口 */
ripple_fastcompare_tablescorrectmanager* ripple_fastcompare_tablescorrectmanager_init(void)
{
    int port = 7932;
    ripple_fastcompare_tablescorrectmanager* correctmanager = NULL;

    correctmanager = rmalloc0(sizeof(ripple_fastcompare_tablescorrectmanager));
    if (NULL == correctmanager)
    {
        elog(RLOG_WARNING, "out of memeory, %s", strerror(errno));
        return NULL;
    }

    /* 初始化网络信息 */
    if (false == ripple_netserver_reset(&correctmanager->base))
    {
        elog(RLOG_WARNING, "fast compare net server reset error");
        return NULL;
    }

    /* 获取配置项 */
    /* 设置端口   */
    port = guc_getConfigOptionInt(RIPPLE_CFG_KEY_PORT);
    ripple_netserver_port_set(&correctmanager->base, port);

    /* 设置系统字典缓存 */
    correctmanager->catalog = ripple_fastcompare_tablecomparecatalog_init();

    /* 设置host */
    ripple_netserver_host_set(&correctmanager->base, guc_getConfigOption(RIPPLE_CFG_KEY_HOST), RIPPLE_NETSERVER_HOSTTYPE_IP);

    /* 设置 type */
    ripple_netserver_type_set(&correctmanager->base, RIPPLE_NETSERVER_TYPE_FASTCMP);

    correctmanager->base.callback = ripple_fastcompare_tablescorrectmanager_registertablecorrectslice;
    if(false == ripple_netserver_create(&correctmanager->base))
    {
        /* 资源回收 */
        ripple_fastcompare_tablescorrectmanager_free(correctmanager);
        return NULL;
    }
    return correctmanager;
}
