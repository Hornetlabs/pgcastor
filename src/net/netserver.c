#include "app_incl.h"
#include "port/file/fd.h"
#include "port/net/net.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "net/netiomp/netiomp.h"
#include "net/netiomp/netiomp_poll.h"
#include "net/netpacket/netpacket.h"
#include "net/netserver.h"

/* 初始设置 */
bool netserver_reset(netserver* netserver)
{
    int index = 0;
    netserver->type = NETSERVER_TYPE_NOP;
    netserver->fdcnt = 0;
    netserver->fdmax = NETSERVER_DEFAULTSOCKSIZE;

    /* 在配置文件中获取 keepalive 和 usertimeout 的配置信息 */
    netserver->keepalive         = guc_getConfigOptionInt(CFG_KEY_TCP_KEEPALIVE);
    netserver->keepaliveidle     = guc_getConfigOptionInt(CFG_KEY_TCP_KEEPALIVES_IDLE);
    netserver->keepaliveinterval = guc_getConfigOptionInt(CFG_KEY_TCP_KEEPALIVES_INTERVAL);
    netserver->keepalivecount    = guc_getConfigOptionInt(CFG_KEY_TCP_KEEPALIVES_COUNT);
    netserver->usertimeout       = guc_getConfigOptionInt(CFG_KEY_TCP_USER_TIMEOUT);

    if (NULL == netserver->fd)
    {
        netserver->fd = rmalloc0(sizeof(rsocket)*netserver->fdmax);
        if (NULL == netserver->fd)
        {
            elog(RLOG_ERROR, "net server reset out of memory");
            return false;
        }
        rmemset0(netserver->fd, 0, '\0', sizeof(rsocket)*netserver->fdmax);

        netserver->pos = rmalloc0(sizeof(int)*netserver->fdmax);
        if (NULL == netserver->pos)
        {
            elog(RLOG_ERROR, "net server reset out of memory");
            return false;
        }
        rmemset0(netserver->pos, 0, '\0', sizeof(int)*netserver->fdmax);

        for (index = 0; index < netserver->fdmax; index++)
        {
            netserver->fd[index] = -1;
            netserver->pos[index] = -1;
        }
    }

    for (index = 0; index < netserver->fdmax; index++)
    {
        if ( -1 != netserver->fd[index])
        {
            osal_close(netserver->fd[index]);
            netserver->fd[index] = -1;
        }
        netserver->pos[index] = -1;
    }

    netserver->ops = netiomp_init(NETIOMP_TYPE_POLL);
    /* 申请 base 信息，用于后续的描述符处理 */
    if(false == netserver->ops->create(&netserver->base))
    {
        elog(RLOG_ERROR, "net server reset NETIOMP_TYPE_POLL create error");
        return false;
    }

    netserver->ops->reset(netserver->base);
    netserver->base->timeout = NET_POLLTIMEOUT;
    netserver->callback = NULL;
    return true;
}

/* 设置netserver svrhost */
bool netserver_host_set(netserver* netserver, char* host, netserver_hosttype hosttype)
{
    netserver_host* nsvrhost = NULL;
    if (NULL == netserver)
    {
        return false;
    }

    if (NULL == host)
    {
        host = "127.0.0.1";
    }

    nsvrhost = rmalloc0(sizeof(netserver_host));
    if (NULL == nsvrhost)
    {
        elog(RLOG_WARNING, "netserver set host out of memory");
        return false;
    }
    rmemset0(nsvrhost, 0, '\0', sizeof(netserver_host));
    nsvrhost->type = hosttype;
    rmemcpy1(nsvrhost->host, 0, host, strlen(host));

    netserver->hosts = dlist_put(netserver->hosts, nsvrhost);
    if (NULL == netserver->hosts)
    {
        elog(RLOG_WARNING, "netserver set host 2 dlist error");
        return false;
    }

    return true;
}

/* 设置netserver svrport */
void netserver_port_set(netserver* netserver, int port)
{
    if (NULL == netserver)
    {
        return;
    }

    rmemset1(netserver->port, 0, '\0', 128);
    snprintf(netserver->port, 128, "%d", port);
    return;
}

/* 设置类型 */
void netserver_type_set(netserver* netserver, int type)
{
    if (NULL == netserver)
    {
        return;
    }
    
    netserver->type = type;
    return;
}

/* 创建server端 */
bool netserver_create(netserver* netserver)
{
    bool bret = false;
    int yes = 1;
    int iret = 0;
    int fd = 0;
    int domain = 0;
    int bindlen = 0;
    dlistnode* dlnode = NULL;
    netserver_host* nsvrhost = NULL;
    struct sockaddr* bindaddr = NULL;
    struct sockaddr_un addrun;
    struct sockaddr_in addr;
    char unixpath[NETSERVER_HOSTMAXLEN] = { 0 };

    if(NULL == netserver)
    {
        return false;
    }

    for (dlnode = netserver->hosts->head; NULL != dlnode; dlnode = dlnode->next)
    {
        domain = AF_INET;
        nsvrhost = (netserver_host*)dlnode->value;
        if (NETSERVER_HOSTTYPE_IP == nsvrhost->type)
        {
            /* 创建服务端监听 */
            domain = AF_INET;
            /* 获取host对应的地址 */
            rmemset1(&addr, 0, 0, sizeof(struct sockaddr_in));
            bret = osal_host2sockaddr(&addr,
                                        nsvrhost->host,
                                        netserver->port,
                                        domain,
                                        SOCK_STREAM,
                                        IPPROTO_TCP,
                                        1);
            if(false == bret)
            {
                elog(RLOG_WARNING, "can not get addr info, %s", strerror(errno));
                return false;
            }
            bindaddr = (struct sockaddr*)&addr;
            bindlen = sizeof(struct sockaddr_in);
            elog(RLOG_INFO, "host %s.%s listend", nsvrhost->host, netserver->port);
        }
        else if (NETSERVER_HOSTTYPE_UNIXDOMAIN == nsvrhost->type)
        {
            domain = AF_LOCAL;
            rmemset1(&addrun, 0, '\0', sizeof(struct sockaddr_un));
            snprintf(unixpath, NETSERVER_HOSTMAXLEN, "%s%s", nsvrhost->host, netserver->port);

            if (strlen(unixpath) > sizeof(addrun.sun_path))
            {
                elog(RLOG_WARNING, "unix domain path too long");
                return false;
            }
            osal_durable_unlink(unixpath, RLOG_INFO);

            rmemcpy1(addrun.sun_path, 0, unixpath, strlen(unixpath));
            addrun.sun_family = domain;
            bindaddr = (struct sockaddr*)&addrun;
            bindlen = sizeof(struct sockaddr_un);
            elog(RLOG_INFO, "use %s as unix domain", addrun.sun_path);
        }

        /* 创建描述符 */
        fd = osal_socket(domain, SOCK_STREAM, 0);
        if(-1 == fd)
        {
            elog(RLOG_WARNING, "call socket error, %s", strerror(errno));
            return false;
        }

        /* 禁用 TCP_NODELAY */
        osal_setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(int));

        /* 设置为 非阻塞模式 */
        osal_setunblock(fd);

        /* 设置可以使用 TIME_WAIT 状态的描述符 */
        osal_setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));

        /* 将 socket 与 地址绑定 */
        iret = osal_bind(fd, bindaddr, bindlen);
        if(0 != iret)
        {
            osal_close(fd);
            elog(RLOG_WARNING, "rbind error, %s", strerror(errno));
            return false;
        }

        /* 启动监听 */
        iret = osal_listen(fd, 10000);
        if(0 != iret)
        {
            osal_close(fd);
            elog(RLOG_WARNING, "osal_listen error, %s", strerror(errno));
            return false;
        }

        netserver->fd[netserver->fdcnt] = fd;
        netserver->fdcnt++;
    }

    return true;
}

/* 创建事件并接收描述符,等待触发后调用回掉函数处理 */
bool netserver_desc(netserver* netserver)
{
    int yes             = 1;
    int iret            = 0;
    int index           = 0;
    int event           = 0;
    int nsock           = -1;
    socklen_t           addrlen;
    struct sockaddr     addr;

    if(NULL == netserver || NULL == netserver->fd)
    {
        return false;
    }

    /* 查看是否有新连接，若有新连接，那么注册新的接收线程处理接收 */
    netserver->ops->reset(netserver->base);

    for (index = 0; index < netserver->fdcnt; index++)
    {
        netserver->pos[index] = netserver->ops->add(netserver->base, netserver->fd[index], POLLIN);
    }

    /* 调用iomp端口 */
    iret = netserver->ops->iomp(netserver->base);
    if(-1 == iret)
    {
        /* 查看错误是否为信号引起的，若为信号引起那么继续监测 */
        if(errno == EINTR)
        {
            return true;
        }
        elog(RLOG_WARNING, "iomp error, %s", strerror(errno));
        return false;
    }

    /* 超时了, 那么继续 */
    if(0 == iret)
    {
        return true;
    }

    /* 有消息触发，那么看看触发的事件类型 */
    for (index = 0; index < netserver->fdcnt; index++)
    {
        event = netserver->ops->getevent(netserver->base, netserver->pos[index]);

        /*
         * 检测事件类型，当为 POLLUP 或者 POLLERROR 时，那么说明出现了错误，退出
         */
        if (0 == event)
        {
            /* 没有事件触发 */
            continue;
        }
        if (POLLHUP == (event&POLLHUP)
            || POLLERR == (event&POLLERR))
        {
            elog(RLOG_WARNING, "iomp error, %s", strerror(errno));
            return false;
        }
        else if(POLLIN != (event&POLLIN))
        {
            elog(RLOG_WARNING, "unexpect iomp error, %s", strerror(errno));
            return false;
        }

        /* 有新的链接, 接收连接并调用回掉函数处理 */
        addrlen = sizeof(struct sockaddr_in);
        nsock =  osal_accept(netserver->fd[index], &addr, &addrlen);
        if(-1 == nsock)
        {
            elog(RLOG_WARNING, "osal_accept error, %s", strerror(errno));
            return false;
        }
        elog(RLOG_INFO, "nsock:%d, %s", nsock, strerror(errno));

        /* 设置新连接的参数 */
        /* 禁用 TCP_NODELAY */
        osal_setsockopt(nsock, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(int));

        /* 设置为 非阻塞模式 */
        osal_setunblock(nsock);

        if(0 == netserver->keepalive)
        {
            /* 调用回调函数处理 */
            return netserver->callback(netserver, nsock);
        }

        osal_setsockopt(nsock,
                        SOL_SOCKET, SO_KEEPALIVE,
                        (char *) &netserver->keepalive, sizeof(netserver->keepalive));

        osal_setsockopt(nsock,
                        IPPROTO_TCP, TCP_KEEPIDLE,
                        (char *) &netserver->keepaliveidle, sizeof(netserver->keepaliveidle));

        osal_setsockopt(nsock, IPPROTO_TCP, TCP_KEEPINTVL,
                        (char *) &netserver->keepaliveinterval, sizeof(netserver->keepaliveinterval));

        osal_setsockopt(nsock,
                        IPPROTO_TCP, TCP_KEEPCNT,
                        (char *) &netserver->keepalivecount, sizeof(netserver->keepalivecount));

        osal_setsockopt(nsock,
                        IPPROTO_TCP, TCP_USER_TIMEOUT,
                        (char *) &netserver->usertimeout, sizeof(netserver->usertimeout));

        if (false == netserver->callback(netserver, nsock))
        {
            return false;
        }
    }

    return true;
}

/* 资源回收 */
void netserver_free(netserver* netserver)
{
    int index = 0;
    if(NULL == netserver)
    {
        return;
    }

    if (NULL != netserver->fd)
    {
        for (index = 0; index < netserver->fdcnt; index++)
        {
            if(-1 != netserver->fd[index])
            {
                elog(RLOG_DEBUG, "netsvr osal_listen sock:%d", netserver->fd[index]);
                osal_close(netserver->fd[index]);
                netserver->fd[index] = -1;
            }
        }

        rfree(netserver->fd);
        netserver->fd = NULL;
    }

    dlist_free(netserver->hosts, free);
    if (NULL != netserver->pos)
    {
        rfree(netserver->pos);
        netserver->pos = NULL;
    }

    if(NULL != netserver->base)
    {
        netserver->ops->free(netserver->base);
        netserver->base = NULL;
    }

    /* 注意, 没有释放 netserver 本身 */
}

