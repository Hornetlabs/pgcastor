#include "app_incl.h"
#include "port/file/fd.h"
#include "port/net/net.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "net/netiomp/netiomp.h"
#include "net/netiomp/netiomp_poll.h"
#include "net/netpacket/netpacket.h"
#include "net/netserver.h"

/* Initial settings */
bool netserver_reset(netserver* netserver)
{
    int index = 0;
    netserver->type = NETSERVER_TYPE_NOP;
    netserver->fdcnt = 0;
    netserver->fdmax = NETSERVER_DEFAULTSOCKSIZE;

    /* Get keepalive and usertimeout configuration from config file */
    netserver->keepalive = guc_getConfigOptionInt(CFG_KEY_TCP_KEEPALIVE);
    netserver->keepaliveidle = guc_getConfigOptionInt(CFG_KEY_TCP_KEEPALIVES_IDLE);
    netserver->keepaliveinterval = guc_getConfigOptionInt(CFG_KEY_TCP_KEEPALIVES_INTERVAL);
    netserver->keepalivecount = guc_getConfigOptionInt(CFG_KEY_TCP_KEEPALIVES_COUNT);
    netserver->usertimeout = guc_getConfigOptionInt(CFG_KEY_TCP_USER_TIMEOUT);

    if (NULL == netserver->fd)
    {
        netserver->fd = rmalloc0(sizeof(rsocket) * netserver->fdmax);
        if (NULL == netserver->fd)
        {
            elog(RLOG_ERROR, "net server reset out of memory");
            return false;
        }
        rmemset0(netserver->fd, 0, '\0', sizeof(rsocket) * netserver->fdmax);

        netserver->pos = rmalloc0(sizeof(int) * netserver->fdmax);
        if (NULL == netserver->pos)
        {
            elog(RLOG_ERROR, "net server reset out of memory");
            return false;
        }
        rmemset0(netserver->pos, 0, '\0', sizeof(int) * netserver->fdmax);

        for (index = 0; index < netserver->fdmax; index++)
        {
            netserver->fd[index] = -1;
            netserver->pos[index] = -1;
        }
    }

    for (index = 0; index < netserver->fdmax; index++)
    {
        if (-1 != netserver->fd[index])
        {
            osal_close(netserver->fd[index]);
            netserver->fd[index] = -1;
        }
        netserver->pos[index] = -1;
    }

    netserver->ops = netiomp_init(NETIOMP_TYPE_POLL);
    /* Apply for base information for subsequent descriptor processing */
    if (false == netserver->ops->create(&netserver->base))
    {
        elog(RLOG_ERROR, "net server reset NETIOMP_TYPE_POLL create error");
        return false;
    }

    netserver->ops->reset(netserver->base);
    netserver->base->timeout = NET_POLLTIMEOUT;
    netserver->callback = NULL;
    return true;
}

/* Set netserver svrhost */
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

/* Set netserver svrport */
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

/* Set type */
void netserver_type_set(netserver* netserver, int type)
{
    if (NULL == netserver)
    {
        return;
    }

    netserver->type = type;
    return;
}

/* Create server side */
bool netserver_create(netserver* netserver)
{
    bool               bret = false;
    int                yes = 1;
    int                iret = 0;
    int                fd = 0;
    int                domain = 0;
    int                bindlen = 0;
    dlistnode*         dlnode = NULL;
    netserver_host*    nsvrhost = NULL;
    struct sockaddr*   bindaddr = NULL;
    struct sockaddr_un addrun;
    struct sockaddr_in addr;
    char               unixpath[NETSERVER_HOSTMAXLEN] = {0};

    if (NULL == netserver)
    {
        return false;
    }

    for (dlnode = netserver->hosts->head; NULL != dlnode; dlnode = dlnode->next)
    {
        domain = AF_INET;
        nsvrhost = (netserver_host*)dlnode->value;
        if (NETSERVER_HOSTTYPE_IP == nsvrhost->type)
        {
            /* Create server listen */
            domain = AF_INET;
            /* Get address corresponding to host */
            rmemset1(&addr, 0, 0, sizeof(struct sockaddr_in));
            bret = osal_host2sockaddr(&addr, nsvrhost->host, netserver->port, domain, SOCK_STREAM, IPPROTO_TCP, 1);
            if (false == bret)
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

        /* Create descriptor */
        fd = osal_socket(domain, SOCK_STREAM, 0);
        if (-1 == fd)
        {
            elog(RLOG_WARNING, "call socket error, %s", strerror(errno));
            return false;
        }

        /* Disable TCP_NODELAY */
        osal_setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(int));

        /* Set to non-blocking mode */
        osal_setunblock(fd);

        /* Set descriptor that can use TIME_WAIT state */
        osal_setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));

        /* Bind socket to address */
        iret = osal_bind(fd, bindaddr, bindlen);
        if (0 != iret)
        {
            osal_close(fd);
            elog(RLOG_WARNING, "rbind error, %s", strerror(errno));
            return false;
        }

        /* Start listening */
        iret = osal_listen(fd, 10000);
        if (0 != iret)
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

/* Create event and receive descriptor, call callback function after trigger */
bool netserver_desc(netserver* netserver)
{
    int             yes = 1;
    int             iret = 0;
    int             index = 0;
    int             event = 0;
    int             nsock = -1;
    socklen_t       addrlen;
    struct sockaddr addr;

    if (NULL == netserver || NULL == netserver->fd)
    {
        return false;
    }

    /* Check if there is new connection, if so register new receive thread to handle */
    netserver->ops->reset(netserver->base);

    for (index = 0; index < netserver->fdcnt; index++)
    {
        netserver->pos[index] = netserver->ops->add(netserver->base, netserver->fd[index], POLLIN);
    }

    /* Call iomp port */
    iret = netserver->ops->iomp(netserver->base);
    if (-1 == iret)
    {
        /* Check if error is caused by signal, if so continue monitoring */
        if (errno == EINTR)
        {
            return true;
        }
        elog(RLOG_WARNING, "iomp error, %s", strerror(errno));
        return false;
    }

    /* Timeout, continue */
    if (0 == iret)
    {
        return true;
    }

    /* Message triggered, check triggered event type */
    for (index = 0; index < netserver->fdcnt; index++)
    {
        event = netserver->ops->getevent(netserver->base, netserver->pos[index]);

        /*
         * Check event type, when POLLUP or POLLERROR, it means error occurred, exit
         */
        if (0 == event)
        {
            /* No event triggered */
            continue;
        }
        if (POLLHUP == (event & POLLHUP) || POLLERR == (event & POLLERR))
        {
            elog(RLOG_WARNING, "iomp error, %s", strerror(errno));
            return false;
        }
        else if (POLLIN != (event & POLLIN))
        {
            elog(RLOG_WARNING, "unexpect iomp error, %s", strerror(errno));
            return false;
        }

        /* New connection, accept connection and call callback function */
        addrlen = sizeof(struct sockaddr_in);
        nsock = osal_accept(netserver->fd[index], &addr, &addrlen);
        if (-1 == nsock)
        {
            elog(RLOG_WARNING, "osal_accept error, %s", strerror(errno));
            return false;
        }
        elog(RLOG_INFO, "nsock:%d, %s", nsock, strerror(errno));

        /* Set new connection parameters */
        /* Disable TCP_NODELAY */
        osal_setsockopt(nsock, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(int));

        /* Set to non-blocking mode */
        osal_setunblock(nsock);

        if (0 == netserver->keepalive)
        {
            /* Call callback function to handle */
            return netserver->callback(netserver, nsock);
        }

        osal_setsockopt(nsock, SOL_SOCKET, SO_KEEPALIVE, (char*)&netserver->keepalive, sizeof(netserver->keepalive));

        osal_setsockopt(nsock,
                        IPPROTO_TCP,
                        TCP_KEEPIDLE,
                        (char*)&netserver->keepaliveidle,
                        sizeof(netserver->keepaliveidle));

        osal_setsockopt(nsock,
                        IPPROTO_TCP,
                        TCP_KEEPINTVL,
                        (char*)&netserver->keepaliveinterval,
                        sizeof(netserver->keepaliveinterval));

        osal_setsockopt(nsock,
                        IPPROTO_TCP,
                        TCP_KEEPCNT,
                        (char*)&netserver->keepalivecount,
                        sizeof(netserver->keepalivecount));

        osal_setsockopt(nsock,
                        IPPROTO_TCP,
                        TCP_USER_TIMEOUT,
                        (char*)&netserver->usertimeout,
                        sizeof(netserver->usertimeout));

        if (false == netserver->callback(netserver, nsock))
        {
            return false;
        }
    }

    return true;
}

/* Resource cleanup */
void netserver_free(netserver* netserver)
{
    int index = 0;
    if (NULL == netserver)
    {
        return;
    }

    if (NULL != netserver->fd)
    {
        for (index = 0; index < netserver->fdcnt; index++)
        {
            if (-1 != netserver->fd[index])
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

    if (NULL != netserver->base)
    {
        netserver->ops->free(netserver->base);
        netserver->base = NULL;
    }

    /* Note: netserver itself is not freed */
}
