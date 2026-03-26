#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <memory.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <netinet/tcp.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netdb.h>
#include <poll.h>
#include <fcntl.h>

#include "app_c.h"
#include "config.h"
#include "xsynch_exbufferdata.h"
#include "xsynch_fe.h"
#include "xsynch_int.h"
#include "xsynch_fenet.h"

static bool xsynch_fenet_desc(int sock, uint16 flag, short int* prevent, int* perror)
{
    int           pos = 0;
    int           iret = 0;
    int           timeout = 10000;
    struct pollfd events[2];

    while (1)
    {
        pos = 0;
        memset(events, '\0', sizeof(struct pollfd) * 2);
        events[pos].fd = sock;
        events[pos].revents = 0;
        events[pos].events |= flag;

        iret = poll(events, 1, timeout);
        if (-1 == iret)
        {
            /* check if error is caused by signal, if so continue monitoring */
            if (errno == EINTR)
            {
                continue;
            }

            *perror = 1;
            return false;
        }

        /* check status, see if connection is normal */
        if (0 == iret)
        {
            continue;
        }

        *prevent = events[pos].revents;
        break;
    }

    return true;
}

/* check connection status */
bool xsynch_fenet_isconn(xsynch_conn* conn)
{
    uint16    flag = 0;
    short int revent = 0;
    int       rerror = 0;

    if (XSYNCHCONN_STATUS_NOP == conn->connstatus)
    {
        return false;
    }

    flag = POLLOUT;
    if (false == xsynch_fenet_desc(conn->sock, flag, &revent, &rerror))
    {
        close(conn->sock);
        conn->connstatus = XSYNCHCONN_STATUS_NOP;
        conn->sock = -1;

        if (1 == rerror)
        {
            xsynch_exbufferdata_append(conn->errmsg, "%s", "can not get sock status.");
        }
        else if (2 == rerror)
        {
            xsynch_exbufferdata_append(conn->errmsg, "%s", "xmanager disconnected.");
        }
        return false;
    }

    if (POLLHUP == (revent & POLLHUP) || POLLERR == (revent & POLLERR))
    {
        close(conn->sock);
        conn->sock = -1;
        conn->connstatus = XSYNCHCONN_STATUS_NOP;
        return false;
    }

    conn->connstatus = XSYNCHCONN_STATUS_OK;
    return true;
}

/* get available address */
static int xsynch_getaddrinfo(const char*            node,
                              const char*            service,
                              const struct addrinfo* hints,
                              struct addrinfo**      res)
{
    if (NULL != node && node[0] == '*')
    {
        node = NULL;
    }
    else if (NULL == node || '\0' == node[0])
    {
        node = NULL;
    }

    if (NULL != service && service[0] == '*')
    {
        service = NULL;
    }
    else if (NULL == service || '\0' == service[0])
    {
        service = NULL;
    }

    *res = NULL;
    return getaddrinfo(node, service, hints, res);
}

/*
 * get address by name or ip address
 * */
static bool xsynch_host2sockaddr(struct sockaddr_in* addr,
                                 const char*         host,
                                 const char*         service,
                                 int                 family,
                                 int                 socktype,
                                 int                 protocol,
                                 int                 passive)
{
    int              ret = 0;
    struct addrinfo  hints;
    struct addrinfo *res, *reshead;
    memset(&hints, 0, sizeof(struct addrinfo));
    res = NULL;
    reshead = NULL;

    hints.ai_family = family;
    hints.ai_protocol = protocol;
    hints.ai_socktype = socktype;
    if (passive)
    {
        hints.ai_flags |= AI_PASSIVE;
    }

    ret = xsynch_getaddrinfo(host, service, &hints, &reshead);
    if (0 != ret)
    {
        return false;
    }

    for (res = reshead; res; res = res->ai_next)
    {
        if (family == res->ai_family && socktype == res->ai_socktype &&
            sizeof(struct sockaddr_in) == res->ai_addrlen)
        {
            *addr = *((struct sockaddr_in*)(res->ai_addr));
            freeaddrinfo(reshead);
            return true;
        }
    }

    freeaddrinfo(reshead);
    return false;
}

/* set to non-blocking mode */
static bool xsynch_setunblock(int sockfd)
{
    /*
     * 1. get descriptor status
     * 2. set to unblock
     */
    int flags;

    /* get status */
    flags = fcntl(sockfd, F_GETFL);
    if (0 > flags)
    {
        return false;
    }

    flags &= (~O_NONBLOCK);
    if (-1 == fcntl(sockfd, F_SETFL, flags))
    {
        return false;
    }

    return true;
}

/* connect to xmanager */
bool xsynch_fenet_conn(xsynch_conn* conn)
{
    int                yes = 1;
    int                addrlen = 0;
    int                domain = AF_INET;
    struct sockaddr*   connaddr = NULL;
    struct sockaddr_in addrin;
    struct sockaddr_un addrun;
    char               unixdoamin[512] = {0};

    conn->errcode = 0;
    if (XSYNCH_SOCKTYPE_TCP == conn->socktype)
    {
        domain = AF_INET;
        if (false == xsynch_host2sockaddr(
                         &addrin, conn->host, conn->port, domain, SOCK_STREAM, IPPROTO_TCP, 1))
        {
            conn->errcode = 1;
            xsynch_exbufferdata_reset(conn->errmsg);
            xsynch_exbufferdata_append(conn->errmsg, "%s", "get server addr error.");
            return false;
        }

        connaddr = (struct sockaddr*)&addrin;
        addrlen = sizeof(struct sockaddr_in);
    }
    else if (XSYNCH_SOCKTYPE_UNIXDOMAIN == conn->socktype)
    {
        domain = AF_LOCAL;
        snprintf(unixdoamin,
                 512,
                 "%s/%s%s",
                 RMANAGER_UNIXDOMAINDIR,
                 RMANAGER_UNIXDOMAINPREFIX,
                 conn->port);
        if (sizeof(addrun.sun_path) <= strlen(unixdoamin))
        {
            conn->errcode = 1;
            xsynch_exbufferdata_reset(conn->errmsg);
            xsynch_exbufferdata_append(conn->errmsg, "%s", "unix domain dir too long.");
            return false;
        }
        memset(addrun.sun_path, '\0', sizeof(addrun.sun_path));
        memcpy(addrun.sun_path, unixdoamin, strlen(unixdoamin));
        addrun.sun_family = domain;
        connaddr = (struct sockaddr*)&addrun;
        addrlen = sizeof(struct sockaddr_un);
    }

    conn->sock = socket(domain, SOCK_STREAM, 0);
    if (-1 == conn->sock)
    {
        conn->errcode = 1;
        xsynch_exbufferdata_reset(conn->errmsg);
        xsynch_exbufferdata_append(conn->errmsg, "%s", "unix domain dir too long.");
        return false;
    }

    /* set to non-blocking mode */
    xsynch_setunblock(conn->sock);
    if (XSYNCH_SOCKTYPE_TCP == conn->socktype)
    {
        setsockopt(conn->sock, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

        if (1 == conn->keepalive)
        {
            setsockopt(conn->sock,
                       SOL_SOCKET,
                       SO_KEEPALIVE,
                       (char*)&conn->keepalive,
                       sizeof(conn->keepalive));

            setsockopt(conn->sock,
                       IPPROTO_TCP,
                       TCP_KEEPIDLE,
                       (char*)&conn->keepaliveidle,
                       sizeof(conn->keepaliveidle));

            setsockopt(conn->sock,
                       IPPROTO_TCP,
                       TCP_KEEPINTVL,
                       (char*)&conn->keepaliveinterval,
                       sizeof(conn->keepaliveinterval));

            setsockopt(conn->sock,
                       IPPROTO_TCP,
                       TCP_KEEPCNT,
                       (char*)&conn->keepalivecount,
                       sizeof(conn->keepalivecount));

            setsockopt(conn->sock,
                       IPPROTO_TCP,
                       TCP_USER_TIMEOUT,
                       (char*)&conn->usertimeout,
                       sizeof(conn->usertimeout));
        }
    }

    /* connect */
    if (-1 == connect(conn->sock, connaddr, addrlen))
    {
        if (errno != EINPROGRESS)
        {
            conn->errcode = 1;
            xsynch_exbufferdata_reset(conn->errmsg);
            xsynch_exbufferdata_append(conn->errmsg, "%s", "conn server error.");
            close(conn->sock);
            conn->sock = -1;
            return false;
        }
        conn->connstatus = XSYNCHCONN_STATUS_INPROCESS;
    }

    if (XSYNCHCONN_STATUS_INPROCESS == conn->connstatus)
    {
        if (false == xsynch_fenet_isconn(conn))
        {
            conn->errcode = 1;
            xsynch_exbufferdata_reset(conn->errmsg);
            xsynch_exbufferdata_append(conn->errmsg, "%s", "conn server error.");
            return false;
        }
    }

    conn->connstatus = XSYNCHCONN_STATUS_OK;
    return true;
}

/* send data and get return result */
bool xsynch_fenet_sendcmd(xsynch_conn* conn)
{
    bool      bhdr = 0;
    uint16    flag = 0;
    short int revent = 0;
    int       rerror = 0;
    int       sendlen = 0;
    int       recvlen = 0;
    int       hdrlen = 4;
    int       rlen = 0;
    char*     cptr = NULL;

    conn->errcode = 0;

    /* check if connected */
    if (false == xsynch_fenet_isconn(conn))
    {
        conn->errcode = 1;
        xsynch_exbufferdata_reset(conn->errmsg);
        xsynch_exbufferdata_append(conn->errmsg, "%s", "please start xmanager.");
        return false;
    }

    /* send data */
    sendlen = conn->sendmsg->len;
    cptr = conn->sendmsg->data;
    while (0 != sendlen)
    {
        flag = POLLOUT;

        /* monitor */
        if (false == xsynch_fenet_desc(conn->sock, flag, &revent, &rerror))
        {
            xsynch_exbufferdata_reset(conn->errmsg);
            xsynch_exbufferdata_append(conn->errmsg, "%s", "xmanager disconnected.");
            goto xsynch_fenet_sendcmd_error;
        }

        if (0 == revent)
        {
            continue;
        }

        if (POLLHUP == (revent & POLLHUP) || POLLERR == (revent & POLLERR))
        {
            xsynch_exbufferdata_reset(conn->errmsg);
            xsynch_exbufferdata_append(conn->errmsg, "%s", "net status error.");
            goto xsynch_fenet_sendcmd_error;
        }

        /* trigger POLLIN event */
        if (POLLIN == (POLLIN & revent))
        {
            /* never come here */
            continue;
        }

        /* send data */
        rlen = write(conn->sock, cptr, sendlen);
        if (rlen < 0)
        {
            /* interrupted by signal, continue reading */
            if (errno == EINTR)
            {
                continue;
            }

            xsynch_exbufferdata_reset(conn->errmsg);
            xsynch_exbufferdata_append(conn->errmsg, "%s", "send command to xmanager error.");
            goto xsynch_fenet_sendcmd_error;
        }

        if (0 == rlen)
        {
            /* already closed */
            xsynch_exbufferdata_reset(conn->errmsg);
            xsynch_exbufferdata_append(conn->errmsg, "%s", "xmanager closed sock.");
            goto xsynch_fenet_sendcmd_error;
        }
        cptr += rlen;
        sendlen -= rlen;
    }
    xsynch_exbufferdata_reset(conn->sendmsg);

    /*
     * get data
     */
    xsynch_exbufferdata_reset(conn->recvmsg);
    cptr = conn->recvmsg->data;
    bhdr = true;
    recvlen = hdrlen;
    while (0 != recvlen)
    {
        flag = POLLIN;

        /* monitor */
        if (false == xsynch_fenet_desc(conn->sock, flag, &revent, &rerror))
        {
            xsynch_exbufferdata_reset(conn->errmsg);
            xsynch_exbufferdata_append(conn->errmsg, "%s", "xmanager disconnected.");
            goto xsynch_fenet_sendcmd_error;
        }

        if (0 == revent)
        {
            continue;
        }

        if (POLLHUP == (revent & POLLHUP) || POLLERR == (revent & POLLERR))
        {
            xsynch_exbufferdata_reset(conn->errmsg);
            xsynch_exbufferdata_append(conn->errmsg, "%s", "net status error.");
            goto xsynch_fenet_sendcmd_error;
        }

        /* trigger POLLOUT event */
        if (POLLOUT == (POLLOUT & revent))
        {
            /* never come here */
            continue;
        }

        /* send data */
        rlen = read(conn->sock, cptr, recvlen);
        if (rlen < 0)
        {
            /* interrupted by signal, continue reading */
            if (errno == EINTR)
            {
                continue;
            }
            xsynch_exbufferdata_reset(conn->errmsg);
            xsynch_exbufferdata_append(conn->errmsg, "%s", "recv message from xmanager error.");
            goto xsynch_fenet_sendcmd_error;
        }

        if (0 == rlen)
        {
            /* already closed */
            xsynch_exbufferdata_reset(conn->errmsg);
            xsynch_exbufferdata_append(conn->errmsg, "%s", "xmanager closed sock.");
            goto xsynch_fenet_sendcmd_error;
        }

        recvlen -= rlen;
        if (0 == recvlen && true == bhdr)
        {
            bhdr = false;
            memcpy(&recvlen, cptr, sizeof(unsigned int));
            recvlen = r_ntoh32(recvlen);

            xsynch_exbufferdata_enlarge(conn->recvmsg, recvlen);
            cptr = conn->recvmsg->data;
            conn->recvmsg->len = hdrlen;
            cptr += hdrlen;
            recvlen -= hdrlen;
        }
        else
        {
            cptr += rlen;
        }
    }
    return true;

xsynch_fenet_sendcmd_error:
    close(conn->sock);
    conn->sock = -1;
    conn->connstatus = XSYNCHCONN_STATUS_NOP;
    return false;
}
