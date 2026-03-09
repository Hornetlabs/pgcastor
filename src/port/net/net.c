#include "app_incl.h"
#include "port/net/net.h"

/* Create descriptor */
rsocket osal_socket(int domain, int type, int protocol)
{
    return socket(domain, type, protocol);
}

/* Set socket option */
int osal_setsockopt(rsocket sockfd, int level, int optname, const void* optval, socklen_t optlen)
{
    return setsockopt(sockfd, level, optname, optval, optlen);
}

/* Get socket option */
int osal_getsockopt(rsocket sockfd, int level, int optname, void* optval, socklen_t* optlen)
{
    return getsockopt(sockfd, level, optname, optval, optlen);
}

/* Set socket to blocking mode */
bool osal_setblock(rsocket sockfd)
{
    /*
     * 1. Obtain the descriptor's status
     * 2. Set to blocking
     */
    int flags;

    /* Get status */
    flags = fcntl(sockfd, F_GETFL);
    if (0 > flags)
    {
        return false;
    }

    flags |= O_NONBLOCK;
    if (-1 == fcntl(sockfd, F_SETFL, flags))
    {
        return false;
    }

    return true;
}

/* Set socket to non-blocking mode */
bool osal_setunblock(rsocket sockfd)
{
    /*
     * 1. Get descriptor status
     * 2. Set to unblock
     */
    int flags;

    /* Get status */
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

/* Get available address */
int osal_getaddrinfo(const char* node, const char* service, const struct addrinfo* hints, struct addrinfo** res)
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

/* bind */
int osal_bind(rsocket sockfd, struct sockaddr* my_addr, socklen_t addlen)
{
    return bind(sockfd, my_addr, addlen);
}

/* listen */
int osal_listen(rsocket sockfd, int backlog)
{
    return listen(sockfd, backlog);
}

/* close */
int osal_close(rsocket sockfd)
{
    return close(sockfd);
}

/* osal_accept - accept connection */
rsocket osal_accept(rsocket sockfd, struct sockaddr* addr, socklen_t* addrlen)
{
    return accept(sockfd, addr, addrlen);
}

/* Get socket address info */
int osal_getsockname(rsocket sockfd, struct sockaddr* addr, socklen_t* addrlen)
{
    return getsockname(sockfd, addr, addrlen);
}

/* Poll events */
int osal_poll(struct pollfd* fds, nfds_t nfds, int timeout)
{
    return poll(fds, nfds, timeout);
}

/* Connect */
int osal_connect(rsocket sockfd, const struct sockaddr* addr, socklen_t addrlen)
{
    return connect(sockfd, addr, addrlen);
}

/*
 * free addr info
 * */
static void osal_rfreeaddrinfo(struct addrinfo* res)
{
    freeaddrinfo(res);
}

/* Read data */
bool osal_net_read(rsocket sockfd, uint8* buffer, int* amount)
{
    int    rlen = 0;
    int    slen = 0;
    uint8* uptr = NULL;

    uptr = buffer;
    slen = *amount;
    while (0 != slen)
    {
        rlen = read(sockfd, uptr, slen);
        if (rlen < 0)
        {
            /* Interrupted by signal, continue reading */
            if (errno == EINTR)
            {
                continue;
            }
            return false;
        }

        if (0 == rlen)
        {
            /* Connection closed */
            *amount = 0;
            return false;
        }
        uptr += rlen;
        slen -= rlen;
    }

    return true;
}

/* Write data */
bool osal_net_write(rsocket sockfd, uint8* buffer, int amount)
{
    int    rlen = 0;
    uint8* uptr = NULL;

    uptr = buffer;
    while (0 != amount)
    {
        rlen = write(sockfd, uptr, amount);
        if (rlen < 0)
        {
            /* Interrupted by signal, continue reading */
            if (errno == EINTR)
            {
                continue;
            }
            return false;
        }

        if (0 == rlen)
        {
            /* Connection closed */
            return false;
        }
        uptr += rlen;
        amount -= rlen;
    }

    return true;
}

/*
 * Get address by name or IP address
 * */
bool osal_host2sockaddr(struct sockaddr_in* addr,
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

    ret = osal_getaddrinfo(host, service, &hints, &reshead);
    if (0 != ret)
    {
        elog(RLOG_WARNING, "osal_getaddrinfo error, %s", strerror(errno));
        return false;
    }

    for (res = reshead; res; res = res->ai_next)
    {
        if (family == res->ai_family && socktype == res->ai_socktype && sizeof(struct sockaddr_in) == res->ai_addrlen)
        {
            *addr = *((struct sockaddr_in*)(res->ai_addr));
            osal_rfreeaddrinfo(reshead);
            return true;
        }
    }

    osal_rfreeaddrinfo(reshead);
    return false;
}
