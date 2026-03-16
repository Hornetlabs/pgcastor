#include "ripple_app_incl.h"
#include "port/net/ripple_net.h"

/* 创建描述符 */
rsocket ripple_socket(int domain, int type, int protocol)
{
    return socket(domain, type, protocol);
}

/* 设置描述符的标识 */
int ripple_setsockopt(rsocket sockfd, int level, int optname, const void* optval, socklen_t optlen)
{
    return setsockopt(sockfd, level, optname, optval, optlen);
}

/* 获取描述符的标识 */
int ripple_getsockopt(rsocket sockfd, int level, int optname, void* optval, socklen_t* optlen)
{
    return getsockopt(sockfd, level, optname, optval, optlen);
}

/* 设置描述符阻塞/非阻塞 */
bool ripple_setblock(rsocket sockfd)
{
    /* 
     * 1、获取 描述符的状态
     * 2、设置为 block
     */
    int flags;

    /* 获取状态 */
    flags = fcntl(sockfd, F_GETFL);
    if(0 > flags)
    {
        return false;
    }

    flags |= O_NONBLOCK;
    if(-1 == fcntl(sockfd, F_SETFL, flags))
    {
        return false;
    }

    return true;
}

/* 设置为非阻塞模式 */
bool ripple_setunblock(rsocket sockfd)
{
    /* 
     * 1、获取 描述符的状态
     * 2、设置为 unblock
     */
    int flags;

    /* 获取状态 */
    flags = fcntl(sockfd, F_GETFL);
    if(0 > flags)
    {
        return false;
    }

    flags &= (~O_NONBLOCK);
    if(-1 == fcntl(sockfd, F_SETFL, flags))
    {
        return false;
    }

    return true;
}

/* 获取可用地址 */
int ripple_getaddrinfo(const char* node,
                        const char* service,
                        const struct addrinfo *hints,
                        struct addrinfo **res)
{
    if(NULL != node && node[0] == '*')
    {
        node = NULL;
    }
    else if(NULL == node || '\0' == node[0])
    {
        node = NULL;
    }

    if(NULL != service && service[0] == '*')
    {
        service = NULL;
    }
    else if(NULL == service || '\0' == service[0])
    {
        service = NULL;
    }

    *res = NULL;
    return getaddrinfo(node, service, hints, res);
}

/* bind */
int ripple_bind(rsocket sockfd, struct sockaddr* my_addr, socklen_t addlen)
{
    return bind(sockfd, my_addr, addlen);
}

/* listen */
int ripple_listen(rsocket sockfd, int backlog)
{
    return listen(sockfd, backlog);
}

/* close */
int ripple_close(rsocket sockfd)
{
    return close(sockfd);
}

/* accept 接收描述符 */
rsocket ripple_accept(rsocket sockfd, struct sockaddr *addr, socklen_t *addrlen)
{
    return accept(sockfd, addr, addrlen);
}

/* 获取描述符中记录的信息 */
int ripple_getsockname(rsocket sockfd, struct sockaddr *addr, socklen_t *addrlen)
{
    return getsockname(sockfd, addr, addrlen);
}

/* 事件 poll */
int ripple_poll(struct pollfd* fds, nfds_t nfds, int timeout)
{
    return poll(fds, nfds, timeout);
}

/* 连接 */
int ripple_connect(rsocket sockfd, const struct sockaddr *addr, socklen_t addrlen)
{
    return connect(sockfd, addr, addrlen);
}

/*
 * free addr info
 * */
static void ripple_freeaddrinfo(struct addrinfo *res)
{
	freeaddrinfo(res);
}

/* 读取数据 */
bool ripple_net_read(rsocket sockfd, uint8 *buffer, int *amount)
{
    int     rlen = 0;
    int     slen = 0;
    uint8*  uptr = NULL;

    uptr = buffer;
    slen = *amount;
    while(0 != slen)
    {
        rlen = read(sockfd, uptr, slen);
        if (rlen < 0)
        {
            /* 被信号中断，那么继续读取 */
            if (errno == EINTR)
            {
                continue;
            }
            return false;
        }

        if(0 == rlen)
        {
            /* 已关闭 */
            *amount = 0;
            return false;
        }
        uptr += rlen;
        slen -= rlen;
    }

    return true;
}

/* 写数据 */
bool ripple_net_write(rsocket sockfd, uint8 *buffer, int amount)
{
    int     rlen = 0;
    uint8*  uptr = NULL;

    uptr = buffer;
    while(0 != amount)
    {
        rlen = write(sockfd, uptr, amount);
        if (rlen < 0)
        {
            /* 被信号中断，那么继续读取 */
            if (errno == EINTR)
            {
                continue;
            }
            return false;
        }

        if(0 == rlen)
        {
            /* 已关闭 */
            return false;
        }
        uptr += rlen;
        amount -= rlen;
    }

    return true;
}

/*
 * 根据名称或ip地址获取地址
 * */
bool ripple_host2sockaddr(struct sockaddr_in *addr,
                            const char *host,
                            const char *service,
                            int family,
                            int socktype,
                            int protocol,
                            int passive)
{
    int ret = 0;
    struct addrinfo hints;
    struct addrinfo *res, *reshead;
    memset(&hints, 0, sizeof(struct addrinfo));
    res = NULL;
    reshead = NULL;

    hints.ai_family = family;
    hints.ai_protocol = protocol;
    hints.ai_socktype = socktype;
    if(passive)
    {
        hints.ai_flags |= AI_PASSIVE;
    }

    ret = ripple_getaddrinfo(host, service, &hints, &reshead);
    if(0 != ret)
    {
        elog(RLOG_WARNING, "getaddrinfo error, %s", strerror(errno));
        return false;
    }

    for(res = reshead; res; res = res->ai_next)
    {
        if(family == res->ai_family 
            && socktype == res->ai_socktype
            && sizeof(struct sockaddr_in) == res->ai_addrlen)
        {
            *addr= *((struct sockaddr_in *)(res->ai_addr));
            ripple_freeaddrinfo(reshead);
            return true;
        }
    }

    ripple_freeaddrinfo(reshead);
    return false;
}
