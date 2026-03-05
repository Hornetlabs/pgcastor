#ifndef _RIPPLE_NET_H
#define _RIPPLE_NET_H

/* 创建描述符 */
rsocket ripple_socket(int domain, int type, int protocol);

/* 设置描述符的标识 */
int ripple_setsockopt(rsocket sockfd, int level, int optname, const void* optval, socklen_t optlen);

/* 获取描述符的标识 */
int ripple_getsockopt(rsocket sockfd, int level, int optname, void* optval, socklen_t* optlen);

/* 设置描述符阻塞/非阻塞 */
bool ripple_setblock(rsocket sockfd);

/* 设置为非阻塞模式 */
bool ripple_setunblock(rsocket sockfd);

/* bind */
int ripple_bind(rsocket sockfd, struct sockaddr* my_addr, socklen_t addlen);

/* listen */
int ripple_listen(rsocket sockfd, int backlog);

/* close */
int ripple_close(rsocket sockfd);

/* accept 接收描述符 */
rsocket ripple_accept(rsocket sockfd, struct sockaddr *addr, socklen_t *addrlen);

/* 获取描述符中记录的信息 */
int ripple_getsockname(rsocket sockfd, struct sockaddr *addr, socklen_t *addrlen);

/* 获取可用地址 */
int ripple_getaddrinfo(const char* node, const char* service, const struct addrinfo *hints, struct addrinfo **res);

/* 事件 poll */
int ripple_poll(struct pollfd* fds, nfds_t nfds, int timeout);

/* 连接 */
int ripple_connect(rsocket sockfd, const struct sockaddr *addr, socklen_t addrlen);

/* 读取数据 */
bool ripple_net_read(rsocket sockfd, uint8 *buffer, int *amount);

/* 写数据 */
bool ripple_net_write(rsocket sockfd, uint8 *buffer, int amount);

/*
 * 根据名称或ip地址获取地址
 * */
bool ripple_host2sockaddr(struct sockaddr_in *addr,
                            const char *host,
                            const char *service,
                            int family,
                            int socktype,
                            int protocol,
                            int passive);

#endif
