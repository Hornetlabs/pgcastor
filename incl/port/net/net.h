#ifndef _NET_H
#define _NET_H

/* 创建描述符 */
rsocket osal_socket(int domain, int type, int protocol);

/* 设置描述符的标识 */
int osal_setsockopt(rsocket sockfd, int level, int optname, const void* optval, socklen_t optlen);

/* 获取描述符的标识 */
int osal_getsockopt(rsocket sockfd, int level, int optname, void* optval, socklen_t* optlen);

/* 设置描述符阻塞/非阻塞 */
bool osal_setblock(rsocket sockfd);

/* 设置为非阻塞模式 */
bool osal_setunblock(rsocket sockfd);

/* bind */
int osal_bind(rsocket sockfd, struct sockaddr* my_addr, socklen_t addlen);

/* listen */
int osal_listen(rsocket sockfd, int backlog);

/* close */
int osal_close(rsocket sockfd);

/* accept 接收描述符 */
rsocket osal_accept(rsocket sockfd, struct sockaddr *addr, socklen_t *addrlen);

/* 获取描述符中记录的信息 */
int osal_getsockname(rsocket sockfd, struct sockaddr *addr, socklen_t *addrlen);

/* 获取可用地址 */
int osal_getaddrinfo(const char* node, const char* service, const struct addrinfo *hints, struct addrinfo **res);

/* 事件 poll */
int osal_poll(struct pollfd* fds, nfds_t nfds, int timeout);

/* 连接 */
int osal_connect(rsocket sockfd, const struct sockaddr *addr, socklen_t addrlen);

/* 读取数据 */
bool osal_net_read(rsocket sockfd, uint8 *buffer, int *amount);

/* 写数据 */
bool osal_net_write(rsocket sockfd, uint8 *buffer, int amount);

/*
 * 根据名称或ip地址获取地址
 * */
bool osal_host2sockaddr(struct sockaddr_in *addr,
                            const char *host,
                            const char *service,
                            int family,
                            int socktype,
                            int protocol,
                            int passive);

#endif
