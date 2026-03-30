#ifndef _NET_H
#define _NET_H

/* Create descriptor */
rsocket osal_socket(int domain, int type, int protocol);

/* Set descriptor flag */
int osal_setsockopt(rsocket sockfd, int level, int optname, const void* optval, socklen_t optlen);

/* Get descriptor flag */
int osal_getsockopt(rsocket sockfd, int level, int optname, void* optval, socklen_t* optlen);

/* Set descriptor blocking/non-blocking */
bool osal_setblock(rsocket sockfd);

/* Set to non-blocking mode */
bool osal_setunblock(rsocket sockfd);

/* bind */
int osal_bind(rsocket sockfd, struct sockaddr* my_addr, socklen_t addlen);

/* listen */
int osal_listen(rsocket sockfd, int backlog);

/* close */
int osal_close(rsocket sockfd);

/* accept receive descriptor */
rsocket osal_accept(rsocket sockfd, struct sockaddr* addr, socklen_t* addrlen);

/* Get information recorded in descriptor */
int osal_getsockname(rsocket sockfd, struct sockaddr* addr, socklen_t* addrlen);

/* Get available address */
int osal_getaddrinfo(const char* node, const char* service, const struct addrinfo* hints, struct addrinfo** res);

/* Event poll */
int osal_poll(struct pollfd* fds, nfds_t nfds, int timeout);

/* Connect */
int osal_connect(rsocket sockfd, const struct sockaddr* addr, socklen_t addrlen);

/* Read data */
bool osal_net_read(rsocket sockfd, uint8* buffer, int* amount);

/* Write data */
bool osal_net_write(rsocket sockfd, uint8* buffer, int amount);

/*
 * Get address by name or ip address
 * */
bool osal_host2sockaddr(struct sockaddr_in* addr,
                        const char*         host,
                        const char*         service,
                        int                 family,
                        int                 socktype,
                        int                 protocol,
                        int                 passive);

#endif
