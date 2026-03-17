#ifndef _NETPACKET_H
#define _NETPACKET_H


typedef struct NETPACKET
{
    int             offset;             /* 含义如下:
                                        * 读数据时: 已经读取到的长度
                                        * 写数据时: 已写完的数据长度
                                        */
    int             used;               /* 总数据长度 */
    int             max;
    uint8*          data;
} netpacket;


netpacket* netpacket_init(void);

uint8* netpacket_data_init(int len);

void netpacket_destroyvoid(void* value);

void netpacket_destroy(netpacket* netpacket);

#endif
