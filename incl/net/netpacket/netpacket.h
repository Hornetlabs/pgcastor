#ifndef _RIPPLE_NETPACKET_H
#define _RIPPLE_NETPACKET_H


typedef struct RIPPLE_NETPACKET
{
    int             offset;             /* 含义如下:
                                        * 读数据时: 已经读取到的长度
                                        * 写数据时: 已写完的数据长度
                                        */
    int             used;               /* 总数据长度 */
    int             max;
    uint8*          data;
} ripple_netpacket;


ripple_netpacket* ripple_netpacket_init(void);

uint8* ripple_netpacket_data_init(int len);

void ripple_netpacket_destroyvoid(void* value);

void ripple_netpacket_destroy(ripple_netpacket* netpacket);

#endif
