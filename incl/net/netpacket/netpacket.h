#ifndef _NETPACKET_H
#define _NETPACKET_H

typedef struct NETPACKET
{
    int offset;  /* meaning varies by context:
                  * when reading: number of bytes already read
                  * when writing: number of bytes already written
                  */
    int    used; /* total data length */
    int    max;
    uint8* data;
} netpacket;

netpacket* netpacket_init(void);

uint8* netpacket_data_init(int len);

void netpacket_destroyvoid(void* value);

void netpacket_destroy(netpacket* netpacket);

#endif
