#ifndef _TXNSTMT_BURST_H_
#define _TXNSTMT_BURST_H_

typedef struct TXNSTMT_burst
{
    uint8               optype;
    uint8*              batchcmd;       /* 批量的 sql */
    char*               table;
    dlist*              rows;           /* 原始数据 */
} txnstmt_burst;

extern txnstmt_burst* txnstmt_burst_init(void);

/* 释放 */
extern void txnstmt_burst_free(void* data);

#endif
