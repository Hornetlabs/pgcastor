#ifndef _RIPPLE_TXNSTMT_BURST_H_
#define _RIPPLE_TXNSTMT_BURST_H_

typedef struct RIPPLE_TXNSTMT_burst
{
    uint8               optype;
    uint8*              batchcmd;       /* 批量的 sql */
    char*               table;
    dlist*              rows;           /* 原始数据 */
} ripple_txnstmt_burst;

extern ripple_txnstmt_burst* ripple_txnstmt_burst_init(void);

/* 释放 */
extern void ripple_txnstmt_burst_free(void* data);

#endif
