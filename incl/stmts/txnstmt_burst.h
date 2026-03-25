#ifndef _TXNSTMT_BURST_H_
#define _TXNSTMT_BURST_H_

typedef struct TXNSTMT_burst
{
    uint8  optype;
    uint8* batchcmd; /* Batch sql */
    char*  table;
    dlist* rows; /* Original data */
} txnstmt_burst;

extern txnstmt_burst* txnstmt_burst_init(void);

/* Release */
extern void txnstmt_burst_free(void* data);

#endif
