#ifndef _RIPPLE_STATE_PUMP_H
#define _RIPPLE_STATE_PUMP_H

typedef struct RIPPLE_STATE_PUMP_STATE
{
    XLogRecPtr          loadlsn;                /* 重组后事务的 lsn */
    XLogRecPtr          sendlsn;                /* 已发送到 collector 的 lsn */
    uint64              loadtrailno;            /* 发送到collector的 trail 文件编号 */
    uint64              loadtrailstart;         /* 加载 capture trail 文件内的偏移 */
    uint64              sendtrailno;            /* 事务重组后发送到 collector 的 trail 编号 */
    uint64              sendtrailstart;         /* 事务重组后发送到 collector 的 trail 文件内的偏移 */
    TimestampTz         loadtimestamp;          /* 重组后事务的提交的时间戳 */
    TimestampTz         sendtimestamp;          /* 已发送事务的时间戳 */
} ripple_state_pump_state;


void* ripple_state_pump_main(void *args);

ripple_state_pump_state* ripple_state_pump_init(void);

void ripple_state_pump_destroy(ripple_state_pump_state* state);

#endif