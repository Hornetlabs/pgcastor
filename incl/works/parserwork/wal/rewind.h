#ifndef _RIPPLE_REWIND_H
#define _RIPPLE_REWIND_H

typedef struct RIPPLE_DECODINGCONTEXT ripple_decodingcontext;

/* rewind结构体中的状态值 */
typedef enum RIPPLE_REWIND_STAT
{
    RIPPLE_REWIND_INIT              = 0x00, /* 初始化状态 */
    RIPPLE_REWIND_SEARCHCHECKPOINT  = 0x01, /* 查找checkpoint状态 */
    RIPPLE_REWIND_REWINDING         = 0x02, /* 过渡状态 */
    RIPPLE_REWIND_EMITING           = 0x03, /* 找到checkpoint后查找emit点 */
    RIPPLE_REWIND_EMITED            = 0x04  /* 找到emit点后设置 */
} ripple_rewind_stat;

typedef struct RIPPLE_REWIND_STRATEGY
{
    TransactionId	xmin;
    TransactionId	xmax;
    HTAB*           xips;
} ripple_rewind_strategy;

/* rewind结构体 */
typedef struct RIPPLE_REWIND
{
    int                     stat;       /* 状态 */
    TransactionId           currentxid; /*xmax*/
    XLogRecPtr              currentlsn; /* 最新点的lsn */
    XLogRecPtr              redolsn;    /* 找到checkpoint后设置的redolsn */
    PGconn*                 conn;       /* 保存连接信息 */
    ripple_rewind_strategy	strategy;   /* snapshot的备份 */
} ripple_rewind;

extern bool ripple_rewind_fastrewind(ripple_decodingcontext *decodingctx);
extern bool ripple_rewind_fastrewind_emit(ripple_decodingcontext *decodingctx);
extern void ripple_rewind_strategy_setfastrewind(ripple_snapshot* snapshot, ripple_decodingcontext* decoingctx);

extern void ripple_rewind_stat_setsearchcheckpoint(ripple_rewind* rewind);
extern void ripple_rewind_stat_setrewinding(ripple_rewind* rewind);
extern void ripple_rewind_stat_setemiting(ripple_rewind* rewind);
extern void ripple_rewind_stat_setemited(ripple_rewind* rewind);

extern bool ripple_rewind_check_stat_allow_get_entry(ripple_rewind* rewind);

#endif
