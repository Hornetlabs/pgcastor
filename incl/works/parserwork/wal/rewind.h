#ifndef _REWIND_H
#define _REWIND_H

typedef struct DECODINGCONTEXT decodingcontext;

/* rewind结构体中的状态值 */
typedef enum REWIND_STAT
{
    REWIND_INIT              = 0x00, /* 初始化状态 */
    REWIND_SEARCHCHECKPOINT  = 0x01, /* 查找checkpoint状态 */
    REWIND_REWINDING         = 0x02, /* 过渡状态 */
    REWIND_EMITING           = 0x03, /* 找到checkpoint后查找emit点 */
    REWIND_EMITED            = 0x04  /* 找到emit点后设置 */
} rewind_stat;

typedef struct REWIND_STRATEGY
{
    TransactionId	xmin;
    TransactionId	xmax;
    HTAB*           xips;
} rewind_strategy;

/* rewind结构体 */
typedef struct REWIND
{
    int                     stat;       /* 状态 */
    TransactionId           currentxid; /*xmax*/
    XLogRecPtr              currentlsn; /* 最新点的lsn */
    XLogRecPtr              redolsn;    /* 找到checkpoint后设置的redolsn */
    PGconn*                 conn;       /* 保存连接信息 */
    rewind_strategy	strategy;   /* snapshot的备份 */
} rewind_info;

extern bool rewind_fastrewind(decodingcontext *decodingctx);
extern bool rewind_fastrewind_emit(decodingcontext *decodingctx);
extern void rewind_strategy_setfastrewind(snapshot* snapshot, decodingcontext* decoingctx);

extern void rewind_stat_setsearchcheckpoint(rewind_info* rewind);
extern void rewind_stat_setrewinding(rewind_info* rewind);
extern void rewind_stat_setemiting(rewind_info* rewind);
extern void rewind_stat_setemited(rewind_info* rewind);

extern bool rewind_check_stat_allow_get_entry(rewind_info* rewind);

#endif
