#ifndef _REBUILD_H
#define _REBUILD_H

typedef struct REBUILD
{
    uint64                  prepareno;                  /* 预解析语句编号，生成名字 */
    HTAB*                   hatatb2prepare;             /* 预解析语句的结构 */
    cache_sysdicts*  sysdicts;                   /* 系统字典 */
}rebuild;

/* 初始化rebuild内容 */
void rebuild_reset(rebuild* rebuild);

/* 对 txn 的内容重组 */
bool rebuild_prepared(rebuild* rebuild, txn* txn);

/* 对 txn 的内容重组为burst */
bool rebuild_txnburst(rebuild* rebuild, txn* txn);

void rebuild_destroy(rebuild* rebuild);

#endif
