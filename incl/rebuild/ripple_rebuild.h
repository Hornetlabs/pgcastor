#ifndef _RIPPLE_REBUILD_H
#define _RIPPLE_REBUILD_H

typedef struct RIPPLE_REBUILD
{
    uint64                  prepareno;                  /* 预解析语句编号，生成名字 */
    HTAB*                   hatatb2prepare;             /* 预解析语句的结构 */
    ripple_cache_sysdicts*  sysdicts;                   /* 系统字典 */
}ripple_rebuild;

/* 初始化rebuild内容 */
void ripple_rebuild_reset(ripple_rebuild* rebuild);

/* 对 txn 的内容重组 */
bool ripple_rebuild_prepared(ripple_rebuild* rebuild, ripple_txn* txn);

/* 对 txn 的内容重组为burst */
bool ripple_rebuild_txnburst(ripple_rebuild* rebuild, ripple_txn* txn);

void ripple_rebuild_destroy(ripple_rebuild* rebuild);

#endif
