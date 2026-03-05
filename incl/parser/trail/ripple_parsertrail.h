#ifndef _RIPPLE_PARSERTRAIL_H
#define _RIPPLE_PARSERTRAIL_H

typedef struct RIPPLE_PARSERTRAIL
{
    dlist*                          records;
    ripple_txn*                     lasttxn;
    ripple_transcache*              transcache;
    ripple_ffsmgr_state*            ffsmgrstate;

    /* 提交的事务 */
    dlist*                          dtxns;
    ripple_cache_txn*               parser2txn;
}ripple_parsertrail;

typedef bool (*parsertrailtokenapplyfunc)(ripple_parsertrail* parsertrail, void* data);
typedef void (*parsertrailtokenclean)(ripple_parsertrail* parsertrail, void* data);

/* 清理内存,并重置内容 */
void ripple_parsertrail_reset(ripple_parsertrail* parsertrail);

/* 将 records 解析为事务 */
bool ripple_parsertrail_traildecode(ripple_parsertrail* parsertrail);

/* 将 records 解析为事务 */
bool ripple_parsertrail_parser(ripple_parsertrail* parsertrail);

void ripple_parsertrail_traildata_shiftfile(ripple_parsertrail* parsertrail);

void ripple_parsertrail_free(ripple_parsertrail* parsertrail);

#endif
