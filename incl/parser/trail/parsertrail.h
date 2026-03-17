#ifndef _PARSERTRAIL_H
#define _PARSERTRAIL_H

typedef struct PARSERTRAIL
{
    dlist*                          records;
    txn*                     lasttxn;
    transcache*              transcache;
    ffsmgr_state*            ffsmgrstate;

    /* 提交的事务 */
    dlist*                          dtxns;
    cache_txn*               parser2txn;
}parsertrail;

typedef bool (*parsertrailtokenapplyfunc)(parsertrail* parsertrail, void* data);
typedef void (*parsertrailtokenclean)(parsertrail* parsertrail, void* data);

/* 清理内存,并重置内容 */
void parsertrail_reset(parsertrail* parsertrail);

/* 将 records 解析为事务 */
bool parsertrail_traildecode(parsertrail* parsertrail);

/* 将 records 解析为事务 */
bool parsertrail_parser(parsertrail* parsertrail);

void parsertrail_traildata_shiftfile(parsertrail* parsertrail);

void parsertrail_free(parsertrail* parsertrail);

#endif
