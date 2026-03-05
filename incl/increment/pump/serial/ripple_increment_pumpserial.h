#ifndef _RIPPLE_INCREMENT_PUMPSERIAL_H
#define _RIPPLE_INCREMENT_PUMPSERIAL_H


typedef struct RIPPLE_INCREMENT_PUMPSERIAL_CALLBACK
{
    /* pump 设置网络端状态 */
    void (*clientstat_state_set)(void* privdata, int state);

    /* pump 设置parserstat */
    void (*parserstat_state_set)(void* privdata, int state);

    /* pump 获取cfileid */
    uint64 (*networkclientstate_cfileid_get)(void* privdata);

} ripple_increment_pumpserial_callback;

typedef struct RIPPLE_INCREMENT_PUMPSERIALSTATE
{
    ripple_serialstate                      base;
    ripple_cache_txn*                       parser2serialtxns;
    ripple_transcache*                      dictcache;
    void*                                   privdata;
    int                                     state;
    ripple_increment_pumpserial_callback    callback;
} ripple_increment_pumpserialstate;

ripple_increment_pumpserialstate* ripple_increment_pumpserialstate_init(void);

void* ripple_increment_pumpserial_main(void *args);

/* 资源回收 */
void ripple_increment_pumpserial_destroy(ripple_increment_pumpserialstate* serialpumpstate);

char* ripple_increment_pumpserial_getdbname(void* pumpserial, Oid oid);

Oid ripple_increment_pumpserial_getdboid(void* pumpserial);

void ripple_increment_pumpserial_setdboid(void* pumpserial, Oid oid);

void* ripple_increment_pumpserial_getnamespace(void* pumpserial, Oid oid);

void* ripple_increment_pumpserial_getclass(void* pumpserial, Oid oid);

void* ripple_increment_pumpserial_getattributes(void* pumpserial, Oid oid);

void* ripple_increment_pumpserial_gettype(void* pumpserial, Oid oid);

void ripple_increment_pumpserial_transcatalog2transcache(void* pumpserial, void* catalog);

void ripple_increment_pumpserial_ffsmgr_setcallback(ripple_increment_pumpserialstate* wstate);

#endif
