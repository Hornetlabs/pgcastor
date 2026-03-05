#ifndef _RIPPLE_ONLINEREFRESH_PUMPSERIAL_H
#define _RIPPLE_ONLINEREFRESH_PUMPSERIAL_H


typedef struct RIPPLE_TASK_ONLINEREFRESHPUMPSERIAL
{
    ripple_serialstate*     serialstate;
    ripple_cache_txn*       parser2serialtxns;
    ripple_transcache*      dictcache;
}ripple_onlinerefresh_pumpserial;

ripple_onlinerefresh_pumpserial* ripple_onlinerefresh_pumpserial_init(void);

void* ripple_onlinerefresh_pumpserial_main(void* args);

void ripple_onlinerefresh_pumpserial_free(void* args);

#endif