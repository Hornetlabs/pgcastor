#ifndef _RIPPLE_WORKTHREADMGR_H
#define _RIPPLE_WORKTHREADMGR_H

#include "port/thread/ripple_thread.h"

#define InvalidTHRID                0

typedef enum RIPPLE_THREAD_TYPE
{
    RIPPLE_THREAD_TYPE_NOP                      = 0x00,
    RIPPLE_THREAD_TYPE_WAL                      ,
    RIPPLE_THREAD_TYPE_PARSER                   ,
    RIPPLE_THREAD_TYPE_TRFSERIAL                ,
    RIPPLE_THREAD_TYPE_TRFW                     ,
    RIPPLE_THREAD_TYPE_TRFR                     ,
    RIPPLE_THREAD_TYPE_SYNC                     ,
    RIPPLE_THREAD_TYPE_INTEGRATE_SYNC           ,
    RIPPLE_THREAD_TYPE_INTEGRATE_TRAIL          ,
    RIPPLE_THREAD_TYPE_INTEGRAT_STATE           ,
    RIPPLE_THREAD_TYPE_INTEGRATE_REBUILD        ,
    RIPPLE_THREAD_TYPE_CAPTURE_BIGTXN_TRFW      ,
    RIPPLE_THREAD_TYPE_CAPTURE_BIGTXN_TRFSERIAL ,
    RIPPLE_THREAD_TYPE_INTEGRATE_BIGTXNMGR
} ripple_thread_type;

typedef struct RIPPLE_WORKMGR_NODE
{
    ripple_thread_type          type;
    ripple_work_status          status;
    pthread_t                   id;
    void*                       privdata;           /* 每个线程需要的结构 */
    struct RIPPLE_WORKMGR_NODE* next;
} ripple_workmgr_node;

typedef struct RIPPLE_WORKTHREADMGRS
{
    int                         thrnum;
    int                         threxitnum;
    ripple_workmgr_node*        head;
    ripple_workmgr_node**       tail;
} ripple_workthreadmgrs;


void ripple_workthreadmgr_add(int thrtype, const pthread_attr_t *attr, thrworkfunc func, void* privdata);

void ripple_workthreadmgr_setstatus_term(void);

bool ripple_workthreadmgr_trydestroy(void);

void ripple_workthreadmgr_init(void);

void ripple_workthreadmgr_waitstatus(ripple_workmgr_node* worknode, ripple_work_status status);

void ripple_workthreadmgr_destroy(int status, void* arg);

#endif
