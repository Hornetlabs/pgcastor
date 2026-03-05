#ifndef _RIPPLE_WORKTHREADMGR_H
#define _RIPPLE_WORKTHREADMGR_H

#include "port/thread/ripple_thread.h"

#define InvalidTHRID                0

typedef enum RIPPLE_THREAD_TYPE
{
    RIPPLE_THREAD_TYPE_NOP                      = 0x00,
    RIPPLE_THREAD_TYPE_WAL                      = 0x01,
    RIPPLE_THREAD_TYPE_PARSER                   = 0x02,
    RIPPLE_THREAD_TYPE_TRFSERIAL                = 0x03,
    RIPPLE_THREAD_TYPE_TRFW                     = 0x04,
    RIPPLE_THREAD_TYPE_TRFR                     = 0x05,
    RIPPLE_THREAD_TYPE_SYNC                     = 0x06,
    RIPPLE_THREAD_TYPE_PUMP_TRAIL               = 0x07,
    RIPPLE_THREAD_TYPE_PUMP_NETCLIENT           = 0x08,
    RIPPLE_THREAD_TYPE_COLLECTOR_SVR            = 0x09,
    RIPPLE_THREAD_TYPE_PUMP_SERIAL              = 0x0A,
    RIPPLE_THREAD_TYPE_INTEGRATE_SYNC           = 0x0B,
    RIPPLE_THREAD_TYPE_INTEGRATE_TRAIL          = 0X0C,
    RIPPLE_THREAD_TYPE_COLLECTOR_STATE          = 0X0D,
    RIPPLE_THREAD_TYPE_INTEGRAT_STATE           = 0X0E,
    RIPPLE_THREAD_TYPE_PUMP_STATE               = 0X0F,
    RIPPLE_THREAD_TYPE_COLLECTOR_FTP            ,
    RIPPLE_THREAD_TYPE_CPUMP_FTP                ,
    RIPPLE_THREAD_TYPE_INTEGRATE_REBUILD        ,
    RIPPLE_THREAD_TYPE_CAPTURE_BIGTXN_TRFW      ,
    RIPPLE_THREAD_TYPE_CAPTURE_BIGTXN_TRFSERIAL ,
    RIPPLE_THREAD_TYPE_INTEGRATE_BIGTXNMGR      ,
    RIPPLE_THREAD_TYPE_PUMP_BIGTXNMGR
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
