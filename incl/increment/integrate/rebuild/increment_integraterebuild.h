#ifndef _INCREMENT_INTEGRATEREBUILD_H
#define _INCREMENT_INTEGRATEREBUILD_H

typedef struct INCREMENT_INTEGRATEREBUILD_CALLBACK
{
    /* Whether refresh is complete, start refresh check if refresh is running */
    bool (*isrefreshdown)(void* privdata);

    /* integratestate add onlinerefresh node */
    void (*addonlinerefresh)(void* privdata, void* onlinerefresh);

    /* Whether sync is idle */
    bool (*issyncidle)(void* privdata);

    /* Whether onlinerefresh is complete */
    bool (*isonlinerefreshdone)(void* privdata, void* no);

    /* Whether onlinerefresh stock is complete */
    bool (*isolrrefreshdone)(void* privdata, void* no);

    /* Set onlinerefresh end */
    void (*setonlinerefreshfree)(void* privdata, void* no);

    /* Whether big transaction is complete, check if corresponding big transaction is running */
    bool (*isbigtxndown)(void* privdata, FullTransactionId xid);

    /* Whether big transaction is sigterm */
    bool (*isbigtxnsigterm)(void* privdata, FullTransactionId xid);

    /* integratestate add bigtxn node */
    void (*addbigtxn)(void* privdata, void* bigtxn);

} increment_integraterebuild_callback;

typedef enum INCREMENT_INTEGRATEREBUILD_STAT
{
    INCREMENT_INTEGRATEREBUILD_STAT_NOP = 0x00,
    INCREMENT_INTEGRATEREBUILD_STAT_READY, /* sync set rebuild */
    INCREMENT_INTEGRATEREBUILD_STAT_WORK,
    INCREMENT_INTEGRATEREBUILD_STAT_WAITTERM /* Received term or other thread abnormal exit */
} increment_integraterebuild_stat;

typedef struct INCREMENT_INTEGRATEREBUILD
{
    rebuild                             rebuild;
    bool                                mergetxn;
    bool                                burst; /* Burst mode */
    increment_integraterebuild_stat     stat;
    int                                 txbundlesize; /* Threshold for merging transactions */
    XLogRecPtr                          filterlsn;    /* Less than or equal to this lsn does not need processing */
    onlinerefresh_integratedataset*     onlinerefreshdataset;
    HTAB*                               honlinerefreshfilterdataset;
    char                                padding[CACHELINE_SIZE];
    cache_txn*                          parser2rebuild;
    char                                padding1[CACHELINE_SIZE];
    cache_txn*                          rebuild2sync;
    char                                padding2[CACHELINE_SIZE];
    bigtxn_persist*                     txnpersist;
    char                                padding3[CACHELINE_SIZE];
    onlinerefresh_persist*              olpersist;
    char                                padding4[CACHELINE_SIZE];
    void*                               privdata;
    char                                padding5[CACHELINE_SIZE];
    increment_integraterebuild_callback callback;
} increment_integraterebuild;

/* Initialize */
increment_integraterebuild* increment_integraterebuild_init(void);

/* Work */
void* increment_integraterebuild_main(void* args);

void increment_integraterebuild_free(increment_integraterebuild* rebuild);

#endif
