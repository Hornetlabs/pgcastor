#ifndef _ONLINEREFRESH_INTEGRATEREBUILD_H
#define _ONLINEREFRESH_INTEGRATEREBUILD_H

typedef struct ONLINEREFRESH_INTEGRATEREBUILD
{
    rebuild    rebuild;
    bool       mergetxn;
    bool       burst;        /* Burst mode */
    int        txbundlesize; /* Threshold for merging transactions */
    char       padding[CACHELINE_SIZE];
    cache_txn* parser2rebuild;
    char       padding1[CACHELINE_SIZE];
    cache_txn* rebuild2sync;
    char       padding2[CACHELINE_SIZE];
    void*      privdata;
    char       padding3[CACHELINE_SIZE];
} onlinerefresh_integraterebuild;

/* Initialize */
onlinerefresh_integraterebuild* onlinerefresh_integraterebuild_init(void);

/* Work */
void* onlinerefresh_integraterebuild_main(void* args);

void onlinerefresh_integraterebuild_free(void* args);

#endif
