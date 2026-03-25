#ifndef _BIGTXN_CAPTURESERIAL_H
#define _BIGTXN_CAPTURESERIAL_H

typedef struct BIGTXN_CAPTURESERIAL_CALLBACK
{
    /* Capture get timeline */
    TimeLineID (*bigtxn_parserstat_curtlid_get)(void* privdata);

} bigtxn_captureserial_callback;

/* Big transaction serialization structure */
typedef struct BIGTXN_CAPTURESERIAL
{
    /* Serialization main structure */
    serialstate base;

    /* Recent txn */
    bigtxn* lasttxn;

    /* Big transaction data dictionary */
    cache_sysdicts* dicts;

    /* Transaction cache, used to receive transactions from increment_captureparser thread */
    cache_txn* bigtxn2serial;

    /* padding */
    char padding[CACHELINE_SIZE];

    /* Transaction hash */
    HTAB* by_txns;

    /* padding */
    char padding1[CACHELINE_SIZE];

    /* Store upper layer pointer */
    void* privdata;

    /* Get timeline callback function */
    bigtxn_captureserial_callback callback;
} bigtxn_captureserial;

/* Initialize */
extern bigtxn_captureserial* bigtxn_captureserial_init(void);

/*
 * Logic processing main function
 */
void* bigtxn_captureserial_main(void* args);

void bigtxn_captureserial_destroy(void* args);

#endif
