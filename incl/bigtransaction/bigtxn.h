#ifndef BIGTXN_H
#define BIGTXN_H

typedef struct BIGTXN
{
    FullTransactionId xid;      /* Transaction ID */
    ffsmgr_fdata*     fdata;    /* Serialization structure */
    List*             txndicts; /* System dictionary */
    file_buffer       fbuffer;  /* Saved temporary page, temporary save */
} bigtxn;

extern bool bigtxn_reset(bigtxn* bigtxn);
extern void bigtxn_clean(bigtxn* htxn);

#endif
