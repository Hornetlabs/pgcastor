#ifndef _ONLINEREFRESH_INTEGRATEFILTERDATASET_H
#define _ONLINEREFRESH_INTEGRATEFILTERDATASET_H

typedef struct ONLINEREFRESH_INTEGRATEFILTERDATASET
{
    Oid               oid;
    FullTransactionId txid;
    char              schema[NAMEDATALEN];
    char              table[NAMEDATALEN];
} onlinerefresh_integratefilterdataset;

HTAB* onlinerefresh_integratefilterdataset_init(void);

bool onlinerefresh_integratefilterdataset_add(HTAB* filterdataset, void* in_tables,
                                              FullTransactionId txid);

/* Copy hash filter set */
HTAB* onlinerefresh_integratefilterdataset_copy(HTAB* filterdataset);

bool onlinerefresh_integratefilterdataset_delete(HTAB* filterdataset, void* in_tables,
                                                 FullTransactionId txid);

#endif
