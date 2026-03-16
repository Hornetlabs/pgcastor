#ifndef _RIPPLE_ONLINEREFRESH_INTEGRATEFILTERDATASET_H
#define _RIPPLE_ONLINEREFRESH_INTEGRATEFILTERDATASET_H

typedef struct RIPPLE_ONLINEREFRESH_INTEGRATEFILTERDATASET
{
    Oid                 oid;
    FullTransactionId   txid;
    char                schema[NAMEDATALEN];
    char                table[NAMEDATALEN];
}ripple_onlinerefresh_integratefilterdataset;


HTAB* ripple_onlinerefresh_integratefilterdataset_init(void);

bool ripple_onlinerefresh_integratefilterdataset_add(HTAB* filterdataset, void* in_tables, FullTransactionId txid);

/* 复制过hash滤集 */
HTAB* ripple_onlinerefresh_integratefilterdataset_copy(HTAB* filterdataset);

bool ripple_onlinerefresh_integratefilterdataset_delete(HTAB* filterdataset, void* in_tables, FullTransactionId txid);

#endif
