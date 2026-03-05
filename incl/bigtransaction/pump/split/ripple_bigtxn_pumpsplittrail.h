#ifndef _RIPPLE_BIGTXN_PUMPSPLITTRAIL_H
#define _RIPPLE_BIGTXN_PUMPSPLITTRAIL_H

typedef struct RIPPLE_BIGTXN_PUMPSPLITTRAIL
{
    FullTransactionId                   xid;
    ripple_increment_pumpsplittrail     *splittrailctx;
}ripple_bigtxn_pumpsplittrail;

extern ripple_bigtxn_pumpsplittrail *ripple_bigtxn_pumpsplittrail_init(FullTransactionId xid);

extern void *ripple_bigtxn_pumpsplittrail_main(void *args);

extern void ripple_bigtxn_pumpsplittrail_free(void *args);

#endif

