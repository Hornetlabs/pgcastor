#ifndef _REBUILD_H
#define _REBUILD_H

typedef struct REBUILD
{
    uint64          prepareno;      /* prepared statement sequence number for name generation */
    HTAB*           hatatb2prepare; /* prepared statement structure */
    cache_sysdicts* sysdicts;       /* system dictionary */
} rebuild;

/* initialize rebuild content */
void rebuild_reset(rebuild* rebuild);

/* reorganize txn content */
bool rebuild_prepared(rebuild* rebuild, txn* txn);

/* reorganize txn content into burst format */
bool rebuild_txnburst(rebuild* rebuild, txn* txn);

void rebuild_destroy(rebuild* rebuild);

#endif
