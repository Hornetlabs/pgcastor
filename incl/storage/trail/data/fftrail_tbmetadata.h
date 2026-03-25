#ifndef _FFTRAIL_TBMETADATA_H
#define _FFTRAIL_TBMETADATA_H

/* Table information serialization */
bool fftrail_tbmetadata_serial(bool force, Oid dbid, Oid tbid, FullTransactionId xid,
                               uint32* dbmdno, uint32* tbmdno, void* state);

/* Table information deserialization */
bool fftrail_tbmetadata_deserial(void** data, void* state);

#endif
