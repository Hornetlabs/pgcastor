#ifndef _RIPPLE_FFTRAIL_TBMETADATA_H
#define _RIPPLE_FFTRAIL_TBMETADATA_H

/* 表信息序列化 */
bool ripple_fftrail_tbmetadata_serial(bool force,
                                      Oid dbid,
                                      Oid tbid,
                                      FullTransactionId xid,
                                      uint32* dbmdno,
                                      uint32* tbmdno,
                                      void* state);

/* 表信息反序列化 */
bool ripple_fftrail_tbmetadata_deserial(void** data, void* state);

#endif
