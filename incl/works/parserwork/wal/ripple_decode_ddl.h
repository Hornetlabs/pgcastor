#ifndef RIPPLE_DECODE_DDL_H
#define RIPPLE_DECODE_DDL_H

extern void ripple_dml2ddl(ripple_decodingcontext* decodingctx, ripple_txn *txn);

extern void heap_ddl_assemble_truncate(ripple_decodingcontext* decodingctx,
                                       ripple_txn *txn,
                                       uint32_t oid);

#endif