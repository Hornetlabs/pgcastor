#ifndef DECODE_DDL_H
#define DECODE_DDL_H

extern void dml2ddl(decodingcontext* decodingctx, txn *txn);

extern void heap_ddl_assemble_truncate(decodingcontext* decodingctx,
                                       txn *txn,
                                       uint32_t oid);

#endif