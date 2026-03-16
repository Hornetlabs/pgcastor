#ifndef RIPPLE_DECODE_HEAP_UTIL_H
#define RIPPLE_DECODE_HEAP_UTIL_H

#define CHECK_EXTERNAL(class) (((class)->relnamespace) == (PG_TOAST_NAMESPACE)) ? (true) : (false)

typedef struct oidmap_entry
{
    Oid temp_oid;
    Oid real_oid;
} oidmap_entry;

extern xk_pg_parser_sysdicts *heap_get_sysdict_by_oid(void *decodingctx,
                                                 ripple_txn *txn,
                                                 Oid oid,
                                                 bool search_his);

extern void heap_storage_external_data(ripple_txn *txn, xk_pg_parser_translog_tbcolbase *trans_return);

extern void heap_free_trans_pre(xk_pg_parser_translog_translog2col *trans_data);

extern void heap_free_trans_result(xk_pg_parser_translog_tbcolbase *trans_return);

extern void heap_parser2sql(void* decodingctx,
                            ripple_txn *txn,
                            xk_pg_parser_translog_tbcolbase *trans_return,
                            Oid oid);

void heap_parser_count_size(void* decodingctx,
                            ripple_txn *txn,
                            xk_pg_parser_translog_tbcolbase *trans_return,
                            Oid oid);

extern HTAB *init_oidmap_hash(void);

extern void add_oidmap(HTAB *htab, Oid temp, Oid real);

extern Oid get_real_oid_from_oidmap(HTAB *htab, Oid temp);


extern List *ripple_decode_heap_multi_insert_save_sysdict_as_insert(List *sysdict,
                                                                    xk_pg_parser_translog_tbcolbase *trans_return);

extern List *ripple_decode_heap_sysdicthis_copy(List *his);

#endif
