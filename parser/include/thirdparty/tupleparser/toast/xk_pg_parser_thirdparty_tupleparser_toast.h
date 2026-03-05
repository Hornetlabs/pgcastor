#ifndef XK_PG_PARSER_THIRDPARTY_TUPLEPARSER_TOAST_H
#define XK_PG_PARSER_THIRDPARTY_TUPLEPARSER_TOAST_H

extern struct xk_pg_parser_varlena *xk_pg_parser_heap_tuple_fetch_attr(struct xk_pg_parser_varlena *attr, bool *is_toast);
extern struct xk_pg_parser_varlena *xk_pg_parser_heap_tuple_untoast_attr(struct xk_pg_parser_varlena *attr, bool *is_toast, bool *need_free, int dbtype, char *dbversion);
extern struct xk_pg_parser_varlena *xk_pg_parser_detoast_datum_packed(struct xk_pg_parser_varlena *datum, bool *is_toast, bool *need_free, int dbtype, char *dbversion);
extern struct xk_pg_parser_varlena *xk_pg_parser_detoast_datum(struct xk_pg_parser_varlena *datum, bool *is_toast, bool *need_free, int dbtype, char *dbversion);
#endif 
