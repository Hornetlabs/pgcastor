#ifndef PG_PARSER_THIRDPARTY_TUPLEPARSER_TOAST_H
#define PG_PARSER_THIRDPARTY_TUPLEPARSER_TOAST_H

extern struct pg_parser_varlena *pg_parser_heap_tuple_fetch_attr(struct pg_parser_varlena *attr, bool *is_toast);
extern struct pg_parser_varlena *pg_parser_heap_tuple_untoast_attr(struct pg_parser_varlena *attr, bool *is_toast, bool *need_free, int dbtype, char *dbversion);
extern struct pg_parser_varlena *pg_parser_detoast_datum_packed(struct pg_parser_varlena *datum, bool *is_toast, bool *need_free, int dbtype, char *dbversion);
extern struct pg_parser_varlena *pg_parser_detoast_datum(struct pg_parser_varlena *datum, bool *is_toast, bool *need_free, int dbtype, char *dbversion);
#endif 
