#ifndef XK_PG_PARSER_THIRDPARTY_BUILTINS_H
#define XK_PG_PARSER_THIRDPARTY_BUILTINS_H


/* src/thirdparty/common/encode.c */
extern unsigned hex_encode(const char *src, unsigned len, char *dst);

/* src/thirdparty/common/numutils.c */
extern void xk_numutils_itoa(int16_t i, char *a);
extern void xk_numutils_ltoa(int32_t l, char *a);
extern void xk_numutils_lltoa(int64_t ll, char *a);
extern char *xk_numutils_ltostr_zeropad(char *str, int32_t value, int32_t minwidth);
extern char *xk_numutils_ltostr(char *str, int32_t value);

/* src/thirdparty/common/rint.c */
extern double rint(double x);

/* src/thirdparty/tupleparser/type_convert/xk_pg_parser_thirdparty_tupleparser_float.c */
extern char *float8out_internal(double num);
extern int strtoint(const char *xk_pg_parser__restrict str, char **xk_pg_parser__restrict endptr, int base);
#endif
