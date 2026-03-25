#ifndef PG_PARSER_THIRDPARTY_BUILTINS_H
#define PG_PARSER_THIRDPARTY_BUILTINS_H

/* src/thirdparty/common/encode.c */
extern unsigned hex_encode(const char* src, unsigned len, char* dst);

/* src/thirdparty/common/numutils.c */
extern void  numutils_itoa(int16_t i, char* a);
extern void  numutils_ltoa(int32_t l, char* a);
extern void  numutils_lltoa(int64_t ll, char* a);
extern char* numutils_ltostr_zeropad(char* str, int32_t value, int32_t minwidth);
extern char* numutils_ltostr(char* str, int32_t value);

/* src/thirdparty/common/rint.c */
extern double rint(double x);

/* src/thirdparty/tupleparser/type_convert/pg_parser_thirdparty_tupleparser_float.c */
extern char* float8out_internal(double num);
extern int   strtoint(const char* pg_parser__restrict str, char** pg_parser__restrict endptr,
                      int base);
#endif
