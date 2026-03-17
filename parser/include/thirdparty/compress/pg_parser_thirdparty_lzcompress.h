#ifndef XK_PG_PARSER_THIRDPARTY_LZCOMPRESS_H
#define XK_PG_PARSER_THIRDPARTY_LZCOMPRESS_H

extern int32_t xk_pg_parser_lz_decompress(const char *source, int32_t slen, char *dest,
                             int32_t rawsize, bool check_complete);

extern int32_t xk_pg_parser_pg14_pglz_decompress(const char *source,
                                                 int32_t slen,
                                                 char *dest,
                                                 int32_t rawsize,
                                                 bool check_complete);

#endif
