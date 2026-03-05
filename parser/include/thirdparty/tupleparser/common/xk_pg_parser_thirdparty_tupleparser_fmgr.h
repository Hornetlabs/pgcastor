#ifndef XK_PG_PARSER_THIRDPARTY_TUPLEPARSER_FMGR_H
#define XK_PG_PARSER_THIRDPARTY_TUPLEPARSER_FMGR_H

/* 函数声明区 */
extern bool xk_pg_parser_convert_attr_to_str_value(xk_pg_parser_Datum attr,
                                                   xk_pg_parser_sysdicts *sysdicts,
                                                   xk_pg_parser_translog_tbcol_value *colvalue,
                                                   xk_pg_parser_translog_convertinfo_with_zic *zicinfo);

extern char *xk_pg_parser_convert_attr_to_str_char(xk_pg_parser_Datum attr,
                                                   xk_pg_parser_sysdicts *sysdicts,
                                                   uint32_t typid,
                                                   bool *istoast,
                                                   xk_pg_parser_translog_convertinfo_with_zic *zicinfo);
extern bool xk_pg_parser_convert_attr_to_str_external_value(xk_pg_parser_Datum attr,
                                                     char *typoutput,
                                                     xk_pg_parser_translog_tbcol_value *colvalue,
                                                     xk_pg_parser_translog_convertinfo_with_zic *zicinfo);
extern char *xk_pg_parser_convert_attr_to_str_by_typid_typoptput(xk_pg_parser_Datum attr,
                                                          uint32_t typid,
                                                          char *typoutput,
                                                          xk_pg_parser_translog_convertinfo_with_zic *zicinfo);
extern char *xk_pg_parser_timestamptz_to_str(int64_t t);
#endif
