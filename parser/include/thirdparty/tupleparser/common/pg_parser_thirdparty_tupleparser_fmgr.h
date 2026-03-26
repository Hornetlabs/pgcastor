#ifndef PG_PARSER_THIRDPARTY_TUPLEPARSER_FMGR_H
#define PG_PARSER_THIRDPARTY_TUPLEPARSER_FMGR_H

/* function declaration */
extern bool pg_parser_convert_attr_to_str_value(pg_parser_Datum                          attr,
                                                pg_parser_sysdicts*                      sysdicts,
                                                pg_parser_translog_tbcol_value*          colvalue,
                                                pg_parser_translog_convertinfo_with_zic* zicinfo);

extern char* pg_parser_convert_attr_to_str_char(pg_parser_Datum                          attr,
                                                pg_parser_sysdicts*                      sysdicts,
                                                uint32_t                                 typid,
                                                bool*                                    istoast,
                                                pg_parser_translog_convertinfo_with_zic* zicinfo);
extern bool pg_parser_convert_attr_to_str_external_value(
    pg_parser_Datum                          attr,
    char*                                    typoutput,
    pg_parser_translog_tbcol_value*          colvalue,
    pg_parser_translog_convertinfo_with_zic* zicinfo);
extern char* pg_parser_convert_attr_to_str_by_typid_typoptput(
    pg_parser_Datum                          attr,
    uint32_t                                 typid,
    char*                                    typoutput,
    pg_parser_translog_convertinfo_with_zic* zicinfo);
extern char* pg_parser_timestamptz_to_str(int64_t t);
#endif
