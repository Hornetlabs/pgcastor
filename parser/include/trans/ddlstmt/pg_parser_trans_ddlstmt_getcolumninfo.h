#ifndef XK_PG_PARSER_TRANS_DDLSTMT_GETCOLUMNINFO_H
#define XK_PG_PARSER_TRANS_DDLSTMT_GETCOLUMNINFO_H

extern char *xk_pg_parser_ddl_getColumnValueByName(char *column_name,
                                                   xk_pg_parser_translog_tbcol_value *record_value,
                                                   uint16_t cnt);

extern bool xk_pg_parser_ddl_get_pg_class_info(xk_pg_parser_ddlstate *ddlstate,
                                               xk_pg_parser_translog_tbcol_values *record,
                                               bool isnew);

extern bool xk_pg_parser_ddl_get_attribute_info(xk_pg_parser_ddlstate *ddlstate,
                                                xk_pg_parser_translog_tbcol_values *record,
                                                bool isnew);

extern bool xk_pg_parser_ddl_checkChangeColumn(char *column_name,
                                               xk_pg_parser_translog_tbcol_value *record_new,
                                               xk_pg_parser_translog_tbcol_value *record_old,
                                               uint16_t cnt,
                                               int32_t *xk_pg_parser_errno);
#if 0
extern bool xk_pg_parser_ddl_getTypnameByOid(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                       uint32_t typid,
                                       char **typname);

extern bool xk_pg_parser_ddl_getRelname(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                        char *reloid_char,
                                        char **relname);

extern bool xk_pg_parser_ddl_getNameSpace(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                          char *nspoid_char,
                                          char **nspname);

extern bool xk_pg_parser_ddl_getRelNameSpace(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                             char *reloid_char,
                                             char **nspname);
#endif
#endif
