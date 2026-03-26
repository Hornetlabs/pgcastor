#ifndef PG_PARSER_TRANS_DDLSTMT_GETCOLUMNINFO_H
#define PG_PARSER_TRANS_DDLSTMT_GETCOLUMNINFO_H

extern char* pg_parser_ddl_getColumnValueByName(char*                           column_name,
                                                pg_parser_translog_tbcol_value* record_value,
                                                uint16_t                        cnt);

extern bool pg_parser_ddl_get_pg_class_info(pg_parser_ddlstate*              ddlstate,
                                            pg_parser_translog_tbcol_values* record,
                                            bool                             isnew);

extern bool pg_parser_ddl_get_attribute_info(pg_parser_ddlstate*              ddlstate,
                                             pg_parser_translog_tbcol_values* record,
                                             bool                             isnew);

extern bool pg_parser_ddl_checkChangeColumn(char*                           column_name,
                                            pg_parser_translog_tbcol_value* record_new,
                                            pg_parser_translog_tbcol_value* record_old,
                                            uint16_t                        cnt,
                                            int32_t*                        pg_parser_errno);

#endif
