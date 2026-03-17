#ifndef XK_PG_PARSER_TRANS_DDLSTMT_TRANSFUNC_H
#define XK_PG_PARSER_TRANS_DDLSTMT_TRANSFUNC_H

/* 初始化ddlstate */
extern void xk_pg_parser_ddl_init_ddlstate(xk_pg_parser_ddlstate *ddlstate);


/* 解析第一条DDL record的函数 */
extern xk_pg_parser_translog_ddlstmt *xk_pg_parser_ddl_firstTransDDL(
                                           xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                           xk_pg_parser_translog_systb2dll_record *current_record,
                                           xk_pg_parser_ddlstate *ddlstate,
                                           int32_t *xk_pg_parser_errno);

extern void xk_pg_parser_ddl_insertRecordTrans(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                               xk_pg_parser_translog_systb2dll_record *current_record,
                                               xk_pg_parser_ddlstate *ddlstate,
                                               int32_t *xk_pg_parser_errno);

extern void xk_pg_parser_ddl_updateRecordTrans(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                               xk_pg_parser_translog_systb2dll_record *current_record,
                                               xk_pg_parser_ddlstate *ddlstate,
                                               int32_t *xk_pg_parser_errno);

extern void xk_pg_parser_ddl_deleteRecordTrans(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                               xk_pg_parser_translog_systb2dll_record *current_record,
                                               xk_pg_parser_ddlstate *ddlstate,
                                               int32_t *xk_pg_parser_errno);

/* DDL解析函数 */
extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_truncate(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                        xk_pg_parser_translog_systb2dll_record *current_record,
                                                        xk_pg_parser_ddlstate *ddlstate,
                                                        int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_alter_table_renameTable(
                                                        xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                        xk_pg_parser_translog_systb2dll_record *current_record,
                                                        xk_pg_parser_ddlstate *ddlstate,
                                                        int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_alter_table_drop_column(
                                                        xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                        xk_pg_parser_translog_systb2dll_record *current_record,
                                                        xk_pg_parser_ddlstate *ddlstate,
                                                        int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_alter_table_rename_column(
                                                        xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                        xk_pg_parser_translog_systb2dll_record *current_record,
                                                        xk_pg_parser_ddlstate *ddlstate,
                                                        int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_create_schema(
                                                        xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                        xk_pg_parser_translog_systb2dll_record *current_record,
                                                        xk_pg_parser_ddlstate *ddlstate,
                                                        int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_drop_schema(
                                                        xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                        xk_pg_parser_translog_systb2dll_record *current_record,
                                                        xk_pg_parser_ddlstate *ddlstate,
                                                        int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_alter_table_column_notnull(
                                                        xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                        xk_pg_parser_translog_systb2dll_record *current_record,
                                                        xk_pg_parser_ddlstate *ddlstate,
                                                        int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_alter_table_column_null(
                                                        xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                        xk_pg_parser_translog_systb2dll_record *current_record,
                                                        xk_pg_parser_ddlstate *ddlstate,
                                                        int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_create_database(
                                                        xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                        xk_pg_parser_translog_systb2dll_record *current_record,
                                                        xk_pg_parser_ddlstate *ddlstate,
                                                        int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_drop_database(
                                                        xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                        xk_pg_parser_translog_systb2dll_record *current_record,
                                                        xk_pg_parser_ddlstate *ddlstate,
                                                        int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_create_type(
                                                        xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                        xk_pg_parser_translog_systb2dll_record *current_record,
                                                        xk_pg_parser_ddlstate *ddlstate,
                                                        int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_drop_type(
                                                        xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                        xk_pg_parser_translog_systb2dll_record *current_record,
                                                        xk_pg_parser_ddlstate *ddlstate,
                                                        int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_create_table(
                                                            xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_create_view(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_drop_view(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                          xk_pg_parser_translog_systb2dll_record *current_record,
                                                          xk_pg_parser_ddlstate *ddlstate,
                                                          int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_drop_table(
                                            xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                            xk_pg_parser_ddlstate *ddlstate,
                                            int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_create_table_partition(
                                                        xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                        xk_pg_parser_translog_systb2dll_record *current_record,
                                                        xk_pg_parser_ddlstate *ddlstate,
                                                        int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_create_table_partition_sub(
                                                        xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                        xk_pg_parser_translog_systb2dll_record *current_record,
                                                        xk_pg_parser_ddlstate *ddlstate,
                                                        int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_create_index(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_alter_table_add_constraint(
                                                            xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_alter_table_add_constraint_foreign(
                                                            xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_alter_table_namespace(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_alter_table_column_add_default(
                                                            xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_alter_table_column_drop_default(
                                                            xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_drop_index(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                          xk_pg_parser_translog_systb2dll_record *current_record,
                                                          xk_pg_parser_ddlstate *ddlstate,
                                                          int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_alter_table_drop_constraint(
                                                            xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_alter_table_set_log(
                                                            xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_alter_table_add_column(
                                                            xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_create_sequence(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_drop_sequence(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_alter_table_column_alter_type(
                                                            xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_alter_table_column_alter_type_short(
                                                            xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno);

extern xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_reindex(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                         xk_pg_parser_translog_systb2dll_record *current_record,
                                                         xk_pg_parser_ddlstate *ddlstate,
                                                         int32_t *xk_pg_parser_errno);

#endif
