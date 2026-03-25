#ifndef PG_PARSER_TRANS_DDLSTMT_TRANSFUNC_H
#define PG_PARSER_TRANS_DDLSTMT_TRANSFUNC_H

/* Initialize ddlstate */
extern void pg_parser_ddl_init_ddlstate(pg_parser_ddlstate* ddlstate);

/* Function to parse the first DDL record */
extern pg_parser_translog_ddlstmt* pg_parser_ddl_firstTransDDL(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern void pg_parser_ddl_insertRecordTrans(pg_parser_translog_systb2ddl*        pg_parser_ddl,
                                            pg_parser_translog_systb2dll_record* current_record,
                                            pg_parser_ddlstate* ddlstate, int32_t* pg_parser_errno);

extern void pg_parser_ddl_updateRecordTrans(pg_parser_translog_systb2ddl*        pg_parser_ddl,
                                            pg_parser_translog_systb2dll_record* current_record,
                                            pg_parser_ddlstate* ddlstate, int32_t* pg_parser_errno);

extern void pg_parser_ddl_deleteRecordTrans(pg_parser_translog_systb2ddl*        pg_parser_ddl,
                                            pg_parser_translog_systb2dll_record* current_record,
                                            pg_parser_ddlstate* ddlstate, int32_t* pg_parser_errno);

/* DDL parse function */
extern pg_parser_translog_ddlstmt* pg_parser_DDL_truncate(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_renameTable(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_drop_column(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_rename_column(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_create_schema(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_drop_schema(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_column_notnull(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_column_null(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_create_database(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_drop_database(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_create_type(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_drop_type(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_create_table(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_create_view(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_drop_view(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_drop_table(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_create_table_partition(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_create_table_partition_sub(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_create_index(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_add_constraint(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_add_constraint_foreign(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_namespace(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_column_add_default(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_column_drop_default(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_drop_index(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_drop_constraint(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_set_log(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_add_column(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_create_sequence(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_drop_sequence(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_column_alter_type(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_column_alter_type_short(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

extern pg_parser_translog_ddlstmt* pg_parser_DDL_reindex(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

#endif
