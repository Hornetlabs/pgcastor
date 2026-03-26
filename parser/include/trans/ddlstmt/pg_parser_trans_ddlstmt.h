#ifndef PG_PARSER_TRANS_DDLSTMT_H
#define PG_PARSER_TRANS_DDLSTMT_H

typedef enum SysdictName
{
    SYS_CLASS = 0,
    SYS_INDEX,
    SYS_NAMESPACE,
    SYS_ATTRDEF,
    SYS_DATABASE,
    SYS_PROC,
    SYS_TRIGGER,
    SYS_TYPE,
    SYS_CONSTRAINT,
    SYS_ATTRIBUTE,
    SYS_ENUM,
    SYS_RANGE,
    SYS_DEPEND,
    SYS_REWRITE,
    SYS_PARTITIONED,
    SYS_INHERITS,
    SYS_SEQUENCE,
    TEMPTABLE_PREFIX
} SysdictName;

typedef struct pg_parser_sysdict_with_name
{
    SysdictName num;
    char*       name;
} pg_parser_sysdict_with_name;

static const pg_parser_sysdict_with_name pg_parser_postgresql_v127[] = {
    {SYS_CLASS, "pg_class"},
    {SYS_INDEX, "pg_index"},
    {SYS_NAMESPACE, "pg_namespace"},
    {SYS_ATTRDEF, "pg_attrdef"},
    {SYS_DATABASE, "pg_database"},
    {SYS_PROC, "pg_proc"},
    {SYS_TRIGGER, "pg_trigger"},
    {SYS_TYPE, "pg_type"},
    {SYS_CONSTRAINT, "pg_constraint"},
    {SYS_ATTRIBUTE, "pg_attribute"},
    {SYS_ENUM, "pg_enum"},
    {SYS_RANGE, "pg_range"},
    {SYS_DEPEND, "pg_depend"},
    {SYS_REWRITE, "pg_rewrite"},
    {SYS_PARTITIONED, "pg_partitioned_table"},
    {SYS_INHERITS, "pg_inherits"},
    {SYS_SEQUENCE, "pg_sequence"},
    {TEMPTABLE_PREFIX, "pg_temp"},
};

#define PG_SYSDICT_PG_TEMPTABLE_NAME "pg_temp"

#define IS_INSERT(x)                 (PG_PARSER_TRANSLOG_DMLTYPE_INSERT == x->m_base.m_dmltype)
#define IS_DELETE(x)                 (PG_PARSER_TRANSLOG_DMLTYPE_DELETE == x->m_base.m_dmltype)
#define IS_UPDATE(x)                 (PG_PARSER_TRANSLOG_DMLTYPE_UPDATE == x->m_base.m_dmltype)

#define PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(colname, values, cnt, return_str)  \
    pg_parser_ddl_getColumnValueByName(colname, values, cnt);                 \
    do                                                                        \
    {                                                                         \
        if (!return_str)                                                      \
        {                                                                     \
            *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_GET_COLUMN_BY_COLNAME; \
            pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,                 \
                                 "ERROR: DDL get column by column name, "     \
                                 "can't get %s\n",                            \
                                 colname);                                    \
            return NULL;                                                      \
        }                                                                     \
    } while (0)

#define PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN(colname, values, cnt, return_str) \
    pg_parser_ddl_getColumnValueByName(colname, values, cnt);                          \
    if (!return_str)                                                                   \
        do                                                                             \
        {                                                                              \
            {                                                                          \
                *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_GET_COLUMN_BY_COLNAME;      \
                pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,                      \
                                     "ERROR: DDL get column by column name, "          \
                                     "can't get %s\n",                                 \
                                     colname);                                         \
                return;                                                                \
            }                                                                          \
    } while (0)

typedef enum DDLKind
{
    /* Short parse (DDL that can be parsed from a single record) begin */
    PG_PARSER_DDL_TABLE_TRUNCATE = 0,
    PG_PARSER_DDL_REINDEX,
    PG_PARSER_DDL_TABLE_ALTER_TABLE_RENAME,
    PG_PARSER_DDL_TABLE_ALTER_NAMESPACE,
    PG_PARSER_DDL_TABLE_ALTER_DROP_COLUMN,
    PG_PARSER_DDL_TABLE_ALTER_COLUMN_RENAME,
    PG_PARSER_DDL_NAMESPACE_CREATE,
    PG_PARSER_DDL_NAMESPACE_DROP,
    PG_PARSER_DDL_TABLE_ALTER_COLUMN_NOTNULL,
    PG_PARSER_DDL_TABLE_ALTER_COLUMN_NULL,
    PG_PARSER_DDL_DATABASE_CREATE,
    PG_PARSER_DDL_TABLE_ALTER_COLUMN_TYPE_SHORT,
    PG_PARSER_DDL_DATABASE_DROP,
    /* Short parse (DDL that can be parsed from a single record) end */
    /* Long parse (DDL that requires multiple records to parse) begin */
    PG_PARSER_DDL_VIEW_DROP,
    PG_PARSER_DDL_TABLE_CREATE,
    PG_PARSER_DDL_TABLE_CREATE_PARTITION,
    PG_PARSER_DDL_TABLE_CREATE_PARTITION_SUB,
    PG_PARSER_DDL_TABLE_DROP,
    PG_PARSER_DDL_TABLE_ALTER_TABLE_SET_LOG,
    PG_PARSER_DDL_TABLE_ALTER_ADD_COLUMN,
    PG_PARSER_DDL_TABLE_ALTER_ADD_CONSTRAINT,
    PG_PARSER_DDL_TABLE_ALTER_ADD_CONSTRAINT_FOREIGN,
    PG_PARSER_DDL_TABLE_ALTER_DROP_CONSTRAINT,
    PG_PARSER_DDL_TABLE_ALTER_COLUMN_TYPE,
    PG_PARSER_DDL_TABLE_ALTER_COLUMN_DEFAULT,
    PG_PARSER_DDL_TABLE_ALTER_COLUMN_DROP_DEFAULT,
    PG_PARSER_DDL_INDEX_CREATE,
    PG_PARSER_DDL_INDEX_DROP,
    PG_PARSER_DDL_SEQUENCE_CREATE,
    PG_PARSER_DDL_SEQUENCE_DROP,
    PG_PARSER_DDL_TOAST_ESCAPE,
    PG_PARSER_DDL_TOAST_INDEX_DROP_ESCAPE,
    PG_PARSER_DDL_VIEW_CREATE,
    PG_PARSER_DDL_FUNCTION_CREATE,
    PG_PARSER_DDL_FUNCTION_DROP,
    PG_PARSER_DDL_TRIGGER_CREATE,
    PG_PARSER_DDL_TRIGGER_DROP,
    PG_PARSER_DDL_TYPE_CREATE,
    PG_PARSER_DDL_TYPE_DROP,
    /* Long parse (DDL that requires multiple records to parse) end */
} DDLKind;

#define PG_PARSER_DDL_TRANSDDL_NUM_SHORT                             PG_PARSER_DDL_DATABASE_DROP + 1
#define PG_PARSER_DDL_TRANSDDL_NUM_MAX                               PG_PARSER_DDL_TYPE_DROP + 1

#define PG_PARSER_DDL_ALTER_LOG_GET_CLASS_UPDATE_BEGIN               0x00
#define PG_PARSER_DDL_ALTER_LOG_GET_CLASS_UPDATE_RELPERSISTENCE_STEP 0x01
#define PG_PARSER_DDL_ALTER_LOG_GET_TEMP_CLASS_UPDATE_STEP           0x02
#define PG_PARSER_DDL_ALTER_LOG_GET_TEMP_CLASS_DELETE_STEP           0x03

typedef struct pg_parser_ddlstate
{
    DDLKind                                  m_ddlKind;
    bool                                     m_inddl;

    /* Table name and namespace name. */
    char*                                    m_relname;
    char*                                    m_nspname;
    uint32_t                                 m_reloid;
    uint32_t                                 m_owner;
    char*                                    m_reloid_char;
    uint32_t                                 m_nspname_oid;
    char*                                    m_nspname_oid_char;
    char                                     m_relpersistence;
    pg_parser_List*                          m_attList;
    /* Array of parent table OIDs for table inheritance */
    uint32_t                                 m_inherits_cnt;
    uint32_t*                                m_inherits_oid;
    /* Whether table is logged */
    bool                                     m_unlogged;
    /* Whether constraint is local */
    bool                                     m_cons_is_local;
    /* Whether table is temporary */
    bool                                     m_temp;
    /* Step flag when modifying whether table is logged */
    uint32_t                                 m_reloid_temp;
    uint8_t                                  m_log_step;

    /* Table rename. */
    char*                                    m_nspoid_new;
    char*                                    m_relname_new;
    uint32_t                                 m_relfile_node_new;
    uint8_t                                  m_alter_coltype_step;

    /* Partitioned table */
    bool                                     m_get_partition;
    pg_parser_translog_tbcol_values*         m_partition;
    pg_parser_nodetree*                      m_partitionsub_node;
    pg_parser_translog_tbcol_values*         m_inherits;
    pg_parser_List*                          m_attrdef_list;

    /* Table column rename and default value, store one pg_attribute information */
    pg_parser_translog_tbcol_values*         m_att;

    /* Index related */
    pg_parser_translog_tbcol_values*         m_index;
    char*                                    m_amoid_char;

    /* Sequence related */
    pg_parser_translog_tbcol_values*         m_sequence;

    /* Store default value information. */
    pg_parser_translog_tbcol_values*         m_att_def;

    /* Store constraint information. */
    pg_parser_translog_tbcol_values*         m_constraint;

    /* Store function definition information. */
    char*                                    m_func_def;

    /* Store trigger definition information. */
    char*                                    m_trig_def;

    /* Store type information. */
    pg_parser_translog_tbcol_values*         m_type_item;
    pg_parser_translog_tbcol_values*         m_type_sub_item;
    int16_t                                  m_type_record_natts;
    int16_t                                  m_type_current_natts;
    pg_parser_List*                          m_enumlist;
    char*                                    m_type_domain;
    uint8_t                                  m_type_savepoint;
    pg_parser_translog_convertinfo_with_zic* m_zicinfo;

    /* Store view definition information. */
    pg_parser_translog_tbcol_values*         m_view_def;
    char                                     m_relkind;
} pg_parser_ddlstate;

typedef pg_parser_translog_ddlstmt* (*pg_parser_DDL_transDDLFunc)(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record,
    pg_parser_ddlstate*                  ddlstate,
    int32_t*                             pg_parser_errno);

extern bool pg_parser_DDL_transRecord2DDL(pg_parser_translog_systb2ddl* pg_parser_ddl,
                                          pg_parser_translog_ddlstmt**  pg_parser_ddl_result,
                                          int32_t*                      pg_parser_errno);

extern char* ddl_char_tolower(char* output);

extern bool pg_parser_check_table_name(char*       tablename,
                                       SysdictName sysdictnum,
                                       int16_t     dbtype,
                                       char*       dbversion);
#endif
