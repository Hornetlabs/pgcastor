#ifndef XK_PG_PARSER_TRANS_DDLSTMT_H
#define XK_PG_PARSER_TRANS_DDLSTMT_H

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
    SYS_HIGHGO_SECLABEL,
    TEMPTABLE_PREFIX
}SysdictName;

typedef struct xk_pg_parser_sysdict_with_name
{
    SysdictName num;
    char *name;
}xk_pg_parser_sysdict_with_name;

static const xk_pg_parser_sysdict_with_name xk_pg_parser_postgresql_v127[] =
{
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
    {SYS_HIGHGO_SECLABEL, "pg_seclabel"},
    {TEMPTABLE_PREFIX, "pg_temp"},
};

static const xk_pg_parser_sysdict_with_name xk_pg_parser_kingbase_v9[] =
{
    {SYS_CLASS, "_rel"},
    {SYS_INDEX, "_ind"},
    {SYS_NAMESPACE, "_nsp"},
    {SYS_ATTRDEF, "_attdef"},
    {SYS_DATABASE, "_db"},
    {SYS_PROC, "_proc"},
    {SYS_TRIGGER, "_trigger"},
    {SYS_TYPE, "_typ"},
    {SYS_CONSTRAINT, "_con"},
    {SYS_ATTRIBUTE, "_att"},
    {SYS_ENUM, "_enum"},
    {SYS_RANGE, "_range"},
    {SYS_DEPEND, "_dep"},
    {SYS_REWRITE, "_rewrite"},
    {SYS_PARTITIONED, "_defpart"},
    {SYS_INHERITS, "_inh"},
    {SYS_SEQUENCE, "_seq"},
    {SYS_HIGHGO_SECLABEL, "pg_seclabel"}, //这里只是占位, 实际用不到
    {TEMPTABLE_PREFIX, "pg_temp"},
};

static const xk_pg_parser_sysdict_with_name xk_pg_parser_uxdb[] =
{
    {SYS_CLASS, "ux_class"},
    {SYS_INDEX, "ux_index"},
    {SYS_NAMESPACE, "ux_namespace"},
    {SYS_ATTRDEF, "ux_attrdef"},
    {SYS_DATABASE, "ux_database"},
    {SYS_PROC, "ux_proc"},
    {SYS_TRIGGER, "ux_trigger"},
    {SYS_TYPE, "ux_type"},
    {SYS_CONSTRAINT, "ux_constraint"},
    {SYS_ATTRIBUTE, "ux_attribute"},
    {SYS_ENUM, "ux_enum"},
    {SYS_RANGE, "ux_range"},
    {SYS_DEPEND, "ux_depend"},
    {SYS_REWRITE, "ux_rewrite"},
    {SYS_PARTITIONED, "ux_partitioned_table"},
    {SYS_INHERITS, "ux_inherits"},
    {SYS_SEQUENCE, "ux_sequence"},
    {SYS_HIGHGO_SECLABEL, "pg_seclabel"}, //这里只是占位, 实际用不到
    {TEMPTABLE_PREFIX, "ux_temp"},
};

#define XK_PG_SYSDICT_PG_TEMPTABLE_NAME             "pg_temp"

#define XK_PG_SYSDICT_UXDB_TEMPTABLE_NAME             "ux_temp"
#define XK_PG_SYSDICT_UXDB_TEMPTABLE_NAME_LARGE       "UX_TEMP"

#define IS_INSERT(x) (XK_PG_PARSER_TRANSLOG_DMLTYPE_INSERT == x->m_base.m_dmltype)
#define IS_DELETE(x) (XK_PG_PARSER_TRANSLOG_DMLTYPE_DELETE == x->m_base.m_dmltype)
#define IS_UPDATE(x) (XK_PG_PARSER_TRANSLOG_DMLTYPE_UPDATE == x->m_base.m_dmltype)

#define XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(colname, values, cnt, return_str) \
        xk_pg_parser_ddl_getColumnValueByName(colname, values, cnt); \
        do \
        { \
            if (!return_str) \
            { \
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_GET_COLUMN_BY_COLNAME; \
                xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, \
                                    "ERROR: DDL get column by column name, " \
                                    "can't get %s\n", colname); \
                return NULL; \
            } \
        } while (0)


#define XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN(colname, values, cnt, return_str) \
        xk_pg_parser_ddl_getColumnValueByName(colname, values, cnt); \
        if (!return_str) \
        do \
        { \
            { \
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_GET_COLUMN_BY_COLNAME; \
                xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, \
                                    "ERROR: DDL get column by column name, " \
                                    "can't get %s\n", colname); \
                return; \
            } \
        } while (0)


typedef enum DDLKind
{
    /* 短解析(一条record就可解析出的DDL) begin */
    XK_PG_PARSER_DDL_TABLE_TRUNCATE = 0,
    XK_PG_PARSER_DDL_REINDEX,
    XK_PG_PARSER_DDL_TABLE_ALTER_TABLE_RENAME,
    XK_PG_PARSER_DDL_TABLE_ALTER_NAMESPACE,
    XK_PG_PARSER_DDL_TABLE_ALTER_DROP_COLUMN,
    XK_PG_PARSER_DDL_TABLE_ALTER_COLUMN_RENAME,
    XK_PG_PARSER_DDL_NAMESPACE_CREATE,
    XK_PG_PARSER_DDL_NAMESPACE_DROP,
    XK_PG_PARSER_DDL_TABLE_ALTER_COLUMN_NOTNULL,
    XK_PG_PARSER_DDL_TABLE_ALTER_COLUMN_NULL,
    XK_PG_PARSER_DDL_DATABASE_CREATE,
    XK_PG_PARSER_DDL_TABLE_ALTER_COLUMN_TYPE_SHORT,
    XK_PG_PARSER_DDL_DATABASE_DROP,
    /* 短解析(一条record就可解析出的DDL) end */
    /* 长解析(多条record才能解析出的DDL) begin */
    XK_PG_PARSER_DDL_VIEW_DROP,
    XK_PG_PARSER_DDL_TABLE_CREATE,
    XK_PG_PARSER_DDL_TABLE_CREATE_PARTITION,
    XK_PG_PARSER_DDL_TABLE_CREATE_PARTITION_SUB,
    XK_PG_PARSER_DDL_TABLE_DROP,
    XK_PG_PARSER_DDL_TABLE_ALTER_TABLE_SET_LOG,
    XK_PG_PARSER_DDL_TABLE_ALTER_ADD_COLUMN,
    XK_PG_PARSER_DDL_TABLE_ALTER_ADD_CONSTRAINT,
    XK_PG_PARSER_DDL_TABLE_ALTER_ADD_CONSTRAINT_FOREIGN,
    XK_PG_PARSER_DDL_TABLE_ALTER_DROP_CONSTRAINT,
    XK_PG_PARSER_DDL_TABLE_ALTER_COLUMN_TYPE,
    XK_PG_PARSER_DDL_TABLE_ALTER_COLUMN_DEFAULT,
    XK_PG_PARSER_DDL_TABLE_ALTER_COLUMN_DROP_DEFAULT,
    XK_PG_PARSER_DDL_INDEX_CREATE,
    XK_PG_PARSER_DDL_INDEX_DROP,
    XK_PG_PARSER_DDL_SEQUENCE_CREATE,
    XK_PG_PARSER_DDL_SEQUENCE_DROP,
    XK_PG_PARSER_DDL_TOAST_ESCAPE,
    XK_PG_PARSER_DDL_TOAST_INDEX_DROP_ESCAPE,
    XK_PG_PARSER_DDL_VIEW_CREATE,
    XK_PG_PARSER_DDL_FUNCTION_CREATE,
    XK_PG_PARSER_DDL_FUNCTION_DROP,
    XK_PG_PARSER_DDL_TRIGGER_CREATE,
    XK_PG_PARSER_DDL_TRIGGER_DROP,
    XK_PG_PARSER_DDL_TYPE_CREATE,
    XK_PG_PARSER_DDL_TYPE_DROP,
    /* 长解析(多条record才能解析出的DDL) end */
} DDLKind;

#define XK_PG_PARSER_DDL_TRANSDDL_NUM_SHORT XK_PG_PARSER_DDL_DATABASE_DROP + 1
#define XK_PG_PARSER_DDL_TRANSDDL_NUM_MAX XK_PG_PARSER_DDL_TYPE_DROP + 1


#define XK_PG_PARSER_DDL_ALTER_LOG_GET_CLASS_UPDATE_BEGIN                       0x00
#define XK_PG_PARSER_DDL_ALTER_LOG_GET_CLASS_UPDATE_RELPERSISTENCE_STEP         0x01
#define XK_PG_PARSER_DDL_ALTER_LOG_GET_TEMP_CLASS_UPDATE_STEP                   0x02
#define XK_PG_PARSER_DDL_ALTER_LOG_GET_TEMP_CLASS_DELETE_STEP                   0x03

typedef struct xk_pg_parser_ddlstate
{
    DDLKind     m_ddlKind;
    bool        m_inddl;

    /* 表名和命名空间名称。 */
    char        *m_relname;
    char        *m_nspname;
    uint32_t     m_reloid;
    uint32_t     m_owner;
    char        *m_reloid_char;
    uint32_t     m_nspname_oid;
    char        *m_nspname_oid_char;
    char         m_relpersistence;
    xk_pg_parser_List *m_attList;
    /* 表继承父表oid数组 */
    uint32_t     m_inherits_cnt;
    uint32_t     *m_inherits_oid;
    /* 表是否被日志记录 */
    bool         m_unlogged;
    /* 约束是否是本地 */
    bool         m_cons_is_local;
    /* 表是否为临时表 */
    bool         m_temp;
    /* 修改表是否被日志记录时的步骤标志 */
    uint32_t     m_reloid_temp;
    uint8_t      m_log_step;

    /* 表重命名。 */
    char        *m_nspoid_new;
    char        *m_relname_new;
    uint32_t     m_relfile_node_new;
    uint8_t      m_alter_coltype_step;

    /* 分区表 */
    bool                                m_get_partition;
    xk_pg_parser_translog_tbcol_values *m_partition;
    xk_pg_parser_nodetree *m_partitionsub_node;
    xk_pg_parser_translog_tbcol_values *m_inherits;
    xk_pg_parser_List                  *m_attrdef_list;

    /* 表列重命名和默认值 存储一条pg_attribute信息 */
    xk_pg_parser_translog_tbcol_values *m_att;

    /* 索引相关 */
    xk_pg_parser_translog_tbcol_values *m_index;
    char                               *m_amoid_char;

    /* 序列相关 */
    xk_pg_parser_translog_tbcol_values *m_sequence;

    /* 存储默认值信息。 */
    xk_pg_parser_translog_tbcol_values *m_att_def;

    /* 存储约束信息。 */
    xk_pg_parser_translog_tbcol_values *m_constraint;

    /* 存储函数定义信息。 */
    char         *m_func_def;

    /* 存储触发器定义信息。 */
    char         *m_trig_def;

    /* 存储类型信息。 */
    xk_pg_parser_translog_tbcol_values *m_type_item;
    xk_pg_parser_translog_tbcol_values *m_type_sub_item;
    int16_t                             m_type_record_natts;
    int16_t                             m_type_current_natts;
    xk_pg_parser_List                  *m_enumlist;
    char                               *m_type_domain;
    uint8_t                             m_type_savepoint;
    xk_pg_parser_translog_convertinfo_with_zic *m_zicinfo;

    /* 存储视图定义信息。 */
    xk_pg_parser_translog_tbcol_values *m_view_def;
    char        m_relkind;
}xk_pg_parser_ddlstate;

typedef xk_pg_parser_translog_ddlstmt* (*xk_pg_parser_DDL_transDDLFunc)(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                              xk_pg_parser_translog_systb2dll_record *current_record,
                                              xk_pg_parser_ddlstate *ddlstate,
                                              int32_t *xk_pg_parser_errno);

extern bool xk_pg_parser_DDL_transRecord2DDL(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                             xk_pg_parser_translog_ddlstmt **xk_pg_parser_ddl_result,
                                             int32_t *xk_pg_parser_errno);

extern char *ddl_char_tolower(char *output);

extern bool xk_pg_parser_check_table_name(char *tablename,
                                          SysdictName sysdictnum,
                                          int16_t dbtype,
                                          char *dbversion);
#endif
