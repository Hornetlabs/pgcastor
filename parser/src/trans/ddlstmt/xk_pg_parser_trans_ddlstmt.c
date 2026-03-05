#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "common/xk_pg_parser_translog.h"
#include "thirdparty/list/xk_pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_transfunc.h"

#define XK_PG_PARSER_DDLSTMT_MCXT NULL

typedef struct xk_pg_parser_DDL_kindWithFunc
{
    DDLKind m_ddlKind;
    xk_pg_parser_DDL_transDDLFunc m_transfunc;
}xk_pg_parser_DDL_kindWithFunc;

static xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_toast_escape(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno);

static xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_toast_index_drop_escape(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno);

static xk_pg_parser_DDL_kindWithFunc m_ddl_transfunc[] = 
{
    {XK_PG_PARSER_DDL_TABLE_TRUNCATE, xk_pg_parser_DDL_truncate},
    {XK_PG_PARSER_DDL_TABLE_ALTER_COLUMN_TYPE_SHORT, xk_pg_parser_DDL_alter_table_column_alter_type_short},
    {XK_PG_PARSER_DDL_REINDEX, xk_pg_parser_DDL_reindex},
    {XK_PG_PARSER_DDL_TABLE_ALTER_TABLE_RENAME, xk_pg_parser_DDL_alter_table_renameTable},
    {XK_PG_PARSER_DDL_TABLE_ALTER_NAMESPACE, xk_pg_parser_DDL_alter_table_namespace},
    {XK_PG_PARSER_DDL_TABLE_ALTER_DROP_COLUMN, xk_pg_parser_DDL_alter_table_drop_column},
    {XK_PG_PARSER_DDL_TABLE_ALTER_COLUMN_RENAME, xk_pg_parser_DDL_alter_table_rename_column},
    {XK_PG_PARSER_DDL_NAMESPACE_CREATE, xk_pg_parser_DDL_create_schema},
    {XK_PG_PARSER_DDL_NAMESPACE_DROP, xk_pg_parser_DDL_drop_schema},
    {XK_PG_PARSER_DDL_TABLE_ALTER_COLUMN_NOTNULL, xk_pg_parser_DDL_alter_table_column_notnull},
    {XK_PG_PARSER_DDL_TABLE_ALTER_COLUMN_NULL, xk_pg_parser_DDL_alter_table_column_null},
    {XK_PG_PARSER_DDL_DATABASE_CREATE, xk_pg_parser_DDL_create_database},
    {XK_PG_PARSER_DDL_DATABASE_DROP, xk_pg_parser_DDL_drop_database},
    {XK_PG_PARSER_DDL_VIEW_DROP, xk_pg_parser_DDL_drop_view},
    {XK_PG_PARSER_DDL_TABLE_CREATE, xk_pg_parser_DDL_create_table},
    {XK_PG_PARSER_DDL_TABLE_CREATE_PARTITION, xk_pg_parser_DDL_create_table_partition},
    {XK_PG_PARSER_DDL_TABLE_CREATE_PARTITION_SUB, xk_pg_parser_DDL_create_table_partition_sub}, /* 含有pg_node_tree */
    {XK_PG_PARSER_DDL_TABLE_DROP, xk_pg_parser_DDL_drop_table},
    {XK_PG_PARSER_DDL_TABLE_ALTER_TABLE_SET_LOG, xk_pg_parser_DDL_alter_table_set_log},
    {XK_PG_PARSER_DDL_TABLE_ALTER_ADD_COLUMN, xk_pg_parser_DDL_alter_table_add_column},
    {XK_PG_PARSER_DDL_TABLE_ALTER_ADD_CONSTRAINT, xk_pg_parser_DDL_alter_table_add_constraint},
    {XK_PG_PARSER_DDL_TABLE_ALTER_ADD_CONSTRAINT_FOREIGN, xk_pg_parser_DDL_alter_table_add_constraint_foreign},
    {XK_PG_PARSER_DDL_TABLE_ALTER_DROP_CONSTRAINT, xk_pg_parser_DDL_alter_table_drop_constraint},
    {XK_PG_PARSER_DDL_TABLE_ALTER_COLUMN_TYPE, xk_pg_parser_DDL_alter_table_column_alter_type},
    {XK_PG_PARSER_DDL_TABLE_ALTER_COLUMN_DEFAULT, xk_pg_parser_DDL_alter_table_column_add_default}, /* 含有pg_node_tree */
    {XK_PG_PARSER_DDL_TABLE_ALTER_COLUMN_DROP_DEFAULT, xk_pg_parser_DDL_alter_table_column_drop_default},
    {XK_PG_PARSER_DDL_INDEX_CREATE, xk_pg_parser_DDL_create_index},
    {XK_PG_PARSER_DDL_INDEX_DROP, xk_pg_parser_DDL_drop_index},
    {XK_PG_PARSER_DDL_TOAST_INDEX_DROP_ESCAPE, xk_pg_parser_DDL_toast_index_drop_escape},
    {XK_PG_PARSER_DDL_SEQUENCE_CREATE, xk_pg_parser_DDL_create_sequence},
    {XK_PG_PARSER_DDL_SEQUENCE_DROP, xk_pg_parser_DDL_drop_sequence},
    {XK_PG_PARSER_DDL_TOAST_ESCAPE, xk_pg_parser_DDL_toast_escape},
    {XK_PG_PARSER_DDL_VIEW_CREATE, xk_pg_parser_DDL_create_view}, /* 含有pg_node_tree */
    {XK_PG_PARSER_DDL_FUNCTION_CREATE, NULL},
    {XK_PG_PARSER_DDL_FUNCTION_DROP, NULL},
    {XK_PG_PARSER_DDL_TRIGGER_CREATE, NULL},
    {XK_PG_PARSER_DDL_TRIGGER_DROP, NULL},
    {XK_PG_PARSER_DDL_TYPE_CREATE, xk_pg_parser_DDL_create_type},
    {XK_PG_PARSER_DDL_TYPE_DROP, xk_pg_parser_DDL_drop_type}
};

bool xk_pg_parser_DDL_transRecord2DDL(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                      xk_pg_parser_translog_ddlstmt **xk_pg_parser_ddl_result,
                                      int32_t *xk_pg_parser_errno)
{
    int32_t i = 0;
    xk_pg_parser_ddlstate ddlstate = {'\0'};
    xk_pg_parser_translog_convertinfo_with_zic zicinfo = {'\0'};
    xk_pg_parser_translog_systb2dll_record *current_record = xk_pg_parser_ddl->m_record;
    xk_pg_parser_translog_ddlstmt *result = NULL,
                                  *current_result = NULL,
                                  *next_result = NULL;
    zicinfo.convertinfo = xk_pg_parser_ddl->m_convert;
    zicinfo.dbtype = xk_pg_parser_ddl->m_dbtype;
    zicinfo.dbversion = xk_pg_parser_ddl->m_dbversion;
    zicinfo.errorno = xk_pg_parser_errno;
    zicinfo.debuglevel = xk_pg_parser_ddl->m_debugLevel;

    ddlstate.m_zicinfo = &zicinfo;

    /* 确保错误码为0 */
    *xk_pg_parser_errno = 0;

    while (current_record)
    {
        if (!ddlstate.m_inddl)
            current_result = xk_pg_parser_ddl_firstTransDDL(xk_pg_parser_ddl, current_record, &ddlstate, xk_pg_parser_errno);
        else
        {
            for (i = XK_PG_PARSER_DDL_TRANSDDL_NUM_SHORT; i < XK_PG_PARSER_DDL_TRANSDDL_NUM_MAX; i++)
            {
                if (m_ddl_transfunc[i].m_ddlKind == ddlstate.m_ddlKind)
                {
                    current_result = m_ddl_transfunc[i].m_transfunc(xk_pg_parser_ddl, current_record, &ddlstate, xk_pg_parser_errno);
                    break;
                }
            }
        }
        /* 如果错误码不为零, 说明在解析DDL时发生了错误，此时应返回错误 */
        if (0 != *xk_pg_parser_errno)
            return false;
        /* 当current result存在时代表我们解析出了一条DDL, 指向next */
        if (current_result)
        {
            if (!result)
                result = next_result = current_result;
            else
            {
                next_result->m_next = current_result;
                next_result = next_result->m_next;
            }
            current_result = NULL;
        }

        current_record = current_record->m_next;
    }
    *xk_pg_parser_ddl_result = result;
    return true;
}

xk_pg_parser_translog_ddlstmt *xk_pg_parser_ddl_firstTransDDL(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                           xk_pg_parser_translog_systb2dll_record *current_record,
                                           xk_pg_parser_ddlstate *ddlstate,
                                           int32_t *xk_pg_parser_errno)
{
    int32_t i = 0;
    xk_pg_parser_translog_ddlstmt *current_result = NULL;
    if (IS_INSERT(current_record->m_record))
        xk_pg_parser_ddl_insertRecordTrans(xk_pg_parser_ddl, current_record, ddlstate, xk_pg_parser_errno);
    else if (IS_DELETE(current_record->m_record))
        xk_pg_parser_ddl_deleteRecordTrans(xk_pg_parser_ddl, current_record, ddlstate, xk_pg_parser_errno);
    else if (IS_UPDATE(current_record->m_record))
        xk_pg_parser_ddl_updateRecordTrans(xk_pg_parser_ddl, current_record, ddlstate, xk_pg_parser_errno);
    /* 如果产生错误, 提前返回 */
    if (XK_ERRNO_SUCCESS != *xk_pg_parser_errno)
        return NULL;
    if (ddlstate->m_inddl)
    {
        for (i = 0; i < XK_PG_PARSER_DDL_TRANSDDL_NUM_SHORT; i++)
        {
            if (m_ddl_transfunc[i].m_ddlKind == ddlstate->m_ddlKind)
            {
                current_result = m_ddl_transfunc[i].m_transfunc(xk_pg_parser_ddl, current_record, ddlstate, xk_pg_parser_errno);
            }
        }
    }
    return current_result;
}

void xk_pg_parser_ddl_init_ddlstate(xk_pg_parser_ddlstate *ddlstate)
{
    xk_pg_parser_translog_convertinfo_with_zic *zicinfo = ddlstate->m_zicinfo;
    if (ddlstate->m_attList)
        xk_pg_parser_list_free(ddlstate->m_attList);
    if (ddlstate->m_enumlist)
        xk_pg_parser_list_free(ddlstate->m_enumlist);
    if (ddlstate->m_attrdef_list)
        xk_pg_parser_list_free(ddlstate->m_attrdef_list);
    if (ddlstate->m_inherits_oid)
        xk_pg_parser_mcxt_free(XK_PG_PARSER_DDLSTMT_MCXT, ddlstate->m_inherits_oid);

    rmemset1(ddlstate, 0, 0, sizeof(xk_pg_parser_ddlstate));
    ddlstate->m_zicinfo = zicinfo;
}

static xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_toast_index_drop_escape(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_ddlstmt *result = NULL;

    XK_PG_PARSER_UNUSED(xk_pg_parser_ddl);
    XK_PG_PARSER_UNUSED(xk_pg_parser_errno);

    if (IS_DELETE(current_record->m_record))
    {
        if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_DEPEND, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
        {
            char *temp_objid = NULL;
            char *temp_classid = NULL;
            temp_objid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("objid",
                                                                current_record->m_record->m_old_values,
                                                                current_record->m_record->m_valueCnt,
                                                                temp_objid);
            temp_classid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("classid",
                                                                  current_record->m_record->m_old_values,
                                                                  current_record->m_record->m_valueCnt,
                                                                  temp_objid);
            if (!strcmp(ddlstate->m_reloid_char, temp_objid)
                && !strcmp(RelationRelationIdChar, temp_classid))
            {
                xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: skip stmt abot toast end \n");
                xk_pg_parser_ddl_init_ddlstate(ddlstate);
            }
        }
    }
    return result;
}
static xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_toast_escape(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_ddlstmt *result = NULL;

    XK_PG_PARSER_UNUSED(xk_pg_parser_ddl);
    XK_PG_PARSER_UNUSED(xk_pg_parser_errno);

    if (IS_INSERT(current_record->m_record))
    {
        if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_DEPEND, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
        {
            char *temp_objid = NULL;
            char *temp_classid = NULL;
            temp_objid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("objid",
                                                               current_record->m_record->m_new_values,
                                                               current_record->m_record->m_valueCnt,
                                                               temp_objid);
            temp_classid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("classid",
                                                                 current_record->m_record->m_new_values,
                                                                 current_record->m_record->m_valueCnt,
                                                                 temp_classid);
            if (!strcmp(ddlstate->m_reloid_char, temp_objid)
                && !strcmp(RelationRelationIdChar, temp_classid))
            {
                xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: skip stmt abot toast end \n");
                xk_pg_parser_ddl_init_ddlstate(ddlstate);
            }
        }
    }
    if (IS_DELETE(current_record->m_record))
    {
        if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_DEPEND, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
        {
            char *temp_objid = NULL;
            char *temp_classid = NULL;
            temp_objid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("objid",
                                                                current_record->m_record->m_old_values,
                                                                current_record->m_record->m_valueCnt,
                                                                temp_objid);
            temp_classid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("classid",
                                                                  current_record->m_record->m_old_values,
                                                                  current_record->m_record->m_valueCnt,
                                                                  temp_objid);
            if (!strcmp(ddlstate->m_reloid_char, temp_objid)
                && !strcmp(RelationRelationIdChar, temp_classid))
            {
                xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: skip stmt abot toast end \n");
                xk_pg_parser_ddl_init_ddlstate(ddlstate);
            }
        }
    }
    return result;
}

char *ddl_char_tolower(char *output)
{
    int i = 0;

    for (i = 0; i < (int32_t)strlen(output); i++)
    {
        if (output[i] >= 'A' && output[i] <= 'Z')
            output[i] += 'a' - 'A';
    } 
    return output;
}
bool xk_pg_parser_check_table_name(char *tablename,
                                   SysdictName sysdictnum,
                                   int16_t dbtype,
                                   char *dbversion)
{
    XK_PG_PARSER_UNUSED(dbtype);
    XK_PG_PARSER_UNUSED(dbversion);
    if (TEMPTABLE_PREFIX == sysdictnum)
        return !strncmp(xk_pg_parser_postgresql_v127[sysdictnum].name, ddl_char_tolower(tablename), 7);
    else
        return !strcmp(xk_pg_parser_postgresql_v127[sysdictnum].name, ddl_char_tolower(tablename));
}
