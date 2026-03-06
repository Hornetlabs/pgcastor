
#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "common/xk_pg_parser_translog.h"
#include "thirdparty/list/xk_pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_transfunc.h"
#include "thirdparty/parsernode/xk_pg_parser_thirdparty_parsernode.h"

#define XK_DDL_ALTERTABLE_MCXT NULL

#define XK_DDL_ALTER_COLUMN_TYPE_BEGIN             (uint8_t) 0x00
#define XK_DDL_ALTER_COLUMN_TYPE_CREATE_TEMP_TABLE (uint8_t) 0x01
#define XK_DDL_ALTER_COLUMN_TYPE_UPDATE_TABLE      (uint8_t) 0x02

static xk_pg_parser_translog_ddlstmt *xk_pg_parser_ddl_assemble_alter_table_type(
                                                xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                xk_pg_parser_ddlstate *ddlstate,
                                                int32_t *xk_pg_parser_errno);

static xk_pg_parser_translog_ddlstmt *xk_pg_parser_ddl_assemble_alter_table_add_default(
                                                xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                xk_pg_parser_ddlstate *ddlstate,
                                                int32_t *xk_pg_parser_errno);

static xk_pg_parser_translog_ddlstmt *xk_pg_parser_ddl_assemble_alter_table_drop_default(
                                                xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                xk_pg_parser_ddlstate *ddlstate,
                                                int32_t *xk_pg_parser_errno);

xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_alter_table_drop_column(
                                                            xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_ddlstmt *result = NULL;
    xk_pg_parser_translog_ddlstmt_altercolumn *dropcol = NULL;

    XK_PG_PARSER_UNUSED(xk_pg_parser_ddl);
    XK_PG_PARSER_UNUSED(current_record);
    XK_PG_PARSER_UNUSED(xk_pg_parser_errno);
    xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture alter table drop column end \n");
    if (!xk_pg_parser_mcxt_malloc(XK_DDL_ALTERTABLE_MCXT,
                                 (void **)&result,
                                  sizeof(xk_pg_parser_translog_ddlstmt)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_00;
        return NULL;
    }
    if (!xk_pg_parser_mcxt_malloc(XK_DDL_ALTERTABLE_MCXT,
                                 (void **)&dropcol,
                                  sizeof(xk_pg_parser_translog_ddlstmt_altercolumn)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_01;
        return NULL;
    }
    dropcol->m_relid = strtoul(ddlstate->m_reloid_char, NULL, 10);

    if (!ddlstate->m_att)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_DROP_COLUMN;
        xk_pg_parser_ddl_init_ddlstate(ddlstate);
        return NULL;
    }

    dropcol->m_colname = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("attname",
                                                    ddlstate->m_att->m_old_values,
                                                    ddlstate->m_att->m_valueCnt,
                                                    dropcol->m_colname);

    result->m_base.m_ddltype = XK_PG_PARSER_DDLTYPE_ALTER;
    result->m_base.m_ddlinfo = XK_PG_PARSER_DDLINFO_ALTER_TABLE_DROP_COLUMN;
    result->m_ddlstmt = (void*)dropcol;
    result->m_next = NULL;

    xk_pg_parser_ddl_init_ddlstate(ddlstate);
    return result;
}

xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_alter_table_rename_column(
                                                            xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_ddlstmt *result = NULL;
    xk_pg_parser_translog_ddlstmt_altercolumn *renamecol = NULL;
    xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture alter table rename column end \n");
    XK_PG_PARSER_UNUSED(xk_pg_parser_ddl);
    XK_PG_PARSER_UNUSED(current_record);
    XK_PG_PARSER_UNUSED(xk_pg_parser_errno);

    if (!xk_pg_parser_mcxt_malloc(XK_DDL_ALTERTABLE_MCXT,
                                 (void **)&result,
                                  sizeof(xk_pg_parser_translog_ddlstmt)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_02;
        return NULL;
    }
    if (!xk_pg_parser_mcxt_malloc(XK_DDL_ALTERTABLE_MCXT,
                                 (void **)&renamecol,
                                  sizeof(xk_pg_parser_translog_ddlstmt_altercolumn)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_03;
        return NULL;
    }
    renamecol->m_relid = strtoul(ddlstate->m_reloid_char, NULL, 10);
    if (!ddlstate->m_att)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_RENAME_COLUMN;
        xk_pg_parser_ddl_init_ddlstate(ddlstate);
        return NULL;
    }
    renamecol->m_colname = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("attname",
                                                    ddlstate->m_att->m_old_values,
                                                    ddlstate->m_att->m_valueCnt,
                                                    renamecol->m_colname);

    renamecol->m_colname_new = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("attname",
                                                    ddlstate->m_att->m_new_values,
                                                    ddlstate->m_att->m_valueCnt,
                                                    renamecol->m_colname_new);
    result->m_base.m_ddltype = XK_PG_PARSER_DDLTYPE_ALTER;
    result->m_base.m_ddlinfo = XK_PG_PARSER_DDLINFO_ALTER_COLUMN_RENAME;
    result->m_ddlstmt = (void*)renamecol;
    result->m_next = NULL;

    xk_pg_parser_ddl_init_ddlstate(ddlstate);
    return result;
}

xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_alter_table_column_notnull(
                                                            xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_ddlstmt *result = NULL;
    xk_pg_parser_translog_ddlstmt_altercolumn *colnotnull = NULL;
    xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture alter table set column not null end \n");
    XK_PG_PARSER_UNUSED(xk_pg_parser_ddl);
    XK_PG_PARSER_UNUSED(current_record);
    XK_PG_PARSER_UNUSED(xk_pg_parser_errno);

    if (!xk_pg_parser_mcxt_malloc(XK_DDL_ALTERTABLE_MCXT,
                                 (void **)&result,
                                  sizeof(xk_pg_parser_translog_ddlstmt)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_04;
        return NULL;
    }
    if (!xk_pg_parser_mcxt_malloc(XK_DDL_ALTERTABLE_MCXT,
                                 (void **)&colnotnull,
                                  sizeof(xk_pg_parser_translog_ddlstmt_altercolumn)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_05;
        return NULL;
    }
    colnotnull->m_relid = strtoul(ddlstate->m_reloid_char, NULL, 10);
    if (!ddlstate->m_att)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_COLUMN_NOTNULL;
        xk_pg_parser_ddl_init_ddlstate(ddlstate);
        return NULL;
    }
    colnotnull->m_colname = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("attname",
                                                    ddlstate->m_att->m_old_values,
                                                    ddlstate->m_att->m_valueCnt,
                                                    colnotnull->m_colname);

    colnotnull->m_notnull = true;
    result->m_base.m_ddltype = XK_PG_PARSER_DDLTYPE_ALTER;
    result->m_base.m_ddlinfo = XK_PG_PARSER_DDLINFO_ALTER_COLUMN_NOTNULL;
    result->m_ddlstmt = (void*)colnotnull;
    result->m_next = NULL;

    xk_pg_parser_ddl_init_ddlstate(ddlstate);
    return result;
}

xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_alter_table_column_null(
                                                            xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_ddlstmt *result = NULL;
    xk_pg_parser_translog_ddlstmt_altercolumn *colnull = NULL;
    xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture alter table set column allow null end \n");
    XK_PG_PARSER_UNUSED(xk_pg_parser_ddl);
    XK_PG_PARSER_UNUSED(current_record);
    XK_PG_PARSER_UNUSED(xk_pg_parser_errno);

    if (!xk_pg_parser_mcxt_malloc(XK_DDL_ALTERTABLE_MCXT,
                                 (void **)&result,
                                  sizeof(xk_pg_parser_translog_ddlstmt)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_06;
        return NULL;
    }
    if (!xk_pg_parser_mcxt_malloc(XK_DDL_ALTERTABLE_MCXT,
                                 (void **)&colnull,
                                  sizeof(xk_pg_parser_translog_ddlstmt_altercolumn)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_07;
        return NULL;
    }
    colnull->m_relid = strtoul(ddlstate->m_reloid_char, NULL, 10);
    if (!ddlstate->m_att)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_COLUMN_NULL;
        xk_pg_parser_ddl_init_ddlstate(ddlstate);
        return NULL;
    }
    colnull->m_colname = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("attname",
                                                    ddlstate->m_att->m_old_values,
                                                    ddlstate->m_att->m_valueCnt,
                                                    colnull->m_colname);

    colnull->m_notnull = false;
    result->m_base.m_ddltype = XK_PG_PARSER_DDLTYPE_ALTER;
    result->m_base.m_ddlinfo = XK_PG_PARSER_DDLINFO_ALTER_COLUMN_NULL;
    result->m_ddlstmt = (void*)colnull;
    result->m_next = NULL;

    xk_pg_parser_ddl_init_ddlstate(ddlstate);
    return result;
}

xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_alter_table_column_add_default(
                                                            xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_ddlstmt *result = NULL;

    XK_PG_PARSER_UNUSED(xk_pg_parser_ddl);
    XK_PG_PARSER_UNUSED(xk_pg_parser_errno);

    if (IS_UPDATE(current_record->m_record))
    {
        if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_ATTRIBUTE, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
        {
            xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: alter table alter column add default, get attribute \n");
            ddlstate->m_att = current_record->m_record;
        }
    }
    else if (IS_INSERT(current_record->m_record))
    {
        if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_DEPEND, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
        {
            result = xk_pg_parser_ddl_assemble_alter_table_add_default(xk_pg_parser_ddl, ddlstate, xk_pg_parser_errno);
        }
    }
    return result;
}

xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_alter_table_column_drop_default(
                                                            xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_ddlstmt *result = NULL;

    XK_PG_PARSER_UNUSED(xk_pg_parser_ddl);
    XK_PG_PARSER_UNUSED(xk_pg_parser_errno);

    if (IS_UPDATE(current_record->m_record))
    {
        if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_ATTRIBUTE, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
        {
            xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: alter table alter column drop default, get attribute \n");
            ddlstate->m_att = current_record->m_record;
        }
    }
    else if (IS_DELETE(current_record->m_record))
    {
        if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_DEPEND, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
        {
            result = xk_pg_parser_ddl_assemble_alter_table_drop_default(xk_pg_parser_ddl, ddlstate, xk_pg_parser_errno);
        }
    }
    return result;
}

xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_alter_table_column_alter_type(
                                                            xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_ddlstmt *result = NULL;

    XK_PG_PARSER_UNUSED(xk_pg_parser_ddl);
    XK_PG_PARSER_UNUSED(current_record);
    XK_PG_PARSER_UNUSED(xk_pg_parser_errno);

    if (IS_INSERT(current_record->m_record))
    {
        if (XK_DDL_ALTER_COLUMN_TYPE_BEGIN == ddlstate->m_alter_coltype_step
            && (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_CLASS, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion)))
        {
            char *temp_kind = NULL;
            char *temp_relname = NULL;

            temp_kind = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("relkind",
                                                    current_record->m_record->m_new_values,
                                                    current_record->m_record->m_valueCnt,
                                                    temp_kind);
            temp_relname = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("relname",
                                                    current_record->m_record->m_new_values,
                                                    current_record->m_record->m_valueCnt,
                                                    temp_relname);
            if ('r' == temp_kind[0] && (!strncmp(temp_relname, XK_PG_SYSDICT_PG_TEMPTABLE_NAME, 7)))
            {
                xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: alter table alter column type, step: get create temp table \n");
                ddlstate->m_alter_coltype_step = XK_DDL_ALTER_COLUMN_TYPE_CREATE_TEMP_TABLE;
            }
        }
    }
    else if (IS_UPDATE(current_record->m_record))
    {
        if (XK_DDL_ALTER_COLUMN_TYPE_CREATE_TEMP_TABLE == ddlstate->m_alter_coltype_step
            && (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_CLASS, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion)))
        {
            char *temp_kind = NULL;
            char *temp_relname = NULL;
            temp_kind = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("relkind",
                                                    current_record->m_record->m_new_values,
                                                    current_record->m_record->m_valueCnt,
                                                    temp_kind);
            temp_relname = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("relname",
                                                    current_record->m_record->m_new_values,
                                                    current_record->m_record->m_valueCnt,
                                                    temp_relname);
            if ('r' == temp_kind[0] && (strncmp(temp_relname, XK_PG_SYSDICT_PG_TEMPTABLE_NAME, 7)))
            {
                bool change_relfilenode = xk_pg_parser_ddl_checkChangeColumn("relfilenode",
                                                    current_record->m_record->m_new_values,
                                                    current_record->m_record->m_old_values,
                                                    current_record->m_record->m_valueCnt,
                                                    xk_pg_parser_errno);
                if (change_relfilenode)
                {
                    xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: alter table alter column type, step: get update table \n");
                    ddlstate->m_alter_coltype_step = XK_DDL_ALTER_COLUMN_TYPE_UPDATE_TABLE;
                    if (!xk_pg_parser_ddl_get_pg_class_info(ddlstate,
                                                           current_record->m_record,
                                                           true))
                    {
                        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                        return NULL;
                    }
                }
            }
        }
    }
    else if (IS_DELETE(current_record->m_record))
    {
        if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_CLASS, xk_pg_parser_ddl->m_dbtype , xk_pg_parser_ddl->m_dbversion))
        {
            char *temp_kind = NULL;
            char *temp_relname = NULL;
            temp_kind = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("relkind",
                                                    current_record->m_record->m_old_values,
                                                    current_record->m_record->m_valueCnt,
                                                    temp_kind);
            temp_relname = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("relname",
                                                    current_record->m_record->m_old_values,
                                                    current_record->m_record->m_valueCnt,
                                                    temp_relname);
            if ('r' == temp_kind[0] && (!strncmp(temp_relname, XK_PG_SYSDICT_PG_TEMPTABLE_NAME, 7)))
                result = xk_pg_parser_ddl_assemble_alter_table_type(xk_pg_parser_ddl,
                                                                    ddlstate,
                                                                    xk_pg_parser_errno);
        }
    }
    return result;
}
xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_alter_table_column_alter_type_short(
                                                            xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_ddlstmt *result = NULL;

    XK_PG_PARSER_UNUSED(current_record);

    result = xk_pg_parser_ddl_assemble_alter_table_type(xk_pg_parser_ddl,
                                                                    ddlstate,
                                                                    xk_pg_parser_errno);
    return result;
}


static xk_pg_parser_translog_ddlstmt *xk_pg_parser_ddl_assemble_alter_table_type(
                                                xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                xk_pg_parser_ddlstate *ddlstate,
                                                int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_ddlstmt *result = NULL;
    xk_pg_parser_translog_ddlstmt_altercolumn *alter_column = NULL;
    int32_t typmod = -1;
    uint32_t typid = 0;
    char *temp_str = NULL;

    XK_PG_PARSER_UNUSED(xk_pg_parser_ddl);
    XK_PG_PARSER_UNUSED(xk_pg_parser_errno);

    if (!xk_pg_parser_mcxt_malloc(XK_DDL_ALTERTABLE_MCXT,
                                 (void **)&result,
                                  sizeof(xk_pg_parser_translog_ddlstmt)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_08;
        return NULL;
    }
    if (!xk_pg_parser_mcxt_malloc(XK_DDL_ALTERTABLE_MCXT,
                                 (void **)&alter_column,
                                  sizeof(xk_pg_parser_translog_ddlstmt_altercolumn)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_09;
        return NULL;
    }
    if (!ddlstate->m_att)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_ALTER_COLUMN_TYPE;
        xk_pg_parser_ddl_init_ddlstate(ddlstate);
        return NULL;
    }
    alter_column->m_colname = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("attname",
                                                    ddlstate->m_att->m_new_values,
                                                    ddlstate->m_att->m_valueCnt,
                                                    alter_column->m_colname);
    temp_str = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("attrelid",
                                                    ddlstate->m_att->m_new_values,
                                                    ddlstate->m_att->m_valueCnt,
                                                    alter_column->m_colname);
    alter_column->m_relid = strtoul(temp_str, NULL, 10);
    temp_str = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("atttypid",
                                                    ddlstate->m_att->m_new_values,
                                                    ddlstate->m_att->m_valueCnt,
                                                    temp_str);
    alter_column->m_type_new = strtoul(temp_str, NULL, 10);
    temp_str = NULL;
    typid = alter_column->m_type_new;
    temp_str = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("atttypmod",
                                                         ddlstate->m_att->m_new_values,
                                                         ddlstate->m_att->m_valueCnt,
                                                         temp_str);
    typmod = atoi(temp_str);
    if (0 <= typmod)
    {
        alter_column->m_typemod = typmod;
        if (XK_PG_SYSDICT_BPCHAROID == typid || XK_PG_SYSDICT_VARCHAROID == typid)
        {
            typmod -= (int32_t) sizeof(int32_t);
            alter_column->m_length = typmod;
            alter_column->m_precision = -1;
            alter_column->m_scale = -1;
        }
        else if (XK_PG_SYSDICT_TIMEOID == typid || XK_PG_SYSDICT_TIMETZOID == typid
              || XK_PG_SYSDICT_TIMESTAMPOID == typid || XK_PG_SYSDICT_TIMESTAMPTZOID == typid)
        {
            alter_column->m_length = -1;
            alter_column->m_precision = typmod;
            alter_column->m_scale = -1;
        }
        else if (XK_PG_SYSDICT_NUMERICOID == typid)
        {
            typmod -= (int32_t) sizeof(int32_t);
            alter_column->m_length = -1;
            alter_column->m_precision = (typmod >> 16) & 0xffff;
            alter_column->m_scale = typmod & 0xffff;
        }
        else if (XK_PG_SYSDICT_BITOID == typid|| XK_PG_SYSDICT_VARBITOID == typid)
        {
            alter_column->m_length = typmod;
            alter_column->m_precision = -1;
            alter_column->m_scale = -1;
        }
        else
        {
            /* 其他情况下, 确保3个值为-1 */
            alter_column->m_length = -1;
            alter_column->m_precision = -1;
            alter_column->m_scale = -1;
        }
    }
    else
    {
        alter_column->m_length = -1;
        alter_column->m_precision = -1;
        alter_column->m_scale = -1;
    }

    result->m_base.m_ddltype = XK_PG_PARSER_DDLTYPE_ALTER;
    result->m_base.m_ddlinfo = XK_PG_PARSER_DDLINFO_ALTER_COLUMN_TYPE;
    result->m_ddlstmt =(void*) alter_column;
    result->m_next = NULL;
    xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: alter table alter column type end \n");
    xk_pg_parser_ddl_init_ddlstate(ddlstate);
    return result;
}

static xk_pg_parser_translog_ddlstmt *xk_pg_parser_ddl_assemble_alter_table_add_default(
                                                xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                xk_pg_parser_ddlstate *ddlstate,
                                                int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_ddlstmt *result = NULL;
    xk_pg_parser_translog_ddlstmt_default *coldefault = NULL;
    char    *Node = NULL;
    char    *temp_str = NULL;

    XK_PG_PARSER_UNUSED(xk_pg_parser_ddl);
    XK_PG_PARSER_UNUSED(xk_pg_parser_errno);

    if (!xk_pg_parser_mcxt_malloc(XK_DDL_ALTERTABLE_MCXT,
                                 (void **)&result,
                                  sizeof(xk_pg_parser_translog_ddlstmt)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_0A;
        return NULL;
    }
    if (!xk_pg_parser_mcxt_malloc(XK_DDL_ALTERTABLE_MCXT,
                                 (void **)&coldefault,
                                  sizeof(xk_pg_parser_translog_ddlstmt_default)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_0B;
        return NULL;
    }
    coldefault->m_relid = ddlstate->m_reloid;
    if (!ddlstate->m_att)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_ADD_DEFAULT;
        xk_pg_parser_ddl_init_ddlstate(ddlstate);
        return NULL;
    }
    coldefault->m_colname = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("attname",
                                                    ddlstate->m_att->m_new_values,
                                                    ddlstate->m_att->m_valueCnt,
                                                    coldefault->m_colname);
    Node = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("adbin",
                                        ddlstate->m_att_def->m_new_values,
                                        ddlstate->m_att_def->m_valueCnt,
                                        Node);
    temp_str = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("atthasmissing",
                                                      ddlstate->m_att->m_new_values,
                                                      ddlstate->m_att->m_valueCnt,
                                                      temp_str);
    if (temp_str[0] == 't')
        coldefault->m_att_default = true;
    else
        coldefault->m_att_default = false;

    coldefault->m_default_node = xk_pg_parser_get_expr(Node, ddlstate->m_zicinfo);
    result->m_base.m_ddltype = XK_PG_PARSER_DDLTYPE_ALTER;
    result->m_base.m_ddlinfo = XK_PG_PARSER_DDLINFO_ALTER_COLUMN_DEFAULT;
    result->m_ddlstmt = (void*)coldefault;
    result->m_next = NULL;
    xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: alter table alter column add default end \n");
    xk_pg_parser_ddl_init_ddlstate(ddlstate);
    return result;
}

static xk_pg_parser_translog_ddlstmt *xk_pg_parser_ddl_assemble_alter_table_drop_default(
                                                xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                xk_pg_parser_ddlstate *ddlstate,
                                                int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_ddlstmt *result = NULL;
    xk_pg_parser_translog_ddlstmt_default *coldefault = NULL;

    XK_PG_PARSER_UNUSED(xk_pg_parser_ddl);
    XK_PG_PARSER_UNUSED(xk_pg_parser_errno);

    if (!xk_pg_parser_mcxt_malloc(XK_DDL_ALTERTABLE_MCXT,
                                 (void **)&result,
                                  sizeof(xk_pg_parser_translog_ddlstmt)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_0C;
        return NULL;
    }
    if (!xk_pg_parser_mcxt_malloc(XK_DDL_ALTERTABLE_MCXT,
                                 (void **)&coldefault,
                                  sizeof(xk_pg_parser_translog_ddlstmt_default)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_0D;
        return NULL;
    }
    coldefault->m_relid = ddlstate->m_reloid;
    if (!ddlstate->m_att)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_DROP_DEFAULT;
        xk_pg_parser_ddl_init_ddlstate(ddlstate);
        return NULL;
    }
    coldefault->m_colname = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("attname",
                                                    ddlstate->m_att->m_new_values,
                                                    ddlstate->m_att->m_valueCnt,
                                                    coldefault->m_colname);

    result->m_base.m_ddltype = XK_PG_PARSER_DDLTYPE_ALTER;
    result->m_base.m_ddlinfo = XK_PG_PARSER_DDLINFO_ALTER_COLUMN_DROP_DEFAULT;
    result->m_ddlstmt = (void*)coldefault;
    result->m_next = NULL;
    xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: alter table alter column drop default end \n");
    xk_pg_parser_ddl_init_ddlstate(ddlstate);
    return result;
}
