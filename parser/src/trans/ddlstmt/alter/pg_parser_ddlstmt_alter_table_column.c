
#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "thirdparty/list/pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_transfunc.h"
#include "thirdparty/parsernode/pg_parser_thirdparty_parsernode.h"

#define DDL_ALTERTABLE_MCXT                     NULL

#define DDL_ALTER_COLUMN_TYPE_BEGIN             (uint8_t)0x00
#define DDL_ALTER_COLUMN_TYPE_CREATE_TEMP_TABLE (uint8_t)0x01
#define DDL_ALTER_COLUMN_TYPE_UPDATE_TABLE      (uint8_t)0x02

static pg_parser_translog_ddlstmt* pg_parser_ddl_assemble_alter_table_type(
    pg_parser_translog_systb2ddl* pg_parser_ddl,
    pg_parser_ddlstate*           ddlstate,
    int32_t*                      pg_parser_errno);

static pg_parser_translog_ddlstmt* pg_parser_ddl_assemble_alter_table_add_default(
    pg_parser_translog_systb2ddl* pg_parser_ddl,
    pg_parser_ddlstate*           ddlstate,
    int32_t*                      pg_parser_errno);

static pg_parser_translog_ddlstmt* pg_parser_ddl_assemble_alter_table_drop_default(
    pg_parser_translog_systb2ddl* pg_parser_ddl,
    pg_parser_ddlstate*           ddlstate,
    int32_t*                      pg_parser_errno);

pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_drop_column(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record,
    pg_parser_ddlstate*                  ddlstate,
    int32_t*                             pg_parser_errno)
{
    pg_parser_translog_ddlstmt*             result = NULL;
    pg_parser_translog_ddlstmt_altercolumn* dropcol = NULL;

    PG_PARSER_UNUSED(pg_parser_ddl);
    PG_PARSER_UNUSED(current_record);
    PG_PARSER_UNUSED(pg_parser_errno);
    pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                         "DEBUG, DDL PARSER: capture alter table drop column end \n");
    if (!pg_parser_mcxt_malloc(
            DDL_ALTERTABLE_MCXT, (void**)&result, sizeof(pg_parser_translog_ddlstmt)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_00;
        return NULL;
    }
    if (!pg_parser_mcxt_malloc(
            DDL_ALTERTABLE_MCXT, (void**)&dropcol, sizeof(pg_parser_translog_ddlstmt_altercolumn)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_01;
        return NULL;
    }
    dropcol->m_relid = strtoul(ddlstate->m_reloid_char, NULL, 10);

    if (!ddlstate->m_att)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_DROP_COLUMN;
        pg_parser_ddl_init_ddlstate(ddlstate);
        return NULL;
    }

    dropcol->m_colname = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
        "attname", ddlstate->m_att->m_old_values, ddlstate->m_att->m_valueCnt, dropcol->m_colname);

    result->m_base.m_ddltype = PG_PARSER_DDLTYPE_ALTER;
    result->m_base.m_ddlinfo = PG_PARSER_DDLINFO_ALTER_TABLE_DROP_COLUMN;
    result->m_ddlstmt = (void*)dropcol;
    result->m_next = NULL;

    pg_parser_ddl_init_ddlstate(ddlstate);
    return result;
}

pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_rename_column(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record,
    pg_parser_ddlstate*                  ddlstate,
    int32_t*                             pg_parser_errno)
{
    pg_parser_translog_ddlstmt*             result = NULL;
    pg_parser_translog_ddlstmt_altercolumn* renamecol = NULL;
    pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                         "DEBUG, DDL PARSER: capture alter table rename column end \n");
    PG_PARSER_UNUSED(pg_parser_ddl);
    PG_PARSER_UNUSED(current_record);
    PG_PARSER_UNUSED(pg_parser_errno);

    if (!pg_parser_mcxt_malloc(
            DDL_ALTERTABLE_MCXT, (void**)&result, sizeof(pg_parser_translog_ddlstmt)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_02;
        return NULL;
    }
    if (!pg_parser_mcxt_malloc(DDL_ALTERTABLE_MCXT,
                               (void**)&renamecol,
                               sizeof(pg_parser_translog_ddlstmt_altercolumn)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_03;
        return NULL;
    }
    renamecol->m_relid = strtoul(ddlstate->m_reloid_char, NULL, 10);
    if (!ddlstate->m_att)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_RENAME_COLUMN;
        pg_parser_ddl_init_ddlstate(ddlstate);
        return NULL;
    }
    renamecol->m_colname = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("attname",
                                                              ddlstate->m_att->m_old_values,
                                                              ddlstate->m_att->m_valueCnt,
                                                              renamecol->m_colname);

    renamecol->m_colname_new = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("attname",
                                                                  ddlstate->m_att->m_new_values,
                                                                  ddlstate->m_att->m_valueCnt,
                                                                  renamecol->m_colname_new);
    result->m_base.m_ddltype = PG_PARSER_DDLTYPE_ALTER;
    result->m_base.m_ddlinfo = PG_PARSER_DDLINFO_ALTER_COLUMN_RENAME;
    result->m_ddlstmt = (void*)renamecol;
    result->m_next = NULL;

    pg_parser_ddl_init_ddlstate(ddlstate);
    return result;
}

pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_column_notnull(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record,
    pg_parser_ddlstate*                  ddlstate,
    int32_t*                             pg_parser_errno)
{
    pg_parser_translog_ddlstmt*             result = NULL;
    pg_parser_translog_ddlstmt_altercolumn* colnotnull = NULL;
    pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                         "DEBUG, DDL PARSER: capture alter table set column not null end \n");
    PG_PARSER_UNUSED(pg_parser_ddl);
    PG_PARSER_UNUSED(current_record);
    PG_PARSER_UNUSED(pg_parser_errno);

    if (!pg_parser_mcxt_malloc(
            DDL_ALTERTABLE_MCXT, (void**)&result, sizeof(pg_parser_translog_ddlstmt)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_04;
        return NULL;
    }
    if (!pg_parser_mcxt_malloc(DDL_ALTERTABLE_MCXT,
                               (void**)&colnotnull,
                               sizeof(pg_parser_translog_ddlstmt_altercolumn)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_05;
        return NULL;
    }
    colnotnull->m_relid = strtoul(ddlstate->m_reloid_char, NULL, 10);
    if (!ddlstate->m_att)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_COLUMN_NOTNULL;
        pg_parser_ddl_init_ddlstate(ddlstate);
        return NULL;
    }
    colnotnull->m_colname = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("attname",
                                                               ddlstate->m_att->m_old_values,
                                                               ddlstate->m_att->m_valueCnt,
                                                               colnotnull->m_colname);

    colnotnull->m_notnull = true;
    result->m_base.m_ddltype = PG_PARSER_DDLTYPE_ALTER;
    result->m_base.m_ddlinfo = PG_PARSER_DDLINFO_ALTER_COLUMN_NOTNULL;
    result->m_ddlstmt = (void*)colnotnull;
    result->m_next = NULL;

    pg_parser_ddl_init_ddlstate(ddlstate);
    return result;
}

pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_column_null(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record,
    pg_parser_ddlstate*                  ddlstate,
    int32_t*                             pg_parser_errno)
{
    pg_parser_translog_ddlstmt*             result = NULL;
    pg_parser_translog_ddlstmt_altercolumn* colnull = NULL;
    pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                         "DEBUG, DDL PARSER: capture alter table set column allow null end \n");
    PG_PARSER_UNUSED(pg_parser_ddl);
    PG_PARSER_UNUSED(current_record);
    PG_PARSER_UNUSED(pg_parser_errno);

    if (!pg_parser_mcxt_malloc(
            DDL_ALTERTABLE_MCXT, (void**)&result, sizeof(pg_parser_translog_ddlstmt)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_06;
        return NULL;
    }
    if (!pg_parser_mcxt_malloc(
            DDL_ALTERTABLE_MCXT, (void**)&colnull, sizeof(pg_parser_translog_ddlstmt_altercolumn)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_07;
        return NULL;
    }
    colnull->m_relid = strtoul(ddlstate->m_reloid_char, NULL, 10);
    if (!ddlstate->m_att)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_COLUMN_NULL;
        pg_parser_ddl_init_ddlstate(ddlstate);
        return NULL;
    }
    colnull->m_colname = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
        "attname", ddlstate->m_att->m_old_values, ddlstate->m_att->m_valueCnt, colnull->m_colname);

    colnull->m_notnull = false;
    result->m_base.m_ddltype = PG_PARSER_DDLTYPE_ALTER;
    result->m_base.m_ddlinfo = PG_PARSER_DDLINFO_ALTER_COLUMN_NULL;
    result->m_ddlstmt = (void*)colnull;
    result->m_next = NULL;

    pg_parser_ddl_init_ddlstate(ddlstate);
    return result;
}

pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_column_add_default(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record,
    pg_parser_ddlstate*                  ddlstate,
    int32_t*                             pg_parser_errno)
{
    pg_parser_translog_ddlstmt* result = NULL;

    PG_PARSER_UNUSED(pg_parser_ddl);
    PG_PARSER_UNUSED(pg_parser_errno);

    if (IS_UPDATE(current_record->m_record))
    {
        if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname,
                                       SYS_ATTRIBUTE,
                                       pg_parser_ddl->m_dbtype,
                                       pg_parser_ddl->m_dbversion))
        {
            pg_parser_log_errlog(
                pg_parser_ddl->m_debugLevel,
                "DEBUG, DDL PARSER: alter table alter column add default, get attribute \n");
            ddlstate->m_att = current_record->m_record;
        }
    }
    else if (IS_INSERT(current_record->m_record))
    {
        if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname,
                                       SYS_DEPEND,
                                       pg_parser_ddl->m_dbtype,
                                       pg_parser_ddl->m_dbversion))
        {
            result = pg_parser_ddl_assemble_alter_table_add_default(
                pg_parser_ddl, ddlstate, pg_parser_errno);
        }
    }
    return result;
}

pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_column_drop_default(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record,
    pg_parser_ddlstate*                  ddlstate,
    int32_t*                             pg_parser_errno)
{
    pg_parser_translog_ddlstmt* result = NULL;

    PG_PARSER_UNUSED(pg_parser_ddl);
    PG_PARSER_UNUSED(pg_parser_errno);

    if (IS_UPDATE(current_record->m_record))
    {
        if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname,
                                       SYS_ATTRIBUTE,
                                       pg_parser_ddl->m_dbtype,
                                       pg_parser_ddl->m_dbversion))
        {
            pg_parser_log_errlog(
                pg_parser_ddl->m_debugLevel,
                "DEBUG, DDL PARSER: alter table alter column drop default, get attribute \n");
            ddlstate->m_att = current_record->m_record;
        }
    }
    else if (IS_DELETE(current_record->m_record))
    {
        if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname,
                                       SYS_DEPEND,
                                       pg_parser_ddl->m_dbtype,
                                       pg_parser_ddl->m_dbversion))
        {
            result = pg_parser_ddl_assemble_alter_table_drop_default(
                pg_parser_ddl, ddlstate, pg_parser_errno);
        }
    }
    return result;
}

pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_column_alter_type(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record,
    pg_parser_ddlstate*                  ddlstate,
    int32_t*                             pg_parser_errno)
{
    pg_parser_translog_ddlstmt* result = NULL;

    PG_PARSER_UNUSED(pg_parser_ddl);
    PG_PARSER_UNUSED(current_record);
    PG_PARSER_UNUSED(pg_parser_errno);

    if (IS_INSERT(current_record->m_record))
    {
        if (DDL_ALTER_COLUMN_TYPE_BEGIN == ddlstate->m_alter_coltype_step &&
            (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname,
                                        SYS_CLASS,
                                        pg_parser_ddl->m_dbtype,
                                        pg_parser_ddl->m_dbversion)))
        {
            char* temp_kind = NULL;
            char* temp_relname = NULL;

            temp_kind = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("relkind",
                                                           current_record->m_record->m_new_values,
                                                           current_record->m_record->m_valueCnt,
                                                           temp_kind);
            temp_relname =
                PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("relname",
                                                   current_record->m_record->m_new_values,
                                                   current_record->m_record->m_valueCnt,
                                                   temp_relname);
            if ('r' == temp_kind[0] && (!strncmp(temp_relname, PG_SYSDICT_PG_TEMPTABLE_NAME, 7)))
            {
                pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                                     "DEBUG, DDL PARSER: alter table alter column type, step: get "
                                     "create temp table \n");
                ddlstate->m_alter_coltype_step = DDL_ALTER_COLUMN_TYPE_CREATE_TEMP_TABLE;
            }
        }
    }
    else if (IS_UPDATE(current_record->m_record))
    {
        if (DDL_ALTER_COLUMN_TYPE_CREATE_TEMP_TABLE == ddlstate->m_alter_coltype_step &&
            (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname,
                                        SYS_CLASS,
                                        pg_parser_ddl->m_dbtype,
                                        pg_parser_ddl->m_dbversion)))
        {
            char* temp_kind = NULL;
            char* temp_relname = NULL;
            temp_kind = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("relkind",
                                                           current_record->m_record->m_new_values,
                                                           current_record->m_record->m_valueCnt,
                                                           temp_kind);
            temp_relname =
                PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("relname",
                                                   current_record->m_record->m_new_values,
                                                   current_record->m_record->m_valueCnt,
                                                   temp_relname);
            if ('r' == temp_kind[0] && (strncmp(temp_relname, PG_SYSDICT_PG_TEMPTABLE_NAME, 7)))
            {
                bool change_relfilenode =
                    pg_parser_ddl_checkChangeColumn("relfilenode",
                                                    current_record->m_record->m_new_values,
                                                    current_record->m_record->m_old_values,
                                                    current_record->m_record->m_valueCnt,
                                                    pg_parser_errno);
                if (change_relfilenode)
                {
                    pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                                         "DEBUG, DDL PARSER: alter table alter column type, step: "
                                         "get update table \n");
                    ddlstate->m_alter_coltype_step = DDL_ALTER_COLUMN_TYPE_UPDATE_TABLE;
                    if (!pg_parser_ddl_get_pg_class_info(ddlstate, current_record->m_record, true))
                    {
                        *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                        return NULL;
                    }
                }
            }
        }
    }
    else if (IS_DELETE(current_record->m_record))
    {
        if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname,
                                       SYS_CLASS,
                                       pg_parser_ddl->m_dbtype,
                                       pg_parser_ddl->m_dbversion))
        {
            char* temp_kind = NULL;
            char* temp_relname = NULL;
            temp_kind = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("relkind",
                                                           current_record->m_record->m_old_values,
                                                           current_record->m_record->m_valueCnt,
                                                           temp_kind);
            temp_relname =
                PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("relname",
                                                   current_record->m_record->m_old_values,
                                                   current_record->m_record->m_valueCnt,
                                                   temp_relname);
            if ('r' == temp_kind[0] && (!strncmp(temp_relname, PG_SYSDICT_PG_TEMPTABLE_NAME, 7)))
            {
                result = pg_parser_ddl_assemble_alter_table_type(
                    pg_parser_ddl, ddlstate, pg_parser_errno);
            }
        }
    }
    return result;
}
pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_column_alter_type_short(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record,
    pg_parser_ddlstate*                  ddlstate,
    int32_t*                             pg_parser_errno)
{
    pg_parser_translog_ddlstmt* result = NULL;

    PG_PARSER_UNUSED(current_record);

    result = pg_parser_ddl_assemble_alter_table_type(pg_parser_ddl, ddlstate, pg_parser_errno);
    return result;
}

static pg_parser_translog_ddlstmt* pg_parser_ddl_assemble_alter_table_type(
    pg_parser_translog_systb2ddl* pg_parser_ddl,
    pg_parser_ddlstate*           ddlstate,
    int32_t*                      pg_parser_errno)
{
    pg_parser_translog_ddlstmt*             result = NULL;
    pg_parser_translog_ddlstmt_altercolumn* alter_column = NULL;
    int32_t                                 typmod = -1;
    uint32_t                                typid = 0;
    char*                                   temp_str = NULL;

    PG_PARSER_UNUSED(pg_parser_ddl);
    PG_PARSER_UNUSED(pg_parser_errno);

    if (!pg_parser_mcxt_malloc(
            DDL_ALTERTABLE_MCXT, (void**)&result, sizeof(pg_parser_translog_ddlstmt)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_08;
        return NULL;
    }
    if (!pg_parser_mcxt_malloc(DDL_ALTERTABLE_MCXT,
                               (void**)&alter_column,
                               sizeof(pg_parser_translog_ddlstmt_altercolumn)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_09;
        return NULL;
    }
    if (!ddlstate->m_att)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_ALTER_COLUMN_TYPE;
        pg_parser_ddl_init_ddlstate(ddlstate);
        return NULL;
    }
    alter_column->m_colname = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("attname",
                                                                 ddlstate->m_att->m_new_values,
                                                                 ddlstate->m_att->m_valueCnt,
                                                                 alter_column->m_colname);
    temp_str = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("attrelid",
                                                  ddlstate->m_att->m_new_values,
                                                  ddlstate->m_att->m_valueCnt,
                                                  alter_column->m_colname);
    alter_column->m_relid = strtoul(temp_str, NULL, 10);
    temp_str = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
        "atttypid", ddlstate->m_att->m_new_values, ddlstate->m_att->m_valueCnt, temp_str);
    alter_column->m_type_new = strtoul(temp_str, NULL, 10);
    temp_str = NULL;
    typid = alter_column->m_type_new;
    temp_str = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
        "atttypmod", ddlstate->m_att->m_new_values, ddlstate->m_att->m_valueCnt, temp_str);
    typmod = atoi(temp_str);
    if (0 <= typmod)
    {
        alter_column->m_typemod = typmod;
        if (PG_SYSDICT_BPCHAROID == typid || PG_SYSDICT_VARCHAROID == typid)
        {
            typmod -= (int32_t)sizeof(int32_t);
            alter_column->m_length = typmod;
            alter_column->m_precision = -1;
            alter_column->m_scale = -1;
        }
        else if (PG_SYSDICT_TIMEOID == typid || PG_SYSDICT_TIMETZOID == typid ||
                 PG_SYSDICT_TIMESTAMPOID == typid || PG_SYSDICT_TIMESTAMPTZOID == typid)
        {
            alter_column->m_length = -1;
            alter_column->m_precision = typmod;
            alter_column->m_scale = -1;
        }
        else if (PG_SYSDICT_NUMERICOID == typid)
        {
            typmod -= (int32_t)sizeof(int32_t);
            alter_column->m_length = -1;
            alter_column->m_precision = (typmod >> 16) & 0xffff;
            alter_column->m_scale = typmod & 0xffff;
        }
        else if (PG_SYSDICT_BITOID == typid || PG_SYSDICT_VARBITOID == typid)
        {
            alter_column->m_length = typmod;
            alter_column->m_precision = -1;
            alter_column->m_scale = -1;
        }
        else
        {
            /* In other cases, ensure the 3 values are -1 */
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

    result->m_base.m_ddltype = PG_PARSER_DDLTYPE_ALTER;
    result->m_base.m_ddlinfo = PG_PARSER_DDLINFO_ALTER_COLUMN_TYPE;
    result->m_ddlstmt = (void*)alter_column;
    result->m_next = NULL;
    pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                         "DEBUG, DDL PARSER: alter table alter column type end \n");
    pg_parser_ddl_init_ddlstate(ddlstate);
    return result;
}

static pg_parser_translog_ddlstmt* pg_parser_ddl_assemble_alter_table_add_default(
    pg_parser_translog_systb2ddl* pg_parser_ddl,
    pg_parser_ddlstate*           ddlstate,
    int32_t*                      pg_parser_errno)
{
    pg_parser_translog_ddlstmt*         result = NULL;
    pg_parser_translog_ddlstmt_default* coldefault = NULL;
    char*                               Node = NULL;
    char*                               temp_str = NULL;

    PG_PARSER_UNUSED(pg_parser_ddl);
    PG_PARSER_UNUSED(pg_parser_errno);

    if (!pg_parser_mcxt_malloc(
            DDL_ALTERTABLE_MCXT, (void**)&result, sizeof(pg_parser_translog_ddlstmt)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_0A;
        return NULL;
    }
    if (!pg_parser_mcxt_malloc(
            DDL_ALTERTABLE_MCXT, (void**)&coldefault, sizeof(pg_parser_translog_ddlstmt_default)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_0B;
        return NULL;
    }
    coldefault->m_relid = ddlstate->m_reloid;
    if (!ddlstate->m_att)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_ADD_DEFAULT;
        pg_parser_ddl_init_ddlstate(ddlstate);
        return NULL;
    }
    coldefault->m_colname = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("attname",
                                                               ddlstate->m_att->m_new_values,
                                                               ddlstate->m_att->m_valueCnt,
                                                               coldefault->m_colname);
    Node = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
        "adbin", ddlstate->m_att_def->m_new_values, ddlstate->m_att_def->m_valueCnt, Node);
    temp_str = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
        "atthasmissing", ddlstate->m_att->m_new_values, ddlstate->m_att->m_valueCnt, temp_str);
    if (temp_str[0] == 't')
    {
        coldefault->m_att_default = true;
    }
    else
    {
        coldefault->m_att_default = false;
    }

    coldefault->m_default_node = pg_parser_get_expr(Node, ddlstate->m_zicinfo);
    result->m_base.m_ddltype = PG_PARSER_DDLTYPE_ALTER;
    result->m_base.m_ddlinfo = PG_PARSER_DDLINFO_ALTER_COLUMN_DEFAULT;
    result->m_ddlstmt = (void*)coldefault;
    result->m_next = NULL;
    pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                         "DEBUG, DDL PARSER: alter table alter column add default end \n");
    pg_parser_ddl_init_ddlstate(ddlstate);
    return result;
}

static pg_parser_translog_ddlstmt* pg_parser_ddl_assemble_alter_table_drop_default(
    pg_parser_translog_systb2ddl* pg_parser_ddl,
    pg_parser_ddlstate*           ddlstate,
    int32_t*                      pg_parser_errno)
{
    pg_parser_translog_ddlstmt*         result = NULL;
    pg_parser_translog_ddlstmt_default* coldefault = NULL;

    PG_PARSER_UNUSED(pg_parser_ddl);
    PG_PARSER_UNUSED(pg_parser_errno);

    if (!pg_parser_mcxt_malloc(
            DDL_ALTERTABLE_MCXT, (void**)&result, sizeof(pg_parser_translog_ddlstmt)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_0C;
        return NULL;
    }
    if (!pg_parser_mcxt_malloc(
            DDL_ALTERTABLE_MCXT, (void**)&coldefault, sizeof(pg_parser_translog_ddlstmt_default)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_0D;
        return NULL;
    }
    coldefault->m_relid = ddlstate->m_reloid;
    if (!ddlstate->m_att)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_DROP_DEFAULT;
        pg_parser_ddl_init_ddlstate(ddlstate);
        return NULL;
    }
    coldefault->m_colname = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("attname",
                                                               ddlstate->m_att->m_new_values,
                                                               ddlstate->m_att->m_valueCnt,
                                                               coldefault->m_colname);

    result->m_base.m_ddltype = PG_PARSER_DDLTYPE_ALTER;
    result->m_base.m_ddlinfo = PG_PARSER_DDLINFO_ALTER_COLUMN_DROP_DEFAULT;
    result->m_ddlstmt = (void*)coldefault;
    result->m_next = NULL;
    pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                         "DEBUG, DDL PARSER: alter table alter column drop default end \n");
    pg_parser_ddl_init_ddlstate(ddlstate);
    return result;
}
