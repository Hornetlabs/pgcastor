#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "thirdparty/list/pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_transfunc.h"
#include "thirdparty/parsernode/pg_parser_thirdparty_parsernode.h"

#define DDL_ALTERTABLE_MCXT NULL

static pg_parser_translog_ddlstmt *pg_parser_ddl_assemble_alter_table_add_constraint(
                                                pg_parser_translog_systb2ddl *pg_parser_ddl,
                                                pg_parser_ddlstate *ddlstate,
                                                int32_t *pg_parser_errno);
static pg_parser_translog_ddlstmt *pg_parser_ddl_assemble_alter_table_drop_constraint(
                                                pg_parser_translog_systb2ddl *pg_parser_ddl,
                                                pg_parser_ddlstate *ddlstate,
                                                int32_t *pg_parser_errno);
static pg_parser_translog_ddlstmt *pg_parser_ddl_assemble_alter_table_add_column(
                                                pg_parser_translog_systb2ddl *pg_parser_ddl,
                                                pg_parser_ddlstate *ddlstate,
                                                int32_t *pg_parser_errno);
static pg_parser_translog_ddlstmt *pg_parser_ddl_assemble_alter_table_set_log(
                                                pg_parser_translog_systb2ddl *pg_parser_ddl,
                                                pg_parser_ddlstate *ddlstate,
                                                int32_t *pg_parser_errno);


pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_renameTable(pg_parser_translog_systb2ddl *pg_parser_ddl,
                                                            pg_parser_translog_systb2dll_record *current_record,
                                                            pg_parser_ddlstate *ddlstate,
                                                            int32_t *pg_parser_errno)
{
    pg_parser_translog_ddlstmt *result = NULL;
    pg_parser_translog_ddlstmt_altertable *rename = NULL;

    PG_PARSER_UNUSED(pg_parser_ddl);
    PG_PARSER_UNUSED(current_record);
    PG_PARSER_UNUSED(pg_parser_errno);
    pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture aleter table rename table end \n");
    if (!pg_parser_mcxt_malloc(DDL_ALTERTABLE_MCXT,
                                 (void **)&result,
                                  sizeof(pg_parser_translog_ddlstmt)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_0E;
        return NULL;
    }
    if (!pg_parser_mcxt_malloc(DDL_ALTERTABLE_MCXT,
                                 (void **)&rename,
                                  sizeof(pg_parser_translog_ddlstmt_altertable)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_0F;
        return NULL;
    }
    rename->m_relid = ddlstate->m_reloid;
    rename->m_relname_new = ddlstate->m_relname_new;
    rename->m_relname = ddlstate->m_relname;
    rename->m_relnamespaceid = strtoul(ddlstate->m_nspname_oid_char, NULL, 10);
    result->m_base.m_ddltype = PG_PARSER_DDLTYPE_ALTER;
    result->m_base.m_ddlinfo = PG_PARSER_DDLINFO_ALTER_TABLE_RENAME;
    result->m_ddlstmt = (void*)rename;
    result->m_next = NULL;

    pg_parser_ddl_init_ddlstate(ddlstate);
    return result;
}

pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_add_constraint(
                                                            pg_parser_translog_systb2ddl *pg_parser_ddl,
                                                            pg_parser_translog_systb2dll_record *current_record,
                                                            pg_parser_ddlstate *ddlstate,
                                                            int32_t *pg_parser_errno)
{
    pg_parser_translog_ddlstmt *result = NULL;


    if (IS_INSERT(current_record->m_record))
    {
        if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_DEPEND, pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
        {
            char *temp_objid = NULL;
            char *temp_classid = NULL;
            temp_objid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("objid",
                                                                current_record->m_record->m_new_values,
                                                                current_record->m_record->m_valueCnt,
                                                                temp_objid);
            temp_classid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("classid",
                                                                  current_record->m_record->m_new_values,
                                                                  current_record->m_record->m_valueCnt,
                                                                  temp_classid);
            if (!strcmp(ddlstate->m_reloid_char, temp_objid)
                && !strcmp(RelationRelationIdChar, temp_classid))
                result = pg_parser_ddl_assemble_alter_table_add_constraint(pg_parser_ddl,
                                                                           ddlstate,
                                                                           pg_parser_errno);
        }
    }
    return result;
}

pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_add_constraint_foreign(
                                                            pg_parser_translog_systb2ddl *pg_parser_ddl,
                                                            pg_parser_translog_systb2dll_record *current_record,
                                                            pg_parser_ddlstate *ddlstate,
                                                            int32_t *pg_parser_errno)
{
    pg_parser_translog_ddlstmt *result = NULL;


    if (IS_INSERT(current_record->m_record))
    {
        if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_DEPEND, pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
        {
            char *temp_objid = NULL;
            char *temp_classid = NULL;
            temp_objid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("objid",
                                                                current_record->m_record->m_new_values,
                                                                current_record->m_record->m_valueCnt,
                                                                temp_objid);
            temp_classid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("classid",
                                                                  current_record->m_record->m_new_values,
                                                                  current_record->m_record->m_valueCnt,
                                                                  temp_classid);
            if (!strcmp(ddlstate->m_reloid_char, temp_objid)
                && !strcmp(ConstraintRelationIdChar, temp_classid))
                result = pg_parser_ddl_assemble_alter_table_add_constraint(pg_parser_ddl,
                                                                           ddlstate,
                                                                           pg_parser_errno);
        }
    }
    return result;
}

pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_add_column(
                                                            pg_parser_translog_systb2ddl *pg_parser_ddl,
                                                            pg_parser_translog_systb2dll_record *current_record,
                                                            pg_parser_ddlstate *ddlstate,
                                                            int32_t *pg_parser_errno)
{
    pg_parser_translog_ddlstmt *result = NULL;


    if (IS_UPDATE(current_record->m_record)
        && (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_CLASS, pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion)))
    {
            bool change_attnum = pg_parser_ddl_checkChangeColumn("relnatts",
                                                         current_record->m_record->m_new_values,
                                                         current_record->m_record->m_old_values,
                                                         current_record->m_record->m_valueCnt,
                                                         pg_parser_errno);
            if (change_attnum)
            {
                if (!pg_parser_ddl_get_pg_class_info(ddlstate, current_record->m_record, true))
                {
                    *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                    return NULL;
                }
                result = pg_parser_ddl_assemble_alter_table_add_column(pg_parser_ddl,
                                                                           ddlstate,
                                                                           pg_parser_errno);
                return result;
            }
    }
    /* 
     * 在现有的解析逻辑下, 程序不会运行到这里 
     * 这是一个保险机制, 防止未知的DDL语句造成的解析错误 
     * 因为向pg_attribute表中插入数据太过于常见, 许多DDL都有这种操作
     * 因此在遇到这种情况时, 将现在的record作为第一条record重新处理DDL
     */
    pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, "WARNING: in pg_parser_DDL_alter_table_add_column ddl jump out\n");
    pg_parser_ddl_init_ddlstate(ddlstate);
    pg_parser_ddl_firstTransDDL(pg_parser_ddl, current_record, ddlstate, pg_parser_errno);
    return NULL;
}

pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_set_log(
                                                            pg_parser_translog_systb2ddl *pg_parser_ddl,
                                                            pg_parser_translog_systb2dll_record *current_record,
                                                            pg_parser_ddlstate *ddlstate,
                                                            int32_t *pg_parser_errno)
{
    pg_parser_translog_ddlstmt *result = NULL;
    if (IS_UPDATE(current_record->m_record)
        && (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_CLASS, pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion)))
    {
        bool temp_check_relpersistence_change = false;
        bool temp_check_relfilenode_change = false;

        temp_check_relpersistence_change = pg_parser_ddl_checkChangeColumn("relpersistence",
                                        current_record->m_record->m_new_values,
                                        current_record->m_record->m_old_values,
                                        current_record->m_record->m_valueCnt,
                                        pg_parser_errno);
        temp_check_relfilenode_change = pg_parser_ddl_checkChangeColumn("relfilenode",
                                        current_record->m_record->m_new_values,
                                        current_record->m_record->m_old_values,
                                        current_record->m_record->m_valueCnt,
                                        pg_parser_errno);
        if (temp_check_relpersistence_change && temp_check_relfilenode_change)
        {
            if (PG_PARSER_DDL_ALTER_LOG_GET_CLASS_UPDATE_BEGIN == ddlstate->m_log_step)
            {
                pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                    "DEBUG, DDL PARSER: in alter table set unlogged/logged, step get class info \n");
                /* 设置是logged还是unlogged在获取pg_class时决定 */
                if (!pg_parser_ddl_get_pg_class_info(ddlstate, current_record->m_record, true))
                {
                    *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                    return NULL;
                }
                ddlstate->m_log_step = PG_PARSER_DDL_ALTER_LOG_GET_CLASS_UPDATE_RELPERSISTENCE_STEP;
            }
            else if (PG_PARSER_DDL_ALTER_LOG_GET_CLASS_UPDATE_RELPERSISTENCE_STEP == ddlstate->m_log_step)
            {
                /* 这里只需要获取pg_temp开头的内部临时表的oid */
                char *temp_str = NULL;
                pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                    "DEBUG, DDL PARSER: in alter table set unlogged/logged, step get temp class \n");
                temp_str = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("oid",
                                                                    current_record->m_record->m_new_values,
                                                                    current_record->m_record->m_valueCnt,
                                                                    temp_str);
                ddlstate->m_reloid_temp = strtoul(temp_str, NULL, 10);
                ddlstate->m_log_step = PG_PARSER_DDL_ALTER_LOG_GET_TEMP_CLASS_UPDATE_STEP;
            }
        }
    }
    if (IS_DELETE(current_record->m_record))
    {
        if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_CLASS, pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
        {
            if (PG_PARSER_DDL_ALTER_LOG_GET_TEMP_CLASS_UPDATE_STEP == ddlstate->m_log_step)
            {
                char *temp_str = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("oid",
                                                                    current_record->m_record->m_old_values,
                                                                    current_record->m_record->m_valueCnt,
                                                                    temp_str);
                if (strtoul(temp_str, NULL, 10) == ddlstate->m_reloid_temp)
                {
                    pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                    "DEBUG, DDL PARSER: in alter table set unlogged/logged, step delete temp class \n");
                    ddlstate->m_log_step = PG_PARSER_DDL_ALTER_LOG_GET_TEMP_CLASS_DELETE_STEP;
                }
            }
        }
        else if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_DEPEND, pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
        {
            char *temp_str = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("objid",
                                                                    current_record->m_record->m_old_values,
                                                                    current_record->m_record->m_valueCnt,
                                                                    temp_str);
            if (strtoul(temp_str, NULL, 10) == ddlstate->m_reloid_temp)
            {
                if (PG_PARSER_DDL_ALTER_LOG_GET_TEMP_CLASS_DELETE_STEP == ddlstate->m_log_step)
                    result = pg_parser_ddl_assemble_alter_table_set_log(pg_parser_ddl,
                                                                ddlstate,
                                                                pg_parser_errno);
            }
        }
    }
    return result;
}

pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_drop_constraint(
                                                            pg_parser_translog_systb2ddl *pg_parser_ddl,
                                                            pg_parser_translog_systb2dll_record *current_record,
                                                            pg_parser_ddlstate *ddlstate,
                                                            int32_t *pg_parser_errno)
{
    pg_parser_translog_ddlstmt *result = NULL;

    if (IS_DELETE(current_record->m_record))
    {
        /* primary key, unique捕获pg_constraint的记录 */
        if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_CONSTRAINT, pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
        {
            char *temp_str = NULL;
            pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: alter table drop constraint, get constraint info \n");
            ddlstate->m_relname = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("conname",
                                                                    current_record->m_record->m_old_values,
                                                                    current_record->m_record->m_valueCnt,
                                                                    ddlstate->m_relname);
            ddlstate->m_reloid_char = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("conrelid",
                                                                    current_record->m_record->m_old_values,
                                                                    current_record->m_record->m_valueCnt,
                                                                    ddlstate->m_reloid_char);
            temp_str = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("conislocal",
                                                              current_record->m_record->m_old_values,
                                                              current_record->m_record->m_valueCnt,
                                                              temp_str);
            ddlstate->m_cons_is_local = temp_str[0] == 't' ? true : false;
            ddlstate->m_reloid= strtoul(ddlstate->m_reloid_char, NULL, 10);
            ddlstate->m_nspname_oid_char = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("connamespace",
                                                                    current_record->m_record->m_old_values,
                                                                    current_record->m_record->m_valueCnt,
                                                                    ddlstate->m_nspname_oid_char);
        }


        else if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_DEPEND, pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
        {
            char *temp_classid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("classid",
                                                                    current_record->m_record->m_old_values,
                                                                    current_record->m_record->m_valueCnt,
                                                                    temp_classid);
            if (!strcmp(ConstraintRelationIdChar, temp_classid))
                result = pg_parser_ddl_assemble_alter_table_drop_constraint(pg_parser_ddl,
                                                                           ddlstate,
                                                                           pg_parser_errno);
        }
    }
    return result;
}

pg_parser_translog_ddlstmt* pg_parser_DDL_alter_table_namespace(pg_parser_translog_systb2ddl *pg_parser_ddl,
                                                            pg_parser_translog_systb2dll_record *current_record,
                                                            pg_parser_ddlstate *ddlstate,
                                                            int32_t *pg_parser_errno)
{
    pg_parser_translog_ddlstmt *result = NULL;
    pg_parser_translog_ddlstmt_altertable *new_namespace = NULL;

    PG_PARSER_UNUSED(pg_parser_ddl);
    PG_PARSER_UNUSED(current_record);
    PG_PARSER_UNUSED(pg_parser_errno);
    pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture alter table alter namespace end \n");
    if (!pg_parser_mcxt_malloc(DDL_ALTERTABLE_MCXT,
                                 (void **)&result,
                                  sizeof(pg_parser_translog_ddlstmt)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_11;
        return NULL;
    }
    if (!pg_parser_mcxt_malloc(DDL_ALTERTABLE_MCXT,
                                 (void **)&new_namespace,
                                  sizeof(pg_parser_translog_ddlstmt_altertable)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_12;
        return NULL;
    }
    new_namespace->m_relid = ddlstate->m_reloid;
    new_namespace->m_relname = ddlstate->m_relname;
    new_namespace->m_relnamespaceid = strtoul(ddlstate->m_nspname_oid_char, NULL, 10);
    new_namespace->m_relnamespaceid_new = strtoul(ddlstate->m_nspoid_new, NULL, 10);
    result->m_base.m_ddltype = PG_PARSER_DDLTYPE_ALTER;
    result->m_base.m_ddlinfo = PG_PARSER_DDLINFO_ALTER_TABLE_NAMESPACE;
    result->m_ddlstmt = (void*)new_namespace;
    result->m_next = NULL;

    pg_parser_ddl_init_ddlstate(ddlstate);
    return result;
}

static pg_parser_translog_ddlstmt *pg_parser_ddl_assemble_alter_table_add_constraint(
                                                pg_parser_translog_systb2ddl *pg_parser_ddl,
                                                pg_parser_ddlstate *ddlstate,
                                                int32_t *pg_parser_errno)
{
        char   *temp_contype = false;
        char   *temp_str = NULL;
        pg_parser_translog_ddlstmt *result = NULL;
        pg_parser_translog_ddlstmt_tbconstraint *cons_return = NULL;

        PG_PARSER_UNUSED(pg_parser_ddl);
        PG_PARSER_UNUSED(pg_parser_errno);

        //todo free
        if (!pg_parser_mcxt_malloc(DDL_ALTERTABLE_MCXT,
                                     (void **)&result,
                                      sizeof(pg_parser_translog_ddlstmt)))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_13;
            return NULL;
        }
        //todo free
        if (!pg_parser_mcxt_malloc(DDL_ALTERTABLE_MCXT,
                                     (void **)&cons_return,
                                      sizeof(pg_parser_translog_ddlstmt_tbconstraint)))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_14;
            return NULL;
        }
        if (!ddlstate->m_constraint)
        {
            *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_ADD_CONSTRAINT;
            pg_parser_ddl_init_ddlstate(ddlstate);
            return NULL;
        }
        temp_str = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("conrelid",
                                                ddlstate->m_constraint->m_new_values,
                                                ddlstate->m_constraint->m_valueCnt,
                                                temp_str);
        cons_return->m_relid = strtoul(temp_str, NULL, 10);
        cons_return->m_consname = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("conname",
                                                ddlstate->m_constraint->m_new_values,
                                                ddlstate->m_constraint->m_valueCnt,
                                                cons_return->m_consname);
        temp_str = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("connamespace",
                                                ddlstate->m_constraint->m_new_values,
                                                ddlstate->m_constraint->m_valueCnt,
                                                temp_str);
        cons_return->m_consnspoid = strtoul(temp_str,NULL, 10);

        temp_contype = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("contype",
                                                ddlstate->m_constraint->m_new_values,
                                                ddlstate->m_constraint->m_valueCnt,
                                                temp_contype);
        if ('p' == temp_contype[0])
        {
            pg_parser_translog_ddlstmt_col *column = NULL;
            pg_parser_ListCell *cell = NULL;
            pg_parser_translog_tbcol_values *consatt = NULL;
            pg_parser_translog_ddlstmt_tbconstraint_key *pkey = NULL;
            int32_t i = 0;

            pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: alter table add constraint, constraint type is primary key \n");
            if (!pg_parser_mcxt_malloc(DDL_ALTERTABLE_MCXT,
                                         (void **) &pkey,
                                          sizeof(pg_parser_translog_ddlstmt_tbconstraint_key)))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_15;
                return NULL;
            }

            cons_return->m_type = PG_PARSER_DDL_CONSTRAINT_PRIMARYKEY;
            pkey->m_colcnt = ddlstate->m_attList->length;

            pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: alter table add column end \n");
            if (!pg_parser_mcxt_malloc(DDL_ALTERTABLE_MCXT,
                                         (void **) &column,
                                          sizeof(pg_parser_translog_ddlstmt_col)
                                                 * pkey->m_colcnt))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_16;
                return NULL;
            }
            /* 将存放于dlstate->m_ddlList中的列类型信息附加到返回值中 */
            i = 0;
            pg_parser_foreach(cell, ddlstate->m_attList)
            {
                consatt = (pg_parser_translog_tbcol_values *)pg_parser_lfirst(cell);
                column[i].m_colname = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("attname",
                                                    consatt->m_new_values,
                                                    consatt->m_valueCnt,
                                                    column[i].m_colname);

                temp_str = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("atttypid",
                                                    consatt->m_new_values,
                                                    consatt->m_valueCnt,
                                                    temp_str);
                column[i].m_coltypid = strtoul(temp_str, NULL, 10);
                temp_str = NULL;
                i++;
            }
            pkey->m_concols = column;
            cons_return->m_constraint_stmt = (void*) pkey;
        }

        else if ('f' == temp_contype[0])
        {
            int32_t cnt = 0;
            int32_t num = 0;
            char  temp_str2num[7] = {'\0'};
            char *temp_conkey = NULL;
            char *temp_confkey = NULL;
            pg_parser_translog_ddlstmt_tbconstraint_fkey *fkey = NULL;
            char *temp_cursor1 = NULL;
            char *temp_cursor2 = NULL;
            char * temp_str = NULL;
            pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: alter table add constraint, constraint type is foreign key \n");
            temp_conkey = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("conkey",
                                              ddlstate->m_constraint->m_new_values,
                                              ddlstate->m_constraint->m_valueCnt,
                                              temp_conkey);
            temp_confkey = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("confkey",
                                              ddlstate->m_constraint->m_new_values,
                                              ddlstate->m_constraint->m_valueCnt,
                                              temp_confkey);

            if (!pg_parser_mcxt_malloc(DDL_ALTERTABLE_MCXT,
                                         (void **) &fkey,
                                          sizeof(pg_parser_translog_ddlstmt_tbconstraint_fkey)))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_17;
                return NULL;
            }
            temp_str = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("confrelid",
                                                    ddlstate->m_constraint->m_new_values,
                                                    ddlstate->m_constraint->m_valueCnt,
                                                    temp_str);
            fkey->m_consfkeyid = strtoul(temp_str, NULL, 10);

            cons_return->m_type = PG_PARSER_DDL_CONSTRAINT_FOREIGNKEY;
            /* 由于没有明确的列数记录, 我们必须先遍历一遍获取列数 */
            temp_cursor1 = temp_conkey;
            while (*temp_cursor1)
            {
                if (',' == *temp_cursor1 || '}' == *temp_cursor1)
                    cnt++;
                if ('\0' != *temp_cursor1)
                    temp_cursor1++;
            }
            /* 保存列数 */
            fkey->m_colcnt = cnt;
            /* 初始化 */
            cnt = 0;
            temp_cursor1 = temp_conkey;
            if (!pg_parser_mcxt_malloc(DDL_ALTERTABLE_MCXT,
                                         (void **) &fkey->m_concols_position,
                                          sizeof(int16_t) * fkey->m_colcnt)
                || !pg_parser_mcxt_malloc(DDL_ALTERTABLE_MCXT,
                                         (void **) &fkey->m_fkeycols_position,
                                          sizeof(int16_t) * fkey->m_colcnt))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_18;
                return NULL;
            }
            /* 获取concols */
            while (*temp_cursor1)
            {
                if ('{' == *temp_cursor1)
                    temp_cursor2 = temp_cursor1 + 1;

                if (',' == *temp_cursor1 || '}' == *temp_cursor1)
                {
                    num = temp_cursor1 - temp_cursor2;
                    strncpy(temp_str2num, temp_cursor2, num);
                    temp_str2num[num] = '\0';
                    fkey->m_concols_position[cnt] = (int16_t) atoi(temp_str2num);
                    rmemset1(temp_str2num, 0, 0, 7);
                    temp_cursor2 = temp_cursor1 + 1;
                    cnt++;
                }
                temp_cursor1++;
            }
            cnt = 0;
            temp_cursor1 = temp_confkey;
            temp_cursor2 = temp_cursor1;
            while (*temp_cursor1)
            {
                if ('{' == *temp_cursor1)
                    temp_cursor2 = ++temp_cursor1;

                if (',' == *temp_cursor1 || '}' == *temp_cursor1)
                {
                    num = temp_cursor1 - temp_cursor2;
                    strncpy(temp_str2num, temp_cursor2, num);
                    temp_str2num[num] = '\0';
                    fkey->m_fkeycols_position[cnt] = (int16_t) atoi(temp_str2num);
                    rmemset1(temp_str2num, 0, 0, 7);
                    temp_cursor2 = temp_cursor1 + 1;
                    cnt++;
                }
                temp_cursor1++;
            }
            cons_return->m_constraint_stmt = (void*) fkey;
        }
        else if ('u' == temp_contype[0])
        {
            pg_parser_translog_ddlstmt_col *column = NULL;
            pg_parser_ListCell *cell = NULL;
            pg_parser_translog_tbcol_values *consatt = NULL;
            pg_parser_translog_ddlstmt_tbconstraint_key *ukey = NULL;
            int32_t i = 0;

            pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: alter table add constraint, constraint type is unique key \n");
            if (!pg_parser_mcxt_malloc(DDL_ALTERTABLE_MCXT,
                                         (void **) &ukey,
                                          sizeof(pg_parser_translog_ddlstmt_tbconstraint_key)))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_19;
                return NULL;
            }

            cons_return->m_type = PG_PARSER_DDL_CONSTRAINT_UNIQUE;
            ukey->m_colcnt = ddlstate->m_attList->length;

            //todo free
            if (!pg_parser_mcxt_malloc(DDL_ALTERTABLE_MCXT,
                                         (void **) &column,
                                          sizeof(pg_parser_translog_ddlstmt_col)
                                                 * ukey->m_colcnt))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_1A;
                return NULL;
            }
            i = 0;
            /* 将存放于dlstate->m_ddlList中的列类型信息附加到返回值中 */
            pg_parser_foreach(cell, ddlstate->m_attList)
            {
                consatt = (pg_parser_translog_tbcol_values *)pg_parser_lfirst(cell);
                column[i].m_colname = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("attname",
                                                    consatt->m_new_values,
                                                    consatt->m_valueCnt,
                                                    column[i].m_colname);
                temp_str = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("atttypid",
                                                    consatt->m_new_values,
                                                    consatt->m_valueCnt,
                                                    temp_str);
                column[i].m_coltypid = strtoul(temp_str, NULL, 10);
                i++;
            }
            ukey->m_concols = column;
            cons_return->m_constraint_stmt = (void*) ukey;
        }
        else if ('c' == temp_contype[0])
        {
            pg_parser_translog_ddlstmt_tbconstraint_check *check = NULL;
            char *Node = NULL;
            pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: alter table add constraint, constraint type is check \n");
            if (!pg_parser_mcxt_malloc(DDL_ALTERTABLE_MCXT,
                                         (void **) &check,
                                          sizeof(pg_parser_translog_ddlstmt_tbconstraint_check)))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_1B;
                return NULL;
            }
            Node = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("conbin",
                                                          ddlstate->m_constraint->m_new_values,
                                                          ddlstate->m_constraint->m_valueCnt,
                                                          Node);
            check->m_check_node = pg_parser_get_expr(Node, ddlstate->m_zicinfo);
            cons_return->m_type = PG_PARSER_DDL_CONSTRAINT_CHECK;
            cons_return->m_constraint_stmt = (void*) check;
        }
        else
        {
            /* 其他约束, 不捕获，清理并退出 */
            pg_parser_ddl_init_ddlstate(ddlstate);
            return NULL;
        }

        result->m_base.m_ddltype = PG_PARSER_DDLTYPE_ALTER;
        result->m_base.m_ddlinfo = PG_PARSER_DDLINFO_ALTER_TABLE_ADD_CONSTRAINT;
        result->m_ddlstmt = (void*) cons_return;
        result->m_next = NULL;
        pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: alter table add constraint end \n");
        pg_parser_ddl_init_ddlstate(ddlstate);
        return result;
}

static pg_parser_translog_ddlstmt *pg_parser_ddl_assemble_alter_table_drop_constraint(
                                                pg_parser_translog_systb2ddl *pg_parser_ddl,
                                                pg_parser_ddlstate *ddlstate,
                                                int32_t *pg_parser_errno)
{
    pg_parser_translog_ddlstmt *result = NULL;
    pg_parser_translog_ddlstmt_drop_constraint *drop_constraint;

    PG_PARSER_UNUSED(pg_parser_ddl);
    PG_PARSER_UNUSED(pg_parser_errno);

    if (!pg_parser_mcxt_malloc(DDL_ALTERTABLE_MCXT,
                                 (void **)&result,
                                  sizeof(pg_parser_translog_ddlstmt)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_1C;
        return NULL;
    }
    if (!pg_parser_mcxt_malloc(DDL_ALTERTABLE_MCXT,
                                 (void **)&drop_constraint,
                                  sizeof(pg_parser_translog_ddlstmt_drop_constraint)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_1D;
        return NULL;
    }
    drop_constraint->m_islocal = ddlstate->m_cons_is_local;
    drop_constraint->m_namespace_oid = strtoul(ddlstate->m_nspname_oid_char, NULL, 10);
    drop_constraint->m_relid = ddlstate->m_reloid;
    drop_constraint->m_consname = ddlstate->m_relname;

    result->m_base.m_ddltype = PG_PARSER_DDLTYPE_ALTER;
    result->m_base.m_ddlinfo = PG_PARSER_DDLINFO_ALTER_TABLE_DROP_CONSTRAINT;
    result->m_ddlstmt =(void*) drop_constraint;
    result->m_next = NULL;
    pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: alter table drop constraint end \n");
    pg_parser_ddl_init_ddlstate(ddlstate);
    return result;
}

static pg_parser_translog_ddlstmt *pg_parser_ddl_assemble_alter_table_add_column(
                                                pg_parser_translog_systb2ddl *pg_parser_ddl,
                                                pg_parser_ddlstate *ddlstate,
                                                int32_t *pg_parser_errno)
{
    pg_parser_translog_ddlstmt *result = NULL;
    pg_parser_translog_ddlstmt_col *add_column = NULL;
    pg_parser_translog_ddlstmt_addcolumn *add_column_table = NULL;
    pg_parser_ListCell *cell = NULL;
    pg_parser_translog_tbcol_values *consatt = NULL;
    int32_t typmod = -1;
    uint32_t typid = 0;
    char *temp_notnull = NULL;
    char * temp_str = NULL;

    PG_PARSER_UNUSED(pg_parser_ddl);
    PG_PARSER_UNUSED(pg_parser_errno);

    if (!pg_parser_mcxt_malloc(DDL_ALTERTABLE_MCXT,
                                 (void **)&result,
                                  sizeof(pg_parser_translog_ddlstmt)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_1E;
        return NULL;
    }
    if (!pg_parser_mcxt_malloc(DDL_ALTERTABLE_MCXT,
                                 (void **)&add_column_table,
                                  sizeof(pg_parser_translog_ddlstmt_addcolumn)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_1F;
        return NULL;
    }
    if (!pg_parser_mcxt_malloc(DDL_ALTERTABLE_MCXT,
                                 (void **)&add_column,
                                  sizeof(pg_parser_translog_ddlstmt_col)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_20;
        return NULL;
    }
    add_column_table->m_relid = ddlstate->m_reloid;
    add_column_table->m_relname = ddlstate->m_relname;
    add_column_table->m_relnamespace = strtoul(ddlstate->m_nspname_oid_char, NULL, 10);
    cell = pg_parser_list_head(ddlstate->m_attList);
    if (!cell)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_ADD_COLUMN1;
        pg_parser_ddl_init_ddlstate(ddlstate);
        return NULL;
    }
    consatt = (pg_parser_translog_tbcol_values *)pg_parser_lfirst(cell);
    if (!consatt)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_ADD_COLUMN2;
        pg_parser_ddl_init_ddlstate(ddlstate);
        return NULL;
    }
    add_column->m_colname = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("attname",
                                                    consatt->m_new_values,
                                                    consatt->m_valueCnt,
                                                    add_column->m_colname);
    temp_str = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("atttypid",
                                                    consatt->m_new_values,
                                                    consatt->m_valueCnt,
                                                    temp_str);
    add_column->m_coltypid = strtoul(temp_str, NULL, 10);
    temp_notnull = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("attnotnull",
                                                    consatt->m_new_values,
                                                    consatt->m_valueCnt,
                                                    temp_notnull);
    if ('t' == temp_notnull[0])
        add_column->m_flag = PG_PARSER_DDL_COLUMN_NOTNULL;

    add_column_table->m_addcolumn = add_column;
    temp_str = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("atttypmod",
                                                      consatt->m_new_values,
                                                      consatt->m_valueCnt,
                                                      temp_str);
    typmod = atoi(temp_str);
    typid = add_column->m_coltypid;
    if (0 <= typmod)
    {
        add_column->m_typemod = typmod;
        if (PG_SYSDICT_BPCHAROID == typid || PG_SYSDICT_VARCHAROID == typid)
        {
            typmod -= (int32_t) sizeof(int32_t);
            add_column->m_length = typmod;
            add_column->m_precision = -1;
            add_column->m_scale = -1;
        }
        else if (PG_SYSDICT_TIMEOID == typid || PG_SYSDICT_TIMETZOID == typid
            || PG_SYSDICT_TIMESTAMPOID == typid || PG_SYSDICT_TIMESTAMPTZOID == typid)
        {
            add_column->m_length = -1;
            add_column->m_precision = typmod;
            add_column->m_scale = -1;
        }
        else if (PG_SYSDICT_NUMERICOID == typid)
        {
            typmod -= (int32_t) sizeof(int32_t);
            add_column->m_length = -1;
            add_column->m_precision = (typmod >> 16) & 0xffff;
            add_column->m_scale = typmod & 0xffff;
        }
        else if (PG_SYSDICT_BITOID == typid|| PG_SYSDICT_VARBITOID == typid)
        {
            add_column->m_length = typmod;
            add_column->m_precision = -1;
            add_column->m_scale = -1;
        }
        else
        {
            /* 其他情况下, 确保3个值为-1 */
            add_column->m_length = -1;
            add_column->m_precision = -1;
            add_column->m_scale = -1;
        }
    }
    else
    {
        add_column->m_length = -1;
        add_column->m_precision = -1;
        add_column->m_scale = -1;
    }
    result->m_base.m_ddltype = PG_PARSER_DDLTYPE_ALTER;
    result->m_base.m_ddlinfo = PG_PARSER_DDLINFO_ALTER_TABLE_ADD_COLUMN;
    result->m_ddlstmt =(void*) add_column_table;
    result->m_next = NULL;
    pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: alter table add column end \n");
    pg_parser_ddl_init_ddlstate(ddlstate);
    return result;
}

static pg_parser_translog_ddlstmt *pg_parser_ddl_assemble_alter_table_set_log(
                                                pg_parser_translog_systb2ddl *pg_parser_ddl,
                                                pg_parser_ddlstate *ddlstate,
                                                int32_t *pg_parser_errno)
{
    pg_parser_translog_ddlstmt *result = NULL;
    pg_parser_translog_ddlstmt_setlog *set_log = NULL;

    PG_PARSER_UNUSED(pg_parser_ddl);

    if (!pg_parser_mcxt_malloc(DDL_ALTERTABLE_MCXT,
                                 (void **)&result,
                                  sizeof(pg_parser_translog_ddlstmt)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_1E;
        return NULL;
    }
    if (!pg_parser_mcxt_malloc(DDL_ALTERTABLE_MCXT,
                                 (void **)&set_log,
                                  sizeof(pg_parser_translog_ddlstmt_setlog)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_1E;
        return NULL;
    }
    set_log->m_relid = ddlstate->m_reloid;
    set_log->m_relname = ddlstate->m_relname;
    set_log->m_relnamespace = strtoul(ddlstate->m_nspname_oid_char, NULL, 10);

    result->m_base.m_ddltype = PG_PARSER_DDLTYPE_ALTER;
    if (ddlstate->m_unlogged)
        result->m_base.m_ddlinfo = PG_PARSER_DDLINFO_ALTER_TABLE_SET_UNLOGGED;
    else
        result->m_base.m_ddlinfo = PG_PARSER_DDLINFO_ALTER_TABLE_SET_LOGGED;

    result->m_ddlstmt =(void*) set_log;
    result->m_next = NULL;

    pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                    "DEBUG, DDL PARSER: in alter table set unlogged/logged end \n");
    pg_parser_ddl_init_ddlstate(ddlstate);
    return result;
}
