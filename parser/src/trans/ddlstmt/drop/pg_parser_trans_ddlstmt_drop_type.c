#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "thirdparty/list/pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_transfunc.h"

#define DDL_DROP_TYPE_MCXT NULL

#define TYPE_SAVE_DEFAULT   (uint8_t) 0
#define TYPE_SAVE_CLASS     (uint8_t) 1
#define TYPE_SAVE_RECORD    (uint8_t) 2
#define TYPE_SAVE_COMPLETE  (uint8_t) 10

static pg_parser_translog_ddlstmt *pg_parser_ddl_assemble_drop_type(
                                                pg_parser_translog_systb2ddl *pg_parser_ddl,
                                                pg_parser_ddlstate *ddlstate,
                                                int32_t *pg_parser_errno);

pg_parser_translog_ddlstmt* pg_parser_DDL_drop_type(
                                            pg_parser_translog_systb2ddl *pg_parser_ddl,
                                            pg_parser_translog_systb2dll_record *current_record,
                                            pg_parser_ddlstate *ddlstate,
                                            int32_t *pg_parser_errno)
{
    pg_parser_translog_ddlstmt *result = NULL;

    if (IS_DELETE(current_record->m_record))
    {
        if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_DEPEND, pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
        {
            char *temp_oid = NULL;
            char *temp_objid = NULL;
            char *temp_classid = NULL;
                     
            if (ddlstate->m_type_savepoint == TYPE_SAVE_CLASS)
                return NULL;

            if (!(strcmp(PG_TOAST_NAMESPACE_CHAR, ddlstate->m_nspname_oid_char)))
            {
                pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in rtop type, skip toast \n");
                pg_parser_ddl_init_ddlstate(ddlstate);
                return NULL;
            }
            if (!ddlstate->m_type_item)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_DROP_TYPE1;
                pg_parser_ddl_init_ddlstate(ddlstate);
                return NULL;
            }
            temp_oid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("oid",
                                                        ddlstate->m_type_item->m_old_values,
                                                        ddlstate->m_type_item->m_valueCnt,
                                                        temp_oid);
            temp_objid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("objid",
                                                        current_record->m_record->m_old_values,
                                                        current_record->m_record->m_valueCnt,
                                                        temp_objid);
            temp_classid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("classid",
                                                        current_record->m_record->m_old_values,
                                                        current_record->m_record->m_valueCnt,
                                                        temp_classid);
            if (!(strcmp(temp_oid, temp_objid))
                && !(strcmp(TypeRelationIdChar, temp_classid))
                && ddlstate->m_type_savepoint == TYPE_SAVE_COMPLETE)
                result = pg_parser_ddl_assemble_drop_type(pg_parser_ddl, ddlstate, pg_parser_errno);
        }
        else if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_TYPE, pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
        {
            char *temp_typtype = NULL;
            char *temp_oid = NULL;
            temp_typtype = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("typtype",
                                                  current_record->m_record->m_old_values,
                                                  current_record->m_record->m_valueCnt,
                                                  temp_typtype);

            temp_oid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("typrelid",
                                                  current_record->m_record->m_old_values,
                                                  current_record->m_record->m_valueCnt,
                                                  temp_oid);

            ddlstate->m_reloid = (uint32_t)strtoul(temp_oid, NULL, 10);
            ddlstate->m_relname = NULL;
            if (ddlstate->m_type_savepoint != TYPE_SAVE_CLASS)
            {
                if (temp_typtype[0] == 'c')
                {
                    pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in drop type, step: save record \n");
                    ddlstate->m_type_savepoint = TYPE_SAVE_RECORD;

                }
                else if (temp_typtype[0] == 'e'
                        || temp_typtype[0] == 'r'
                        || temp_typtype[0] == 'd')
                {
                    ddlstate->m_type_item = current_record->m_record;
                    ddlstate->m_type_savepoint = TYPE_SAVE_COMPLETE;
                    pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in drop type, step: save complete \n");
                }

            }
            else if (ddlstate->m_type_savepoint == TYPE_SAVE_CLASS)
            {
                if (temp_typtype[0] == 'c')
                    ddlstate->m_type_item = current_record->m_record;
                ddlstate->m_type_savepoint = TYPE_SAVE_COMPLETE;
                pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in drop type, step: save complete \n");
            }

        }

        else if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_CLASS, pg_parser_ddl->m_dbtype , pg_parser_ddl->m_dbversion))
        {
            char *temp_relkind = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("relkind",
                                        current_record->m_record->m_old_values,
                                        current_record->m_record->m_valueCnt,
                                        temp_relkind);
            if (PG_SYSDICT_RELKIND_SEQUENCE == temp_relkind[0])
            {
                pg_parser_ddl_init_ddlstate(ddlstate);
                pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in drop type, stmt type changet to drop sequence \n");
                ddlstate->m_ddlKind = PG_PARSER_DDL_SEQUENCE_DROP;
                if (!pg_parser_ddl_get_pg_class_info(ddlstate, current_record->m_record, false))
                {
                    *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                    return NULL;
                }
                ddlstate->m_inddl = true;
            }
            if (ddlstate->m_type_savepoint == TYPE_SAVE_DEFAULT)
            {
                pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in drop type, step: save class \n");
                ddlstate->m_type_savepoint = TYPE_SAVE_CLASS;
                return NULL;
            }
            else
            {
                if (ddlstate->m_type_savepoint == TYPE_SAVE_RECORD)
                {
                    pg_parser_ddl_init_ddlstate(ddlstate);
                    if (PG_SYSDICT_RELKIND_RELATION == temp_relkind[0]
                     || PG_SYSDICT_RELKIND_PARTITIONED_TABLE == temp_relkind[0])
                    {
                        pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in drop type, change stmt type to drop table \n");
                        ddlstate->m_ddlKind = PG_PARSER_DDL_TABLE_DROP;
                        if (!pg_parser_ddl_get_pg_class_info(ddlstate, current_record->m_record, false))
                        {
                            *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                            return NULL;
                        }
                        ddlstate->m_inddl = true;
                    }
                    /* 目前走到这里的逻辑是drop了行外存储的index */
                    else if (PG_SYSDICT_RELKIND_INDEX == temp_relkind[0])
                    {
                        pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in drop type, change stmt type to drop index \n");
                        ddlstate->m_ddlKind = PG_PARSER_DDL_TOAST_INDEX_DROP_ESCAPE;
                        if (!pg_parser_ddl_get_pg_class_info(ddlstate, current_record->m_record, false))
                        {
                            *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                            return NULL;
                        }
                        ddlstate->m_inddl = true;
                    }
                    else  if (PG_SYSDICT_RELKIND_FOREIGN_TABLE == temp_relkind[0])
                    {
                        pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in drop type, ignore drop foreign table \n");
                        ddlstate->m_ddlKind = PG_PARSER_DDL_TABLE_DROP;
                        if (!pg_parser_ddl_get_pg_class_info(ddlstate, current_record->m_record, false))
                        {
                            *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                            return NULL;
                        }
                        ddlstate->m_relkind = temp_relkind[0];
                        ddlstate->m_inddl = true;
                        return NULL;
                    }
                    /* 如果到达了这里，证明逻辑没有覆盖全 */
                    else
                    {
                        pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                            "DEBUG, relkind: %s \n", temp_relkind);
                        *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_DROP_TABLE_RELKIND_CHECK;
                        pg_parser_ddl_init_ddlstate(ddlstate);
                        return NULL;
                    }
                }
                /* 如果到达了这里，证明逻辑没有覆盖全 */
                else
                {
                    pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                            "DEBUG, relkind: %s \n", temp_relkind);
                    *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_DROP_TABLE_RELKIND_CHECK;
                    pg_parser_ddl_init_ddlstate(ddlstate);
                    return NULL;
                }
            }
        }
    }
    return result;
}

static pg_parser_translog_ddlstmt *pg_parser_ddl_assemble_drop_type(
                                                pg_parser_translog_systb2ddl *pg_parser_ddl,
                                                pg_parser_ddlstate *ddlstate,
                                                int32_t *pg_parser_errno)
{
    pg_parser_translog_ddlstmt *result = NULL;
    pg_parser_translog_ddlstmt_drop_base *type_return = NULL;

    PG_PARSER_UNUSED(pg_parser_ddl);
    PG_PARSER_UNUSED(pg_parser_errno);

    if (!pg_parser_mcxt_malloc(DDL_DROP_TYPE_MCXT,
                                 (void **)&result,
                                  sizeof(pg_parser_translog_ddlstmt)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_47;
        return NULL;
    }
    //todo free
    if (!pg_parser_mcxt_malloc(DDL_DROP_TYPE_MCXT,
                                 (void **)&type_return,
                                  sizeof(pg_parser_translog_ddlstmt_drop_base)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_48;
        return NULL;
    }
    if (!ddlstate->m_type_item)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_DROP_TYPE2;
        pg_parser_ddl_init_ddlstate(ddlstate);
        return NULL;
    }
    type_return->m_name = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("typname",
                                            ddlstate->m_type_item->m_old_values,
                                            ddlstate->m_type_item->m_valueCnt,
                                            type_return->m_name);

    type_return->m_namespace_oid = strtoul(ddlstate->m_nspname_oid_char, NULL, 10);

    result->m_base.m_ddltype = PG_PARSER_DDLTYPE_DROP;
    result->m_base.m_ddlinfo = PG_PARSER_DDLINFO_DROP_TYPE;
    result->m_ddlstmt = (void *)type_return;
    pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: drop type end \n");
    pg_parser_ddl_init_ddlstate(ddlstate);
    result->m_next = NULL;
    return result;
}
