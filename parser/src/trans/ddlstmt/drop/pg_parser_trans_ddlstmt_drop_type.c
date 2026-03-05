#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "common/xk_pg_parser_translog.h"
#include "thirdparty/list/xk_pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_transfunc.h"

#define XK_DDL_DROP_TYPE_MCXT NULL

#define TYPE_SAVE_DEFAULT   (uint8_t) 0
#define TYPE_SAVE_CLASS     (uint8_t) 1
#define TYPE_SAVE_RECORD    (uint8_t) 2
#define TYPE_SAVE_COMPLETE  (uint8_t) 10

static xk_pg_parser_translog_ddlstmt *xk_pg_parser_ddl_assemble_drop_type(
                                                xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                xk_pg_parser_ddlstate *ddlstate,
                                                int32_t *xk_pg_parser_errno);

xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_drop_type(
                                            xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                            xk_pg_parser_ddlstate *ddlstate,
                                            int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_ddlstmt *result = NULL;

    if (IS_DELETE(current_record->m_record))
    {
        if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_DEPEND, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
        {
            char *temp_oid = NULL;
            char *temp_objid = NULL;
            char *temp_classid = NULL;
                     
            if (ddlstate->m_type_savepoint == TYPE_SAVE_CLASS)
                return NULL;

            if (!(strcmp(PG_TOAST_NAMESPACE_CHAR, ddlstate->m_nspname_oid_char)))
            {
                xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in rtop type, skip toast \n");
                xk_pg_parser_ddl_init_ddlstate(ddlstate);
                return NULL;
            }
            if (!ddlstate->m_type_item)
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_DROP_TYPE1;
                xk_pg_parser_ddl_init_ddlstate(ddlstate);
                return NULL;
            }
            temp_oid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("oid",
                                                        ddlstate->m_type_item->m_old_values,
                                                        ddlstate->m_type_item->m_valueCnt,
                                                        temp_oid);
            temp_objid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("objid",
                                                        current_record->m_record->m_old_values,
                                                        current_record->m_record->m_valueCnt,
                                                        temp_objid);
            temp_classid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("classid",
                                                        current_record->m_record->m_old_values,
                                                        current_record->m_record->m_valueCnt,
                                                        temp_classid);
            if (!(strcmp(temp_oid, temp_objid))
                && !(strcmp(TypeRelationIdChar, temp_classid))
                && ddlstate->m_type_savepoint == TYPE_SAVE_COMPLETE)
                result = xk_pg_parser_ddl_assemble_drop_type(xk_pg_parser_ddl, ddlstate, xk_pg_parser_errno);
        }
        else if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_TYPE, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
        {
            char *temp_typtype = NULL;
            char *temp_oid = NULL;
            temp_typtype = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("typtype",
                                                  current_record->m_record->m_old_values,
                                                  current_record->m_record->m_valueCnt,
                                                  temp_typtype);

            temp_oid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("typrelid",
                                                  current_record->m_record->m_old_values,
                                                  current_record->m_record->m_valueCnt,
                                                  temp_oid);

            ddlstate->m_reloid = (uint32_t)strtoul(temp_oid, NULL, 10);
            ddlstate->m_relname = NULL;
            if (ddlstate->m_type_savepoint != TYPE_SAVE_CLASS)
            {
                if (temp_typtype[0] == 'c')
                {
                    xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in drop type, step: save record \n");
                    ddlstate->m_type_savepoint = TYPE_SAVE_RECORD;

                }
                else if (temp_typtype[0] == 'e'
                        || temp_typtype[0] == 'r'
                        || temp_typtype[0] == 'd')
                {
                    ddlstate->m_type_item = current_record->m_record;
                    ddlstate->m_type_savepoint = TYPE_SAVE_COMPLETE;
                    xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in drop type, step: save complete \n");
                }

            }
            else if (ddlstate->m_type_savepoint == TYPE_SAVE_CLASS)
            {
                if (temp_typtype[0] == 'c')
                    ddlstate->m_type_item = current_record->m_record;
                ddlstate->m_type_savepoint = TYPE_SAVE_COMPLETE;
                xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in drop type, step: save complete \n");
            }

        }

        else if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_CLASS, xk_pg_parser_ddl->m_dbtype , xk_pg_parser_ddl->m_dbversion))
        {
            char *temp_relkind = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("relkind",
                                        current_record->m_record->m_old_values,
                                        current_record->m_record->m_valueCnt,
                                        temp_relkind);
            if (XK_PG_SYSDICT_RELKIND_SEQUENCE == temp_relkind[0])
            {
                xk_pg_parser_ddl_init_ddlstate(ddlstate);
                xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in drop type, stmt type changet to drop sequence \n");
                ddlstate->m_ddlKind = XK_PG_PARSER_DDL_SEQUENCE_DROP;
                if (!xk_pg_parser_ddl_get_pg_class_info(ddlstate, current_record->m_record, false))
                {
                    *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                    return NULL;
                }
                ddlstate->m_inddl = true;
            }
            if (ddlstate->m_type_savepoint == TYPE_SAVE_DEFAULT)
            {
                xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in drop type, step: save class \n");
                ddlstate->m_type_savepoint = TYPE_SAVE_CLASS;
                return NULL;
            }
            else
            {
                if (ddlstate->m_type_savepoint == TYPE_SAVE_RECORD)
                {
                    xk_pg_parser_ddl_init_ddlstate(ddlstate);
                    if (XK_PG_SYSDICT_RELKIND_RELATION == temp_relkind[0]
                     || XK_PG_SYSDICT_RELKIND_PARTITIONED_TABLE == temp_relkind[0])
                    {
                        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in drop type, change stmt type to drop table \n");
                        ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TABLE_DROP;
                        if (!xk_pg_parser_ddl_get_pg_class_info(ddlstate, current_record->m_record, false))
                        {
                            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                            return NULL;
                        }
                        ddlstate->m_inddl = true;
                    }
                    /* 目前走到这里的逻辑是drop了行外存储的index */
                    else if (XK_PG_SYSDICT_RELKIND_INDEX == temp_relkind[0])
                    {
                        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in drop type, change stmt type to drop index \n");
                        ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TOAST_INDEX_DROP_ESCAPE;
                        if (!xk_pg_parser_ddl_get_pg_class_info(ddlstate, current_record->m_record, false))
                        {
                            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                            return NULL;
                        }
                        ddlstate->m_inddl = true;
                    }
                    else  if (XK_PG_SYSDICT_RELKIND_FOREIGN_TABLE == temp_relkind[0])
                    {
                        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in drop type, ignore drop foreign table \n");
                        ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TABLE_DROP;
                        if (!xk_pg_parser_ddl_get_pg_class_info(ddlstate, current_record->m_record, false))
                        {
                            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                            return NULL;
                        }
                        ddlstate->m_relkind = temp_relkind[0];
                        ddlstate->m_inddl = true;
                        return NULL;
                    }
                    /* 如果到达了这里，证明逻辑没有覆盖全 */
                    else
                    {
                        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                            "DEBUG, relkind: %s \n", temp_relkind);
                        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_DROP_TABLE_RELKIND_CHECK;
                        xk_pg_parser_ddl_init_ddlstate(ddlstate);
                        return NULL;
                    }
                }
                /* 如果到达了这里，证明逻辑没有覆盖全 */
                else
                {
                    xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                            "DEBUG, relkind: %s \n", temp_relkind);
                    *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_DROP_TABLE_RELKIND_CHECK;
                    xk_pg_parser_ddl_init_ddlstate(ddlstate);
                    return NULL;
                }
            }
        }
    }
    return result;
}

static xk_pg_parser_translog_ddlstmt *xk_pg_parser_ddl_assemble_drop_type(
                                                xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                xk_pg_parser_ddlstate *ddlstate,
                                                int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_ddlstmt *result = NULL;
    xk_pg_parser_translog_ddlstmt_drop_base *type_return = NULL;

    XK_PG_PARSER_UNUSED(xk_pg_parser_ddl);
    XK_PG_PARSER_UNUSED(xk_pg_parser_errno);

    if (!xk_pg_parser_mcxt_malloc(XK_DDL_DROP_TYPE_MCXT,
                                 (void **)&result,
                                  sizeof(xk_pg_parser_translog_ddlstmt)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_47;
        return NULL;
    }
    //todo free
    if (!xk_pg_parser_mcxt_malloc(XK_DDL_DROP_TYPE_MCXT,
                                 (void **)&type_return,
                                  sizeof(xk_pg_parser_translog_ddlstmt_drop_base)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_48;
        return NULL;
    }
    if (!ddlstate->m_type_item)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_DROP_TYPE2;
        xk_pg_parser_ddl_init_ddlstate(ddlstate);
        return NULL;
    }
    type_return->m_name = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("typname",
                                            ddlstate->m_type_item->m_old_values,
                                            ddlstate->m_type_item->m_valueCnt,
                                            type_return->m_name);

    type_return->m_namespace_oid = strtoul(ddlstate->m_nspname_oid_char, NULL, 10);

    result->m_base.m_ddltype = XK_PG_PARSER_DDLTYPE_DROP;
    result->m_base.m_ddlinfo = XK_PG_PARSER_DDLINFO_DROP_TYPE;
    result->m_ddlstmt = (void *)type_return;
    xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: drop type end \n");
    xk_pg_parser_ddl_init_ddlstate(ddlstate);
    result->m_next = NULL;
    return result;
}
