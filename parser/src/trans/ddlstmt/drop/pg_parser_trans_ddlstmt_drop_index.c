#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "common/xk_pg_parser_translog.h"
#include "thirdparty/list/xk_pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_transfunc.h"

#define XK_DDL_DROP_INDEX_MCXT NULL

static xk_pg_parser_translog_ddlstmt *xk_pg_parser_ddl_assemble_drop_index(
                                                xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                xk_pg_parser_ddlstate *ddlstate,
                                                int32_t *xk_pg_parser_errno);

/* 
 * drop index 包含alter table drop constraint的入口
 */
xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_drop_index(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                          xk_pg_parser_translog_systb2dll_record *current_record,
                                                          xk_pg_parser_ddlstate *ddlstate,
                                                          int32_t *xk_pg_parser_errno)
{   
    xk_pg_parser_translog_ddlstmt *result = NULL;
    if (IS_DELETE(current_record->m_record))
    {
        if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_CLASS, xk_pg_parser_ddl->m_dbtype , xk_pg_parser_ddl->m_dbversion))
        {
            char *temp_namespaceid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("relnamespace",
                                                current_record->m_record->m_old_values,
                                                current_record->m_record->m_valueCnt,
                                                temp_namespaceid);

            if (!xk_pg_parser_ddl_get_pg_class_info(ddlstate, current_record->m_record, false))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                return NULL;
            }
            /* 过滤行外存储 */
            if (!strcmp(PG_TOAST_NAMESPACE_CHAR, temp_namespaceid))
            {
                xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: in drop index, index is abot toast, ignore \n");
                ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TOAST_ESCAPE;
            }
        }
        else if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_DEPEND, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
        {
            char *temp_objid = NULL;
            char *temp_classid = NULL;
            char *temp_refclassid = NULL;
            temp_objid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("objid",
                                                          current_record->m_record->m_old_values,
                                                          current_record->m_record->m_valueCnt,
                                                          temp_objid);
            temp_classid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("classid",
                                                          current_record->m_record->m_old_values,
                                                          current_record->m_record->m_valueCnt,
                                                          temp_classid);
            temp_refclassid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("refclassid",
                                                          current_record->m_record->m_old_values,
                                                          current_record->m_record->m_valueCnt,
                                                          temp_refclassid);
            if ((ddlstate->m_reloid_char ? !strcmp(temp_objid, ddlstate->m_reloid_char) : false)
                 && !strcmp(RelationRelationIdChar, temp_classid))
            {   /* 如果depend的refclassid为pg_constraint, 说明这个索引其实是constraint */
                if (!strcmp(ConstraintRelationIdChar, temp_refclassid))
                {
                    xk_pg_parser_ddl_init_ddlstate(ddlstate);
                    xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: in drop index, stmt type change to alter table drop constraint \n");
                    ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TABLE_ALTER_DROP_CONSTRAINT;
                    ddlstate->m_inddl = true;
                }
                else
                    result = xk_pg_parser_ddl_assemble_drop_index(xk_pg_parser_ddl, ddlstate, xk_pg_parser_errno);

            }
        }
    }
    return result;
}

static xk_pg_parser_translog_ddlstmt *xk_pg_parser_ddl_assemble_drop_index(
                                                xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                xk_pg_parser_ddlstate *ddlstate,
                                                int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_ddlstmt *result = NULL;
    xk_pg_parser_translog_ddlstmt_drop_base *drop_index;
    char *relid_char = NULL;

    XK_PG_PARSER_UNUSED(xk_pg_parser_ddl);
    XK_PG_PARSER_UNUSED(xk_pg_parser_errno);

    if (!xk_pg_parser_mcxt_malloc(XK_DDL_DROP_INDEX_MCXT,
                                 (void **)&result,
                                  sizeof(xk_pg_parser_translog_ddlstmt)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_3F;
        return NULL;
    }
    if (!xk_pg_parser_mcxt_malloc(XK_DDL_DROP_INDEX_MCXT,
                                 (void **)&drop_index,
                                  sizeof(xk_pg_parser_translog_ddlstmt_drop_base)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_40;
        return NULL;
    }
    if (!ddlstate->m_index)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_DROP_INDEX;
        xk_pg_parser_ddl_init_ddlstate(ddlstate);
        return NULL;
    }
    relid_char = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("indrelid",
                                           ddlstate->m_index->m_old_values,
                                           ddlstate->m_index->m_valueCnt,
                                           relid_char);
    drop_index->m_namespace_oid = strtoul(ddlstate->m_nspname_oid_char, NULL, 10);
    drop_index->m_relid = strtoul(relid_char, NULL, 10);
    drop_index->m_name = ddlstate->m_relname;

    result->m_base.m_ddltype = XK_PG_PARSER_DDLTYPE_DROP;
    result->m_base.m_ddlinfo = XK_PG_PARSER_DDLINFO_DROP_INDEX;
    result->m_ddlstmt =(void*) drop_index;
    result->m_next = NULL;
    xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: drop index end \n");
    xk_pg_parser_ddl_init_ddlstate(ddlstate);
    return result;
}
