#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "common/xk_pg_parser_translog.h"
#include "thirdparty/list/xk_pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_transfunc.h"

#define XK_DDL_DROP_TABLE_MCXT NULL

xk_pg_parser_translog_ddlstmt* xk_pg_parser_ddl_assemble_drop_table(
                                            xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                            xk_pg_parser_ddlstate *ddlstate,
                                            int32_t *xk_pg_parser_errno);

xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_drop_table(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                          xk_pg_parser_translog_systb2dll_record *current_record,
                                                          xk_pg_parser_ddlstate *ddlstate,
                                                          int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_ddlstmt *result = NULL;
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
                                                        temp_classid);
            if (!(strcmp(ddlstate->m_reloid_char, temp_objid))
                && !(strcmp(RelationRelationIdChar, temp_classid)))
            {
                /* 过滤外部表 */
                if (ddlstate->m_relkind == 'f')
                {
                    xk_pg_parser_ddl_init_ddlstate(ddlstate);
                    result = NULL;
                }
                else
                {
                    result = xk_pg_parser_ddl_assemble_drop_table(xk_pg_parser_ddl,
                                    current_record,
                                    ddlstate,
                                    xk_pg_parser_errno);
                }
            }
        }
    }
    return result;
}

xk_pg_parser_translog_ddlstmt* xk_pg_parser_ddl_assemble_drop_table(
                                            xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                            xk_pg_parser_ddlstate *ddlstate,
                                            int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_ddlstmt *result = NULL;
    xk_pg_parser_translog_ddlstmt_drop_base *drop;

    XK_PG_PARSER_UNUSED(xk_pg_parser_ddl);
    XK_PG_PARSER_UNUSED(current_record);
    XK_PG_PARSER_UNUSED(xk_pg_parser_errno);

    //todo free
    if (!xk_pg_parser_mcxt_malloc(XK_DDL_DROP_TABLE_MCXT,
                                 (void **)&result,
                                  sizeof(xk_pg_parser_translog_ddlstmt)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_45;
        return NULL;
    }
    if (!xk_pg_parser_mcxt_malloc(XK_DDL_DROP_TABLE_MCXT,
                                 (void **)&drop,
                                  sizeof(xk_pg_parser_translog_ddlstmt_drop_base)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_46;
        return NULL;
    }
    drop->m_namespace_oid = strtoul(ddlstate->m_nspname_oid_char, NULL, 10);
    drop->m_name = ddlstate->m_relname;
    drop->m_relid = ddlstate->m_reloid;
    result->m_base.m_ddltype = XK_PG_PARSER_DDLTYPE_DROP;
    result->m_base.m_ddlinfo = XK_PG_PARSER_DDLINFO_DROP_TABLE;
    result->m_ddlstmt =(void*) drop;
    result->m_next = NULL;
    xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: drop table end \n");
    xk_pg_parser_ddl_init_ddlstate(ddlstate);
    return result;
}
