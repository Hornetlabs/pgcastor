#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "common/xk_pg_parser_translog.h"
#include "thirdparty/list/xk_pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_transfunc.h"

#define XK_DDL_CREATE_VIEW_MCXT NULL

static xk_pg_parser_translog_ddlstmt *xk_pg_parser_ddl_assemble_create_view(
                                                xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                xk_pg_parser_ddlstate *ddlstate,
                                                int32_t *xk_pg_parser_errno);

xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_create_view(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_ddlstmt *result = NULL;
    if (IS_INSERT(current_record->m_record))
    {
        if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_ATTRIBUTE, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
        {
            /* nothing need to do */
        }
        else if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_REWRITE, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
        {
            /* nothing need to do */
            //ddlstate->m_view_def = current_record->m_record;
        }
        else if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_DEPEND, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
        {
            char *temp_refobjid = NULL;
            char *temp_classid = NULL;
            temp_refobjid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("refobjid",
                                                              current_record->m_record->m_new_values,
                                                              current_record->m_record->m_valueCnt,
                                                              temp_refobjid);
            temp_classid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("classid",
                                                              current_record->m_record->m_new_values,
                                                              current_record->m_record->m_valueCnt,
                                                              temp_classid);
            if (!strcmp(ddlstate->m_reloid_char, temp_refobjid)
                && !strcmp(RewriteRelationIdChar, temp_classid))
                result = xk_pg_parser_ddl_assemble_create_view(xk_pg_parser_ddl, ddlstate, xk_pg_parser_errno);
        }
    }
    return result;
}

static xk_pg_parser_translog_ddlstmt *xk_pg_parser_ddl_assemble_create_view(
                                                xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                xk_pg_parser_ddlstate *ddlstate,
                                                int32_t *xk_pg_parser_errno)
{
        XK_PG_PARSER_UNUSED(xk_pg_parser_ddl);
        XK_PG_PARSER_UNUSED(xk_pg_parser_errno);
        //todo COMPLETE
        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: create view(ignore) end \n");
        xk_pg_parser_ddl_init_ddlstate(ddlstate);
        return NULL;
}
