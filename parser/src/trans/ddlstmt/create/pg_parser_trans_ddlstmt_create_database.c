#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "common/xk_pg_parser_translog.h"
#include "thirdparty/list/xk_pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_transfunc.h"

#define XK_DDL_CREATE_DATABASE_MCXT NULL

xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_create_database(
                                                            xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno)
{
    XK_PG_PARSER_UNUSED(xk_pg_parser_ddl);
    XK_PG_PARSER_UNUSED(current_record);
    XK_PG_PARSER_UNUSED(xk_pg_parser_errno);
    xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture create database(ignore) end \n");
    xk_pg_parser_ddl_init_ddlstate(ddlstate);
    return NULL;
}