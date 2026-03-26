#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "thirdparty/list/pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_transfunc.h"

#define DDL_CREATE_DATABASE_MCXT NULL

pg_parser_translog_ddlstmt* pg_parser_DDL_create_database(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record,
    pg_parser_ddlstate*                  ddlstate,
    int32_t*                             pg_parser_errno)
{
    PG_PARSER_UNUSED(pg_parser_ddl);
    PG_PARSER_UNUSED(current_record);
    PG_PARSER_UNUSED(pg_parser_errno);
    pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                         "DEBUG, DDL PARSER: capture create database(ignore) end \n");
    pg_parser_ddl_init_ddlstate(ddlstate);
    return NULL;
}