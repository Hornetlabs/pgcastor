#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "common/xk_pg_parser_translog.h"
#include "thirdparty/list/xk_pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_transfunc.h"

#define XK_DDL_TRUNCATE_MCXT NULL

xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_reindex(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                         xk_pg_parser_translog_systb2dll_record *current_record,
                                                         xk_pg_parser_ddlstate *ddlstate,
                                                         int32_t *xk_pg_parser_errno)
{                           
    xk_pg_parser_translog_ddlstmt *result = NULL;
    xk_pg_parser_translog_ddlstmt_drop_base *reindex = NULL;

    XK_PG_PARSER_UNUSED(xk_pg_parser_ddl);
    XK_PG_PARSER_UNUSED(current_record);
    XK_PG_PARSER_UNUSED(xk_pg_parser_errno);
    xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture reindex end \n");
    if (!xk_pg_parser_mcxt_malloc(XK_DDL_TRUNCATE_MCXT,
                                 (void **)&result,
                                  sizeof(xk_pg_parser_translog_ddlstmt)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_21;
        return NULL;
    }
    if (!xk_pg_parser_mcxt_malloc(XK_DDL_TRUNCATE_MCXT,
                                 (void **)&reindex,
                                  sizeof(xk_pg_parser_translog_ddlstmt_drop_base)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_22;
        return NULL;
    }
    reindex->m_namespace_oid = strtoul(ddlstate->m_nspname_oid_char, NULL, 10);
    reindex->m_name = ddlstate->m_relname;
    result->m_base.m_ddltype = XK_PG_PARSER_DDLTYPE_SPECIAL;
    result->m_base.m_ddlinfo = XK_PG_PARSER_DDLINFO_REINDEX;
    result->m_ddlstmt = (void *)reindex;

    xk_pg_parser_ddl_init_ddlstate(ddlstate);
    return result;
}
