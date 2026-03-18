#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "thirdparty/list/pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_transfunc.h"

#define DDL_TRUNCATE_MCXT NULL

pg_parser_translog_ddlstmt* pg_parser_DDL_reindex(pg_parser_translog_systb2ddl*        pg_parser_ddl,
                                                  pg_parser_translog_systb2dll_record* current_record,
                                                  pg_parser_ddlstate*                  ddlstate,
                                                  int32_t*                             pg_parser_errno)
{
    pg_parser_translog_ddlstmt*           result = NULL;
    pg_parser_translog_ddlstmt_drop_base* reindex = NULL;

    PG_PARSER_UNUSED(pg_parser_ddl);
    PG_PARSER_UNUSED(current_record);
    PG_PARSER_UNUSED(pg_parser_errno);
    pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, "DEBUG, DDL PARSER: capture reindex end \n");
    if (!pg_parser_mcxt_malloc(DDL_TRUNCATE_MCXT, (void**)&result, sizeof(pg_parser_translog_ddlstmt)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_21;
        return NULL;
    }
    if (!pg_parser_mcxt_malloc(DDL_TRUNCATE_MCXT, (void**)&reindex, sizeof(pg_parser_translog_ddlstmt_drop_base)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_22;
        return NULL;
    }
    reindex->m_namespace_oid = strtoul(ddlstate->m_nspname_oid_char, NULL, 10);
    reindex->m_name = ddlstate->m_relname;
    result->m_base.m_ddltype = PG_PARSER_DDLTYPE_SPECIAL;
    result->m_base.m_ddlinfo = PG_PARSER_DDLINFO_REINDEX;
    result->m_ddlstmt = (void*)reindex;

    pg_parser_ddl_init_ddlstate(ddlstate);
    return result;
}
