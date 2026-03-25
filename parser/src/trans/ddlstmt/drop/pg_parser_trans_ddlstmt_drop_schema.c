#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "thirdparty/list/pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_transfunc.h"

#define DDL_DROP_SCHEMA_MCXT NULL

pg_parser_translog_ddlstmt* pg_parser_DDL_drop_schema(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno)
{
    pg_parser_translog_ddlstmt*           result = NULL;
    pg_parser_translog_ddlstmt_valuebase* dropensp = NULL;
    pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                         "DEBUG, DDL PARSER: capture drop schema end \n");
    PG_PARSER_UNUSED(pg_parser_ddl);
    PG_PARSER_UNUSED(current_record);
    PG_PARSER_UNUSED(pg_parser_errno);

    if (!pg_parser_mcxt_malloc(DDL_DROP_SCHEMA_MCXT, (void**)&result,
                               sizeof(pg_parser_translog_ddlstmt)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_41;
        return NULL;
    }
    if (!pg_parser_mcxt_malloc(DDL_DROP_SCHEMA_MCXT, (void**)&dropensp,
                               sizeof(pg_parser_translog_ddlstmt_valuebase)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_42;
        return NULL;
    }
    dropensp->m_value = ddlstate->m_nspname;
    dropensp->m_valuelen = strlen(ddlstate->m_nspname);
    result->m_base.m_ddltype = PG_PARSER_DDLTYPE_DROP;
    result->m_base.m_ddlinfo = PG_PARSER_DDLINFO_DROP_NAMESPACE;
    result->m_ddlstmt = (void*)dropensp;
    result->m_next = NULL;

    pg_parser_ddl_init_ddlstate(ddlstate);
    return result;
}