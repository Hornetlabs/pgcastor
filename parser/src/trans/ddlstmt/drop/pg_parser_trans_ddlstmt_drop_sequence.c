#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "thirdparty/list/pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_transfunc.h"

#define DDL_DROP_SEQUENCE_MCXT NULL

static pg_parser_translog_ddlstmt* pg_parser_ddl_assemble_drop_sequence(
    pg_parser_translog_systb2ddl* pg_parser_ddl,
    pg_parser_ddlstate*           ddlstate,
    int32_t*                      pg_parser_errno);

pg_parser_translog_ddlstmt* pg_parser_DDL_drop_sequence(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record,
    pg_parser_ddlstate*                  ddlstate,
    int32_t*                             pg_parser_errno)
{
    pg_parser_translog_ddlstmt* result = NULL;
    if (IS_DELETE(current_record->m_record))
    {
        if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname,
                                       SYS_SEQUENCE,
                                       pg_parser_ddl->m_dbtype,
                                       pg_parser_ddl->m_dbversion))
        {
            result = pg_parser_ddl_assemble_drop_sequence(pg_parser_ddl, ddlstate, pg_parser_errno);
        }
    }
    return result;
}

static pg_parser_translog_ddlstmt* pg_parser_ddl_assemble_drop_sequence(
    pg_parser_translog_systb2ddl* pg_parser_ddl,
    pg_parser_ddlstate*           ddlstate,
    int32_t*                      pg_parser_errno)
{
    pg_parser_translog_ddlstmt*           result = NULL;
    pg_parser_translog_ddlstmt_drop_base* seq_return = NULL;

    PG_PARSER_UNUSED(pg_parser_ddl);
    PG_PARSER_UNUSED(pg_parser_errno);

    // todo free
    if (!pg_parser_mcxt_malloc(
            DDL_DROP_SEQUENCE_MCXT, (void**)&result, sizeof(pg_parser_translog_ddlstmt)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_43;
        return NULL;
    }
    // todo free
    if (!pg_parser_mcxt_malloc(DDL_DROP_SEQUENCE_MCXT,
                               (void**)&seq_return,
                               sizeof(pg_parser_translog_ddlstmt_drop_base)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_44;
        return NULL;
    }

    seq_return->m_name = ddlstate->m_relname;
    seq_return->m_namespace_oid = strtoul(ddlstate->m_nspname_oid_char, NULL, 10);

    result->m_base.m_ddltype = PG_PARSER_DDLTYPE_DROP;
    result->m_base.m_ddlinfo = PG_PARSER_DDLINFO_DROP_SEQUENCE;
    result->m_ddlstmt = (void*)seq_return;
    result->m_next = NULL;
    pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, "DEBUG, DDL PARSER: drop sequence end \n");
    pg_parser_ddl_init_ddlstate(ddlstate);
    return result;
}
