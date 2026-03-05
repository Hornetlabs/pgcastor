#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "common/xk_pg_parser_translog.h"
#include "thirdparty/list/xk_pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_transfunc.h"

#define XK_DDL_DROP_SEQUENCE_MCXT NULL

static xk_pg_parser_translog_ddlstmt *xk_pg_parser_ddl_assemble_drop_sequence(
                                                xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                xk_pg_parser_ddlstate *ddlstate,
                                                int32_t *xk_pg_parser_errno);

xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_drop_sequence(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_ddlstmt *result = NULL;
    if (IS_DELETE(current_record->m_record))
    {
#if XK_PG_VERSION_NUM >= 100000
        if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_SEQUENCE, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
            result = xk_pg_parser_ddl_assemble_drop_sequence(xk_pg_parser_ddl, ddlstate, xk_pg_parser_errno);
#else
        if (DependRelationId == mte->reloid)
        {
            FormData_pg_depend *fpd = NULL;

            fpd = (FormData_pg_depend *)get_tuple_from_change(true, mchange);
            if (fpd->objid == ddldata->reloid &&
                RelationRelationId == fpd->classid)
                assemble_create_sequence(mte, ddldata, wds->wdd);
        }
#endif
    }
    return result;
}

static xk_pg_parser_translog_ddlstmt *xk_pg_parser_ddl_assemble_drop_sequence(
                                                xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                xk_pg_parser_ddlstate *ddlstate,
                                                int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_ddlstmt *result = NULL;
    xk_pg_parser_translog_ddlstmt_drop_base *seq_return = NULL;

    XK_PG_PARSER_UNUSED(xk_pg_parser_ddl);
    XK_PG_PARSER_UNUSED(xk_pg_parser_errno);

    //todo free
    if (!xk_pg_parser_mcxt_malloc(XK_DDL_DROP_SEQUENCE_MCXT,
                                 (void **)&result,
                                  sizeof(xk_pg_parser_translog_ddlstmt)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_43;
        return NULL;
    }
    //todo free
    if (!xk_pg_parser_mcxt_malloc(XK_DDL_DROP_SEQUENCE_MCXT,
                                 (void **)&seq_return,
                                  sizeof(xk_pg_parser_translog_ddlstmt_drop_base)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_44;
        return NULL;
    }

    seq_return->m_name = ddlstate->m_relname;
    seq_return->m_namespace_oid = strtoul(ddlstate->m_nspname_oid_char, NULL, 10);

    result->m_base.m_ddltype = XK_PG_PARSER_DDLTYPE_DROP;
    result->m_base.m_ddlinfo = XK_PG_PARSER_DDLINFO_DROP_SEQUENCE;
    result->m_ddlstmt = (void*) seq_return;
    result->m_next = NULL;
    xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: drop sequence end \n");
    xk_pg_parser_ddl_init_ddlstate(ddlstate);
    return result;
}
