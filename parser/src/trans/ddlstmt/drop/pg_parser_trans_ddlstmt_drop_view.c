#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "common/xk_pg_parser_translog.h"
#include "thirdparty/list/xk_pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_transfunc.h"

#define XK_DDL_DROP_VIEW_MCXT NULL

/* 
 * 该版本中只是对drop view做一个过滤操作, 不返回实际解析内容
 */
xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_drop_view(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                          xk_pg_parser_translog_systb2dll_record *current_record,
                                                          xk_pg_parser_ddlstate *ddlstate,
                                                          int32_t *xk_pg_parser_errno)
{
    XK_PG_PARSER_UNUSED(xk_pg_parser_ddl);
    XK_PG_PARSER_UNUSED(xk_pg_parser_errno);
    if (IS_DELETE(current_record->m_record))
    {
        if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_CLASS, xk_pg_parser_ddl->m_dbtype , xk_pg_parser_ddl->m_dbversion))
        {
            char *temprelid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("oid",
                                                        current_record->m_record->m_old_values,
                                                        current_record->m_record->m_valueCnt,
                                                        temprelid);
            if (!strcmp(temprelid, ddlstate->m_reloid_char))
            {
                xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture drop view(ignore) end \n");
                xk_pg_parser_ddl_init_ddlstate(ddlstate);
            }
        }
    }
    return NULL;
}