#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "thirdparty/list/pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "sysdict/pg_parser_sysdict_getinfo.h"

char* pg_parser_ddl_getColumnValueByName(char*                           column_name,
                                         pg_parser_translog_tbcol_value* record_value, uint16_t cnt)
{
    int32_t i = 0;
    for (i = 0; i < cnt; i++)
    {
        if (!record_value)
        {
            return NULL;
        }

        if (record_value[i].m_colName &&
            !strcmp(ddl_char_tolower((char*)record_value[i].m_colName), column_name))
        {
            if (INFO_COL_IS_NULL == record_value[i].m_info ||
                INFO_COL_MAY_NULL == record_value[i].m_info)
            {
                return NULL;
            }
            else
            {
                return (char*)record_value[i].m_value;
            }
        }
    }
    return NULL;
}

bool pg_parser_ddl_get_pg_class_info(pg_parser_ddlstate*              ddlstate,
                                     pg_parser_translog_tbcol_values* record, bool isnew)
{
    char*                           temp_str = NULL;
    pg_parser_translog_tbcol_value* values = NULL;
    if (isnew)
    {
        values = record->m_new_values;
    }
    else
    {
        values = record->m_old_values;
    }
    temp_str = pg_parser_ddl_getColumnValueByName("relpersistence", values, record->m_valueCnt);
    if (!temp_str)
    {
        return false;
    }
    ddlstate->m_relpersistence = temp_str[0];
    temp_str = pg_parser_ddl_getColumnValueByName("relpersistence", values, record->m_valueCnt);
    if (!temp_str)
    {
        return false;
    }
    ddlstate->m_unlogged = ('u' == temp_str[0] ? true : false);
    ddlstate->m_temp = ('t' == temp_str[0] ? true : false);
    temp_str = pg_parser_ddl_getColumnValueByName("oid", values, record->m_valueCnt);
    if (!temp_str)
    {
        return false;
    }
    ddlstate->m_reloid_char = temp_str;
    ddlstate->m_reloid = (uint32_t)strtoul(temp_str, NULL, 10);
    ddlstate->m_amoid_char =
        pg_parser_ddl_getColumnValueByName("relam", values, record->m_valueCnt);
    if (!ddlstate->m_amoid_char)
    {
        return false;
    }
    ddlstate->m_relname = pg_parser_ddl_getColumnValueByName("relname", values, record->m_valueCnt);
    if (!ddlstate->m_relname)
    {
        return false;
    }
    ddlstate->m_nspname_oid_char =
        pg_parser_ddl_getColumnValueByName("relnamespace", values, record->m_valueCnt);
    if (!ddlstate->m_nspname_oid_char)
    {
        return false;
    }
    temp_str = NULL;
    temp_str = pg_parser_ddl_getColumnValueByName("relowner", values, record->m_valueCnt);
    if (!temp_str)
    {
        return false;
    }
    ddlstate->m_owner = (uint32_t)strtoul(temp_str, NULL, 10);
    return true;
}

bool pg_parser_ddl_get_attribute_info(pg_parser_ddlstate*              ddlstate,
                                      pg_parser_translog_tbcol_values* record, bool isnew)
{
    char*                           temp_str = NULL;
    int16_t                         attnum = 0;
    pg_parser_translog_tbcol_value* values = NULL;
    bool                            attisdropped = false;

    if (isnew)
    {
        values = record->m_new_values;
    }
    else
    {
        values = record->m_old_values;
    }
    temp_str = pg_parser_ddl_getColumnValueByName("attnum", values, record->m_valueCnt);
    if (!temp_str)
    {
        return false;
    }
    attnum = (int16_t)atoi(temp_str);
    temp_str = NULL;
    temp_str = pg_parser_ddl_getColumnValueByName("attisdropped", values, record->m_valueCnt);
    if (!temp_str)
    {
        return false;
    }
    if ('t' == temp_str[0])
    {
        attisdropped = true;
    }

    if (0 < attnum && false == attisdropped)
    {
        ddlstate->m_attList = pg_parser_list_lappend(ddlstate->m_attList, record);
    }
    return true;
}

bool pg_parser_ddl_checkChangeColumn(char* column_name, pg_parser_translog_tbcol_value* record_new,
                                     pg_parser_translog_tbcol_value* record_old, uint16_t cnt,
                                     int32_t* pg_parser_errno)
{
    char *new = NULL, *old = NULL;

    new = pg_parser_ddl_getColumnValueByName(column_name, record_new, cnt);
    old = pg_parser_ddl_getColumnValueByName(column_name, record_old, cnt);

    if (!new && !old)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_GET_COLUMN_BY_COLNAME;
        return NULL;
    }
    else if ((new && !old) || (!new && old))
    {
        return true;
    }
    if (strcmp(new, old))
    {
        return true;
    }
    else
    {
        return false;
    }
}
#if 0
bool pg_parser_ddl_getTypnameByOid(pg_parser_translog_systb2ddl *pg_parser_ddl,
                                       uint32_t typid,
                                       char **typname)
{
    return pg_parser_sysdict_getTypeNameByOid(typid,
                                                 &pg_parser_ddl->m_systbs->m_pg_type,
                                                 typname);
}

bool pg_parser_ddl_getRelname(pg_parser_translog_systb2ddl *pg_parser_ddl,
                                 char *reloid_char,
                                 char **relname)
{
    return pg_parser_sysdict_getRelNameByOid((uint32_t)strtoul(reloid_char, NULL, 10),
                                              &pg_parser_ddl->m_systbs->m_pg_class,
                                              relname);
}

bool pg_parser_ddl_getNameSpace(pg_parser_translog_systb2ddl *pg_parser_ddl,
                                   char *nspoid_char,
                                   char **nspname)
{
    return pg_parser_sysdict_getNspNameByOid((uint32_t)strtoul(nspoid_char, NULL, 10),
                                              &pg_parser_ddl->m_systbs->m_pg_namespace,
                                              nspname);
}

bool pg_parser_ddl_getRelNameSpace(pg_parser_translog_systb2ddl *pg_parser_ddl,
                                      char *reloid_char,
                                      char **nspname)
{
    uint32_t nspid = pg_parser_InvalidOid;

    if (!pg_parser_sysdict_getNspidByRelid((uint32_t)strtoul(reloid_char, NULL, 10),
                                              &pg_parser_ddl->m_systbs->m_pg_class,
                                              &nspid))
        return false;

    return pg_parser_sysdict_getNspNameByOid(nspid, &pg_parser_ddl->m_systbs->m_pg_namespace, nspname);
}
#endif