#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "thirdparty/list/pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_transfunc.h"

#define PG_PARSER_DDLSTMT_MCXT NULL

typedef struct pg_parser_DDL_kindWithFunc
{
    DDLKind                    m_ddlKind;
    pg_parser_DDL_transDDLFunc m_transfunc;
} pg_parser_DDL_kindWithFunc;

static pg_parser_translog_ddlstmt* pg_parser_DDL_toast_escape(pg_parser_translog_systb2ddl*        pg_parser_ddl,
                                                              pg_parser_translog_systb2dll_record* current_record,
                                                              pg_parser_ddlstate*                  ddlstate,
                                                              int32_t*                             pg_parser_errno);

static pg_parser_translog_ddlstmt* pg_parser_DDL_toast_index_drop_escape(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record,
    pg_parser_ddlstate*                  ddlstate,
    int32_t*                             pg_parser_errno);

static pg_parser_DDL_kindWithFunc m_ddl_transfunc[] = {
    {PG_PARSER_DDL_TABLE_TRUNCATE,                     pg_parser_DDL_truncate                           },
    {PG_PARSER_DDL_TABLE_ALTER_COLUMN_TYPE_SHORT,      pg_parser_DDL_alter_table_column_alter_type_short},
    {PG_PARSER_DDL_REINDEX,                            pg_parser_DDL_reindex                            },
    {PG_PARSER_DDL_TABLE_ALTER_TABLE_RENAME,           pg_parser_DDL_alter_table_renameTable            },
    {PG_PARSER_DDL_TABLE_ALTER_NAMESPACE,              pg_parser_DDL_alter_table_namespace              },
    {PG_PARSER_DDL_TABLE_ALTER_DROP_COLUMN,            pg_parser_DDL_alter_table_drop_column            },
    {PG_PARSER_DDL_TABLE_ALTER_COLUMN_RENAME,          pg_parser_DDL_alter_table_rename_column          },
    {PG_PARSER_DDL_NAMESPACE_CREATE,                   pg_parser_DDL_create_schema                      },
    {PG_PARSER_DDL_NAMESPACE_DROP,                     pg_parser_DDL_drop_schema                        },
    {PG_PARSER_DDL_TABLE_ALTER_COLUMN_NOTNULL,         pg_parser_DDL_alter_table_column_notnull         },
    {PG_PARSER_DDL_TABLE_ALTER_COLUMN_NULL,            pg_parser_DDL_alter_table_column_null            },
    {PG_PARSER_DDL_DATABASE_CREATE,                    pg_parser_DDL_create_database                    },
    {PG_PARSER_DDL_DATABASE_DROP,                      pg_parser_DDL_drop_database                      },
    {PG_PARSER_DDL_VIEW_DROP,                          pg_parser_DDL_drop_view                          },
    {PG_PARSER_DDL_TABLE_CREATE,                       pg_parser_DDL_create_table                       },
    {PG_PARSER_DDL_TABLE_CREATE_PARTITION,             pg_parser_DDL_create_table_partition             },
    {PG_PARSER_DDL_TABLE_CREATE_PARTITION_SUB,         pg_parser_DDL_create_table_partition_sub         }, /* Contains pg_node_tree */
    {PG_PARSER_DDL_TABLE_DROP,                         pg_parser_DDL_drop_table                         },
    {PG_PARSER_DDL_TABLE_ALTER_TABLE_SET_LOG,          pg_parser_DDL_alter_table_set_log                },
    {PG_PARSER_DDL_TABLE_ALTER_ADD_COLUMN,             pg_parser_DDL_alter_table_add_column             },
    {PG_PARSER_DDL_TABLE_ALTER_ADD_CONSTRAINT,         pg_parser_DDL_alter_table_add_constraint         },
    {PG_PARSER_DDL_TABLE_ALTER_ADD_CONSTRAINT_FOREIGN, pg_parser_DDL_alter_table_add_constraint_foreign },
    {PG_PARSER_DDL_TABLE_ALTER_DROP_CONSTRAINT,        pg_parser_DDL_alter_table_drop_constraint        },
    {PG_PARSER_DDL_TABLE_ALTER_COLUMN_TYPE,            pg_parser_DDL_alter_table_column_alter_type      },
    {PG_PARSER_DDL_TABLE_ALTER_COLUMN_DEFAULT,
     pg_parser_DDL_alter_table_column_add_default                                                       }, /* Contains pg_node_tree */
    {PG_PARSER_DDL_TABLE_ALTER_COLUMN_DROP_DEFAULT,    pg_parser_DDL_alter_table_column_drop_default    },
    {PG_PARSER_DDL_INDEX_CREATE,                       pg_parser_DDL_create_index                       },
    {PG_PARSER_DDL_INDEX_DROP,                         pg_parser_DDL_drop_index                         },
    {PG_PARSER_DDL_TOAST_INDEX_DROP_ESCAPE,            pg_parser_DDL_toast_index_drop_escape            },
    {PG_PARSER_DDL_SEQUENCE_CREATE,                    pg_parser_DDL_create_sequence                    },
    {PG_PARSER_DDL_SEQUENCE_DROP,                      pg_parser_DDL_drop_sequence                      },
    {PG_PARSER_DDL_TOAST_ESCAPE,                       pg_parser_DDL_toast_escape                       },
    {PG_PARSER_DDL_VIEW_CREATE,                        pg_parser_DDL_create_view                        }, /* Contains pg_node_tree */
    {PG_PARSER_DDL_FUNCTION_CREATE,                    NULL                                             },
    {PG_PARSER_DDL_FUNCTION_DROP,                      NULL                                             },
    {PG_PARSER_DDL_TRIGGER_CREATE,                     NULL                                             },
    {PG_PARSER_DDL_TRIGGER_DROP,                       NULL                                             },
    {PG_PARSER_DDL_TYPE_CREATE,                        pg_parser_DDL_create_type                        },
    {PG_PARSER_DDL_TYPE_DROP,                          pg_parser_DDL_drop_type                          }
};

bool pg_parser_DDL_transRecord2DDL(pg_parser_translog_systb2ddl* pg_parser_ddl,
                                   pg_parser_translog_ddlstmt**  pg_parser_ddl_result,
                                   int32_t*                      pg_parser_errno)
{
    int32_t                                 i = 0;
    pg_parser_ddlstate                      ddlstate = {'\0'};
    pg_parser_translog_convertinfo_with_zic zicinfo = {'\0'};
    pg_parser_translog_systb2dll_record*    current_record = pg_parser_ddl->m_record;
    pg_parser_translog_ddlstmt *            result = NULL, *current_result = NULL, *next_result = NULL;
    zicinfo.convertinfo = pg_parser_ddl->m_convert;
    zicinfo.dbtype = pg_parser_ddl->m_dbtype;
    zicinfo.dbversion = pg_parser_ddl->m_dbversion;
    zicinfo.errorno = pg_parser_errno;
    zicinfo.debuglevel = pg_parser_ddl->m_debugLevel;

    ddlstate.m_zicinfo = &zicinfo;

    /* Ensure error code is 0 */
    *pg_parser_errno = 0;

    while (current_record)
    {
        if (!ddlstate.m_inddl)
        {
            current_result = pg_parser_ddl_firstTransDDL(pg_parser_ddl, current_record, &ddlstate, pg_parser_errno);
        }
        else
        {
            for (i = PG_PARSER_DDL_TRANSDDL_NUM_SHORT; i < PG_PARSER_DDL_TRANSDDL_NUM_MAX; i++)
            {
                if (m_ddl_transfunc[i].m_ddlKind == ddlstate.m_ddlKind)
                {
                    current_result =
                        m_ddl_transfunc[i].m_transfunc(pg_parser_ddl, current_record, &ddlstate, pg_parser_errno);
                    break;
                }
            }
        }
        /* If error code is not zero, it indicates an error occurred during DDL parsing, should
         * return error */
        if (0 != *pg_parser_errno)
        {
            return false;
        }
        /* When current result exists it means we parsed a DDL, point to next */
        if (current_result)
        {
            if (!result)
            {
                result = next_result = current_result;
            }
            else
            {
                next_result->m_next = current_result;
                next_result = next_result->m_next;
            }
            current_result = NULL;
        }

        current_record = current_record->m_next;
    }
    *pg_parser_ddl_result = result;
    return true;
}

pg_parser_translog_ddlstmt* pg_parser_ddl_firstTransDDL(pg_parser_translog_systb2ddl*        pg_parser_ddl,
                                                        pg_parser_translog_systb2dll_record* current_record,
                                                        pg_parser_ddlstate*                  ddlstate,
                                                        int32_t*                             pg_parser_errno)
{
    int32_t                     i = 0;
    pg_parser_translog_ddlstmt* current_result = NULL;
    if (IS_INSERT(current_record->m_record))
    {
        pg_parser_ddl_insertRecordTrans(pg_parser_ddl, current_record, ddlstate, pg_parser_errno);
    }
    else if (IS_DELETE(current_record->m_record))
    {
        pg_parser_ddl_deleteRecordTrans(pg_parser_ddl, current_record, ddlstate, pg_parser_errno);
    }
    else if (IS_UPDATE(current_record->m_record))
    {
        pg_parser_ddl_updateRecordTrans(pg_parser_ddl, current_record, ddlstate, pg_parser_errno);
    }
    /* If error occurs, return early */
    if (ERRNO_SUCCESS != *pg_parser_errno)
    {
        return NULL;
    }
    if (ddlstate->m_inddl)
    {
        for (i = 0; i < PG_PARSER_DDL_TRANSDDL_NUM_SHORT; i++)
        {
            if (m_ddl_transfunc[i].m_ddlKind == ddlstate->m_ddlKind)
            {
                current_result =
                    m_ddl_transfunc[i].m_transfunc(pg_parser_ddl, current_record, ddlstate, pg_parser_errno);
            }
        }
    }
    return current_result;
}

void pg_parser_ddl_init_ddlstate(pg_parser_ddlstate* ddlstate)
{
    pg_parser_translog_convertinfo_with_zic* zicinfo = ddlstate->m_zicinfo;
    if (ddlstate->m_attList)
    {
        pg_parser_list_free(ddlstate->m_attList);
    }
    if (ddlstate->m_enumlist)
    {
        pg_parser_list_free(ddlstate->m_enumlist);
    }
    if (ddlstate->m_attrdef_list)
    {
        pg_parser_list_free(ddlstate->m_attrdef_list);
    }
    if (ddlstate->m_inherits_oid)
    {
        pg_parser_mcxt_free(PG_PARSER_DDLSTMT_MCXT, ddlstate->m_inherits_oid);
    }

    rmemset1(ddlstate, 0, 0, sizeof(pg_parser_ddlstate));
    ddlstate->m_zicinfo = zicinfo;
}

static pg_parser_translog_ddlstmt* pg_parser_DDL_toast_index_drop_escape(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record,
    pg_parser_ddlstate*                  ddlstate,
    int32_t*                             pg_parser_errno)
{
    pg_parser_translog_ddlstmt* result = NULL;

    PG_PARSER_UNUSED(pg_parser_ddl);
    PG_PARSER_UNUSED(pg_parser_errno);

    if (IS_DELETE(current_record->m_record))
    {
        if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname,
                                       SYS_DEPEND,
                                       pg_parser_ddl->m_dbtype,
                                       pg_parser_ddl->m_dbversion))
        {
            char* temp_objid = NULL;
            char* temp_classid = NULL;
            temp_objid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("objid",
                                                            current_record->m_record->m_old_values,
                                                            current_record->m_record->m_valueCnt,
                                                            temp_objid);
            temp_classid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("classid",
                                                              current_record->m_record->m_old_values,
                                                              current_record->m_record->m_valueCnt,
                                                              temp_objid);
            if (!strcmp(ddlstate->m_reloid_char, temp_objid) && !strcmp(RelationRelationIdChar, temp_classid))
            {
                pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, "DEBUG, DDL PARSER: skip stmt abot toast end \n");
                pg_parser_ddl_init_ddlstate(ddlstate);
            }
        }
    }
    return result;
}

static pg_parser_translog_ddlstmt* pg_parser_DDL_toast_escape(pg_parser_translog_systb2ddl*        pg_parser_ddl,
                                                              pg_parser_translog_systb2dll_record* current_record,
                                                              pg_parser_ddlstate*                  ddlstate,
                                                              int32_t*                             pg_parser_errno)
{
    pg_parser_translog_ddlstmt* result = NULL;

    PG_PARSER_UNUSED(pg_parser_ddl);
    PG_PARSER_UNUSED(pg_parser_errno);

    if (IS_INSERT(current_record->m_record))
    {
        if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname,
                                       SYS_DEPEND,
                                       pg_parser_ddl->m_dbtype,
                                       pg_parser_ddl->m_dbversion))
        {
            char* temp_objid = NULL;
            char* temp_classid = NULL;
            temp_objid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("objid",
                                                            current_record->m_record->m_new_values,
                                                            current_record->m_record->m_valueCnt,
                                                            temp_objid);
            temp_classid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("classid",
                                                              current_record->m_record->m_new_values,
                                                              current_record->m_record->m_valueCnt,
                                                              temp_classid);
            if (!strcmp(ddlstate->m_reloid_char, temp_objid) && !strcmp(RelationRelationIdChar, temp_classid))
            {
                pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, "DEBUG, DDL PARSER: skip stmt abot toast end \n");
                pg_parser_ddl_init_ddlstate(ddlstate);
            }
        }
    }
    if (IS_DELETE(current_record->m_record))
    {
        if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname,
                                       SYS_DEPEND,
                                       pg_parser_ddl->m_dbtype,
                                       pg_parser_ddl->m_dbversion))
        {
            char* temp_objid = NULL;
            char* temp_classid = NULL;
            temp_objid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("objid",
                                                            current_record->m_record->m_old_values,
                                                            current_record->m_record->m_valueCnt,
                                                            temp_objid);
            temp_classid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("classid",
                                                              current_record->m_record->m_old_values,
                                                              current_record->m_record->m_valueCnt,
                                                              temp_objid);
            if (!strcmp(ddlstate->m_reloid_char, temp_objid) && !strcmp(RelationRelationIdChar, temp_classid))
            {
                pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, "DEBUG, DDL PARSER: skip stmt abot toast end \n");
                pg_parser_ddl_init_ddlstate(ddlstate);
            }
        }
    }
    return result;
}

char* ddl_char_tolower(char* output)
{
    int i = 0;

    for (i = 0; i < (int32_t)strlen(output); i++)
    {
        if (output[i] >= 'A' && output[i] <= 'Z')
        {
            output[i] += 'a' - 'A';
        }
    }
    return output;
}

bool pg_parser_check_table_name(char* tablename, SysdictName sysdictnum, int16_t dbtype, char* dbversion)
{
    PG_PARSER_UNUSED(dbtype);
    PG_PARSER_UNUSED(dbversion);
    if (TEMPTABLE_PREFIX == sysdictnum)
    {
        return !strncmp(pg_parser_postgresql_v127[sysdictnum].name, ddl_char_tolower(tablename), 7);
    }
    else
    {
        return !strcmp(pg_parser_postgresql_v127[sysdictnum].name, ddl_char_tolower(tablename));
    }
}
