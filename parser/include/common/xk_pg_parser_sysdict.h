#ifndef XK_PG_PARSER_SYSDICT_H
#define XK_PG_PARSER_SYSDICT_H

#include "sysdict/xk_pg_parser_sysdict_pg_class.h"
#include "sysdict/xk_pg_parser_sysdict_pg_attribute.h"
#include "sysdict/xk_pg_parser_sysdict_pg_namespace.h"
#include "sysdict/xk_pg_parser_sysdict_pg_enum.h"
#include "sysdict/xk_pg_parser_sysdict_pg_type.h"
#include "sysdict/xk_pg_parser_sysdict_pg_proc.h"
#include "sysdict/xk_pg_parser_sysdict_pg_range.h"
#include "sysdict/xk_pg_parser_sysdict_pg_constraint.h"
#include "sysdict/xk_pg_parser_sysdict_pg_operator.h"
#include "sysdict/xk_pg_parser_sysdict_pg_authid.h"
#include "sysdict/xk_pg_parser_sysdict_pg_database.h"
#include "sysdict/xk_pg_parser_sysdict_pg_index.h"

typedef enum XK_PG_PARSER_SYSDICT_TYPE
{
    XK_PG_PARSER_SYSDICT_PG_CLASS_TYPE = 0,
    XK_PG_PARSER_SYSDICT_PG_ATTRIBUTES_TYPE,
    XK_PG_PARSER_SYSDICT_PG_NAMESPACE_TYPE,
    XK_PG_PARSER_SYSDICT_PG_ENUM_TYPE,
    XK_PG_PARSER_SYSDICT_PG_TYPE_TYPE,
    XK_PG_PARSER_SYSDICT_PG_PROC_TYPE,
    XK_PG_PARSER_SYSDICT_PG_RANGE_TYPE,
    XK_PG_PARSER_SYSDICT_PG_CONSTRAINT_TYPE
} xk_pg_parser_sysdict_type;

typedef struct XK_PG_PARSER_SYSDICT_PGCLASS_DICT
{
    int32_t                            m_count;
    xk_pg_parser_sysdict_pgclass      *m_pg_class;
} xk_pg_parser_sysdict_pgclass_dict;

typedef struct XK_PG_PARSER_SYSDICT_PGATTRIBUTES_DICT
{
    int32_t                            m_count;
    xk_pg_parser_sysdict_pgattributes *m_pg_attributes;
} xk_pg_parser_sysdict_pgattributes_dict;


typedef struct XK_PG_PARSER_SYSDICT_PGNAMESPACE_DICT
{
    int32_t                            m_count;
    xk_pg_parser_sysdict_pgnamespace  *m_pg_namespace;
} xk_pg_parser_sysdict_pgnamespace_dict;


typedef struct XK_PG_PARSER_SYSDICT_PGENUM_DICT
{
    int32_t                            m_count;
    xk_pg_parser_sysdict_pgenum       *m_pg_enum;
} xk_pg_parser_sysdict_pgenum_dict;

typedef struct XK_PG_PARSER_SYSDICT_PGTYPE_DICT
{
    int32_t                            m_count;
    xk_pg_parser_sysdict_pgtype       *m_pg_type;
} xk_pg_parser_sysdict_pgtype_dict;

typedef struct XK_PG_PARSER_SYSDICT_PGPROC_DICT
{
    int32_t                            m_count;
    xk_pg_parser_sysdict_pgproc        *m_pg_proc;
} xk_pg_parser_sysdict_pgproc_dict;

typedef struct XK_PG_PARSER_SYSDICT_PGRANGE_DICT
{
    int32_t                            m_count;
    xk_pg_parser_sysdict_pgrange      *m_pg_range;
} xk_pg_parser_sysdict_pgrange_dict;

#if 0
typedef struct XK_PG_PARSER_SYSDCIT_PGLANGUAGE
{
    int32_t                            m_count;
    xk_pg_parser_sysdict_             *m_pg_language;
} xk_pg_parser_sysdict_pglanguage;


typedef struct XK_PG_PARSER_SYSDICT_PGCONSTRAINT_DICT
{
    int32_t                            m_count;
    xk_pg_parser_sysdict_pgconstraint *m_pg_constraint;
} xk_pg_parser_sysdict_pgconstraint_dict;
#endif

#endif
