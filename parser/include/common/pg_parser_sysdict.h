#ifndef PG_PARSER_SYSDICT_H
#define PG_PARSER_SYSDICT_H

#include "sysdict/pg_parser_sysdict_pg_class.h"
#include "sysdict/pg_parser_sysdict_pg_attribute.h"
#include "sysdict/pg_parser_sysdict_pg_namespace.h"
#include "sysdict/pg_parser_sysdict_pg_enum.h"
#include "sysdict/pg_parser_sysdict_pg_type.h"
#include "sysdict/pg_parser_sysdict_pg_proc.h"
#include "sysdict/pg_parser_sysdict_pg_range.h"
#include "sysdict/pg_parser_sysdict_pg_constraint.h"
#include "sysdict/pg_parser_sysdict_pg_operator.h"
#include "sysdict/pg_parser_sysdict_pg_authid.h"
#include "sysdict/pg_parser_sysdict_pg_database.h"
#include "sysdict/pg_parser_sysdict_pg_index.h"

typedef enum PG_PARSER_SYSDICT_TYPE
{
    PG_PARSER_SYSDICT_PG_CLASS_TYPE = 0,
    PG_PARSER_SYSDICT_PG_ATTRIBUTES_TYPE,
    PG_PARSER_SYSDICT_PG_NAMESPACE_TYPE,
    PG_PARSER_SYSDICT_PG_ENUM_TYPE,
    PG_PARSER_SYSDICT_PG_TYPE_TYPE,
    PG_PARSER_SYSDICT_PG_PROC_TYPE,
    PG_PARSER_SYSDICT_PG_RANGE_TYPE,
    PG_PARSER_SYSDICT_PG_CONSTRAINT_TYPE
} pg_parser_sysdict_type;

typedef struct PG_PARSER_SYSDICT_PGCLASS_DICT
{
    int32_t                            m_count;
    pg_parser_sysdict_pgclass      *m_pg_class;
} pg_parser_sysdict_pgclass_dict;

typedef struct PG_PARSER_SYSDICT_PGATTRIBUTES_DICT
{
    int32_t                            m_count;
    pg_parser_sysdict_pgattributes *m_pg_attributes;
} pg_parser_sysdict_pgattributes_dict;


typedef struct PG_PARSER_SYSDICT_PGNAMESPACE_DICT
{
    int32_t                            m_count;
    pg_parser_sysdict_pgnamespace  *m_pg_namespace;
} pg_parser_sysdict_pgnamespace_dict;


typedef struct PG_PARSER_SYSDICT_PGENUM_DICT
{
    int32_t                            m_count;
    pg_parser_sysdict_pgenum       *m_pg_enum;
} pg_parser_sysdict_pgenum_dict;

typedef struct PG_PARSER_SYSDICT_PGTYPE_DICT
{
    int32_t                            m_count;
    pg_parser_sysdict_pgtype       *m_pg_type;
} pg_parser_sysdict_pgtype_dict;

typedef struct PG_PARSER_SYSDICT_PGPROC_DICT
{
    int32_t                            m_count;
    pg_parser_sysdict_pgproc        *m_pg_proc;
} pg_parser_sysdict_pgproc_dict;

typedef struct PG_PARSER_SYSDICT_PGRANGE_DICT
{
    int32_t                            m_count;
    pg_parser_sysdict_pgrange      *m_pg_range;
} pg_parser_sysdict_pgrange_dict;

#if 0
typedef struct PG_PARSER_SYSDCIT_PGLANGUAGE
{
    int32_t                            m_count;
    pg_parser_sysdict_             *m_pg_language;
} pg_parser_sysdict_pglanguage;


typedef struct PG_PARSER_SYSDICT_PGCONSTRAINT_DICT
{
    int32_t                            m_count;
    pg_parser_sysdict_pgconstraint *m_pg_constraint;
} pg_parser_sysdict_pgconstraint_dict;
#endif

#endif
