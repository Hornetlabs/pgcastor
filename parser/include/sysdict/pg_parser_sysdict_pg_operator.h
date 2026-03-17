#ifndef XK_PG_PARSER_SYSDICT_PG_OPERATOR_H
#define XK_PG_PARSER_SYSDICT_PG_OPERATOR_H

#define OperatorRelationId 2617


typedef struct XK_PG_PARSER_SYSDICT_PGOPERATOR
{
#if XK_PG_VERSION_NUM >= 120000
    uint32_t                oid;
#endif
    xk_pg_parser_NameData   oprname;         /* operator name */
} xk_pg_parser_sysdict_pgoperator;
typedef xk_pg_parser_sysdict_pgoperator *xk_pg_sysdict_Form_pg_operator;


#endif
