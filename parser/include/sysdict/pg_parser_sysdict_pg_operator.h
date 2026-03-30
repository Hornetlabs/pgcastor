#ifndef PG_PARSER_SYSDICT_PG_OPERATOR_H
#define PG_PARSER_SYSDICT_PG_OPERATOR_H

#define OperatorRelationId 2617

typedef struct PG_PARSER_SYSDICT_PGOPERATOR
{
    uint32_t           oid;
    pg_parser_NameData oprname; /* operator name */
} pg_parser_sysdict_pgoperator;

typedef pg_parser_sysdict_pgoperator* pg_sysdict_Form_pg_operator;

#endif
