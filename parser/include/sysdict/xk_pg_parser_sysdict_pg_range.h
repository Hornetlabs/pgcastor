#ifndef XK_PG_PARSER_SYSDICT_PG_RANGE_H
#define XK_PG_PARSER_SYSDICT_PG_RANGE_H

#define RangeRelationId 3541

typedef struct XK_PG_PARSER_SYSDICT_PGRANGE
{
    uint32_t rngtypid;
    uint32_t rngsubtype;
} xk_pg_parser_sysdict_pgrange;

typedef xk_pg_parser_sysdict_pgrange *xk_pg_sysdict_Form_pg_range;

#endif
