#ifndef PG_PARSER_SYSDICT_PG_RANGE_H
#define PG_PARSER_SYSDICT_PG_RANGE_H

#define RangeRelationId 3541

typedef struct PG_PARSER_SYSDICT_PGRANGE
{
    uint32_t rngtypid;
    uint32_t rngsubtype;
} pg_parser_sysdict_pgrange;

typedef pg_parser_sysdict_pgrange* pg_sysdict_Form_pg_range;

#endif
