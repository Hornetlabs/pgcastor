#ifndef XK_PG_PARSER_THIRDPARTY_PGCONVERT_H
#define XK_PG_PARSER_THIRDPARTY_PGCONVERT_H

typedef enum xk_pg_convert_enum
{
    indextype_convert = 1,
    seqtype_convert = 2
}xk_pg_convert_enum;


extern uint32_t xk_pg_convert_typid2type(uint32_t oid, xk_pg_convert_enum type);

#endif
