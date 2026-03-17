#ifndef PG_PARSER_THIRDPARTY_PGCONVERT_H
#define PG_PARSER_THIRDPARTY_PGCONVERT_H

typedef enum pg_convert_enum
{
    indextype_convert = 1,
    seqtype_convert = 2
}pg_convert_enum;


extern uint32_t pg_convert_typid2type(uint32_t oid, pg_convert_enum type);

#endif
