#ifndef PG_PARSER_SYSDICT_PG_ENUM_H
#define PG_PARSER_SYSDICT_PG_ENUM_H

#define EnumRelationId 3501

typedef struct PG_PARSER_SYSDICT_PGENUM
{
    uint32_t           oid;
    uint32_t           enumtypid;
    pg_parser_NameData enumlabel;
} pg_parser_sysdict_pgenum;

typedef pg_parser_sysdict_pgenum* pg_sysdict_Form_pg_enum;

#endif
