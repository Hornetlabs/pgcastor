#ifndef XK_PG_PARSER_SYSDICT_PG_ENUM_H
#define XK_PG_PARSER_SYSDICT_PG_ENUM_H

#define EnumRelationId 3501

typedef struct XK_PG_PARSER_SYSDICT_PGENUM
{
#if XK_PG_VERSION_NUM >= 120000
    uint32_t                 oid;
#endif
    uint32_t                 enumtypid;
    xk_pg_parser_NameData    enumlabel;
} xk_pg_parser_sysdict_pgenum;

typedef xk_pg_parser_sysdict_pgenum *xk_pg_sysdict_Form_pg_enum;

#endif
