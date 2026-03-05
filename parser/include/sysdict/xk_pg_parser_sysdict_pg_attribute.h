#ifndef XK_PG_PARSER_SYSDICT_PG_ATTRIBUTE_H
#define XK_PG_PARSER_SYSDICT_PG_ATTRIBUTE_H

#define AttributeRelationId 1249
#define AttributeRelation_Rowtype_Id 75
#define xk_pg_parser_InvalidAttrNumber 0
typedef struct XK_PG_PARSER_SYSDICT_PGATTRIBUTES
{
    uint32_t                     attrelid;        /* OID of relation containing this attribute */
    xk_pg_parser_NameData        attname;         /* name of attribute */
    uint32_t                     atttypid;
    int32_t                      attstattarget;
    int16_t                      attlen;
    int16_t                      attnum;
    int32_t                      attndims;
    int32_t                      attcacheoff;
    int32_t                      atttypmod;
    bool                         attbyval;

#if XK_PG_VERSION_NUM >= 140000
    char                         attalign;
    char                         attstorage;
    char                         attcompression BKI_DEFAULT('\0');
#else
    char                         attstorage;
    char                         attalign;
#endif
    bool                         attnotnull;
    bool                         atthasdef;

#if XK_PG_VERSION_NUM >= 110000
    bool                         atthasmissing;
#endif

#if XK_PG_VERSION_NUM >= 100000
    char                         attidentity;
#endif

#if XK_PG_VERSION_NUM >= 120000
    char                         attgenerated;
#endif

    bool                         attisdropped;
    bool                         attislocal;
    int32_t                      attinhcount;
    uint32_t                     attcollation;

#ifdef CATALOG_VARLEN /* 变长字段, 这里虽然用不到, 但是仍做保留 */
    aclitem                      attacl[1];
    xk_pg_parser_text                         attoptions[1];
    xk_pg_parser_text                         attfdwoptions[1];

#if XK_PG_VERSION_NUM >= 110000
    anyarray                     attmissingval;
#endif

#endif
} xk_pg_parser_sysdict_pgattributes;


typedef xk_pg_parser_sysdict_pgattributes *xk_pg_sysdict_Form_pg_attribute;

#define          XK_PG_SYSDICT_ATTRIBUTE_IDENTITY_ALWAYS        'a'
#define          XK_PG_SYSDICT_ATTRIBUTE_IDENTITY_BY_DEFAULT 'd'
#define          XK_PG_SYSDICT_ATTRIBUTE_GENERATED_STORED    's'

#endif
