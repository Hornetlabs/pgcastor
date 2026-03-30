#ifndef PG_PARSER_SYSDICT_PG_ATTRIBUTE_H
#define PG_PARSER_SYSDICT_PG_ATTRIBUTE_H

#define AttributeRelationId          1249
#define AttributeRelation_Rowtype_Id 75
#define pg_parser_InvalidAttrNumber  0

typedef struct PG_PARSER_SYSDICT_PGATTRIBUTES
{
    uint32_t           attrelid; /* OID of relation containing this attribute */
    pg_parser_NameData attname;  /* name of attribute */
    uint32_t           atttypid;
    int32_t            attstattarget;
    int16_t            attlen;
    int16_t            attnum;
    int32_t            attndims;
    int32_t            attcacheoff;
    int32_t            atttypmod;
    bool               attbyval;
    char               attstorage;
    char               attalign;
    bool               attnotnull;
    bool               atthasdef;
    bool               atthasmissing;
    char               attidentity;
    char               attgenerated;
    bool               attisdropped;
    bool               attislocal;
    int32_t            attinhcount;
    uint32_t           attcollation;

#ifdef CATALOG_VARLEN /* Variable-length fields, although not needed here, still kept for \
                         reference */
    aclitem        attacl[1];
    pg_parser_text attoptions[1];
    pg_parser_text attfdwoptions[1];

    anyarray       attmissingval;

#endif
} pg_parser_sysdict_pgattributes;

typedef pg_parser_sysdict_pgattributes* pg_sysdict_Form_pg_attribute;

#define PG_SYSDICT_ATTRIBUTE_IDENTITY_ALWAYS     'a'
#define PG_SYSDICT_ATTRIBUTE_IDENTITY_BY_DEFAULT 'd'
#define PG_SYSDICT_ATTRIBUTE_GENERATED_STORED    's'

#endif
