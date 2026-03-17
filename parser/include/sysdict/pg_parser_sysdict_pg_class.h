#ifndef XK_PG_PARSER_SYSDICT_PG_CLASS_H
#define XK_PG_PARSER_SYSDICT_PG_CLASS_H

typedef struct XK_PG_PARSER_SYSDICT_PGCLASS
{
#if XK_PG_VERSION_NUM >= 120000
    uint32_t                oid;
#endif
    xk_pg_parser_NameData   relname;         /* class name */
    uint32_t                relnamespace;    /* namespace oid */
    uint32_t                reltype;         /* rel type oid */
    uint32_t                relfilenode;     /* file oid */
    char                    relkind;         /* table kind */
    int16_t                 relnatts;        /* column num */
    uint32_t                reltoastrelid;   /* toast table */
    uint32_t                reltablespace;
    char                    relreplident;
    char                    relpersistence;
    uint32_t                relowner;
    xk_pg_parser_NameData   nspname;
    bool                    relhaspk;
    bool                    relhasindex;
} xk_pg_parser_sysdict_pgclass;

typedef xk_pg_parser_sysdict_pgclass *xk_pg_sysdict_Form_pg_class;

#define RelationRelationId 1259
#define RelationRelationIdChar "1259"
#define RelationRelation_Rowtype_Id 83

#define RewriteRelationId 2618
#define RewriteRelationIdChar "2618"

#define AccessMethodRelationId 2601
#define CollationRelationId 3456
#define OperatorClassRelationId 2616
#define OperatorRelationId 2617

#define ConstraintRelationId 2606
#define ConstraintRelationIdChar "2606"

#define AttrDefaultRelationId 2604

#define PG_TOAST_NAME "pg_toast"

#define XK_PG_SYSDICT_RELKIND_RELATION              'r'    /* ordinary table */
#define XK_PG_SYSDICT_RELKIND_INDEX                 'i'    /* secondary index */
#define XK_PG_SYSDICT_RELKIND_SEQUENCE              'S'    /* sequence object */
#define XK_PG_SYSDICT_RELKIND_TOASTVALUE            't'    /* for out-of-line values */
#define XK_PG_SYSDICT_RELKIND_VIEW                  'v'    /* view */
#define XK_PG_SYSDICT_RELKIND_MATVIEW               'm'    /* materialized view */
#define XK_PG_SYSDICT_RELKIND_COMPOSITE_TYPE        'c'    /* composite type */
#define XK_PG_SYSDICT_RELKIND_FOREIGN_TABLE         'f'    /* foreign table */
#define XK_PG_SYSDICT_RELKIND_PARTITIONED_TABLE     'p'    /* partitioned table */
#define XK_PG_SYSDICT_RELKIND_PARTITIONED_INDEX     'I'    /* partitioned index */

#define XK_PG_SYSDICT_RELPERSISTENCE_PERMANENT      'p'    /* regular table */
#define XK_PG_SYSDICT_RELPERSISTENCE_UNLOGGED       'u'    /* unlogged permanent table */
#define XK_PG_SYSDICT_RELPERSISTENCE_TEMP           't'    /* temporary table */

/* default selection for replica identity (primary key or nothing) */
#define XK_PG_SYSDICT_REPLICA_IDENTITY_DEFAULT      'd'
/* no replica identity is logged for this relation */
#define XK_PG_SYSDICT_REPLICA_IDENTITY_NOTHING      'n'
/* all columns are logged as replica identity */
#define XK_PG_SYSDICT_REPLICA_IDENTITY_FULL         'f'
/*
 * an explicitly chosen candidate key's columns are used as replica identity.
 * Note this will still be set if the index has been dropped; in that case it
 * has the same meaning as 'd'.
 */
#define XK_PG_SYSDICT_REPLICA_IDENTITY_INDEX    'i'

#if XK_PG_VERSION_NUM >= 120000
/*
 * Relation kinds that have physical storage. These relations normally have
 * relfilenode set to non-zero, but it can also be zero if the relation is
 * mapped.
 */
#define XK_PG_SYSDICT_RELKIND_HAS_STORAGE(relkind) \
    ((relkind) == XK_PG_SYSDICT_RELKIND_RELATION || \
     (relkind) == XK_PG_SYSDICT_RELKIND_INDEX || \
     (relkind) == XK_PG_SYSDICT_RELKIND_SEQUENCE || \
     (relkind) == XK_PG_SYSDICT_RELKIND_TOASTVALUE || \
     (relkind) == XK_PG_SYSDICT_RELKIND_MATVIEW)
#endif


#endif
