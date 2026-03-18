#ifndef PG_PARSER_SYSDICT_PG_CLASS_H
#define PG_PARSER_SYSDICT_PG_CLASS_H

typedef struct PG_PARSER_SYSDICT_PGCLASS
{
    uint32_t           oid;
    pg_parser_NameData relname;       /* class name */
    uint32_t           relnamespace;  /* namespace oid */
    uint32_t           reltype;       /* rel type oid */
    uint32_t           relfilenode;   /* file oid */
    char               relkind;       /* table kind */
    int16_t            relnatts;      /* column num */
    uint32_t           reltoastrelid; /* toast table */
    uint32_t           reltablespace;
    char               relreplident;
    char               relpersistence;
    uint32_t           relowner;
    pg_parser_NameData nspname;
    bool               relhaspk;
    bool               relhasindex;
} pg_parser_sysdict_pgclass;

typedef pg_parser_sysdict_pgclass* pg_sysdict_Form_pg_class;

#define RelationRelationId                   1259
#define RelationRelationIdChar               "1259"
#define RelationRelation_Rowtype_Id          83

#define RewriteRelationId                    2618
#define RewriteRelationIdChar                "2618"

#define AccessMethodRelationId               2601
#define CollationRelationId                  3456
#define OperatorClassRelationId              2616
#define OperatorRelationId                   2617

#define ConstraintRelationId                 2606
#define ConstraintRelationIdChar             "2606"

#define AttrDefaultRelationId                2604

#define PG_TOAST_NAME                        "pg_toast"

#define PG_SYSDICT_RELKIND_RELATION          'r' /* ordinary table */
#define PG_SYSDICT_RELKIND_INDEX             'i' /* secondary index */
#define PG_SYSDICT_RELKIND_SEQUENCE          'S' /* sequence object */
#define PG_SYSDICT_RELKIND_TOASTVALUE        't' /* for out-of-line values */
#define PG_SYSDICT_RELKIND_VIEW              'v' /* view */
#define PG_SYSDICT_RELKIND_MATVIEW           'm' /* materialized view */
#define PG_SYSDICT_RELKIND_COMPOSITE_TYPE    'c' /* composite type */
#define PG_SYSDICT_RELKIND_FOREIGN_TABLE     'f' /* foreign table */
#define PG_SYSDICT_RELKIND_PARTITIONED_TABLE 'p' /* partitioned table */
#define PG_SYSDICT_RELKIND_PARTITIONED_INDEX 'I' /* partitioned index */

#define PG_SYSDICT_RELPERSISTENCE_PERMANENT  'p' /* regular table */
#define PG_SYSDICT_RELPERSISTENCE_UNLOGGED   'u' /* unlogged permanent table */
#define PG_SYSDICT_RELPERSISTENCE_TEMP       't' /* temporary table */

/* default selection for replica identity (primary key or nothing) */
#define PG_SYSDICT_REPLICA_IDENTITY_DEFAULT 'd'
/* no replica identity is logged for this relation */
#define PG_SYSDICT_REPLICA_IDENTITY_NOTHING 'n'
/* all columns are logged as replica identity */
#define PG_SYSDICT_REPLICA_IDENTITY_FULL 'f'
/*
 * an explicitly chosen candidate key's columns are used as replica identity.
 * Note this will still be set if the index has been dropped; in that case it
 * has the same meaning as 'd'.
 */
#define PG_SYSDICT_REPLICA_IDENTITY_INDEX 'i'

/*
 * Relation kinds that have physical storage. These relations normally have
 * relfilenode set to non-zero, but it can also be zero if the relation is
 * mapped.
 */
#define PG_SYSDICT_RELKIND_HAS_STORAGE(relkind)                                                \
    ((relkind) == PG_SYSDICT_RELKIND_RELATION || (relkind) == PG_SYSDICT_RELKIND_INDEX ||      \
     (relkind) == PG_SYSDICT_RELKIND_SEQUENCE || (relkind) == PG_SYSDICT_RELKIND_TOASTVALUE || \
     (relkind) == PG_SYSDICT_RELKIND_MATVIEW)

#endif
