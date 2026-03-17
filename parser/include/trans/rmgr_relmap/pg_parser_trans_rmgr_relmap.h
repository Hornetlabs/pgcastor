#ifndef PG_PARSER_TRANS_RMGR_RELMAP_H
#define PG_PARSER_TRANS_RMGR_RELMAP_H

#define PG_PARSER_MAX_MAPPINGS           62    /* 62 * 8 + 16 = 512 */
#define PG_PARSER_XLOG_RELMAP_UPDATE     0x00

typedef struct pg_parser_xl_relmap_update
{
    uint32_t    dbid;           /* database ID, or 0 for shared map */
    uint32_t    tsid;           /* database's tablespace, or pg_global */
    int32_t     nbytes;         /* size of relmap data */
    char        data[FLEXIBLE_ARRAY_MEMBER];
} pg_parser_xl_relmap_update;

typedef struct pg_parser_RelMapping
{
    uint32_t    mapoid;         /* OID of a catalog */
    uint32_t    mapfilenode;    /* its filenode number */
} pg_parser_RelMapping;

typedef struct pg_parser_RelMapFile
{
    int32_t                 magic;          /* always RELMAPPER_FILEMAGIC */
    int32_t                 num_mappings;   /* number of valid RelMapping entries */
    pg_parser_RelMapping mappings[PG_PARSER_MAX_MAPPINGS];
    pg_parser_crc32c     crc;            /* CRC of all above */
    int32_t                 pad;            /* to make the struct size be 512 exactly */
} pg_parser_RelMapFile;

extern bool pg_parser_trans_rmgr_relmap_pre(pg_parser_trans_transrec_decode_XLogReaderState *state,
                            pg_parser_translog_pre_base **result, 
                            int32_t *pg_parser_errno);


#endif
