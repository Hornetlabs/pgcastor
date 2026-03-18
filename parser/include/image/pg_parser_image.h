#ifndef PG_PARSER_IMAGE_H
#define PG_PARSER_IMAGE_H

typedef struct
{
    uint32_t xlogid;  /* high bits */
    uint32_t xrecoff; /* low bits */
} pg_parser_PageXLogRecPtr;

typedef struct pg_parser_PageHeaderData
{
    /* XXX LSN is member of *any* block, not only page-organized ones */
    pg_parser_PageXLogRecPtr pd_lsn;      /* LSN: next byte after last byte of xlog
                                           * record for last change to this page */
    uint16_t                 pd_checksum; /* checksum */
    uint16_t                 pd_flags;    /* flag bits, see below */
    uint16_t                 pd_lower;    /* offset to start of free space */
    uint16_t                 pd_upper;    /* offset to end of free space */
    uint16_t                 pd_special;  /* offset to start of special space */
    uint16_t                 pd_pagesize_version;
    pg_parser_TransactionId  pd_prune_xid;                   /* oldest prunable XID, or zero if none */
    pg_parser_ItemIdData     pd_linp[FLEXIBLE_ARRAY_MEMBER]; /* line pointer array */
} pg_parser_PageHeaderData;

typedef pg_parser_PageHeaderData* pg_parser_PageHeader;

#define pg_parser_SizeOfPageHeaderData (offsetof(pg_parser_PageHeaderData, pd_linp))

extern bool pg_parser_image_get_block_image(pg_parser_XLogReaderState* record,
                                            uint8_t                    block_id,
                                            char*                      page,
                                            int32_t                    block_size);
extern pg_parser_translog_tuplecache* pg_parser_image_get_tuple_from_image(char*     page,
                                                                           uint32_t* tupcnt,
                                                                           uint32_t  pageno,
                                                                           uint8_t   debug_level);

extern pg_parser_translog_tuplecache* pg_parser_image_get_tuple_from_image_with_dbtype(int32_t   dbtype,
                                                                                       char*     dbversion,
                                                                                       uint32_t  pagesize,
                                                                                       char*     page,
                                                                                       uint32_t* tupcnt,
                                                                                       uint32_t  pageno,
                                                                                       uint8_t   debug_level);

extern pg_parser_translog_tuplecache* pg_parser_image_getTupleFromCache(pg_parser_translog_tuplecache* cache,
                                                                        uint32_t                       cnt,
                                                                        uint32_t                       off,
                                                                        uint32_t                       pageno);

extern bool check_page_have_item(char* page);
#endif
