#ifndef XK_PG_PARSER_IMAGE_H
#define XK_PG_PARSER_IMAGE_H

typedef struct
{
    uint32_t        xlogid;            /* high bits */
    uint32_t        xrecoff;        /* low bits */
} xk_pg_parser_PageXLogRecPtr;

typedef struct xk_pg_parser_PageHeaderData
{
    /* XXX LSN is member of *any* block, not only page-organized ones */
    xk_pg_parser_PageXLogRecPtr pd_lsn;        /* LSN: next byte after last byte of xlog
                                                * record for last change to this page */
    uint16_t                    pd_checksum;   /* checksum */
    uint16_t                    pd_flags;      /* flag bits, see below */
    uint16_t                    pd_lower;      /* offset to start of free space */
    uint16_t                    pd_upper;      /* offset to end of free space */
    uint16_t                    pd_special;    /* offset to start of special space */
    uint16_t                    pd_pagesize_version;
    xk_pg_parser_TransactionId               pd_prune_xid;  /* oldest prunable XID, or zero if none */
    xk_pg_parser_ItemIdData     pd_linp[FLEXIBLE_ARRAY_MEMBER]; /* line pointer array */
} xk_pg_parser_PageHeaderData;

typedef struct xk_pg_parser_PageHeaderData_Kingbase_sstorage
{
    /* XXX LSN is member of *any* block, not only page-organized ones */
    xk_pg_parser_PageXLogRecPtr pd_lsn;        /* LSN: next byte after last byte of xlog
                                 * record for last change to this page */
    union
    {
        uint16_t pd_crc;
        char    pd_sm3hash[32];
    }pd_checksum;    /* checksum */
    uint16_t        pd_flags;        /* flag bits, see below */
    uint16_t        pd_lower;        /* offset to start of free space */
    uint16_t        pd_upper;        /* offset to end of free space */
    uint16_t        pd_special;    /* offset to start of special space */
    uint16_t    pd_pagesize_version;
    xk_pg_parser_TransactionId pd_prune_xid; /* oldest prunable XID, or zero if none */
    xk_pg_parser_ItemIdData    pd_linp[FLEXIBLE_ARRAY_MEMBER]; /* line pointer array */
} xk_pg_parser_PageHeaderData_Kingbase_sstorage;

typedef struct xk_pg_parser_PageHeaderData_Kingbase_bstorage
{
    /* XXX LSN is member of *any* block, not only page-organized ones */
    xk_pg_parser_PageXLogRecPtr pd_lsn;        /* LSN: next byte after last byte of xlog
                                 * record for last change to this page */
    union
    {
        uint16_t pd_crc;
        char    pd_sm3hash[32];
    }pd_checksum;    /* checksum */
    uint16_t        pd_flags;        /* flag bits, see below */
    uint16_t        pd_lower;        /* offset to start of free space */
    uint16_t        pd_upper;        /* offset to end of free space */
    uint16_t        pd_special;    /* offset to start of special space */
    uint32_t        pd_pagesize_version;
    xk_pg_parser_TransactionId pd_prune_xid; /* oldest prunable XID, or zero if none */
    xk_pg_parser_ItemIdData_Kingbase_sizeB    pd_linp[FLEXIBLE_ARRAY_MEMBER]; /* line pointer array */
} xk_pg_parser_PageHeaderData_Kingbase_bstorage;


typedef xk_pg_parser_PageHeaderData *xk_pg_parser_PageHeader;
typedef xk_pg_parser_PageHeaderData_Kingbase_sstorage *xk_pg_parser_PageHeader_Kingbase_S;
typedef xk_pg_parser_PageHeaderData_Kingbase_bstorage *xk_pg_parser_PageHeader_Kingbase_B;

#define xk_pg_parser_SizeOfPageHeaderData (offsetof(xk_pg_parser_PageHeaderData, pd_linp))
#define xk_pg_parser_SizeOfPageHeaderData_Kingbase_sizeS (offsetof(xk_pg_parser_PageHeaderData_Kingbase_sstorage, pd_linp))
#define xk_pg_parser_SizeOfPageHeaderData_Kingbase_sizeB (offsetof(xk_pg_parser_PageHeaderData_Kingbase_bstorage, pd_linp))

extern bool xk_pg_parser_image_get_block_image(xk_pg_parser_trans_transrec_decode_XLogReaderState *record,
                                                            uint8_t block_id,
                                                            char *page,
                                                            int32_t block_size);
extern xk_pg_parser_translog_tuplecache *xk_pg_parser_image_get_tuple_from_image(char *page,
                                                                                 uint32_t *tupcnt,
                                                                                 uint32_t pageno,
                                                                                 uint8_t debug_level);
extern xk_pg_parser_translog_tuplecache *xk_pg_parser_image_get_tuple_from_image_Kingbase_szieS(char *page,
                                                                          uint32_t *tupcnt,
                                                                          uint32_t pageno,
                                                                          uint8_t debug_level);
extern xk_pg_parser_translog_tuplecache *xk_pg_parser_image_get_tuple_from_image_Kingbase_szieB(char *page,
                                                                          uint32_t *tupcnt,
                                                                          uint32_t pageno,
                                                                          uint8_t debug_level);
extern xk_pg_parser_translog_tuplecache *xk_pg_parser_image_get_tuple_from_image_with_dbtype(
                                                                          int32_t dbtype,
                                                                          char *dbversion,
                                                                          uint32_t pagesize,
                                                                          char *page,
                                                                          uint32_t *tupcnt,
                                                                          uint32_t pageno,
                                                                          uint8_t  debug_level);

extern xk_pg_parser_translog_tuplecache *xk_pg_parser_image_getTupleFromCache(xk_pg_parser_translog_tuplecache *cache,
                                                                              uint32_t cnt,
                                                                              uint32_t off,
                                                                              uint32_t pageno);

extern bool check_page_have_item(char *page);
#endif
