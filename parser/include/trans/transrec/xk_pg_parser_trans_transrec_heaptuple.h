#ifndef XK_PG_PARSER_TRANS_TRANSREC_HEAPTUPLE_H
#define XK_PG_PARSER_TRANS_TRANSREC_HEAPTUPLE_H

#define xk_pg_parser_InvalidBlockNumber     ((uint32_t) 0xFFFFFFFF)
#define xk_pg_parser_InvalidOffsetNumber    ((uint16_t) 0)

/* heap的一些标志位 begin*/
#define XK_PG_PARSER_HEAP_NATTS_MASK            0x07FF    /* 11 bits for number of attributes */
/* bits 0x1800 are available */
#define XK_PG_PARSER_HEAP_KEYS_UPDATED          0x2000    /* tuple was updated and key cols
                                                           * modified, or tuple deleted */
#define XK_PG_PARSER_HEAP_HOT_UPDATED           0x4000    /* tuple was HOT-updated */
#define XK_PG_PARSER_HEAP_ONLY_TUPLE            0x8000    /* this is heap-only tuple */
#define XK_PG_PARSER_HEAP2_XACT_MASK            0xE000    /* visibility-related bits */
#define XK_PG_PARSER_HEAP_TUPLE_HAS_MATCH    XK_PG_PARSER_HEAP_ONLY_TUPLE /* tuple has a join match */

/*
 * information stored in t_infomask:
 */
#define XK_PG_PARSER_HEAP_HASNULL                0x0001    /* has null attribute(s) */
#define XK_PG_PARSER_HEAP_HASVARWIDTH            0x0002    /* has variable-width attribute(s) */
#define XK_PG_PARSER_HEAP_HASEXTERNAL            0x0004    /* has external stored attribute(s) */
#if XK_PG_VERSION_NUM >= 120000
#define XK_PG_PARSER_HEAP_HASOID_OLD             0x0008    /* has an object-id field */
#else
#define XK_PG_PARSER_HEAP_HASOID                 0x0008    /* has an object-id field */
#endif
#define XK_PG_PARSER_HEAP_XMAX_KEYSHR_LOCK       0x0010    /* xmax is a key-shared locker */
#define XK_PG_PARSER_HEAP_COMBOCID               0x0020    /* t_cid is a combo cid */
#define XK_PG_PARSER_HEAP_XMAX_EXCL_LOCK         0x0040    /* xmax is exclusive locker */
#define XK_PG_PARSER_HEAP_XMAX_LOCK_ONLY         0x0080    /* xmax, if valid, is only a locker */


/* heap的一些标志位 end*/
typedef struct xk_pg_parser_xl_heap_header
{
    uint16_t        t_infomask2;
    uint16_t        t_infomask;
    uint8_t         t_hoff;
} xk_pg_parser_xl_heap_header;

#define xk_pg_parser_SizeOfHeapHeader (offsetof(xk_pg_parser_xl_heap_header, t_hoff) \
                                     + sizeof(uint8_t))

typedef struct xk_pg_parser_HGDB_xl_heap_header
{
    uint16_t        t_infomask2;
    uint16_t        t_infomask;
    int16_t         t_seclabel_len;
    uint8_t         t_hoff;
} xk_pg_parser_HGDB_xl_heap_header;

#define xk_pg_parser_HGDB_SizeOfHeapHeader (offsetof(xk_pg_parser_HGDB_xl_heap_header, t_hoff) \
                                     + sizeof(uint8_t))

typedef struct xk_pg_parser_HeapTupleFields
{
    uint32_t t_xmin;        /* inserting xact ID */
    uint32_t t_xmax;        /* deleting or locking xact ID */

    union
    {
        uint32_t    t_cid;        /* inserting or deleting command ID, or both */
        uint32_t    t_xvac;    /* old-style VACUUM FULL xact ID */
    }               t_field3;
} xk_pg_parser_HeapTupleFields;

typedef struct xk_pg_parser_DatumTupleFields
{
    int32_t        datum_len_;        /* xk_pg_parser_varlena header (do not touch directly!) */

    int32_t        datum_typmod;    /* -1, or identifier of a record type */

    uint32_t       datum_typeid;    /* composite type OID, or RECORDOID */

} xk_pg_parser_DatumTupleFields;

struct xk_pg_parser_HeapTupleHeaderData
{
    union
    {
        xk_pg_parser_HeapTupleFields t_heap;
        xk_pg_parser_DatumTupleFields t_datum;
    }            t_choice;

    xk_pg_parser_ItemPointerData t_ctid;        /* current TID of this or newer tuple (or a
                                 * speculative insertion token) */

    /* Fields below here must match MinimalTupleData! */

#define FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK2 2
    uint16_t        t_infomask2;    /* number of attributes + various flags */

#define FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK 3
    uint16_t        t_infomask;        /* various flag bits, see below */

#define FIELDNO_HEAPTUPLEHEADERDATA_HOFF 4
    uint8_t        t_hoff;            /* sizeof header incl. bitmap, padding */

    /* ^ - 23 bytes - ^ */

#define FIELDNO_HEAPTUPLEHEADERDATA_BITS 5
    uint8_t        t_bits[FLEXIBLE_ARRAY_MEMBER];    /* bitmap of NULLs */

    /* MORE DATA FOLLOWS AT END OF STRUCT */
};

struct xk_pg_parser_HGDB_HeapTupleHeaderData
{
    union
    {
        xk_pg_parser_HeapTupleFields t_heap;
        xk_pg_parser_DatumTupleFields t_datum;
    }            t_choice;

    xk_pg_parser_ItemPointerData t_ctid;        /* current TID of this or newer tuple (or a
                                 * speculative insertion token) */

    /* Fields below here must match MinimalTupleData! */

#define FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK2 2
    uint16_t        t_infomask2;    /* number of attributes + various flags */

#define FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK 3
    uint16_t        t_infomask;        /* various flag bits, see below */

    /* support for highgo database */
    int16_t         t_seclabel_len;

#define FIELDNO_HEAPTUPLEHEADERDATA_HOFF 4
    uint8_t        t_hoff;            /* sizeof header incl. bitmap, padding */

    /* ^ - 23 bytes - ^ */

#define FIELDNO_HEAPTUPLEHEADERDATA_BITS 5
    uint8_t        t_bits[FLEXIBLE_ARRAY_MEMBER];    /* bitmap of NULLs */

    /* MORE DATA FOLLOWS AT END OF STRUCT */
};

typedef struct xk_pg_parser_HeapTupleHeaderData xk_pg_parser_HeapTupleHeaderData;

#define xk_pg_parser_SizeofHeapTupleHeader offsetof(xk_pg_parser_HeapTupleHeaderData, t_bits)

typedef xk_pg_parser_HeapTupleHeaderData *xk_pg_parser_HeapTupleHeader;

typedef struct xk_pg_parser_HGDB_HeapTupleHeaderData xk_pg_parser_HGDB_HeapTupleHeaderData;

#define xk_pg_parser_HGDB_SizeofHeapTupleHeader offsetof(xk_pg_parser_HGDB_HeapTupleHeaderData, t_bits)

typedef xk_pg_parser_HGDB_HeapTupleHeaderData *xk_pg_parser_HGDB_HeapTupleHeader;

typedef struct xk_pg_parser_HeapTupleData
{
    uint32_t          t_len;              /* length of *t_data */
    xk_pg_parser_ItemPointerData t_self;             /* SelfItemPointer */
    uint32_t             t_tableOid;         /* table the tuple came from */
#define FIELDNO_HEAPTUPLEDATA_DATA 3
    xk_pg_parser_HeapTupleHeader t_data;        /* -> tuple header and data */
} xk_pg_parser_HeapTupleData;

typedef xk_pg_parser_HeapTupleData *xk_pg_parser_HeapTuple;

typedef struct xk_pg_parser_HGDB_HeapTupleData
{
    uint32_t          t_len;              /* length of *t_data */
    xk_pg_parser_ItemPointerData t_self;             /* SelfItemPointer */
    uint32_t             t_tableOid;         /* table the tuple came from */
#define FIELDNO_HEAPTUPLEDATA_DATA 3
    xk_pg_parser_HGDB_HeapTupleHeader t_data;        /* -> tuple header and data */
} xk_pg_parser_HGDB_HeapTupleData;

typedef xk_pg_parser_HGDB_HeapTupleData *xk_pg_parser_HGDB_HeapTuple;

typedef struct xk_pg_parser_slist_node xk_pg_parser_slist_node;
struct xk_pg_parser_slist_node
{
    xk_pg_parser_slist_node *next;
};

typedef struct xk_pg_parser_ReorderBufferTupleBuf
{
    /* position in preallocated list */
    xk_pg_parser_slist_node    node;

    /* tuple header, the interesting bit for users of logical decoding */
    xk_pg_parser_HeapTupleData tuple;

    /* pre-allocated size of tuple buffer, different from tuple size */
    size_t        alloc_tuple_size;

    /* actual tuple data follows */
} xk_pg_parser_ReorderBufferTupleBuf;

typedef struct xk_pg_parser_HGDB_ReorderBufferTupleBuf
{
    /* position in preallocated list */
    xk_pg_parser_slist_node    node;

    /* tuple header, the interesting bit for users of logical decoding */
    xk_pg_parser_HGDB_HeapTupleData tuple;

    /* pre-allocated size of tuple buffer, different from tuple size */
    size_t        alloc_tuple_size;

    /* actual tuple data follows */
} xk_pg_parser_HGDB_ReorderBufferTupleBuf;

/* tupledesc begin */
typedef struct xk_pg_parser_AttrMissing
{
    bool        am_present;        /* true if non-NULL missing value exists */
    uint64_t    am_value;          /* value when attribute is missing */
} xk_pg_parser_AttrMissing;

typedef struct xk_pg_parser_AttrDefault
{
    int16_t     adnum;
    char       *adbin;            /* nodeToString representation of expr */
} xk_pg_parser_AttrDefault;

typedef struct xk_pg_parser_ConstrCheck
{
    char       *ccname;
    char       *ccbin;            /* nodeToString representation of expr */
    bool        ccvalid;
    bool        ccnoinherit;    /* this is a non-inheritable constraint */
} xk_pg_parser_ConstrCheck;


typedef struct xk_pg_parser_TupleConstr
{
    xk_pg_parser_AttrDefault *defval;        /* array */
    xk_pg_parser_ConstrCheck *check;            /* array */
#if XK_PG_VERSION_NUM >= 110000
    struct xk_pg_parser_AttrMissing *missing;    /* missing attributes values, NULL if none */
#endif
    uint16_t        num_defval;
    uint16_t        num_check;
    bool        has_not_null;
#if XK_PG_VERSION_NUM >= 120000
    bool        has_generated_stored;
#endif
} xk_pg_parser_TupleConstr;

#if XK_PG_VERSION_NUM >= 120000
typedef struct xk_pg_parser_TupleDescData
{
    int32_t             natts;              /* number of attributes in the tuple */
    uint32_t            tdtypeid;           /* composite type ID for tuple type */
    int32_t             tdtypmod;           /* typmod for tuple type */
    int32_t             tdrefcount;         /* reference count, or -1 if not counting */
    xk_pg_parser_TupleConstr        *constr;             /* constraints, or NULL if none */
    /* attrs[N] is the description of Attribute Number N+1 */
    xk_pg_parser_sysdict_pgattributes attrs[FLEXIBLE_ARRAY_MEMBER];
}            xk_pg_parser_TupleDescData;
typedef struct xk_pg_parser_TupleDescData *xk_pg_parser_TupleDesc;
#else
typedef struct xk_pg_parser_TupleDescData
{
    int32_t            natts;            /* number of attributes in the tuple */
    uint32_t            tdtypeid;        /* composite type ID for tuple type */
    int32_t        tdtypmod;        /* typmod for tuple type */
    bool        tdhasoid;        /* tuple has oid attribute in its header */
    int32_t            tdrefcount;        /* reference count, or -1 if not counting */
    xk_pg_parser_TupleConstr *constr;        /* constraints, or NULL if none */
    /* attrs[N] is the description of Attribute Number N+1 */
    FormData_pg_attribute *attrs;
}           xk_pg_parser_TupleDescData;
typedef struct xk_pg_parser_TupleDescData *xk_pg_parser_TupleDesc;
#endif

#define xk_pg_parser_TupleDescAttr(tupdesc, i) (&(tupdesc)->attrs[(i)])

/* tupledesc end */

#define XK_PG_PARSER_HEAPTUPLESIZE    XK_PG_PARSER_MAXALIGN(sizeof(xk_pg_parser_HeapTupleData))
#define XK_PG_PARSER_HGDB_HEAPTUPLESIZE    XK_PG_PARSER_MAXALIGN(sizeof(xk_pg_parser_HGDB_HeapTupleData))

#define xk_pg_parser_ReorderBufferTupleBufData(p) \
    ((xk_pg_parser_HeapTupleHeader) XK_PG_PARSER_MAXALIGN(((char *) p) \
    + sizeof(xk_pg_parser_ReorderBufferTupleBuf)))

#define xk_pg_parser_HGDB_ReorderBufferTupleBufData(p) \
    ((xk_pg_parser_HGDB_HeapTupleHeader) XK_PG_PARSER_MAXALIGN(((char *) p) \
    + sizeof(xk_pg_parser_HGDB_ReorderBufferTupleBuf)))

/*
 * Accessor macros to be used with xk_pg_parser_HeapTuple pointers.
 */
#define XK_PG_PARSER_HeapTupleIsValid(tuple) ((const void*)(tuple) != NULL)

#define xk_pg_parser_BlockIdSet(blockId, blockNumber) \
( \
    xk_pg_parser_AssertMacro(((const void*)(blockId) != NULL)), \
    (blockId)->bi_hi = (blockNumber) >> 16, \
    (blockId)->bi_lo = (blockNumber) & 0xffff \
)

#define xk_pg_parser_HeapTupleHeaderSetXmin(tup, xid) \
( \
    (tup)->t_choice.t_heap.t_xmin = (xid) \
)
#define xk_pg_parser_HeapTupleHeaderSetXmax(tup, xid) \
( \
    (tup)->t_choice.t_heap.t_xmax = (xid) \
)

#define xk_pg_parser_FirstCommandId ((uint32_t) 0)
/* SetCmin is reasonably simple since we never need a combo CID */
#define xk_pg_parser_HeapTupleHeaderSetCmin(tup, cid) \
do { \
    (tup)->t_choice.t_heap.t_field3.t_cid = (cid); \
    (tup)->t_infomask &= ~XK_PG_PARSER_HEAP_COMBOCID; \
} while (0)

/* tuple中取出数据 begin */

#define xk_pg_parser_fetch_att(T,attbyval,attlen) \
( \
    (attbyval) ? \
    ( \
        (attlen) == (int32_t) sizeof(xk_pg_parser_Datum) ? \
            *((xk_pg_parser_Datum *)(T)) \
        : \
      ( \
        (attlen) == (int32_t) sizeof(int32_t) ? \
            xk_pg_parser_Int32GetDatum(*((int32_t *)(T))) \
        : \
        ( \
            (attlen) == (int32_t) sizeof(int16_t) ? \
                xk_pg_parser_Int16GetDatum(*((int16_t *)(T))) \
            : \
            ( \
                xk_pg_parser_AssertMacro((attlen) == 1), \
                xk_pg_parser_CharGetDatum(*((char *)(T))) \
            ) \
        ) \
      ) \
    ) \
    : \
    xk_pg_parser_PointerGetDatum((char *) (T)) \
)

#define xk_pg_parser_fetchatt(A,T) xk_pg_parser_fetch_att(T, (A)->attbyval, (A)->attlen)

/* Accessor for the i'th attribute of tupdesc. */
#define xk_pg_parser_TupleDescAttr(tupdesc, i) (&(tupdesc)->attrs[(i)])

#define xk_pg_parser_att_align_nominal(cur_offset, attalign) \
( \
    ((attalign) == 'i') ? XK_PG_PARSER_INTALIGN(cur_offset) : \
     (((attalign) == 'c') ? (uintptr_t) (cur_offset) : \
      (((attalign) == 'd') ? XK_PG_PARSER_DOUBLEALIGN(cur_offset) : \
       ( \
            xk_pg_parser_AssertMacro((attalign) == 's'), \
            XK_PG_PARSER_SHORTALIGN(cur_offset) \
       ))) \
)

#define XK_PG_PARSER_VARATT_NOT_PAD_BYTE(PTR) \
    (*((uint8_t *) (PTR)) != 0)

#define xk_pg_parser_att_align_pointer(cur_offset, attalign, attlen, attptr) \
( \
    ((attlen) == -1 && XK_PG_PARSER_VARATT_NOT_PAD_BYTE(attptr)) ? \
    (uintptr_t) (cur_offset) : \
    xk_pg_parser_att_align_nominal(cur_offset, attalign) \
)

#define xk_pg_parser_att_addlength_pointer(cur_offset, attlen, attptr) \
( \
    ((attlen) > 0) ? \
    ( \
        (cur_offset) + (attlen) \
    ) \
    : ((attlen == -1) ? \
    ( \
        (cur_offset) + (int32_t)XK_PG_PARSER_VARSIZE_ANY(attptr) \
    ) \
    : \
    ( \
        xk_pg_parser_AssertMacro((attlen) == -2), \
        (cur_offset) + (int32_t)(strlen((char *) (attptr)) + 1) \
    )) \
)

#define xk_pg_parser_att_isnull(ATT, BITS) (!((BITS)[(ATT) >> 3] & (1 << ((ATT) & 0x07))))

#define xk_pg_parser_HeapTupleHasNulls(tuple) \
        (((tuple)->t_data->t_infomask & XK_PG_PARSER_HEAP_HASNULL) != 0)
#define xk_pg_parser_HeapTupleNoNulls(tuple) \
        (!((tuple)->t_data->t_infomask & XK_PG_PARSER_HEAP_HASNULL))
#define xk_pg_parser_HeapTupleHasVarWidth(tuple) \
        (((tuple)->t_data->t_infomask & XK_PG_PARSER_HEAP_HASVARWIDTH) != 0)

#define xk_pg_parser_fastgetattr(tup, attnum, tupleDesc, isnull, dbtype, dbversion)                     \
(                                                                       \
    xk_pg_parser_AssertMacro((attnum) > 0),                             \
    (*(isnull) = false),                                                \
    xk_pg_parser_HeapTupleNoNulls(tup) ?                                     \
    (                                    \
        xk_pg_parser_TupleDescAttr((tupleDesc), (attnum)-1)->attcacheoff >= 0 ?    \
        (                                \
            (check_special_hgdb_version((dbtype), (dbversion))) ? \
            (                           \
                xk_pg_parser_fetchatt(xk_pg_parser_TupleDescAttr((tupleDesc), (attnum)-1),    \
                    (char *) ((xk_pg_parser_HGDB_HeapTuple)(tup))->t_data + \
                    ((xk_pg_parser_HGDB_HeapTuple)(tup))->t_data->t_hoff +\
                    xk_pg_parser_TupleDescAttr((tupleDesc), (attnum)-1)->attcacheoff)\
            )                            \
            :                            \
            ( \
                xk_pg_parser_fetchatt(xk_pg_parser_TupleDescAttr((tupleDesc), (attnum)-1),    \
                    (char *) (tup)->t_data + (tup)->t_data->t_hoff +\
                    xk_pg_parser_TupleDescAttr((tupleDesc), (attnum)-1)->attcacheoff)\
            ) \
        )                                \
        :                                \
            xk_pg_parser_nocachegetattr((tup), (attnum), (tupleDesc), (dbtype), (dbversion))        \
    )                                    \
    :                                    \
    (                                    \
        ( \
            (check_special_hgdb_version((dbtype), (dbversion))) ? \
            ( \
                xk_pg_parser_att_isnull((attnum)-1, ((xk_pg_parser_HGDB_HeapTuple)(tup))->t_data->t_bits) ?            \
                (                                \
                    (*(isnull) = true),                    \
                    (xk_pg_parser_Datum)NULL                        \
                )                                \
                :                                \
                (                                \
                    xk_pg_parser_nocachegetattr((tup), (attnum), (tupleDesc), (dbtype), (dbversion))        \
                )                                \
            ) \
            : \
            ( \
                xk_pg_parser_att_isnull((attnum)-1, (tup)->t_data->t_bits) ?            \
                (                                \
                    (*(isnull) = true),                    \
                    (xk_pg_parser_Datum)NULL                        \
                )                                \
                :                                \
                (                                \
                    xk_pg_parser_nocachegetattr((tup), (attnum), (tupleDesc), (dbtype), (dbversion))        \
                )                                \
            ) \
        ) \
    )                                    \
)

#define xk_pg_parser_HeapTupleHeaderGetNatts(tup) \
    ((tup)->t_infomask2 & XK_PG_PARSER_HEAP_NATTS_MASK)

#if XK_PG_VERSION_NUM >= 110000
#define xk_pg_parser_heap_getattr(tup, attnum, tupleDesc, isnull, ismissing, dbtype, dbversion) \
    ( \
        ((attnum) > 0) ? \
        ( \
            ((attnum) > (int32_t) xk_pg_parser_HeapTupleHeaderGetNatts((tup)->t_data)) ? \
                xk_pg_parser_getmissingattr((tupleDesc), (attnum), (isnull), (ismissing)) \
            : \
                xk_pg_parser_fastgetattr((tup), (attnum), (tupleDesc), (isnull), (dbtype), (dbversion)) \
        ) \
        : \
            xk_pg_parser_heap_getsysattr((tup), (attnum), (tupleDesc), (isnull), (dbtype), (dbversion)) \
    )
#else
#define xk_pg_parser_heap_getattr(tup, attnum, tupleDesc, isnull) \
    ( \
        ((attnum) > 0) ? \
        ( \
            ((attnum) > (int32_t) xk_pg_parser_HeapTupleHeaderGetNatts((tup)->t_data)) ? \
            ( \
                (*(isnull) = true), \
                (xk_pg_parser_Datum)NULL \
            ) \
            : \
                xk_pg_parser_fastgetattr((tup), (attnum), (tupleDesc), (isnull)) \
        ) \
        : \
            xk_pg_parser_heap_getsysattr((tup), (attnum), (tupleDesc), (isnull)) \
    )
#endif

#define xk_pg_parser_HeapTupleHeaderGetRawXmin(tup) \
( \
    (tup)->t_choice.t_heap.t_xmin \
)

#define xk_pg_parser_HeapTupleHeaderGetRawXmax(tup) \
( \
    (tup)->t_choice.t_heap.t_xmax \
)

#define xk_pg_parser_HeapTupleHeaderGetRawCommandId(tup) \
( \
    (tup)->t_choice.t_heap.t_field3.t_cid \
)

#define XK_PG_PARSER_MINIMAL_TUPLE_OFFSET \
    ((offsetof(xk_pg_parser_HeapTupleHeaderData, t_infomask2) - sizeof(uint32_t)) / XK_PG_PARSER_MAXIMUM_ALIGNOF * XK_PG_PARSER_MAXIMUM_ALIGNOF)
#define XK_PG_PARSER_MINIMAL_TUPLE_PADDING \
    ((offsetof(xk_pg_parser_HeapTupleHeaderData, t_infomask2) - sizeof(uint32_t)) % XK_PG_PARSER_MAXIMUM_ALIGNOF)
#define XK_PG_PARSER_MINIMAL_TUPLE_DATA_OFFSET \
    offsetof(xk_pg_parser_MinimalTupleData, t_infomask2)

typedef struct xk_pg_parser_MinimalTupleData
{
    uint32_t        t_len;            /* actual length of minimal tuple */

    char        mt_padding[XK_PG_PARSER_MINIMAL_TUPLE_PADDING];

    /* Fields below here must match HeapTupleHeaderData! */

    uint16_t        t_infomask2;    /* number of attributes + various flags */

    uint16_t        t_infomask;        /* various flag bits, see below */

    uint8_t        t_hoff;            /* sizeof header incl. bitmap, padding */

    /* ^ - 23 bytes - ^ */

    uint8_t        t_bits[FLEXIBLE_ARRAY_MEMBER];    /* bitmap of NULLs */

    /* MORE DATA FOLLOWS AT END OF STRUCT */
}xk_pg_parser_MinimalTupleData;

typedef xk_pg_parser_MinimalTupleData *xk_pg_parser_MinimalTuple;

/* tuple中取出数据 end */
/* 函数声明 */
extern xk_pg_parser_ReorderBufferTupleBuf * xk_pg_parser_heaptuple_get_tuple_space(size_t tuple_len,
                                                                                   int16_t dbtype,
                                                                                   char *dbversion);
extern void xk_pg_parser_reassemble_tuple_from_heap_tuple_header(
                                             void *hth,
                                             size_t len,
                                             xk_pg_parser_ReorderBufferTupleBuf *tuple,
                                             int16_t dbtype,
                                             char *dbversion);

extern void xk_pg_parser_re_tuple_from_wal_data(char *data,
                                             size_t len,
                                             xk_pg_parser_ReorderBufferTupleBuf *tuple,
                                             uint32_t xmin,
                                             uint32_t xmax);

extern xk_pg_parser_TupleDesc xk_pg_parser_get_desc(xk_pg_parser_sysdict_tableInfo* tbinfo);
extern xk_pg_parser_Datum xk_pg_parser_nocachegetattr(xk_pg_parser_HeapTuple tuple,
                                  int32_t attnum,
                                  xk_pg_parser_TupleDesc tupleDesc,
                                  int16_t dbtype,
                                  char *dbversion);
extern xk_pg_parser_Datum xk_pg_parser_heap_getsysattr(xk_pg_parser_HeapTuple tup,
                                   int32_t attnum,
                                   xk_pg_parser_TupleDesc tupleDesc,
                                   bool *isnull,
                                   int16_t dbtype,
                                   char *dbversion);
extern void xk_pg_parser_DecodeXLogTuple(char *data,
                                         size_t len,
                                         xk_pg_parser_ReorderBufferTupleBuf *tuple,
                                         int16_t dbtype,
                                         char *dbversion);
extern void xk_pg_parser_reassemble_tuple_from_wal_data(char *data,
                                                        size_t len,
                                                        xk_pg_parser_ReorderBufferTupleBuf *tuple,
                                                        uint32_t xmin,
                                                        uint32_t xmax,
                                                        int16_t dbtype,
                                                        char *dbversion);
extern void xk_pg_parser_heap_deform_tuple(xk_pg_parser_HeapTuple tuple,
                                           xk_pg_parser_TupleDesc tupleDesc,
                                           xk_pg_parser_Datum *values,
                                           bool *isnull);

extern void xk_pg_parser_heap_deform_tuple_HGDB(xk_pg_parser_HGDB_HeapTuple tuple,
                                           xk_pg_parser_TupleDesc tupleDesc,
                                           xk_pg_parser_Datum *values,
                                           bool *isnull);

extern xk_pg_parser_ReorderBufferTupleBuf *xk_pg_parser_assemble_tuple(int32_t dbtype,
                                                                char* dbversion,
                                                                uint32_t pagesize,
                                                                char* page,
                                                                uint16_t offnum);

#if XK_PG_VERSION_NUM >= 110000
extern xk_pg_parser_Datum xk_pg_parser_getmissingattr(xk_pg_parser_TupleDesc tupleDesc,
                                  int32_t attnum,
                                  bool *isnull,
                                  bool *ismissing);
#endif
#endif
