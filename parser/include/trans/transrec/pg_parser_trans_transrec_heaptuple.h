#ifndef PG_PARSER_TRANS_TRANSREC_HEAPTUPLE_H
#define PG_PARSER_TRANS_TRANSREC_HEAPTUPLE_H

#define pg_parser_InvalidBlockNumber  ((uint32_t)0xFFFFFFFF)
#define pg_parser_InvalidOffsetNumber ((uint16_t)0)

/* heap begin*/
#define PG_PARSER_HEAP_NATTS_MASK 0x07FF /* 11 bits for number of attributes */
/* bits 0x1800 are available */
#define PG_PARSER_HEAP_KEYS_UPDATED                                                                \
    0x2000                                                       /* tuple was updated and key cols \
                                                                  * modified, or tuple deleted */
#define PG_PARSER_HEAP_HOT_UPDATED     0x4000                    /* tuple was HOT-updated */
#define PG_PARSER_HEAP_ONLY_TUPLE      0x8000                    /* this is heap-only tuple */
#define PG_PARSER_HEAP2_XACT_MASK      0xE000                    /* visibility-related bits */
#define PG_PARSER_HEAP_TUPLE_HAS_MATCH PG_PARSER_HEAP_ONLY_TUPLE /* tuple has a join match */

/*
 * information stored in t_infomask:
 */
#define PG_PARSER_HEAP_HASNULL          0x0001 /* has null attribute(s) */
#define PG_PARSER_HEAP_HASVARWIDTH      0x0002 /* has variable-width attribute(s) */
#define PG_PARSER_HEAP_HASEXTERNAL      0x0004 /* has external stored attribute(s) */
#define PG_PARSER_HEAP_HASOID_OLD       0x0008 /* has an object-id field */
#define PG_PARSER_HEAP_XMAX_KEYSHR_LOCK 0x0010 /* xmax is a key-shared locker */
#define PG_PARSER_HEAP_COMBOCID         0x0020 /* t_cid is a combo cid */
#define PG_PARSER_HEAP_XMAX_EXCL_LOCK   0x0040 /* xmax is exclusive locker */
#define PG_PARSER_HEAP_XMAX_LOCK_ONLY   0x0080 /* xmax, if valid, is only a locker */

/* heap some flag bits end*/
typedef struct pg_parser_xl_heap_header
{
    uint16_t t_infomask2;
    uint16_t t_infomask;
    uint8_t  t_hoff;
} pg_parser_xl_heap_header;

#define pg_parser_SizeOfHeapHeader (offsetof(pg_parser_xl_heap_header, t_hoff) + sizeof(uint8_t))

typedef struct pg_parser_HeapTupleFields
{
    uint32_t t_xmin; /* inserting xact ID */
    uint32_t t_xmax; /* deleting or locking xact ID */

    union
    {
        uint32_t t_cid;  /* inserting or deleting command ID, or both */
        uint32_t t_xvac; /* old-style VACUUM FULL xact ID */
    } t_field3;
} pg_parser_HeapTupleFields;

typedef struct pg_parser_DatumTupleFields
{
    int32_t  datum_len_; /* pg_parser_varlena header (do not touch directly!) */

    int32_t  datum_typmod; /* -1, or identifier of a record type */

    uint32_t datum_typeid; /* composite type OID, or RECORDOID */

} pg_parser_DatumTupleFields;

struct pg_parser_HeapTupleHeaderData
{
    union
    {
        pg_parser_HeapTupleFields  t_heap;
        pg_parser_DatumTupleFields t_datum;
    } t_choice;

    pg_parser_ItemPointerData t_ctid; /* current TID of this or newer tuple (or a
                                       * speculative insertion token) */

    /* Fields below here must match MinimalTupleData! */

#define FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK2 2
    uint16_t t_infomask2; /* number of attributes + various flags */

#define FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK 3
    uint16_t t_infomask; /* various flag bits, see below */

#define FIELDNO_HEAPTUPLEHEADERDATA_HOFF 4
    uint8_t t_hoff; /* sizeof header incl. bitmap, padding */

    /* ^ - 23 bytes - ^ */

#define FIELDNO_HEAPTUPLEHEADERDATA_BITS 5
    uint8_t t_bits[FLEXIBLE_ARRAY_MEMBER]; /* bitmap of NULLs */

    /* MORE DATA FOLLOWS AT END OF STRUCT */
};

typedef struct pg_parser_HeapTupleHeaderData pg_parser_HeapTupleHeaderData;

#define pg_parser_SizeofHeapTupleHeader offsetof(pg_parser_HeapTupleHeaderData, t_bits)

typedef pg_parser_HeapTupleHeaderData* pg_parser_HeapTupleHeader;

typedef struct pg_parser_HeapTupleData
{
    uint32_t                  t_len;      /* length of *t_data */
    pg_parser_ItemPointerData t_self;     /* SelfItemPointer */
    uint32_t                  t_tableOid; /* table the tuple came from */
#define FIELDNO_HEAPTUPLEDATA_DATA 3
    pg_parser_HeapTupleHeader t_data; /* -> tuple header and data */
} pg_parser_HeapTupleData;

typedef pg_parser_HeapTupleData*    pg_parser_HeapTuple;

typedef struct pg_parser_slist_node pg_parser_slist_node;

struct pg_parser_slist_node
{
    pg_parser_slist_node* next;
};

typedef struct pg_parser_ReorderBufferTupleBuf
{
    /* position in preallocated list */
    pg_parser_slist_node    node;

    /* tuple header, the interesting bit for users of logical decoding */
    pg_parser_HeapTupleData tuple;

    /* pre-allocated size of tuple buffer, different from tuple size */
    size_t                  alloc_tuple_size;

    /* actual tuple data follows */
} pg_parser_ReorderBufferTupleBuf;

/* tupledesc begin */
typedef struct pg_parser_AttrMissing
{
    bool     am_present; /* true if non-NULL missing value exists */
    uint64_t am_value;   /* value when attribute is missing */
} pg_parser_AttrMissing;

typedef struct pg_parser_AttrDefault
{
    int16_t adnum;
    char*   adbin; /* nodeToString representation of expr */
} pg_parser_AttrDefault;

typedef struct pg_parser_ConstrCheck
{
    char* ccname;
    char* ccbin; /* nodeToString representation of expr */
    bool  ccvalid;
    bool  ccnoinherit; /* this is a non-inheritable constraint */
} pg_parser_ConstrCheck;

typedef struct pg_parser_TupleConstr
{
    pg_parser_AttrDefault*        defval;  /* array */
    pg_parser_ConstrCheck*        check;   /* array */
    struct pg_parser_AttrMissing* missing; /* missing attributes values, NULL if none */
    uint16_t                      num_defval;
    uint16_t                      num_check;
    bool                          has_not_null;
    bool                          has_generated_stored;
} pg_parser_TupleConstr;

typedef struct pg_parser_TupleDescData
{
    int32_t                        natts;      /* number of attributes in the tuple */
    uint32_t                       tdtypeid;   /* composite type ID for tuple type */
    int32_t                        tdtypmod;   /* typmod for tuple type */
    int32_t                        tdrefcount; /* reference count, or -1 if not counting */
    pg_parser_TupleConstr*         constr;     /* constraints, or NULL if none */
    /* attrs[N] is the description of Attribute Number N+1 */
    pg_parser_sysdict_pgattributes attrs[FLEXIBLE_ARRAY_MEMBER];
} pg_parser_TupleDescData;
typedef struct pg_parser_TupleDescData* pg_parser_TupleDesc;

#define pg_parser_TupleDescAttr(tupdesc, i) (&(tupdesc)->attrs[(i)])

/* tupledesc end */

#define PG_PARSER_HEAPTUPLESIZE PG_PARSER_MAXALIGN(sizeof(pg_parser_HeapTupleData))

#define pg_parser_ReorderBufferTupleBufData(p) \
    ((pg_parser_HeapTupleHeader)PG_PARSER_MAXALIGN(((char*)p) + sizeof(pg_parser_ReorderBufferTupleBuf)))

/*
 * Accessor macros to be used with pg_parser_HeapTuple pointers.
 */
#define PG_PARSER_HeapTupleIsValid(tuple) ((const void*)(tuple) != NULL)

#define pg_parser_BlockIdSet(blockId, blockNumber)            \
    (pg_parser_AssertMacro(((const void*)(blockId) != NULL)), \
     (blockId)->bi_hi = (blockNumber) >> 16,                  \
     (blockId)->bi_lo = (blockNumber) & 0xffff)

#define pg_parser_HeapTupleHeaderSetXmin(tup, xid) ((tup)->t_choice.t_heap.t_xmin = (xid))
#define pg_parser_HeapTupleHeaderSetXmax(tup, xid) ((tup)->t_choice.t_heap.t_xmax = (xid))

#define pg_parser_FirstCommandId                   ((uint32_t)0)
/* SetCmin is reasonably simple since we never need a combo CID */
#define pg_parser_HeapTupleHeaderSetCmin(tup, cid)     \
    do                                                 \
    {                                                  \
        (tup)->t_choice.t_heap.t_field3.t_cid = (cid); \
        (tup)->t_infomask &= ~PG_PARSER_HEAP_COMBOCID; \
    } while (0)

/* tuple begin */

#define pg_parser_fetch_att(T, attbyval, attlen)                                                                   \
    ((attbyval) ? ((attlen) == (int32_t)sizeof(pg_parser_Datum)                                                    \
                       ? *((pg_parser_Datum*)(T))                                                                  \
                       : ((attlen) == (int32_t)sizeof(int32_t)                                                     \
                              ? pg_parser_Int32GetDatum(*((int32_t*)(T)))                                          \
                              : ((attlen) == (int32_t)sizeof(int16_t) ? pg_parser_Int16GetDatum(*((int16_t*)(T)))  \
                                                                      : (pg_parser_AssertMacro((attlen) == 1),     \
                                                                         pg_parser_CharGetDatum(*((char*)(T))))))) \
                : pg_parser_PointerGetDatum((char*)(T)))

#define pg_parser_fetchatt(A, T) pg_parser_fetch_att(T, (A)->attbyval, (A)->attlen)

/* Accessor for the i'th attribute of tupdesc. */
#define pg_parser_TupleDescAttr(tupdesc, i) (&(tupdesc)->attrs[(i)])

#define pg_parser_att_align_nominal(cur_offset, attalign)                                                           \
    (((attalign) == 'i') ? PG_PARSER_INTALIGN(cur_offset)                                                           \
                         : (((attalign) == 'c') ? (uintptr_t)(cur_offset)                                           \
                                                : (((attalign) == 'd') ? PG_PARSER_DOUBLEALIGN(cur_offset)          \
                                                                       : (pg_parser_AssertMacro((attalign) == 's'), \
                                                                          PG_PARSER_SHORTALIGN(cur_offset)))))

#define PG_PARSER_VARATT_NOT_PAD_BYTE(PTR) (*((uint8_t*)(PTR)) != 0)

#define pg_parser_att_align_pointer(cur_offset, attalign, attlen, attptr)                \
    (((attlen) == -1 && PG_PARSER_VARATT_NOT_PAD_BYTE(attptr)) ? (uintptr_t)(cur_offset) \
                                                               : pg_parser_att_align_nominal(cur_offset, attalign))

#define pg_parser_att_addlength_pointer(cur_offset, attlen, attptr)                             \
    (((attlen) > 0) ? ((cur_offset) + (attlen))                                                 \
                    : ((attlen == -1) ? ((cur_offset) + (int32_t)PG_PARSER_VARSIZE_ANY(attptr)) \
                                      : (pg_parser_AssertMacro((attlen) == -2),                 \
                                         (cur_offset) + (int32_t)(strlen((char*)(attptr)) + 1))))

#define pg_parser_att_isnull(ATT, BITS)       (!((BITS)[(ATT) >> 3] & (1 << ((ATT) & 0x07))))

#define pg_parser_HeapTupleHasNulls(tuple)    (((tuple)->t_data->t_infomask & PG_PARSER_HEAP_HASNULL) != 0)
#define pg_parser_HeapTupleNoNulls(tuple)     (!((tuple)->t_data->t_infomask & PG_PARSER_HEAP_HASNULL))
#define pg_parser_HeapTupleHasVarWidth(tuple) (((tuple)->t_data->t_infomask & PG_PARSER_HEAP_HASVARWIDTH) != 0)

#define pg_parser_fastgetattr(tup, attnum, tupleDesc, isnull, dbtype, dbversion)                            \
    (pg_parser_AssertMacro((attnum) > 0),                                                                   \
     (*(isnull) = false),                                                                                   \
     pg_parser_HeapTupleNoNulls(tup)                                                                        \
         ? (pg_parser_TupleDescAttr((tupleDesc), (attnum) - 1)->attcacheoff >= 0                            \
                ? (pg_parser_fetchatt(pg_parser_TupleDescAttr((tupleDesc), (attnum) - 1),                   \
                                      (char*)(tup)->t_data + (tup)->t_data->t_hoff +                        \
                                          pg_parser_TupleDescAttr((tupleDesc), (attnum) - 1)->attcacheoff)) \
                : pg_parser_nocachegetattr((tup), (attnum), (tupleDesc), (dbtype), (dbversion)))            \
         : ((pg_parser_att_isnull((attnum) - 1, (tup)->t_data->t_bits)                                      \
                 ? ((*(isnull) = true), (pg_parser_Datum)NULL)                                              \
                 : (pg_parser_nocachegetattr((tup), (attnum), (tupleDesc), (dbtype), (dbversion))))))

#define pg_parser_HeapTupleHeaderGetNatts(tup) ((tup)->t_infomask2 & PG_PARSER_HEAP_NATTS_MASK)

#define pg_parser_heap_getattr(tup, attnum, tupleDesc, isnull, ismissing, dbtype, dbversion)                       \
    (((attnum) > 0) ? (((attnum) > (int32_t)pg_parser_HeapTupleHeaderGetNatts((tup)->t_data))                      \
                           ? pg_parser_getmissingattr((tupleDesc), (attnum), (isnull), (ismissing))                \
                           : pg_parser_fastgetattr((tup), (attnum), (tupleDesc), (isnull), (dbtype), (dbversion))) \
                    : pg_parser_heap_getsysattr((tup), (attnum), (tupleDesc), (isnull), (dbtype), (dbversion)))

#define pg_parser_HeapTupleHeaderGetRawXmin(tup)      ((tup)->t_choice.t_heap.t_xmin)

#define pg_parser_HeapTupleHeaderGetRawXmax(tup)      ((tup)->t_choice.t_heap.t_xmax)

#define pg_parser_HeapTupleHeaderGetRawCommandId(tup) ((tup)->t_choice.t_heap.t_field3.t_cid)

#define PG_PARSER_MINIMAL_TUPLE_OFFSET                                                                       \
    ((offsetof(pg_parser_HeapTupleHeaderData, t_infomask2) - sizeof(uint32_t)) / PG_PARSER_MAXIMUM_ALIGNOF * \
     PG_PARSER_MAXIMUM_ALIGNOF)
#define PG_PARSER_MINIMAL_TUPLE_PADDING \
    ((offsetof(pg_parser_HeapTupleHeaderData, t_infomask2) - sizeof(uint32_t)) % PG_PARSER_MAXIMUM_ALIGNOF)
#define PG_PARSER_MINIMAL_TUPLE_DATA_OFFSET offsetof(pg_parser_MinimalTupleData, t_infomask2)

typedef struct pg_parser_MinimalTupleData
{
    uint32_t t_len; /* actual length of minimal tuple */

    char     mt_padding[PG_PARSER_MINIMAL_TUPLE_PADDING];

    /* Fields below here must match HeapTupleHeaderData! */

    uint16_t t_infomask2; /* number of attributes + various flags */

    uint16_t t_infomask; /* various flag bits, see below */

    uint8_t  t_hoff; /* sizeof header incl. bitmap, padding */

    /* ^ - 23 bytes - ^ */

    uint8_t  t_bits[FLEXIBLE_ARRAY_MEMBER]; /* bitmap of NULLs */

    /* MORE DATA FOLLOWS AT END OF STRUCT */
} pg_parser_MinimalTupleData;

typedef pg_parser_MinimalTupleData* pg_parser_MinimalTuple;

/* tuple end */
/* function declaration */
extern pg_parser_ReorderBufferTupleBuf* pg_parser_heaptuple_get_tuple_space(size_t  tuple_len,
                                                                            int16_t dbtype,
                                                                            char*   dbversion);
extern void pg_parser_reassemble_tuple_from_heap_tuple_header(void*                            hth,
                                                              size_t                           len,
                                                              pg_parser_ReorderBufferTupleBuf* tuple,
                                                              int16_t                          dbtype,
                                                              char*                            dbversion);

extern void pg_parser_re_tuple_from_wal_data(char*                            data,
                                             size_t                           len,
                                             pg_parser_ReorderBufferTupleBuf* tuple,
                                             uint32_t                         xmin,
                                             uint32_t                         xmax);

extern pg_parser_TupleDesc pg_parser_get_desc(pg_parser_sysdict_tableInfo* tbinfo);
extern pg_parser_Datum pg_parser_nocachegetattr(pg_parser_HeapTuple tuple,
                                                int32_t             attnum,
                                                pg_parser_TupleDesc tupleDesc,
                                                int16_t             dbtype,
                                                char*               dbversion);
extern pg_parser_Datum pg_parser_heap_getsysattr(pg_parser_HeapTuple tup,
                                                 int32_t             attnum,
                                                 pg_parser_TupleDesc tupleDesc,
                                                 bool*               isnull,
                                                 int16_t             dbtype,
                                                 char*               dbversion);
extern void pg_parser_DecodeXLogTuple(char*                            data,
                                      size_t                           len,
                                      pg_parser_ReorderBufferTupleBuf* tuple,
                                      int16_t                          dbtype,
                                      char*                            dbversion);
extern void pg_parser_reassemble_tuple_from_wal_data(char*                            data,
                                                     size_t                           len,
                                                     pg_parser_ReorderBufferTupleBuf* tuple,
                                                     uint32_t                         xmin,
                                                     uint32_t                         xmax,
                                                     int16_t                          dbtype,
                                                     char*                            dbversion);
extern void pg_parser_heap_deform_tuple(pg_parser_HeapTuple tuple,
                                        pg_parser_TupleDesc tupleDesc,
                                        pg_parser_Datum*    values,
                                        bool*               isnull);

extern pg_parser_ReorderBufferTupleBuf* pg_parser_assemble_tuple(int32_t  dbtype,
                                                                 char*    dbversion,
                                                                 uint32_t pagesize,
                                                                 char*    page,
                                                                 uint16_t offnum);

extern pg_parser_Datum pg_parser_getmissingattr(pg_parser_TupleDesc tupleDesc,
                                                int32_t             attnum,
                                                bool*               isnull,
                                                bool*               ismissing);
#endif
