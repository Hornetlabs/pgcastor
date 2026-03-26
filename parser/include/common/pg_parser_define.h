#ifndef PG_PARSER_DEFINE_H
#define PG_PARSER_DEFINE_H

#ifndef true
#define true 1
#endif

#ifndef false
#define false 0
#endif

#ifndef FLEXIBLE_ARRAY_MEMBER
#define FLEXIBLE_ARRAY_MEMBER
#endif

#ifndef HAVE_INT8
typedef signed char  int8;  /* == 8 bits */
typedef signed short int16; /* == 16 bits */
typedef signed int   int32; /* == 32 bits */
#endif                      /* not HAVE_INT8 */

#ifndef HAVE_UINT8
typedef unsigned char  uint8;  /* == 8 bits */
typedef unsigned short uint16; /* == 16 bits */
typedef unsigned int   uint32; /* == 32 bits */
#endif                         /* not HAVE_UINT8 */

typedef uint8  bits8;  /* >= 8 bits */
typedef uint16 bits16; /* >= 16 bits */
typedef uint32 bits32; /* >= 32 bits */

#ifndef HAVE_INT64
typedef long int int64;
#endif
#ifndef HAVE_UINT64
typedef unsigned long int uint64;
#endif

/* make gnu happy */
#define PG_PARSER_UNUSED(x)      (void)(x)

#define pg_parser_AssertMacro(p) ((void)true)

#define pg_parser__restrict      __restrict

/* Byte alignment related begin */
#define PG_PARSER_MAXIMUM_ALIGNOF 8
#define PG_PARSER_ALIGNOF_INT     4
#define PG_PARSER_ALIGNOF_SHORT   2
#define PG_PARSER_ALIGNOF_DOUBLE  8

#define PG_PARSER_TYPEALIGN(ALIGNVAL, LEN) \
    (((uintptr_t)(LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t)((ALIGNVAL) - 1)))

#define PG_PARSER_DOUBLEALIGN(LEN) PG_PARSER_TYPEALIGN(PG_PARSER_ALIGNOF_DOUBLE, (LEN))
#define PG_PARSER_INTALIGN(LEN)    PG_PARSER_TYPEALIGN(PG_PARSER_ALIGNOF_INT, (LEN))
#define PG_PARSER_SHORTALIGN(LEN)  PG_PARSER_TYPEALIGN(PG_PARSER_ALIGNOF_SHORT, (LEN))
#define PG_PARSER_MAXALIGN(LEN)    PG_PARSER_TYPEALIGN(PG_PARSER_MAXIMUM_ALIGNOF, (LEN))

#define PG_PARSER_Max(x, y)        ((x) > (y) ? (x) : (y))

#define PG_PARSER_INT64CONST(x)    (x##L)
#define PG_PARSER_UINT64CONST(x)   (x##UL)

#define PG_PARSER_INT32_MIN        (-0x7FFFFFFF - 1)
#define PG_PARSER_INT64_MIN        (-PG_PARSER_INT64CONST(0x7FFFFFFFFFFFFFFF) - 1)
#define PG_PARSER_INT64_MAX        PG_PARSER_INT64CONST(0x7FFFFFFFFFFFFFFF)
/* Byte alignment related end */

/* Variable-length byte related begin */
typedef struct
{
    uint8_t va_header;
    char    va_data[FLEXIBLE_ARRAY_MEMBER]; /* Data begins here */
} pg_parser_varattrib_1b;

typedef struct
{
    uint8_t va_header;                      /* Always 0x80 or 0x01 */
    uint8_t va_tag;                         /* Type of datum */
    char    va_data[FLEXIBLE_ARRAY_MEMBER]; /* Type-specific data */
} pg_parser_varattrib_1b_e;

struct pg_parser_varlena
{
    char vl_len_[4];                    /* Do not touch this field directly! */
    char vl_dat[FLEXIBLE_ARRAY_MEMBER]; /* Data content is here */
};

typedef union
{
    struct /* Normal pg_parser_varlena (4-byte length) */
    {
        uint32_t va_header;
        char     va_data[FLEXIBLE_ARRAY_MEMBER];
    } va_4byte;
    struct /* Compressed-in-line format */
    {
        uint32_t va_header;
        uint32_t va_rawsize;                     /* Original data size (excludes header) */
        char     va_data[FLEXIBLE_ARRAY_MEMBER]; /* Compressed data */
    } va_compressed;
} pg_parser_varattrib_4b;

typedef union
{
    struct /* Normal varlena (4-byte length) */
    {
        uint32_t va_header;
        char     va_data[FLEXIBLE_ARRAY_MEMBER];
    } va_4byte;
    struct /* Compressed-in-line format */
    {
        uint32_t va_header;
        uint32_t va_tcinfo;                      /* Original data size (excludes header) and
                                                  * compression method; see va_extinfo */
        char     va_data[FLEXIBLE_ARRAY_MEMBER]; /* Compressed data */
    } va_compressed;
} pg_parser_pg14_varattrib_4b;

typedef struct pg_parser_varatt_external
{
    int32_t  va_rawsize;    /* Original data size (includes header) */
    int32_t  va_extsize;    /* External saved size (doesn't) */
    uint32_t va_valueid;    /* Unique ID of value within TOAST table */
    uint32_t va_toastrelid; /* RelID of TOAST table containing it */
} pg_parser_varatt_external;

typedef enum pg_parser_vartag_external
{
    PG_PARSER_VARTAG_INDIRECT = 1,
    PG_PARSER_VARTAG_EXPANDED_RO = 2,
    PG_PARSER_VARTAG_EXPANDED_RW = 3,
    PG_PARSER_VARTAG_ONDISK = 18
} pg_parser_vartag_external;

typedef struct pg_parser_varatt_indirect
{
    struct pg_parser_varlena* pointer; /* Pointer to in-memory pg_parser_varlena */
} pg_parser_varatt_indirect;

typedef struct pg_parser_ExpandedObjectHeader pg_parser_ExpandedObjectHeader;

typedef struct pg_parser_varatt_expanded
{
    pg_parser_ExpandedObjectHeader* eohptr;
} pg_parser_varatt_expanded;

#define PG_PARSER_VARTAG_IS_EXPANDED(tag) (((tag) & ~1) == PG_PARSER_VARTAG_EXPANDED_RO)

#define TrapMacro(condition, errorType)   (true)

#define PG_PARSER_VARTAG_SIZE(tag)                                           \
    ((tag) == PG_PARSER_VARTAG_INDIRECT  ? sizeof(pg_parser_varatt_indirect) \
     : PG_PARSER_VARTAG_IS_EXPANDED(tag) ? sizeof(pg_parser_varatt_expanded) \
     : (tag) == PG_PARSER_VARTAG_ONDISK  ? sizeof(pg_parser_varatt_external) \
                                         : TrapMacro(true, "unrecognized TOAST vartag"))

#define PG_PARSER_VARDATA_1B_E(PTR)   (((pg_parser_varattrib_1b_e*)(PTR))->va_data)
#define PG_PARSER_VARTAG_1B_E(PTR)    (((pg_parser_varattrib_1b_e*)(PTR))->va_tag)

#define PG_PARSER_VARATT_IS_1B_E(PTR) ((((pg_parser_varattrib_1b*)(PTR))->va_header) == 0x01)
#define PG_PARSER_VARATT_IS_1B(PTR)   ((((pg_parser_varattrib_1b*)(PTR))->va_header & 0x01) == 0x01)
#define PG_PARSER_VARATT_IS_4B_U(PTR) ((((pg_parser_varattrib_1b*)(PTR))->va_header & 0x03) == 0x00)

#define PG_PARSER_VARSIZE_1B(PTR)     ((((pg_parser_varattrib_1b*)(PTR))->va_header >> 1) & 0x7F)
#define PG_PARSER_VARSIZE_4B(PTR) \
    ((((pg_parser_varattrib_4b*)(PTR))->va_4byte.va_header >> 2) & 0x3FFFFFFF)

#define PG_PARSER_VARATT_IS_4B_C(PTR)   ((((pg_parser_varattrib_1b*)(PTR))->va_header & 0x03) == 0x02)

#define PG_PARSER_VARHDRSZ_EXTERNAL     offsetof(pg_parser_varattrib_1b_e, va_data)

#define PG_PARSER_VARTAG_EXTERNAL(PTR)  PG_PARSER_VARTAG_1B_E(PTR)
#define PG_PARSER_VARSIZE_SHORT(PTR)    PG_PARSER_VARSIZE_1B(PTR)
#define PG_PARSER_VARDATA_EXTERNAL(PTR) PG_PARSER_VARDATA_1B_E(PTR)
#define PG_PARSER_VARSIZE_EXTERNAL(PTR) \
    (PG_PARSER_VARHDRSZ_EXTERNAL + PG_PARSER_VARTAG_SIZE(PG_PARSER_VARTAG_EXTERNAL(PTR)))

#define PG_PARSER_VARSIZE_ANY(PTR)         \
    (PG_PARSER_VARATT_IS_1B_E(PTR)         \
         ? PG_PARSER_VARSIZE_EXTERNAL(PTR) \
         : (PG_PARSER_VARATT_IS_1B(PTR) ? PG_PARSER_VARSIZE_1B(PTR) : PG_PARSER_VARSIZE_4B(PTR)))

#define PG_PARSER_VARHDRSZ       ((int32_t)sizeof(int32_t))

#define PG_PARSER_VARHDRSZ_SHORT offsetof(pg_parser_varattrib_1b, va_data)

#define PG_PARSER_VARSIZE_ANY_EXHDR(PTR)                                                       \
    (PG_PARSER_VARATT_IS_1B_E(PTR)                                                             \
         ? PG_PARSER_VARSIZE_EXTERNAL(PTR) - PG_PARSER_VARHDRSZ_EXTERNAL                       \
         : (PG_PARSER_VARATT_IS_1B(PTR) ? PG_PARSER_VARSIZE_1B(PTR) - PG_PARSER_VARHDRSZ_SHORT \
                                        : PG_PARSER_VARSIZE_4B(PTR) - PG_PARSER_VARHDRSZ))

#define PG_PARSER_VARDATA_1B(PTR) (((pg_parser_varattrib_1b*)(PTR))->va_data)
#define PG_PARSER_VARDATA_4B(PTR) (((pg_parser_varattrib_4b*)(PTR))->va_4byte.va_data)

#define PG_PARSER_VARDATA_ANY(PTR) \
    (PG_PARSER_VARATT_IS_1B(PTR) ? PG_PARSER_VARDATA_1B(PTR) : PG_PARSER_VARDATA_4B(PTR))

#define PG_PARSER_VARATT_IS_EXTERNAL(PTR)   PG_PARSER_VARATT_IS_1B_E(PTR)
#define PG_PARSER_VARATT_IS_EXTENDED(PTR)   (!PG_PARSER_VARATT_IS_4B_U(PTR))
#define PG_PARSER_VARATT_IS_COMPRESSED(PTR) PG_PARSER_VARATT_IS_4B_C(PTR)

#define PG_PARSER_VARATT_IS_EXTERNAL_ONDISK(PTR) \
    (PG_PARSER_VARATT_IS_EXTERNAL(PTR) && PG_PARSER_VARTAG_EXTERNAL(PTR) == PG_PARSER_VARTAG_ONDISK)

#define PG_PARSER_VARATT_IS_EXTERNAL_INDIRECT(PTR) \
    (PG_PARSER_VARATT_IS_EXTERNAL(PTR) &&          \
     PG_PARSER_VARTAG_EXTERNAL(PTR) == PG_PARSER_VARTAG_INDIRECT)

#define PG_PARSER_VARATT_IS_EXTERNAL_EXPANDED_RO(PTR) \
    (PG_PARSER_VARATT_IS_EXTERNAL(PTR) &&             \
     PG_PARSER_VARTAG_EXTERNAL(PTR) == PG_PARSER_VARTAG_EXPANDED_RO)

#define PG_PARSER_VARATT_IS_EXTERNAL_EXPANDED_RW(PTR) \
    (PG_PARSER_VARATT_IS_EXTERNAL(PTR) &&             \
     PG_PARSER_VARTAG_EXTERNAL(PTR) == PG_PARSER_VARTAG_EXPANDED_RW)

#define PG_PARSER_VARATT_IS_EXTERNAL_EXPANDED(PTR) \
    (PG_PARSER_VARATT_IS_EXTERNAL(PTR) &&          \
     PG_PARSER_VARTAG_IS_EXPANDED(PG_PARSER_VARTAG_EXTERNAL(PTR)))

#define PG_PARSER_SET_VARSIZE_4B(PTR, len) \
    (((pg_parser_varattrib_4b*)(PTR))->va_4byte.va_header = (((uint32_t)(len)) << 2))

#define PG_PARSER_SET_VARSIZE_4B_C(PTR, len) \
    (((pg_parser_varattrib_4b*)(PTR))->va_4byte.va_header = (((uint32_t)(len)) << 2) | 0x02)

#define PG_PARSER_SET_VARSIZE(PTR, len) PG_PARSER_SET_VARSIZE_4B(PTR, len)

#define PG_PARSER_VARATT_IS_SHORT(PTR)  PG_PARSER_VARATT_IS_1B(PTR)

#define PG_PARSER_VARDATA(PTR)          PG_PARSER_VARDATA_4B(PTR)
#define PG_PARSER_VARSIZE(PTR)          PG_PARSER_VARSIZE_4B(PTR)
#define PG_PARSER_VARDATA_SHORT(PTR)    PG_PARSER_VARDATA_1B(PTR)

typedef struct pg_parser_toast_compress_header
{
    int32_t vl_len_; /* varlena header (do not touch directly!) */
    int32_t rawsize;
} pg_parser_toast_compress_header;

#define PG_PARSER_TOAST_COMPRESS_HDRSZ        ((int32_t)sizeof(pg_parser_toast_compress_header))
#define PG_PARSER_TOAST_COMPRESS_RAWSIZE(ptr) (((pg_parser_toast_compress_header*)(ptr))->rawsize)
#define PG_PARSER_TOAST_COMPRESS_RAWDATA(ptr) (((char*)(ptr)) + PG_PARSER_TOAST_COMPRESS_HDRSZ)

/* Compression related */
#define PG_PARSER_VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer) \
    ((toast_pointer)->m_extsize < (toast_pointer)->m_rawsize - PG_PARSER_VARHDRSZ)

#define PG_PARSER_VARATT_EXTERNAL_CHECK_COMPRESSED(extsize, rawsize) \
    (extsize < (rawsize - PG_PARSER_VARHDRSZ))

#define PG_PARSER_SET_VARSIZE_COMPRESSED(PTR, len) PG_PARSER_SET_VARSIZE_4B_C(PTR, len)

/* PG14 compression related */
typedef enum pg_parser_ToastCompressionId
{
    PG_PARSER_PG14_TOAST_PGLZ_COMPRESSION_ID = 0,
    PG_PARSER_PG14_TOAST_LZ4_COMPRESSION_ID = 1,
    PG_PARSER_PG14_TOAST_INVALID_COMPRESSION_ID = 2
} pg_parser_ToastCompressionId;
typedef struct pg_parser_pg14_toast_compress_header
{
    int32_t  vl_len_; /* varlena header (do not touch directly!) */
    uint32_t tcinfo;  /* 2 bits for compression method and 30 bits
                       * external size; see va_extinfo */
} pg_parser_pg14_toast_compress_header;
#define PG_PARSER_PG14_VARHDRSZ_COMPRESSED \
    offsetof(pg_parser_pg14_varattrib_4b, va_compressed.va_data)
#define PG_PARSER_PG14_VARLENA_EXTSIZE_BITS 30
#define PG_PARSER_PG14_VARLENA_EXTSIZE_MASK ((1U << PG_PARSER_PG14_VARLENA_EXTSIZE_BITS) - 1)
#define PG_PARSER_PG14_VARDATA_COMPRESSED_GET_EXTSIZE(PTR)            \
    (((pg_parser_pg14_varattrib_4b*)(PTR))->va_compressed.va_tcinfo & \
     PG_PARSER_PG14_VARLENA_EXTSIZE_MASK)
#define PG_PARSER_PG14_VARDATA_COMPRESSED_GET_COMPRESS_METHOD(PTR)     \
    (((pg_parser_pg14_varattrib_4b*)(PTR))->va_compressed.va_tcinfo >> \
     PG_PARSER_PG14_VARLENA_EXTSIZE_BITS)
#define PG_PARSER_PG14_TOAST_COMPRESS_EXTSIZE(ptr) \
    (((pg_parser_pg14_toast_compress_header*)(ptr))->tcinfo & PG_PARSER_PG14_VARLENA_EXTSIZE_MASK)
#define PG_PARSER_PG14_TOAST_COMPRESS_METHOD(ptr) \
    (((pg_parser_pg14_toast_compress_header*)(ptr))->tcinfo >> PG_PARSER_PG14_VARLENA_EXTSIZE_BITS)
#define PG_PARSER_PG14_TOAST_COMPRESS_SET_SIZE_AND_COMPRESS_METHOD(ptr, len, cm_method) \
    do                                                                                  \
    {                                                                                   \
        ((pg_parser_pg14_toast_compress_header*)(ptr))->tcinfo =                        \
            (len) | ((uint32)(cm_method) << PG_PARSER_PG14_VARLENA_EXTSIZE_BITS);       \
    } while (0)

/* Variable-length byte related end */

/* System catalog related constants begin */
#define PG_PARSER_SYSDICT_PRIMARY_KEY_MAX 20
#define pg_parser_InvalidOid              0
#define pg_parser_InvalidTransactionId    0
/* end */

/* Datum related begin */
typedef unsigned long int   pg_parser_uintptr_t;
typedef pg_parser_uintptr_t pg_parser_Datum;
typedef char*               pg_parser_Pointer;
#define pg_parser_CharGetDatum(X)          ((pg_parser_Datum)(X))
#define pg_parser_Int16GetDatum(X)         ((pg_parser_Datum)(X))
#define pg_parser_Int32GetDatum(X)         ((pg_parser_Datum)(X))
#define pg_parser_PointerGetDatum(X)       ((pg_parser_Datum)(X))
#define pg_parser_TransactionIdGetDatum(X) ((pg_parser_Datum)(X))
#define pg_parser_CommandIdGetDatum(X)     ((pg_parser_Datum)(X))
#define pg_parser_ObjectIdGetDatum(X)      ((pg_parser_Datum)(X))
#define pg_parser_DatumGetPointer(X)       ((pg_parser_Pointer)(X))
#define pg_parser_DatumGetInt32(X)         ((int32_t)(X))
/* Datum related end */

/* Type definitions */
#define PG_PARSER_NAMEDATALEN 64

typedef struct pg_parser_nameData
{
    char data[PG_PARSER_NAMEDATALEN];
} pg_parser_NameData;
typedef pg_parser_NameData*      pg_parser_Name;

typedef struct pg_parser_varlena pg_parser_bytea;
typedef struct pg_parser_varlena pg_parser_text;
typedef struct pg_parser_varlena pg_parser_BpChar;
typedef struct pg_parser_varlena pg_parser_VarChar;

/* Type definitions */
typedef uint32_t                 pg_parser_TransactionId;
typedef uint64_t                 pg_parser_XLogRecPtr;
typedef uint8_t                  pg_parser_RmgrId;
typedef uint32_t                 pg_parser_crc32c;
typedef uint32_t                 pg_parser_BlockNumber;
typedef uint16_t                 pg_parser_RepOriginId;
typedef uint64_t                 pg_parser_XLogSegNo;
typedef uint32_t                 pg_parser_TimeLineID;
typedef uint16_t                 pg_parser_OffsetNumber;
typedef uint32_t                 pg_parser_CommandId;
#endif
