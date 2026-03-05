#ifndef XK_PG_PARSER_DEFINE_H
#define XK_PG_PARSER_DEFINE_H

#ifndef true
#define true          1
#endif

#ifndef false
#define false         0
#endif

#ifndef FLEXIBLE_ARRAY_MEMBER
#define FLEXIBLE_ARRAY_MEMBER
#endif

#ifndef HAVE_INT8
typedef signed char int8;		/* == 8 bits */
typedef signed short int16;		/* == 16 bits */
typedef signed int int32;		/* == 32 bits */
#endif							/* not HAVE_INT8 */

#ifndef HAVE_UINT8
typedef unsigned char uint8;	/* == 8 bits */
typedef unsigned short uint16;	/* == 16 bits */
typedef unsigned int uint32;	/* == 32 bits */
#endif							/* not HAVE_UINT8 */

typedef uint8 bits8;			/* >= 8 bits */
typedef uint16 bits16;			/* >= 16 bits */
typedef uint32 bits32;			/* >= 32 bits */

#ifndef HAVE_INT64
typedef long int int64;
#endif
#ifndef HAVE_UINT64
typedef unsigned long int uint64;
#endif

/* make gnu happy */
#define XK_PG_PARSER_UNUSED(x) (void) (x)

#define xk_pg_parser_AssertMacro(p) ((void) true)

#define XK_PG_VERSION_NUM 120000

#define xk_pg_parser__restrict __restrict

/* 字节对齐相关 begin */
#define XK_PG_PARSER_MAXIMUM_ALIGNOF 8
#define XK_PG_PARSER_ALIGNOF_INT 4
#define XK_PG_PARSER_ALIGNOF_SHORT 2
#define XK_PG_PARSER_ALIGNOF_DOUBLE 8

#define XK_PG_PARSER_TYPEALIGN(ALIGNVAL,LEN)  \
    (((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))

#define XK_PG_PARSER_DOUBLEALIGN(LEN)    XK_PG_PARSER_TYPEALIGN(XK_PG_PARSER_ALIGNOF_DOUBLE, (LEN))
#define XK_PG_PARSER_INTALIGN(LEN)   XK_PG_PARSER_TYPEALIGN(XK_PG_PARSER_ALIGNOF_INT, (LEN))
#define XK_PG_PARSER_SHORTALIGN(LEN)    XK_PG_PARSER_TYPEALIGN(XK_PG_PARSER_ALIGNOF_SHORT, (LEN))
#define XK_PG_PARSER_MAXALIGN(LEN)    XK_PG_PARSER_TYPEALIGN(XK_PG_PARSER_MAXIMUM_ALIGNOF, (LEN))

#define XK_PG_PARSER_Max(x, y)    ((x) > (y) ? (x) : (y))

#define XK_PG_PARSER_INT64CONST(x)  (x##L)
#define XK_PG_PARSER_UINT64CONST(x) (x##UL)

#define XK_PG_PARSER_INT32_MIN    (-0x7FFFFFFF-1)
#define XK_PG_PARSER_INT64_MIN    (-XK_PG_PARSER_INT64CONST(0x7FFFFFFFFFFFFFFF) - 1)
#define XK_PG_PARSER_INT64_MAX     XK_PG_PARSER_INT64CONST(0x7FFFFFFFFFFFFFFF)
/* 字节对齐相关 end */

/* 变长字节相关 begin */
typedef struct
{
    uint8_t        va_header;
    char        va_data[FLEXIBLE_ARRAY_MEMBER]; /* Data begins here */
} xk_pg_parser_varattrib_1b;

typedef struct
{
    uint8_t        va_header;        /* Always 0x80 or 0x01 */
    uint8_t        va_tag;            /* Type of datum */
    char           va_data[FLEXIBLE_ARRAY_MEMBER]; /* Type-specific data */
} xk_pg_parser_varattrib_1b_e;

struct xk_pg_parser_varlena
{
    char        vl_len_[4];        /* Do not touch this field directly! */
    char        vl_dat[FLEXIBLE_ARRAY_MEMBER];    /* Data content is here */
};

typedef union
{
    struct                        /* Normal xk_pg_parser_varlena (4-byte length) */
    {
        uint32_t        va_header;
        char            va_data[FLEXIBLE_ARRAY_MEMBER];
    } va_4byte;
    struct                        /* Compressed-in-line format */
    {
        uint32_t        va_header;
        uint32_t        va_rawsize; /* Original data size (excludes header) */
        char            va_data[FLEXIBLE_ARRAY_MEMBER]; /* Compressed data */
    } va_compressed;
} xk_pg_parser_varattrib_4b;

typedef union
{
    struct                            /* Normal varlena (4-byte length) */
    {
        uint32_t        va_header;
        char            va_data[FLEXIBLE_ARRAY_MEMBER];
    }va_4byte;
    struct                            /* Compressed-in-line format */
    {
        uint32_t        va_header;
        uint32_t        va_tcinfo;    /* Original data size (excludes header) and
                                       * compression method; see va_extinfo */
        char            va_data[FLEXIBLE_ARRAY_MEMBER]; /* Compressed data */
    }va_compressed;
} xk_pg_parser_pg14_varattrib_4b;

typedef struct xk_pg_parser_varatt_external
{
    int32_t           va_rawsize;        /* Original data size (includes header) */
    int32_t           va_extsize;        /* External saved size (doesn't) */
    uint32_t          va_valueid;        /* Unique ID of value within TOAST table */
    uint32_t          va_toastrelid;    /* RelID of TOAST table containing it */
} xk_pg_parser_varatt_external;

typedef enum xk_pg_parser_vartag_external
{
    XK_PG_PARSER_VARTAG_INDIRECT = 1,
    XK_PG_PARSER_VARTAG_EXPANDED_RO = 2,
    XK_PG_PARSER_VARTAG_EXPANDED_RW = 3,
    XK_PG_PARSER_VARTAG_ONDISK = 18
} xk_pg_parser_vartag_external;

typedef struct xk_pg_parser_varatt_indirect
{
    struct xk_pg_parser_varlena *pointer;    /* Pointer to in-memory xk_pg_parser_varlena */
}xk_pg_parser_varatt_indirect;

typedef struct xk_pg_parser_ExpandedObjectHeader xk_pg_parser_ExpandedObjectHeader;

typedef struct xk_pg_parser_varatt_expanded
{
    xk_pg_parser_ExpandedObjectHeader *eohptr;
} xk_pg_parser_varatt_expanded;

#define XK_PG_PARSER_VARTAG_IS_EXPANDED(tag) \
    (((tag) & ~1) == XK_PG_PARSER_VARTAG_EXPANDED_RO)

#define TrapMacro(condition, errorType) (true)

#define XK_PG_PARSER_VARTAG_SIZE(tag) \
    ((tag) == XK_PG_PARSER_VARTAG_INDIRECT ? sizeof(xk_pg_parser_varatt_indirect) : \
     XK_PG_PARSER_VARTAG_IS_EXPANDED(tag) ? sizeof(xk_pg_parser_varatt_expanded) : \
     (tag) == XK_PG_PARSER_VARTAG_ONDISK ? sizeof(xk_pg_parser_varatt_external) : \
     TrapMacro(true, "unrecognized TOAST vartag"))



#define XK_PG_PARSER_VARDATA_1B_E(PTR)    (((xk_pg_parser_varattrib_1b_e *) (PTR))->va_data)
#define XK_PG_PARSER_VARTAG_1B_E(PTR) \
    (((xk_pg_parser_varattrib_1b_e *) (PTR))->va_tag)

#define XK_PG_PARSER_VARATT_IS_1B_E(PTR) \
    ((((xk_pg_parser_varattrib_1b *) (PTR))->va_header) == 0x01)
#define XK_PG_PARSER_VARATT_IS_1B(PTR) \
    ((((xk_pg_parser_varattrib_1b *) (PTR))->va_header & 0x01) == 0x01)
#define XK_PG_PARSER_VARATT_IS_4B_U(PTR) \
	((((xk_pg_parser_varattrib_1b *) (PTR))->va_header & 0x03) == 0x00)

#define XK_PG_PARSER_VARSIZE_1B(PTR) \
    ((((xk_pg_parser_varattrib_1b *) (PTR))->va_header >> 1) & 0x7F)
#define XK_PG_PARSER_VARSIZE_4B(PTR) \
    ((((xk_pg_parser_varattrib_4b *) (PTR))->va_4byte.va_header >> 2) & 0x3FFFFFFF)

#define XK_PG_PARSER_VARATT_IS_4B_C(PTR) \
    ((((xk_pg_parser_varattrib_1b *) (PTR))->va_header & 0x03) == 0x02)

#define XK_PG_PARSER_VARHDRSZ_EXTERNAL   offsetof(xk_pg_parser_varattrib_1b_e, va_data)

#define XK_PG_PARSER_VARTAG_EXTERNAL(PTR)     XK_PG_PARSER_VARTAG_1B_E(PTR)
#define XK_PG_PARSER_VARSIZE_SHORT(PTR)       XK_PG_PARSER_VARSIZE_1B(PTR)
#define XK_PG_PARSER_VARDATA_EXTERNAL(PTR)    XK_PG_PARSER_VARDATA_1B_E(PTR)
#define XK_PG_PARSER_VARSIZE_EXTERNAL(PTR)   (XK_PG_PARSER_VARHDRSZ_EXTERNAL + XK_PG_PARSER_VARTAG_SIZE(XK_PG_PARSER_VARTAG_EXTERNAL(PTR)))

#define XK_PG_PARSER_VARSIZE_ANY(PTR) \
    (XK_PG_PARSER_VARATT_IS_1B_E(PTR) ? XK_PG_PARSER_VARSIZE_EXTERNAL(PTR) : \
     (XK_PG_PARSER_VARATT_IS_1B(PTR) ? XK_PG_PARSER_VARSIZE_1B(PTR) : \
      XK_PG_PARSER_VARSIZE_4B(PTR)))

#define XK_PG_PARSER_VARHDRSZ ((int32_t) sizeof(int32_t))

#define XK_PG_PARSER_VARHDRSZ_SHORT offsetof(xk_pg_parser_varattrib_1b, va_data)

#define XK_PG_PARSER_VARSIZE_ANY_EXHDR(PTR) \
    (XK_PG_PARSER_VARATT_IS_1B_E(PTR) ? XK_PG_PARSER_VARSIZE_EXTERNAL(PTR)-XK_PG_PARSER_VARHDRSZ_EXTERNAL : \
     (XK_PG_PARSER_VARATT_IS_1B(PTR) ? XK_PG_PARSER_VARSIZE_1B(PTR)-XK_PG_PARSER_VARHDRSZ_SHORT : \
      XK_PG_PARSER_VARSIZE_4B(PTR)-XK_PG_PARSER_VARHDRSZ))

#define XK_PG_PARSER_VARDATA_1B(PTR) (((xk_pg_parser_varattrib_1b *) (PTR))->va_data)
#define XK_PG_PARSER_VARDATA_4B(PTR) (((xk_pg_parser_varattrib_4b *) (PTR))->va_4byte.va_data)

#define XK_PG_PARSER_VARDATA_ANY(PTR) \
     (XK_PG_PARSER_VARATT_IS_1B(PTR) ? XK_PG_PARSER_VARDATA_1B(PTR) : XK_PG_PARSER_VARDATA_4B(PTR))

#define XK_PG_PARSER_VARATT_IS_EXTERNAL(PTR) XK_PG_PARSER_VARATT_IS_1B_E(PTR)
#define XK_PG_PARSER_VARATT_IS_EXTENDED(PTR) (!XK_PG_PARSER_VARATT_IS_4B_U(PTR))
#define XK_PG_PARSER_VARATT_IS_COMPRESSED(PTR)   XK_PG_PARSER_VARATT_IS_4B_C(PTR)

#define XK_PG_PARSER_VARATT_IS_EXTERNAL_ONDISK(PTR) \
    (XK_PG_PARSER_VARATT_IS_EXTERNAL(PTR) \
    && XK_PG_PARSER_VARTAG_EXTERNAL(PTR) == XK_PG_PARSER_VARTAG_ONDISK)

#define XK_PG_PARSER_VARATT_IS_EXTERNAL_INDIRECT(PTR) \
    (XK_PG_PARSER_VARATT_IS_EXTERNAL(PTR) \
    && XK_PG_PARSER_VARTAG_EXTERNAL(PTR) == XK_PG_PARSER_VARTAG_INDIRECT)

#define XK_PG_PARSER_VARATT_IS_EXTERNAL_EXPANDED_RO(PTR) \
    (XK_PG_PARSER_VARATT_IS_EXTERNAL(PTR) \
    && XK_PG_PARSER_VARTAG_EXTERNAL(PTR) == XK_PG_PARSER_VARTAG_EXPANDED_RO)

#define XK_PG_PARSER_VARATT_IS_EXTERNAL_EXPANDED_RW(PTR) \
    (XK_PG_PARSER_VARATT_IS_EXTERNAL(PTR) \
    && XK_PG_PARSER_VARTAG_EXTERNAL(PTR) == XK_PG_PARSER_VARTAG_EXPANDED_RW)

#define XK_PG_PARSER_VARATT_IS_EXTERNAL_EXPANDED(PTR) \
    (XK_PG_PARSER_VARATT_IS_EXTERNAL(PTR) \
    && XK_PG_PARSER_VARTAG_IS_EXPANDED(XK_PG_PARSER_VARTAG_EXTERNAL(PTR)))

#define XK_PG_PARSER_SET_VARSIZE_4B(PTR,len) \
    (((xk_pg_parser_varattrib_4b *) (PTR))->va_4byte.va_header = (((uint32_t) (len)) << 2))

#define XK_PG_PARSER_SET_VARSIZE_4B_C(PTR,len) \
    (((xk_pg_parser_varattrib_4b *) (PTR))->va_4byte.va_header = (((uint32_t) (len)) << 2) | 0x02)

#define XK_PG_PARSER_SET_VARSIZE(PTR, len)   XK_PG_PARSER_SET_VARSIZE_4B(PTR, len)

#define XK_PG_PARSER_VARATT_IS_SHORT(PTR)   XK_PG_PARSER_VARATT_IS_1B(PTR)

#define XK_PG_PARSER_VARDATA(PTR)           XK_PG_PARSER_VARDATA_4B(PTR)
#define XK_PG_PARSER_VARSIZE(PTR)           XK_PG_PARSER_VARSIZE_4B(PTR)
#define XK_PG_PARSER_VARDATA_SHORT(PTR)     XK_PG_PARSER_VARDATA_1B(PTR)

typedef struct xk_pg_parser_toast_compress_header
{
    int32_t        vl_len_;        /* varlena header (do not touch directly!) */
    int32_t        rawsize;
} xk_pg_parser_toast_compress_header;

#define XK_PG_PARSER_TOAST_COMPRESS_HDRSZ    ((int32_t) sizeof(xk_pg_parser_toast_compress_header))
#define XK_PG_PARSER_TOAST_COMPRESS_RAWSIZE(ptr) \
    (((xk_pg_parser_toast_compress_header *) (ptr))->rawsize)
#define XK_PG_PARSER_TOAST_COMPRESS_RAWDATA(ptr) \
    (((char *) (ptr)) + XK_PG_PARSER_TOAST_COMPRESS_HDRSZ)

/* 压缩相关 */
#define XK_PG_PARSER_VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer) \
    ((toast_pointer)->m_extsize < (toast_pointer)->m_rawsize - XK_PG_PARSER_VARHDRSZ)

#define XK_PG_PARSER_VARATT_EXTERNAL_CHECK_COMPRESSED(extsize, rawsize) \
    (extsize < (rawsize - XK_PG_PARSER_VARHDRSZ))

#define XK_PG_PARSER_SET_VARSIZE_COMPRESSED(PTR, len) XK_PG_PARSER_SET_VARSIZE_4B_C(PTR, len)

/* PG14压缩相关 */
typedef enum xk_pg_parser_ToastCompressionId
{
    XK_PG_PARSER_PG14_TOAST_PGLZ_COMPRESSION_ID = 0,
    XK_PG_PARSER_PG14_TOAST_LZ4_COMPRESSION_ID = 1,
    XK_PG_PARSER_PG14_TOAST_INVALID_COMPRESSION_ID = 2
} xk_pg_parser_ToastCompressionId;
typedef struct xk_pg_parser_pg14_toast_compress_header
{
    int32_t        vl_len_;        /* varlena header (do not touch directly!) */
    uint32_t       tcinfo;            /* 2 bits for compression method and 30 bits
                                 * external size; see va_extinfo */
} xk_pg_parser_pg14_toast_compress_header;
#define XK_PG_PARSER_PG14_VARHDRSZ_COMPRESSED offsetof(xk_pg_parser_pg14_varattrib_4b, va_compressed.va_data)
#define XK_PG_PARSER_PG14_VARLENA_EXTSIZE_BITS    30
#define XK_PG_PARSER_PG14_VARLENA_EXTSIZE_MASK    ((1U << XK_PG_PARSER_PG14_VARLENA_EXTSIZE_BITS) - 1)
#define XK_PG_PARSER_PG14_VARDATA_COMPRESSED_GET_EXTSIZE(PTR) \
    (((xk_pg_parser_pg14_varattrib_4b *) (PTR))->va_compressed.va_tcinfo & XK_PG_PARSER_PG14_VARLENA_EXTSIZE_MASK)
#define XK_PG_PARSER_PG14_VARDATA_COMPRESSED_GET_COMPRESS_METHOD(PTR) \
    (((xk_pg_parser_pg14_varattrib_4b *) (PTR))->va_compressed.va_tcinfo >> XK_PG_PARSER_PG14_VARLENA_EXTSIZE_BITS)
#define XK_PG_PARSER_PG14_TOAST_COMPRESS_EXTSIZE(ptr) \
    (((xk_pg_parser_pg14_toast_compress_header *) (ptr))->tcinfo & XK_PG_PARSER_PG14_VARLENA_EXTSIZE_MASK)
#define XK_PG_PARSER_PG14_TOAST_COMPRESS_METHOD(ptr) \
    (((xk_pg_parser_pg14_toast_compress_header *) (ptr))->tcinfo >> XK_PG_PARSER_PG14_VARLENA_EXTSIZE_BITS)
#define XK_PG_PARSER_PG14_TOAST_COMPRESS_SET_SIZE_AND_COMPRESS_METHOD(ptr, len, cm_method) \
    do { \
        ((xk_pg_parser_pg14_toast_compress_header *) (ptr))->tcinfo = \
            (len) | ((uint32) (cm_method) << XK_PG_PARSER_PG14_VARLENA_EXTSIZE_BITS); \
    } while (0)

/* 变长字节相关 end */

/* 系统表用到的相关常量 begin*/
#define XK_PG_PARSER_SYSDICT_PRIMARY_KEY_MAX 20
#define xk_pg_parser_InvalidOid 0
#define xk_pg_parser_InvalidTransactionId 0
/* end */

/* datum相关 begin*/
typedef unsigned long int xk_pg_parser_uintptr_t;
typedef xk_pg_parser_uintptr_t xk_pg_parser_Datum;
typedef char *xk_pg_parser_Pointer;
#define xk_pg_parser_CharGetDatum(X) ((xk_pg_parser_Datum) (X))
#define xk_pg_parser_Int16GetDatum(X) ((xk_pg_parser_Datum) (X))
#define xk_pg_parser_Int32GetDatum(X) ((xk_pg_parser_Datum) (X))
#define xk_pg_parser_PointerGetDatum(X) ((xk_pg_parser_Datum) (X))
#define xk_pg_parser_TransactionIdGetDatum(X) ((xk_pg_parser_Datum) (X))
#define xk_pg_parser_CommandIdGetDatum(X) ((xk_pg_parser_Datum) (X))
#define xk_pg_parser_ObjectIdGetDatum(X) ((xk_pg_parser_Datum) (X))
#define xk_pg_parser_DatumGetPointer(X) ((xk_pg_parser_Pointer) (X))
#define xk_pg_parser_DatumGetInt32(X) ((int32_t) (X))
/* datum相关 end*/

/* 类型定义*/
#define XK_PG_PARSER_NAMEDATALEN 64

typedef struct xk_pg_parser_nameData
{
    char data[XK_PG_PARSER_NAMEDATALEN];
} xk_pg_parser_NameData;
typedef xk_pg_parser_NameData *xk_pg_parser_Name;

typedef struct xk_pg_parser_varlena xk_pg_parser_bytea;
typedef struct xk_pg_parser_varlena xk_pg_parser_text;
typedef struct xk_pg_parser_varlena xk_pg_parser_BpChar;
typedef struct xk_pg_parser_varlena xk_pg_parser_VarChar;

/* 类型定义 */
typedef uint32_t xk_pg_parser_TransactionId;
typedef uint64_t xk_pg_parser_XLogRecPtr;
typedef uint8_t  xk_pg_parser_RmgrId;
typedef uint32_t xk_pg_parser_crc32c;
typedef uint32_t xk_pg_parser_BlockNumber;
typedef uint16_t xk_pg_parser_RepOriginId;
typedef uint64_t xk_pg_parser_XLogSegNo;
typedef uint32_t xk_pg_parser_TimeLineID;
typedef uint16_t xk_pg_parser_OffsetNumber;
typedef uint32_t xk_pg_parser_CommandId;
#endif
