#ifndef _RIPPLE_C_H
#define _RIPPLE_C_H

#define NAMEDATALEN 64
#define gettext_noop(x) (x)


/* ----------------------------------------------------------------
 *				Section 2:	bool, true, false
 * ----------------------------------------------------------------
 */
#ifndef bool
typedef int8_t bool;
#endif

#ifndef true
#define true	((bool) 1)
#endif

#ifndef false
#define false	((bool) 0)
#endif


/* ----------------------------------------------------------------
 *				Section 3:	standard system types
 * ----------------------------------------------------------------
 */
typedef char*                           Pointer;


#ifndef HAVE_INT8
typedef signed char                     int8;		/* == 8 bits */
typedef signed short                    int16;		/* == 16 bits */
typedef signed int                      int32;		/* == 32 bits */
#endif							/* not HAVE_INT8 */

#ifndef HAVE_UINT8
typedef unsigned char                   uint8;	/* == 8 bits */
typedef unsigned short                  uint16;	/* == 16 bits */
typedef unsigned int                    uint32;	/* == 32 bits */
#endif							/* not HAVE_UINT8 */

typedef uint8                           bits8;			/* >= 8 bits */
typedef uint16                          bits16;			/* >= 16 bits */
typedef uint32                          bits32;			/* >= 32 bits */

#ifndef HAVE_INT64
typedef long int                        int64;
#endif
#ifndef HAVE_UINT64
typedef unsigned long int               uint64;
#endif

#define INT64CONST(x)                   (x##L)
#define UINT64CONST(x)                  (x##UL)

#define RIPPLE_INT8_MIN                 (-0x7F-1)
#define RIPPLE_INT8_MAX                 (0x7F)
#define RIPPLE_UINT8_MAX                (0xFF)
#define RIPPLE_INT16_MIN                (-0x7FFF-1)
#define RIPPLE_INT16_MAX                (0x7FFF)
#define RIPPLE_UINT16_MAX               (0xFFFF)
#define RIPPLE_INT32_MIN                (-0x7FFFFFFF-1)
#define RIPPLE_INT32_MAX                (0x7FFFFFFF)
#define RIPPLE_UINT32_MAX               (0xFFFFFFFFU)
#define RIPPLE_INT64_MIN                (-INT64CONST(0x7FFFFFFFFFFFFFFF) - 1)
#define RIPPLE_INT64_MAX                INT64CONST(0x7FFFFFFFFFFFFFFF)
#define RIPPLE_UINT64_MAX               UINT64CONST(0xFFFFFFFFFFFFFFFF)

typedef size_t                          Size;

typedef int                             rsocket;

typedef unsigned int                    Index;

typedef signed int                      Offset;

typedef float                           float4;
typedef double                          float8;

typedef unsigned int                    Oid;


#define InvalidOid                      ((Oid) 0)
#define OID_MAX                         UINT_MAX


typedef Oid                             regproc;

typedef uint32                          TransactionId;

typedef uint64                          FullTransactionId;

typedef TransactionId                   MultiXactId;

typedef uint32                          MultiXactOffset;

typedef uint32                          CommandId;

#define FirstCommandId                  ((CommandId) 0)
#define InvalidCommandId                (~(CommandId)0)

#define InvalidTransactionId            ((TransactionId) 0)

#define InvalidFullTransactionId        ((FullTransactionId) 0)

typedef uint32                          TimeLineID;

typedef int64                           TimestampTz;

#define InvalidTimeLineID               ((TimeLineID)0)

#define FLEXIBLE_ARRAY_MEMBER

#define VARHDRSZ		((int32) sizeof(int32))

/*
 * Pointer to a location in the XLOG.  These pointers are 64 bits wide,
 * because we don't want them ever to overflow.
 */
typedef uint64 XLogRecPtr;

typedef struct RelFileNode
{
	Oid			spcNode;		/* tablespace */
	Oid			dbNode;			/* database */
	Oid			relNode;		/* relation */
} RelFileNode;


/* ----------------------------------------------------------------
 *				Section 4:	IsValid macros for system types
 * ----------------------------------------------------------------
 */
/*
 * BoolIsValid
 *		True iff bool is valid.
 */
#define BoolIsValid(boolean)	((boolean) == false || (boolean) == true)

/*
 * PointerIsValid
 *		True iff pointer is valid.
 */
#define PointerIsValid(pointer) ((const void*)(pointer) != NULL)

/*
 * PointerIsAligned
 *		True iff pointer is properly aligned to point to the given type.
 */
#define PointerIsAligned(pointer, type) \
		(((uintptr_t)(pointer) % (sizeof (type))) == 0)

#define OffsetToPointer(base, offset) \
		((void *)((char *) base + offset))

#define OidIsValid(objectId)  ((bool) ((objectId) != InvalidOid))

#define RegProcedureIsValid(p)	OidIsValid(p)

/*
 * Zero is used indicate an invalid pointer. Bootstrap skips the first possible
 * WAL segment, initializing the first WAL page at WAL segment size, so no XLOG
 * record can begin at zero.
 */
#define InvalidXLogRecPtr	0
#define XLogRecPtrIsInvalid(r)	((r) == InvalidXLogRecPtr)


/*
 * XLogSegNo - physical log file sequence number.
 */
typedef uint64 XLogSegNo;

typedef uint8 RmgrId;

typedef uint32          pg_crc32c;
typedef pg_crc32c       r_crc32c;

typedef enum RIPPLE_RECPOS_TYPE
{
    RIPPLE_RECPOS_TYPE_NOP = 0x00,
    RIPPLE_RECPOS_TYPE_TRAIL,
    RIPPLE_RECPOS_TYPE_WAL
} ripple_recpos_type;

typedef union RIPPLE_RECPOS
{
    struct{
        int type;
        TimeLineID              timeline;
        XLogRecPtr              lsn;           /* commit时，父子事务排序     */
    }wal;

    struct{
        int type;
        uint64                  fileid;/* 文件 ID */
        uint64                  offset;/* 偏移量 */
    }trail;
}ripple_recpos;

/*
 * TimeLineID (TLI) - identifies different database histories to prevent
 * confusion after restoring a prior state of a database installation.
 * TLI does not change in a normal stop/restart of the database (including
 * crash-and-recover cases); but we must assign a new TLI after doing
 * a recovery to a prior state, a/k/a point-in-time recovery.  This makes
 * the new WAL logfile sequence we generate distinguishable from the
 * sequence that was generated in the previous incarnation.
 */

#define MAXFNAMELEN 64
#define MAXPGPATH 1024

/* ----------------------------------------------------------------
 *				Section 5:	offsetof, lengthof, alignment
 * ----------------------------------------------------------------
 */
#ifndef offsetof
#define offsetof(type, field)	((long) &((type *)0)->field)
#endif							/* offsetof */

/*
 * lengthof
 *		Number of elements in an array.
 */
#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))

#define MAXIMUM_ALIGNOF 8

#define TYPEALIGN(ALIGNVAL,LEN)  \
	(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))

#define SHORTALIGN(LEN)			TYPEALIGN(ALIGNOF_SHORT, (LEN))
#define INTALIGN(LEN)			TYPEALIGN(ALIGNOF_INT, (LEN))
#define LONGALIGN(LEN)			TYPEALIGN(ALIGNOF_LONG, (LEN))
#define DOUBLEALIGN(LEN)		TYPEALIGN(ALIGNOF_DOUBLE, (LEN))
#define MAXALIGN(LEN)			TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))
/* MAXALIGN covers only built-in types, not buffers */
#define BUFFERALIGN(LEN)		TYPEALIGN(ALIGNOF_BUFFER, (LEN))
#define CACHELINEALIGN(LEN)		TYPEALIGN(PG_CACHE_LINE_SIZE, (LEN))

#define TYPEALIGN_DOWN(ALIGNVAL,LEN)  \
	(((uintptr_t) (LEN)) & ~((uintptr_t) ((ALIGNVAL) - 1)))

#define SHORTALIGN_DOWN(LEN)	TYPEALIGN_DOWN(ALIGNOF_SHORT, (LEN))
#define INTALIGN_DOWN(LEN)		TYPEALIGN_DOWN(ALIGNOF_INT, (LEN))
#define LONGALIGN_DOWN(LEN)		TYPEALIGN_DOWN(ALIGNOF_LONG, (LEN))
#define DOUBLEALIGN_DOWN(LEN)	TYPEALIGN_DOWN(ALIGNOF_DOUBLE, (LEN))
#define MAXALIGN_DOWN(LEN)		TYPEALIGN_DOWN(MAXIMUM_ALIGNOF, (LEN))
#define BUFFERALIGN_DOWN(LEN)	TYPEALIGN_DOWN(ALIGNOF_BUFFER, (LEN))

/*
 * The above macros will not work with types wider than uintptr_t, like with
 * uint64 on 32-bit platforms.  That's not problem for the usual use where a
 * pointer or a length is aligned, but for the odd case that you need to
 * align something (potentially) wider, use TYPEALIGN64.
 */
#define TYPEALIGN64(ALIGNVAL,LEN)  \
	(((uint64) (LEN) + ((ALIGNVAL) - 1)) & ~((uint64) ((ALIGNVAL) - 1)))

/* we don't currently need wider versions of the other ALIGN macros */
#define MAXALIGN64(LEN)			TYPEALIGN64(MAXIMUM_ALIGNOF, (LEN))



/* ----------------------------------------------------------------
 *				Section 6:	assertions
 * ----------------------------------------------------------------
 */
#ifndef USE_ASSERT_CHECKING

#define Assert(condition)	((void)true)
#define AssertMacro(condition)	((void)true)
#define AssertArg(condition)	((void)true)
#define AssertState(condition)	((void)true)
#define AssertPointerAlignment(ptr, bndr)	((void)true)
#define Trap(condition, errorType)	((void)true)
#define TrapMacro(condition, errorType) (true)

#elif defined(FRONTEND)

#include <assert.h>
#define Assert(p) assert(p)
#define AssertMacro(p)	((void) assert(p))
#define AssertArg(condition) assert(condition)
#define AssertState(condition) assert(condition)
#define AssertPointerAlignment(ptr, bndr)	((void)true)

#else							/* USE_ASSERT_CHECKING && !FRONTEND */

/*
 * Trap
 *		Generates an exception if the given condition is true.
 */
#define Trap(condition, errorType) \
	do { \
		if (condition) \
			ExceptionalCondition(CppAsString(condition), (errorType), \
								 __FILE__, __LINE__); \
	} while (0)

/*
 *	TrapMacro is the same as Trap but it's intended for use in macros:
 *
 *		#define foo(x) (AssertMacro(x != 0), bar(x))
 *
 *	Isn't CPP fun?
 */
#define TrapMacro(condition, errorType) \
	((bool) (! (condition) || \
			 (ExceptionalCondition(CppAsString(condition), (errorType), \
								   __FILE__, __LINE__), 0)))

#define Assert(condition) \
		Trap(!(condition), "FailedAssertion")

#define AssertMacro(condition) \
		((void) TrapMacro(!(condition), "FailedAssertion"))

#define AssertArg(condition) \
		Trap(!(condition), "BadArgument")

#define AssertState(condition) \
		Trap(!(condition), "BadState")

/*
 * Check that `ptr' is `bndr' aligned.
 */
#define AssertPointerAlignment(ptr, bndr) \
	Trap(TYPEALIGN(bndr, (uintptr_t)(ptr)) != (uintptr_t)(ptr), \
		 "UnalignedPointer")

#endif							/* USE_ASSERT_CHECKING && !FRONTEND */

/* ----------------------------------------------------------------
 *				Section 7:	widely useful macros
 * ----------------------------------------------------------------
 */
/*
 * Max
 *		Return the maximum of two numbers.
 */
#define Max(x, y)		((x) > (y) ? (x) : (y))

/*
 * Min
 *		Return the minimum of two numbers.
 */
#define Min(x, y)		((x) < (y) ? (x) : (y))

/*
 * Abs
 *		Return the absolute value of the argument.
 */
#define Abs(x)			((x) >= 0 ? (x) : -(x))

#if defined(WIN32) || defined(__CYGWIN__)
#define RIPPLE_BINARY	O_BINARY
#define RIPPLE_BINARY_A "ab"
#define RIPPLE_BINARY_R "rb"
#define RIPPLE_BINARY_W "wb"
#else
#define RIPPLE_BINARY	0
#define RIPPLE_BINARY_A "a"
#define RIPPLE_BINARY_R "r"
#define RIPPLE_BINARY_W "w"
#endif

/* Get a bit mask of the bits set in non-long aligned addresses */
#define LONG_ALIGN_MASK (sizeof(long) - 1)

/* Define bytes to use libc memset(). */
#define MEMSET_LOOP_LIMIT 1024

#define PG_PRINTF_ATTRIBUTE gnu_printf
#define pg_attribute_printf(f,a) __attribute__((format(PG_PRINTF_ATTRIBUTE, f, a)))


#define RIPPLE_CONCAT(x,y)                  x##y

/*-------------------align begin--------------------------------*/
#define RIPPLE_MAXIMUM_ALIGNOF 8

#define RIPPLE_TYPEALIGN(ALIGNVAL,LEN)  \
    (((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))

/* 8 字节对齐 */
#define RIPPLE_MAXALIGN(LEN)            RIPPLE_TYPEALIGN(RIPPLE_MAXIMUM_ALIGNOF, (LEN))

/*-------------------align   end--------------------------------*/

/* 主机字节序到网络字节序 */
#define r_hton16(x)		__builtin_bswap16(x)
#define r_hton32(x)		__builtin_bswap32(x)
#define r_hton64(x)		__builtin_bswap64(x)

/* 网络字节序到主机字节序 */
#define r_ntoh16(x)		__builtin_bswap16(x)
#define r_ntoh32(x)		__builtin_bswap32(x)
#define r_ntoh64(x)     __builtin_bswap64(x)

#endif
