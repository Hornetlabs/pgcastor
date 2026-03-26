/**
 * @file pg_parser_thirdparty_tupleparser_numeric.c
 * @author bytesync
 * @brief
 * @version 0.1
 * @date 2023-08-03
 *
 * @copyright Copyright (c) 2023
 *
 */
#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_pgsfunc.h"
#include "thirdparty/tupleparser/toast/pg_parser_thirdparty_tupleparser_toast.h"
#include "thirdparty/stringinfo/pg_parser_thirdparty_stringinfo.h"
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_fmgr.h"

#define PGFUNC_NUMERIC_MCXT       NULL
#define PGFUNC_JSONB_MCXT         NULL

#define PG_PARSER_NUMERIC_OUT_OID 1702

typedef int16_t NumericDigit;

struct NumericShort
{
    uint16_t     n_header;                      /* Sign + display scale + weight */
    NumericDigit n_data[FLEXIBLE_ARRAY_MEMBER]; /* Digits */
};

struct NumericLong
{
    uint16_t     n_sign_dscale;                 /* Sign + display scale */
    int16_t      n_weight;                      /* Weight of 1st digit    */
    NumericDigit n_data[FLEXIBLE_ARRAY_MEMBER]; /* Digits */
};

union NumericChoice
{
    uint16_t            n_header; /* Header word */
    struct NumericLong  n_long;   /* Long form (4-byte header) */
    struct NumericShort n_short;  /* Short form (2-byte header) */
};

struct NumericData
{
    int32_t             vl_len_; /* varlena header (do not touch directly!) */
    union NumericChoice choice;  /* choice of format */
};
struct NumericData;
typedef struct NumericData* Numeric;

typedef struct NumericVar
{
    int32_t       ndigits; /* # of digits in digits[] - can be 0! */
    int32_t       weight;  /* weight of first digit */
    int32_t       sign;    /* NUMERIC_POS, NUMERIC_NEG, or NUMERIC_NAN */
    int32_t       dscale;  /* display scale */
    NumericDigit* buf;     /* start of palloc'd space for digits[] */
    NumericDigit* digits;  /* base-NBASE digits */
} NumericVar;

#define NBASE                      10000
#define HALF_NBASE                 5000
#define DEC_DIGITS                 4 /* decimal digits per NBASE digit */
#define MUL_GUARD_DIGITS           2 /* these are measured in NBASE digits */
#define DIV_GUARD_DIGITS           4

#define NUMERIC_SIGN_MASK          0xC000
#define NUMERIC_POS                0x0000
#define NUMERIC_NEG                0x4000
#define NUMERIC_SHORT              0x8000
#define NUMERIC_NAN                0xC000

#define NUMERIC_FLAGBITS(n)        ((n)->choice.n_header & NUMERIC_SIGN_MASK)
#define NUMERIC_IS_NAN(n)          (NUMERIC_FLAGBITS(n) == NUMERIC_NAN)
#define NUMERIC_IS_SHORT(n)        (NUMERIC_FLAGBITS(n) == NUMERIC_SHORT)

#define NUMERIC_HDRSZ              (PG_PARSER_VARHDRSZ + sizeof(uint16_t) + sizeof(int16_t))
#define NUMERIC_HDRSZ_SHORT        (PG_PARSER_VARHDRSZ + sizeof(uint16_t))

#define NUMERIC_HEADER_IS_SHORT(n) (((n)->choice.n_header & 0x8000) != 0)
#define NUMERIC_HEADER_SIZE(n) \
    (PG_PARSER_VARHDRSZ + sizeof(uint16_t) + (NUMERIC_HEADER_IS_SHORT(n) ? 0 : sizeof(int16_t)))

#define NUMERIC_NDIGITS(num) \
    ((PG_PARSER_VARSIZE(num) - NUMERIC_HEADER_SIZE(num)) / sizeof(NumericDigit))

#define NUMERIC_DSCALE_MASK            0x3FFF
#define NUMERIC_SHORT_DSCALE_MASK      0x1F80
#define NUMERIC_SHORT_DSCALE_SHIFT     7
#define NUMERIC_SHORT_SIGN_MASK        0x2000
#define NUMERIC_SHORT_WEIGHT_SIGN_MASK 0x0040
#define NUMERIC_SHORT_WEIGHT_MASK      0x003F

#define NUMERIC_SIGN(n)                                                                           \
    (NUMERIC_IS_SHORT(n)                                                                          \
         ? (((n)->choice.n_short.n_header & NUMERIC_SHORT_SIGN_MASK) ? NUMERIC_NEG : NUMERIC_POS) \
         : NUMERIC_FLAGBITS(n))

#define NUMERIC_DSCALE(n)                                                                         \
    (NUMERIC_HEADER_IS_SHORT((n)) ? ((n)->choice.n_short.n_header & NUMERIC_SHORT_DSCALE_MASK) >> \
                                        NUMERIC_SHORT_DSCALE_SHIFT                                \
                                  : ((n)->choice.n_long.n_sign_dscale & NUMERIC_DSCALE_MASK))

#define NUMERIC_WEIGHT(n)                                                  \
    (NUMERIC_HEADER_IS_SHORT((n))                                          \
         ? (((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_SIGN_MASK \
                 ? ~NUMERIC_SHORT_WEIGHT_MASK                              \
                 : 0) |                                                    \
            ((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_MASK))    \
         : ((n)->choice.n_long.n_weight))

#define NUMERIC_DIGITS(num) \
    (NUMERIC_HEADER_IS_SHORT(num) ? (num)->choice.n_short.n_data : (num)->choice.n_long.n_data)

/* static function statement */
static void init_var_from_num(Numeric num, NumericVar* dest);
static char* get_str_from_var(const NumericVar* var);

static void init_var_from_num(Numeric num, NumericVar* dest)
{
    dest->ndigits = NUMERIC_NDIGITS(num);
    dest->weight = NUMERIC_WEIGHT(num);
    dest->sign = NUMERIC_SIGN(num);
    dest->dscale = NUMERIC_DSCALE(num);
    dest->digits = NUMERIC_DIGITS(num);
    dest->buf = NULL; /* digits array is not palloc'd */
}

/*
 * get_str_from_var() -
 *
 *    Convert a var to pg_parser_text representation (guts of numeric_out).
 *    The var is displayed to the number of digits indicated by its dscale.
 *    Returns a palloc'd string.
 */
static char* get_str_from_var(const NumericVar* var)
{
    int32_t      dscale;
    char*        str;
    char*        cp;
    char*        endcp;
    int32_t      i;
    int32_t      d;
    NumericDigit dig;

#if DEC_DIGITS > 1
    NumericDigit d1;
#endif

    dscale = var->dscale;

    /*
     * Allocate space for the result.
     *
     * i is set to the # of decimal digits before decimal point. dscale is the
     * # of decimal digits we will print after decimal point. We may generate
     * as many as DEC_DIGITS-1 excess digits at the end, and in addition we
     * need room for sign, decimal point, null terminator.
     */
    i = (var->weight + 1) * DEC_DIGITS;
    if (i <= 0)
    {
        i = 1;
    }

    if (!pg_parser_mcxt_malloc(PGFUNC_NUMERIC_MCXT, (void**)&str, i + dscale + DEC_DIGITS + 2))
    {
        return NULL;
    }

    cp = str;

    /*
     * Output a dash for negative values
     */
    if (var->sign == NUMERIC_NEG)
    {
        *cp++ = '-';
    }

    /*
     * Output all digits before the decimal point
     */
    if (var->weight < 0)
    {
        d = var->weight + 1;
        *cp++ = '0';
    }
    else
    {
        for (d = 0; d <= var->weight; d++)
        {
            dig = (d < var->ndigits) ? var->digits[d] : 0;
            /* In the first digit, suppress extra leading decimal zeroes */
#if DEC_DIGITS == 4
            {
                bool putit = (d > 0);

                d1 = dig / 1000;
                dig -= d1 * 1000;
                putit |= (d1 > 0);
                if (putit)
                {
                    *cp++ = d1 + '0';
                }
                d1 = dig / 100;
                dig -= d1 * 100;
                putit |= (d1 > 0);
                if (putit)
                {
                    *cp++ = d1 + '0';
                }
                d1 = dig / 10;
                dig -= d1 * 10;
                putit |= (d1 > 0);
                if (putit)
                {
                    *cp++ = d1 + '0';
                }
                *cp++ = dig + '0';
            }
#elif DEC_DIGITS == 2
            d1 = dig / 10;
            dig -= d1 * 10;
            if (d1 > 0 || d > 0)
            {
                *cp++ = d1 + '0';
            }
            *cp++ = dig + '0';
#elif DEC_DIGITS == 1
            *cp++ = dig + '0';
#else
#error unsupported NBASE
#endif
        }
    }

    /*
     * If requested, output a decimal point and all the digits that follow it.
     * We initially put out a multiple of DEC_DIGITS digits, then truncate if
     * needed.
     */
    if (dscale > 0)
    {
        *cp++ = '.';
        endcp = cp + dscale;
        for (i = 0; i < dscale; d++, i += DEC_DIGITS)
        {
            dig = (d >= 0 && d < var->ndigits) ? var->digits[d] : 0;
#if DEC_DIGITS == 4
            d1 = dig / 1000;
            dig -= d1 * 1000;
            *cp++ = d1 + '0';
            d1 = dig / 100;
            dig -= d1 * 100;
            *cp++ = d1 + '0';
            d1 = dig / 10;
            dig -= d1 * 10;
            *cp++ = d1 + '0';
            *cp++ = dig + '0';
#elif DEC_DIGITS == 2
            d1 = dig / 10;
            dig -= d1 * 10;
            *cp++ = d1 + '0';
            *cp++ = dig + '0';
#elif DEC_DIGITS == 1
            *cp++ = dig + '0';
#else
#error unsupported NBASE
#endif
        }
        cp = endcp;
    }

    /*
     * terminate the string and return it
     */
    *cp = '\0';
    return str;
}

/*
 * numeric_out() -
 *
 *    Output function for numeric data type
 */
pg_parser_Datum numeric_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info)
{
    bool       is_toast = false;
    bool       need_free = false;
    Numeric    num = (Numeric)pg_parser_detoast_datum((struct pg_parser_varlena*)attr,
                                                   &is_toast,
                                                   &need_free,
                                                   info->zicinfo->dbtype,
                                                   info->zicinfo->dbversion);
    NumericVar x;
    char*      str;

    if (is_toast)
    {
        info->valueinfo = INFO_COL_IS_TOAST;
        info->valuelen = sizeof(struct pg_parser_varatt_external);
        return (pg_parser_Datum)num;
    }

    /*
     * Handle NaN
     */
    if (NUMERIC_IS_NAN(num))
    {
        return (pg_parser_Datum)pg_parser_mcxt_strdup("NaN");
    }

    /*
     * Get the number in the variable format.
     */
    init_var_from_num(num, &x);

    str = get_str_from_var(&x);
    info->valuelen = strlen(str);
    if (need_free)
    {
        pg_parser_mcxt_free(PGFUNC_NUMERIC_MCXT, num);
    }
    return (pg_parser_Datum)str;
}

/* -------------------------------- jsonb -------------------------------- */
typedef uint32_t JEntry;

/* flags for the header-field in JsonbContainer */
#define JB_CMASK   0x0FFFFFFF /* mask for count field */
#define JB_FSCALAR 0x10000000 /* flag bits */
#define JB_FOBJECT 0x20000000
#define JB_FARRAY  0x40000000

/* convenience macros for accessing a JsonbContainer struct */
#define JsonContainerSize(jc)     ((jc)->header & JB_CMASK)
#define JsonContainerIsScalar(jc) (((jc)->header & JB_FSCALAR) != 0)
#define JsonContainerIsObject(jc) (((jc)->header & JB_FOBJECT) != 0)
#define JsonContainerIsArray(jc)  (((jc)->header & JB_FARRAY) != 0)

/* convenience macros for accessing the root container in a Jsonb datum */
#define JB_ROOT_COUNT(jbp_)     (*(uint32*)PG_PARSER_VARDATA(jbp_) & JB_CMASK)
#define JB_ROOT_IS_SCALAR(jbp_) ((*(uint32*)PG_PARSER_VARDATA(jbp_) & JB_FSCALAR) != 0)
#define JB_ROOT_IS_OBJECT(jbp_) ((*(uint32*)PG_PARSER_VARDATA(jbp_) & JB_FOBJECT) != 0)
#define JB_ROOT_IS_ARRAY(jbp_)  ((*(uint32*)PG_PARSER_VARDATA(jbp_) & JB_FARRAY) != 0)

#define JENTRY_OFFLENMASK       0x0FFFFFFF
#define JENTRY_TYPEMASK         0x70000000
#define JENTRY_HAS_OFF          0x80000000

/* values stored in the type bits */
#define JENTRY_ISSTRING       0x00000000
#define JENTRY_ISNUMERIC      0x10000000
#define JENTRY_ISBOOL_FALSE   0x20000000
#define JENTRY_ISBOOL_TRUE    0x30000000
#define JENTRY_ISNULL         0x40000000
#define JENTRY_ISCONTAINER    0x50000000 /* array or object */

#define JBE_OFFLENFLD(je_)    ((je_) & JENTRY_OFFLENMASK)
#define JBE_HAS_OFF(je_)      (((je_) & JENTRY_HAS_OFF) != 0)
#define JBE_ISSTRING(je_)     (((je_) & JENTRY_TYPEMASK) == JENTRY_ISSTRING)
#define JBE_ISNUMERIC(je_)    (((je_) & JENTRY_TYPEMASK) == JENTRY_ISNUMERIC)
#define JBE_ISCONTAINER(je_)  (((je_) & JENTRY_TYPEMASK) == JENTRY_ISCONTAINER)
#define JBE_ISNULL(je_)       (((je_) & JENTRY_TYPEMASK) == JENTRY_ISNULL)
#define JBE_ISBOOL_TRUE(je_)  (((je_) & JENTRY_TYPEMASK) == JENTRY_ISBOOL_TRUE)
#define JBE_ISBOOL_FALSE(je_) (((je_) & JENTRY_TYPEMASK) == JENTRY_ISBOOL_FALSE)
#define JBE_ISBOOL(je_)       (JBE_ISBOOL_TRUE(je_) || JBE_ISBOOL_FALSE(je_))

/* Macro for advancing an offset variable to the next JEntry */
#define JBE_ADVANCE_OFFSET(offset, je)      \
    do                                      \
    {                                       \
        JEntry je_ = (je);                  \
        if (JBE_HAS_OFF(je_))               \
            (offset) = JBE_OFFLENFLD(je_);  \
        else                                \
            (offset) += JBE_OFFLENFLD(je_); \
    } while (0)

#define IsAJsonbScalar(jsonbval) ((jsonbval)->type >= jbvNull && (jsonbval)->type <= jbvBool)

typedef struct JsonbContainer
{
    uint32_t header; /* number of elements or key/value pairs, and
                      * flags */
    JEntry   children[FLEXIBLE_ARRAY_MEMBER];

    /* the data for each child node follows. */
} JsonbContainer;

typedef struct
{
    int32_t        vl_len_; /* varlena header (do not touch directly!) */
    JsonbContainer root;
} Jsonb;

/*
 * JsonbIterator holds details of the type for each iteration. It also stores a
 * Jsonb varlena buffer, which can be directly accessed in some contexts.
 */
typedef enum
{
    JBI_ARRAY_START,
    JBI_ARRAY_ELEM,
    JBI_OBJECT_START,
    JBI_OBJECT_KEY,
    JBI_OBJECT_VALUE
} JsonbIterState;

typedef struct JsonbIterator
{
    /* Container being iterated */
    JsonbContainer*       container;
    uint32_t              nElems;   /* Number of elements in children array (will
                                     * be nPairs for objects) */
    bool                  isScalar; /* Pseudo-array scalar value? */
    JEntry*               children; /* JEntrys for child nodes */
    /* Data proper.  This points to the beginning of the variable-length data */
    char*                 dataProper;

    /* Current item in buffer (up to nElems) */
    int32_t               curIndex;

    /* Data offset corresponding to current item */
    uint32_t              curDataOffset;

    /*
     * If the container is an object, we want to return keys and values
     * alternately; so curDataOffset points to the current key, and
     * curValueOffset points to the current value.
     */
    uint32_t              curValueOffset;

    /* Private state */
    JsonbIterState        state;

    struct JsonbIterator* parent;
} JsonbIterator;

enum jbvType
{
    /* Scalar types */
    jbvNull = 0x0,
    jbvString,
    jbvNumeric,
    jbvBool,
    /* Composite types */
    jbvArray = 0x10,
    jbvObject,
    /* Binary (i.e. struct Jsonb) jbvArray/jbvObject */
    jbvBinary
};

typedef struct JsonbPair  JsonbPair;
typedef struct JsonbValue JsonbValue;

/*
 * JsonbValue:    In-memory representation of Jsonb.  This is a convenient
 * deserialized representation, that can easily support using the "val"
 * union across underlying types during manipulation.  The Jsonb on-disk
 * representation has various alignment considerations.
 */
struct JsonbValue
{
    enum jbvType type; /* Influences sort order */

    union
    {
        Numeric numeric;
        bool    boolean;
        struct
        {
            int32_t len;
            char*   val; /* Not necessarily null-terminated */
        } string;        /* String primitive type */

        struct
        {
            int32_t     nElems;
            JsonbValue* elems;
            bool        rawScalar; /* Top-level "raw scalar" array? */
        } array;                   /* Array container type */

        struct
        {
            int32_t    nPairs; /* 1 pair, 2 elements */
            JsonbPair* pairs;
        } object; /* Associative container type */

        struct
        {
            int32_t         len;
            JsonbContainer* data;
        } binary; /* Array or object, in on-disk format */
    } val;
};

struct JsonbPair
{
    JsonbValue key;   /* Must be a jbvString */
    JsonbValue value; /* May be of any type */
    uint32_t   order; /* Pair's index in original sequence */
};

/* Tokens used when sequentially processing a jsonb value */
typedef enum
{
    WJB_DONE,
    WJB_KEY,
    WJB_VALUE,
    WJB_ELEM,
    WJB_BEGIN_ARRAY,
    WJB_END_ARRAY,
    WJB_BEGIN_OBJECT,
    WJB_END_OBJECT
} JsonbIteratorToken;

/*
 * Initialize an iterator for iterating all elements in a container.
 */
static JsonbIterator* iteratorFromContainer(JsonbContainer* container, JsonbIterator* parent)
{
    JsonbIterator* it;

    pg_parser_mcxt_malloc(PGFUNC_JSONB_MCXT, (void**)&it, sizeof(JsonbIterator));
    it->container = container;
    it->parent = parent;
    it->nElems = JsonContainerSize(container);

    /* Array starts just after header */
    it->children = container->children;

    switch (container->header & (JB_FARRAY | JB_FOBJECT))
    {
        case JB_FARRAY:
            it->dataProper = (char*)it->children + it->nElems * sizeof(JEntry);
            it->isScalar = JsonContainerIsScalar(container);
            /* This is either a "raw scalar", or an array */

            it->state = JBI_ARRAY_START;
            break;

        case JB_FOBJECT:
            it->dataProper = (char*)it->children + it->nElems * sizeof(JEntry) * 2;
            it->state = JBI_OBJECT_START;
            break;

        default:
            break;
    }

    return it;
}

/*
 * Given a JsonbContainer, expand to JsonbIterator to iterate over items
 * fully expanded to in-memory representation for manipulation.
 *
 * See JsonbIteratorNext() for notes on memory management.
 */
static JsonbIterator* JsonbIteratorInit(JsonbContainer* container)
{
    return iteratorFromContainer(container, NULL);
}

/*
 * JsonbIteratorNext() worker:    Return parent, while freeing memory for current
 * iterator
 */
static JsonbIterator* freeAndGetParent(JsonbIterator* it)
{
    JsonbIterator* v = it->parent;

    pg_parser_mcxt_free(PGFUNC_JSONB_MCXT, it);
    return v;
}

/*
 * Get the offset of the variable-length portion of a Jsonb node within
 * the variable-length-data part of its container.  The node is identified
 * by index within the container's JEntry array.
 */
static uint32_t getJsonbOffset(const JsonbContainer* jc, int32_t index)
{
    uint32_t offset = 0;
    int32_t  i;

    /*
     * Start offset of this entry is equal to the end offset of the previous
     * entry.  Walk backwards to the most recent entry stored as an end
     * offset, returning that offset plus any lengths in between.
     */
    for (i = index - 1; i >= 0; i--)
    {
        offset += JBE_OFFLENFLD(jc->children[i]);
        if (JBE_HAS_OFF(jc->children[i]))
        {
            break;
        }
    }

    return offset;
}

/*
 * Get the length of the variable-length portion of a Jsonb node.
 * The node is identified by index within the container's JEntry array.
 */
static uint32_t getJsonbLength(const JsonbContainer* jc, int32_t index)
{
    uint32_t off;
    uint32_t len;

    /*
     * If the length is stored directly in the JEntry, just return it.
     * Otherwise, get the begin offset of the entry, and subtract that from
     * the stored end+1 offset.
     */
    if (JBE_HAS_OFF(jc->children[index]))
    {
        off = getJsonbOffset(jc, index);
        len = JBE_OFFLENFLD(jc->children[index]) - off;
    }
    else
    {
        len = JBE_OFFLENFLD(jc->children[index]);
    }

    return len;
}

/*
 * A helper function to fill in a JsonbValue to represent an element of an
 * array, or a key or value of an object.
 *
 * The node's JEntry is at container->children[index], and its variable-length
 * data is at base_addr + offset.  We make the caller determine the offset
 * since in many cases the caller can amortize that work across multiple
 * children.  When it can't, it can just call getJsonbOffset().
 *
 * A nested array or object will be returned as jbvBinary, ie. it won't be
 * expanded.
 */
static void fillJsonbValue(
    JsonbContainer* container, int32_t index, char* base_addr, uint32_t offset, JsonbValue* result)
{
    JEntry entry = container->children[index];

    if (JBE_ISNULL(entry))
    {
        result->type = jbvNull;
    }
    else if (JBE_ISSTRING(entry))
    {
        result->type = jbvString;
        result->val.string.val = base_addr + offset;
        result->val.string.len = getJsonbLength(container, index);
    }
    else if (JBE_ISNUMERIC(entry))
    {
        result->type = jbvNumeric;
        result->val.numeric = (Numeric)(base_addr + PG_PARSER_INTALIGN(offset));
    }
    else if (JBE_ISBOOL_TRUE(entry))
    {
        result->type = jbvBool;
        result->val.boolean = true;
    }
    else if (JBE_ISBOOL_FALSE(entry))
    {
        result->type = jbvBool;
        result->val.boolean = false;
    }
    else
    {
        result->type = jbvBinary;
        /* Remove alignment padding from data pointer and length */
        result->val.binary.data = (JsonbContainer*)(base_addr + PG_PARSER_INTALIGN(offset));
        result->val.binary.len =
            getJsonbLength(container, index) - (PG_PARSER_INTALIGN(offset) - offset);
    }
}

/*
 * Get next JsonbValue while iterating
 *
 * Caller should initially pass their own, original iterator.  They may get
 * back a child iterator palloc()'d here instead.  The function can be relied
 * on to free those child iterators, lest the memory allocated for highly
 * nested objects become unreasonable, but only if callers don't end iteration
 * early (by breaking upon having found something in a search, for example).
 *
 * Callers in such a scenario, that are particularly sensitive to leaking
 * memory in a long-lived context may walk the ancestral tree from the final
 * iterator we left them with to its oldest ancestor, pfree()ing as they go.
 * They do not have to free any other memory previously allocated for iterators
 * but not accessible as direct ancestors of the iterator they're last passed
 * back.
 *
 * Returns "Jsonb sequential processing" token value.  Iterator "state"
 * reflects the current stage of the process in a less granular fashion, and is
 * mostly used here to track things internally with respect to particular
 * iterators.
 *
 * Clients of this function should not have to handle any jbvBinary values
 * (since recursive calls will deal with this), provided skipNested is false.
 * It is our job to expand the jbvBinary representation without bothering them
 * with it.  However, clients should not take it upon themselves to touch array
 * or Object element/pair buffers, since their element/pair pointers are
 * garbage.  Also, *val will not be set when returning WJB_END_ARRAY or
 * WJB_END_OBJECT, on the assumption that it's only useful to access values
 * when recursing in.
 */
static JsonbIteratorToken JsonbIteratorNext(JsonbIterator** it, JsonbValue* val, bool skipNested)
{
    if (*it == NULL)
    {
        return WJB_DONE;
    }

    /*
     * When stepping into a nested container, we jump back here to start
     * processing the child. We will not recurse further in one call, because
     * processing the child will always begin in JBI_ARRAY_START or
     * JBI_OBJECT_START state.
     */
recurse:
    switch ((*it)->state)
    {
        case JBI_ARRAY_START:
            /* Set v to array on first array call */
            val->type = jbvArray;
            val->val.array.nElems = (*it)->nElems;

            /*
             * v->val.array.elems is not actually set, because we aren't doing
             * a full conversion
             */
            val->val.array.rawScalar = (*it)->isScalar;
            (*it)->curIndex = 0;
            (*it)->curDataOffset = 0;
            (*it)->curValueOffset = 0; /* not actually used */
            /* Set state for next call */
            (*it)->state = JBI_ARRAY_ELEM;
            return WJB_BEGIN_ARRAY;

        case JBI_ARRAY_ELEM:
            if ((*it)->curIndex >= (int32_t)((*it)->nElems))
            {
                /*
                 * All elements within array already processed.  Report this
                 * to caller, and give it back original parent iterator (which
                 * independently tracks iteration progress at its level of
                 * nesting).
                 */
                *it = freeAndGetParent(*it);
                return WJB_END_ARRAY;
            }

            fillJsonbValue(
                (*it)->container, (*it)->curIndex, (*it)->dataProper, (*it)->curDataOffset, val);

            JBE_ADVANCE_OFFSET((*it)->curDataOffset, (*it)->children[(*it)->curIndex]);
            (*it)->curIndex++;

            if (!IsAJsonbScalar(val) && !skipNested)
            {
                /* Recurse into container. */
                *it = iteratorFromContainer(val->val.binary.data, *it);
                goto recurse;
            }
            else
            {
                /*
                 * Scalar item in array, or a container and caller didn't want
                 * us to recurse into it.
                 */
                return WJB_ELEM;
            }

        case JBI_OBJECT_START:
            /* Set v to object on first object call */
            val->type = jbvObject;
            val->val.object.nPairs = (*it)->nElems;

            /*
             * v->val.object.pairs is not actually set, because we aren't
             * doing a full conversion
             */
            (*it)->curIndex = 0;
            (*it)->curDataOffset = 0;
            (*it)->curValueOffset = getJsonbOffset((*it)->container, (*it)->nElems);
            /* Set state for next call */
            (*it)->state = JBI_OBJECT_KEY;
            return WJB_BEGIN_OBJECT;

        case JBI_OBJECT_KEY:
            if ((*it)->curIndex >= (int32_t)((*it)->nElems))
            {
                /*
                 * All pairs within object already processed.  Report this to
                 * caller, and give it back original containing iterator
                 * (which independently tracks iteration progress at its level
                 * of nesting).
                 */
                *it = freeAndGetParent(*it);
                return WJB_END_OBJECT;
            }
            else
            {
                /* Return key of a key/value pair.  */
                fillJsonbValue((*it)->container,
                               (*it)->curIndex,
                               (*it)->dataProper,
                               (*it)->curDataOffset,
                               val);
                if (val->type != jbvString)
                {
                    /* todo error handling */
                }

                /* Set state for next call */
                (*it)->state = JBI_OBJECT_VALUE;
                return WJB_KEY;
            }

        case JBI_OBJECT_VALUE:
            /* Set state for next call */
            (*it)->state = JBI_OBJECT_KEY;

            fillJsonbValue((*it)->container,
                           (*it)->curIndex + (*it)->nElems,
                           (*it)->dataProper,
                           (*it)->curValueOffset,
                           val);

            JBE_ADVANCE_OFFSET((*it)->curDataOffset, (*it)->children[(*it)->curIndex]);
            JBE_ADVANCE_OFFSET((*it)->curValueOffset,
                               (*it)->children[(*it)->curIndex + (*it)->nElems]);
            (*it)->curIndex++;

            /*
             * Value may be a container, in which case we recurse with new,
             * child iterator (unless the caller asked not to, by passing
             * skipNested).
             */
            if (!IsAJsonbScalar(val) && !skipNested)
            {
                *it = iteratorFromContainer(val->val.binary.data, *it);
                goto recurse;
            }
            else
            {
                return WJB_VALUE;
            }
    }

    return -1;
}

static void add_indent(pg_parser_StringInfo out, bool indent, int32_t level)
{
    if (indent)
    {
        int32_t i;

        pg_parser_appendStringInfoCharMacro(out, '\n');
        for (i = 0; i < level; i++)
        {
            pg_parser_appendBinaryStringInfo(out, "    ", 4);
        }
    }
}

/*
 * Produce a JSON string literal, properly escaping characters in the text.
 */
static void escape_json(pg_parser_StringInfo buf, const char* str)
{
    const char* p;

    pg_parser_appendStringInfoCharMacro(buf, '"');
    for (p = str; *p; p++)
    {
        switch (*p)
        {
            case '\b':
                pg_parser_appendStringInfoString(buf, "\\b");
                break;
            case '\f':
                pg_parser_appendStringInfoString(buf, "\\f");
                break;
            case '\n':
                pg_parser_appendStringInfoString(buf, "\\n");
                break;
            case '\r':
                pg_parser_appendStringInfoString(buf, "\\r");
                break;
            case '\t':
                pg_parser_appendStringInfoString(buf, "\\t");
                break;
            case '"':
                pg_parser_appendStringInfoString(buf, "\\\"");
                break;
            case '\\':
                pg_parser_appendStringInfoString(buf, "\\\\");
                break;
            default:
                if ((unsigned char)*p < ' ')
                {
                    pg_parser_appendStringInfo(buf, "\\u%04x", (int32_t)*p);
                }
                else
                {
                    pg_parser_appendStringInfoCharMacro(buf, *p);
                }
                break;
        }
    }
    pg_parser_appendStringInfoCharMacro(buf, '"');
}

static void jsonb_put_escaped_value(pg_parser_StringInfo       out,
                                    JsonbValue*                scalarVal,
                                    pg_parser_extraTypoutInfo* info)
{
    bool  is_toast = false;
    char* temp_str = NULL;
    switch (scalarVal->type)
    {
        case jbvNull:
            pg_parser_appendBinaryStringInfo(out, "null", 4);
            break;
        case jbvString:
            temp_str = pg_parser_mcxt_strndup(scalarVal->val.string.val, scalarVal->val.string.len);
            escape_json(out, temp_str);
            pg_parser_mcxt_free(PGFUNC_JSONB_MCXT, temp_str);
            break;
        case jbvNumeric:
            temp_str = (char*)numeric_out((pg_parser_Datum)(scalarVal->val.numeric), info);
            if (is_toast)
            {
                pg_parser_log_errlog(info->zicinfo->debuglevel,
                                     "WARNING: unsupport toast numeric!\n");
                pg_parser_appendStringInfoString(out, "UNSUPPORT TOAST NUMERIC");
            }
            else
            {
                pg_parser_appendStringInfoString(out, temp_str);
            }
            pg_parser_mcxt_free(PGFUNC_JSONB_MCXT, temp_str);
            break;
        case jbvBool:
            if (scalarVal->val.boolean)
            {
                pg_parser_appendBinaryStringInfo(out, "true", 4);
            }
            else
            {
                pg_parser_appendBinaryStringInfo(out, "false", 5);
            }
            break;
        default:
            pg_parser_log_errlog(info->zicinfo->debuglevel, "WARNING: unknown jsonb type!\n");
    }
}

/*
 * common worker for above two functions
 */
static char* JsonbToCStringWorker(JsonbContainer*            in,
                                  int32_t                    estimated_len,
                                  bool                       indent,
                                  pg_parser_extraTypoutInfo* info)
{
    bool                 first = true;
    JsonbIterator*       it;
    JsonbValue           v;
    JsonbIteratorToken   type = WJB_DONE;
    int32_t              level = 0;
    bool                 redo_switch = false;

    /* If we are indenting, don't add a space after a comma */
    int32_t              ispaces = indent ? 1 : 2;

    /*
     * Don't indent the very first item. This gets set to the indent flag at
     * the bottom of the loop.
     */
    bool                 use_indent = false;
    bool                 raw_scalar = false;
    bool                 last_was_key = false;
    char*                result = NULL;
    pg_parser_StringInfo out = NULL;
    out = pg_parser_makeStringInfo();

    pg_parser_enlargeStringInfo(out, (estimated_len >= 0) ? estimated_len : 64);

    it = JsonbIteratorInit(in);

    while (redo_switch || ((type = JsonbIteratorNext(&it, &v, false)) != WJB_DONE))
    {
        redo_switch = false;
        switch (type)
        {
            case WJB_BEGIN_ARRAY:
                if (!first)
                {
                    pg_parser_appendBinaryStringInfo(out, ", ", ispaces);
                }

                if (!v.val.array.rawScalar)
                {
                    add_indent(out, use_indent && !last_was_key, level);
                    pg_parser_appendStringInfoCharMacro(out, '[');
                }
                else
                {
                    raw_scalar = true;
                }

                first = true;
                level++;
                break;
            case WJB_BEGIN_OBJECT:
                if (!first)
                {
                    pg_parser_appendBinaryStringInfo(out, ", ", ispaces);
                }

                add_indent(out, use_indent && !last_was_key, level);
                pg_parser_appendStringInfoCharMacro(out, '{');

                first = true;
                level++;
                break;
            case WJB_KEY:
                if (!first)
                {
                    pg_parser_appendBinaryStringInfo(out, ", ", ispaces);
                }
                first = true;

                add_indent(out, use_indent, level);

                /* json rules guarantee this is a string */
                jsonb_put_escaped_value(out, &v, info);
                pg_parser_appendBinaryStringInfo(out, ": ", 2);

                type = JsonbIteratorNext(&it, &v, false);
                if (type == WJB_VALUE)
                {
                    first = false;
                    jsonb_put_escaped_value(out, &v, info);
                }
                else
                {
                    /*
                     * We need to rerun the current switch() since we need to
                     * output the object which we just got from the iterator
                     * before calling the iterator again.
                     */
                    redo_switch = true;
                }
                break;
            case WJB_ELEM:
                if (!first)
                {
                    pg_parser_appendBinaryStringInfo(out, ", ", ispaces);
                }
                first = false;

                if (!raw_scalar)
                {
                    add_indent(out, use_indent, level);
                }
                jsonb_put_escaped_value(out, &v, info);
                break;
            case WJB_END_ARRAY:
                level--;
                if (!raw_scalar)
                {
                    add_indent(out, use_indent, level);
                    pg_parser_appendStringInfoCharMacro(out, ']');
                }
                first = false;
                break;
            case WJB_END_OBJECT:
                level--;
                add_indent(out, use_indent, level);
                pg_parser_appendStringInfoCharMacro(out, '}');
                first = false;
                break;
            default:
                pg_parser_log_errlog(info->zicinfo->debuglevel,
                                     "ERROR: unknown jsonb iterator token type\n");
        }
        use_indent = indent;
        last_was_key = redo_switch;
    }
    pg_parser_mcxt_malloc(PGFUNC_JSONB_MCXT, (void**)&result, out->len + 1);
    rmemcpy0(result, 0, out->data, out->len);

    if (out)
    {
        if (out->data)
        {
            pg_parser_mcxt_free(PGFUNC_JSONB_MCXT, out->data);
        }
        pg_parser_mcxt_free(PGFUNC_JSONB_MCXT, out);
    }

    return result;
}

static char* JsonbToCString(JsonbContainer*            in,
                            int32_t                    estimated_len,
                            pg_parser_extraTypoutInfo* info)
{
    return JsonbToCStringWorker(in, estimated_len, false, info);
}

/*
 * jsonb type output function
 */
pg_parser_Datum jsonb_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info)
{
    bool   is_toast = false;
    bool   need_free = false;
    Jsonb* jb = (Jsonb*)pg_parser_detoast_datum((struct pg_parser_varlena*)attr,
                                                &is_toast,
                                                &need_free,
                                                info->zicinfo->dbtype,
                                                info->zicinfo->dbversion);
    char*  out;

    if (is_toast)
    {
        info->valueinfo = INFO_COL_IS_TOAST;
        info->valuelen = sizeof(struct pg_parser_varatt_external);
        return (pg_parser_Datum)jb;
    }

    out = JsonbToCString(&jb->root, PG_PARSER_VARSIZE(jb), info);

    info->valuelen = strlen(out);

    if (need_free)
    {
        pg_parser_mcxt_free(PGFUNC_JSONB_MCXT, jb);
    }
    return (pg_parser_Datum)out;
}
