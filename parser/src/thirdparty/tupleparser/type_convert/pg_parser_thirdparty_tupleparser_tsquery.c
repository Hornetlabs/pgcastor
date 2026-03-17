/**
 * @file xk_pg_parser_thirdparty_tupleparser_tsquery.c
 * @author bytesync
 * @brief 
 * @version 0.1
 * @date 2023-08-03
 * 
 * @copyright Copyright (c) 2023
 * 
 */
#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "thirdparty/tupleparser/common/xk_pg_parser_thirdparty_tupleparser_pgfunc.h"
#include "thirdparty/encoding/xk_pg_parser_thirdparty_encoding_wchar.h"

#define PGFUNC_TSQUERY_MCXT NULL
#define QI_VAL 1

#define OP_NOT           1
#define OP_AND           2
#define OP_OR            3
#define OP_PHRASE        4        /* highest code, tsquery_cleanup.c */
#define OP_COUNT         4

typedef struct
{
    int32_t vl_len_;        /* varlena header (do not touch directly!) */
    int32_t size;           /* number of QueryItems */
    char    data[FLEXIBLE_ARRAY_MEMBER];    /* data starts here */
} TSQueryData;

typedef TSQueryData *TSQuery;

typedef int8_t QueryItemType;

typedef struct
{
    QueryItemType type;
    int8_t        oper;            /* see above */
    int16_t       distance;        /* distance between agrs for OP_PHRASE */
    uint32_t      left;            /* pointer to left operand. Right operand is
                                    * item + 1, left operand is placed
                                    * item+item->left */
} QueryOperator;

typedef struct
{
    QueryItemType type;              /* operand or kind of operator (ts_tokentype) */
    uint8_t       weight;            /* weights of operand to search. It's a
                                      * bitmask of allowed weights. if it =0 then
                                      * any weight are allowed. Weights and bit
                                      * map: A: 1<<3 B: 1<<2 C: 1<<1 D: 1<<0 */
    bool          prefix;            /* true if it's a prefix search */
    int32_t       valcrc;            /* XXX: pg_crc32 would be a more appropriate
                                      * data type, but we use comparisons to signed
                                      * integers in the code. They would need to be
                                      * changed as well. */

    /* pointer to xk_pg_parser_text value of operand, must correlate with WordEntry */
    uint32_t
                  length:12,
                  distance:20;
} QueryOperand;

typedef union
{
    QueryItemType type;
    QueryOperator qoperator;
    QueryOperand qoperand;
} QueryItem;

typedef struct
{
    QueryItem  *curpol;
    char       *buf;
    char       *cur;
    char       *op;
    int32_t     buflen;
} INFIX;

#define HDRSIZETQ    ( XK_PG_PARSER_VARHDRSZ + sizeof(int32_t) )
/* Returns a pointer to the first QueryItem in a TSQuery */
#define GETQUERY(x)  ((QueryItem*)( (char*)(x)+HDRSIZETQ ))

/* Returns a pointer to the beginning of operands in a TSQuery */
#define GETOPERAND(x) ( (char*)GETQUERY(x) + ((TSQuery)(x))->size * sizeof(QueryItem) )

/* Makes sure inf->buf is large enough for adding 'addsize' bytes */
#define RESIZEBUF(inf, addsize) \
while( ( (inf)->cur - (inf)->buf ) + (addsize) + 1 >= (inf)->buflen ) \
{ \
    int32_t len = (inf)->cur - (inf)->buf; \
    (inf)->buflen *= 2; \
    xk_pg_parser_mcxt_realloc(PGFUNC_TSQUERY_MCXT,\
                             (void**) &((inf)->buf),\
                             (inf)->buflen ); \
    (inf)->cur = (inf)->buf + len; \
}

#define TOUCHAR(x) (*((const unsigned char *) (x)))

/* The second argument of t_iseq() must be a plain ASCII character */
#define t_iseq(x,c) (TOUCHAR(x) == (unsigned char) (c))
#define COPYCHAR(d,s) rmemcpy1(d, 0, s, xk_character_encoding_mblen(XK_CHARACTER_UTF8, s))

/* FTS operator priorities, see ts_type.h */
const int32_t tsearch_op_priority[OP_COUNT] =
{
    4,                            /* OP_NOT */
    2,                            /* OP_AND */
    1,                            /* OP_OR */
    3                            /* OP_PHRASE */
};

#define OP_PRIORITY(x) ( tsearch_op_priority[(x) - 1] )
#define QO_PRIORITY(x) OP_PRIORITY(((QueryOperator *) (x))->oper)


static void infix(INFIX *in, int32_t parentPriority, bool rightPhraseOp);

xk_pg_parser_Datum tsqueryout(xk_pg_parser_Datum attr)
{
    TSQuery      query = (TSQuery) attr;
    INFIX        nrm;

    if (query->size == 0)
        return (xk_pg_parser_Datum) 0;

    nrm.curpol = GETQUERY(query);
    nrm.buflen = 32;
    if (!xk_pg_parser_mcxt_malloc(PGFUNC_TSQUERY_MCXT,
                                 (void **) &(nrm.buf),
                                  sizeof(char) * nrm.buflen))
        return (xk_pg_parser_Datum) 0;
    nrm.cur = nrm.buf;
    *(nrm.cur) = '\0';
    nrm.op = GETOPERAND(query);
    infix(&nrm, -1 /* lowest priority */ , false);

    return (xk_pg_parser_Datum) nrm.buf;
}

/*
 * recursively traverse the tree and
 * print it in infix (human-readable) form
 */
static void infix(INFIX *in, int32_t parentPriority, bool rightPhraseOp)
{
    if (in->curpol->type == QI_VAL)
    {
        QueryOperand *curpol = &in->curpol->qoperand;
        char       *op = in->op + curpol->distance;
        int32_t            clen;

        RESIZEBUF(in, curpol->length * 
                 (xk_character_encoding_max_length(XK_CHARACTER_UTF8) + 1) + 2 + 6);

        *(in->cur) = '\'';
        in->cur++;
        while (*op)
        {
            if (t_iseq(op, '\''))
            {
                *(in->cur) = '\'';
                in->cur++;
            }
            else if (t_iseq(op, '\\'))
            {
                *(in->cur) = '\\';
                in->cur++;
            }
            COPYCHAR(in->cur, op);

            clen = xk_character_encoding_mblen(XK_CHARACTER_UTF8, op);
            op += clen;
            in->cur += clen;
        }
        *(in->cur) = '\'';
        in->cur++;
        if (curpol->weight || curpol->prefix)
        {
            *(in->cur) = ':';
            in->cur++;
            if (curpol->prefix)
            {
                *(in->cur) = '*';
                in->cur++;
            }
            if (curpol->weight & (1 << 3))
            {
                *(in->cur) = 'A';
                in->cur++;
            }
            if (curpol->weight & (1 << 2))
            {
                *(in->cur) = 'B';
                in->cur++;
            }
            if (curpol->weight & (1 << 1))
            {
                *(in->cur) = 'C';
                in->cur++;
            }
            if (curpol->weight & 1)
            {
                *(in->cur) = 'D';
                in->cur++;
            }
        }
        *(in->cur) = '\0';
        in->curpol++;
    }
    else if (in->curpol->qoperator.oper == OP_NOT)
    {
        int32_t            priority = QO_PRIORITY(in->curpol);

        if (priority < parentPriority)
        {
            RESIZEBUF(in, 2);
            sprintf(in->cur, "( ");
            in->cur = strchr(in->cur, '\0');
        }
        RESIZEBUF(in, 1);
        *(in->cur) = '!';
        in->cur++;
        *(in->cur) = '\0';
        in->curpol++;

        infix(in, priority, false);
        if (priority < parentPriority)
        {
            RESIZEBUF(in, 2);
            sprintf(in->cur, " )");
            in->cur = strchr(in->cur, '\0');
        }
    }
    else
    {
        int8_t      op = in->curpol->qoperator.oper;
        int32_t     priority = QO_PRIORITY(in->curpol);
        int16_t     distance = in->curpol->qoperator.distance;
        INFIX       nrm;
        bool        needParenthesis = false;

        in->curpol++;
        if (priority < parentPriority ||
        /* phrase operator depends on order */
            (op == OP_PHRASE && rightPhraseOp))
        {
            needParenthesis = true;
            RESIZEBUF(in, 2);
            sprintf(in->cur, "( ");
            in->cur = strchr(in->cur, '\0');
        }

        nrm.curpol = in->curpol;
        nrm.op = in->op;
        nrm.buflen = 16;
        xk_pg_parser_mcxt_malloc(PGFUNC_TSQUERY_MCXT,
                                     (void **) &(nrm.buf),
                                      sizeof(char) * nrm.buflen);
        nrm.cur = nrm.buf;

        /* get right operand */
        infix(&nrm, priority, (op == OP_PHRASE));

        /* get & print left operand */
        in->curpol = nrm.curpol;
        infix(in, priority, false);

        /* print operator & right operand */
        RESIZEBUF(in, 3 + (2 + 10 /* distance */ ) + (nrm.cur - nrm.buf));
        switch (op)
        {
            case OP_OR:
                sprintf(in->cur, " | %s", nrm.buf);
                break;
            case OP_AND:
                sprintf(in->cur, " & %s", nrm.buf);
                break;
            case OP_PHRASE:
                if (distance != 1)
                    sprintf(in->cur, " <%d> %s", distance, nrm.buf);
                else
                    sprintf(in->cur, " <-> %s", nrm.buf);
                break;
            default:
                /* OP_NOT is handled in above if-branch */
                /* elog(ERROR, "unrecognized operator type: %d", op); */
                break;
        }
        in->cur = strchr(in->cur, '\0');
        xk_pg_parser_mcxt_free(PGFUNC_TSQUERY_MCXT, nrm.buf);

        if (needParenthesis)
        {
            RESIZEBUF(in, 2);
            sprintf(in->cur, " )");
            in->cur = strchr(in->cur, '\0');
        }
    }
}