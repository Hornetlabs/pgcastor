/**
 * @file xk_pg_parser_thirdparty_tupleparser_tsvector.c
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
#include "common/xk_pg_parser_translog.h"
#include "thirdparty/tupleparser/common/xk_pg_parser_thirdparty_tupleparser_pgsfunc.h"
#include "thirdparty/encoding/xk_pg_parser_thirdparty_encoding_wchar.h"
#include "thirdparty/tupleparser/toast/xk_pg_parser_thirdparty_tupleparser_toast.h"

#define PGFUNC_TSVECTOR_MCXT NULL

#define TOUCHAR(x)	(*((const unsigned char *) (x)))
/* The second argument of t_iseq() must be a plain ASCII character */
#define t_iseq(x,c) (TOUCHAR(x) == (unsigned char) (c))

typedef struct
{
    uint32_t
                haspos:1,
                len:11,            /* MAX 2Kb */
                pos:20;            /* MAX 1Mb */
} WordEntry;

/* This struct represents a complete tsvector datum */
typedef struct
{
    int32_t        vl_len_;        /* varlena header (do not touch directly!) */
    int32_t        size;
    WordEntry      entries[FLEXIBLE_ARRAY_MEMBER];
    /* lexemes follow the entries[] array */
} TSVectorData;

typedef TSVectorData *TSVector;

typedef uint16_t WordEntryPos;

typedef struct
{
    uint16_t        npos;
    WordEntryPos pos[FLEXIBLE_ARRAY_MEMBER];
} WordEntryPosVector;

#define ARRPTR(x) ( (x)->entries )
#define STRPTR(x)    ( (char *) &(x)->entries[(x)->size] )

#define _POSVECPTR(x, e) ((WordEntryPosVector *)(STRPTR(x) \
                          + XK_PG_PARSER_SHORTALIGN((e)->pos + (e)->len)))

#define POSDATALEN(x,e) ( ( (e)->haspos ) ? (_POSVECPTR(x,e)->npos) : 0 )
#define POSDATAPTR(x,e) (_POSVECPTR(x,e)->pos)

#define WEP_GETPOS(x) ( (x) & 0x3fff )
#define WEP_GETWEIGHT(x) ( (x) >> 14 )

xk_pg_parser_Datum tsvectorout(xk_pg_parser_Datum attr,
                               xk_pg_parser_extraTypoutInfo *info)
{
    bool        is_toast = false;
    bool        need_free = false;
    TSVector    out = (TSVector) xk_pg_parser_detoast_datum((struct xk_pg_parser_varlena *) attr,
                                                            &is_toast,
                                                            &need_free,
                                                             info->zicinfo->dbtype,
                                                             info->zicinfo->dbversion);
    char       *outbuf;
    int32_t     i,
                lenbuf = 0,
                pp;
    WordEntry  *ptr = ARRPTR(out);
    char       *curbegin,
               *curin,
               *curout;

    if (is_toast)
    {
        info->valueinfo = INFO_COL_IS_TOAST;
        info->valuelen = sizeof(struct xk_pg_parser_varatt_external);
        return (xk_pg_parser_Datum) out;
    }

    lenbuf = out->size * 2 /* '' */ + out->size - 1 /* space */ + 2 /* \0 */ ;
    for (i = 0; i < out->size; i++)
    {
        lenbuf += ptr[i].len * 2 * xk_character_encoding_max_length(XK_CHARACTER_UTF8) /* for escape */ ;
        if (ptr[i].haspos)
            lenbuf += 1 /* : */ + 7 /* int2 + , + weight */ * POSDATALEN(out, &(ptr[i]));
    }
    if (!xk_pg_parser_mcxt_malloc(PGFUNC_TSVECTOR_MCXT, (void**) &outbuf, lenbuf))
        return (xk_pg_parser_Datum) 0;

    curout = outbuf;
    for (i = 0; i < out->size; i++)
    {
        curbegin = curin = STRPTR(out) + ptr->pos;
        if (i != 0)
            *curout++ = ' ';
        *curout++ = '\'';
        while (curin - curbegin < ptr->len)
        {
            int32_t len = xk_character_encoding_mblen(XK_CHARACTER_UTF8, curin);

            if (t_iseq(curin, '\''))
                *curout++ = '\'';
            else if (t_iseq(curin, '\\'))
                *curout++ = '\\';

            while (len--)
                *curout++ = *curin++;
        }

        *curout++ = '\'';
        if ((pp = POSDATALEN(out, ptr)) != 0)
        {
            WordEntryPos *wptr;

            *curout++ = ':';
            wptr = POSDATAPTR(out, ptr);
            while (pp)
            {
                curout += sprintf(curout, "%d", WEP_GETPOS(*wptr));
                switch (WEP_GETWEIGHT(*wptr))
                {
                    case 3:
                        *curout++ = 'A';
                        break;
                    case 2:
                        *curout++ = 'B';
                        break;
                    case 1:
                        *curout++ = 'C';
                        break;
                    case 0:
                    default:
                        break;
                }

                if (pp > 1)
                    *curout++ = ',';
                pp--;
                wptr++;
            }
        }
        ptr++;
    }

    *curout = '\0';
    info->valuelen = strlen(outbuf);
    if (need_free)
        xk_pg_parser_mcxt_free(PGFUNC_TSVECTOR_MCXT, out);
    return (xk_pg_parser_Datum) outbuf;
}