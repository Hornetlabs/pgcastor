/**
 * @file xk_pg_parser_thirdparty_tupleparser_geo_ops.c
 * @author bytesynch
 * @brief 
 * @version 0.1
 * @date 2023-08-04
 * 
 * @copyright Copyright (c) 2023
 * 
 */
#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "common/xk_pg_parser_translog.h"
#include "thirdparty/tupleparser/common/xk_pg_parser_thirdparty_tupleparser_pgfunc.h"
#include "thirdparty/tupleparser/common/xk_pg_parser_thirdparty_tupleparser_pgsfunc.h"
#include "thirdparty/common/xk_pg_parser_thirdparty_builtins.h"
#include "thirdparty/tupleparser/toast/xk_pg_parser_thirdparty_tupleparser_toast.h"
#include "thirdparty/stringinfo/xk_pg_parser_thirdparty_stringinfo.h"

#define PGFUNC_GEOS_MCXT NULL

typedef struct
{
    double x,
           y;
} Point;

typedef struct
{
    Point p[2];
} LSEG;

/*---------------------------------------------------------------------
 * PATH - Specified by vertex points.
 *-------------------------------------------------------------------*/
typedef struct
{
    int32_t        vl_len_;           /* varlena header (do not touch directly!) */
    int32_t        npts;
    int32_t        closed;            /* is this a closed polygon? */
    int32_t        dummy;             /* padding to make it double align */
    Point          p[FLEXIBLE_ARRAY_MEMBER];
} PATH;

typedef struct
{
    Point high,
          low;            /* corner POINTs */
} BOX;

/*---------------------------------------------------------------------
 * POLYGON - Specified by an array of doubles defining the points,
 *        keeping the number of points and the bounding box for
 *        speed purposes.
 *-------------------------------------------------------------------*/
typedef struct
{
    int32_t        vl_len_;        /* varlena header (do not touch directly!) */
    int32_t        npts;
    BOX          boundbox;
    Point        p[FLEXIBLE_ARRAY_MEMBER];
} POLYGON;

/*---------------------------------------------------------------------
 * CIRCLE - Specified by a center point and radius.
 *-------------------------------------------------------------------*/
typedef struct
{
    Point         center;
    double        radius;
} CIRCLE;

/*---------------------------------------------------------------------
 * LINE - Specified by its general equation (Ax+By+C=0).
 *-------------------------------------------------------------------*/
typedef struct
{
    double  A,
            B,
            C;
} LINE;

#define LDELIM            '('
#define RDELIM            ')'
#define DELIM             ','
#define LDELIM_EP         '['
#define RDELIM_EP         ']'
#define LDELIM_C          '<'
#define RDELIM_C          '>'
#define LDELIM_L          '{'
#define RDELIM_L          '}'

enum path_delim
{
    PATH_NONE, PATH_OPEN, PATH_CLOSED
};

static char *path_encode(enum path_delim path_delim, int32_t npts, Point *pt);
static void pair_encode(double x, double y, xk_pg_parser_StringInfo str);
static void single_encode(double x, xk_pg_parser_StringInfo str);
/***********************************************************************
 **
 **        Routines for 2D points.
 **
 ***********************************************************************/

xk_pg_parser_Datum point_out(xk_pg_parser_Datum attr)
{
    Point *pt = (Point *) attr;

    return (xk_pg_parser_Datum) path_encode(PATH_NONE, 1, pt);
}

/***********************************************************************
 **
 **        Routines for 2D line segments.
 **
 ***********************************************************************/

xk_pg_parser_Datum lseg_out(xk_pg_parser_Datum attr)
{
    LSEG       *ls = (LSEG *) attr;

    return (xk_pg_parser_Datum) path_encode(PATH_OPEN, 2, &ls->p[0]);
}


/***********************************************************************
 **
 **        Routines for 2D paths (sequences of line segments, also
 **                called `polylines').
 **
 **                This is not a general package for geometric paths,
 **                which of course include polygons; the emphasis here
 **                is on (for example) usefulness in wire layout.
 **
 ***********************************************************************/

xk_pg_parser_Datum path_out(xk_pg_parser_Datum attr,
                            xk_pg_parser_extraTypoutInfo *info)
{
    bool        is_toast = false;
    bool        need_free = false;
    char       *result = NULL;
    PATH       *path = (PATH *) xk_pg_parser_detoast_datum((struct xk_pg_parser_varlena *) attr,
                                                           &is_toast, 
                                                           &need_free,
                                                            info->zicinfo->dbtype,
                                                            info->zicinfo->dbversion);
    if (is_toast)
    {
        info->valueinfo = INFO_COL_IS_TOAST;
        info->valuelen = sizeof(struct xk_pg_parser_varatt_external);
        return (xk_pg_parser_Datum) path;
    }
    result = path_encode(path->closed ? PATH_CLOSED : PATH_OPEN, path->npts, path->p);
    info->valuelen = strlen(result);
    if (need_free)
        xk_pg_parser_mcxt_free(PGFUNC_GEOS_MCXT, path);
    return (xk_pg_parser_Datum) result;
}

/*        box_out -        convert a box to external form.
 */
xk_pg_parser_Datum box_out(xk_pg_parser_Datum attr)
{
    BOX *box = (BOX *) attr;

    return (xk_pg_parser_Datum) path_encode(PATH_NONE, 2, &(box->high));
}

/*---------------------------------------------------------------
 * poly_out - convert internal POLYGON representation to the
 *              character string format "((f8,f8),...,(f8,f8))"
 *---------------------------------------------------------------*/
xk_pg_parser_Datum poly_out(xk_pg_parser_Datum attr,
                            xk_pg_parser_extraTypoutInfo *info)
{
    bool        is_toast = false;
    bool        need_free = false;
    char       *result = NULL;
    POLYGON    *poly = (POLYGON *) xk_pg_parser_detoast_datum((struct xk_pg_parser_varlena *) attr,
                                                              &is_toast,
                                                              &need_free,
                                                               info->zicinfo->dbtype,
                                                               info->zicinfo->dbversion);
    if (is_toast)
    {
        info->valueinfo = INFO_COL_IS_TOAST;
        info->valuelen = sizeof(struct xk_pg_parser_varatt_external);
        return (xk_pg_parser_Datum) poly;
    }
    result = path_encode(PATH_CLOSED, poly->npts, poly->p);
    info->valuelen = strlen(result);
    if (need_free)
        xk_pg_parser_mcxt_free(PGFUNC_GEOS_MCXT, poly);
    return (xk_pg_parser_Datum) result;
}

static char *path_encode(enum path_delim path_delim, int32_t npts, Point *pt)
{
    xk_pg_parser_StringInfoData str;
    int32_t            i;
    char *result = NULL;

    xk_pg_parser_initStringInfo(&str);

    switch (path_delim)
    {
        case PATH_CLOSED:
            xk_pg_parser_appendStringInfoChar(&str, LDELIM);
            break;
        case PATH_OPEN:
            xk_pg_parser_appendStringInfoChar(&str, LDELIM_EP);
            break;
        case PATH_NONE:
            break;
    }

    for (i = 0; i < npts; i++)
    {
        if (i > 0)
            xk_pg_parser_appendStringInfoChar(&str, DELIM);
        xk_pg_parser_appendStringInfoChar(&str, LDELIM);
        pair_encode(pt->x, pt->y, &str);
        xk_pg_parser_appendStringInfoChar(&str, RDELIM);
        pt++;
    }

    switch (path_delim)
    {
        case PATH_CLOSED:
            xk_pg_parser_appendStringInfoChar(&str, RDELIM);
            break;
        case PATH_OPEN:
            xk_pg_parser_appendStringInfoChar(&str, RDELIM_EP);
            break;
        case PATH_NONE:
            break;
    }
    result = xk_pg_parser_mcxt_strdup(str.data);
    xk_pg_parser_mcxt_free(PGFUNC_GEOS_MCXT, str.data);
    return result;
}                                /* path_encode() */

static void pair_encode(double x, double y, xk_pg_parser_StringInfo str)
{
    char       *xstr = float8out_internal(x);
    char       *ystr = float8out_internal(y);
    if (xstr && ystr)
    {
        xk_pg_parser_appendStringInfo(str, "%s,%s", xstr, ystr);
        xk_pg_parser_mcxt_free(PGFUNC_GEOS_MCXT, xstr);
        xk_pg_parser_mcxt_free(PGFUNC_GEOS_MCXT, ystr);
    }
}

/*
 * Geometric data types are composed of points.
 * This code tries to support a common format throughout the data types,
 *    to allow for more predictable usage and data type conversion.
 * The fundamental unit is the point. Other units are line segments,
 *    open paths, boxes, closed paths, and polygons (which should be considered
 *    non-intersecting closed paths).
 *
 * Data representation is as follows:
 *    point:                (x,y)
 *    line segment:        [(x1,y1),(x2,y2)]
 *    box:                (x1,y1),(x2,y2)
 *    open path:            [(x1,y1),...,(xn,yn)]
 *    closed path:        ((x1,y1),...,(xn,yn))
 *    polygon:            ((x1,y1),...,(xn,yn))
 *
 * For boxes, the points are opposite corners with the first point at the top right.
 * For closed paths and polygons, the points should be reordered to allow
 *    fast and correct equality comparisons.
 *
 * XXX perhaps points in complex shapes should be reordered internally
 *    to allow faster internal operations, but should keep track of input order
 *    and restore that order for xk_pg_parser_text output - tgl 97/01/16
 */

static void single_encode(double x, xk_pg_parser_StringInfo str)
{
    char       *xstr = float8out_internal(x);
    if (xstr)
    {
        xk_pg_parser_appendStringInfoString(str, xstr);
        xk_pg_parser_mcxt_free(PGFUNC_GEOS_MCXT, xstr);
    }
}                                /* single_encode() */

/*        circle_out        -        convert a circle to external form.
 */
xk_pg_parser_Datum circle_out(xk_pg_parser_Datum attr)
{
    CIRCLE       *circle = (CIRCLE *) attr;
    char *result = NULL;
    xk_pg_parser_StringInfoData str;

    xk_pg_parser_initStringInfo(&str);

    xk_pg_parser_appendStringInfoChar(&str, LDELIM_C);
    xk_pg_parser_appendStringInfoChar(&str, LDELIM);
    pair_encode(circle->center.x, circle->center.y, &str);
    xk_pg_parser_appendStringInfoChar(&str, RDELIM);
    xk_pg_parser_appendStringInfoChar(&str, DELIM);
    single_encode(circle->radius, &str);
    xk_pg_parser_appendStringInfoChar(&str, RDELIM_C);

    result = xk_pg_parser_mcxt_strdup(str.data);
    xk_pg_parser_mcxt_free(PGFUNC_GEOS_MCXT, str.data);

    return (xk_pg_parser_Datum) result;
}

/***********************************************************************
 **
 **        Routines for 2D lines.
 **
 ***********************************************************************/

xk_pg_parser_Datum line_out(xk_pg_parser_Datum attr)
{
    LINE       *line = (LINE *) attr;
    char       *astr = float8out_internal(line->A);
    char       *bstr = float8out_internal(line->B);
    char       *cstr = float8out_internal(line->C);
    int32_t     len = strlen(astr) + strlen(bstr) + strlen(cstr);
    char       *result = NULL;
    if(!xk_pg_parser_mcxt_malloc(PGFUNC_GEOS_MCXT, (void**)&result, len + 5))
    {
        return (xk_pg_parser_Datum) 0;
    }
    sprintf(result, "%c%s%c%s%c%s%c", LDELIM_L, astr, DELIM, bstr, DELIM, cstr, RDELIM_L);
    if (astr)
        xk_pg_parser_mcxt_free(PGFUNC_GEOS_MCXT, astr);
    if (bstr)
        xk_pg_parser_mcxt_free(PGFUNC_GEOS_MCXT, bstr);
    if (cstr)
        xk_pg_parser_mcxt_free(PGFUNC_GEOS_MCXT, cstr);

    return (xk_pg_parser_Datum) result;
}
