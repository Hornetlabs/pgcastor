/**
 * @file pg_parser_thirdparty_tupleparser_geo_ops.c
 * @author bytesynch
 * @brief 
 * @version 0.1
 * @date 2023-08-04
 * 
 * @copyright Copyright (c) 2023
 * 
 */
#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_pgfunc.h"
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_pgsfunc.h"
#include "thirdparty/common/pg_parser_thirdparty_builtins.h"
#include "thirdparty/tupleparser/toast/pg_parser_thirdparty_tupleparser_toast.h"
#include "thirdparty/stringinfo/pg_parser_thirdparty_stringinfo.h"

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
static void pair_encode(double x, double y, pg_parser_StringInfo str);
static void single_encode(double x, pg_parser_StringInfo str);
/***********************************************************************
 **
 **        Routines for 2D points.
 **
 ***********************************************************************/

pg_parser_Datum point_out(pg_parser_Datum attr)
{
    Point *pt = (Point *) attr;

    return (pg_parser_Datum) path_encode(PATH_NONE, 1, pt);
}

/***********************************************************************
 **
 **        Routines for 2D line segments.
 **
 ***********************************************************************/

pg_parser_Datum lseg_out(pg_parser_Datum attr)
{
    LSEG       *ls = (LSEG *) attr;

    return (pg_parser_Datum) path_encode(PATH_OPEN, 2, &ls->p[0]);
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

pg_parser_Datum path_out(pg_parser_Datum attr,
                            pg_parser_extraTypoutInfo *info)
{
    bool        is_toast = false;
    bool        need_free = false;
    char       *result = NULL;
    PATH       *path = (PATH *) pg_parser_detoast_datum((struct pg_parser_varlena *) attr,
                                                           &is_toast, 
                                                           &need_free,
                                                            info->zicinfo->dbtype,
                                                            info->zicinfo->dbversion);
    if (is_toast)
    {
        info->valueinfo = INFO_COL_IS_TOAST;
        info->valuelen = sizeof(struct pg_parser_varatt_external);
        return (pg_parser_Datum) path;
    }
    result = path_encode(path->closed ? PATH_CLOSED : PATH_OPEN, path->npts, path->p);
    info->valuelen = strlen(result);
    if (need_free)
        pg_parser_mcxt_free(PGFUNC_GEOS_MCXT, path);
    return (pg_parser_Datum) result;
}

/*        box_out -        convert a box to external form.
 */
pg_parser_Datum box_out(pg_parser_Datum attr)
{
    BOX *box = (BOX *) attr;

    return (pg_parser_Datum) path_encode(PATH_NONE, 2, &(box->high));
}

/*---------------------------------------------------------------
 * poly_out - convert internal POLYGON representation to the
 *              character string format "((f8,f8),...,(f8,f8))"
 *---------------------------------------------------------------*/
pg_parser_Datum poly_out(pg_parser_Datum attr,
                            pg_parser_extraTypoutInfo *info)
{
    bool        is_toast = false;
    bool        need_free = false;
    char       *result = NULL;
    POLYGON    *poly = (POLYGON *) pg_parser_detoast_datum((struct pg_parser_varlena *) attr,
                                                              &is_toast,
                                                              &need_free,
                                                               info->zicinfo->dbtype,
                                                               info->zicinfo->dbversion);
    if (is_toast)
    {
        info->valueinfo = INFO_COL_IS_TOAST;
        info->valuelen = sizeof(struct pg_parser_varatt_external);
        return (pg_parser_Datum) poly;
    }
    result = path_encode(PATH_CLOSED, poly->npts, poly->p);
    info->valuelen = strlen(result);
    if (need_free)
        pg_parser_mcxt_free(PGFUNC_GEOS_MCXT, poly);
    return (pg_parser_Datum) result;
}

static char *path_encode(enum path_delim path_delim, int32_t npts, Point *pt)
{
    pg_parser_StringInfoData str;
    int32_t            i;
    char *result = NULL;

    pg_parser_initStringInfo(&str);

    switch (path_delim)
    {
        case PATH_CLOSED:
            pg_parser_appendStringInfoChar(&str, LDELIM);
            break;
        case PATH_OPEN:
            pg_parser_appendStringInfoChar(&str, LDELIM_EP);
            break;
        case PATH_NONE:
            break;
    }

    for (i = 0; i < npts; i++)
    {
        if (i > 0)
            pg_parser_appendStringInfoChar(&str, DELIM);
        pg_parser_appendStringInfoChar(&str, LDELIM);
        pair_encode(pt->x, pt->y, &str);
        pg_parser_appendStringInfoChar(&str, RDELIM);
        pt++;
    }

    switch (path_delim)
    {
        case PATH_CLOSED:
            pg_parser_appendStringInfoChar(&str, RDELIM);
            break;
        case PATH_OPEN:
            pg_parser_appendStringInfoChar(&str, RDELIM_EP);
            break;
        case PATH_NONE:
            break;
    }
    result = pg_parser_mcxt_strdup(str.data);
    pg_parser_mcxt_free(PGFUNC_GEOS_MCXT, str.data);
    return result;
}                                /* path_encode() */

static void pair_encode(double x, double y, pg_parser_StringInfo str)
{
    char       *xstr = float8out_internal(x);
    char       *ystr = float8out_internal(y);
    if (xstr && ystr)
    {
        pg_parser_appendStringInfo(str, "%s,%s", xstr, ystr);
        pg_parser_mcxt_free(PGFUNC_GEOS_MCXT, xstr);
        pg_parser_mcxt_free(PGFUNC_GEOS_MCXT, ystr);
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
 *    and restore that order for pg_parser_text output - tgl 97/01/16
 */

static void single_encode(double x, pg_parser_StringInfo str)
{
    char       *xstr = float8out_internal(x);
    if (xstr)
    {
        pg_parser_appendStringInfoString(str, xstr);
        pg_parser_mcxt_free(PGFUNC_GEOS_MCXT, xstr);
    }
}                                /* single_encode() */

/*        circle_out        -        convert a circle to external form.
 */
pg_parser_Datum circle_out(pg_parser_Datum attr)
{
    CIRCLE       *circle = (CIRCLE *) attr;
    char *result = NULL;
    pg_parser_StringInfoData str;

    pg_parser_initStringInfo(&str);

    pg_parser_appendStringInfoChar(&str, LDELIM_C);
    pg_parser_appendStringInfoChar(&str, LDELIM);
    pair_encode(circle->center.x, circle->center.y, &str);
    pg_parser_appendStringInfoChar(&str, RDELIM);
    pg_parser_appendStringInfoChar(&str, DELIM);
    single_encode(circle->radius, &str);
    pg_parser_appendStringInfoChar(&str, RDELIM_C);

    result = pg_parser_mcxt_strdup(str.data);
    pg_parser_mcxt_free(PGFUNC_GEOS_MCXT, str.data);

    return (pg_parser_Datum) result;
}

/***********************************************************************
 **
 **        Routines for 2D lines.
 **
 ***********************************************************************/

pg_parser_Datum line_out(pg_parser_Datum attr)
{
    LINE       *line = (LINE *) attr;
    char       *astr = float8out_internal(line->A);
    char       *bstr = float8out_internal(line->B);
    char       *cstr = float8out_internal(line->C);
    int32_t     len = strlen(astr) + strlen(bstr) + strlen(cstr);
    char       *result = NULL;
    if(!pg_parser_mcxt_malloc(PGFUNC_GEOS_MCXT, (void**)&result, len + 5))
    {
        return (pg_parser_Datum) 0;
    }
    sprintf(result, "%c%s%c%s%c%s%c", LDELIM_L, astr, DELIM, bstr, DELIM, cstr, RDELIM_L);
    if (astr)
        pg_parser_mcxt_free(PGFUNC_GEOS_MCXT, astr);
    if (bstr)
        pg_parser_mcxt_free(PGFUNC_GEOS_MCXT, bstr);
    if (cstr)
        pg_parser_mcxt_free(PGFUNC_GEOS_MCXT, cstr);

    return (pg_parser_Datum) result;
}
