#include <lz4.h>
#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "common/xk_pg_parser_translog.h"
#include "thirdparty/tupleparser/toast/xk_pg_parser_thirdparty_tupleparser_toast.h"
#include "thirdparty/compress/xk_pg_parser_thirdparty_lzcompress.h"

#define TOAST_MCXT NULL

#define XK_PG_PARSER_VARATT_EXTERNAL_GET_POINTER0(toast_pointer, attr) \
do { \
    xk_pg_parser_varattrib_1b_e *attre = (xk_pg_parser_varattrib_1b_e *) (attr); \
    rmemcpy0(&(toast_pointer), 0, XK_PG_PARSER_VARDATA_EXTERNAL(attre), sizeof(toast_pointer)); \
} while (0)

#define XK_PG_PARSER_VARATT_EXTERNAL_GET_POINTER1(toast_pointer, attr) \
do { \
    xk_pg_parser_varattrib_1b_e *attre = (xk_pg_parser_varattrib_1b_e *) (attr); \
    rmemcpy1(&(toast_pointer), 0, XK_PG_PARSER_VARDATA_EXTERNAL(attre), sizeof(toast_pointer)); \
} while (0)

static struct xk_pg_parser_varlena *toast_decompress_datum(struct xk_pg_parser_varlena *attr,
                                                           bool *need_free);
static struct xk_pg_parser_varlena *toast_decompress_datum_pg14(struct xk_pg_parser_varlena *attr,
                                                           bool *need_free);

struct xk_pg_parser_varlena *xk_pg_parser_detoast_datum(struct xk_pg_parser_varlena *datum, bool *is_toast, bool *need_free, int dbtype, char *dbversion)
{
    if (XK_PG_PARSER_VARATT_IS_EXTENDED(datum))
        return xk_pg_parser_heap_tuple_untoast_attr(datum, is_toast, need_free, dbtype, dbversion);
    else
        return datum;
}

struct xk_pg_parser_varlena *
xk_pg_parser_heap_tuple_fetch_attr(struct xk_pg_parser_varlena *attr, bool *is_toast)
{
    struct xk_pg_parser_varlena *result;

    if (XK_PG_PARSER_VARATT_IS_EXTERNAL_ONDISK(attr))
    {
        /* 获取toast存储的结构体 */
        struct xk_pg_parser_varatt_external *toast_pointer;
        if (!xk_pg_parser_mcxt_malloc(TOAST_MCXT,
                                (void**)(&toast_pointer),
                                 sizeof(struct xk_pg_parser_varatt_external)))
            return NULL;
        XK_PG_PARSER_VARATT_EXTERNAL_GET_POINTER0(*toast_pointer, attr);
        *is_toast = true;
        return (struct xk_pg_parser_varlena *)toast_pointer;
    }
    else if (XK_PG_PARSER_VARATT_IS_EXTERNAL_INDIRECT(attr))
    {
        /*
         * This is an indirect pointer --- dereference it
         */
        struct xk_pg_parser_varatt_indirect redirect;

        XK_PG_PARSER_VARATT_EXTERNAL_GET_POINTER1(redirect, attr);
        attr = (struct xk_pg_parser_varlena *) redirect.pointer;

        /* recurse if value is still external in some other way */
        if (XK_PG_PARSER_VARATT_IS_EXTERNAL(attr))
            return xk_pg_parser_heap_tuple_fetch_attr(attr, is_toast);

        /*
         * Copy into the caller's memory context, in case caller tries to
         * pfree the result.
         */
        if (!xk_pg_parser_mcxt_malloc(TOAST_MCXT,
                                     (void**)(&result),
                                      (int32_t) XK_PG_PARSER_VARSIZE_ANY(attr)))
            return NULL;
        rmemcpy0(result, 0, attr, XK_PG_PARSER_VARSIZE_ANY(attr));
    }
    else if (XK_PG_PARSER_VARATT_IS_EXTERNAL_EXPANDED(attr))
    {
        /* 内存扩展存储的TOAST指针, 应该不会进入这里，如果进入这里，报错 */
        //printf("WARNING: VARATT IS EXTERNAL EXPANDED\n");
        return NULL;
    }
    else
    {
        /*
         * This is a plain value inside of the main tuple - why am I called?
         */
        result = attr;
    }

    return result;
}

/* ----------
 * xk_pg_parser_heap_tuple_untoast_attr -
 *
 *    Public entry point to get back a toasted value from compression
 *    or external storage.  The result is always non-extended varlena form.
 *
 * Note some callers assume that if the input is an EXTERNAL or COMPRESSED
 * datum, the result will be a pfree'able chunk.
 * ----------
 */
struct xk_pg_parser_varlena *xk_pg_parser_heap_tuple_untoast_attr(struct xk_pg_parser_varlena *attr,
                                                                  bool *is_toast,
                                                                  bool *need_free,
                                                                  int dbtype,
                                                                  char *dbversion)
{
    if (XK_PG_PARSER_VARATT_IS_EXTERNAL_ONDISK(attr))
    {
        /* 获取toast存储的结构体 */
        struct xk_pg_parser_varatt_external *toast_pointer;
        if (!xk_pg_parser_mcxt_malloc(TOAST_MCXT,
                                (void**)(&toast_pointer),
                                 sizeof(struct xk_pg_parser_varatt_external)))
            return NULL;
        XK_PG_PARSER_VARATT_EXTERNAL_GET_POINTER0(*toast_pointer, attr);
        *is_toast = true;
        return (struct xk_pg_parser_varlena *)toast_pointer;
    }
    else if (XK_PG_PARSER_VARATT_IS_EXTERNAL_INDIRECT(attr))
    {
        /*
         * This is an indirect pointer --- dereference it
         */
        struct xk_pg_parser_varatt_indirect redirect;

        XK_PG_PARSER_VARATT_EXTERNAL_GET_POINTER1(redirect, attr); 
        attr = (struct xk_pg_parser_varlena *) redirect.pointer;

        /* recurse in case value is still extended in some other way */
        attr = xk_pg_parser_heap_tuple_untoast_attr(attr, is_toast, need_free, dbtype, dbversion);

        /* if it isn't, we'd better copy it */
        if (attr == (struct xk_pg_parser_varlena *) redirect.pointer)
        {
            struct xk_pg_parser_varlena *result;
            if (!xk_pg_parser_mcxt_malloc(TOAST_MCXT,
                                         (void**)(&result),
                                          XK_PG_PARSER_VARSIZE_ANY(attr)))
                return NULL;
            rmemcpy0(result, 0, attr, XK_PG_PARSER_VARSIZE_ANY(attr));
            *need_free = true;
            attr = result;
        }
    }
    else if (XK_PG_PARSER_VARATT_IS_EXTERNAL_EXPANDED(attr))
    {
        /*
         * This is an expanded-object pointer --- get flat format
         */
        attr = xk_pg_parser_heap_tuple_fetch_attr(attr, is_toast);
    }
    else if (XK_PG_PARSER_VARATT_IS_COMPRESSED(attr))
    {
        /*
         * This is a compressed value inside of the main tuple
         */
        switch (dbtype)
        {
            case XK_DATABASE_TYPE_POSTGRESQL:
            {
                if (!strcmp(XK_DATABASE_PG1410, dbversion))
                {
                    attr = toast_decompress_datum_pg14(attr, need_free);
                }
                else
                {
                    attr = toast_decompress_datum(attr, need_free);
                }
                break;
            }
            case XK_DATABASE_TYPE_HGDB:
            {
                if (!strcmp(XK_DATABASE_HGDBV901, dbversion))
                {
                    attr = toast_decompress_datum_pg14(attr, need_free);
                }
                else
                {
                    attr = toast_decompress_datum(attr, need_free);
                }
                break;
            }
            default:
            {
                attr = toast_decompress_datum(attr, need_free);
                break;
            }
        }
    }
    else if (XK_PG_PARSER_VARATT_IS_SHORT(attr))
    {
        /*
         * This is a short-header varlena --- convert to 4-byte header format
         */
        size_t        data_size = XK_PG_PARSER_VARSIZE_SHORT(attr) - XK_PG_PARSER_VARHDRSZ_SHORT;
        size_t        new_size = data_size + XK_PG_PARSER_VARHDRSZ;
        struct xk_pg_parser_varlena *new_attr;
        if (!xk_pg_parser_mcxt_malloc(TOAST_MCXT,
                                     (void**)(&new_attr),
                                      new_size))
            return NULL;

        XK_PG_PARSER_SET_VARSIZE(new_attr, new_size);
        rmemcpy1(XK_PG_PARSER_VARDATA(new_attr), 0, XK_PG_PARSER_VARDATA_SHORT(attr), data_size);
        *need_free = true;
        attr = new_attr;
    }

    return (void*) attr;
}

struct xk_pg_parser_varlena *xk_pg_parser_detoast_datum_packed(struct xk_pg_parser_varlena *datum, bool *is_toast, bool *need_free, int dbtype, char *dbversion)
{
    if (XK_PG_PARSER_VARATT_IS_COMPRESSED(datum) || XK_PG_PARSER_VARATT_IS_EXTERNAL(datum))
        return xk_pg_parser_heap_tuple_untoast_attr(datum, is_toast, need_free, dbtype, dbversion);
    else
        return datum;
}

static struct xk_pg_parser_varlena *toast_decompress_datum(struct xk_pg_parser_varlena *attr,
                                                           bool *need_free)
{
    struct xk_pg_parser_varlena *result;

    if (!xk_pg_parser_mcxt_malloc(TOAST_MCXT,
                                     (void**)(&result),
                                      XK_PG_PARSER_TOAST_COMPRESS_RAWSIZE(attr) + XK_PG_PARSER_VARHDRSZ))
            return NULL;

    XK_PG_PARSER_SET_VARSIZE(result, XK_PG_PARSER_TOAST_COMPRESS_RAWSIZE(attr) + XK_PG_PARSER_VARHDRSZ);

    if (xk_pg_parser_lz_decompress(XK_PG_PARSER_TOAST_COMPRESS_RAWDATA(attr),
                        XK_PG_PARSER_VARSIZE(attr) - XK_PG_PARSER_TOAST_COMPRESS_HDRSZ,
                        XK_PG_PARSER_VARDATA(result),
                        XK_PG_PARSER_TOAST_COMPRESS_RAWSIZE(attr), true) < 0)
        return NULL;
    *need_free = true;
    return result;
}

static struct xk_pg_parser_varlena *xk_pg_parser_pg14_pglz_decompress_datum(const struct xk_pg_parser_varlena *value)
{
    struct xk_pg_parser_varlena *result;
    int32_t rawsize;
    /* allocate memory for the uncompressed data */
    if (!xk_pg_parser_mcxt_malloc(TOAST_MCXT,
                                        (void**)(&result),
                                        XK_PG_PARSER_PG14_VARDATA_COMPRESSED_GET_EXTSIZE(value)
                                      + XK_PG_PARSER_VARHDRSZ))
        return NULL;

    /* decompress the data */
    rawsize = xk_pg_parser_pg14_pglz_decompress((char *) value + XK_PG_PARSER_PG14_VARHDRSZ_COMPRESSED,
                            XK_PG_PARSER_VARSIZE(value) - XK_PG_PARSER_PG14_VARHDRSZ_COMPRESSED,
                            XK_PG_PARSER_VARDATA(result),
                            XK_PG_PARSER_PG14_VARDATA_COMPRESSED_GET_EXTSIZE(value), true);
    if (rawsize < 0)
        return NULL;

    XK_PG_PARSER_SET_VARSIZE(result, rawsize + XK_PG_PARSER_VARHDRSZ);
    return result;
}

/*
 * Decompress a varlena that was compressed using LZ4.
 */
static struct xk_pg_parser_varlena *xk_pg_parser_pg14_lz4_decompress_datum(const struct xk_pg_parser_varlena *value)
{
    int32_t         rawsize;
    struct xk_pg_parser_varlena *result;

    /* allocate memory for the uncompressed data */
    if (!xk_pg_parser_mcxt_malloc(TOAST_MCXT,
                                        (void**)(&result),
                                        XK_PG_PARSER_PG14_VARDATA_COMPRESSED_GET_EXTSIZE(value)
                                      + XK_PG_PARSER_VARHDRSZ))
        return NULL;

    /* decompress the data */
    rawsize = LZ4_decompress_safe((char *) value + XK_PG_PARSER_PG14_VARHDRSZ_COMPRESSED,
                                  XK_PG_PARSER_VARDATA(result),
                                  XK_PG_PARSER_VARSIZE(value) - XK_PG_PARSER_PG14_VARHDRSZ_COMPRESSED,
                                  XK_PG_PARSER_PG14_VARDATA_COMPRESSED_GET_EXTSIZE(value));
    if (rawsize < 0)
        return NULL;

    XK_PG_PARSER_SET_VARSIZE(result, rawsize + XK_PG_PARSER_VARHDRSZ);

    return result;
}

static struct xk_pg_parser_varlena *toast_decompress_datum_pg14(struct xk_pg_parser_varlena *attr,
                                                           bool *need_free)
{
    struct xk_pg_parser_varlena *result;
    xk_pg_parser_ToastCompressionId cmid;

    /*
    * Fetch the compression method id stored in the compression header and
    * decompress the data using the appropriate decompression routine.
    */
    cmid = XK_PG_PARSER_PG14_TOAST_COMPRESS_METHOD(attr);
    switch (cmid)
    {
        case XK_PG_PARSER_PG14_TOAST_PGLZ_COMPRESSION_ID:
            result = xk_pg_parser_pg14_pglz_decompress_datum(attr);
            break;
        case XK_PG_PARSER_PG14_TOAST_LZ4_COMPRESSION_ID:
            result = xk_pg_parser_pg14_lz4_decompress_datum(attr);
            break;
        default:
            return NULL;        /* keep compiler quiet */
    }

    *need_free = true;
    return result;
}
