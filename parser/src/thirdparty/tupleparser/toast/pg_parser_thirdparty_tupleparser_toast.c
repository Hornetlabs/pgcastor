#include <lz4.h>
#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "thirdparty/tupleparser/toast/pg_parser_thirdparty_tupleparser_toast.h"
#include "thirdparty/compress/pg_parser_thirdparty_lzcompress.h"

#define TOAST_MCXT NULL

#define PG_PARSER_VARATT_EXTERNAL_GET_POINTER0(toast_pointer, attr)                              \
    do                                                                                           \
    {                                                                                            \
        pg_parser_varattrib_1b_e* attre = (pg_parser_varattrib_1b_e*)(attr);                     \
        rmemcpy0(&(toast_pointer), 0, PG_PARSER_VARDATA_EXTERNAL(attre), sizeof(toast_pointer)); \
    } while (0)

#define PG_PARSER_VARATT_EXTERNAL_GET_POINTER1(toast_pointer, attr)                              \
    do                                                                                           \
    {                                                                                            \
        pg_parser_varattrib_1b_e* attre = (pg_parser_varattrib_1b_e*)(attr);                     \
        rmemcpy1(&(toast_pointer), 0, PG_PARSER_VARDATA_EXTERNAL(attre), sizeof(toast_pointer)); \
    } while (0)

static struct pg_parser_varlena* toast_decompress_datum(struct pg_parser_varlena* attr,
                                                        bool*                     need_free);
static struct pg_parser_varlena* toast_decompress_datum_pg14(struct pg_parser_varlena* attr,
                                                             bool*                     need_free);

struct pg_parser_varlena* pg_parser_detoast_datum(
    struct pg_parser_varlena* datum, bool* is_toast, bool* need_free, int dbtype, char* dbversion)
{
    if (PG_PARSER_VARATT_IS_EXTENDED(datum))
    {
        return pg_parser_heap_tuple_untoast_attr(datum, is_toast, need_free, dbtype, dbversion);
    }
    else
    {
        return datum;
    }
}

struct pg_parser_varlena* pg_parser_heap_tuple_fetch_attr(struct pg_parser_varlena* attr,
                                                          bool*                     is_toast)
{
    struct pg_parser_varlena* result;

    if (PG_PARSER_VARATT_IS_EXTERNAL_ONDISK(attr))
    {
        /* Get structure for toast storage */
        struct pg_parser_varatt_external* toast_pointer;
        if (!pg_parser_mcxt_malloc(
                TOAST_MCXT, (void**)(&toast_pointer), sizeof(struct pg_parser_varatt_external)))
        {
            return NULL;
        }
        PG_PARSER_VARATT_EXTERNAL_GET_POINTER0(*toast_pointer, attr);
        *is_toast = true;
        return (struct pg_parser_varlena*)toast_pointer;
    }
    else if (PG_PARSER_VARATT_IS_EXTERNAL_INDIRECT(attr))
    {
        /*
         * This is an indirect pointer --- dereference it
         */
        struct pg_parser_varatt_indirect redirect;

        PG_PARSER_VARATT_EXTERNAL_GET_POINTER1(redirect, attr);
        attr = (struct pg_parser_varlena*)redirect.pointer;

        /* recurse if value is still external in some other way */
        if (PG_PARSER_VARATT_IS_EXTERNAL(attr))
        {
            return pg_parser_heap_tuple_fetch_attr(attr, is_toast);
        }

        /*
         * Copy into the caller's memory context, in case caller tries to
         * pfree the result.
         */
        if (!pg_parser_mcxt_malloc(
                TOAST_MCXT, (void**)(&result), (int32_t)PG_PARSER_VARSIZE_ANY(attr)))
        {
            return NULL;
        }
        rmemcpy0(result, 0, attr, PG_PARSER_VARSIZE_ANY(attr));
    }
    else if (PG_PARSER_VARATT_IS_EXTERNAL_EXPANDED(attr))
    {
        /* TOAST pointer for extended memory storage, should not enter here, if entered, report
         * error */
        /* todo error handling */
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
 * pg_parser_heap_tuple_untoast_attr -
 *
 *    Public entry point to get back a toasted value from compression
 *    or external storage.  The result is always non-extended varlena form.
 *
 * Note some callers assume that if the input is an EXTERNAL or COMPRESSED
 * datum, the result will be a pfree'able chunk.
 * ----------
 */
struct pg_parser_varlena* pg_parser_heap_tuple_untoast_attr(
    struct pg_parser_varlena* attr, bool* is_toast, bool* need_free, int dbtype, char* dbversion)
{
    if (PG_PARSER_VARATT_IS_EXTERNAL_ONDISK(attr))
    {
        /* Get structure for toast storage */
        struct pg_parser_varatt_external* toast_pointer;
        if (!pg_parser_mcxt_malloc(
                TOAST_MCXT, (void**)(&toast_pointer), sizeof(struct pg_parser_varatt_external)))
        {
            return NULL;
        }
        PG_PARSER_VARATT_EXTERNAL_GET_POINTER0(*toast_pointer, attr);
        *is_toast = true;
        return (struct pg_parser_varlena*)toast_pointer;
    }
    else if (PG_PARSER_VARATT_IS_EXTERNAL_INDIRECT(attr))
    {
        /*
         * This is an indirect pointer --- dereference it
         */
        struct pg_parser_varatt_indirect redirect;

        PG_PARSER_VARATT_EXTERNAL_GET_POINTER1(redirect, attr);
        attr = (struct pg_parser_varlena*)redirect.pointer;

        /* recurse in case value is still extended in some other way */
        attr = pg_parser_heap_tuple_untoast_attr(attr, is_toast, need_free, dbtype, dbversion);

        /* if it isn't, we'd better copy it */
        if (attr == (struct pg_parser_varlena*)redirect.pointer)
        {
            struct pg_parser_varlena* result;
            if (!pg_parser_mcxt_malloc(TOAST_MCXT, (void**)(&result), PG_PARSER_VARSIZE_ANY(attr)))
            {
                return NULL;
            }
            rmemcpy0(result, 0, attr, PG_PARSER_VARSIZE_ANY(attr));
            *need_free = true;
            attr = result;
        }
    }
    else if (PG_PARSER_VARATT_IS_EXTERNAL_EXPANDED(attr))
    {
        /*
         * This is an expanded-object pointer --- get flat format
         */
        attr = pg_parser_heap_tuple_fetch_attr(attr, is_toast);
    }
    else if (PG_PARSER_VARATT_IS_COMPRESSED(attr))
    {
        /*
         * This is a compressed value inside of the main tuple
         */
        switch (dbtype)
        {
            case DATABASE_TYPE_POSTGRESQL:
            {
                if (!strcmp(DATABASE_PG1410, dbversion))
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
    else if (PG_PARSER_VARATT_IS_SHORT(attr))
    {
        /*
         * This is a short-header varlena --- convert to 4-byte header format
         */
        size_t data_size = PG_PARSER_VARSIZE_SHORT(attr) - PG_PARSER_VARHDRSZ_SHORT;
        size_t new_size = data_size + PG_PARSER_VARHDRSZ;
        struct pg_parser_varlena* new_attr;
        if (!pg_parser_mcxt_malloc(TOAST_MCXT, (void**)(&new_attr), new_size))
        {
            return NULL;
        }

        PG_PARSER_SET_VARSIZE(new_attr, new_size);
        rmemcpy1(PG_PARSER_VARDATA(new_attr), 0, PG_PARSER_VARDATA_SHORT(attr), data_size);
        *need_free = true;
        attr = new_attr;
    }

    return (void*)attr;
}

struct pg_parser_varlena* pg_parser_detoast_datum_packed(
    struct pg_parser_varlena* datum, bool* is_toast, bool* need_free, int dbtype, char* dbversion)
{
    if (PG_PARSER_VARATT_IS_COMPRESSED(datum) || PG_PARSER_VARATT_IS_EXTERNAL(datum))
    {
        return pg_parser_heap_tuple_untoast_attr(datum, is_toast, need_free, dbtype, dbversion);
    }
    else
    {
        return datum;
    }
}

static struct pg_parser_varlena* toast_decompress_datum(struct pg_parser_varlena* attr,
                                                        bool*                     need_free)
{
    struct pg_parser_varlena* result;

    if (!pg_parser_mcxt_malloc(TOAST_MCXT,
                               (void**)(&result),
                               PG_PARSER_TOAST_COMPRESS_RAWSIZE(attr) + PG_PARSER_VARHDRSZ))
    {
        return NULL;
    }

    PG_PARSER_SET_VARSIZE(result, PG_PARSER_TOAST_COMPRESS_RAWSIZE(attr) + PG_PARSER_VARHDRSZ);

    if (pg_parser_lz_decompress(PG_PARSER_TOAST_COMPRESS_RAWDATA(attr),
                                PG_PARSER_VARSIZE(attr) - PG_PARSER_TOAST_COMPRESS_HDRSZ,
                                PG_PARSER_VARDATA(result),
                                PG_PARSER_TOAST_COMPRESS_RAWSIZE(attr),
                                true) < 0)
    {
        return NULL;
    }
    *need_free = true;
    return result;
}

static struct pg_parser_varlena* pg_parser_pg14_pglz_decompress_datum(
    const struct pg_parser_varlena* value)
{
    struct pg_parser_varlena* result;
    int32_t                   rawsize;
    /* allocate memory for the uncompressed data */
    if (!pg_parser_mcxt_malloc(
            TOAST_MCXT,
            (void**)(&result),
            PG_PARSER_PG14_VARDATA_COMPRESSED_GET_EXTSIZE(value) + PG_PARSER_VARHDRSZ))
    {
        return NULL;
    }

    /* decompress the data */
    rawsize = pg_parser_pg14_pglz_decompress(
        (char*)value + PG_PARSER_PG14_VARHDRSZ_COMPRESSED,
        PG_PARSER_VARSIZE(value) - PG_PARSER_PG14_VARHDRSZ_COMPRESSED,
        PG_PARSER_VARDATA(result),
        PG_PARSER_PG14_VARDATA_COMPRESSED_GET_EXTSIZE(value),
        true);
    if (rawsize < 0)
    {
        return NULL;
    }

    PG_PARSER_SET_VARSIZE(result, rawsize + PG_PARSER_VARHDRSZ);
    return result;
}

/*
 * Decompress a varlena that was compressed using LZ4.
 */
static struct pg_parser_varlena* pg_parser_pg14_lz4_decompress_datum(
    const struct pg_parser_varlena* value)
{
    int32_t                   rawsize;
    struct pg_parser_varlena* result;

    /* allocate memory for the uncompressed data */
    if (!pg_parser_mcxt_malloc(
            TOAST_MCXT,
            (void**)(&result),
            PG_PARSER_PG14_VARDATA_COMPRESSED_GET_EXTSIZE(value) + PG_PARSER_VARHDRSZ))
    {
        return NULL;
    }

    /* decompress the data */
    rawsize = LZ4_decompress_safe((char*)value + PG_PARSER_PG14_VARHDRSZ_COMPRESSED,
                                  PG_PARSER_VARDATA(result),
                                  PG_PARSER_VARSIZE(value) - PG_PARSER_PG14_VARHDRSZ_COMPRESSED,
                                  PG_PARSER_PG14_VARDATA_COMPRESSED_GET_EXTSIZE(value));
    if (rawsize < 0)
    {
        return NULL;
    }

    PG_PARSER_SET_VARSIZE(result, rawsize + PG_PARSER_VARHDRSZ);

    return result;
}

static struct pg_parser_varlena* toast_decompress_datum_pg14(struct pg_parser_varlena* attr,
                                                             bool*                     need_free)
{
    struct pg_parser_varlena*    result;
    pg_parser_ToastCompressionId cmid;

    /*
     * Fetch the compression method id stored in the compression header and
     * decompress the data using the appropriate decompression routine.
     */
    cmid = PG_PARSER_PG14_TOAST_COMPRESS_METHOD(attr);
    switch (cmid)
    {
        case PG_PARSER_PG14_TOAST_PGLZ_COMPRESSION_ID:
            result = pg_parser_pg14_pglz_decompress_datum(attr);
            break;
        case PG_PARSER_PG14_TOAST_LZ4_COMPRESSION_ID:
            result = pg_parser_pg14_lz4_decompress_datum(attr);
            break;
        default:
            return NULL; /* keep compiler quiet */
    }

    *need_free = true;
    return result;
}
