/**
 * @file pg_parser_thirdparty_tupleparser_array.c
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
#include "sysdict/pg_parser_sysdict_getinfo.h"
#include "trans/transrec/pg_parser_trans_transrec_itemptr.h"
#include "trans/transrec/pg_parser_trans_transrec_heaptuple.h"
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_fmgr.h"

#define PGFUNC_ARRAY_MCXT NULL

#define MAXDIM 6
#define MaxAllocSize ((size_t)0x3fffffff) /* 1 gigabyte - 1 */

static pg_parser_Datum array_out_assemble(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

typedef struct array_iter
{
    /* datumptr being NULL or not tells if we have flat or expanded array */

    /* Fields used when we have an expanded array */
    pg_parser_Datum* datumptr;  /* Pointer to pg_parser_Datum array */
    bool*            isnullptr; /* Pointer to isnull array */

    /* Fields used when we have a flat array */
    char*    dataptr;   /* Current spot in the data area */
    uint8_t* bitmapptr; /* Current byte of the nulls bitmap, or NULL */
    int32_t  bitmask;   /* mask for current bit in nulls bitmap */
} array_iter;

typedef struct
{
    int32_t  vl_len_;    /* varlena header (do not touch directly!) */
    int32_t  ndim;       /* # of dimensions */
    int32_t  dataoffset; /* offset to data, or 0 if no bitmap */
    uint32_t elemtype;   /* element type OID */
} ArrayType;

#define ARR_SIZE(a) PG_PARSER_VARSIZE(a)
#define ARR_NDIM(a) ((a)->ndim)
#define ARR_HASNULL(a) ((a)->dataoffset != 0)
#define ARR_ELEMTYPE(a) ((a)->elemtype)
#define ARR_DIMS(a) ((int32_t*)(((char*)(a)) + sizeof(ArrayType)))
#define ARR_LBOUND(a) ((int32_t*)(((char*)(a)) + sizeof(ArrayType) + sizeof(int32_t) * ARR_NDIM(a)))

#define ARR_NULLBITMAP(a)                                                                   \
    (ARR_HASNULL(a)                                                                         \
         ? (uint8_t*)(((char*)(a)) + sizeof(ArrayType) + 2 * sizeof(int32_t) * ARR_NDIM(a)) \
         : (uint8_t*)NULL)

#define ARR_OVERHEAD_NONULLS(ndims) \
    PG_PARSER_MAXALIGN(sizeof(ArrayType) + 2 * sizeof(int32_t) * (ndims))

#define ARR_OVERHEAD_WITHNULLS(ndims, nitems) \
    PG_PARSER_MAXALIGN(sizeof(ArrayType) + 2 * sizeof(int32_t) * (ndims) + ((nitems) + 7) / 8)

#define ARR_DATA_OFFSET(a) \
    (ARR_HASNULL(a) ? (uint32_t)((a)->dataoffset) : ARR_OVERHEAD_NONULLS(ARR_NDIM(a)))

/*
 * Returns a pointer to the actual array data.
 */
#define ARR_DATA_PTR(a) (((char*)(a)) + ARR_DATA_OFFSET(a))
static int32_t ArrayGetNItems(int32_t ndim, const int32_t* dims)
{
    int32_t ret;
    int32_t i;

#define MaxArraySize ((size_t)(MaxAllocSize / sizeof(pg_parser_Datum)))

    if (ndim <= 0)
    {
        return 0;
    }
    ret = 1;
    for (i = 0; i < ndim; i++)
    {
        int64_t prod;

        /* A negative dimension implies that UB-LB overflowed ... */
        if (dims[i] < 0)
        {
            return 0;
        }

        prod = (int64_t)ret * (int64_t)dims[i];

        ret = (int32_t)prod;
        if ((int64_t)ret != prod)
        {
            return 0;
        }
    }
    if ((size_t)ret > MaxArraySize)
    {
        return 0;
    }
    return (int32_t)ret;
}

static inline void array_iter_setup(array_iter* it, ArrayType* a)
{
    {
        it->datumptr = NULL;
        it->isnullptr = NULL;
        it->dataptr = ARR_DATA_PTR((ArrayType*)a);
        it->bitmapptr = ARR_NULLBITMAP((ArrayType*)a);
    }
    it->bitmask = 1;
}

static inline pg_parser_Datum array_iter_next(array_iter* it, bool* isnull, int32_t i,
                                              int32_t elmlen, bool elmbyval, char elmalign)
{
    pg_parser_Datum ret;

    if (it->datumptr)
    {
        ret = it->datumptr[i];
        *isnull = it->isnullptr ? it->isnullptr[i] : false;
    }
    else
    {
        if (it->bitmapptr && (*(it->bitmapptr) & it->bitmask) == 0)
        {
            *isnull = true;
            ret = (pg_parser_Datum)0;
        }
        else
        {
            *isnull = false;
            ret = pg_parser_fetch_att(it->dataptr, elmbyval, elmlen);
            it->dataptr = pg_parser_att_addlength_pointer(it->dataptr, elmlen, it->dataptr);
            it->dataptr = (char*)pg_parser_att_align_nominal(it->dataptr, elmalign);
        }
        it->bitmask <<= 1;
        if (it->bitmask == 0x100)
        {
            if (it->bitmapptr)
            {
                it->bitmapptr++;
            }
            it->bitmask = 1;
        }
    }

    return ret;
}

static bool array_isspace(char ch)
{
    if (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\v' || ch == '\f')
    {
        return true;
    }
    return false;
}

// todo Big-endian and Little-endian
static int32_t pg_strcasecmp(const char* s1, const char* s2)
{
    for (;;)
    {
        unsigned char ch1 = (unsigned char)*s1++;
        unsigned char ch2 = (unsigned char)*s2++;

        if (ch1 != ch2)
        {
            if (ch1 >= 'A' && ch1 <= 'Z')
            {
                ch1 += 'a' - 'A';
            }
            // else if (IS_HIGHBIT_SET(ch1) && isupper(ch1))
            //     ch1 = tolower(ch1);

            if (ch2 >= 'A' && ch2 <= 'Z')
            {
                ch2 += 'a' - 'A';
            }
            // else if (IS_HIGHBIT_SET(ch2) && isupper(ch2))
            //     ch2 = tolower(ch2);

            if (ch1 != ch2)
            {
                return (int32_t)ch1 - (int32_t)ch2;
            }
        }
        if (ch1 == 0)
        {
            break;
        }
    }
    return 0;
}

#define ASSGN "="

/*
 * array_out :
 *           takes the internal representation of an array and returns a string
 *          containing the array in its external format.
 */
pg_parser_Datum array_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info)
{
    bool       is_toast = false;
    bool       need_free = false;
    ArrayType* v =
        (ArrayType*)pg_parser_detoast_datum((struct pg_parser_varlena*)attr, &is_toast, &need_free,
                                            info->zicinfo->dbtype, info->zicinfo->dbversion);
    uint32_t                  element_type = 0;
    int32_t                   typlen;
    bool                      typbyval;
    char                      typalign;
    char                      typdelim;
    char *                    p, *tmp, *retval, **values, dims_str[(MAXDIM * 33) + 2];
    pg_parser_sysdict_pgtype* sys_type = NULL;
    /*
     * 33 per dim since we assume 15 digits per number + ':' +'[]'
     *
     * +2 allows for assignment operator + trailing null
     */
    bool *     needquotes, needdims = false;
    size_t     overall_length;
    int32_t    nitems, i, j, k, indx[MAXDIM];
    int32_t    ndim, *dims, *lb;
    array_iter iter;

    if (is_toast)
    {
        /* Out-of-line storage array parsing is not yet supported, because subsequent parsing
         * requires system tables, but out-of-line storage parsing interface does not pass in system
         * tables */
        if (info != NULL)
        {
            info->valueinfo = INFO_COL_MAY_NULL;
        }
        info->valuelen = 0;
        return (pg_parser_Datum)v;
    }
    element_type = v->elemtype;

    sys_type = pg_parser_sysdict_getSysdictType(element_type, info->sysdicts);
    if (!sys_type)
    {
        pg_parser_log_errlog(info->zicinfo->debuglevel, "ERROR: sys_type is NULL\n");
        return (pg_parser_Datum)0;
    }
    typlen = sys_type->typlen;
    typbyval = sys_type->typbyval;
    typalign = sys_type->typalign;
    typdelim = sys_type->typdelim;

    ndim = ARR_NDIM(v);
    dims = ARR_DIMS(v);
    lb = ARR_LBOUND(v);
    nitems = ArrayGetNItems(ndim, dims);

    if (nitems == 0)
    {
        retval = pg_parser_mcxt_strdup("{}");
        return (pg_parser_Datum)retval;
    }

    /*
     * we will need to add explicit dimensions if any dimension has a lower
     * bound other than one
     */
    for (i = 0; i < ndim; i++)
    {
        if (lb[i] != 1)
        {
            needdims = true;
            break;
        }
    }

    /*
     * Convert all values to string form, count total space needed (including
     * any overhead such as escaping backslashes), and detect whether each
     * item needs double quotes.
     */
    if (!pg_parser_mcxt_malloc(PGFUNC_ARRAY_MCXT, (void**)&values, nitems * sizeof(char*)))
    {
        return (pg_parser_Datum)0;
    }

    if (!pg_parser_mcxt_malloc(PGFUNC_ARRAY_MCXT, (void**)&needquotes, nitems * sizeof(bool)))
    {
        return (pg_parser_Datum)0;
    }

    overall_length = 0;

    array_iter_setup(&iter, v);

    for (i = 0; i < nitems; i++)
    {
        pg_parser_Datum itemvalue;
        bool            isnull;
        bool            needquote;

        /* Get source element, checking for NULL */
        itemvalue = array_iter_next(&iter, &isnull, i, typlen, typbyval, typalign);

        if (isnull)
        {
            values[i] = pg_parser_mcxt_strdup("NULL");
            overall_length += 4;
            needquote = false;
        }
        else
        {
            if (PG_SYSDICT_TYPTYPE_BASE == sys_type->typtype ||
                PG_SYSDICT_TYPTYPE_DOMAIN == sys_type->typtype ||
                PG_SYSDICT_TYPTYPE_ENUM == sys_type->typtype ||
                PG_SYSDICT_TYPTYPE_RANGE == sys_type->typtype)
            {
                bool istoast = false;

                values[i] = pg_parser_convert_attr_to_str_char(
                    itemvalue, info->sysdicts, element_type, &istoast, info->zicinfo);
                if (!values[i])
                {
                    return (pg_parser_Datum)0;
                }
                /* First do simple processing for non-toast storage */
                if (!is_toast)
                {
                    /* count data plus backslashes; detect chars needing quotes */
                    if (values[i][0] == '\0')
                    {
                        needquote = true; /* force quotes for empty string */
                    }
                    else if (pg_strcasecmp(values[i], "NULL") == 0)
                    {
                        needquote = true; /* force quotes for literal NULL */
                    }
                    else
                    {
                        needquote = false;
                    }

                    for (tmp = values[i]; *tmp != '\0'; tmp++)
                    {
                        char ch = *tmp;

                        overall_length += 1;
                        if (ch == '"' || ch == '\\')
                        {
                            needquote = true;
                            overall_length += 1;
                        }
                        else if (ch == '{' || ch == '}' || ch == typdelim || array_isspace(ch))
                        {
                            needquote = true;
                        }
                    }
                }
                else
                {
                    goto pg_parser_thirdparty_tupleparser_array_clean_and_do_deep_assemble;
                }
            }
            else if (PG_SYSDICT_TYPTYPE_COMPOSITE == sys_type->typtype)
            {
                goto pg_parser_thirdparty_tupleparser_array_clean_and_do_deep_assemble;
            }
            /* This will not be reached, just in case, throw an error */
            else
            {
                return (pg_parser_Datum)0;
            }
        }

        needquotes[i] = needquote;

        /* Count the pair of double quotes, if needed */
        if (needquote)
        {
            overall_length += 2;
        }
        /* and the comma (or other typdelim delimiter) */
        overall_length += 1;
    }

    /*
     * The very last array element doesn't have a typdelim delimiter after it,
     * but that's OK; that space is needed for the trailing '\0'.
     *
     * Now count total number of curly brace pairs in output string.
     */
    for (i = j = 0, k = 1; i < ndim; i++)
    {
        j += k, k *= dims[i];
    }
    overall_length += 2 * j;

    /* Format explicit dimensions if required */
    dims_str[0] = '\0';
    if (needdims)
    {
        char* ptr = dims_str;

        for (i = 0; i < ndim; i++)
        {
            sprintf(ptr, "[%d:%d]", lb[i], lb[i] + dims[i] - 1);
            ptr += strlen(ptr);
        }
        *ptr++ = *ASSGN;
        *ptr = '\0';
        overall_length += ptr - dims_str;
    }

    /* Now construct the output string */
    if (!pg_parser_mcxt_malloc(PGFUNC_ARRAY_MCXT, (void**)&retval, overall_length))
    {
        return (pg_parser_Datum)0;
    }
    p = retval;

#define APPENDSTR(str) (strcpy(p, (str)), p += strlen(p))
#define APPENDCHAR(ch) (*p++ = (ch), *p = '\0')

    if (needdims)
    {
        APPENDSTR(dims_str);
    }
    APPENDCHAR('{');
    for (i = 0; i < ndim; i++)
    {
        indx[i] = 0;
    }
    j = 0;
    k = 0;
    do
    {
        for (i = j; i < ndim - 1; i++)
        {
            APPENDCHAR('{');
        }

        if (needquotes[k])
        {
            APPENDCHAR('"');
            for (tmp = values[k]; *tmp; tmp++)
            {
                char ch = *tmp;

                if (ch == '"' || ch == '\\')
                {
                    *p++ = '\\';
                }
                *p++ = ch;
            }
            *p = '\0';
            APPENDCHAR('"');
        }
        else
        {
            APPENDSTR(values[k]);
        }
        pg_parser_mcxt_free(PGFUNC_ARRAY_MCXT, values[k++]);

        for (i = ndim - 1; i >= 0; i--)
        {
            if (++(indx[i]) < dims[i])
            {
                APPENDCHAR(typdelim);
                break;
            }
            else
            {
                indx[i] = 0;
                APPENDCHAR('}');
            }
        }
        j = i;
    } while (j != -1);

#undef APPENDSTR
#undef APPENDCHAR

    /* Assert that we calculated the string length accurately */

    pg_parser_mcxt_free(PGFUNC_ARRAY_MCXT, values);
    pg_parser_mcxt_free(PGFUNC_ARRAY_MCXT, needquotes);
    if (need_free)
    {
        pg_parser_mcxt_free(PGFUNC_ARRAY_MCXT, v);
    }
    info->valuelen = strlen(retval);
    return (pg_parser_Datum)retval;
pg_parser_thirdparty_tupleparser_array_clean_and_do_deep_assemble:
    pg_parser_mcxt_free(PGFUNC_ARRAY_MCXT, values);
    pg_parser_mcxt_free(PGFUNC_ARRAY_MCXT, needquotes);
    if (need_free)
    {
        pg_parser_mcxt_free(PGFUNC_ARRAY_MCXT, v);
    }
    return array_out_assemble(attr, info);
}

/* When array type cannot be simply returned as string, we need to assemble it */
static pg_parser_Datum array_out_assemble(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info)
{
    // todo
    PG_PARSER_UNUSED(attr);
    PG_PARSER_UNUSED(info);
#if 0
    bool is_toast = false;
    bool need_free = false;
    ArrayType *v = (ArrayType *) pg_parser_detoast_datum((struct pg_parser_varlena *) attr,
                                                             &is_toast,
                                                             &need_free);
    uint32_t    element_type = 0;
    int32_t     typlen;
    bool        typbyval;
    char        typalign;
    char        typdelim;
    char       *p,
               *tmp,
               *retval,
              **values,
                dims_str[(MAXDIM * 33) + 2];
    pg_parser_sysdict_pgtype *sys_type = NULL;
    /*
     * 33 per dim since we assume 15 digits per number + ':' +'[]'
     *
     * +2 allows for assignment operator + trailing null
     */
    bool       *needquotes,
                needdims = false;
    size_t        overall_length;
    int32_t            nitems,
                i,
                j,
                k,
                indx[MAXDIM];
    int32_t            ndim,
               *dims,
               *lb;
    array_iter    iter;

    if (is_toast)
    {
        if (info != NULL)
            info->valueinfo = INFO_COL_IS_TOAST;
        info->valuelen = sizeof(struct pg_parser_varatt_external);
        return (pg_parser_Datum)0;
    }
    element_type = v->elemtype;

    sys_type = pg_parser_sysdict_getSysdictType(element_type, info->sysdicts);
    typlen = sys_type->typlen;
    typbyval = sys_type->typbyval;
    typalign = sys_type->typalign;
    typdelim = sys_type->typdelim;

    ndim = ARR_NDIM(v);
    dims = ARR_DIMS(v);
    lb = ARR_LBOUND(v);
    nitems = ArrayGetNItems(ndim, dims);

    if (nitems == 0)
    {
        retval = pg_parser_mcxt_strdup("{}");
        return (pg_parser_Datum) retval;
    }

    /*
     * we will need to add explicit dimensions if any dimension has a lower
     * bound other than one
     */
    for (i = 0; i < ndim; i++)
    {
        if (lb[i] != 1)
        {
            needdims = true;
            break;
        }
    }

    /*
     * Convert all values to string form, count total space needed (including
     * any overhead such as escaping backslashes), and detect whether each
     * item needs double quotes.
     */
    if (!pg_parser_mcxt_malloc(PGFUNC_ARRAY_MCXT, (void **)&values, nitems * sizeof(char *)))
        return (pg_parser_Datum)0;

    if (!pg_parser_mcxt_malloc(PGFUNC_ARRAY_MCXT, (void **)&needquotes, nitems * sizeof(bool)))
        return (pg_parser_Datum)0;

    overall_length = 0;

    array_iter_setup(&iter, v);

    for (i = 0; i < nitems; i++)
    {
        pg_parser_Datum        itemvalue;
        bool        isnull;
        bool        needquote;

        /* Get source element, checking for NULL */
        itemvalue = array_iter_next(&iter, &isnull, i,
                                    typlen, typbyval, typalign);

        if (isnull)
        {
            values[i] = pg_parser_mcxt_strdup("NULL");
            overall_length += 4;
            needquote = false;
        }
        else
        {
            if (PG_SYSDICT_TYPTYPE_BASE == sys_type->typtype
             || PG_SYSDICT_TYPTYPE_DOMAIN == sys_type->typtype
             || PG_SYSDICT_TYPTYPE_ENUM == sys_type->typtype
             || PG_SYSDICT_TYPTYPE_RANGE == sys_type->typtype)
            {
                bool istoast = false;

                values[i] = pg_parser_convert_attr_to_str_char(itemvalue, info->sysdicts,
                                                                  element_type, &istoast, info->zicinfo);
                /* First do simple processing for non-toast storage */
                if (!is_toast)
                {
                    /* count data plus backslashes; detect chars needing quotes */
                    if (values[i][0] == '\0')
                        needquote = true;    /* force quotes for empty string */
                    else if (pg_strcasecmp(values[i], "NULL") == 0)
                        needquote = true;    /* force quotes for literal NULL */
                    else
                        needquote = false;

                    for (tmp = values[i]; *tmp != '\0'; tmp++)
                    {
                        char ch = *tmp;

                        overall_length += 1;
                        if (ch == '"' || ch == '\\')
                        {
                            needquote = true;
                            overall_length += 1;
                        }
                        else if (ch == '{' || ch == '}' || ch == typdelim || array_isspace(ch))
                            needquote = true;
                    }
                }
                else
                    goto pg_parser_thirdparty_tupleparser_array_clean_and_do_deep_assemble;
            }
            else if (PG_SYSDICT_TYPTYPE_COMPOSITE == sys_type->typtype)
                goto pg_parser_thirdparty_tupleparser_array_clean_and_do_deep_assemble;
            /* This will not be reached, just in case, throw an error */
            else
            {
                //TODO
            }

        }

        needquotes[i] = needquote;

        /* Count the pair of double quotes, if needed */
        if (needquote)
            overall_length += 2;
        /* and the comma (or other typdelim delimiter) */
        overall_length += 1;
    }

    /*
     * The very last array element doesn't have a typdelim delimiter after it,
     * but that's OK; that space is needed for the trailing '\0'.
     *
     * Now count total number of curly brace pairs in output string.
     */
    for (i = j = 0, k = 1; i < ndim; i++)
    {
        j += k, k *= dims[i];
    }
    overall_length += 2 * j;

    /* Format explicit dimensions if required */
    dims_str[0] = '\0';
    if (needdims)
    {
        char *ptr = dims_str;

        for (i = 0; i < ndim; i++)
        {
            sprintf(ptr, "[%d:%d]", lb[i], lb[i] + dims[i] - 1);
            ptr += strlen(ptr);
        }
        *ptr++ = *ASSGN;
        *ptr = '\0';
        overall_length += ptr - dims_str;
    }

    /* Now construct the output string */
    if (!pg_parser_mcxt_malloc(PGFUNC_ARRAY_MCXT, (void **)&retval, overall_length))
        return (pg_parser_Datum)0;
    p = retval;

#define APPENDSTR(str) (strcpy(p, (str)), p += strlen(p))
#define APPENDCHAR(ch) (*p++ = (ch), *p = '\0')

    if (needdims)
        APPENDSTR(dims_str);
    APPENDCHAR('{');
    for (i = 0; i < ndim; i++)
        indx[i] = 0;
    j = 0;
    k = 0;
    do
    {
        for (i = j; i < ndim - 1; i++)
            APPENDCHAR('{');

        if (needquotes[k])
        {
            APPENDCHAR('"');
            for (tmp = values[k]; *tmp; tmp++)
            {
                char        ch = *tmp;

                if (ch == '"' || ch == '\\')
                    *p++ = '\\';
                *p++ = ch;
            }
            *p = '\0';
            APPENDCHAR('"');
        }
        else
            APPENDSTR(values[k]);
        pg_parser_mcxt_free(PGFUNC_ARRAY_MCXT, values[k++]);

        for (i = ndim - 1; i >= 0; i--)
        {
            if (++(indx[i]) < dims[i])
            {
                APPENDCHAR(typdelim);
                break;
            }
            else
            {
                indx[i] = 0;
                APPENDCHAR('}');
            }
        }
        j = i;
    } while (j != -1);

#undef APPENDSTR
#undef APPENDCHAR

    /* Assert that we calculated the string length accurately */

    pg_parser_mcxt_free(PGFUNC_ARRAY_MCXT, values);
    pg_parser_mcxt_free(PGFUNC_ARRAY_MCXT, needquotes);
    if (need_free)
        pg_parser_mcxt_free(PGFUNC_ARRAY_MCXT, v);
    return (pg_parser_Datum) retval;
#endif
    return (pg_parser_Datum)pg_parser_mcxt_strdup(">NOT SUPPORTED<");
}