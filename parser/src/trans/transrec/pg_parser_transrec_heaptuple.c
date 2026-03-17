#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "common/xk_pg_parser_translog.h"
#include "trans/transrec/xk_pg_parser_trans_transrec_itemptr.h"
#include "sysdict/xk_pg_parser_sysdict_getinfo.h"
#include "trans/transrec/xk_pg_parser_trans_transrec_heaptuple.h"
#include "trans/transrec/xk_pg_parser_trans_transrec_decode.h"
#include "image/xk_pg_parser_image.h"

#define HEAP_TUPLE_MCXT NULL
#define XK_PG_PARSER_RECORDOID       2249

/* sysattr */
#define xk_pg_parser_SelfItemPointerAttributeNumber             (-1)
#define xk_pg_parser_MinTransactionIdAttributeNumber            (-2)
#define xk_pg_parser_MinCommandIdAttributeNumber                (-3)
#define xk_pg_parser_MaxTransactionIdAttributeNumber            (-4)
#define xk_pg_parser_MaxCommandIdAttributeNumber                (-5)
#define xk_pg_parser_TableOidAttributeNumber                    (-6)
#define xk_pg_parser_FirstLowInvalidHeapAttributeNumber         (-7)

#define Min(x,y) ((x) < (y) ? (x) : (y))

xk_pg_parser_ReorderBufferTupleBuf * xk_pg_parser_heaptuple_get_tuple_space(size_t tuple_len,
                                                                            int16_t dbtype,
                                                                            char *dbversion)
{
    xk_pg_parser_ReorderBufferTupleBuf *tuple;
    size_t alloc_len;

    alloc_len = tuple_len + xk_pg_parser_SizeofHeapTupleHeader;

    if (!xk_pg_parser_mcxt_malloc(HEAP_TUPLE_MCXT, (void **)&tuple,
                                sizeof(xk_pg_parser_ReorderBufferTupleBuf)
                            + XK_PG_PARSER_MAXIMUM_ALIGNOF + alloc_len))
        return NULL;
    tuple->alloc_tuple_size = alloc_len;
    tuple->tuple.t_data = xk_pg_parser_ReorderBufferTupleBufData(tuple);
    return tuple;
}

void xk_pg_parser_reassemble_tuple_from_heap_tuple_header(
                                             void *hth,
                                             size_t len,
                                             xk_pg_parser_ReorderBufferTupleBuf *tuple,
                                             int16_t dbtype,
                                             char *dbversion)
{
    xk_pg_parser_HeapTupleHeader header = NULL;

    tuple->tuple.t_len = len;
    header = tuple->tuple.t_data;

    xk_pg_parser_ItemPointerSetInvalid(&tuple->tuple.t_self);
    tuple->tuple.t_tableOid = xk_pg_parser_InvalidOid;

    rmemcpy1(header, 0, hth, len);
}

void xk_pg_parser_reassemble_tuple_from_wal_data(char *data,
                                    size_t len,
                                    xk_pg_parser_ReorderBufferTupleBuf *tup,
                                    uint32_t xmin,
                                    uint32_t xmax,
                                    int16_t dbtype,
                                    char *dbversion)
{
    xk_pg_parser_xl_heap_header xlhdr;
    int32_t datalen = len - xk_pg_parser_SizeOfHeapHeader;
    xk_pg_parser_HeapTupleHeader header;
    xk_pg_parser_ReorderBufferTupleBuf *tuple = tup;

    tuple->tuple.t_len = datalen + xk_pg_parser_SizeofHeapTupleHeader;
    header = tuple->tuple.t_data;

    /* not a disk based tuple. */
    xk_pg_parser_ItemPointerSetInvalid(&tuple->tuple.t_self);

    /* we can only figure this out after reassembling the transactions. */
    tuple->tuple.t_tableOid = xk_pg_parser_InvalidOid;

    /* data is not stored aligned, copy to aligned storage. */
    rmemcpy1((char *)&xlhdr,
        0,
        data,
        xk_pg_parser_SizeOfHeapHeader);

    rmemset1(header, 0, 0, xk_pg_parser_SizeofHeapTupleHeader);

    rmemcpy1(((char *)tuple->tuple.t_data) + xk_pg_parser_SizeofHeapTupleHeader,
        0,
        data + xk_pg_parser_SizeOfHeapHeader,
        datalen);

    header->t_infomask = xlhdr.t_infomask;
    header->t_infomask2 = xlhdr.t_infomask2;
    header->t_hoff = xlhdr.t_hoff;
    if (xk_pg_parser_InvalidTransactionId != xmin)
        xk_pg_parser_HeapTupleHeaderSetXmin(header, xmin);
    if (xk_pg_parser_InvalidTransactionId != xmax)
        xk_pg_parser_HeapTupleHeaderSetXmax(header, xmax);
}

#if XK_PG_VERSION_NUM < 120000
xk_pg_parser_TupleDesc
CreateTemplateTupleDesc(int32_t natts, bool hasoid)
#else
static xk_pg_parser_TupleDesc
CreateTemplateTupleDesc(int32_t natts)
#endif
{
    xk_pg_parser_TupleDesc    desc;
    /*
     * Allocate enough memory for the tuple descriptor, including the
     * attribute rows.
     *
     * Note: the attribute array stride is sizeof(FormData_pg_attribute),
     * since we declare the array elements as FormData_pg_attribute for
     * notational convenience.  However, we only guarantee that the first
     * ATTRIBUTE_FIXED_PART_SIZE bytes of each entry are valid; most code that
     * copies tupdesc entries around copies just that much.  In principle that
     * could be less due to trailing padding, although with the current
     * definition of pg_attribute there probably isn't any padding.
     */
    if (!xk_pg_parser_mcxt_malloc(HEAP_TUPLE_MCXT,
                                 (void **)&desc,
                                  offsetof(struct xk_pg_parser_TupleDescData, attrs) +
                                  natts * sizeof(xk_pg_parser_sysdict_pgattributes)))
        return NULL;

    /*
     * Initialize other fields of the tupdesc.
     */
    desc->natts = natts;
    desc->constr = NULL;
    desc->tdtypeid = XK_PG_PARSER_RECORDOID;
    desc->tdtypmod = -1;
#if XK_PG_VERSION_NUM < 120000
    desc->tdhasoid = hasoid;
#endif
    desc->tdrefcount = -1;        /* assume not reference-counted */

    return desc;
}

xk_pg_parser_TupleDesc xk_pg_parser_get_desc(xk_pg_parser_sysdict_tableInfo* tbinfo)
{
    xk_pg_parser_TupleDesc tupdesc = NULL;
    xk_pg_sysdict_Form_pg_attribute fpa = NULL;
    int32_t i = 0;
#if XK_PG_VERSION_NUM < 120000
    tupdesc = CreateTemplateTupleDesc(tbinfo->natts, false);
#else
    tupdesc = CreateTemplateTupleDesc(tbinfo->natts);
#endif
    if (!tupdesc)
        return NULL;
    for (i = 0; i < tbinfo->natts; i++)
    {
        fpa = tbinfo->pgattr[i];
        rmemcpy1(&tupdesc->attrs[i], 0, fpa, sizeof(xk_pg_parser_sysdict_pgattributes));
    }
    return tupdesc;
}

#if XK_PG_VERSION_NUM >= 110000
xk_pg_parser_Datum xk_pg_parser_getmissingattr(xk_pg_parser_TupleDesc tupleDesc,
                                  int32_t attnum,
                                  bool *isnull,
                                  bool *ismissing)
{
    xk_pg_sysdict_Form_pg_attribute att;

    att = xk_pg_parser_TupleDescAttr(tupleDesc, attnum - 1);

    if (att->atthasmissing)
    {
        *isnull = false;
        *ismissing = true;
    }
    else
        *isnull = true;
    return xk_pg_parser_PointerGetDatum(NULL);
}
#endif

xk_pg_parser_Datum xk_pg_parser_nocachegetattr(xk_pg_parser_HeapTuple tuple,
                                  int32_t attnum,
                                  xk_pg_parser_TupleDesc tupleDesc,
                                  int16_t dbtype,
                                  char *dbversion)
{
    xk_pg_parser_HeapTupleHeader tup = NULL;
    char            *tp;                    /* ptr to data part of tuple */
    uint8_t         *bp = NULL;             /* ptr to null bitmap in tuple */
    bool             slow = false;          /* do we have to walk attrs? */
    int32_t          off;                   /* current offset within data */

    /* ----------------
     *     Three cases:
     *
     *     1: No nulls and no variable-width attributes.
     *     2: Has a null or a var-width AFTER att.
     *     3: Has nulls or var-widths BEFORE att.
     * ----------------
     */

    tup = tuple->t_data;
    bp = tup->t_bits;

    attnum--;

    if (!xk_pg_parser_HeapTupleNoNulls(tuple))
    {
        /*
         * there's a null somewhere in the tuple
         *
         * check to see if any preceding bits are null...
         */
        int32_t            byte = attnum >> 3;
        int32_t            finalbit = attnum & 0x07;

        /* check for nulls "before" final bit of last byte */
        if ((~bp[byte]) & ((1 << finalbit) - 1))
            slow = true;
        else
        {
            /* check for nulls in any "earlier" bytes */
            int32_t            i;

            for (i = 0; i < byte; i++)
            {
                if (bp[i] != 0xFF)
                {
                    slow = true;
                    break;
                }
            }
        }
    }

    tp = (char *) tup + tup->t_hoff;

    if (!slow)
    {
        xk_pg_sysdict_Form_pg_attribute att;

        /*
         * If we get here, there are no nulls up to and including the target
         * attribute.  If we have a cached offset, we can use it.
         */
        att = xk_pg_parser_TupleDescAttr(tupleDesc, attnum);
        if (att->attcacheoff >= 0)
            return xk_pg_parser_fetchatt(att, tp + att->attcacheoff);

        /*
         * Otherwise, check for non-fixed-length attrs up to and including
         * target.  If there aren't any, it's safe to cheaply initialize the
         * cached offsets for these attrs.
         */
        if (xk_pg_parser_HeapTupleHasVarWidth(tuple))
        {
            int32_t            j;

            for (j = 0; j <= attnum; j++)
            {
                if (xk_pg_parser_TupleDescAttr(tupleDesc, j)->attlen <= 0)
                {
                    slow = true;
                    break;
                }
            }
        }
    }

    if (!slow)
    {
        int32_t            natts = tupleDesc->natts;
        int32_t            j = 1;

        /*
         * If we get here, we have a tuple with no nulls or var-widths up to
         * and including the target attribute, so we can use the cached offset
         * ... only we don't have it yet, or we'd not have got here.  Since
         * it's cheap to compute offsets for fixed-width columns, we take the
         * opportunity to initialize the cached offsets for *all* the leading
         * fixed-width columns, in hope of avoiding future visits to this
         * routine.
         */
        xk_pg_parser_TupleDescAttr(tupleDesc, 0)->attcacheoff = 0;

        /* we might have set some offsets in the slow path previously */
        while (j < natts && xk_pg_parser_TupleDescAttr(tupleDesc, j)->attcacheoff > 0)
            j++;

        off = xk_pg_parser_TupleDescAttr(tupleDesc, j - 1)->attcacheoff +
            xk_pg_parser_TupleDescAttr(tupleDesc, j - 1)->attlen;

        for (; j < natts; j++)
        {
            xk_pg_sysdict_Form_pg_attribute att = xk_pg_parser_TupleDescAttr(tupleDesc, j);

            if (att->attlen <= 0)
                break;

            off = xk_pg_parser_att_align_nominal(off, att->attalign);

            att->attcacheoff = off;

            off += att->attlen;
        }

        off = xk_pg_parser_TupleDescAttr(tupleDesc, attnum)->attcacheoff;
    }
    else
    {
        bool        usecache = true;
        int32_t            i;

        /*
         * Now we know that we have to walk the tuple CAREFULLY.  But we still
         * might be able to cache some offsets for next time.
         *
         * Note - This loop is a little tricky.  For each non-null attribute,
         * we have to first account for alignment padding before the attr,
         * then advance over the attr based on its length.  Nulls have no
         * storage and no alignment padding either.  We can use/set
         * attcacheoff until we reach either a null or a var-width attribute.
         */
        off = 0;
        for (i = 0;; i++)        /* loop exit is at "break" */
        {
            xk_pg_sysdict_Form_pg_attribute att = xk_pg_parser_TupleDescAttr(tupleDesc, i);

            if (xk_pg_parser_HeapTupleHasNulls(tuple) && xk_pg_parser_att_isnull(i, bp))
            {
                usecache = false;
                continue;        /* this cannot be the target att */
            }

            /* If we know the next offset, we can skip the rest */
            if (usecache && att->attcacheoff >= 0)
                off = att->attcacheoff;
            else if (att->attlen == -1)
            {
                /*
                 * We can only cache the offset for a xk_pg_parser_varlena attribute if the
                 * offset is already suitably aligned, so that there would be
                 * no pad bytes in any case: then the offset will be valid for
                 * either an aligned or unaligned value.
                 */
                if (usecache &&
                    (uint32_t) off == xk_pg_parser_att_align_nominal(off, att->attalign))
                    att->attcacheoff = off;
                else
                {
                    off = xk_pg_parser_att_align_pointer(off, att->attalign, -1,
                                            tp + off);
                    usecache = false;
                }
            }
            else
            {
                /* not xk_pg_parser_varlena, so safe to use xk_pg_parser_att_align_nominal */
                off = xk_pg_parser_att_align_nominal(off, att->attalign);

                if (usecache)
                    att->attcacheoff = off;
            }

            if (i == attnum)
                break;

            off = xk_pg_parser_att_addlength_pointer(off, att->attlen, tp + off);

            if (usecache && att->attlen <= 0)
                usecache = false;
        }
    }

    return xk_pg_parser_fetchatt(xk_pg_parser_TupleDescAttr(tupleDesc, attnum), tp + off);
}

xk_pg_parser_Datum xk_pg_parser_heap_getsysattr(xk_pg_parser_HeapTuple tuple,
                                   int32_t attnum,
                                   xk_pg_parser_TupleDesc tupleDesc,
                                   bool *isnull,
                                   int16_t dbtype,
                                   char *dbversion)
{
    xk_pg_parser_Datum        result;
    xk_pg_parser_HeapTuple tup = tuple;

    XK_PG_PARSER_UNUSED(tupleDesc);

    /* Currently, no sys attribute ever reads as NULL. */
    *isnull = false;

    switch (attnum)
    {
        case xk_pg_parser_SelfItemPointerAttributeNumber:
            /* pass-by-reference datatype */
            result = xk_pg_parser_PointerGetDatum(&(tup->t_self));
            break;
        case xk_pg_parser_MinTransactionIdAttributeNumber:
            result = xk_pg_parser_TransactionIdGetDatum(xk_pg_parser_HeapTupleHeaderGetRawXmin(tup->t_data));
            break;
        case xk_pg_parser_MaxTransactionIdAttributeNumber:
            result = xk_pg_parser_TransactionIdGetDatum(xk_pg_parser_HeapTupleHeaderGetRawXmax(tup->t_data));
            break;
        case xk_pg_parser_MinCommandIdAttributeNumber:
        case xk_pg_parser_MaxCommandIdAttributeNumber:

            /*
            * cmin and cmax are now both aliases for the same field, which
            * can in fact also be a combo command id.  XXX perhaps we should
            * return the "real" cmin or cmax if possible, that is if we are
            * inside the originating transaction?
            */
            result = xk_pg_parser_CommandIdGetDatum(xk_pg_parser_HeapTupleHeaderGetRawCommandId(tup->t_data));
            break;
        case xk_pg_parser_TableOidAttributeNumber:
            result = xk_pg_parser_ObjectIdGetDatum(tup->t_tableOid);
            break;
        default:
            result = 0;            /* keep compiler quiet */
            break;
    }

    return result;
}

void xk_pg_parser_DecodeXLogTuple(char *data,
                                  size_t len,
                                  xk_pg_parser_ReorderBufferTupleBuf *tup,
                                  int16_t dbtype,
                                  char *dbversion)
{
    xk_pg_parser_xl_heap_header xlhdr;
    int32_t datalen = len - xk_pg_parser_SizeOfHeapHeader;
    xk_pg_parser_HeapTupleHeader header;
    xk_pg_parser_ReorderBufferTupleBuf *tuple = tup;

    tuple->tuple.t_len = datalen + xk_pg_parser_SizeofHeapTupleHeader;
    header = tuple->tuple.t_data;

    /* not a disk based tuple */
    xk_pg_parser_ItemPointerSetInvalid(&tuple->tuple.t_self);

    /* we can only figure this out after reassembling the transactions */
    tuple->tuple.t_tableOid = xk_pg_parser_InvalidOid;

    /* data is not stored aligned, copy to aligned storage */
    rmemcpy1((char *) &xlhdr,
        0,
        data,
        xk_pg_parser_SizeOfHeapHeader);

    rmemset1(header, 0, 0, xk_pg_parser_SizeofHeapTupleHeader);

    rmemcpy1(((char *) tuple->tuple.t_data) + xk_pg_parser_SizeofHeapTupleHeader,
        0,
        data + xk_pg_parser_SizeOfHeapHeader,
        datalen);

    header->t_infomask = xlhdr.t_infomask;
    header->t_infomask2 = xlhdr.t_infomask2;
    header->t_hoff = xlhdr.t_hoff;
}

/*
 * heap_deform_tuple
 *        Given a tuple, extract data into values/isnull arrays; this is
 *        the inverse of heap_form_tuple.
 *
 *        Storage for the values/isnull arrays is provided by the caller;
 *        it should be sized according to tupleDesc->natts not
 *        HeapTupleHeaderGetNatts(tuple->t_data).
 *
 *        Note that for pass-by-reference datatypes, the pointer placed
 *        in the Datum will point into the given tuple.
 *
 *        When all or most of a tuple's fields need to be extracted,
 *        this routine will be significantly quicker than a loop around
 *        heap_getattr; the loop will become O(N^2) as soon as any
 *        noncacheable attribute offsets are involved.
 */
void xk_pg_parser_heap_deform_tuple(xk_pg_parser_HeapTuple tuple,
                               xk_pg_parser_TupleDesc tupleDesc,
                               xk_pg_parser_Datum *values,
                               bool *isnull)
{
    xk_pg_parser_HeapTupleHeader tup = tuple->t_data;
    bool                         hasnulls = xk_pg_parser_HeapTupleHasNulls(tuple);
    int                          tdesc_natts = tupleDesc->natts;
    int                          natts;            /* number of atts to extract */
    int                          attnum;
    char                        *tp;                /* ptr to tuple data */
    uint32_t        off;            /* offset in tuple data */
    uint8_t       *bp = tup->t_bits;    /* ptr to null bitmap in tuple */
    bool        slow = false;    /* can we use/set attcacheoff? */

    natts = xk_pg_parser_HeapTupleHeaderGetNatts(tup);

    /*
     * In inheritance situations, it is possible that the given tuple actually
     * has more fields than the caller is expecting.  Don't run off the end of
     * the caller's arrays.
     */
    natts = Min(natts, tdesc_natts);

    tp = (char *) tup + tup->t_hoff;

    off = 0;

    for (attnum = 0; attnum < natts; attnum++)
    {
        xk_pg_sysdict_Form_pg_attribute thisatt = xk_pg_parser_TupleDescAttr(tupleDesc, attnum);

        if (hasnulls && xk_pg_parser_att_isnull(attnum, bp))
        {
            values[attnum] = (xk_pg_parser_Datum) 0;
            isnull[attnum] = true;
            slow = true;        /* can't use attcacheoff anymore */
            continue;
        }

        isnull[attnum] = false;

        if (!slow && thisatt->attcacheoff >= 0)
            off = thisatt->attcacheoff;
        else if (thisatt->attlen == -1)
        {
            /*
             * We can only cache the offset for a varlena attribute if the
             * offset is already suitably aligned, so that there would be no
             * pad bytes in any case: then the offset will be valid for either
             * an aligned or unaligned value.
             */
            if (!slow &&
                off == xk_pg_parser_att_align_nominal(off, thisatt->attalign))
                thisatt->attcacheoff = off;
            else
            {
                off = xk_pg_parser_att_align_pointer(off, thisatt->attalign, -1,
                                        tp + off);
                slow = true;
            }
        }
        else
        {
            /* not varlena, so safe to use att_align_nominal */
            off = xk_pg_parser_att_align_nominal(off, thisatt->attalign);

            if (!slow)
                thisatt->attcacheoff = off;
        }

        values[attnum] = xk_pg_parser_fetchatt(thisatt, tp + off);

        off = xk_pg_parser_att_addlength_pointer(off, thisatt->attlen, tp + off);

        if (thisatt->attlen <= 0)
            slow = true;        /* can't use attcacheoff anymore */
    }

    /*
     * If tuple doesn't have all the atts indicated by tupleDesc, read the
     * rest as nulls or missing values as appropriate.
     */
#if PG_VERSION_NUM >= 110000
    for (; attnum < tdesc_natts; attnum++)
        values[attnum] = getmissingattr(tupleDesc, attnum + 1, &isnull[attnum]);
#else
    for (; attnum < tdesc_natts; attnum++)
    {
        values[attnum] = (xk_pg_parser_Datum) 0;
        isnull[attnum] = true;
    }
#endif
}

static xk_pg_parser_ReorderBufferTupleBuf *xk_pg_parser_assemble_tuple_pg(char* page, uint16_t offnum)
{
    xk_pg_parser_ReorderBufferTupleBuf *tuple = NULL;
    xk_pg_parser_ItemId id = NULL;
    xk_pg_parser_HeapTupleHeader tuphdr = NULL;
    size_t tuplelen = 0;

    id = xk_pg_parser_PageGetItemId(page, offnum);
    tuphdr = (xk_pg_parser_HeapTupleHeader)xk_pg_parser_PageGetItem(page, id);
    tuplelen = (size_t)id->lp_len;
    tuple = xk_pg_parser_heaptuple_get_tuple_space(tuplelen, XK_DATABASE_TYPE_POSTGRESQL, XK_DATABASE_PG127);
    if (!tuple)
        return NULL;
    xk_pg_parser_reassemble_tuple_from_heap_tuple_header(tuphdr,
                                                         tuplelen,
                                                         tuple,
                                                         XK_DATABASE_TYPE_POSTGRESQL,
                                                         XK_DATABASE_PG127);
    return tuple;
}

xk_pg_parser_ReorderBufferTupleBuf *xk_pg_parser_assemble_tuple(int32_t dbtype,
                                                                char* dbversion,
                                                                uint32_t pagesize,
                                                                char* page,
                                                                uint16_t offnum)
{
    XK_PG_PARSER_UNUSED(pagesize);

    switch (dbtype)
    {
        case XK_DATABASE_TYPE_POSTGRESQL:
        {
            return xk_pg_parser_assemble_tuple_pg(page, offnum);
        }
        default:
        {
            return xk_pg_parser_assemble_tuple_pg(page, offnum);
        }
    }
}

