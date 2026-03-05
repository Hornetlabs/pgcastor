/**
 * @file xk_pg_parser_thirdparty_tupleparser_rowid.c
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
#include "thirdparty/tupleparser/toast/xk_pg_parser_thirdparty_tupleparser_toast.h"

#define PGFUNC_ROWID_MCXT NULL

typedef uint8_t RowidByte;
/* RowidRawData is a variable data which store row unique logic address in database, it include
 * header and data, header describe how long its real internal data distribute.
 * rowidData include three part:
 * 1. transaction wrapped times using range [0 ~ uint32], using header high 3-bits to save bytes length
 * 2. transaction id using range [0 ~ uint32], using header middle 2-bits to save bytes length
 * 3. inserted times in current txn [0 ~ uint64] using header low 3-bits to save bytes length
 * */
typedef struct RowidRawData
{
    uint8_t          header;        /* header describe how rowiddata distribute */
    RowidByte        rawData[FLEXIBLE_ARRAY_MEMBER];
} RowidRawData;

typedef RowidRawData *RowidRawDataPtr;
/* RowidData is parsed from RowIdRawData, it just store in memory structure it include three parts.
 * 1. wrapped times. 4bytes [0 ~ UINT32_MAX], current wrapped times, just for case of
       xid wrapped. sure 42 billion will wrapped once.
 * 2. transaction id. 4bytes [ValidTransactionId ~ UINT32_MAX], row's inserted operation xid.
 * 3. inserted times. 8bytes [0 ~ UINT64_MAX], for this transaction, when inserted or call nextrowid func it will increase.
 * it will be transformed in/out as 64-base code, using A-Z, a-z, 0-9, +, /
 *
 *    See as 64base code dict:
 * |A|B|C|D|E|F|G|H|I|J|K |L |M |N |O |P |Q |R |S |T |U |V |W |X |Y |Z |a |b |c |d |e |f |g |h |i |j |k |l |m |n |o |p |q |r |s |t |u |v |w |x |y |z |0 |1 |2 |3 |4 |5 |6 |7 |8 |9 |+ |/ |
 * |0|1|2|3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28|29|30|31|32|33|34|35|36|37|38|39|40|41|42|43|44|45|46|47|48|49|50|51|52|53|54|55|56|57|58|59|60|61|62|63|
 * */
typedef struct RowidData
{
    uint32_t                        wrappedTimes;
    xk_pg_parser_TransactionId      txnid;
    uint64_t                        insertedTimes;
} RowidData;

typedef RowidData *RowidPtr;
/* get bytes from rowid's header */
/* higher 3bit is how many bytes which wrapped times stored, 0 => non wrapped, x => x bytes max 4bytes */
#define RID_WRPTIMES_BYTES(PTR)            (uint8_t) ((((RowidRawDataPtr)(PTR))->header) >> 5)
/* as transaction can not be zero, so atlease on byte, just use middle two bits stand for, x => x + 1 bytes*/
#define RID_TXN_BYTES(PTR)                (uint8_t) ((((((RowidRawDataPtr)(PTR))->header) & 0x1F) >> 3) + 1)
/* higher 3bit is how many bytes which wrapped times stored, x => x + 1 bytes max 8bytes */
#define RID_INSRTIMES_BYTES(PTR)        (uint8_t) (((((RowidRawDataPtr)(PTR))->header) & 0x7) + 1)

static const unsigned char _base64[] =
"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static void convet_64_4byte(uint32_t src, char *out)
{
    int32_t i = 0;
    uint32_t temp1 = src;
    uint32_t temp2 = 0;
    for (i = 0; i < 6; i++)
    {
        temp2 = temp1 % 64;
        temp1 = temp1 / 64;
        out[6 - i - 1] = _base64[temp2];
    }
    out[i] = '\0';
}

static void convet_64_8byte(uint64_t src, char *out)
{
    int32_t i = 0;
    uint64_t temp1 = src;
    uint64_t temp2 = 0;
    for (i = 0; i < 11; i++)
    {
        temp2 = temp1 % 64;
        temp1 = temp1 / 64;
        out[11 - i - 1] = _base64[temp2];
    }
    out[i] = '\0';
}

#if 0
static void rowid_base64_encode(const uint8_t *src, unsigned len, uint8_t *dst)
{
    int32_t i = 0;
    int32_t num = 0;
    char base_result[12] = {'\0'};
    uint8_t borrow = 0;
    uint8_t borrow_num = 0;
    uint8_t count = 0;

    if (4 == len)
        num = 6;
    else
        num = 11;
    for (i = 0; i < num; i++)
    {
        uint8_t temp_byte = 0;
        if (0 == borrow_num)
        {
            base_result[i] = _base64[src[count] & 0x3F];
            borrow_num = 2;
            borrow = (src[count] >> 6) & 0x03;
            count++;
        }
        else if (2 == borrow_num)
        {
            base_result[i] = _base64[borrow | ((src[count] & 0x0f) << 2)];
            borrow = (src[count] >> 4) & 0x0f;
            borrow_num = 4;
            count++;
        }
        else if (4 == borrow_num)
        {
            base_result[i] = _base64[borrow | ((src[count] & 0x03) << 4)];
            borrow = (src[count] >> 2) & 0x3f;
            borrow_num = 6;
        }
        else
        {
            base_result[i] = _base64[borrow & 0x3f];
            borrow = 0;
            borrow_num = 0;
            count++;
        }
        if (i == num - 2)
        {
            if (6 == num)
                base_result[i + 1] = _base64[borrow & 0x03];
            else
                base_result[i + 1] = _base64[borrow & 0x0f];
            break;
        }
    }
    for (i = 0; i < num ; i++)
        dst[i] = base_result[num - i - 1];
    dst[num] = '\0';
}
#endif

static xk_pg_parser_Datum rowidout_kingbase(xk_pg_parser_Datum attr,
                            xk_pg_parser_extraTypoutInfo *info)
{
    bool is_toast = false;
    bool need_free = false;
    RowidRawDataPtr rowid = NULL;
    char  convert[8] = {'\0'};
    char  base_wrapped[7] = {'\0'};
    char  base_txn[7] = {'\0'};
    char  base_insrt[12] = {'\0'};
    char  result[256] = {'\0'};
    int16_t wrapped_byte = RID_WRPTIMES_BYTES(rowid);
    int16_t txn_byte = RID_TXN_BYTES(rowid);
    int16_t insrt_byte = RID_INSRTIMES_BYTES(rowid);
    uint32_t                        wrappedTimes;
    xk_pg_parser_TransactionId      txnid;
    uint64_t                        insertedTimes;

    rowid = (RowidRawDataPtr)XK_PG_PARSER_VARDATA_ANY(
                                xk_pg_parser_detoast_datum((struct xk_pg_parser_varlena *)attr,
                                                           &is_toast,
                                                           &need_free,
                                                            info->zicinfo->dbtype,
                                                            info->zicinfo->dbversion));
    if (is_toast)
    {
        info->valueinfo = INFO_COL_IS_TOAST;
        info->valuelen = sizeof(struct xk_pg_parser_varatt_external);
        return (xk_pg_parser_Datum) rowid;
    }

    rmemcpy1(convert, 0, rowid->rawData, wrapped_byte);
    wrappedTimes = *((uint32_t*) convert);
    rmemset1(convert, 0, 0, 8);

    rmemcpy1(convert, 0, rowid->rawData + wrapped_byte, txn_byte);
    txnid = *((xk_pg_parser_TransactionId*) convert);
    rmemset1(convert, 0, 0, 8);

    rmemcpy1(convert, 0, rowid->rawData + wrapped_byte + txn_byte, insrt_byte);
    insertedTimes = *((uint64_t*) convert);
    rmemset1(convert, 0, 0, 8);

    //rowid_base64_encode(&wrappedTimes, 4, base_wrapped);
    //rowid_base64_encode(&txnid, 4, base_txn);
    //rowid_base64_encode(&insertedTimes, 8, base_insrt);

    convet_64_4byte(wrappedTimes, base_wrapped);
    convet_64_4byte(txnid, base_txn);
    convet_64_8byte(insertedTimes, base_insrt);

    sprintf(result, "%s %s %s", base_wrapped, base_txn, base_insrt);
    if (need_free)
        xk_pg_parser_mcxt_free(PGFUNC_ROWID_MCXT, rowid);
    return (xk_pg_parser_Datum) xk_pg_parser_mcxt_strdup(result);
}

typedef struct xk_pg_parser_hgdb902_rowid
{
       uint32_t tabOid;
       uint32_t objOid;
} xk_pg_parser_hgdb902_rowid;

static xk_pg_parser_Datum rowidout_hgdb902(xk_pg_parser_Datum attr,
                            xk_pg_parser_extraTypoutInfo *info)
{
    xk_pg_parser_hgdb902_rowid *rowid = (xk_pg_parser_hgdb902_rowid*)attr;
    char *result = NULL;

    XK_PG_PARSER_UNUSED(info);

    if(!xk_pg_parser_mcxt_malloc(PGFUNC_ROWID_MCXT, (void **) &result, 25))
        return (xk_pg_parser_Datum) 0;
    snprintf(result, 25, "(%u,%u)", rowid->tabOid, rowid->objOid);
    return (xk_pg_parser_Datum)result;
}

xk_pg_parser_Datum rowidout(xk_pg_parser_Datum attr,
                            xk_pg_parser_extraTypoutInfo *info)
{
    if (XK_DATABASE_TYPE_HGDB == info->zicinfo->dbtype
        && !strcmp(info->zicinfo->dbversion, XK_DATABASE_HGDBV9PG))
    {
        return rowidout_hgdb902(attr, info);
    }
    else
    {
        return rowidout_kingbase(attr, info);
    }
}
