/**
 * @file xk_pg_parser_thirdparty_tupleparser_cash.c
 * @author bytesync
 * @brief 
 * @version 0.1
 * @date 2023-08-03
 * 
 * @copyright Copyright (c) 2023
 * 
 */
#include <locale.h>
#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "common/xk_pg_parser_translog.h"
#include "thirdparty/tupleparser/common/xk_pg_parser_thirdparty_tupleparser_pgsfunc.h"

#define PGFUNC_CASH_MCXT NULL

typedef int64_t Cash;

/* cash_out()
 * Function to convert cash to a dollars and cents representation, using
 * the lc_monetary locale's formatting.
 */
xk_pg_parser_Datum cash_out(xk_pg_parser_Datum attr,
                            xk_pg_parser_extraTypoutInfo *info)
{
    Cash        value = (Cash) attr;
    char       *result;
    char        buf[128];
    char       *bufptr;
    int32_t            digit_pos;
    int32_t            points,
                mon_group;
    char        dsymbol;
    const char *ssymbol,
               *csymbol,
               *signsymbol;
    char        sign_posn,
                cs_precedes,
                sep_by_space;
    struct lconv lconvert_tmp;
    struct lconv *extlconv,
                 *lconvert;
    char *save_lc_monetary;
    char *save_lc_numeric;
    if (!xk_pg_parser_mcxt_malloc(PGFUNC_CASH_MCXT, (void **) &result, 32))
        return (xk_pg_parser_Datum) 0;
    rmemset1(&lconvert_tmp, 0, 0, sizeof(struct lconv));
    save_lc_monetary = setlocale(LC_MONETARY, NULL);
    save_lc_monetary = xk_pg_parser_mcxt_strdup(save_lc_monetary);
    save_lc_numeric = setlocale(LC_NUMERIC, NULL);
    save_lc_numeric = xk_pg_parser_mcxt_strdup(save_lc_numeric);
    setlocale(LC_NUMERIC, info->zicinfo->convertinfo->m_numeric);
    extlconv = localeconv();
    lconvert_tmp.decimal_point = xk_pg_parser_mcxt_strdup(extlconv->decimal_point);
    lconvert_tmp.thousands_sep = xk_pg_parser_mcxt_strdup(extlconv->thousands_sep);
    lconvert_tmp.grouping = xk_pg_parser_mcxt_strdup(extlconv->grouping);
    setlocale(LC_MONETARY, info->zicinfo->convertinfo->m_monetary);
    extlconv = localeconv();
    /* Must copy data now in case setlocale() overwrites it */
    lconvert_tmp.int_curr_symbol = xk_pg_parser_mcxt_strdup(extlconv->int_curr_symbol);
    lconvert_tmp.currency_symbol = xk_pg_parser_mcxt_strdup(extlconv->currency_symbol);
    lconvert_tmp.mon_decimal_point = xk_pg_parser_mcxt_strdup(extlconv->mon_decimal_point);
    lconvert_tmp.mon_thousands_sep = xk_pg_parser_mcxt_strdup(extlconv->mon_thousands_sep);
    lconvert_tmp.mon_grouping = xk_pg_parser_mcxt_strdup(extlconv->mon_grouping);
    lconvert_tmp.positive_sign = xk_pg_parser_mcxt_strdup(extlconv->positive_sign);
    lconvert_tmp.negative_sign = xk_pg_parser_mcxt_strdup(extlconv->negative_sign);
    /* Copy scalar fields as well */
    lconvert_tmp.int_frac_digits = extlconv->int_frac_digits;
    lconvert_tmp.frac_digits = extlconv->frac_digits;
    lconvert_tmp.p_cs_precedes = extlconv->p_cs_precedes;
    lconvert_tmp.p_sep_by_space = extlconv->p_sep_by_space;
    lconvert_tmp.n_cs_precedes = extlconv->n_cs_precedes;
    lconvert_tmp.n_sep_by_space = extlconv->n_sep_by_space;
    lconvert_tmp.p_sign_posn = extlconv->p_sign_posn;
    lconvert_tmp.n_sign_posn = extlconv->n_sign_posn;
    setlocale(LC_MONETARY, save_lc_monetary);
    setlocale(LC_NUMERIC, save_lc_numeric);
    xk_pg_parser_mcxt_free(PGFUNC_CASH_MCXT, save_lc_monetary);
    xk_pg_parser_mcxt_free(PGFUNC_CASH_MCXT, save_lc_numeric);
    /* Get formatting information for monetary */
    lconvert = &lconvert_tmp;

    /* see comments about frac_digits in cash_in() */
    points = lconvert->frac_digits;
    if (points < 0 || points > 10)
        points = 2;                /* best guess in this case, I think */

    /*
     * As with frac_digits, must apply a range check to mon_grouping to avoid
     * being fooled by variant CHAR_MAX values.
     */
    mon_group = *lconvert->mon_grouping;
    if (mon_group <= 0 || mon_group > 6)
        mon_group = 3;

    /* we restrict dsymbol to be a single byte, but not the other symbols */
    if (*lconvert->mon_decimal_point != '\0' &&
        lconvert->mon_decimal_point[1] == '\0')
        dsymbol = *lconvert->mon_decimal_point;
    else
        dsymbol = '.';
    if (*lconvert->mon_thousands_sep != '\0')
        ssymbol = lconvert->mon_thousands_sep;
    else                        /* ssymbol should not equal dsymbol */
        ssymbol = (dsymbol != ',') ? "," : ".";
    csymbol = (*lconvert->currency_symbol != '\0') ? lconvert->currency_symbol : "$";

    if (value < 0)
    {
        /* make the amount positive for digit-reconstruction loop */
        value = -value;
        /* set up formatting data */
        signsymbol = (*lconvert->negative_sign != '\0') ? lconvert->negative_sign : "-";
        sign_posn = lconvert->n_sign_posn;
        cs_precedes = lconvert->n_cs_precedes;
        sep_by_space = lconvert->n_sep_by_space;
    }
    else
    {
        signsymbol = lconvert->positive_sign;
        sign_posn = lconvert->p_sign_posn;
        cs_precedes = lconvert->p_cs_precedes;
        sep_by_space = lconvert->p_sep_by_space;
    }

    /* we build the digits+decimal-point+sep string right-to-left in buf[] */
    bufptr = buf + sizeof(buf) - 1;
    *bufptr = '\0';

    /*
     * Generate digits till there are no non-zero digits left and we emitted
     * at least one to the left of the decimal point.  digit_pos is the
     * current digit position, with zero as the digit just left of the decimal
     * point, increasing to the right.
     */
    digit_pos = points;
    do
    {
        if (points && digit_pos == 0)
        {
            /* insert decimal point, but not if value cannot be fractional */
            *(--bufptr) = dsymbol;
        }
        else if (digit_pos < 0 && (digit_pos % mon_group) == 0)
        {
            /* insert thousands sep, but only to left of radix point */
            bufptr -= strlen(ssymbol);
            rmemcpy1(bufptr, 0, ssymbol, strlen(ssymbol));
        }

        *(--bufptr) = ((uint64_t) value % 10) + '0';
        value = ((uint64_t) value) / 10;
        digit_pos--;
    } while (value || digit_pos >= 0);

    /*----------
     * Now, attach currency symbol and sign symbol in the correct order.
     *
     * The POSIX spec defines these values controlling this code:
     *
     * p/n_sign_posn:
     *    0    Parentheses enclose the quantity and the currency_symbol.
     *    1    The sign string precedes the quantity and the currency_symbol.
     *    2    The sign string succeeds the quantity and the currency_symbol.
     *    3    The sign string precedes the currency_symbol.
     *    4    The sign string succeeds the currency_symbol.
     *
     * p/n_cs_precedes: 0 means currency symbol after value, else before it.
     *
     * p/n_sep_by_space:
     *    0    No <space> separates the currency symbol and value.
     *    1    If the currency symbol and sign string are adjacent, a <space>
     *        separates them from the value; otherwise, a <space> separates
     *        the currency symbol from the value.
     *    2    If the currency symbol and sign string are adjacent, a <space>
     *        separates them; otherwise, a <space> separates the sign string
     *        from the value.
     *----------
     */
    switch (sign_posn)
    {
        case 0:
            if (cs_precedes)
                sprintf(result, "(%s%s%s)",
                                  csymbol,
                                  (sep_by_space == 1) ? " " : "",
                                  bufptr);
            else
                sprintf(result, "(%s%s%s)",
                                  bufptr,
                                  (sep_by_space == 1) ? " " : "",
                                  csymbol);
            break;
        case 1:
        default:
            if (cs_precedes)
                sprintf(result, "%s%s%s%s%s",
                                  signsymbol,
                                  (sep_by_space == 2) ? " " : "",
                                  csymbol,
                                  (sep_by_space == 1) ? " " : "",
                                  bufptr);
            else
                sprintf(result, "%s%s%s%s%s",
                                  signsymbol,
                                  (sep_by_space == 2) ? " " : "",
                                  bufptr,
                                  (sep_by_space == 1) ? " " : "",
                                  csymbol);
            break;
        case 2:
            if (cs_precedes)
                sprintf(result, "%s%s%s%s%s",
                                  csymbol,
                                  (sep_by_space == 1) ? " " : "",
                                  bufptr,
                                  (sep_by_space == 2) ? " " : "",
                                  signsymbol);
            else
                sprintf(result, "%s%s%s%s%s",
                                  bufptr,
                                  (sep_by_space == 1) ? " " : "",
                                  csymbol,
                                  (sep_by_space == 2) ? " " : "",
                                  signsymbol);
            break;
        case 3:
            if (cs_precedes)
                sprintf(result, "%s%s%s%s%s",
                                  signsymbol,
                                  (sep_by_space == 2) ? " " : "",
                                  csymbol,
                                  (sep_by_space == 1) ? " " : "",
                                  bufptr);
            else
                sprintf(result, "%s%s%s%s%s",
                                  bufptr,
                                  (sep_by_space == 1) ? " " : "",
                                  signsymbol,
                                  (sep_by_space == 2) ? " " : "",
                                  csymbol);
            break;
        case 4:
            if (cs_precedes)
                sprintf(result, "%s%s%s%s%s",
                                  csymbol,
                                  (sep_by_space == 2) ? " " : "",
                                  signsymbol,
                                  (sep_by_space == 1) ? " " : "",
                                  bufptr);
            else
                sprintf(result, "%s%s%s%s%s",
                                  bufptr,
                                  (sep_by_space == 1) ? " " : "",
                                  csymbol,
                                  (sep_by_space == 2) ? " " : "",
                                  signsymbol);
            break;
    }

    if (lconvert->decimal_point)
        xk_pg_parser_mcxt_free(PGFUNC_CASH_MCXT, lconvert->decimal_point);
    if (lconvert->thousands_sep)
        xk_pg_parser_mcxt_free(PGFUNC_CASH_MCXT, lconvert->thousands_sep);
    if (lconvert->grouping)
        xk_pg_parser_mcxt_free(PGFUNC_CASH_MCXT, lconvert->grouping);
    if (lconvert->int_curr_symbol)
        xk_pg_parser_mcxt_free(PGFUNC_CASH_MCXT, lconvert->int_curr_symbol);
    if (lconvert->currency_symbol)
        xk_pg_parser_mcxt_free(PGFUNC_CASH_MCXT, lconvert->currency_symbol);
    if (lconvert->mon_decimal_point)
        xk_pg_parser_mcxt_free(PGFUNC_CASH_MCXT, lconvert->mon_decimal_point);
    if (lconvert->mon_thousands_sep)
        xk_pg_parser_mcxt_free(PGFUNC_CASH_MCXT, lconvert->mon_thousands_sep);
    if (lconvert->mon_grouping)
        xk_pg_parser_mcxt_free(PGFUNC_CASH_MCXT, lconvert->mon_grouping);
    if (lconvert->positive_sign)
        xk_pg_parser_mcxt_free(PGFUNC_CASH_MCXT, lconvert->positive_sign);
    if (lconvert->negative_sign)
        xk_pg_parser_mcxt_free(PGFUNC_CASH_MCXT, lconvert->negative_sign);
    info->valuelen = strlen(result);
    return (xk_pg_parser_Datum) result;
}