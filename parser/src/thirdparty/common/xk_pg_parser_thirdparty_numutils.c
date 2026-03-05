/**
 * @file xk_pg_parser_thirdparty_numutils.c
 * @author bytesync
 * @brief 
 * @version 0.1
 * @date 2023-08-03
 * 
 * @copyright Copyright (c) 2023
 * 
 */

#include <math.h>
#include <limits.h>
#include <ctype.h>
#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"

#include "thirdparty/common/xk_pg_parser_thirdparty_builtins.h"

/*
 * xk_numutils_itoa: converts a signed 16-bit integer to its string representation
 *
 * Caller must ensure that 'a' points to enough memory to hold the result
 * (at least 7 bytes, counting a leading sign and trailing NUL).
 *
 * It doesn't seem worth implementing this separately.
 */
void xk_numutils_itoa(int16_t i, char *a)
{
    xk_numutils_ltoa((int32_t) i, a);
}

/*
 * xk_numutils_ltoa: converts a signed 32-bit integer to its string representation
 *
 * Caller must ensure that 'a' points to enough memory to hold the result
 * (at least 12 bytes, counting a leading sign and trailing NUL).
 */
void xk_numutils_ltoa(int32_t value, char *a)
{
    char       *start = a;
    bool        neg = false;

    /*
     * Avoid problems with the most negative integer not being representable
     * as a positive integer.
     */
    if (value == XK_PG_PARSER_INT32_MIN)
    {
        rmemcpy0(a, 0, "-2147483648", 12);
        return;
    }
    else if (value < 0)
    {
        value = -value;
        neg = true;
    }

    /* Compute the result string backwards. */
    do
    {
        int32_t        remainder;
        int32_t        oldval = value;

        value /= 10;
        remainder = oldval - value * 10;
        *a++ = '0' + remainder;
    } while (value != 0);

    if (neg)
        *a++ = '-';

    /* Add trailing NUL byte, and back up 'a' to the last character. */
    *a-- = '\0';

    /* Reverse string. */
    while (start < a)
    {
        char        swap = *start;

        *start++ = *a;
        *a-- = swap;
    }
}

/*
 * xk_numutils_lltoa: convert a signed 64-bit integer to its string representation
 *
 * Caller must ensure that 'a' points to enough memory to hold the result
 * (at least MAXINT8LEN+1 bytes, counting a leading sign and trailing NUL).
 */
void xk_numutils_lltoa(int64_t value, char *a)
{
    char       *start = a;
    bool        neg = false;

    /*
     * Avoid problems with the most negative integer not being representable
     * as a positive integer.
     */
    if (value == XK_PG_PARSER_INT64_MIN)
    {
        rmemcpy1(a, 0, "-9223372036854775808", 21);
        return;
    }
    else if (value < 0)
    {
        value = -value;
        neg = true;
    }

    /* Compute the result string backwards. */
    do
    {
        int64_t        remainder;
        int64_t        oldval = value;

        value /= 10;
        remainder = oldval - value * 10;
        *a++ = '0' + remainder;
    } while (value != 0);

    if (neg)
        *a++ = '-';

    /* Add trailing NUL byte, and back up 'a' to the last character. */
    *a-- = '\0';

    /* Reverse string. */
    while (start < a)
    {
        char        swap = *start;

        *start++ = *a;
        *a-- = swap;
    }
}


/*
 * xk_numutils_ltostr_zeropad
 *        Converts 'value' into a decimal string representation stored at 'str'.
 *        'minwidth' specifies the minimum width of the result; any extra space
 *        is filled up by prefixing the number with zeros.
 *
 * Returns the ending address of the string result (the last character written
 * plus 1).  Note that no NUL terminator is written.
 *
 * The intended use-case for this function is to build strings that contain
 * multiple individual numbers, for example:
 *
 *    str = xk_numutils_ltostr_zeropad(str, hours, 2);
 *    *str++ = ':';
 *    str = xk_numutils_ltostr_zeropad(str, mins, 2);
 *    *str++ = ':';
 *    str = xk_numutils_ltostr_zeropad(str, secs, 2);
 *    *str = '\0';
 *
 * Note: Caller must ensure that 'str' points to enough memory to hold the
 * result.
 */
char *xk_numutils_ltostr_zeropad(char *str, int32_t value, int32_t minwidth)
{
    char       *start = str;
    char       *end = &str[minwidth];
    int32_t        num = value;

    /*
     * Handle negative numbers in a special way.  We can't just write a '-'
     * prefix and reverse the sign as that would overflow for INT32_MIN.
     */
    if (num < 0)
    {
        *start++ = '-';
        minwidth--;

        /*
         * Build the number starting at the last digit.  Here remainder will
         * be a negative number, so we must reverse the sign before adding '0'
         * in order to get the correct ASCII digit.
         */
        while (minwidth--)
        {
            int32_t        oldval = num;
            int32_t        remainder;

            num /= 10;
            remainder = oldval - num * 10;
            start[minwidth] = '0' - remainder;
        }
    }
    else
    {
        /* Build the number starting at the last digit */
        while (minwidth--)
        {
            int32_t        oldval = num;
            int32_t        remainder;

            num /= 10;
            remainder = oldval - num * 10;
            start[minwidth] = '0' + remainder;
        }
    }

    /*
     * If minwidth was not high enough to fit the number then num won't have
     * been divided down to zero.  We punt the problem to xk_numutils_ltostr(), which
     * will generate a correct answer in the minimum valid width.
     */
    if (num != 0)
        return xk_numutils_ltostr(str, value);

    /* Otherwise, return last output character + 1 */
    return end;
}

/*
 * xk_numutils_ltostr
 *        Converts 'value' into a decimal string representation stored at 'str'.
 *
 * Returns the ending address of the string result (the last character written
 * plus 1).  Note that no NUL terminator is written.
 *
 * The intended use-case for this function is to build strings that contain
 * multiple individual numbers, for example:
 *
 *    str = xk_numutils_ltostr(str, a);
 *    *str++ = ' ';
 *    str = xk_numutils_ltostr(str, b);
 *    *str = '\0';
 *
 * Note: Caller must ensure that 'str' points to enough memory to hold the
 * result.
 */
char *xk_numutils_ltostr(char *str, int32_t value)
{
    char       *start;
    char       *end;

    /*
     * Handle negative numbers in a special way.  We can't just write a '-'
     * prefix and reverse the sign as that would overflow for INT32_MIN.
     */
    if (value < 0)
    {
        *str++ = '-';

        /* Mark the position we must reverse the string from. */
        start = str;

        /* Compute the result string backwards. */
        do
        {
            int32_t        oldval = value;
            int32_t        remainder;

            value /= 10;
            remainder = oldval - value * 10;
            /* As above, we expect remainder to be negative. */
            *str++ = '0' - remainder;
        } while (value != 0);
    }
    else
    {
        /* Mark the position we must reverse the string from. */
        start = str;

        /* Compute the result string backwards. */
        do
        {
            int32_t        oldval = value;
            int32_t        remainder;

            value /= 10;
            remainder = oldval - value * 10;
            *str++ = '0' + remainder;
        } while (value != 0);
    }

    /* Remember the end+1 and back up 'str' to the last character. */
    end = str--;

    /* Reverse string. */
    while (start < str)
    {
        char        swap = *start;

        *start++ = *str;
        *str-- = swap;
    }

    return end;
}
