#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "mcxt/xk_pg_parser_mcxt.h"
#include "common/xk_pg_parser_define.h"
#include "thirdparty/stringinfo/xk_pg_parser_thirdparty_stringinfo.h"

#define STRINGINFO_MCXT NULL

#define XK_STRINFO_MaxAllocSize    ((size_t) 0x3fffffff) /* 1 gigabyte - 1 */

/*
 * xk_pg_parser_makeStringInfo
 *
 * Create an empty 'xk_pg_parser_StringInfoData' & return a pointer to it.
 */
xk_pg_parser_StringInfo
xk_pg_parser_makeStringInfo(void)
{
    xk_pg_parser_StringInfo res;
    if (!xk_pg_parser_mcxt_malloc(STRINGINFO_MCXT, (void **) &res, sizeof(xk_pg_parser_StringInfoData)))
    {
        //printf("something wrong in malloc stringinfp\n");
        return NULL;
    }
    xk_pg_parser_initStringInfo(res);
    return res;
}

/*
 * xk_pg_parser_initStringInfo
 *
 * Initialize a xk_pg_parser_StringInfoData struct (with previously undefined contents)
 * to describe an empty string.
 */
void
xk_pg_parser_initStringInfo(xk_pg_parser_StringInfo str)
{
    int32_t size = 1024; /* initial default buffer size */
    if (!xk_pg_parser_mcxt_malloc(STRINGINFO_MCXT, (void **) &(str->data), size))
    {
        //printf("something wrong in malloc stringinfp\n");
        str->maxlen = 0;
    }
    else
    {
        str->maxlen = size;
        xk_pg_parser_resetStringInfo(str);
    }
}

/*
 * xk_pg_parser_resetStringInfo
 *
 * Reset the xk_pg_parser_StringInfo: the data buffer remains valid, but its
 * previous content, if any, is cleared.
 */
void
xk_pg_parser_resetStringInfo(xk_pg_parser_StringInfo str)
{
    str->data[0] = '\0';
    str->len = 0;
    str->cursor = 0;
}

/*
 * xk_pg_parser_appendStringInfo
 *
 * Format xk_pg_parser_text data under the control of fmt (an sprintf-style format string)
 * and append it to whatever is already in str.  More space is allocated
 * to str if necessary.  This is sort of like a combination of sprintf and
 * strcat.
 */
void
xk_pg_parser_appendStringInfo(xk_pg_parser_StringInfo str, const char *fmt, ...)
{
    int32_t save_errno = errno;

    for (;;)
    {
        va_list args;
        int32_t needed;

        /* Try to format the data. */
        errno = save_errno;
        va_start(args, fmt);
        needed = xk_pg_parser_appendStringInfoVA(str, fmt, args);
        va_end(args);

        if (needed == 0)
            break; /* success */

        /* Increase the buffer size and try again. */
        xk_pg_parser_enlargeStringInfo(str, needed);
    }
}

/*
 * xk_pg_parser_appendStringInfoVA
 *
 * Attempt to format xk_pg_parser_text data under the control of fmt (an sprintf-style
 * format string) and append it to whatever is already in str.  If successful
 * return zero; if not (because there's not enough space), return an estimate
 * of the space needed, without modifying str.  Typically the caller should
 * pass the return value to xk_pg_parser_enlargeStringInfo() before trying again; see
 * xk_pg_parser_appendStringInfo for standard usage pattern.
 *
 * Caution: callers must be sure to preserve their entry-time errno
 * when looping, in case the fmt contains "%m".
 *
 * XXX This API is ugly, but there seems no alternative given the C spec's
 * restrictions on what can portably be done with va_list arguments: you have
 * to redo va_start before you can rescan the argument list, and we can't do
 * that from here.
 */
int32_t
xk_pg_parser_appendStringInfoVA(xk_pg_parser_StringInfo str, const char *fmt, va_list args)
{
    int32_t avail;
    size_t nprinted;

    /*
     * If there's hardly any space, don't bother trying, just fail to make the
     * caller enlarge the buffer first.  We have to guess at how much to
     * enlarge, since we're skipping the formatting work.
     */
    avail = str->maxlen - str->len;
    if (avail < 16)
        return 32;

    nprinted = vsnprintf(str->data + str->len, (size_t)avail, fmt, args);

    if (nprinted < (size_t)avail)
    {
        /* Success.  Note nprinted does not include trailing null. */
        str->len += (int32_t)nprinted;
        return 0;
    }

    /* Restore the trailing null so that str is unmodified. */
    str->data[str->len] = '\0';

    /*
     * Return pvsnprintf's estimate of the space needed.  (Although this is
     * given as a size_t, we know it will fit in int32_t because it's not more
     * than MaxAllocSize.)
     */
    return (int32_t)nprinted;
}

/*
 * xk_pg_parser_appendStringInfoString
 *
 * Append a null-terminated string to str.
 * Like xk_pg_parser_appendStringInfo(str, "%s", s) but faster.
 */
void
xk_pg_parser_appendStringInfoString(xk_pg_parser_StringInfo str, const char *s)
{
    xk_pg_parser_appendBinaryStringInfo(str, s, strlen(s));
}

/*
 * xk_pg_parser_appendStringInfoSpaces
 *
 * Append the specified number of spaces to a buffer.
 */
void
xk_pg_parser_appendStringInfoSpaces(xk_pg_parser_StringInfo str, int32_t count)
{
    if (count > 0)
    {
        /* Make more room if needed */
        xk_pg_parser_enlargeStringInfo(str, count);

        /* OK, append the spaces */
        while (--count >= 0)
            str->data[str->len++] = ' ';
        str->data[str->len] = '\0';
    }
}

/*
 * xk_pg_parser_appendBinaryStringInfo
 *
 * Append arbitrary binary data to a xk_pg_parser_StringInfo, allocating more space
 * if necessary. Ensures that a trailing null byte is present.
 */
void
xk_pg_parser_appendBinaryStringInfo(xk_pg_parser_StringInfo str, const char *data, int32_t datalen)
{
    /* Make more room if needed */
    xk_pg_parser_enlargeStringInfo(str, datalen);

    /* OK, append the data */
    rmemcpy0(str->data, str->len, data, datalen);
    str->len += datalen;

    /*
     * Keep a trailing null in place, even though it's probably useless for
     * binary data.  (Some callers are dealing with xk_pg_parser_text but call this because
     * their input isn't null-terminated.)
     */
    str->data[str->len] = '\0';
}

/*
 * xk_pg_parser_enlargeStringInfo
 *
 * Make sure there is enough space for 'needed' more bytes
 * ('needed' does not include the terminating null).
 *
 * External callers usually need not concern themselves with this, since
 * all stringinfo.c routines do it automatically.  However, if a caller
 * knows that a xk_pg_parser_StringInfo will eventually become X bytes large, it
 * can save some palloc overhead by enlarging the buffer before starting
 * to store data in it.
 *
 * NB: because we use repalloc() to enlarge the buffer, the string buffer
 * will remain allocated in the same memory context that was current when
 * xk_pg_parser_initStringInfo was called, even if another context is now current.
 * This is the desired and indeed critical behavior!
 */
void
xk_pg_parser_enlargeStringInfo(xk_pg_parser_StringInfo str, int32_t needed)
{
    int32_t newlen;

    /*
     * Guard against out-of-range "needed" values.  Without this, we can get
     * an overflow or infinite loop in the following.
     */
    if (needed < 0) /* should not happen */
        return;
    if (((size_t)needed) >= (XK_STRINFO_MaxAllocSize - (size_t)str->len))
        return;

    needed += str->len + 1; /* total space required now */

    /* Because of the above test, we now have needed <= MaxAllocSize */

    if (needed <= str->maxlen)
        return; /* got enough space already */

    /*
     * We don't want to allocate just a little more space with each append;
     * for efficiency, double the buffer size each time it overflows.
     * Actually, we might need to more than double it if 'needed' is big...
     */
    newlen = 2 * str->maxlen;
    while (needed > newlen)
        newlen = 2 * newlen;

    /*
     * Clamp to MaxAllocSize in case we went past it.  Note we are assuming
     * here that MaxAllocSize <= INT_MAX/2, else the above loop could
     * overflow.  We will still have newlen >= needed.
     */
    if (newlen > (int32_t)XK_STRINFO_MaxAllocSize)
        newlen = (int32_t)XK_STRINFO_MaxAllocSize;

    if (!xk_pg_parser_mcxt_realloc(STRINGINFO_MCXT, (void **) &(str->data), newlen))
    {
        //printf("something wrong in repalloc stringinfo\n");
        str->maxlen = 0;
    }
    else
        str->maxlen = newlen;
}

/*
 * xk_pg_parser_appendStringInfoChar
 *
 * Append a single byte to str.
 * Like xk_pg_parser_appendStringInfo(str, "%c", ch) but much faster.
 */
void
xk_pg_parser_appendStringInfoChar(xk_pg_parser_StringInfo str, char ch)
{
    /* Make more room if needed */
    if (str->len + 1 >= str->maxlen)
        xk_pg_parser_enlargeStringInfo(str, 1);

    /* OK, append the character */
    str->data[str->len] = ch;
    str->len++;
    str->data[str->len] = '\0';
}
