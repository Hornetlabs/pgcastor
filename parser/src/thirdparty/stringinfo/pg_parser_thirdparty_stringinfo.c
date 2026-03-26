#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "mcxt/pg_parser_mcxt.h"
#include "common/pg_parser_define.h"
#include "thirdparty/stringinfo/pg_parser_thirdparty_stringinfo.h"

#define STRINGINFO_MCXT      NULL

#define STRINFO_MaxAllocSize ((size_t)0x3fffffff) /* 1 gigabyte - 1 */

/*
 * pg_parser_makeStringInfo
 *
 * Create an empty 'pg_parser_StringInfoData' & return a pointer to it.
 */
pg_parser_StringInfo pg_parser_makeStringInfo(void)
{
    pg_parser_StringInfo res;
    if (!pg_parser_mcxt_malloc(STRINGINFO_MCXT, (void**)&res, sizeof(pg_parser_StringInfoData)))
    {
        /* todo error handling */
        return NULL;
    }
    pg_parser_initStringInfo(res);
    return res;
}

/*
 * pg_parser_initStringInfo
 *
 * Initialize a pg_parser_StringInfoData struct (with previously undefined contents)
 * to describe an empty string.
 */
void pg_parser_initStringInfo(pg_parser_StringInfo str)
{
    int32_t size = 1024; /* initial default buffer size */
    if (!pg_parser_mcxt_malloc(STRINGINFO_MCXT, (void**)&(str->data), size))
    {
        /* todo error handling */
        str->maxlen = 0;
    }
    else
    {
        str->maxlen = size;
        pg_parser_resetStringInfo(str);
    }
}

/*
 * pg_parser_resetStringInfo
 *
 * Reset the pg_parser_StringInfo: the data buffer remains valid, but its
 * previous content, if any, is cleared.
 */
void pg_parser_resetStringInfo(pg_parser_StringInfo str)
{
    str->data[0] = '\0';
    str->len = 0;
    str->cursor = 0;
}

/*
 * pg_parser_appendStringInfo
 *
 * Format pg_parser_text data under the control of fmt (an sprintf-style format string)
 * and append it to whatever is already in str.  More space is allocated
 * to str if necessary.  This is sort of like a combination of sprintf and
 * strcat.
 */
void pg_parser_appendStringInfo(pg_parser_StringInfo str, const char* fmt, ...)
{
    int32_t save_errno = errno;

    for (;;)
    {
        va_list args;
        int32_t needed;

        /* Try to format the data. */
        errno = save_errno;
        va_start(args, fmt);
        needed = pg_parser_appendStringInfoVA(str, fmt, args);
        va_end(args);

        if (needed == 0)
        {
            break; /* success */
        }

        /* Increase the buffer size and try again. */
        pg_parser_enlargeStringInfo(str, needed);
    }
}

/*
 * pg_parser_appendStringInfoVA
 *
 * Attempt to format pg_parser_text data under the control of fmt (an sprintf-style
 * format string) and append it to whatever is already in str.  If successful
 * return zero; if not (because there's not enough space), return an estimate
 * of the space needed, without modifying str.  Typically the caller should
 * pass the return value to pg_parser_enlargeStringInfo() before trying again; see
 * pg_parser_appendStringInfo for standard usage pattern.
 *
 * Caution: callers must be sure to preserve their entry-time errno
 * when looping, in case the fmt contains "%m".
 *
 * XXX This API is ugly, but there seems no alternative given the C spec's
 * restrictions on what can portably be done with va_list arguments: you have
 * to redo va_start before you can rescan the argument list, and we can't do
 * that from here.
 */
int32_t pg_parser_appendStringInfoVA(pg_parser_StringInfo str, const char* fmt, va_list args)
{
    int32_t avail;
    size_t  nprinted;

    /*
     * If there's hardly any space, don't bother trying, just fail to make the
     * caller enlarge the buffer first.  We have to guess at how much to
     * enlarge, since we're skipping the formatting work.
     */
    avail = str->maxlen - str->len;
    if (avail < 16)
    {
        return 32;
    }

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
 * pg_parser_appendStringInfoString
 *
 * Append a null-terminated string to str.
 * Like pg_parser_appendStringInfo(str, "%s", s) but faster.
 */
void pg_parser_appendStringInfoString(pg_parser_StringInfo str, const char* s)
{
    pg_parser_appendBinaryStringInfo(str, s, strlen(s));
}

/*
 * pg_parser_appendStringInfoSpaces
 *
 * Append the specified number of spaces to a buffer.
 */
void pg_parser_appendStringInfoSpaces(pg_parser_StringInfo str, int32_t count)
{
    if (count > 0)
    {
        /* Make more room if needed */
        pg_parser_enlargeStringInfo(str, count);

        /* OK, append the spaces */
        while (--count >= 0)
        {
            str->data[str->len++] = ' ';
        }
        str->data[str->len] = '\0';
    }
}

/*
 * pg_parser_appendBinaryStringInfo
 *
 * Append arbitrary binary data to a pg_parser_StringInfo, allocating more space
 * if necessary. Ensures that a trailing null byte is present.
 */
void pg_parser_appendBinaryStringInfo(pg_parser_StringInfo str, const char* data, int32_t datalen)
{
    /* Make more room if needed */
    pg_parser_enlargeStringInfo(str, datalen);

    /* OK, append the data */
    rmemcpy0(str->data, str->len, data, datalen);
    str->len += datalen;

    /*
     * Keep a trailing null in place, even though it's probably useless for
     * binary data.  (Some callers are dealing with pg_parser_text but call this because
     * their input isn't null-terminated.)
     */
    str->data[str->len] = '\0';
}

/*
 * pg_parser_enlargeStringInfo
 *
 * Make sure there is enough space for 'needed' more bytes
 * ('needed' does not include the terminating null).
 *
 * External callers usually need not concern themselves with this, since
 * all stringinfo.c routines do it automatically.  However, if a caller
 * knows that a pg_parser_StringInfo will eventually become X bytes large, it
 * can save some palloc overhead by enlarging the buffer before starting
 * to store data in it.
 *
 * NB: because we use repalloc() to enlarge the buffer, the string buffer
 * will remain allocated in the same memory context that was current when
 * pg_parser_initStringInfo was called, even if another context is now current.
 * This is the desired and indeed critical behavior!
 */
void pg_parser_enlargeStringInfo(pg_parser_StringInfo str, int32_t needed)
{
    int32_t newlen;

    /*
     * Guard against out-of-range "needed" values.  Without this, we can get
     * an overflow or infinite loop in the following.
     */
    if (needed < 0) /* should not happen */
    {
        return;
    }
    if (((size_t)needed) >= (STRINFO_MaxAllocSize - (size_t)str->len))
    {
        return;
    }

    needed += str->len + 1; /* total space required now */

    /* Because of the above test, we now have needed <= MaxAllocSize */

    if (needed <= str->maxlen)
    {
        return; /* got enough space already */
    }

    /*
     * We don't want to allocate just a little more space with each append;
     * for efficiency, double the buffer size each time it overflows.
     * Actually, we might need to more than double it if 'needed' is big...
     */
    newlen = 2 * str->maxlen;
    while (needed > newlen)
    {
        newlen = 2 * newlen;
    }

    /*
     * Clamp to MaxAllocSize in case we went past it.  Note we are assuming
     * here that MaxAllocSize <= INT_MAX/2, else the above loop could
     * overflow.  We will still have newlen >= needed.
     */
    if (newlen > (int32_t)STRINFO_MaxAllocSize)
    {
        newlen = (int32_t)STRINFO_MaxAllocSize;
    }

    if (!pg_parser_mcxt_realloc(STRINGINFO_MCXT, (void**)&(str->data), newlen))
    {
        /* todo error handling */
        str->maxlen = 0;
    }
    else
    {
        str->maxlen = newlen;
    }
}

/*
 * pg_parser_appendStringInfoChar
 *
 * Append a single byte to str.
 * Like pg_parser_appendStringInfo(str, "%c", ch) but much faster.
 */
void pg_parser_appendStringInfoChar(pg_parser_StringInfo str, char ch)
{
    /* Make more room if needed */
    if (str->len + 1 >= str->maxlen)
    {
        pg_parser_enlargeStringInfo(str, 1);
    }

    /* OK, append the character */
    str->data[str->len] = ch;
    str->len++;
    str->data[str->len] = '\0';
}
