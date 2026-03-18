#include <math.h>

#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "thirdparty/common/pg_parser_thirdparty_snprintf.h"

#define Min(x, y) ((x) < (y) ? (x) : (y))

static void dostr(const char* str, int32_t slen, PrintfTarget* target);
static void flushbuffer(PrintfTarget* target);
static void dopr_outch(int32_t c, PrintfTarget* target);

static void dostr(const char* str, int32_t slen, PrintfTarget* target)
{
    /* fast path for common case of slen == 1 */
    if (slen == 1)
    {
        dopr_outch(*str, target);
        return;
    }

    while (slen > 0)
    {
        int32_t avail;

        if (target->bufend != NULL)
        {
            avail = target->bufend - target->bufptr;
        }
        else
        {
            avail = slen;
        }
        if (avail <= 0)
        {
            /* buffer full, can we dump to stream? */
            if (target->stream == NULL)
            {
                target->nchars += slen; /* no, lose the data */
                return;
            }
            flushbuffer(target);
            continue;
        }
        avail = Min(avail, slen);
        memmove(target->bufptr, str, avail);
        target->bufptr += avail;
        str += avail;
        slen -= avail;
    }
}

static void flushbuffer(PrintfTarget* target)
{
    size_t nc = target->bufptr - target->bufstart;

    /*
     * Don't write anything if we already failed; this is to ensure we
     * preserve the original failure's errno.
     */
    if (!target->failed && nc > 0)
    {
        size_t written;

        written = fwrite(target->bufstart, 1, nc, target->stream);
        target->nchars += written;
        if (written != nc)
        {
            target->failed = true;
        }
    }
    target->bufptr = target->bufstart;
}

static void dopr_outch(int32_t c, PrintfTarget* target)
{
    if (target->bufend != NULL && target->bufptr >= target->bufend)
    {
        /* buffer full, can we dump to stream? */
        if (target->stream == NULL)
        {
            target->nchars++; /* no, lose the data */
            return;
        }
        flushbuffer(target);
    }
    *(target->bufptr++) = c;
}

/*
 * Nonstandard entry point to print a double value efficiently.
 *
 * This is approximately equivalent to strfromd(), but has an API more
 * adapted to what float8out() wants.  The behavior is like snprintf()
 * with a format of "%.ng", where n is the specified precision.
 * However, the target buffer must be nonempty (i.e. count > 0), and
 * the precision is silently bounded to a sane range.
 */
int32_t pg_parser_strfromd(char* str, size_t count, int32_t precision, double value)
{
    PrintfTarget target;
    int32_t      signvalue = 0;
    int32_t      vallen;
    char         fmt[8];
    char         convert[64];

    /* Set up the target like pg_snprintf, but require nonempty buffer */
    target.bufstart = target.bufptr = str;
    target.bufend = str + count - 1;
    target.stream = NULL;
    target.nchars = 0;
    target.failed = false;

    /*
     * We bound precision to a reasonable range; the combination of this and
     * the knowledge that we're using "g" format without padding allows the
     * convert[] buffer to be reasonably small.
     */
    if (precision < 1)
    {
        precision = 1;
    }
    else if (precision > 32)
    {
        precision = 32;
    }

    /*
     * The rest is just an inlined version of the fmtfloat() logic above,
     * simplified using the knowledge that no padding is wanted.
     */
    if (isnan(value))
    {
        strcpy(convert, "NaN");
        vallen = 3;
    }
    else
    {
        static const double dzero = 0.0;

        if (value < 0.0 || (value == 0.0 && memcmp(&value, &dzero, sizeof(double)) != 0))
        {
            signvalue = '-';
            value = -value;
        }

        if (isinf(value))
        {
            strcpy(convert, "Infinity");
            vallen = 8;
        }
        else
        {
            fmt[0] = '%';
            fmt[1] = '.';
            fmt[2] = '*';
            fmt[3] = 'g';
            fmt[4] = '\0';
            vallen = sprintf(convert, fmt, precision, value);
            if (vallen < 0)
            {
                target.failed = true;
                goto pg_parser_thirdparty_snprintf_fail;
            }

#ifdef WIN32
            if (vallen >= 6 && convert[vallen - 5] == 'e' && convert[vallen - 3] == '0')
            {
                convert[vallen - 3] = convert[vallen - 2];
                convert[vallen - 2] = convert[vallen - 1];
                vallen--;
            }
#endif
        }
    }

    if (signvalue)
    {
        dopr_outch(signvalue, &target);
    }

    dostr(convert, vallen, &target);

pg_parser_thirdparty_snprintf_fail:
    *(target.bufptr) = '\0';
    return target.failed ? -1 : (target.bufptr - target.bufstart + target.nchars);
}