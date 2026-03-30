#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdarg.h>
#include <unistd.h>
#include <memory.h>
#include <errno.h>

#include "app_c.h"
#include "pgcastor_exbufferdata.h"

/* initialize content */
bool pgcastor_exbufferdata_initdata(pgcastor_exbuffer exbuffer)
{
    exbuffer->data = (char*)malloc(PGCASTOR_EXPBUFFER_DEFAULTSIZE);
    if (NULL == exbuffer->data)
    {
        return false;
    }
    exbuffer->maxlen = PGCASTOR_EXPBUFFER_DEFAULTSIZE;
    memset(exbuffer->data, '\0', exbuffer->maxlen);
    exbuffer->len = 0;

    return true;
}

/* create an ex buffer */
pgcastor_exbuffer pgcastor_exbufferdata_init(void)
{
    pgcastor_exbuffer exbuffer = NULL;

    exbuffer = (pgcastor_exbuffer)malloc(sizeof(pgcastor_exbufferdata));
    if (NULL == exbuffer)
    {
        return NULL;
    }
    memset(exbuffer, 0, sizeof(pgcastor_exbufferdata));

    if (false == pgcastor_exbufferdata_initdata(exbuffer))
    {
        free(exbuffer);
        return NULL;
    }

    return exbuffer;
}

/* reset */
bool pgcastor_exbufferdata_reset(pgcastor_exbuffer exbuffer)
{
    if (NULL == exbuffer)
    {
        return true;
    }

    if (NULL == exbuffer->data)
    {
        if (false == pgcastor_exbufferdata_initdata(exbuffer))
        {
            return false;
        }
    }

    exbuffer->len = 0;
    memset(exbuffer->data, '\0', exbuffer->maxlen);
    return true;
}

/*
 * expand buffer
 *  no length or input parameter checking
 */
bool pgcastor_exbufferdata_enlarge(pgcastor_exbuffer exbuffer, size_t needed)
{
    size_t newlen = 0;
    char*  newdata = NULL;

    /* required space */
    needed += exbuffer->len + 1;
    if (needed <= exbuffer->maxlen)
    {
        return true;
    }

    newlen = (exbuffer->maxlen > 0) ? (2 * exbuffer->maxlen) : 64;
    while (needed > newlen)
    {
        newlen = 2 * newlen;
    }

    newdata = (char*)realloc(exbuffer->data, newlen);
    if (newdata != NULL)
    {
        exbuffer->data = newdata;
        exbuffer->maxlen = newlen;
        return true;
    }

    return false;
}

static bool pgcastor_exbufferdata_appendva(pgcastor_exbuffer exbuffer, const char* fmt, bool* enlargememory, va_list args)
{
    int    nprinted;
    size_t avail;
    size_t needed;

    *enlargememory = false;
    if (exbuffer->maxlen > exbuffer->len + 16)
    {
        avail = exbuffer->maxlen - exbuffer->len;
        nprinted = vsnprintf(exbuffer->data + exbuffer->len, avail, fmt, args);
        if ((size_t)nprinted < avail)
        {
            exbuffer->len += nprinted;
            return true;
        }

        return false;
    }
    else
    {
        needed = 32;
    }

    /* Increase the buffer size and try again. */
    if (false == pgcastor_exbufferdata_enlarge(exbuffer, needed))
    {
        return false;
    }

    *enlargememory = true;
    return false;
}

/* append content */
bool pgcastor_exbufferdata_append(pgcastor_exbuffer exbuffer, const char* fmt, ...)
{
    bool    done = false;
    bool    enlargememory = true;
    int     save_errno = errno;
    va_list args;

    do
    {
        errno = save_errno;
        if (false == enlargememory)
        {
            return false;
        }
        va_start(args, fmt);

        done = pgcastor_exbufferdata_appendva(exbuffer, fmt, &enlargememory, args);
        va_end(args);
    } while (false == done);

    return true;
}

/* append string data */
bool pgcastor_exbufferdata_appendbinary(pgcastor_exbuffer exbuffer, const char* data, size_t datalen)
{
    if (false == pgcastor_exbufferdata_enlarge(exbuffer, datalen))
    {
        return false;
    }

    /* OK, append the data */
    memcpy(exbuffer->data + exbuffer->len, data, datalen);
    exbuffer->len += datalen;

    /*
     * Keep a trailing null in place, even though it's probably useless for
     * binary data...
     */
    exbuffer->data[exbuffer->len] = '\0';

    return true;
}

/* append string */
bool pgcastor_exbufferdata_appendstr(pgcastor_exbuffer exbuffer, const char* data)
{
    return pgcastor_exbufferdata_appendbinary(exbuffer, data, strlen(data));
}

/* append character */
bool pgcastor_exbufferdata_appendchar(pgcastor_exbuffer exbuffer, char ch)
{
    if (false == pgcastor_exbufferdata_enlarge(exbuffer, 1))
    {
        return false;
    }

    exbuffer->data[exbuffer->len] = ch;
    exbuffer->len++;
    exbuffer->data[exbuffer->len] = '\0';
    return true;
}

/* free buffer */
void pgcastor_exbufferdata_free(pgcastor_exbuffer exbuffer)
{
    if (NULL == exbuffer)
    {
        return;
    }

    if (NULL != exbuffer->data)
    {
        free(exbuffer->data);
    }
    exbuffer->len = 0;
    exbuffer->maxlen = 0;
    exbuffer->data = NULL;
    free(exbuffer);
}