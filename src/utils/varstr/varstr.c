#include "app_incl.h"
#include "utils/varstr/varstr.h"

/* Initialization */
varstr* varstr_init(uint64 len)
{
    varstr* vstr = NULL;

    vstr = rmalloc0(sizeof(varstr));
    if (NULL == vstr)
    {
        return NULL;
    }
    rmemset0(vstr, 0, '\0', sizeof(varstr));

    /* Allocate content */
    vstr->start = 0;
    vstr->size = len;
    len += 1;
    vstr->data = rmalloc0(len);
    if (NULL == vstr->data)
    {
        rfree(vstr);
        return NULL;
    }
    rmemset0(vstr->data, 0, '\0', len);

    return vstr;
}

/* Reset */
bool varstr_reset(varstr* vstr)
{
    uint64 len = 0;

    if (NULL == vstr)
    {
        return true;
    }

    len = vstr->size + 1;

    if (NULL == vstr->data)
    {
        vstr->data = rmalloc0(len);
        if (NULL == vstr->data)
        {
            return false;
        }
    }

    vstr->start = 0;
    rmemset0(vstr->data, 0, '\0', len);
    return true;
}

/*
 * Expand
 *  No length and input parameter validation
 */
bool varstr_enlarge(varstr* vstr, uint64 needed)
{
    uint64 newlen = 0;
    char*  newdata = NULL;

    /* Space required */
    needed += vstr->start + 1;
    if (needed <= vstr->size)
    {
        return true;
    }

    newlen = (vstr->size > 0) ? (2 * vstr->size) : 64;
    while (needed > newlen)
    {
        newlen = 2 * newlen;
    }

    newdata = (char*)rrealloc0(vstr->data, newlen + 1);
    if (newdata != NULL)
    {
        vstr->data = (uint8_t*)newdata;
        vstr->size = newlen;
        return true;
    }

    return false;
}

static bool varstr_appendva(varstr* vstr, const char* fmt, bool* enlargememory, va_list args)
{
    int    nprinted;
    size_t avail;
    size_t needed;

    *enlargememory = false;
    if (vstr->size > vstr->start + 16)
    {
        avail = vstr->size - vstr->start;
        nprinted = vsnprintf((char*)vstr->data + vstr->start, avail, fmt, args);
        if ((size_t)nprinted < avail)
        {
            vstr->start += nprinted;
            return true;
        }

        return false;
    }
    else
    {
        needed = 32;
    }

    /* Increase the buffer size and try again. */
    if (false == varstr_enlarge(vstr, needed))
    {
        return false;
    }

    *enlargememory = true;
    return false;
}

/* Add content */
bool varstr_append(varstr* vstr, const char* fmt, ...)
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

        done = varstr_appendva(vstr, fmt, &enlargememory, args);
        va_end(args);
    } while (false == done);

    return true;
}

/* Add string */
bool varstr_appendbinary(varstr* vstr, const char* data, uint64 datalen)
{
    if (false == varstr_enlarge(vstr, datalen))
    {
        return false;
    }

    /* OK, append the data */
    rmemcpy0(vstr->data, vstr->start, data, datalen);
    vstr->start += datalen;

    /*
     * Keep a trailing null in place, even though it's probably useless for
     * binary data...
     */
    vstr->data[vstr->start] = '\0';

    return true;
}

/* Concatenate strings */
bool varstr_appendstr(varstr* vstr, const char* data)
{
    return varstr_appendbinary(vstr, data, strlen(data));
}

/* Add character */
bool varstr_appendchar(varstr* vstr, char ch)
{
    if (false == varstr_enlarge(vstr, 1))
    {
        return false;
    }

    vstr->data[vstr->start] = ch;
    vstr->start++;
    vstr->data[vstr->start] = '\0';
    return true;
}

/* Free */
void varstr_free(varstr* vstr)
{
    if (NULL == vstr)
    {
        return;
    }

    if (NULL != vstr->data)
    {
        rfree(vstr->data);
    }
    vstr->start = 0;
    vstr->size = 0;
    vstr->data = NULL;
    rfree(vstr);
}
