#include "app_incl.h"
#include "utils/string/strtrim.h"

/* Trim spaces and tabs from left */
char* leftstrtrim(char* str, int maxsize)
{
    int   lineoffset = 0;
    char* cptr = NULL;

    if (NULL == str)
    {
        return NULL;
    }

    /* Remove left spaces */
    cptr = str;
    for (lineoffset = 0; lineoffset < maxsize; lineoffset++)
    {
        if (' ' == *cptr || '\t' == *cptr)
        {
            cptr++;
            continue;
        }
        break;
    }

    if (lineoffset == maxsize || '\0' == *cptr)
    {
        str[0] = '\0';
        return str;
    }
    /* Move data */
    memmove(str, str + lineoffset, maxsize - lineoffset);
    str[maxsize - lineoffset - 1] = '\0';
    return str;
}

/* Trim spaces and tabs from right */
char* rightstrtrim(char* str)
{
    int   len = 0;
    char* cptr = NULL;

    if (NULL == str)
    {
        return NULL;
    }

    cptr = str;
    len = strlen(str);
    for (--len; 0 <= len; len--)
    {
        if (' ' == cptr[len] || '\t' == cptr[len] || '\r' == cptr[len] || '\n' == cptr[len])
        {
            cptr[len] = '\0';
            continue;
        }
        break;
    }

    return str;
}