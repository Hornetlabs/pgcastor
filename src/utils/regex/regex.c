#include "app_incl.h"
#include "utils/regex/regex.h"

bool cmp_regexbase(regex* regex, char* name)
{
    size_t len = 0;
    if (regex->blike == false)
    {
        /* No wildcard matching */
        if (NULL == regex->tokenbefore)
        {
            return false;
        }
        if (strcmp(regex->tokenbefore, name) == 0)
        {
            return true;
        }
        else
        {
            return false;
        }
    }
    /* Wildcard with both sides empty '_*_' */
    if (regex->tokenbefore == NULL && regex->tokenafter == NULL)
    {
        return true;
    }
    else if (regex->tokenbefore == NULL && regex->tokenafter != NULL)
    {
        /* "*aaa" comparison */
        len = strlen(name) - strlen(regex->tokenafter);
        if (strncmp(regex->tokenafter, name + len, strlen(regex->tokenafter)) == 0)
        {
            return true;
        }
    }
    else if (regex->tokenbefore != NULL && regex->tokenafter == NULL)
    {
        /* "aaa*" comparison */
        if (strncmp(regex->tokenbefore, name, strlen(regex->tokenbefore)) == 0)
        {
            return true;
        }
    }
    else
    {
        /* "aa*aa" comparison */
        if (strncmp(regex->tokenbefore, name, strlen(regex->tokenbefore)) == 0)
        {
            len = strlen(name) - strlen(regex->tokenafter);
            if (strncmp(regex->tokenafter, name + len, strlen(regex->tokenafter)) == 0)
            {
                return true;
            }
        }
    }
    return false;
}

void make_regexbase(regex* regex, char* rule)
{
    char* uptr = rule;
    int   pos = 0;
    int   len = 0;
    /* Find data before *, record length */
    while ('\0' != *uptr)
    {
        if ('*' == *uptr)
        {
            /* Skip * */
            regex->blike = true;
            uptr++;
            break;
        }
        len++;
        uptr++;
    }
    if (len == 0)
    {
        regex->tokenbefore = NULL;
        len += 1;
    }
    else
    {
        /* Assign tokenbefore */
        len += 1;
        regex->tokenbefore = (char*)rmalloc0(len);
        if (NULL == regex->tokenbefore)
        {
            elog(RLOG_ERROR, "out of memory");
        }
        rmemset0(regex->tokenbefore, 0, '\0', len);
        rmemcpy0(regex->tokenbefore, 0, rule, len);
        regex->tokenbefore[len - 1] = '\0';
    }

    if ('\0' == *uptr)
    {
        regex->tokenafter = NULL;
    }
    else
    {
        while ('\0' != *uptr)
        {
            /* Record length after * */
            pos++;
            uptr++;
        }
        pos += 1;
        regex->tokenafter = (char*)rmalloc0(pos);
        if (NULL == regex->tokenafter)
        {
            elog(RLOG_ERROR, "out of memory");
        }
        rmemset0(regex->tokenafter, 0, '\0', pos);
        rmemcpy0(regex->tokenafter, 0, rule + len, pos);
        regex->tokenafter[pos - 1] = '\0';
    }
}

void free_regexbase(regex* regex)
{
    if (NULL == regex)
    {
        return;
    }

    if (NULL != regex->tokenbefore)
    {
        rfree(regex->tokenbefore);
    }

    if (NULL != regex->tokenafter)
    {
        rfree(regex->tokenafter);
    }

    rfree(regex);
}
