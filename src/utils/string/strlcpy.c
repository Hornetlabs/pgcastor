/*-------------------------------------------------------------------------
 *
 * strlcpy.c
 *      strlcpy.c supports strlcpy function
 *
 *
 * All Copyright (c) 2024-2024, Byte Sync Development Group
 *
 *
 * src/utils/string/strlcpy.c
 *
 *-------------------------------------------------------------------------
 */
#include "app_incl.h"
#include "utils/string/strlcpy.h"

/*
 * Copy src to string dst of size siz.  At most siz-1 characters
 * will be copied.  Always NUL terminates (unless siz == 0).
 * Returns strlen(src); if retval >= siz, truncation occurred.
 * Function creation history:  http://www.gratisoft.us/todd/papers/strlcpy.html
 */
size_t strlcpy(char* dst, const char* src, size_t siz)
{
    char*       d = dst;
    const char* s = src;
    size_t      n = siz;

    /* Copy as many bytes as will fit */
    if (n != 0)
    {
        while (--n != 0)
        {
            if ((*d++ = *s++) == '\0')
            {
                break;
            }
        }
    }

    /* Not enough room in dst, add NUL and traverse rest of src */
    if (n == 0)
    {
        if (siz != 0)
        {
            *d = '\0'; /* NUL-terminate dst */
        }
        while (*s++)
            ;
    }

    return (s - src - 1); /* count does not include NUL */
}