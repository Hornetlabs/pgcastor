#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "app_c.h"
#include "xscsci_scansup.h"

static void xscsci_scansup_truncateident(char* ident, int len, bool warn)
{
    /* do not consider character set for now */
    ident[len] = '\0';
}

static char* xscsci_scansup_downcaseident(const char* ident, int len, bool warn, bool truncate)
{
    char* result = NULL;
    int   i = 0;

    result = malloc(len + 1);
    for (i = 0; i < len; i++)
    {
        unsigned char ch = (unsigned char)ident[i];
        if (ch >= 'A' && ch <= 'Z')
        {
            ch += 'a' - 'A';
        }
        result[i] = (char)ch;
    }
    result[i] = '\0';

    if (i >= NAMEDATALEN && truncate)
    {
        xscsci_scansup_truncateident(result, i, warn);
    }

    return result;
}

char* xscsci_scansup_downcasetruncateident(const char* ident, int len, bool warn)
{
    return xscsci_scansup_downcaseident(ident, len, warn, true);
}

/* after excluding characters that don't need attention, check if only semicolon */
bool xscsci_scansup_onlysemicolon(const char* str)
{
    bool first = true;
    int  index = 0;
    int  len = strlen(str);

    for (index = 0; index < len; index++)
    {
        if (' ' == *str || '\r' == *str || '\n' == *str || '\t' == *str || '\f' == *str)
        {
            str++;
            continue;
        }

        if (true == first)
        {
            if (';' != *str)
            {
                return false;
            }
            first = false;
            continue;
        }

        return false;
    }
    return true;
}
