#include <math.h>
#include <limits.h>
#include <ctype.h>
#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"

#include "thirdparty/common/xk_pg_parser_thirdparty_builtins.h"

int strtoint(const char *xk_pg_parser__restrict str, char **xk_pg_parser__restrict endptr, int base)
{
    long        val;

    val = strtol(str, endptr, base);
    if (val != (int) val)
        errno = ERANGE;
    return (int) val;
}