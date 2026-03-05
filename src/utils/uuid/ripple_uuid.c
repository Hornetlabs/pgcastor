#include "ripple_app_incl.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/string/stringinfo.h"

static bool
random_from_file(const char *filename, void *buf, size_t len)
{
    int     f;
    char   *p = buf;
    ssize_t res = 0;

    f = open(filename, O_RDONLY, 0);
    if (f == -1)
    {
        return false;
    }

    while (len)
    {
        res = read(f, p, len);
        if (res <= 0)
        {
            if (errno == EINTR)
            {
                continue;        /* interrupted by signal, just retry */
            }

            close(f);
            return false;
        }

        p += res;
        len -= res;
    }

    close(f);
    return true;
}

static bool ripple_strong_random(void *buf, size_t len)
{
    if (random_from_file("/dev/urandom", buf, len))
    {
        return true;
    }
    return false;
}

/* uuid初始化 */
ripple_uuid_t *ripple_uuid_init(void)
{
    ripple_uuid_t *result = rmalloc0(RIPPLE_UUID_LEN);
    if (!result)
    {
        elog(RLOG_WARNING, "malloc uuid oom");
        return NULL;
    }
    rmemset0(result, 0, 0, sizeof(ripple_uuid_t));
    return result;
}

ripple_uuid_t *ripple_random_uuid(void)
{
    ripple_uuid_t *result = rmalloc0(RIPPLE_UUID_LEN);
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(ripple_uuid_t));

    /* Generate random bits. */
    if (!ripple_strong_random((void *)result->data, RIPPLE_UUID_LEN))
    {
        elog(RLOG_ERROR, "can't gen uuid");
    }

    /*
     * Set magic numbers for a "version 4" (pseudorandom) UUID, see
     * http://tools.ietf.org/html/rfc4122#section-4.4
     */
    result->data[6] = (result->data[6] & 0x0f) | 0x40;    /* "version" field */
    result->data[8] = (result->data[8] & 0x3f) | 0x80;    /* "variant" field */

    return result;
}

/* uuid拷贝 */
ripple_uuid_t *ripple_uuid_copy(ripple_uuid_t *uuid)
{
    ripple_uuid_t *result = rmalloc0(RIPPLE_UUID_LEN);
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(ripple_uuid_t));

    rmemcpy0(result, 0, uuid, sizeof(ripple_uuid_t));

    return result;
}

void ripple_uuid_free(ripple_uuid_t *uuid)
{
    rfree(uuid);
}

char *uuid2string(ripple_uuid_t *uuid)
{
    static const char hex_chars[] = "0123456789abcdef";
    StringInfoData buf;
    int            i;
    char *result = NULL;

    initStringInfo(&buf);
    for (i = 0; i < RIPPLE_UUID_LEN; i++)
    {
        int            hi;
        int            lo;

        /*
         * We print uuid values as a string of 8, 4, 4, 4, and then 12
         * hexadecimal characters, with each group is separated by a hyphen
         * ("-"). Therefore, add the hyphens at the appropriate places here.
         */
        if (i == 4 || i == 6 || i == 8 || i == 10)
            appendStringInfoChar(&buf, '-');

        hi = uuid->data[i] >> 4;
        lo = uuid->data[i] & 0x0F;

        appendStringInfoChar(&buf, hex_chars[hi]);
        appendStringInfoChar(&buf, hex_chars[lo]);
    }

    result = rstrdup(buf.data);
    rfree(buf.data);

    return result;
}
