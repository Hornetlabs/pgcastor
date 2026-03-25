#include "app_incl.h"
#include "utils/uint64array/uint64array.h"

#define UINT64_ARRAY_LEN 1024
#define UINT64_ARRAY_CNT2SIZE(cnt) ((cnt) * (uint64)8)

/* Initialization function */
uint64array* uint64array_init(void)
{
    uint64array* u64a = NULL;

    u64a = rmalloc0(sizeof(uint64array));
    if (NULL == u64a)
    {
        elog(RLOG_WARNING, "uint64array init error");
        return NULL;
    }
    rmemset0(u64a, 0, '\0', sizeof(uint64array));

    u64a->cnt = 0;
    u64a->len = UINT64_ARRAY_LEN;
    u64a->data = rmalloc0(u64a->len);
    if (NULL == u64a->data)
    {
        rfree(u64a);
        elog(RLOG_WARNING, "uint64array init error");
        return NULL;
    }
    rmemset0(u64a->data, 0, '\0', u64a->len);

    return u64a;
}

/* Add a new value */
bool uint64array_add(uint64array* u64a, uint64 value)
{
    uint64 cnt = 0;
    uint64 nsize = 0;

    if (NULL == u64a)
    {
        return false;
    }

    cnt = u64a->cnt;
    cnt++;
    if (u64a->len < UINT64_ARRAY_CNT2SIZE(cnt))
    {
        /* Need to realloc */
        nsize = u64a->len;
        nsize += UINT64_ARRAY_LEN;

        u64a->data = rrealloc0(u64a->data, nsize);
        if (NULL == u64a->data)
        {
            elog(RLOG_WARNING, "ralloc out of memory");
            return false;
        }
        rmemset1(u64a->data, u64a->len, '\0', UINT64_ARRAY_LEN);
        u64a->len = nsize;
    }
    *(((uint64*)u64a->data) + u64a->cnt) = value;
    u64a->cnt++;
    return true;
}

/*
 * Quicksort comparison algorithm
 *  Return value description:
 *      < 0                 Element pointed by s1 will be placed to the left of element pointed by
 * s2 = 0                 Order of element pointed by s1 relative to element pointed by s2 is
 * undefined > 0                 Element pointed by s1 will be placed to the right of element
 * pointed by s2
 */
static int uint64array_cmp(const void* s1, const void* s2)
{
    uint64 v1 = 0;
    uint64 v2 = 0;
    v1 = *((uint64*)s1);
    v2 = *((uint64*)s2);

    if (v1 == v2)
    {
        return 0;
    }
    else if (v1 > v2)
    {
        return 1;
    }
    else
    {
        return -1;
    }
}

/* Comparison, for quicksort */
void uint64array_qsort(uint64array* u64a)
{
    if (NULL == u64a)
    {
        return;
    }

    qsort(u64a->data, u64a->cnt, sizeof(uint64), uint64array_cmp);
}

/* Free */
void uint64array_free(uint64array* u64a)
{
    if (NULL == u64a)
    {
        return;
    }

    if (NULL != u64a->data)
    {
        rfree(u64a->data);
    }

    rfree(u64a);
}
