#ifndef _UINT64ARRAY_H_
#define _UINT64ARRAY_H_

typedef struct UINT64ARRAY
{
    /* Number of uint64 in data */
    uint64 cnt;

    /* data length */
    uint64 len;

    /* For quick sort */
    uint8* data;
} uint64array;

uint64array* uint64array_init(void);

/* Add a new value */
bool uint64array_add(uint64array* u64a, uint64 value);

/* Sort */
void uint64array_qsort(uint64array* u64a);

/* Reclaim */
void uint64array_free(uint64array* u64a);

#endif
