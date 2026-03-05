#ifndef _UINT64ARRAY_H_
#define _UINT64ARRAY_H_

typedef struct UINT64ARRAY
{
    /* data 中含有 uint64 的个数 */
    uint64              cnt;

    /* data 长度 */
    uint64              len;

    /* 用于快速排序 */
    uint8*              data;
} uint64array;

uint64array* uint64array_init(void);

/* 增加一个新值 */
bool uint64array_add(uint64array* u64a, uint64 value);

/* 排序 */
void uint64array_qsort(uint64array* u64a);

/* 回收 */
void uint64array_free(uint64array* u64a);

#endif
