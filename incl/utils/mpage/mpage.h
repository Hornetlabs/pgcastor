#ifndef _MPAGE_H_
#define _MPAGE_H_

typedef struct MPAGE
{
    /* data 的长度 */
    uint64_t                size;

    /* 使用到的长度 */
    uint64_t                doffset;

    /* 页编号 */
    uint64_t                pno;

    /* 数据 */
    uint8_t*                data;
} mpage;

#endif

