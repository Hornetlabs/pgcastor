#ifndef RIPPLE_FASTCOMPARE_SIMPLEDATACHUNK_H
#define RIPPLE_FASTCOMPARE_SIMPLEDATACHUNK_H

#define RIPPLE_FASTCOMPARE_SIMPLEDATACHUNK_MAX_SEND_SIZE 67108864

/* chunk位置标记 */
#define RIPPLE_FASTCOMPARE_SIMPLEDATACHUNK_CHUNK_NORMAL  0x00
#define RIPPLE_FASTCOMPARE_SIMPLEDATACHUNK_CHUNK_FIRST   0x01
#define RIPPLE_FASTCOMPARE_SIMPLEDATACHUNK_CHUNK_LAST    0x02

#define RIPPLE_FASTCOMPARE_SIMPLEDATACHUNK_CHUNK_SET_FIRST(flag)     (flag |= RIPPLE_FASTCOMPARE_SIMPLEDATACHUNK_CHUNK_FIRST)
#define RIPPLE_FASTCOMPARE_SIMPLEDATACHUNK_CHUNK_SET_LAST(flag)      (flag |= RIPPLE_FASTCOMPARE_SIMPLEDATACHUNK_CHUNK_LAST)

#define RIPPLE_FASTCOMPARE_SIMPLEDATACHUNK_CHUNK_IS_FIRST(flag)     (flag & RIPPLE_FASTCOMPARE_SIMPLEDATACHUNK_CHUNK_FIRST)
#define RIPPLE_FASTCOMPARE_SIMPLEDATACHUNK_CHUNK_IS_LAST(flag)      (flag & RIPPLE_FASTCOMPARE_SIMPLEDATACHUNK_CHUNK_LAST)


typedef struct RIPPLE_FASTCOMPARE_SIMPLEDATACHUNK
{
    ripple_fastcompare_chunk    base;
    uint8                       flag;           /* 第一个和最后一个块 */
    uint32                      crc;            /* 所有行crc计算结果 */
    uint32                      datacnt;
    uint32                      size;
    List                       *minprivalues;   /* 最小主键列 */
    List                       *maxprivalues;   /* 最大主键列 */
    List                       *data;           /* 行数据 ripple_fastcompare_simplerow */
} ripple_fastcompare_simpledatachunk;

extern ripple_fastcompare_simpledatachunk *ripple_fastcompare_simpledatachunk_init(void);
extern void ripple_fastcompare_simpledatachunk_clean(ripple_fastcompare_simpledatachunk *chunk);
extern void ripple_fastcompare_simpledatachunk_appendFin(ripple_fastcompare_simpledatachunk *chunk);
extern bool ripple_fastcompare_simpledatachunk_append(ripple_fastcompare_simpledatachunk *chunk,
                                                      PGresult *res,
                                                      List *pkey_define_list,
                                                      int tuple_num);
extern uint8 *ripple_fastcompare_simpledatachunk_serial(ripple_fastcompare_simpledatachunk *chunk,
                                                 uint32 *size);
extern bool ripple_fastcompare_simpledatachunk_send(void* netclient, ripple_fastcompare_simpledatachunk *simpledatachunk);
extern ripple_fastcompare_simpledatachunk* ripple_fastcompare_simpledatachunk_fetchdata(void* privdata, uint8* buffer);
#endif
