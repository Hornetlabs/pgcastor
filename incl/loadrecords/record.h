#ifndef _RECORD_H_
#define _RECORD_H_

#define RECORD_TAIL_LEN          128

/*
 * 在传统的 record 中，正常一个 record 会包含三项内容:
 * 1、record 头部
 * 2、record 中的数据
 * 3、record 的尾部
 * 
 * 在 trail 文件中遵守了上述的设计
 * 在 wal 文件中, 只含有 record 头部和 record 中的数据
 * 所以在对 wal 和 trail 文件的处理中应注意区分此处的区别
*/

typedef enum RECORD_TYPE
{
    RECORD_TYPE_NOP              = 0x00,
    RECORD_TYPE_WAL_NORMAL       ,
    RECORD_TYPE_WAL_CROSS        ,
    RECORD_TYPE_WAL_CONT         ,
    RECORD_TYPE_WAL_TIMELINE     ,
    RECORD_TYPE_TRAIL_HEADER     ,
    RECORD_TYPE_TRAIL_DBMETA     ,
    RECORD_TYPE_TRAIL_NORMAL     ,
    RECORD_TYPE_TRAIL_CROSS      ,
    RECORD_TYPE_TRAIL_CONT       ,
    RECORD_TYPE_TRAIL_RESET      ,
    RECORD_TYPE_TRAIL_TAIL       
} record_type;

typedef struct RECORD
{
    int                                 type;
    recpos                       start;
    recpos                       end;
    recpos                       orgpos;

    /* 
     * data 数据的长度,当前 record 的长度
     *  当在 trail cross record 中时, 此长度会被裁剪, 裁剪为不包含RECTAIL的长度
     */
    uint64                              len;

    /* 真实数据基于 data 头部的偏移, 排除 record 头部等无用信息 */
    /* data 中有效数据的总长度 */
    uint64                              totallength;

    /* data 中有效数据的长度 */
    uint16                              reallength;

    /* data 中有效数据基于 data 头的偏移 */
    uint64                              dataoffset;

    /* for debug */
    uint64                              debugno;
    uint8*                              data;
} record;


typedef struct RECORDCROSS
{
    /* uint16 尾部长度 */
    uint16                              rectaillen;

    /* 总长度:
     *  trail 文件:包含首个 crossrecord 的头部
     */
    uint64                              totallen;

    /* 组装一个完整 record 还差多少字节 */
    uint64                              remainlen;

    /* 
     * 尾部的数据
     *  trail: 含有一个尾部的长度
     */
    uint8                               rectail[RECORD_TAIL_LEN];

    record*                      record;
} recordcross;

record* record_init(void);

/* record 释放 */
void record_free(record* rec);

/* recordcross初始化 */
recordcross* recordcross_init(void);

/* recordcross释放 */
void recordcross_free(recordcross* rec_cross);

/* record 释放 */
void record_freevoid(void* args);

#endif
