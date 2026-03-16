#ifndef RIPPLE_LOADWALRECORDS_H
#define RIPPLE_LOADWALRECORDS_H

typedef enum RIPPLE_LOADWALRECORDS_STATUS
{
    RIPPLE_LOADWALRECORDS_STATUS_INIT = 0,
    RIPPLE_LOADWALRECORDS_STATUS_REWIND,
    RIPPLE_LOADWALRECORDS_STATUS_NORMAL
} ripple_loadwalrecords_status;

typedef struct RIPPLE_LOADWALRECORDS
{
    ripple_loadrecords      loadrecords;

    /* wal 文件版本 */
    bool        need_decrypt;   /* 需要解密 */
    TimeLineID  timeline;       /* 时间线 */
    XLogRecPtr  block_startptr; /* block开始的lsn */
    XLogRecPtr  startptr;       /* 开始的lsn */
    XLogRecPtr  endptr;         /* 结束的lsn */
    XLogRecPtr  prev;           /* 最后一条已划分record的lsn */

    mpage*      page;           /* 读取缓冲区 */

    ripple_recordcross*     page_last_record_incomplete;    /* (若存在) 解析到的页的最后一条不完整record* */
    ripple_recordcross*     seg_first_incomplete;           /* (若存在) 当前文件第一条不完整record* */
    ripple_recordcross*     seg_first_incomplete_next;      /* (若存在) 下一个wal文件第一条不完整record*/

    dlist*                  records;  /* 划分完的完整record链表 */
    ripple_loadpage*        loadpage;
    ripple_loadpageroutine* loadpageroutine;
} ripple_loadwalrecords;

/* 初始化 */
extern ripple_loadwalrecords* ripple_loadwalrecords_init(void);

extern void ripple_loadwalrecords_free(ripple_loadwalrecords* loadrecords);

extern bool ripple_loadwalrecords_load(ripple_loadwalrecords* loadrecords);

extern void ripple_loadwalrecords_clean(ripple_loadwalrecords* loadrecords);

extern bool ripple_loadwalrecords_merge_seg_last_record(ripple_loadwalrecords *rctl);

extern bool ripple_loadwalrecords_checkend(XLogRecPtr cur, ripple_loadwalrecords *rctl);

#endif
