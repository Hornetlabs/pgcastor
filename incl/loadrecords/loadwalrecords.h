#ifndef LOADWALRECORDS_H
#define LOADWALRECORDS_H

typedef enum LOADWALRECORDS_STATUS
{
    LOADWALRECORDS_STATUS_INIT = 0,
    LOADWALRECORDS_STATUS_REWIND,
    LOADWALRECORDS_STATUS_NORMAL
} loadwalrecords_status;

typedef struct LOADWALRECORDS
{
    loadrecords      loadrecords;

    /* wal 文件版本 */
    bool        need_decrypt;   /* 需要解密 */
    TimeLineID  timeline;       /* 时间线 */
    XLogRecPtr  block_startptr; /* block开始的lsn */
    XLogRecPtr  startptr;       /* 开始的lsn */
    XLogRecPtr  endptr;         /* 结束的lsn */
    XLogRecPtr  prev;           /* 最后一条已划分record的lsn */

    mpage*      page;           /* 读取缓冲区 */

    recordcross*     page_last_record_incomplete;    /* (若存在) 解析到的页的最后一条不完整record* */
    recordcross*     seg_first_incomplete;           /* (若存在) 当前文件第一条不完整record* */
    recordcross*     seg_first_incomplete_next;      /* (若存在) 下一个wal文件第一条不完整record*/

    dlist*                  records;  /* 划分完的完整record链表 */
    loadpage*        loadpage;
    loadpageroutine* loadpageroutine;
} loadwalrecords;

/* 初始化 */
extern loadwalrecords* loadwalrecords_init(void);

extern void loadwalrecords_free(loadwalrecords* loadrecords);

extern bool loadwalrecords_load(loadwalrecords* loadrecords);

extern void loadwalrecords_clean(loadwalrecords* loadrecords);

extern bool loadwalrecords_merge_seg_last_record(loadwalrecords *rctl);

extern bool loadwalrecords_checkend(XLogRecPtr cur, loadwalrecords *rctl);

#endif
