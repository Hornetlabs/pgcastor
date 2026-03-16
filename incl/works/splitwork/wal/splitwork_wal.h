#ifndef _RIPPLE_SPLITWAL_WORK_H
#define _RIPPLE_SPLITWAL_WORK_H

typedef enum ripple_splitwork_wal_status
{
    RIPPLE_SPLITWORK_WAL_STATUS_INIT = 0,
    RIPPLE_SPLITWORK_WAL_STATUS_REWIND,
    RIPPLE_SPLITWORK_WAL_STATUS_NORMAL
} ripple_splitwork_wal_status;

// typedef struct RIPPLE_SPLITWAL_INCOMPLETERECORD
// {
//     uint32      len;                /* 保存原始长度 */
//     uint32      incomplete_len;     /* 已存储长度 */
//     XLogRecPtr  startlsn;           /* 起始lsn, wal文件开始第一条不完整record记为record结束lsn */
//     char       *record;             /* record数据 */
// }ripple_splitwal_incompleteRecord;

// typedef struct RIPPLE_SPLITWAL_PAGEBUFFER
// {
//     uint32              size;                   /* block大小 */
//     XLogRecPtr          startptr;               /* block开始的lsn */
//     char               *buf;                    /* 数据 */
//     ripple_splitwal_incompleteRecord   *incomplete;             /* (若存在) block最后一条不完整record */
// }ripple_splitwal_pageBuffer;

typedef struct TimeLine2lsn
{
    TimeLineID timeline;
    XLogRecPtr lsn;
} TimeLine2lsn;

typedef struct timelineMAP
{
    uint32_t    size;
    List       *map; /* 保存TimeLine2lsn */
} timelineMAP;

// typedef struct RIPPLE_SPLITWAL_WALREADCTL
// {
//     bool        wait;           /* 等待标记 */
//     bool        change;         /* 状态改变标记 */
//     bool        need_decrypt;
//     int         status;         /* 状态信息, rewind或者normal */
//     int         fd;             /* 文件描述符 */
//     TimeLineID  timeline;       /* 时间线 */
//     char       *inpath;         /* wal文件夹路径 */
//     XLogRecPtr  change_startptr; /* 状态切换后开始的lsn */
//     XLogRecPtr  startptr;       /* 开始的lsn */
//     XLogRecPtr  endptr;         /* 结束的lsn */
//     uint32      blcksz;         /* wal文件的block大小 */
//     uint32      walsz;          /* wal文件大小 */
//     XLogRecPtr  prev;           /* 最后一条已划分record的lsn */
//     XLogSegNo sendSegNo;
//     uint32 sendOff;
//     ripple_splitwal_incompleteRecord *seg_first_incomplete;      /* (若存在) 当前文件第一条不完整record* */
//     ripple_splitwal_incompleteRecord *seg_first_incomplete_next; /* (若存在) 下一个wal文件第一条不完整record*/
// } ripple_splitwal_WalReadCtl;

typedef struct RIPPLE_SPLITWALCTX_PRIVDATACALLBACK
{
    /* 设置parser状态为emiting */
    void (*parserwal_rewindstat_setemiting)(void* privdata);

    /* 设置capturestate的拆分到的lsn */
    void (*capturestate_loadlsn_set)(void* privdata, XLogRecPtr splitls);

} ripple_splitwalctx_privdatacallback;

typedef struct RIPPLE_SPLITWALCONTEXT
{
    void                   *privdata;       /* ripple_increment_capture */
    bool                    change;         /* 状态改变标记 */
    int                     status;         /* 状态信息, rewind或者normal */
    XLogRecPtr              change_startptr;/* 状态切换后开始的lsn */
    ripple_queue           *recordqueue;    /* 划分好的record缓存 */
    ripple_loadwalrecords  *loadrecords;    /* 读取records的控制器 */
    XLogRecPtr              rewind_start;   /* rewind起点 */
    ripple_splitwalctx_privdatacallback	callback;
} ripple_splitwalcontext;

extern void* ripple_splitwork_wal_main(void *args);
extern ripple_splitwalcontext *ripple_splitwal_init(void);
extern void ripple_splitwal_destroy(ripple_splitwalcontext *split_wal_ctx);
extern void *ripple_onlinerefresh_captureloadrecord_main(void *args);
extern void ripple_onlinerefresh_captureloadrecord_free(void* args);

#endif
