#ifndef _STORAGE_FILE_BUFFER_H
#define _STORAGE_FILE_BUFFER_H

typedef enum FILE_BUFFER_FLAG
{
    FILE_BUFFER_FLAG_NOP                     = 0x00,
    FILE_BUFFER_FLAG_DATA                    = 0x01,     /* 含有数据，代表 data 中的内容含有数据 */
    FILE_BUFFER_FLAG_REDO                    = 0x02,     /* 该 buffer 中包含了 redolsn，在 extra 数据中含有自上个checkpoint到当前的checkpoint所有的系统表变化 */
    FILE_BUFFER_FLAG_REWIND                  = 0x04,     /* 该 buffer 中包含了 restartlsn 和 confirmlsn，在 extra 数据中含有 restartlsn 和 confirmlsn */
    FILE_BUFFER_FLAG_ONLINREFRESHEND         = 0x08,
    FILE_BUFFER_FLAG_ONLINREFRESH_DATASET    = 0x10,
    FILE_BUFFER_FLAG_BIGTXNEND               = 0x20,      /* 大事务提交标识       */
} file_buffer_flag;

#define INVALID_BUFFERID                 0

typedef struct FILE_BUFFER_EXTRA_CHECKPOINT
{
    recpos           redolsn;                    /* redolsn              */
    recpos           orgaddr;                    /* trail 文件的信息   */
    recpos           segno;                      /* segno              */
    List*                   sysdicts;                   /* 待应用的系统表         */
} file_buffer_extra_checkpoint;

typedef struct FILE_BUFFER_EXTRA_REWIND
{
    recpos           restartlsn;                 /* 开始解析的 lsn                                */
    recpos           confirmlsn;                 /* 应用的起点 lsn                                */
    recpos           flushlsn;                   /* 用于记录刷新到文件的 lsn                        */
    recpos           fileaddr;                   /* 写入到的文件编号/写入到的基于文件头的偏移          */
    TimeLineID              curtlid;
} file_buffer_extra_rewind;

typedef struct FILE_BUFFER_EXTRA_ONLINEDATASET
{
    List                   *dataset;                    /* 存放refresh_table */
} file_buffer_extra_onlinedataset;

typedef struct FILE_BUFFER_EXTRA
{
    file_buffer_extra_rewind         rewind;
    file_buffer_extra_checkpoint     chkpoint;
    file_buffer_extra_onlinedataset  dataset;
    TimestampTz                             timestamp;
} file_buffer_extra;

typedef struct FILE_BUFFER
{
    bool                            used;       /* 标识该块是否被使用        */
    int                             bufid;      /* 块标识                  */
    int                             flag;       /* 标识信息                */
    uint64                          maxsize;    /* data 的空间             */
    uint64                          start;      /* data 可用的偏移          */
                                                /* 在落盘线程中会判断 start 是否为0,为0时则不执行落盘操作 */
    file_buffer_extra        extra;      /* 附加信息                 */
    void*                           privdata;   /* 私有数据                 */
                                                /* 当前的保存内容为 ff_fileinfo */
    uint8*                          data;       /* 数据库空间               */
    struct FILE_BUFFER*      next;       /* 后节点                   */
    struct FILE_BUFFER*      tail;       /* 最后的节点                */
} file_buffer;

typedef struct FILE_BUFFERS
{
    bool                    flwsignal;                  /* 空闲链表等待信号 */
    bool                    wflwsignal;                 /* 待刷新缓存等待信号 */
    int                     maxbufid;                   /* buffers 中含有的 file_buffer 的个数 */
    pthread_cond_t          flcond;
    pthread_cond_t          wflcond;
    pthread_mutex_t         fllock;                     /* 空闲链表锁 */
    pthread_mutex_t         wfllock;                    /* 待刷新缓存锁 */
    file_buffer*     freelist;                   /* 空闲链表                                  */
    file_buffer*     wflushlist;                 /* 等待落盘链表                               */
    file_buffer*     buffers;                    /* buffers 缓存                              */
} file_buffers;

/* 初始化 */
file_buffers* file_buffer_init(void);

/* 根据 bufid 获取 buffer */
file_buffer* file_buffer_getbybufid(file_buffers* filebuffers, int bufid);

/* 获取可用的 buffer */
int file_buffer_get(file_buffers* filebuffers, int* timeout);

/* 将 buffer 放入空闲队列 */
void file_buffer_free(file_buffers* filebuffers, file_buffer* rfbuffer);

/* 
 * 做 copy
 *  需要关注的时 src->privdata 会设置为空
*/
void file_buffer_copy(file_buffer* src, file_buffer* dst);

/* 将 buffer 放入待刷新队列 */
void file_buffer_waitflush_add(file_buffers* filebuffers, file_buffer* fbuffer);

/* 在待刷新缓存中获取buffer */
file_buffer* file_buffer_waitflush_get(file_buffers* filebuffers, int* timeout);

void file_buffer_clean(file_buffers* filebuffers);

void file_buffer_destroy(file_buffers* filebuffers);

/* 清理 waitflush */
void riple_file_buffer_clean_waitflush(file_buffers* filebuffers);

#endif
