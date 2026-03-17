#ifndef _MEM_H
#define _MEM_H

typedef struct MALLOC_NODE
{
    uint16                          flag;       /* 全局/局部变量 */
    uint32                          magic;      /* 魔数--判断是否为可用空间 */
    uint64                          number;     /* 编号 */
    uint32                          line;       /* 行号 */
    uint64                          size;       /* 数据大小 */
    char                            file[128];  /* 文件名称 */
    struct MALLOC_NODE*      prev;
    struct MALLOC_NODE*      next;
} malloc_node;


typedef struct MALLOC_NODES
{
    uint64                          number;      /* 编号 */
    pthread_mutex_t                 listlock;   /* 链表锁 */
    malloc_node*             head;       /* 链表头 */
    malloc_node*             tail;       /* 链表尾 */
} malloc_nodes;


typedef enum MEMPRINT_FLAG{
    MEMPRINT_GLOBAL, // 全局变量
    MEMPRINT_LOCAL,  // 局部变量
    MEMPRINT_ALL     // 全部变量
} memprint_flag;

/* 申请空间 */
void* rmalloc(char* file, uint32 line, size_t size, bool local);

/* 初始化 */
void* rmemset(void* s, uint64 offset, int c, size_t n, bool heap);

/* 内存拷贝 */
void* rmemcpy(void* dest, uint64 offset, const void* src, size_t n, bool heap);

/* 重新分配 */
void* rrealloc(char* file, uint32 line, void* ptr, size_t size, bool local);

/* 字符串拷贝 */
char * _rstrdup(char* file, uint32 line, const char *s);


char * _rstrndup(char* file, uint32 line, const char* s, uint32 n);

/* 释放 */
void rfree(void* ptr);

void mem_print(memprint_flag flag);

void mem_init(void);


/* 宏定义 */
#define rmalloc0(size)                          rmalloc(__FILE__, __LINE__, size, true)
#define rmalloc1(size)                          rmalloc(__FILE__, __LINE__, size, false)

#define rmemset0(s, offset, c, n)               rmemset(s, offset, c, n, true)
#define rmemset1(s, offset, c, n)               rmemset(s, offset, c, n, false)


#define rmemcpy0(dest, dstoffset, src, n)       rmemcpy(dest, dstoffset, src, n, true)
#define rmemcpy1(dest, dstoffset, src, n)       rmemcpy(dest, dstoffset, src, n, false)


#define rrealloc0(ptr, size)                    rrealloc(__FILE__, __LINE__, ptr, size, true)
#define rrealloc1(ptr, size)                    rrealloc(__FILE__, __LINE__, ptr, size, false)


#define rfree(ptr)                              rfree(ptr)

#define rstrdup(s)                              _rstrdup(__FILE__, __LINE__, s)
#define rstrndup(s, n)                          _rstrndup(__FILE__, __LINE__, s, n)


#define MALLOC_MAGIC                0x134DAE5
#define MALLOC_NODE_SIZE            sizeof(malloc_node)

#endif
