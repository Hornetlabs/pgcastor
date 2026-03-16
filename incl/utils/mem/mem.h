#ifndef _RIPPLE_MEM_H
#define _RIPPLE_MEM_H

typedef struct RIPPLE_MALLOC_NODE
{
    uint16                          flag;       /* 全局/局部变量 */
    uint32                          magic;      /* 魔数--判断是否为可用空间 */
    uint64                          number;     /* 编号 */
    uint32                          line;       /* 行号 */
    uint64                          size;       /* 数据大小 */
    char                            file[128];  /* 文件名称 */
    struct RIPPLE_MALLOC_NODE*      prev;
    struct RIPPLE_MALLOC_NODE*      next;
} ripple_malloc_node;


typedef struct RIPPLE_MALLOC_NODES
{
    uint64                          number;      /* 编号 */
    pthread_mutex_t                 listlock;   /* 链表锁 */
    ripple_malloc_node*             head;       /* 链表头 */
    ripple_malloc_node*             tail;       /* 链表尾 */
} ripple_malloc_nodes;


typedef enum RIPPLE_MEMPRINT_FLAG{
    RIPPLE_MEMPRINT_GLOBAL, // 全局变量
    RIPPLE_MEMPRINT_LOCAL,  // 局部变量
    RIPPLE_MEMPRINT_ALL     // 全部变量
} ripple_memprint_flag;

/* 申请空间 */
void* ripple_malloc(char* file, uint32 line, size_t size, bool local);

/* 初始化 */
void* ripple_memset(void* s, uint64 offset, int c, size_t n, bool heap);

/* 内存拷贝 */
void* ripple_memcpy(void* dest, uint64 offset, const void* src, size_t n, bool heap);

/* 重新分配 */
void* ripple_realloc(char* file, uint32 line, void* ptr, size_t size, bool local);

/* 字符串拷贝 */
char * ripple_strdup(char* file, uint32 line, const char *s);

/* 按长度字符串拷贝 */
char * ripple_strndup(char* file, uint32 line, const char* s, uint32 n);

/* 释放 */
void ripple_free(void* ptr);

void ripple_mem_print(ripple_memprint_flag flag);

void ripple_mem_init(void);


/* 宏定义 */
#define rmalloc0(size)                          ripple_malloc(__FILE__, __LINE__, size, true)
#define rmalloc1(size)                          ripple_malloc(__FILE__, __LINE__, size, false)

#define rmemset0(s, offset, c, n)               ripple_memset(s, offset, c, n, true)
#define rmemset1(s, offset, c, n)               ripple_memset(s, offset, c, n, false)


#define rmemcpy0(dest, dstoffset, src, n)       ripple_memcpy(dest, dstoffset, src, n, true)
#define rmemcpy1(dest, dstoffset, src, n)       ripple_memcpy(dest, dstoffset, src, n, false)


#define rrealloc0(ptr, size)                    ripple_realloc(__FILE__, __LINE__, ptr, size, true)
#define rrealloc1(ptr, size)                    ripple_realloc(__FILE__, __LINE__, ptr, size, false)


#define rfree(ptr)                              ripple_free(ptr)

#define rstrdup(s)                              ripple_strdup(__FILE__, __LINE__, s)
#define rstrndup(s, n)                          ripple_strndup(__FILE__, __LINE__, s, n)


#define RIPPLE_MALLOC_MAGIC                0x134DAE5
#define RIPPLE_MALLOC_NODE_SIZE            sizeof(ripple_malloc_node)

#endif
