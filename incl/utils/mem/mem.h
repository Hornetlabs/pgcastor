#ifndef _MEM_H
#define _MEM_H

typedef struct MALLOC_NODE
{
    uint16              flag;      /* global/local variable flag */
    uint32              magic;     /* magic number - used to validate allocated space */
    uint64              number;    /* sequence number */
    uint32              line;      /* source line number */
    uint64              size;      /* data size */
    char                file[128]; /* source file name */
    struct MALLOC_NODE* prev;
    struct MALLOC_NODE* next;
} malloc_node;

typedef struct MALLOC_NODES
{
    uint64          number;   /* sequence number */
    pthread_mutex_t listlock; /* linked list lock */
    malloc_node*    head;     /* linked list head */
    malloc_node*    tail;     /* linked list tail */
} malloc_nodes;

typedef enum MEMPRINT_FLAG
{
    MEMPRINT_GLOBAL, /* global variables */
    MEMPRINT_LOCAL,  /* local variables */
    MEMPRINT_ALL     /* all variables */
} memprint_flag;

/* allocate memory */
void* rmalloc(char* file, uint32 line, size_t size, bool local);

/* initialize memory */
void* rmemset(void* s, uint64 offset, int c, size_t n, bool heap);

/* memory copy */
void* rmemcpy(void* dest, uint64 offset, const void* src, size_t n, bool heap);

/* reallocate memory */
void* rrealloc(char* file, uint32 line, void* ptr, size_t size, bool local);

/* string duplicate */
char* _rstrdup(char* file, uint32 line, const char* s);

char* _rstrndup(char* file, uint32 line, const char* s, uint32 n);

/* free memory */
void rfree(void* ptr);

void mem_print(memprint_flag flag);

void mem_init(void);

/* macro definitions */
#define rmalloc0(size) rmalloc(__FILE__, __LINE__, size, true)
#define rmalloc1(size) rmalloc(__FILE__, __LINE__, size, false)

#define rmemset0(s, offset, c, n) rmemset(s, offset, c, n, true)
#define rmemset1(s, offset, c, n) rmemset(s, offset, c, n, false)

#define rmemcpy0(dest, dstoffset, src, n) rmemcpy(dest, dstoffset, src, n, true)
#define rmemcpy1(dest, dstoffset, src, n) rmemcpy(dest, dstoffset, src, n, false)

#define rrealloc0(ptr, size) rrealloc(__FILE__, __LINE__, ptr, size, true)
#define rrealloc1(ptr, size) rrealloc(__FILE__, __LINE__, ptr, size, false)

#define rfree(ptr) rfree(ptr)

#define rstrdup(s) _rstrdup(__FILE__, __LINE__, s)
#define rstrndup(s, n) _rstrndup(__FILE__, __LINE__, s, n)

#define MALLOC_MAGIC 0x134DAE5
#define MALLOC_NODE_SIZE sizeof(malloc_node)

#endif
