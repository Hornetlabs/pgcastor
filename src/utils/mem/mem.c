#include "app_incl.h"
#include "port/file/fd.h"
#include "port/thread/thread.h"

static malloc_nodes m_nodes = { 0, PTHREAD_MUTEX_INITIALIZER, NULL, NULL};

void mem_init(void)
{
    m_nodes.number = 0;
    osal_thread_mutex_init(&m_nodes.listlock, NULL);
    m_nodes.head = NULL;
    m_nodes.tail = NULL;
}

/*
 * 分配 size 字节的内存
 * 参数说明:
 *  file            分配空间所在的文件
 *  line            分配空间所在的文件内的行号
 *  size            分配的空间
 *  local           用于标识分配的空间是局部空间还是全局空间
 *                      true            局部空间
 *                      false           全局空间
 */
void* rmalloc(char* file, uint32 line, size_t size, bool local)
{
    void* addr = NULL;
#ifdef MEMCHECK
    malloc_node* node = NULL;
    if (0 == size)
    {
        elog(RLOG_ERROR, "%s:%d not supported size 0", file, line);
    }
    addr = malloc(size + MALLOC_NODE_SIZE);
    if (addr == NULL)
    {
        elog(RLOG_ERROR, "%s:%d out of memory, %s", file, line, strerror(errno));
    }
    memset(addr,'\0', MALLOC_NODE_SIZE);

    node = (malloc_node*)addr;

    node->magic = MALLOC_MAGIC;
    node->flag = local;
    node->number = ++m_nodes.number;
    node->line = line;
    memcpy(node->file, file, strlen(file));
    node->size = size;
    node->prev = NULL;
    node->next = NULL;

    osal_thread_lock(&m_nodes.listlock);
    if (!m_nodes.head)
    {
        m_nodes.head = m_nodes.tail = node;
    }
    else
    {
        m_nodes.tail->next = node;
        node->prev = m_nodes.tail;
        m_nodes.tail = node;
    }
    osal_thread_unlock(&m_nodes.listlock);

    addr = (uint8*)addr + MALLOC_NODE_SIZE;
#else
    addr = (void*)malloc(size);
#endif
    return addr;
}

/* 
 * 对指定内存设置初始值
 * 参数说明:
 *  s               待初始化的内存地址
 *  offset          基于 s 的偏移
 *  c               设置的字符
 *  n               长度
 *  heap            标识 s 的指向为堆还是栈
 *                      true            堆
 *                      false           栈
 */
void* rmemset(void* s, uint64 offset, int c, size_t n, bool heap)
{    
#ifdef MEMCHECK
    malloc_node* node = NULL;
    if (false == heap)
    {
        memset(((uint8*)s) + offset, c, n);
    }
    else
    {
        
        node = (malloc_node*)(((uint8_t*)s) - MALLOC_NODE_SIZE);
        if (node->magic != MALLOC_MAGIC)
        {
            elog(RLOG_ERROR, "Memory failure or released");
        }
        
        if (node->size < offset + n)
        {
            elog(RLOG_ERROR, " Memset out of memory, file %s: %u", node->file, node->line);
            return s;
        }

        memset(((uint8*)s) + offset, c, n);
    }
#else
     memset(((uint8*)s) + offset, c, n);
#endif
    return s;
}

/* 
 * 对指定内存设置初始值
 * 参数说明:
 *  dest            目标地址
 *  offset          基于dest的偏移
 *  src             源地址
 *  n               长度
 *  heap            标识 s 的指向为堆还是栈
 *                      true            堆
 *                      false           栈
 */
void* rmemcpy(void* dest, uint64 offset, const void* src, size_t n, bool heap)
{
#ifdef MEMCHECK
    malloc_node* node = NULL;
    if (false == heap)
    {
        memcpy(((uint8_t*)dest) + offset, src, n);
    }
    else
    {
        node = (malloc_node*)(((uint8_t*)dest) - MALLOC_NODE_SIZE);
        if (node->magic != MALLOC_MAGIC)
        {
            elog(RLOG_ERROR, "Memory failure or released");
        }
        
        if (node->size < offset + n)
        {
            elog(RLOG_ERROR, "Memcpy out of memory, file %s: %u", node->file, node->line);
            return dest;
        }

        memcpy(((uint8*)dest) + offset, src, n);
    }
#else
    memcpy(((uint8*)dest) + offset, src, n);
#endif
    return dest;
}

/*
 * 分配 size 字节的内存
 * 参数说明:
 *  file            分配空间所在的文件
 *  line            分配空间所在的文件内的行号
 *  size            分配的空间
 *  local           用于标识分配的空间是局部空间还是全局空间
 *                      true            局部空间
 *                      false           全局空间
 * 
 * 备注:
 *  1、ptr 为空时，标识是新申请空间，功能与 rmalloc 相同
 *  2、在自写的逻辑里面判断待分配的空间是小于源空间，那么返回源空间即可
 * 
 */
void* rrealloc(char* file, uint32 line, void* ptr, size_t size, bool local)
{
    void* addr = NULL;
#ifdef MEMCHECK
    malloc_node new_node = {0};
    malloc_node* node = NULL;

    new_node.magic = MALLOC_MAGIC;
    new_node.flag = local;
    new_node.line = line;
    memcpy(new_node.file, file, strlen(file));
    new_node.size = size;
    
    osal_thread_lock(&m_nodes.listlock);
    if (ptr == NULL)
    {
        new_node.number = ++m_nodes.number;
        new_node.prev = NULL;
        new_node.next = NULL;
    }
    else
    {
        ptr = ((uint8_t*)ptr) - MALLOC_NODE_SIZE;
        node = (malloc_node*)ptr;
        new_node.number = node->number;
        new_node.prev = node->prev;
        new_node.next = node->next;
        if (size < node->size)
        {
            memset(ptr, '\0', MALLOC_NODE_SIZE);
            memcpy(ptr, &new_node, MALLOC_NODE_SIZE);
            return ((uint8_t*)ptr) + MALLOC_NODE_SIZE;
        }
    }

    addr = realloc(ptr, size + MALLOC_NODE_SIZE);
    if (addr == NULL)
    {
        elog(RLOG_ERROR, "%s:%d out of memory, %s", file, line, strerror(errno));
    }
    memcpy(addr, &new_node, MALLOC_NODE_SIZE);

    node = (malloc_node*)addr;
    if (ptr == NULL)
    {
        if (NULL == m_nodes.head) 
        {
            m_nodes.head = m_nodes.tail = node;
        } 
        else
        {
            m_nodes.tail->next = node;
            node->prev = m_nodes.tail;
            m_nodes.tail = node;
        }
    }
    else
    {
        if (node->prev)
        {
            node->prev->next = node;
        } 
        else 
        {
            m_nodes.head = node;
        }

        if (node->next) 
        {
            node->next->prev = node;
        } 
        else 
        {
            m_nodes.tail = node;
        }
    }
    
    osal_thread_unlock(&m_nodes.listlock);
    addr = ((uint8_t*)addr) + MALLOC_NODE_SIZE;
#else
    addr = realloc(ptr, size);
#endif
    return addr;
}

/* 
 * 释放空间
 *  参数说明:
 *      ptr  待释放的空间
 * 
 * 备注:
 *  在自写逻辑中发现 ptr 无对应的内存时，那么表名内存已经被释放,此时则为 doublefree，报错即可
 */
void rfree(void* ptr)
{
#ifdef MEMCHECK
    malloc_node* node = NULL;
    node = (malloc_node*)(((uint8_t*)ptr) - MALLOC_NODE_SIZE);
    if(node->magic != MALLOC_MAGIC)
    {
        elog(RLOG_ERROR,"doublefree");
        return;
    }

    /* 添加到链表中 */
    osal_thread_lock(&m_nodes.listlock);
    if (node->prev)
    {
        node->prev->next = node->next;
    }
    else
    {
        m_nodes.head = node->next;
    }

    if (node->next)
    {
        node->next->prev = node->prev;
    }
    else
    {
        m_nodes.tail = node->prev;
    }
    osal_thread_unlock(&m_nodes.listlock);

    ptr = ((uint8_t*)ptr) - MALLOC_NODE_SIZE;
#endif
    free(ptr);
}

void mem_print(memprint_flag flag)
{
    uint64 totalsize = 0;
    malloc_node* cur = m_nodes.head;
    osal_thread_lock(&m_nodes.listlock);
    while (cur) {
        if ((flag == MEMPRINT_GLOBAL && cur->flag == 0) ||
            (flag == MEMPRINT_LOCAL && cur->flag == 1) ||
            (flag == MEMPRINT_ALL))
            {
                totalsize += cur->size;
                elog(RLOG_INFO,"Allocated at %s:%u, size %lu, address %p, number %u\n",
                    cur->file, cur->line, cur->size, cur , cur->number);
            }

        cur = cur->next;
    }
    osal_thread_unlock(&m_nodes.listlock);
    elog(RLOG_INFO, "ripple C Port Use Memory Size:%lu", totalsize);
}

/*
 * 字符串拷贝函数
 * 参数说明:
 *  file            分配空间所在的文件
 *  line            分配空间所在的文件内的行号
 *  s               要拷贝的字符串
 */
char * _rstrdup(char* file, uint32 line, const char* s)
{
    char* str = NULL;
    uint32 len = 0;
    if (!s)
    {
        return NULL;
    }
    len = strlen(s) + 1;
#ifdef MEMCHECK
    str = (char*)malloc(file, line, len, true);
    rmemset0(str, 0, '\0', len);
    rmemcpy0(str, 0, s, len - 1);
#else
    str = (char*)malloc(len);
    if (str == NULL)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    memset(str, '\0', len);
    memcpy(str, s, len - 1);
#endif
    return str;
}

/*
 * 字符串拷贝函数
 * 参数说明:
 *  file            分配空间所在的文件
 *  line            分配空间所在的文件内的行号
 *  s               要拷贝的字符串
 *  n               要拷贝的长度
 */
char * _rstrndup(char* file, uint32 line, const char* s, uint32 n)
{
    char* str = NULL;
    uint32 len = 0;
    if (!s)
    {
        return NULL;
    }
    len = strnlen(s, n);
#ifdef MEMCHECK
    str = malloc(file, line, len + 1, true);
    rmemcpy0(str, 0, s, len);
    str[len] = '\0';
#else
    str = (char*)malloc(len + 1);
    if (str == NULL)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    memcpy(str, s, len);
    str[len] = '\0';
#endif
    return str;
}
