#include "app_incl.h"
#include "port/file/fd.h"
#include "port/thread/thread.h"

static malloc_nodes m_nodes = {0, PTHREAD_MUTEX_INITIALIZER, NULL, NULL};

void mem_init(void)
{
    m_nodes.number = 0;
    osal_thread_mutex_init(&m_nodes.listlock, NULL);
    m_nodes.head = NULL;
    m_nodes.tail = NULL;
}

/*
 * allocate size bytes of memory
 * parameters:
 *  file            source file where allocation occurs
 *  line            source line number where allocation occurs
 *  size            size of memory to allocate
 *  local           flag indicating whether allocation is local or global
 *                      true            local space
 *                      false           global space
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
    memset(addr, '\0', MALLOC_NODE_SIZE);

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
 * set initial value for specified memory
 * parameters:
 *  s               memory address to initialize
 *  offset          offset from s
 *  c               character to set
 *  n               length
 *  heap            flag indicating whether s points to heap or stack
 *                      true            heap
 *                      false           stack
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
 * copy memory from source to destination
 * parameters:
 *  dest            destination address
 *  offset          offset from dest
 *  src             source address
 *  n               length
 *  heap            flag indicating whether s points to heap or stack
 *                      true            heap
 *                      false           stack
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
 * reallocate size bytes of memory
 * parameters:
 *  file            source file where allocation occurs
 *  line            source line number where allocation occurs
 *  size            size of memory to allocate
 *  local           flag indicating whether allocation is local or global
 *                      true            local space
 *                      false           global space
 *
 * notes:
 *  1. when ptr is NULL, indicates new allocation, function behaves same as rmalloc
 *  2. if requested size is smaller than original, returns original space
 *
 */
void* rrealloc(char* file, uint32 line, void* ptr, size_t size, bool local)
{
    void* addr = NULL;
#ifdef MEMCHECK
    malloc_node  new_node = {0};
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
 * free memory space
 *  parameters:
 *      ptr  memory space to free
 *
 * notes:
 *  if ptr has no corresponding memory in custom logic, indicates memory was already freed,
 *  which is a double-free error, report error
 */
void rfree(void* ptr)
{
#ifdef MEMCHECK
    malloc_node* node = NULL;
    node = (malloc_node*)(((uint8_t*)ptr) - MALLOC_NODE_SIZE);
    if (node->magic != MALLOC_MAGIC)
    {
        elog(RLOG_ERROR, "doublefree");
        return;
    }

    /* add to linked list */
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
    uint64       totalsize = 0;
    malloc_node* cur = m_nodes.head;
    osal_thread_lock(&m_nodes.listlock);
    while (cur)
    {
        if ((flag == MEMPRINT_GLOBAL && cur->flag == 0) || (flag == MEMPRINT_LOCAL && cur->flag == 1) ||
            (flag == MEMPRINT_ALL))
        {
            totalsize += cur->size;
            elog(RLOG_INFO,
                 "Allocated at %s:%u, size %lu, address %p, number %u\n",
                 cur->file,
                 cur->line,
                 cur->size,
                 cur,
                 cur->number);
        }

        cur = cur->next;
    }
    osal_thread_unlock(&m_nodes.listlock);
    elog(RLOG_INFO, "castor C Port Use Memory Size:%lu", totalsize);
}

/*
 * string copy function
 * parameters:
 *  file            source file where allocation occurs
 *  line            source line number where allocation occurs
 *  s               string to copy
 */
char* _rstrdup(char* file, uint32 line, const char* s)
{
    char*  str = NULL;
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
 * string copy function (with length limit)
 * parameters:
 *  file            source file where allocation occurs
 *  line            source line number where allocation occurs
 *  s               string to copy
 *  n               maximum length to copy
 */
char* _rstrndup(char* file, uint32 line, const char* s, uint32 n)
{
    char*  str = NULL;
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
