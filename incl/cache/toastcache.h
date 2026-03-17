#ifndef TOASTCACHE_H
#define TOASTCACHE_H

typedef struct chunk_data
{
    int     chunk_seq;
    int     chunk_len;
    char   *chunk_data;
}chunk_data;

typedef struct toast_cache_entry
{
    Oid     chunk_id;
    List   *chunk_list;     /* 保存chunk_data */
}toast_cache_entry;

#endif
