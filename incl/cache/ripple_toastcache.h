#ifndef RIPPLE_TOASTCACHE_H
#define RIPPLE_TOASTCACHE_H

typedef struct ripple_chunk_data
{
    int     chunk_seq;
    int     chunk_len;
    char   *chunk_data;
}ripple_chunk_data;

typedef struct ripple_toast_cache_entry
{
    Oid     chunk_id;
    List   *chunk_list;     /* 保存ripple_chunk_data */
}ripple_toast_cache_entry;

#endif
