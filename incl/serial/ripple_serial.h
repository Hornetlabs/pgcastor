#ifndef _RIPPLE_SERIAL_H
#define _RIPPLE_SERIAL_H

/* 将事务落盘线程结构体 */
typedef struct RIPPLE_SERIALSTATE
{
    Oid                                 database;                       /* 数据库oid */
    ripple_ffsmgr_state*                ffsmgrstate;
    ripple_file_buffers*                txn2filebuffer;
} ripple_serialstate;

void ripple_serialstate_init(ripple_serialstate* serialstate);

ripple_file_buffers* ripple_serialstate_getfilebuffer(void* privdata);

void ripple_serialstate_fbuffer_set(ripple_serialstate* serialstate, uint64 fileid, uint64 fileoffset, FullTransactionId xid);

void ripple_serialstate_ffsmgr_set(ripple_serialstate* serialstate, int serialtype);

void ripple_serialstate_destroy(ripple_serialstate* serialstate);

#endif