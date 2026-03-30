#ifndef _SERIAL_H
#define _SERIAL_H

/* transaction serialization thread structure */
typedef struct SERIALSTATE
{
    Oid           database; /* database oid */
    ffsmgr_state* ffsmgrstate;
    file_buffers* txn2filebuffer;
} serialstate;

void serialstate_init(serialstate* serialstate);

file_buffers* serialstate_getfilebuffer(void* privdata);

void serialstate_fbuffer_set(serialstate* serialstate, uint64 fileid, uint64 fileoffset, FullTransactionId xid);

void serialstate_ffsmgr_set(serialstate* serialstate, int serialtype);

void serialstate_destroy(serialstate* serialstate);

#endif