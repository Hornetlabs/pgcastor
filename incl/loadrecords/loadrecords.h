#ifndef _LOADRECORDS_H_
#define _LOADRECORDS_H_

typedef enum LOADRECORDS_TYPE
{
    LOADRECORDS_TYPE_NOP = 0x00,
    LOADRECORDS_TYPE_WAL,
    LOADRECORDS_TYPE_TRAIL
} loadrecords_type;

typedef struct LOADRECORDS
{
    int    type;
    int    error;
    int    blksize;
    uint64 filesize;
} loadrecords;

#endif
