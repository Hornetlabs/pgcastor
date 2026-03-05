#ifndef _RIPPLE_LOADRECORDS_H_
#define _RIPPLE_LOADRECORDS_H_

typedef enum RIPPLE_LOADRECORDS_TYPE
{
    RIPPLE_LOADRECORDS_TYPE_NOP         = 0x00,
    RIPPLE_LOADRECORDS_TYPE_WAL         ,
    RIPPLE_LOADRECORDS_TYPE_TRAIL       
} ripple_loadrecords_type;

typedef struct RIPPLE_LOADRECORDS
{
    int                     type;
    int                     error;
    int                     blksize;
    uint64                  filesize;
} ripple_loadrecords;

#endif
