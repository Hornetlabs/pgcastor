#ifndef _LOADPAGE_H_
#define _LOADPAGE_H_

typedef enum LOADPAGE_TYPE
{
    LOADPAGE_TYPE_NOP = 0x00,
    LOADPAGE_TYPE_FILE,
    LOADPAGE_TYPE_REPLICATION
} loadpage_type;

typedef struct LOADPAGE
{
    loadpage_type type;
    uint32        blksize;
    uint64        filesize;

    /* Error code */
    int           error;
} loadpage;

#define LOADPAGEBLKSIZEMASK(blksize) ((uint64)(blksize - 1))

#endif
