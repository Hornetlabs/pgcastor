#ifndef _RIPPLE_LOADPAGE_H_
#define _RIPPLE_LOADPAGE_H_

typedef enum RIPPLE_LOADPAGE_TYPE
{
    RIPPLE_LOADPAGE_TYPE_NOP                = 0x00,
    RIPPLE_LOADPAGE_TYPE_FILE               ,
    RIPPLE_LOADPAGE_TYPE_REPLICATION        
} ripple_loadpage_type;

typedef struct RIPPLE_LOADPAGE
{
    ripple_loadpage_type    type;
    uint32                  blksize;
    uint64                  filesize;

    /* 错误码 */
    int                     error;
} ripple_loadpage;


#define LOADPAGEBLKSIZEMASK(blksize)            ((uint64)(blksize - 1))

#endif
