#ifndef _MPAGE_H_
#define _MPAGE_H_

typedef struct MPAGE
{
    /* Length of data */
    uint64_t size;

    /* Used length */
    uint64_t doffset;

    /* Page number */
    uint64_t pno;

    /* Data */
    uint8_t* data;
} mpage;

#endif
