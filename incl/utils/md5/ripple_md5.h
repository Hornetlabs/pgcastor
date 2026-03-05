
#ifndef _RIPPLE_MD5_H
#define _RIPPLE_MD5_H

typedef struct RIPPLE_MD5_CONTEXT
{
    uint64          bytes;
    uint32          state[4];
    uint8           buffer[64];
} ripple_md5_context;


void ripple_md5_init(ripple_md5_context *ctx);
void ripple_md5_update(ripple_md5_context *ctx, const void *data, uint64 size);
void ripple_md5_final(uint8 result[16], ripple_md5_context *ctx);

bool ripple_md5_filemd5_get(char* path , uint8* md5);

#endif
