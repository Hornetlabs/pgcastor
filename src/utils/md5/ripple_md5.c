#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "utils/md5/ripple_md5.h"

static const uint8 *ripple_md5_body(ripple_md5_context *ctx, const uint8 *data, uint64 size);

void ripple_md5_init(ripple_md5_context *ctx)
{
    ctx->state[0] = 0x67452301;
    ctx->state[1] = 0xefcdab89;
    ctx->state[2] = 0x98badcfe;
    ctx->state[3] = 0x10325476;

    ctx->bytes = 0;
}

void
ripple_md5_update(ripple_md5_context *ctx, const void *data, uint64 size)
{
    uint64 used = 0;
    uint64 free = 0;

    used = (uint64) (ctx->bytes & 0x3f);
    ctx->bytes += size;

    if (used)
    {
        free = 64 - used;

        if (size < free)
        {
            rmemcpy1(&ctx->buffer[used], 0, data, size);
            return;
        }

        rmemcpy1(&ctx->buffer[used], 0, data, free);
        data = (uint8 *) data + free;
        size -= free;
        (void) ripple_md5_body(ctx, ctx->buffer, 64);
    }

    if (size >= 64)
    {
        data = ripple_md5_body(ctx, data, size & ~(uint64) 0x3f);
        size &= 0x3f;
    }

    rmemcpy1(ctx->buffer, 0, data, size);
}


void
ripple_md5_final(uint8 result[16], ripple_md5_context *ctx)
{
    uint64 used = 0;
    uint64 free = 0;

    used = (uint64) (ctx->bytes & 0x3f);

    ctx->buffer[used++] = 0x80;

    free = 64 - used;

    if (free < 8)
    {
        rmemset1(&ctx->buffer[used], 0, 0, free);
        (void) ripple_md5_body(ctx, ctx->buffer, 64);
        used = 0;
        free = 64;
    }

    rmemset1(&ctx->buffer[used], 0, 0, free - 8);

    ctx->bytes <<= 3;
    ctx->buffer[56] = (uint8) ctx->bytes;
    ctx->buffer[57] = (uint8) (ctx->bytes >> 8);
    ctx->buffer[58] = (uint8) (ctx->bytes >> 16);
    ctx->buffer[59] = (uint8) (ctx->bytes >> 24);
    ctx->buffer[60] = (uint8) (ctx->bytes >> 32);
    ctx->buffer[61] = (uint8) (ctx->bytes >> 40);
    ctx->buffer[62] = (uint8) (ctx->bytes >> 48);
    ctx->buffer[63] = (uint8) (ctx->bytes >> 56);

    (void) ripple_md5_body(ctx, ctx->buffer, 64);

    result[0] = (uint8) ctx->state[0];
    result[1] = (uint8) (ctx->state[0] >> 8);
    result[2] = (uint8) (ctx->state[0] >> 16);
    result[3] = (uint8) (ctx->state[0] >> 24);
    result[4] = (uint8) ctx->state[1];
    result[5] = (uint8) (ctx->state[1] >> 8);
    result[6] = (uint8) (ctx->state[1] >> 16);
    result[7] = (uint8) (ctx->state[1] >> 24);
    result[8] = (uint8) ctx->state[2];
    result[9] = (uint8) (ctx->state[2] >> 8);
    result[10] = (uint8) (ctx->state[2] >> 16);
    result[11] = (uint8) (ctx->state[2] >> 24);
    result[12] = (uint8) ctx->state[3];
    result[13] = (uint8) (ctx->state[3] >> 8);
    result[14] = (uint8) (ctx->state[3] >> 16);
    result[15] = (uint8) (ctx->state[3] >> 24);
}


/*
 * The basic MD5 functions.
 *
 * F and G are optimized compared to their RFC 1321 definitions for
 * architectures that lack an AND-NOT instruction, just like in
 * Colin Plumb's implementation.
 */

#define F(x, y, z)  ((z) ^ ((x) & ((y) ^ (z))))
#define G(x, y, z)  ((y) ^ ((z) & ((x) ^ (y))))
#define H(x, y, z)  ((x) ^ (y) ^ (z))
#define I(x, y, z)  ((y) ^ ((x) | ~(z)))

/*
 * The MD5 transformation for all four rounds.
 */

#define RIPPLE_MD5STEP(f, a, b, c, d, x, t, s)                                          \
    (a) += f((b), (c), (d)) + (x) + (t);                                      \
    (a) = (((a) << (s)) | (((a) & 0xffffffff) >> (32 - (s))));                \
    (a) += (b)


#define RIPPLE_MD5STE(n)                                                                \
    (block[n] =                                                               \
    (uint32_t) p[n * 4] |                                                     \
    ((uint32_t) p[n * 4 + 1] << 8) |                                          \
    ((uint32_t) p[n * 4 + 2] << 16) |                                         \
    ((uint32_t) p[n * 4 + 3] << 24))

#define RIPPLE_MD5GET(n)      block[n]


/*
 * This processes one or more 64-byte data blocks, but does not update
 * the bit counters.  There are no alignment requirements.
 */

static const uint8 *
ripple_md5_body(ripple_md5_context *ctx, const uint8 *data, uint64 size)
{
    uint32_t       a, b, c, d;
    uint32_t       saved_a, saved_b, saved_c, saved_d;
    const uint8 *p;
    uint32_t block[16];

    p = data;

    a = ctx->state[0];
    b = ctx->state[1];
    c = ctx->state[2];
    d = ctx->state[3];

    do {
        saved_a = a;
        saved_b = b;
        saved_c = c;
        saved_d = d;

        /* Round 1 */

        RIPPLE_MD5STEP(F, a, b, c, d, RIPPLE_MD5STE(0),  0xd76aa478, 7);
        RIPPLE_MD5STEP(F, d, a, b, c, RIPPLE_MD5STE(1),  0xe8c7b756, 12);
        RIPPLE_MD5STEP(F, c, d, a, b, RIPPLE_MD5STE(2),  0x242070db, 17);
        RIPPLE_MD5STEP(F, b, c, d, a, RIPPLE_MD5STE(3),  0xc1bdceee, 22);
        RIPPLE_MD5STEP(F, a, b, c, d, RIPPLE_MD5STE(4),  0xf57c0faf, 7);
        RIPPLE_MD5STEP(F, d, a, b, c, RIPPLE_MD5STE(5),  0x4787c62a, 12);
        RIPPLE_MD5STEP(F, c, d, a, b, RIPPLE_MD5STE(6),  0xa8304613, 17);
        RIPPLE_MD5STEP(F, b, c, d, a, RIPPLE_MD5STE(7),  0xfd469501, 22);
        RIPPLE_MD5STEP(F, a, b, c, d, RIPPLE_MD5STE(8),  0x698098d8, 7);
        RIPPLE_MD5STEP(F, d, a, b, c, RIPPLE_MD5STE(9),  0x8b44f7af, 12);
        RIPPLE_MD5STEP(F, c, d, a, b, RIPPLE_MD5STE(10), 0xffff5bb1, 17);
        RIPPLE_MD5STEP(F, b, c, d, a, RIPPLE_MD5STE(11), 0x895cd7be, 22);
        RIPPLE_MD5STEP(F, a, b, c, d, RIPPLE_MD5STE(12), 0x6b901122, 7);
        RIPPLE_MD5STEP(F, d, a, b, c, RIPPLE_MD5STE(13), 0xfd987193, 12);
        RIPPLE_MD5STEP(F, c, d, a, b, RIPPLE_MD5STE(14), 0xa679438e, 17);
        RIPPLE_MD5STEP(F, b, c, d, a, RIPPLE_MD5STE(15), 0x49b40821, 22);

        /* Round 2 */

        RIPPLE_MD5STEP(G, a, b, c, d, RIPPLE_MD5GET(1),  0xf61e2562, 5);
        RIPPLE_MD5STEP(G, d, a, b, c, RIPPLE_MD5GET(6),  0xc040b340, 9);
        RIPPLE_MD5STEP(G, c, d, a, b, RIPPLE_MD5GET(11), 0x265e5a51, 14);
        RIPPLE_MD5STEP(G, b, c, d, a, RIPPLE_MD5GET(0),  0xe9b6c7aa, 20);
        RIPPLE_MD5STEP(G, a, b, c, d, RIPPLE_MD5GET(5),  0xd62f105d, 5);
        RIPPLE_MD5STEP(G, d, a, b, c, RIPPLE_MD5GET(10), 0x02441453, 9);
        RIPPLE_MD5STEP(G, c, d, a, b, RIPPLE_MD5GET(15), 0xd8a1e681, 14);
        RIPPLE_MD5STEP(G, b, c, d, a, RIPPLE_MD5GET(4),  0xe7d3fbc8, 20);
        RIPPLE_MD5STEP(G, a, b, c, d, RIPPLE_MD5GET(9),  0x21e1cde6, 5);
        RIPPLE_MD5STEP(G, d, a, b, c, RIPPLE_MD5GET(14), 0xc33707d6, 9);
        RIPPLE_MD5STEP(G, c, d, a, b, RIPPLE_MD5GET(3),  0xf4d50d87, 14);
        RIPPLE_MD5STEP(G, b, c, d, a, RIPPLE_MD5GET(8),  0x455a14ed, 20);
        RIPPLE_MD5STEP(G, a, b, c, d, RIPPLE_MD5GET(13), 0xa9e3e905, 5);
        RIPPLE_MD5STEP(G, d, a, b, c, RIPPLE_MD5GET(2),  0xfcefa3f8, 9);
        RIPPLE_MD5STEP(G, c, d, a, b, RIPPLE_MD5GET(7),  0x676f02d9, 14);
        RIPPLE_MD5STEP(G, b, c, d, a, RIPPLE_MD5GET(12), 0x8d2a4c8a, 20);

        /* Round 3 */

        RIPPLE_MD5STEP(H, a, b, c, d, RIPPLE_MD5GET(5),  0xfffa3942, 4);
        RIPPLE_MD5STEP(H, d, a, b, c, RIPPLE_MD5GET(8),  0x8771f681, 11);
        RIPPLE_MD5STEP(H, c, d, a, b, RIPPLE_MD5GET(11), 0x6d9d6122, 16);
        RIPPLE_MD5STEP(H, b, c, d, a, RIPPLE_MD5GET(14), 0xfde5380c, 23);
        RIPPLE_MD5STEP(H, a, b, c, d, RIPPLE_MD5GET(1),  0xa4beea44, 4);
        RIPPLE_MD5STEP(H, d, a, b, c, RIPPLE_MD5GET(4),  0x4bdecfa9, 11);
        RIPPLE_MD5STEP(H, c, d, a, b, RIPPLE_MD5GET(7),  0xf6bb4b60, 16);
        RIPPLE_MD5STEP(H, b, c, d, a, RIPPLE_MD5GET(10), 0xbebfbc70, 23);
        RIPPLE_MD5STEP(H, a, b, c, d, RIPPLE_MD5GET(13), 0x289b7ec6, 4);
        RIPPLE_MD5STEP(H, d, a, b, c, RIPPLE_MD5GET(0),  0xeaa127fa, 11);
        RIPPLE_MD5STEP(H, c, d, a, b, RIPPLE_MD5GET(3),  0xd4ef3085, 16);
        RIPPLE_MD5STEP(H, b, c, d, a, RIPPLE_MD5GET(6),  0x04881d05, 23);
        RIPPLE_MD5STEP(H, a, b, c, d, RIPPLE_MD5GET(9),  0xd9d4d039, 4);
        RIPPLE_MD5STEP(H, d, a, b, c, RIPPLE_MD5GET(12), 0xe6db99e5, 11);
        RIPPLE_MD5STEP(H, c, d, a, b, RIPPLE_MD5GET(15), 0x1fa27cf8, 16);
        RIPPLE_MD5STEP(H, b, c, d, a, RIPPLE_MD5GET(2),  0xc4ac5665, 23);

        /* Round 4 */

        RIPPLE_MD5STEP(I, a, b, c, d, RIPPLE_MD5GET(0),  0xf4292244, 6);
        RIPPLE_MD5STEP(I, d, a, b, c, RIPPLE_MD5GET(7),  0x432aff97, 10);
        RIPPLE_MD5STEP(I, c, d, a, b, RIPPLE_MD5GET(14), 0xab9423a7, 15);
        RIPPLE_MD5STEP(I, b, c, d, a, RIPPLE_MD5GET(5),  0xfc93a039, 21);
        RIPPLE_MD5STEP(I, a, b, c, d, RIPPLE_MD5GET(12), 0x655b59c3, 6);
        RIPPLE_MD5STEP(I, d, a, b, c, RIPPLE_MD5GET(3),  0x8f0ccc92, 10);
        RIPPLE_MD5STEP(I, c, d, a, b, RIPPLE_MD5GET(10), 0xffeff47d, 15);
        RIPPLE_MD5STEP(I, b, c, d, a, RIPPLE_MD5GET(1),  0x85845dd1, 21);
        RIPPLE_MD5STEP(I, a, b, c, d, RIPPLE_MD5GET(8),  0x6fa87e4f, 6);
        RIPPLE_MD5STEP(I, d, a, b, c, RIPPLE_MD5GET(15), 0xfe2ce6e0, 10);
        RIPPLE_MD5STEP(I, c, d, a, b, RIPPLE_MD5GET(6),  0xa3014314, 15);
        RIPPLE_MD5STEP(I, b, c, d, a, RIPPLE_MD5GET(13), 0x4e0811a1, 21);
        RIPPLE_MD5STEP(I, a, b, c, d, RIPPLE_MD5GET(4),  0xf7537e82, 6);
        RIPPLE_MD5STEP(I, d, a, b, c, RIPPLE_MD5GET(11), 0xbd3af235, 10);
        RIPPLE_MD5STEP(I, c, d, a, b, RIPPLE_MD5GET(2),  0x2ad7d2bb, 15);
        RIPPLE_MD5STEP(I, b, c, d, a, RIPPLE_MD5GET(9),  0xeb86d391, 21);

        a += saved_a;
        b += saved_b;
        c += saved_c;
        d += saved_d;

        p += 64;

    } while (size -= 64);

    ctx->state[0] = a;
    ctx->state[1] = b;
    ctx->state[2] = c;
    ctx->state[3] = d;

    return p;
}

bool ripple_md5_filemd5_get(char* path , uint8* md5)
{
    int fd;
    uint64 rlen = 0;
    uint64 offset = 0;
    ripple_md5_context ctx;
    uint8* buffer[RIPPLE_FILE_BUFFER_SIZE] = {'\0'};

    rmemset1(&ctx, 0, '\0', sizeof(ripple_md5_context));
    ripple_md5_init(&ctx);
    fd = BasicOpenFile(path,  O_RDWR | RIPPLE_BINARY);
    if (fd < 0)
    {
        elog(RLOG_WARNING, "open file %s error %s", path, strerror(errno));
        return false;
    }

    while(0 < (rlen = FilePRead(fd, (char*)buffer, RIPPLE_FILE_BUFFER_SIZE, offset)))
    {
        ripple_md5_update(&ctx, buffer, rlen);
        offset += rlen;
        rmemset1(buffer, 0, '\0', RIPPLE_FILE_BUFFER_SIZE);
    }

    ripple_md5_final(md5, &ctx);

    if(FileClose(fd))
    {
        elog(RLOG_WARNING, "could not close file %s", path);
        return false;
    }

    return true;
}
