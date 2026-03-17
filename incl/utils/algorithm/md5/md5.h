#ifndef _MD5_H
#define _MD5_H

#define MD5_BUFLEN 64

typedef struct
{
    union
    {
        uint32_t md5_state32[4];
        uint8_t md5_state8[16];
    } md5_st;

#define md5_sta md5_st.md5_state32[0]
#define md5_stb md5_st.md5_state32[1]
#define md5_stc md5_st.md5_state32[2]
#define md5_std md5_st.md5_state32[3]
#define md5_st8 md5_st.md5_state8

    union
    {
        uint64_t md5_count64;
        uint8_t md5_count8[8];
    } md5_count;
#define md5_n md5_count.md5_count64
#define md5_n8 md5_count.md5_count8

    uint32_t md5_i;
    uint8_t md5_buf[MD5_BUFLEN];
} md5_ctxt;

extern void md5_init(md5_ctxt *);
extern void md5_loop(md5_ctxt *, const uint8_t *, unsigned int);
extern void md5_pad(md5_ctxt *);
extern void md5_result(uint8_t *, md5_ctxt *);

/* compatibility */
#define MD5_CTX md5_ctxt
#define MD5Init(x) md5_init((x))
#define MD5Update(x, y, z) md5_loop((x), (y), (z))
#define MD5Final(x, y)        \
    do                        \
    {                         \
        md5_pad((y));         \
        md5_result((x), (y)); \
    } while (0)

#endif /* ! _NETINET6_MD5_H_ */
