#ifndef RIPPLE_UTIL_LICENSE_UTIL_H
#define RIPPLE_UTIL_LICENSE_UTIL_H

typedef struct ripple_license_ctx
{
    uint32_t    endata_size;
    char       *license_path;
    char       *password;
    char       *endata;
    uint8_t     iv[16];
    uint8_t     pkey[16];
    uint8_t     salt[20];
} ripple_license_ctx;

extern uint16_t license_get_salt_and_iv(ripple_license_ctx *ctx);
extern void ripple_license_get_pbkdf2_key(ripple_license_ctx *ctx);
extern void ripple_license_decrypt(ripple_license_ctx *ctx, char *decrypt_data, size_t len);
extern char *ripple_license_decompress(char *src, size_t src_len, size_t *uncompress_len);
extern void ripple_license_check_valid_time(char *data);
extern void ripple_license_get_valid_time(char *data, uint64_t *start_res, uint64_t *end_res);
extern int ripple_license_base64_decode(const uint8_t *src, uint8_t *dst, size_t srclen);

#endif
