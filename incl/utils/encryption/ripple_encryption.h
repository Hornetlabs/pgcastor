#ifndef RIPPLE_ENCRYPTION_H
#define RIPPLE_ENCRYPTION_H

#include <openssl/cast.h>
#include <openssl/des.h>

typedef uint8_t u1byte;
typedef uint16_t u2byte;
typedef uint32_t u4byte;

typedef int8_t s1byte;
typedef int16_t s2byte;
typedef int32_t s4byte;

typedef struct _rijndael_ctx
{
    u4byte        k_len;
    int           decrypt;
    u4byte        e_key[64];
    u4byte        d_key[64];
} rijndael_ctx;


typedef struct xts_encrypt_ctx
{
    rijndael_ctx twk_ctx[1];
    rijndael_ctx enc_ctx[1];
} xts_encrypt_ctx;

typedef struct xts_decrypt_ctx
{
    rijndael_ctx twk_ctx[1];
    rijndael_ctx dec_ctx[1];
} xts_decrypt_ctx;

typedef struct db_encryption_ctx
{
    xts_encrypt_ctx enc_ctx[1];
    xts_decrypt_ctx dec_ctx[1];
} db_encryption_ctx;

typedef struct BlowfishContext
{
    uint32_t    S0[256],
                S1[256],
                S2[256],
                S3[256],
                P[18];
    uint32_t    iv0,
                iv1;            /* for CBC mode */
} BlowfishContext;

#define IV_SIZE 8

typedef struct EncryptInfo
{
    ripple_encryption_encrypt_method algo;
    union
    {
        db_encryption_ctx aes_key;
        BlowfishContext bf_key;
        struct
        {
            DES_key_schedule key;
        } des;
        struct
        {
            DES_key_schedule k1,
                k2,
                k3;
        } des3;
        CAST_KEY cast_key;
    } u;
    uint8_t iv[IV_SIZE];
} EncryptInfo;

#endif
