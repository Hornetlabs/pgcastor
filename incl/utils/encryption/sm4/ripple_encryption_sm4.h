#ifndef RIPPLE_ENCRYPTION_SM4_H
#define RIPPLE_ENCRYPTION_SM4_H

typedef struct sm4_context
{
    int32_t mode;         /*!<  encrypt/decrypt   */
    unsigned long sk[32]; /*!<  SM4 subkeys       */
} sm4_context;

extern void hg_sm4_crypt_cbc(sm4_context *ctx,
                             int mode,
                             int length,
                             unsigned char *iv,
                             unsigned char *input,
                             unsigned char *output);

extern void sm4_setkey_dec(sm4_context *ctx, unsigned char key[16]);
extern void sm4_setkey_enc(sm4_context *ctx, unsigned char key[16]);

#endif
