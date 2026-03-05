#ifndef RIPPLE_ENCRYPTION_FUNC_H
#define RIPPLE_ENCRYPTION_FUNC_H

typedef enum RIPPLE_ENCRYPTION_HGDB_ENCRYPT_METHOD
{
    RIPPLE_ENCRYPTION_AES_128 = 1,
    RIPPLE_ENCRYPTION_AES_192,
    RIPPLE_ENCRYPTION_AES_256,
    RIPPLE_ENCRYPTION_SM4,
    RIPPLE_ENCRYPTION_BF,
    RIPPLE_ENCRYPTION_DES,
    RIPPLE_ENCRYPTION_DES3,
    RIPPLE_ENCRYPTION_CAST5
} ripple_encryption_encrypt_method;

extern bool ripple_encryption_decrypt_waldata(char *input,
                                              char *output,
                                              size_t block_size,
                                              uint32_t timeline,
                                              ripple_encryption_encrypt_method method);

extern bool ripple_encryption_encrypt_waldata(char *input,
                                              char *output,
                                              size_t block_size,
                                              uint32_t timeline,
                                              ripple_encryption_encrypt_method method);

#endif
