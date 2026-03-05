#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "utils/encryption/ripple_encryption_func.h"
#include "utils/encryption/ripple_encryption.h"
#include "utils/encryption/sm4/ripple_encryption_sm4.h"
#include "utils/encryption/md5/ripple_encryption_md5.h"

#define XK_ENCRYPTION_MCXT NULL

#define FDE_KEY "highgo@123"

#define RIPPLE_ENCRYPTION_UNUSED(x) ((void) (x))

#define RIPPLE_ENCRYPTION_TYPEALIGN64(ALIGNVAL, LEN) \
    (((uint64_t)(LEN) + ((ALIGNVAL)-1)) & ~((uint64_t)((ALIGNVAL)-1)))

#define RIPPLE_ENCRYPTION_MAXALIGN64(LEN) RIPPLE_ENCRYPTION_TYPEALIGN64(XK_PG_PARSER_MAXIMUM_ALIGNOF, (LEN))


typedef struct RIPPLE_ENCRYPTION_ENCRYPT_KEY
{
    char *key;
    int32_t key_len;
} ripple_encryption_encrypt_key;

#define RIPPLE_ENCRYPTION_HGDB_V457_XLOG_PAGE_MAGIC 0xD101

// typedef struct RIPPLE_ENCRYPTION_HGDB_V457_XLOGPAGEHEADERDATA
// {
//     uint16_t        xlp_magic;
//     uint16_t        xlp_info;
//     uint32_t        xlp_tli;
//     uint64_t        xlp_pageaddr;
//     uint32_t        xlp_rem_len;
// } ripple_encryption_hgdb_v457_XLogPageHeaderData;

static bool IsAllZero(const char *input, size_t size);
static void pgcrypto_decrypt_block(const char *input,
                                   char *output,
                                   size_t size,
                                   const char *tweak,
                                   EncryptInfo *encryptinfo,
                                   sm4_context *sm4_ctx_dec);

static void pgcrypto_encrypt_block(const char *input,
                                   char *output,
                                   size_t size,
                                   const char *tweak,
                                   EncryptInfo *encryptinfo,
                                   sm4_context *sm4_ctx_dec);

static void decrypt_block(const char *input,
                   char *output,
                   size_t size,
                   const char *tweak,
                   EncryptInfo *encryptinfo,
                   sm4_context *sm4_ctx_dec)
{
    if (IsAllZero(input, size))
    {
        if (input != output)
            memset(output, 0, size);
    }
    else
        pgcrypto_decrypt_block(input, output, size, tweak, encryptinfo, sm4_ctx_dec);
}

static void encrypt_block(const char *input,
                   char *output,
                   size_t size,
                   const char *tweak,
                   EncryptInfo *encryptinfo,
                   sm4_context *sm4_ctx_dec)
{
    if (IsAllZero(input, size))
    {
        if (input != output)
            memset(output, 0, size);
    }
    else
        pgcrypto_encrypt_block(input, output, size, tweak, encryptinfo, sm4_ctx_dec);
}


static bool IsAllZero(const char *input, size_t size)
{
    const char *pos = input;
    const char *aligned_start = (char *)RIPPLE_ENCRYPTION_MAXALIGN64(input);
    const char *end = input + size;

    /* Check 1 byte at a time until pos is 8 byte aligned */
    while (pos < aligned_start)
        if (*pos++ != 0)
            return false;

    /*
     * Run 8 parallel 8 byte checks in one iteration. On 2016 hardware
     * slightly faster than 4 parallel checks.
     */
    while (pos + 8 * sizeof(uint64_t) <= end)
    {
        uint64_t *p = (uint64_t *)pos;
        if ((p[0] | p[1] | p[2] | p[3] | p[4] | p[5] | p[6] | p[7]) != 0)
            return false;
        pos += 8 * sizeof(uint64_t);
    }

    /* Handle unaligned tail. */
    while (pos < end)
        if (*pos++ != 0)
            return false;

    return true;
}

static void pgcrypto_decrypt_block(const char *input,
                                   char *output,
                                   size_t size,
                                   const char *tweak,
                                   EncryptInfo *encryptinfo,
                                   sm4_context *sm4_ctx_dec)
{
    /* add begin by wanghaoyan at 20201026 */
    switch(encryptinfo->algo)
    {
#if 0
        case 1:
        case 2:
        case 3:
            if (input != output)
                memcpy(output, input, size);
            xts_decrypt_block((uint8_t*) output, (const uint8_t*) tweak, size, encryptinfo->u.aes_key.dec_ctx);
            break;
#endif
        case 4:
            if (input != output)
                memcpy(output, input, size);
            hg_sm4_crypt_cbc(sm4_ctx_dec, 0, size, (uint8_t*)tweak, (uint8_t*)input, (uint8_t*)output);
            break;
#if 0
        case 5:
            blowfish_setiv(&(encryptinfo->u.bf_key), (uint8_t*) tweak);
            if (input != output)
                memcpy(output, input, size);
            blowfish_decrypt_cbc((uint8_t*) output,(int32_t)size,&(encryptinfo->u.bf_key));
            break;
        case 6: //des
            des_setIv(&encryptinfo,(uint8_t*) tweak);
            des_cbc_decrypt(&encryptinfo,(uint8_t*)input,size,(uint8_t*)output);
            break;
        case 7: //des3
            des_setIv(&encryptinfo,(uint8_t*) tweak);
            des3_cbc_decrypt(&encryptinfo,(uint8_t*)input,size,(uint8_t*)output);
            break;
        case 8: //cast5
            cast_setIv(&encryptinfo,(uint8_t*) tweak);
            cast_cbc_decrypt(&encryptinfo,(uint8_t*)input,size,(uint8_t*)output);
            break;    
#endif
        default:
            break;    
    }
    /*
    if (input != output)
        memcpy(output, input, size);
    hg_sm4_crypt_cbc(sm4_ctx_dec, 0, size, (uint8_t*)tweak, (uint8_t*)input, (uint8_t*)output);
    */
    /* add end by wanghaoyan at 20201026 */
}

static void pgcrypto_encrypt_block(const char *input,
                                   char *output,
                                   size_t size,
                                   const char *tweak,
                                   EncryptInfo *encryptinfo,
                                   sm4_context *sm4_ctx_dec)
{
    /* add begin by wanghaoyan at 20201026 */
    switch(encryptinfo->algo)
    {
#if 0
        case 1:
        case 2:
        case 3:
            if (input != output)
                memcpy(output, input, size);
            xts_decrypt_block((uint8_t*) output, (const uint8_t*) tweak, size, encryptinfo->u.aes_key.dec_ctx);
            break;
#endif
        case 4:
            if (input != output)
                memcpy(output, input, size);
            hg_sm4_crypt_cbc(sm4_ctx_dec, 1, size, (uint8_t*)tweak, (uint8_t*)input, (uint8_t*)output);
            break;
#if 0
        case 5:
            blowfish_setiv(&(encryptinfo->u.bf_key), (uint8_t*) tweak);
            if (input != output)
                memcpy(output, input, size);
            blowfish_decrypt_cbc((uint8_t*) output,(int32_t)size,&(encryptinfo->u.bf_key));
            break;
        case 6: //des
            des_setIv(&encryptinfo,(uint8_t*) tweak);
            des_cbc_decrypt(&encryptinfo,(uint8_t*)input,size,(uint8_t*)output);
            break;
        case 7: //des3
            des_setIv(&encryptinfo,(uint8_t*) tweak);
            des3_cbc_decrypt(&encryptinfo,(uint8_t*)input,size,(uint8_t*)output);
            break;
        case 8: //cast5
            cast_setIv(&encryptinfo,(uint8_t*) tweak);
            cast_cbc_decrypt(&encryptinfo,(uint8_t*)input,size,(uint8_t*)output);
            break;    
#endif
        default:
            break;    
    }
    /*
    if (input != output)
        memcpy(output, input, size);
    hg_sm4_crypt_cbc(sm4_ctx_dec, 0, size, (uint8_t*)tweak, (uint8_t*)input, (uint8_t*)output);
    */
    /* add end by wanghaoyan at 20201026 */
}

static const char hextable[] = "0123456789abcdef";

static char *hex_encode(char *key, int32_t len)
{
    char *result = NULL;
    char *rp = NULL;

    const char *end = key + len;
    result = rmalloc0((len * 2) + 1);
    rp = result;
    while (key < end)
    {
        *rp++ = hextable[(*key >> 4) & 0xF];
        *rp++ = hextable[*key & 0xF];
        key++;
    }
    *rp = '\0';
    return result;
}

static bool pgcrypto_decryption_setup(uint8_t *key,
                               ripple_encryption_encrypt_method algo,
                               int32_t keylenth,
                               EncryptInfo *encryptinfo,
                               sm4_context *sm4_ctx_dec)
{
    encryptinfo->algo = algo;
    RIPPLE_ENCRYPTION_UNUSED(keylenth);
    /* add begin by wanghaoyan at 20201026 */
    switch (algo)
    {
#if 0
    case 1: // aes-128
    case 2: // aes-192
    case 3: // aes-256
        if (xts_encrypt_key(key, keylenth, encryptinfo->u.aes_key.enc_ctx) != EXIT_SUCCESS ||
            xts_decrypt_key(key, keylenth, encryptinfo->u.aes_key.dec_ctx) != EXIT_SUCCESS)
        {
            printf("Encryption key setup failed.");
            return false;
        }
        break;
#endif
    case 4: // sm4
        sm4_setkey_dec(sm4_ctx_dec, key);
        break;
#if 0
    case 5: // bf
        blowfish_setkey(&(encryptinfo.u.bf_key), key, keylenth);
        break;
    case 6: // des
        des_setkey(&encryptinfo, key, keylenth);
        break;
    case 7: // des3
        des3_setkey(&encryptinfo, key, keylenth);
        break;
    case 8: // cast5
        cast_setkey(&encryptinfo, key, keylenth);
        break;
#endif
    default:
        break;
    }
    /*
    sm4_setkey_enc(&sm4_ctx_enc, key);
    sm4_setkey_dec(&sm4_ctx_dec, key);
    */
    /* add end by wanghaoyan at 20201026 */
    return true;
}

static bool pgcrypto_encryption_setup(uint8_t *key,
                               ripple_encryption_encrypt_method algo,
                               int32_t keylenth,
                               EncryptInfo *encryptinfo,
                               sm4_context *sm4_ctx_enc)
{
    encryptinfo->algo = algo;
    XK_PG_PARSER_UNUSED(keylenth);
    /* add begin by wanghaoyan at 20201026 */
    switch (algo)
    {
#if 0
    case 1: // aes-128
    case 2: // aes-192
    case 3: // aes-256
        if (xts_encrypt_key(key, keylenth, encryptinfo->u.aes_key.enc_ctx) != EXIT_SUCCESS ||
            xts_decrypt_key(key, keylenth, encryptinfo->u.aes_key.dec_ctx) != EXIT_SUCCESS)
        {
            printf("Encryption key setup failed.");
            return false;
        }
        break;
#endif
    case 4: // sm4
        sm4_setkey_enc(sm4_ctx_enc, key);
        break;
#if 0
    case 5: // bf
        blowfish_setkey(&(encryptinfo.u.bf_key), key, keylenth);
        break;
    case 6: // des
        des_setkey(&encryptinfo, key, keylenth);
        break;
    case 7: // des3
        des3_setkey(&encryptinfo, key, keylenth);
        break;
    case 8: // cast5
        cast_setkey(&encryptinfo, key, keylenth);
        break;
#endif
    default:
        break;
    }
    /*
    sm4_setkey_enc(&sm4_ctx_enc, key);
    sm4_setkey_dec(&sm4_ctx_dec, key);
    */
    /* add end by wanghaoyan at 20201026 */
    return true;
}

static int32_t get_keylength(ripple_encryption_encrypt_method algo)
{
    int32_t length = 0;

    switch (algo)
    {
    case 1: // aes-128
        length = 32;
        break;
    case 2: // aes-192
        length = 48;
        break;
    case 3: // aes-256
        length = 64;
        break;
    case 4: // sm4
        length = 16;
        break;
    case 5: // bf
        length = 56;
        break;
    case 6: // des
        length = 8;
        break;
    case 7: // des3
        length = 24;
        break;
    case 8: // cast5
        length = 16;
        break;
    default:
        break;
    }
    return length;
}

static char *hashkey(ripple_encryption_encrypt_method algo)
{
    int32_t length = 0;

    char *result;
    char *key = NULL;
    char *key1 = NULL;
    MD5_CTX md5;

    RIPPLE_ENCRYPTION_UNUSED(algo);
    RIPPLE_ENCRYPTION_UNUSED(key1);
#if 0 /* 所有 */
    SHA256_CTX sha256;
    SHA384_CTX sha384;
    SHA512_CTX sha512;


    switch (algo)
    {
    case 1: // aes-128
        length = 32;
        key = palloc(length);
        SHA256_Init(&sha256);
        SHA256_Update(&sha256, (uint8 *)FDE_KEY, strlen(FDE_KEY));
        SHA256_Final((uint8 *)key, &sha256);
        break;
    case 2: // aes-192
        length = 48;
        key = palloc(length);
        SHA384_Init(&sha384);
        SHA384_Update(&sha384, (uint8 *)FDE_KEY, strlen(FDE_KEY));
        SHA384_Final((uint8 *)key, &sha384);
        break;
    case 3: // aes-256
        length = 64;
        key = palloc(length);
        SHA512_Init(&sha512);
        SHA512_Update(&sha512, (uint8 *)FDE_KEY, strlen(FDE_KEY));
        SHA512_Final((uint8 *)key, &sha512);
        break;
    case 4: // SM4
        length = 16;
        key = palloc(length);
        MD5Init(&md5);
        MD5Update(&md5, (uint8 *)FDE_KEY, strlen(FDE_KEY));
        MD5Final((uint8 *)key, &md5);
        break;
    case 5: // bf
        length = 56;
        key1 = palloc(64);
        SHA512_Init(&sha512);
        SHA512_Update(&sha512, (uint8 *)FDE_KEY, strlen(FDE_KEY));
        SHA512_Final((uint8 *)key1, &sha512);
        key = palloc(length);
        memcpy(key, key1, length);
        pfree(key1);
        break;
    case 6: // des
        length = 8;
        key1 = palloc(16);
        MD5Init(&md5);
        MD5Update(&md5, (uint8 *)FDE_KEY, strlen(FDE_KEY));
        MD5Final((uint8 *)key1, &md5);
        key = palloc(length);
        memcpy(key, key1, length);
        pfree(key1);
        break;
    case 7: // des3
        length = 24;
        key1 = palloc(32);
        SHA256_Init(&sha256);
        SHA256_Update(&sha256, (uint8 *)FDE_KEY, strlen(FDE_KEY));
        SHA256_Final((uint8 *)key1, &sha256);
        key = palloc(length);
        memcpy(key, key1, length);
        pfree(key1);
        break;
    case 8: // cast5
        length = 16;
        key = palloc(length);
        MD5Init(&md5);
        MD5Update(&md5, (uint8 *)FDE_KEY, strlen(FDE_KEY));
        MD5Final((uint8 *)key, &md5);
        break;
    default:
        printf("Invalid algo\n");
        exit(0);
    }
    result = hex_encode(key, length);
    pfree(key);

    return result;
#endif
    length = 16;
    key = rmalloc0(length);
    MD5Init(&md5);
    MD5Update(&md5, (uint8_t *)FDE_KEY, strlen(FDE_KEY));
    MD5Final((uint8_t *)key, &md5);
    result = hex_encode(key, length);
    rfree(key);
    return result;
}

#define TWEAK_SIZE 16

static void
XLogTweak(char *tweak, uint32_t timeline, uint64_t segment, uint32_t offset)
{
    memcpy(tweak, &segment, sizeof(uint64_t));
    memcpy(tweak  + sizeof(uint64_t), &offset, sizeof(offset));
    memcpy(tweak + sizeof(uint64_t) + sizeof(uint32_t), &timeline, sizeof(timeline));
}


static bool check_magic_valid(char *decrypt_data)
{
    RIPPLE_ENCRYPTION_UNUSED(decrypt_data);
    //ripple_encryption_hgdb_v457_XLogPageHeaderData *page_header =
    //            (ripple_encryption_hgdb_v457_XLogPageHeaderData *)decrypt_data;
    
    //if (!(RIPPLE_ENCRYPTION_HGDB_V457_XLOG_PAGE_MAGIC1 == page_header->xlp_magic
    // || RIPPLE_ENCRYPTION_HGDB_V457_XLOG_PAGE_MAGIC2 == page_header->xlp_magic))
    //    return false;

    return true;
}



bool ripple_encryption_decrypt_waldata(char *input,
                         char *output,
                         size_t block_size,
                         uint32_t timeline,
                         ripple_encryption_encrypt_method method)
{
    EncryptInfo *encryptinfo = NULL;
    sm4_context *sm4_ctx_dec = NULL;
    ripple_encryption_encrypt_key enckey = {'\0'};
    bool         result = true;
    char         tweak[TWEAK_SIZE] = {'\0'};

    encryptinfo = rmalloc0(sizeof(EncryptInfo));
    if (!encryptinfo)
    {
        result = false;
        goto ripple_encryption_try_decrypt_waldata_do_clean;
    }

    sm4_ctx_dec = rmalloc0(sizeof(sm4_context));
    if (!sm4_ctx_dec)
    {
        result = false;
        goto ripple_encryption_try_decrypt_waldata_do_clean;
    }
    enckey.key_len = get_keylength(method);
    if (0 == enckey.key_len)
    {
        result = false;
        goto ripple_encryption_try_decrypt_waldata_do_clean;
    }
    enckey.key = hashkey(method);
    if (!pgcrypto_decryption_setup((uint8_t *)enckey.key, method, enckey.key_len, encryptinfo, sm4_ctx_dec))
    {
        result = false;
        goto ripple_encryption_try_decrypt_waldata_do_clean;
    }
    XLogTweak(tweak, timeline, 1,0);
    decrypt_block(input, output, block_size, tweak, encryptinfo, sm4_ctx_dec);

    if (!check_magic_valid(output))
    {
        result = false;
    }
ripple_encryption_try_decrypt_waldata_do_clean:
    if (encryptinfo)
        rfree(encryptinfo);
    if (sm4_ctx_dec)
        rfree(sm4_ctx_dec);
    if (enckey.key)
        rfree(enckey.key);

    return result;
}

bool ripple_encryption_encrypt_waldata(char *input,
                                       char *output,
                                       size_t block_size,
                                       uint32_t timeline,
                                       ripple_encryption_encrypt_method method)
{
    EncryptInfo *encryptinfo = NULL;
    sm4_context *sm4_ctx_dec = NULL;
    ripple_encryption_encrypt_key enckey = {'\0'};
    bool         result = true;
    char         tweak[TWEAK_SIZE] = {'\0'};

    encryptinfo = rmalloc0(sizeof(EncryptInfo));
    if (!encryptinfo)
    {
        result = false;
        goto ripple_encryption_try_encrypt_waldata_do_clean;
    }

    sm4_ctx_dec = rmalloc0(sizeof(sm4_context));
    if (!sm4_ctx_dec)
    {
        result = false;
        goto ripple_encryption_try_encrypt_waldata_do_clean;
    }
    enckey.key_len = get_keylength(method);
    if (0 == enckey.key_len)
    {
        result = false;
        goto ripple_encryption_try_encrypt_waldata_do_clean;
    }
    enckey.key = hashkey(method);
    if (!pgcrypto_encryption_setup((uint8_t *)enckey.key, method, enckey.key_len, encryptinfo, sm4_ctx_dec))
    {
        result = false;
        goto ripple_encryption_try_encrypt_waldata_do_clean;
    }
    XLogTweak(tweak, timeline, 1,0);
    encrypt_block(input, output, block_size, tweak, encryptinfo, sm4_ctx_dec);

ripple_encryption_try_encrypt_waldata_do_clean:
    if (encryptinfo)
        rfree(encryptinfo);
    if (sm4_ctx_dec)
        rfree(sm4_ctx_dec);
    if (enckey.key)
        rfree(enckey.key);

    return result;
}
