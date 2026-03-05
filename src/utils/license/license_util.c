/*-------------------------------------------------------------------------
 *
 * license_util.c
 *
 *
 * All Copyright (c) 2024-2024, Byte Sync Development Group
 *
 * Author: liuzihe  Date: 2024/08/02 
 * 
 * src/utils/license/license_util.c
 *
 *-------------------------------------------------------------------------
 */
#include "ripple_app_incl.h"
#include "utils/license/license_util.h"
#include "utils/license/license.h"
#include "utils/miniz/miniz.h"

#define LICENSE_MEM_ALLOC rmalloc0
#define LICENSE_MEM_FREE rfree

#define tag_Boolean                 0x01
#define tag_Integer                 0x02
#define tag_BitString               0x03
#define tag_OctetString             0x04
#define tag_Null                    0x05
#define tag_ObjectId                0x06
#define tag_Enumerated              0x0A
#define tag_UTF8String              0x0C
#define tag_PrintableString         0x13
#define tag_T61String               0x14
#define tag_IA5String               0x16
#define tag_UtcTime                 0x17
#define tag_GeneralizedTime         0x18
#define tag_GeneralString           0x1B
#define tag_UniversalString         0x1C
#define tag_BMPString               0x1E
#define tag_Sequence                0x30
#define tag_SequenceOf              0x30
#define tag_Set                     0x31
#define tag_SetOf                   0x31

#define LICENSE_PARAM_KDF_OID_LEN   9
#define LICENSE_PARAM_PRF_OID_LEN   8
#define LICENSE_PARAM_ES_OID_LEN    9
#define LICENSE_SALT_LEN            20
#define LICENSE_IV_LEN              16

#define LICENSE_CHECK_OUTOF_DATA(offset, max_size) \
do \
{ \
    if (offset > max_size) \
        elog(RLOG_ERROR, "when decoding license, out of data!"); \
} while (0)



/* endata数据指针和offset偏移推进 */
#define LICENSE_READ_SKIP(endata, num, offset, max_size) \
do \
{ \
    endata += num; \
    offset += num; \
    LICENSE_CHECK_OUTOF_DATA(offset, max_size); \
} while(0)

/* 检查sign类型 */
#define LICENSE_CHECK_SIGN_TYPE(sign, type) \
do \
{ \
    if (sign != type) \
        elog(RLOG_ERROR, "sign type error when check license!"); \
    sign = 0; \
} while(0)

/* 读取一个字节长度的sign标签 */
#define LICENSE_READ_SIGN(endata, sign, offset, max_size) \
do \
{ \
    sign = (uint8_t) *endata; \
    LICENSE_READ_SKIP(endata, 1, offset, max_size); \
} while(0)

/* 读取一个字节长度的len长度 */
#define LICENSE_READ_LEN(endata, len, offset, max_size) \
do \
{ \
    size_t lenByte = 0; \
    lenByte = (uint8_t) *endata; \
    LICENSE_READ_SKIP(endata, 1, offset, max_size); \
    if ((lenByte & 0x80) == 0) \
        len = lenByte; \
    else \
        elog(RLOG_ERROR, "when decoding license, get param len failed!"); \
} while(0)

/* 读取一个字节长度的len长度, 检验正确性后再将长度置为0, 无需解析长度信息时使用此宏定义 */
#define LICENSE_READ_LEN_AND_SKIP(endata, len, offset, max_size) \
do \
{ \
    LICENSE_READ_LEN(endata, len, offset, max_size); \
    len = 0; \
} while(0)

/* 从license中获取salt盐值和iv向量 */
uint16_t license_get_salt_and_iv(ripple_license_ctx *ctx)
{
    uint8_t license_kdf_oid_byte[9] = {0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x05, 0x0C};
    uint8_t license_prf_oid_byte[8] = {0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x02, 0x09};
    uint8_t license_es_oid_byte[9] = {0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x01, 0x02};

    char       *endata = ctx->endata;
    uint8_t     sign = 0;
    uint8_t     len = 0;
    uint16_t    offset = 0;
    uint16_t    max_size = 0;

    /* 读取加密相关信息的总长度 */
    max_size = get16bit((uint8_t **)&endata) + 2;
    offset += 2;

    /* 读取PARAM数据标签 */
    LICENSE_READ_SIGN(endata, sign, offset, max_size);
    LICENSE_CHECK_SIGN_TYPE(sign, tag_Sequence);

    /* 跳过PARAM数据长度记录, 不需要 */
    LICENSE_READ_LEN_AND_SKIP(endata, len, offset, max_size);

    /* 读取kdf标签 */
    LICENSE_READ_SIGN(endata, sign, offset, max_size);
    LICENSE_CHECK_SIGN_TYPE(sign, tag_Sequence);

    /* 跳过kdf数据长度记录, 不需要 */
    LICENSE_READ_LEN_AND_SKIP(endata, len, offset, max_size);

    /* 读取kdf->oid标签 */
    LICENSE_READ_SIGN(endata, sign, offset, max_size);
    LICENSE_CHECK_SIGN_TYPE(sign, tag_ObjectId);

    /* 获取kdf->oid数据长度记录 */
    LICENSE_READ_LEN(endata, len, offset, max_size);
    if (len != LICENSE_PARAM_KDF_OID_LEN)
        elog(RLOG_ERROR, "when decoding license, check len failed!");

    if (memcmp(license_kdf_oid_byte, endata, LICENSE_PARAM_KDF_OID_LEN))
        elog(RLOG_ERROR, "when decoding license, check endata failed!");

    /* 跳过kdf->oid数据部分 */
    LICENSE_READ_SKIP(endata, LICENSE_PARAM_KDF_OID_LEN, offset, max_size);

    /* 读取kdf->param标签 */
    LICENSE_READ_SIGN(endata, sign, offset, max_size);
    LICENSE_CHECK_SIGN_TYPE(sign, tag_Sequence);

    /* 跳过kdf->param数据长度记录, 不需要 */
    LICENSE_READ_LEN_AND_SKIP(endata, len, offset, max_size);

    /* 读取kdf->params->specified标签 */
    LICENSE_READ_SIGN(endata, sign, offset, max_size);
    LICENSE_CHECK_SIGN_TYPE(sign, tag_OctetString);

    /* 获取kdf->params->specified数据长度记录 */
    LICENSE_READ_LEN(endata, len, offset, max_size);
    if (len != LICENSE_SALT_LEN)
        elog(RLOG_ERROR, "when decoding license, check salt failed!");

    rmemcpy1(ctx->salt, 0, endata, LICENSE_SALT_LEN);
    /* 已获取salt, 推进 */
    LICENSE_READ_SKIP(endata, LICENSE_SALT_LEN, offset, max_size);

    /* 读取kdf->param->icount标签 */
    LICENSE_READ_SIGN(endata, sign, offset, max_size);
    LICENSE_CHECK_SIGN_TYPE(sign, tag_Integer);

    /* 读取kdf->param->icount数据长度记录 */
    LICENSE_READ_LEN(endata, len, offset, max_size);
    /* 无需获取数据, 不需要 */
    LICENSE_READ_SKIP(endata, len, offset, max_size);
    len = 0;

    /* 读取kdf->keylength标签 */
    LICENSE_READ_SIGN(endata, sign, offset, max_size);
    LICENSE_CHECK_SIGN_TYPE(sign, tag_Integer);

    /* 读取kdf->keylength数据长度记录 */
    LICENSE_READ_LEN(endata, len, offset, max_size);
    /* 无需获取数据, 不需要 */
    LICENSE_READ_SKIP(endata, len, offset, max_size);
    len = 0;

    /* 读取kdf->prf标签 */
    LICENSE_READ_SIGN(endata, sign, offset, max_size);
    LICENSE_CHECK_SIGN_TYPE(sign, tag_Sequence);

    /* 读取kdf->prf数据长度记录, 不需要, 跳过 */
    LICENSE_READ_LEN_AND_SKIP(endata, len, offset, max_size);

    /* 读取kdf->prf->oid标签 */
    LICENSE_READ_SIGN(endata, sign, offset, max_size);
    LICENSE_CHECK_SIGN_TYPE(sign, tag_ObjectId);

    /* 读取kdf->prf->oid数据长度记录 */
    LICENSE_READ_LEN(endata, len, offset, max_size);
    if (len != LICENSE_PARAM_PRF_OID_LEN)
        elog(RLOG_ERROR, "when decoding license, check len failed!");
    len = 0;

    if (memcmp(license_prf_oid_byte, endata, LICENSE_PARAM_PRF_OID_LEN))
        elog(RLOG_ERROR, "when decoding license, check endata failed!");

    /* 跳过kdf->prf->oid数据部分 */
    LICENSE_READ_SKIP(endata, LICENSE_PARAM_PRF_OID_LEN, offset, max_size);

    /* 读取kdf->prf->param标签 */
    LICENSE_READ_SIGN(endata, sign, offset, max_size);
    LICENSE_CHECK_SIGN_TYPE(sign, tag_Null);

    /* 读取kdf->prf->param数据长度记录 */
    LICENSE_READ_LEN(endata, len, offset, max_size);
    if (len != 0)
        elog(RLOG_ERROR, "when decoding license, len check failed!");
    len = 0;

    /* 读取es标签 */
    LICENSE_READ_SIGN(endata, sign, offset, max_size);
    LICENSE_CHECK_SIGN_TYPE(sign, tag_Sequence);

    /* 读取es数据长度记录, 不需要, 跳过 */
    LICENSE_READ_LEN_AND_SKIP(endata, len, offset, max_size);

    /* 读取es->oid标签 */
    LICENSE_READ_SIGN(endata, sign, offset, max_size);
    LICENSE_CHECK_SIGN_TYPE(sign, tag_ObjectId);

    /* 读取es->oid数据长度记录 */
    LICENSE_READ_LEN(endata, len, offset, max_size);
    if (len != LICENSE_PARAM_ES_OID_LEN)
        elog(RLOG_ERROR, "when decoding license, check len failed!");

    if (memcmp(license_es_oid_byte, endata, LICENSE_PARAM_ES_OID_LEN))
        elog(RLOG_ERROR, "when decoding license, check endata failed!");
    /* 跳过kdf->prf->oid数据部分 */
    LICENSE_READ_SKIP(endata, LICENSE_PARAM_ES_OID_LEN, offset, max_size);

    /* 读取es->cipherparam标签 */
    LICENSE_READ_SIGN(endata, sign, offset, max_size);
    LICENSE_CHECK_SIGN_TYPE(sign, tag_OctetString);

    /* 读取es->cipherparam数据长度记录*/
    LICENSE_READ_LEN(endata, len, offset, max_size);
    if (len != LICENSE_IV_LEN)
        elog(RLOG_ERROR, "when decoding license, check iv failed!");

    rmemcpy1(ctx->iv, 0, endata, LICENSE_IV_LEN);

    /* 已获取iv, 推进 */
    LICENSE_READ_SKIP(endata, LICENSE_IV_LEN, offset, max_size);

    /* 此时数据应该全部读完了, 判断是否正确 */
    if (offset != max_size)
        elog(RLOG_ERROR, "when decoding license, check header data failed!");
    return max_size;
}

#define RIPPLE_LICENSE_COMPRESS_SIZE 8192

char *ripple_license_decompress(char *src, size_t src_len, size_t *uncompress_len)
{
    char *decompress = NULL;
    size_t delen = 8192;
    z_stream stream = {'\0'};

    if (inflateInit(&stream) != Z_OK) {
        return NULL;
    }

    decompress = LICENSE_MEM_ALLOC(RIPPLE_LICENSE_COMPRESS_SIZE);
    rmemset1(decompress, 0, 0, RIPPLE_LICENSE_COMPRESS_SIZE);

    stream.next_in = (const unsigned char *)src;
    stream.avail_in = src_len;
    stream.next_out = (unsigned char *)decompress;
    stream.avail_out = delen;

    if (inflate(&stream, Z_NO_FLUSH) != Z_STREAM_END) {
        return NULL;
    }

    *uncompress_len = stream.total_out;

    if (inflateEnd(&stream) != Z_OK) {
        return NULL;
    }
    return decompress;
}

void ripple_license_check_valid_time(char *data)
{
    char *start = data;
    char *end = data;
    char time_str[32] = {'\0'};
    time_t start_time = 0;
    time_t end_time = 0;
    time_t current_time = 0;

    time(&current_time);
    current_time = 
        (current_time < 1000000000L * 1000) ? (current_time * 1000) : current_time;

    start = strstr(end, "\\\"notAfter\\\":");
    if (!start)
        elog(RLOG_ERROR, "license format error!");
    end = start;

    end = strstr(start, ",");
    if (!end)
        elog(RLOG_ERROR, "license format error!");

    rmemcpy1(time_str, 0, start + strlen("\\\"notAfter\\\":"), end - start);

    end_time = (time_t) strtoul(time_str, NULL, 10);
    rmemset1(time_str, 0, 0, 32);

    start = end;
    start = strstr(end, "\\\"notBefore\\\":");
    if (!start)
        elog(RLOG_ERROR, "license format error!");
    end = start;

    end = strstr(start, ",");
    if (!end)
        elog(RLOG_ERROR, "license format error!");

    rmemcpy1(time_str, 0, start + strlen("\\\"notBefore\\\":"), end - start);
    start_time = (time_t) strtoul(time_str, NULL, 10);

    if (current_time < start_time || current_time > end_time)
        elog(RLOG_ERROR, "The license has expired, please check!");
}

void ripple_license_get_valid_time(char *data, uint64_t *start_res, uint64_t *end_res)
{
    char *start = data;
    char *end = data;
    char time_str[32] = {'\0'};
    time_t start_time = 0;
    time_t end_time = 0;

    start = strstr(end, "\\\"notAfter\\\":");
    if (!start)
        elog(RLOG_ERROR, "license format error!");
    end = start;

    end = strstr(start, ",");
    if (!end)
        elog(RLOG_ERROR, "license format error!");

    rmemcpy1(time_str, 0, start + strlen("\\\"notAfter\\\":"), end - start);

    end_time = (time_t) strtoul(time_str, NULL, 10);
    rmemset1(time_str, 0, 0, 32);

    start = end;
    start = strstr(end, "\\\"notBefore\\\":");
    if (!start)
        elog(RLOG_ERROR, "license format error!");
    end = start;

    end = strstr(start, ",");
    if (!end)
        elog(RLOG_ERROR, "license format error!");

    rmemcpy1(time_str, 0, start + strlen("\\\"notBefore\\\":"), end - start);
    start_time = (time_t) strtoul(time_str, NULL, 10);

    *start_res = start_time;
    *end_res = end_time;
}

int ripple_license_base64_decode(const uint8_t *src, uint8_t *dst, size_t srclen)
{
    const uint8_t  *srcend = src + srclen,
                   *s = src;
    uint8_t        *p = dst;
    char            c;
    unsigned        b = 0;
    unsigned long   buf = 0;
    int             pos = 0,
                    end = 0;

    while (s < srcend)
    {
        c = *s++;
        if (c >= 'A' && c <= 'Z')
            b = c - 'A';
        else if (c >= 'a' && c <= 'z')
            b = c - 'a' + 26;
        else if (c >= '0' && c <= '9')
            b = c - '0' + 52;
        else if (c == '+')
            b = 62;
        else if (c == '/')
            b = 63;
        else if (c == '=')
        {
            /*
             * end sequence
             */
            if (!end)
            {
                if (pos == 2)
                    end = 1;
                else if (pos == 3)
                    end = 2;
                else
                    return 0;
            }
            b = 0;
        }
        else if (c == ' ' || c == '\t' || c == '\n' || c == '\r')
            continue;
        else
            return 0;

        /*
         * add it to buffer
         */
        buf = (buf << 6) + b;
        pos++;
        if (pos == 4)
        {
            *p++ = (buf >> 16) & 255;
            if (end == 0 || end > 1)
                *p++ = (buf >> 8) & 255;
            if (end == 0 || end > 2)
                *p++ = buf & 255;
            buf = 0;
            pos = 0;
        }
    }

    if (pos != 0)
        return 0;
    return p - dst;
}
