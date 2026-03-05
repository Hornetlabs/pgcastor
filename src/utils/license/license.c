/*-------------------------------------------------------------------------
 *
 * license.c
 *      license check main function
 *
 *
 * All Copyright (c) 2024-2024, Byte Sync Development Group
 *
 * Author: liuzihe  Date: 2024/08/02 
 * 
 * src/utils/license/license.c
 *
 *-------------------------------------------------------------------------
 */
#include "ripple_app_incl.h"

#include "utils/license/license_util.h"
#include "utils/license/license.h"
#include "port/file/fd.h"

#define LICENSE_MEM_ALLOC rmalloc0
#define LICENSE_MEM_FREE rfree

#define CHECK_DATA_SIZE(size) ((size % 16) == 0) ? true : false

/* 获取加密的license数据 */
static bool license_get_lic_data(ripple_license_ctx *ctx)
{
    int fd = -1;
    size_t lic_size = 0;
    char *lic_data = NULL;

    fd = FileOpen(ctx->license_path, O_RDONLY, 0);

    /* 不存在授权文件的情况下, 报错退出 */
    if (fd < 0)
    {
        elog(RLOG_WARNING, "can't open license file: %s, please check!", ctx->license_path);
        return false;
    }

    lic_size = FileSize(fd);
    lic_data = LICENSE_MEM_ALLOC(lic_size);
    lseek(fd, 0, SEEK_SET);

    /* 无法读取文件的情况下, 报错退出 */
    if (FileRead(fd, lic_data, lic_size) < 0)
    {
        elog(RLOG_WARNING, "can't read license file: %s, please check!", ctx->license_path);
        return false;
    }
    FileClose(fd);

    /* 赋值 */
    ctx->endata = lic_data;
    ctx->endata_size = lic_size;
    return true;
}

bool ripple_license_check(char *lic_path)
{
    ripple_license_ctx ctx = {'\0'};
    uint16_t    endata_header_len = 0;
    size_t      endata_real_size = 0;
    char       *license_endata = NULL;
    char       *decrypt_license = NULL;
    char       *uncompress_data = NULL;
    char       *debase64_data = NULL;
    uint32_t    debase64_data_len = 0;
    size_t      uncompress_data_len = 0;

    ctx.license_path = lic_path;
    ctx.password = "ByteSynch@2022";

    /* 获取license数据 */
    license_get_lic_data(&ctx);

    /* base64解码 */
    debase64_data_len = ctx.endata_size;
    debase64_data_len = ((debase64_data_len * 3) >> 2);

    debase64_data = LICENSE_MEM_ALLOC(debase64_data_len);
    rmemset0(debase64_data, 0, 0, debase64_data_len);

    ctx.endata_size = ripple_license_base64_decode((const uint8_t *)ctx.endata, (uint8_t *)debase64_data, ctx.endata_size);
    if (!ctx.endata_size)
    {
        LICENSE_MEM_FREE(ctx.endata);
        LICENSE_MEM_FREE(debase64_data);
        elog(RLOG_WARNING, "decode base64 license failed!");
        return false;
    }
    LICENSE_MEM_FREE(ctx.endata);
    ctx.endata = debase64_data;

    /* 按格式获取license头部的salt和iv */
    endata_header_len = license_get_salt_and_iv(&ctx);

    /* 获取license加密部分和实际偏移 */
    license_endata = ctx.endata + endata_header_len;
    endata_real_size = ctx.endata_size - endata_header_len;

    /* 检查license加密部分长度是否是16的倍数 */
    if (!CHECK_DATA_SIZE(endata_real_size))
    {
        elog(RLOG_WARNING, "check license data size error!");
        return false;
    }

    /* 使用pbkdf2的方式获取盐值和密钥迭代后的密钥 */
    ripple_license_get_pbkdf2_key(&ctx);

    /* 分配解密空间 */
    decrypt_license = LICENSE_MEM_ALLOC(endata_real_size);
    rmemset0(decrypt_license, 0, 0, endata_real_size);
    rmemcpy0(decrypt_license, 0, license_endata, endata_real_size);

    /* 使用aes128算法解密 */
    ripple_license_decrypt(&ctx, decrypt_license, endata_real_size);

    /* 使用zlib算法对解密后的数据解压缩 */
    uncompress_data = ripple_license_decompress(decrypt_license, endata_real_size, &uncompress_data_len);
    if (!uncompress_data)
    {
        elog(RLOG_WARNING, "uncompress license error!");
        return false;
    }
    /* 日期检查 */
    ripple_license_check_valid_time(uncompress_data);

    if (ctx.endata)
    {
        LICENSE_MEM_FREE(ctx.endata);
    }

    if (decrypt_license)
    {
        LICENSE_MEM_FREE(decrypt_license);
    }

    if (uncompress_data)
    {
        LICENSE_MEM_FREE(uncompress_data);
    }
    return true;
}

bool ripple_license_get_time(char *lic_path, uint64_t *start, uint64_t *end)
{
    ripple_license_ctx ctx = {'\0'};
    uint16_t    endata_header_len = 0;
    size_t      endata_real_size = 0;
    char       *license_endata = NULL;
    char       *decrypt_license = NULL;
    char       *uncompress_data = NULL;
    char       *debase64_data = NULL;
    uint32_t    debase64_data_len = 0;
    size_t      uncompress_data_len = 0;

    ctx.license_path = lic_path;
    ctx.password = "ByteSynch@2022";

    /* 获取license数据 */
    license_get_lic_data(&ctx);

    /* base64解码 */
    debase64_data_len = ctx.endata_size;
    debase64_data_len = ((debase64_data_len * 3) >> 2);

    debase64_data = LICENSE_MEM_ALLOC(debase64_data_len);
    rmemset0(debase64_data, 0, 0, debase64_data_len);

    ctx.endata_size = ripple_license_base64_decode((const uint8_t *)ctx.endata, (uint8_t *)debase64_data, ctx.endata_size);
    if (!ctx.endata_size)
    {
        LICENSE_MEM_FREE(ctx.endata);
        LICENSE_MEM_FREE(debase64_data);
        elog(RLOG_WARNING, "decode base64 license failed!");
        return false;
    }
    LICENSE_MEM_FREE(ctx.endata);
    ctx.endata = debase64_data;

    /* 按格式获取license头部的salt和iv */
    endata_header_len = license_get_salt_and_iv(&ctx);

    /* 获取license加密部分和实际偏移 */
    license_endata = ctx.endata + endata_header_len;
    endata_real_size = ctx.endata_size - endata_header_len;

    /* 检查license加密部分长度是否是16的倍数 */
    if (!CHECK_DATA_SIZE(endata_real_size))
    {
        elog(RLOG_WARNING, "check license data size error!");
        return false;
    }

    /* 使用pbkdf2的方式获取盐值和密钥迭代后的密钥 */
    ripple_license_get_pbkdf2_key(&ctx);

    /* 分配解密空间 */
    decrypt_license = LICENSE_MEM_ALLOC(endata_real_size);
    rmemset0(decrypt_license, 0, 0, endata_real_size);
    rmemcpy0(decrypt_license, 0, license_endata, endata_real_size);

    /* 使用aes128算法解密 */
    ripple_license_decrypt(&ctx, decrypt_license, endata_real_size);

    /* 使用zlib算法对解密后的数据解压缩 */
    uncompress_data = ripple_license_decompress(decrypt_license, endata_real_size, &uncompress_data_len);
    if (!uncompress_data)
    {
        elog(RLOG_WARNING, "uncompress license error!");
        return false;
    }

    /* 日期检查 */
    ripple_license_get_valid_time(uncompress_data, start, end);

    if (ctx.endata)
    {
        LICENSE_MEM_FREE(ctx.endata);
    }

    if (decrypt_license)
    {
        LICENSE_MEM_FREE(decrypt_license);
    }

    if (uncompress_data)
    {
        LICENSE_MEM_FREE(uncompress_data);
    }

    return true;
}
