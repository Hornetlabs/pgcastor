/**
 * @file xk_pg_parser_mcxt.c
 * @author ByteSynch
 * @brief 内存申请、释放的封装函数的实现
 * @version 0.1
 * @date 2023-07-24
 * 
 * @copyright Copyright (c) 2023
 * 
 */
#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"

#define SIZE_ZERO 0
/**
 * @brief           申请内存函数，并初始化内存空间
 *
 * @param[out]      ref_mcxt       内存池，当前没用
 * @param[out]      ref_pointer    内存地址的指针
 * @param[in]       in_size        申请内存空间的大小
 * @retval          true           函数调用成功
 * @retval          false          函数调用失败
 */
bool xk_pg_parser_mcxt_malloc(void *ref_mcxt, void **ref_pointer, int32_t in_size)
{
    (void)ref_mcxt; /* keep complier quite */

    if (SIZE_ZERO == in_size)
        return true;

    *ref_pointer = rmalloc0(sizeof(char) * in_size);
    if (NULL == *ref_pointer)
    {
        return false;
    }

    rmemset0(*ref_pointer, 0, 0, in_size);

    return true;
}

/**
 * @brief           重新申请内存函数
 *
 * @param[out]      ref_mcxt       内存池，当前没用
 * @param[out]      ref_pointer    内存地址的指针
 * @param[in]       in_size        申请内存空间的大小
 * @retval          true           函数调用成功
 * @retval          false          函数调用失败
 */
bool xk_pg_parser_mcxt_realloc(void *ref_mcxt, void **ref_pointer, int32_t in_size)
{
    (void)ref_mcxt; /* keep complier quite */

    if (SIZE_ZERO == in_size)
        return true;

    *ref_pointer = rrealloc0(*ref_pointer, sizeof(char) * in_size);
    if (NULL == *ref_pointer)
    {
        return false;
    }

    return true;
}

/**
 * @brief           释放内存空间
 *
 * @param[in]       ref_mcxt       内存池，当前没用
 * @param[in]       ref_pointer    内存地址的指针
 */
void xk_pg_parser_mcxt_free(void *ref_mcxt, void *ref_pointer)
{
    (void)ref_mcxt; /* keep complier quite */

    if (NULL == ref_pointer)
    {
        return;
    }

    rfree(ref_pointer);
    ref_pointer = NULL;
}

/**
 * @brief         安全的字符串拷贝
 * 
 * @param[in] in 
 * @return char* 
 */
char *xk_pg_parser_mcxt_strdup(const char *in)
{
    char       *tmp;
    if (!in)
        return NULL;
    tmp = rstrdup(in);
    if (!tmp)
        return NULL;
    return tmp;
}

/**
 * @brief 拷贝n个字符到新的字符串中
 * 
 * @param[in] in len 
 * @return char* 
 */
char *xk_pg_parser_mcxt_strndup(const char *in, size_t len)
{
    char       *out;

    len = strnlen(in, len);

    out = rmalloc0(len + 1);
    rmemcpy0(out, 0, in, len);
    out[len] = '\0';

    return out;
}
