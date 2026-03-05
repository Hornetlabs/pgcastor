/**
 * @file xk_pg_parser_mcxt.h
 * @author ByteSynch
 * @brief 定义内存申请、释放的封装函数
 * @version 0.1
 * @date 2023-07-24
 * 
 * @copyright Copyright (c) 2023
 * 
 */

#ifndef XK_PG_PARSER_MCXT_H
#define XK_PG_PARSER_MCXT_H

/**
 * @brief           申请内存函数，并初始化内存空间
 *
 * @param[out]      ref_mcxt       内存池，当前没用
 * @param[out]      ref_pointer    内存地址的指针
 * @param[in]       in_size        申请内存空间的大小
 * @retval          true           函数调用成功
 * @retval          false          函数调用失败
 */
extern bool xk_pg_parser_mcxt_malloc(void *ref_mcxt, void **ref_pointer, int32_t in_size);

/**
 * @brief           重新申请内存函数
 *
 * @param[out]      ref_mcxt       内存池，当前没用
 * @param[out]      ref_pointer    内存地址的指针
 * @param[in]       in_size        申请内存空间的大小
 * @retval          true           函数调用成功
 * @retval          false          函数调用失败
 */
extern bool xk_pg_parser_mcxt_realloc(void *ref_mcxt, void **ref_pointer, int32_t in_size);

/**
 * @brief           释放内存空间
 *
 * @param[in]       ref_mcxt       内存池，当前没用
 * @param[in]       ref_pointer    内存地址的指针
 */
extern void xk_pg_parser_mcxt_free(void *ref_mcxt, void *ref_pointer);

extern char *xk_pg_parser_mcxt_strdup(const char *in);
extern char *xk_pg_parser_mcxt_strndup(const char *in, size_t len);

#endif /* XK_PG_PARSER_MCXT_H */
