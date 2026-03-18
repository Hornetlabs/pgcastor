/**
 * @file pg_parser_mcxt.c
 * @author ByteSynch
 * @brief Implementation of wrapper functions for memory allocation and release
 * @version 0.1
 * @date 2023-07-24
 *
 * @copyright Copyright (c) 2023
 *
 */
#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"

#define SIZE_ZERO 0

/**
 * @brief           Memory allocation function, and Initialize memory space
 *
 * @param[out]      ref_mcxt       Memory pool, currently not used
 * @param[out]      ref_pointer    Pointer to memory address
 * @param[in]       in_size        Size of memory space to allocate
 * @retval          true           Function call successful
 * @retval          false          Function call failed
 */
bool pg_parser_mcxt_malloc(void* ref_mcxt, void** ref_pointer, int32_t in_size)
{
    (void)ref_mcxt; /* keep complier quite */

    if (SIZE_ZERO == in_size)
    {
        return true;
    }

    *ref_pointer = rmalloc0(sizeof(char) * in_size);
    if (NULL == *ref_pointer)
    {
        return false;
    }

    rmemset0(*ref_pointer, 0, 0, in_size);

    return true;
}

/**
 * @brief           Reallocation memory function
 *
 * @param[out]      ref_mcxt       Memory pool, currently not used
 * @param[out]      ref_pointer    Pointer to memory address
 * @param[in]       in_size        Size of memory space to allocate
 * @retval          true           Function call successful
 * @retval          false          Function call failed
 */
bool pg_parser_mcxt_realloc(void* ref_mcxt, void** ref_pointer, int32_t in_size)
{
    (void)ref_mcxt; /* keep complier quite */

    if (SIZE_ZERO == in_size)
    {
        return true;
    }

    *ref_pointer = rrealloc0(*ref_pointer, sizeof(char) * in_size);
    if (NULL == *ref_pointer)
    {
        return false;
    }

    return true;
}

/**
 * @brief           Release memory space
 *
 * @param[in]       ref_mcxt       Memory pool, currently not used
 * @param[in]       ref_pointer    Pointer to memory address
 */
void pg_parser_mcxt_free(void* ref_mcxt, void* ref_pointer)
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
 * @brief         Safe string copy
 *
 * @param[in] in
 * @return char*
 */
char* pg_parser_mcxt_strdup(const char* in)
{
    char* tmp;
    if (!in)
    {
        return NULL;
    }
    tmp = rstrdup(in);
    if (!tmp)
    {
        return NULL;
    }
    return tmp;
}

/**
 * @brief Copy n characters to new string
 *
 * @param[in] in len
 * @return char*
 */
char* pg_parser_mcxt_strndup(const char* in, size_t len)
{
    char* out;

    len = strnlen(in, len);

    out = rmalloc0(len + 1);
    rmemcpy0(out, 0, in, len);
    out[len] = '\0';

    return out;
}
