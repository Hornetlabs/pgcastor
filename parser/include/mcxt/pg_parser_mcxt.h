/**
 * @file pg_parser_mcxt.h
 * @author ByteSynch
 * @brief Define memory allocation and release wrapper functions
 * @version 0.1
 * @date 2023-07-24
 *
 * @copyright Copyright (c) 2023
 *
 */

#ifndef PG_PARSER_MCXT_H
#define PG_PARSER_MCXT_H

/**
 * @brief           Memory allocation function, initializes memory space
 *
 * @param[out]      ref_mcxt       Memory pool, currently unused
 * @param[out]      ref_pointer    Pointer to memory address
 * @param[in]       in_size        Size of memory to allocate
 * @retval          true           Function call succeeded
 * @retval          false          Function call failed
 */
extern bool pg_parser_mcxt_malloc(void* ref_mcxt, void** ref_pointer, int32_t in_size);

/**
 * @brief           Reallocate memory function
 *
 * @param[out]      ref_mcxt       Memory pool, currently unused
 * @param[out]      ref_pointer    Pointer to memory address
 * @param[in]       in_size        Size of memory to allocate
 * @retval          true           Function call succeeded
 * @retval          false          Function call failed
 */
extern bool pg_parser_mcxt_realloc(void* ref_mcxt, void** ref_pointer, int32_t in_size);

/**
 * @brief           Free memory space
 *
 * @param[in]       ref_mcxt       Memory pool, currently unused
 * @param[in]       ref_pointer    Pointer to memory address
 */
extern void pg_parser_mcxt_free(void* ref_mcxt, void* ref_pointer);

extern char* pg_parser_mcxt_strdup(const char* in);
extern char* pg_parser_mcxt_strndup(const char* in, size_t len);

#endif /* PG_PARSER_MCXT_H */
