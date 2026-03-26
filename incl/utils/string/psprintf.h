/*-------------------------------------------------------------------------
 *
 * psprintf.h
 *      exported definitions for src/utils/string/psprintf.c
 *
 *
 * All Copyright (c) 2024-2024, Byte Sync Development Group
 *
 * Author: liuzihe  Date: 2024/07/10
 *
 * incl/utils/string/psprintf.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PSPRINTF_H
#define PSPRINTF_H

extern char* psprintf(const char* fmt, ...) pg_attribute_printf(1, 2);
extern size_t pvsnprintf(char* buf, size_t len, const char* fmt, va_list args)
    pg_attribute_printf(3, 0);

#endif
