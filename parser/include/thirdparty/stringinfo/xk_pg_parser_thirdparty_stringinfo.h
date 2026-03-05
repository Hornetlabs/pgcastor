/**
 * @file xk_pg_parser_thirdparty_stringinfo.h
 * @author byresync
 * @brief 
 * @version 0.1
 * @date 2023-07-26
 * 
 * @copyright Copyright (c) 2023
 * 
 */
#ifndef XK_PG_PARSER_THIRDPARTY_STRINGINFO_H
#define XK_PG_PARSER_THIRDPARTY_STRINGINFO_H

/*-------------------------
 * xk_pg_parser_StringInfoData holds information about an extensible string.
 *  data    is the current buffer for the string (allocated with palloc).
 *  len     is the current string length.  There is guaranteed to be
 *          a terminating '\0' at data[len], although this is not very
 *          useful when the string holds binary data rather than xk_pg_parser_text.
 *  maxlen  is the allocated size in bytes of 'data', i.e. the maximum
 *          string size (including the terminating '\0' char) that we can
 *          currently store in 'data' without having to reallocate
 *          more space.  We must always have maxlen > len.
 *  cursor  is initialized to zero by xk_pg_parser_makeStringInfo or xk_pg_parser_initStringInfo,
 *          but is not otherwise touched by the stringinfo.c routines.
 *          Some routines use it to scan through a xk_pg_parser_StringInfo.
 *-------------------------
 */
typedef struct xk_pg_parser_StringInfoData
{
    char           *data;
    int32_t         len;
    int32_t         maxlen;
    int32_t         cursor;
} xk_pg_parser_StringInfoData;

typedef xk_pg_parser_StringInfoData *xk_pg_parser_StringInfo;

/*------------------------
 * There are two ways to create a xk_pg_parser_StringInfo object initially:
 *
 * xk_pg_parser_StringInfo stringptr = xk_pg_parser_makeStringInfo();
 *        Both the xk_pg_parser_StringInfoData and the data buffer are palloc'd.
 *
 * xk_pg_parser_StringInfoData string;
 * xk_pg_parser_initStringInfo(&string);
 *        The data buffer is palloc'd but the xk_pg_parser_StringInfoData is just local.
 *        This is the easiest approach for a xk_pg_parser_StringInfo object that will
 *        only live as long as the current routine.
 *
 * To destroy a xk_pg_parser_StringInfo, pfree() the data buffer, and then pfree() the
 * xk_pg_parser_StringInfoData if it was palloc'd.  There's no special support for this.
 *
 * NOTE: some routines build up a string using xk_pg_parser_StringInfo, and then
 * release the xk_pg_parser_StringInfoData but return the data string itself to their
 * caller.  At that point the data string looks like a plain palloc'd
 * string.
 *-------------------------
 */

/*------------------------
 * xk_pg_parser_makeStringInfo
 * Create an empty 'xk_pg_parser_StringInfoData' & return a pointer to it.
 */
extern xk_pg_parser_StringInfo xk_pg_parser_makeStringInfo(void);

/*------------------------
 * xk_pg_parser_initStringInfo
 * Initialize a xk_pg_parser_StringInfoData struct (with previously undefined contents)
 * to describe an empty string.
 */
extern void xk_pg_parser_initStringInfo(xk_pg_parser_StringInfo str);

/*------------------------
 * xk_pg_parser_resetStringInfo
 * Clears the current content of the xk_pg_parser_StringInfo, if any. The
 * xk_pg_parser_StringInfo remains valid.
 */
extern void xk_pg_parser_resetStringInfo(xk_pg_parser_StringInfo str);

/*------------------------
 * xk_pg_parser_appendStringInfo
 * Format xk_pg_parser_text data under the control of fmt (an sprintf-style format string)
 * and append it to whatever is already in str.  More space is allocated
 * to str if necessary.  This is sort of like a combination of sprintf and
 * strcat.
 */
extern void xk_pg_parser_appendStringInfo(xk_pg_parser_StringInfo str, const char *fmt,...) __attribute__((format(printf, 2, 3)));

/*------------------------
 * xk_pg_parser_appendStringInfoVA
 * Attempt to format xk_pg_parser_text data under the control of fmt (an sprintf-style
 * format string) and append it to whatever is already in str.  If successful
 * return zero; if not (because there's not enough space), return an estimate
 * of the space needed, without modifying str.  Typically the caller should
 * pass the return value to xk_pg_parser_enlargeStringInfo() before trying again; see
 * xk_pg_parser_appendStringInfo for standard usage pattern.
 */
extern int32_t    xk_pg_parser_appendStringInfoVA(xk_pg_parser_StringInfo str, const char *fmt, va_list args) __attribute__((format(printf, 2, 0)));

/*------------------------
 * xk_pg_parser_appendStringInfoString
 * Append a null-terminated string to str.
 * Like xk_pg_parser_appendStringInfo(str, "%s", s) but faster.
 */
extern void xk_pg_parser_appendStringInfoString(xk_pg_parser_StringInfo str, const char *s);

/*------------------------
 * xk_pg_parser_appendStringInfoChar
 * Append a single byte to str.
 * Like xk_pg_parser_appendStringInfo(str, "%c", ch) but much faster.
 */
extern void xk_pg_parser_appendStringInfoChar(xk_pg_parser_StringInfo str, char ch);

/*------------------------
 * xk_pg_parser_appendStringInfoCharMacro
 * As above, but a macro for even more speed where it matters.
 * Caution: str argument will be evaluated multiple times.
 */
#define xk_pg_parser_appendStringInfoCharMacro(str,ch) \
    (((str)->len + 1 >= (str)->maxlen) ? \
     xk_pg_parser_appendStringInfoChar(str, ch) : \
     (void)((str)->data[(str)->len] = (ch), (str)->data[++(str)->len] = '\0'))

/*------------------------
 * xk_pg_parser_appendStringInfoSpaces
 * Append a given number of spaces to str.
 */
extern void xk_pg_parser_appendStringInfoSpaces(xk_pg_parser_StringInfo str, int32_t count);

/*------------------------
 * xk_pg_parser_appendBinaryStringInfo
 * Append arbitrary binary data to a xk_pg_parser_StringInfo, allocating more space
 * if necessary.
 */
extern void xk_pg_parser_appendBinaryStringInfo(xk_pg_parser_StringInfo str,
                                   const char *data, int32_t datalen);

/*------------------------
 * xk_pg_parser_enlargeStringInfo
 * Make sure a xk_pg_parser_StringInfo's buffer can hold at least 'needed' more bytes.
 */
extern void xk_pg_parser_enlargeStringInfo(xk_pg_parser_StringInfo str, int32_t needed);

#endif                            /* XK_PG_PARSER_THIRDPARTY_STRINGINFO_H */
