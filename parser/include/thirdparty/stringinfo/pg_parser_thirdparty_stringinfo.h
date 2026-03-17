/**
 * @file pg_parser_thirdparty_stringinfo.h
 * @author byresync
 * @brief 
 * @version 0.1
 * @date 2023-07-26
 * 
 * @copyright Copyright (c) 2023
 * 
 */
#ifndef PG_PARSER_THIRDPARTY_STRINGINFO_H
#define PG_PARSER_THIRDPARTY_STRINGINFO_H

/*-------------------------
 * pg_parser_StringInfoData holds information about an extensible string.
 *  data    is the current buffer for the string (allocated with palloc).
 *  len     is the current string length.  There is guaranteed to be
 *          a terminating '\0' at data[len], although this is not very
 *          useful when the string holds binary data rather than pg_parser_text.
 *  maxlen  is the allocated size in bytes of 'data', i.e. the maximum
 *          string size (including the terminating '\0' char) that we can
 *          currently store in 'data' without having to reallocate
 *          more space.  We must always have maxlen > len.
 *  cursor  is initialized to zero by pg_parser_makeStringInfo or pg_parser_initStringInfo,
 *          but is not otherwise touched by the stringinfo.c routines.
 *          Some routines use it to scan through a pg_parser_StringInfo.
 *-------------------------
 */
typedef struct pg_parser_StringInfoData
{
    char           *data;
    int32_t         len;
    int32_t         maxlen;
    int32_t         cursor;
} pg_parser_StringInfoData;

typedef pg_parser_StringInfoData *pg_parser_StringInfo;

/*------------------------
 * There are two ways to create a pg_parser_StringInfo object initially:
 *
 * pg_parser_StringInfo stringptr = pg_parser_makeStringInfo();
 *        Both the pg_parser_StringInfoData and the data buffer are palloc'd.
 *
 * pg_parser_StringInfoData string;
 * pg_parser_initStringInfo(&string);
 *        The data buffer is palloc'd but the pg_parser_StringInfoData is just local.
 *        This is the easiest approach for a pg_parser_StringInfo object that will
 *        only live as long as the current routine.
 *
 * To destroy a pg_parser_StringInfo, pfree() the data buffer, and then pfree() the
 * pg_parser_StringInfoData if it was palloc'd.  There's no special support for this.
 *
 * NOTE: some routines build up a string using pg_parser_StringInfo, and then
 * release the pg_parser_StringInfoData but return the data string itself to their
 * caller.  At that point the data string looks like a plain palloc'd
 * string.
 *-------------------------
 */

/*------------------------
 * pg_parser_makeStringInfo
 * Create an empty 'pg_parser_StringInfoData' & return a pointer to it.
 */
extern pg_parser_StringInfo pg_parser_makeStringInfo(void);

/*------------------------
 * pg_parser_initStringInfo
 * Initialize a pg_parser_StringInfoData struct (with previously undefined contents)
 * to describe an empty string.
 */
extern void pg_parser_initStringInfo(pg_parser_StringInfo str);

/*------------------------
 * pg_parser_resetStringInfo
 * Clears the current content of the pg_parser_StringInfo, if any. The
 * pg_parser_StringInfo remains valid.
 */
extern void pg_parser_resetStringInfo(pg_parser_StringInfo str);

/*------------------------
 * pg_parser_appendStringInfo
 * Format pg_parser_text data under the control of fmt (an sprintf-style format string)
 * and append it to whatever is already in str.  More space is allocated
 * to str if necessary.  This is sort of like a combination of sprintf and
 * strcat.
 */
extern void pg_parser_appendStringInfo(pg_parser_StringInfo str, const char *fmt,...) __attribute__((format(printf, 2, 3)));

/*------------------------
 * pg_parser_appendStringInfoVA
 * Attempt to format pg_parser_text data under the control of fmt (an sprintf-style
 * format string) and append it to whatever is already in str.  If successful
 * return zero; if not (because there's not enough space), return an estimate
 * of the space needed, without modifying str.  Typically the caller should
 * pass the return value to pg_parser_enlargeStringInfo() before trying again; see
 * pg_parser_appendStringInfo for standard usage pattern.
 */
extern int32_t    pg_parser_appendStringInfoVA(pg_parser_StringInfo str, const char *fmt, va_list args) __attribute__((format(printf, 2, 0)));

/*------------------------
 * pg_parser_appendStringInfoString
 * Append a null-terminated string to str.
 * Like pg_parser_appendStringInfo(str, "%s", s) but faster.
 */
extern void pg_parser_appendStringInfoString(pg_parser_StringInfo str, const char *s);

/*------------------------
 * pg_parser_appendStringInfoChar
 * Append a single byte to str.
 * Like pg_parser_appendStringInfo(str, "%c", ch) but much faster.
 */
extern void pg_parser_appendStringInfoChar(pg_parser_StringInfo str, char ch);

/*------------------------
 * pg_parser_appendStringInfoCharMacro
 * As above, but a macro for even more speed where it matters.
 * Caution: str argument will be evaluated multiple times.
 */
#define pg_parser_appendStringInfoCharMacro(str,ch) \
    (((str)->len + 1 >= (str)->maxlen) ? \
     pg_parser_appendStringInfoChar(str, ch) : \
     (void)((str)->data[(str)->len] = (ch), (str)->data[++(str)->len] = '\0'))

/*------------------------
 * pg_parser_appendStringInfoSpaces
 * Append a given number of spaces to str.
 */
extern void pg_parser_appendStringInfoSpaces(pg_parser_StringInfo str, int32_t count);

/*------------------------
 * pg_parser_appendBinaryStringInfo
 * Append arbitrary binary data to a pg_parser_StringInfo, allocating more space
 * if necessary.
 */
extern void pg_parser_appendBinaryStringInfo(pg_parser_StringInfo str,
                                   const char *data, int32_t datalen);

/*------------------------
 * pg_parser_enlargeStringInfo
 * Make sure a pg_parser_StringInfo's buffer can hold at least 'needed' more bytes.
 */
extern void pg_parser_enlargeStringInfo(pg_parser_StringInfo str, int32_t needed);

#endif                            /* PG_PARSER_THIRDPARTY_STRINGINFO_H */
