/*-------------------------------------------------------------------------
 *
 * psprintf.c
 *      sprintf into an allocated-on-demand buffer
 *
 *
 * All Copyright (c) 2024-2024, Byte Sync Development Group
 *
 * Author: liuzihe  Date: 2024/07/10 
 * 
 * src/utils/string/psprintf.c
 *
 *-------------------------------------------------------------------------
 */
#include "ripple_app_incl.h"
#include "utils/string/psprintf.h"

#define MaxAllocSize	((Size) 0x3fffffff) /* 1 gigabyte - 1 */
#define PSPRINTF_MEM_ALLOC rmalloc0
#define PSPRINTF_MEM_FREE rfree

#define ps_unlikely(x) __builtin_expect((x) != 0, 0)

/*
 * psprintf
 *
 * Format text data under the control of fmt (an sprintf-style format string)
 * and return it in an allocated-on-demand buffer.  The buffer is allocated
 * with PSPRINTF_MEM_ALLOC in the backend, or malloc in frontend builds.  Caller is
 * responsible to free the buffer when no longer needed, if appropriate.
 *
 * Errors are not returned to the caller, but are reported via elog(ERROR)
 * in the backend, or printf-to-stderr-and-exit() in frontend builds.
 * One should therefore think twice about using this in libpq.
 */
char *
psprintf(const char *fmt,...)
{
	int			save_errno = errno;
	size_t		len = 128;		/* initial assumption about buffer size */

	for (;;)
	{
		char	   *result;
		va_list		args;
		size_t		newlen;

		/*
		 * Allocate result buffer.  Note that in frontend this maps to malloc
		 * with exit-on-error.
		 */
		result = (char *) PSPRINTF_MEM_ALLOC(len);

		/* Try to format the data. */
		errno = save_errno;
		va_start(args, fmt);
		newlen = pvsnprintf(result, len, fmt, args);
		va_end(args);

		if (newlen < len)
			return result;		/* success */

		/* Release buffer and loop around to try again with larger len. */
		PSPRINTF_MEM_FREE(result);
		len = newlen;
	}
}

/*
 * pvsnprintf
 *
 * Attempt to format text data under the control of fmt (an sprintf-style
 * format string) and insert it into buf (which has length len).
 *
 * If successful, return the number of bytes emitted, not counting the
 * trailing zero byte.  This will always be strictly less than len.
 *
 * If there's not enough space in buf, return an estimate of the buffer size
 * needed to succeed (this *must* be more than the given len, else callers
 * might loop infinitely).
 *
 * Other error cases do not return, but exit via elog(ERROR) or exit().
 * Hence, this shouldn't be used inside libpq.
 *
 * Caution: callers must be sure to preserve their entry-time errno
 * when looping, in case the fmt contains "%m".
 *
 * Note that the semantics of the return value are not exactly C99's.
 * First, we don't promise that the estimated buffer size is exactly right;
 * callers must be prepared to loop multiple times to get the right size.
 * (Given a C99-compliant vsnprintf, that won't happen, but it is rumored
 * that some implementations don't always return the same value ...)
 * Second, we return the recommended buffer size, not one less than that;
 * this lets overflow concerns be handled here rather than in the callers.
 */
size_t
pvsnprintf(char *buf, size_t len, const char *fmt, va_list args)
{
	int			nprinted;

	nprinted = vsnprintf(buf, len, fmt, args);

	/* We assume failure means the fmt is bogus, hence hard failure is OK */
	if (ps_unlikely(nprinted < 0))
	{
		elog(RLOG_ERROR, "pvsnprintf failure");
	}

	if ((size_t) nprinted < len)
	{
		/* Success.  Note nprinted does not include trailing null. */
		return (size_t) nprinted;
	}

	/*
	 * We assume a C99-compliant vsnprintf, so believe its estimate of the
	 * required space, and add one for the trailing null.  (If it's wrong, the
	 * logic will still work, but we may loop multiple times.)
	 *
	 * Choke if the required space would exceed MaxAllocSize.  Note we use
	 * this PSPRINTF_MEM_ALLOC-oriented overflow limit even when in frontend.
	 */
	if (ps_unlikely((size_t) nprinted > MaxAllocSize - 1))
	{
		elog(RLOG_ERROR, "out of memory");
	}

	return nprinted + 1;
}
