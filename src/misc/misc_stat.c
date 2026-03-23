#include "app_incl.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "port/thread/thread.h"
#include "misc/misc_stat.h"

/* Load decode.stat file */
void misc_stat_loaddecode(capturebase* decodebase)
{
    int  r;
    int  fd;
    char path[MAXPATH] = {0};

    if (NULL == decodebase)
    {
        elog(RLOG_ERROR, "argv error");
    }

    snprintf(path, MAXPATH, "%s/%s", STAT, STAT_DECODE);
    rmemset1(decodebase, 0, '\0', sizeof(capturebase));

    /* Load data from disk */
    fd = osal_basic_open_file(path, O_RDWR | BINARY);
    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", path);
    }

    /* Read file */
    r = osal_file_read(fd, (char*)decodebase, sizeof(capturebase));
    if (r != sizeof(capturebase))
    {
        if (r < 0)
        {
            elog(RLOG_ERROR, "could not read file %s", path);
        }
        else
        {
            elog(RLOG_ERROR, "could not read file %s : %d of %zu", path, r, sizeof(capturebase));
        }
    }

    osal_file_close(fd);
    return;
}

/*
 * Write decode file to disk
 * Parameters:
 *  decodebase              Data to be written to disk
 *  *pfd                    Both input and output parameter. If not opened before, open and return
 * the opened file descriptor lock                    Whether locking is needed true - need to lock
 *                          false - no need to lock
 *                          This function is currently called in three places: parse transaction
 * log, format transaction trail file to disk
 */
void misc_stat_decodewrite(capturebase* decodebase, int* pfd)
{
    int  fd = -1;
    char path[MAXPATH] = {'\0'};
    char buffer[DECODE_STAT] = {0}; /* need not be aligned */

    if (NULL == decodebase)
    {
        return;
    }

    if (NULL == pfd || -1 == *pfd)
    {
        snprintf(path, MAXPATH, "%s/%s", STAT, STAT_DECODE);
        rmemset1(buffer, 0, 0, DECODE_STAT);
    }
    else
    {
        fd = *pfd;
    }

    rmemcpy1(buffer, 0, decodebase, sizeof(capturebase));
    if (NULL == pfd || -1 == *pfd)
    {
        fd = osal_basic_open_file(path, O_RDWR | O_CREAT | BINARY);
        if (fd < 0)
        {
            elog(RLOG_ERROR, "could not create file %s", path);
        }

        if (NULL != pfd && -1 == *pfd)
        {
            *pfd = fd;
        }
    }

    if (CONTROL_FILE_SIZE != osal_file_pwrite(fd, buffer, CONTROL_FILE_SIZE, 0))
    {
        elog(RLOG_ERROR, "could not write to file %s", path);
    }

    if (0 != osal_file_sync(fd))
    {
        elog(RLOG_ERROR, "could not fsync file %s", path);
    }

    if (NULL == pfd)
    {
        if (osal_file_close(fd))
        {
            elog(RLOG_ERROR, "could not close file %s", path);
        }
    }
}

/* Initialize */
static capturebase* misc_stat_decodeinit(void)
{
    capturebase* decodebase = NULL;

    decodebase = (capturebase*)rmalloc1(sizeof(capturebase));
    if (NULL == decodebase)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset1(decodebase, 0, 0, sizeof(capturebase));

    decodebase->confirmedlsn = InvalidXLogRecPtr;
    decodebase->restartlsn = InvalidXLogRecPtr;
    decodebase->fileid = 0;
    decodebase->fileoffset = 0;

    return decodebase;
}

/* Initialize capture status information */
void misc_capturestat_init(void)
{
    capturebase* decodebase = NULL;

    /* Get replication slot information */
    decodebase = misc_stat_decodeinit();

    misc_stat_decodewrite(decodebase, NULL);
    rfree(decodebase);
}
