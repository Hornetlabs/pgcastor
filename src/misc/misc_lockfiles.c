#include "app_incl.h"
#include "port/ipc/ipc.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "misc/misc_lockfiles.h"

static char* m_lockfile = NULL;

/* Delete lock file */
void misc_lockfiles_unlink(int status, void* arg)
{
    if (NULL == m_lockfile)
    {
        return;
    }
    unlink(m_lockfile);

    rfree(m_lockfile);
    m_lockfile = NULL;
    elog(RLOG_INFO, "shut down.");
}

/*
 * Create lock file
 */
void misc_lockfiles_create(const char* filename)
{
    int   fd;
    char  buffer[1024 * 2 + 256];
    int   ntries;
    int   len;
    int   encoded_pid;
    pid_t other_pid;
    pid_t my_pid;
    pid_t my_p_pid;
    char* wdata = NULL;

    wdata = guc_getdata();
    my_pid = getpid();
    my_p_pid = getppid();

    /*
     * We need a loop here because of race conditions.  But don't loop forever
     * (for example, a non-writable $PGDATA directory might cause a failure
     * that won't go away).  100 tries seems like plenty.
     */
    for (ntries = 0;; ntries++)
    {
        /*
         * Try to create the lock file --- O_EXCL makes this atomic.
         *
         * Think not to make the file protection weaker than 0600/0640.  See
         * comments below.
         */
        fd = osal_file_open(filename, O_RDWR | O_CREAT | O_EXCL, g_file_create_mode);
        if (fd >= 0)
        {
            break; /* Success; exit the retry loop */
        }

        /*
         * Couldn't create the pid file. Probably it already exists.
         */
        if ((errno != EEXIST && errno != EACCES) || ntries > 100)
        {
            elog(RLOG_ERROR, "could not create lock file:%s", filename);
        }

        /*
         * Read the file to get the old owner's PID.  Note race condition
         * here: file might have been deleted since we tried to create it.
         */
        fd = osal_file_open(filename, O_RDONLY, g_file_create_mode);
        if (fd < 0)
        {
            if (errno == ENOENT)
            {
                continue; /* race condition; try again */
            }

            elog(RLOG_ERROR, "could not open lock file:%s", filename);
        }

        if ((len = osal_file_read(fd, buffer, sizeof(buffer) - 1)) < 0)
        {
            elog(RLOG_ERROR, "could not read lock file:%s", filename);
        }
        close(fd);

        if (len == 0)
        {
            elog(RLOG_ERROR,
                 "lock file %s is empty, Either another ripple is starting, or the lock file is "
                 "the remnant of a previous server startup crash.",
                 filename);
        }

        buffer[len] = '\0';
        encoded_pid = atoi(buffer);

        /* if pid < 0, the pid is for postgres, not postmaster */
        other_pid = (pid_t)(encoded_pid < 0 ? -encoded_pid : encoded_pid);

        if (other_pid <= 0)
        {
            elog(RLOG_ERROR, "bogus data in lock file %s:%s", filename, buffer);
        }

        if (other_pid != my_pid && other_pid != my_p_pid)
        {
            if (kill(other_pid, 0) == 0 || (errno != ESRCH && errno != EPERM))
            {
                elog(RLOG_ERROR,
                     "lock file %s already exists, Is another ripple (PID %d) running in data "
                     "directory:%s",
                     filename,
                     (int)other_pid,
                     wdata);
            }
        }

        /*
         * Looks like nobody's home.  Unlink the file and try again to create
         * it.  Need a loop because of possible race condition against other
         * would-be creators.
         */
        if (unlink(filename) < 0)
        {
            elog(RLOG_ERROR,
                 "could not remove old lock file %s, "
                 "The file seems accidentally left over, but",
                 "it could not be removed. Please remove the file "
                 "by hand and try again.",
                 filename);
        }
    }

    /*
     * Successfully created the file, now fill it.  See comment in pidfile.h
     * about the contents.  Note that we write the same first five lines into
     * both datadir and socket lockfiles; although more stuff may get added to
     * the datadir lockfile later.
     */
    snprintf(buffer, sizeof(buffer), "%d\n%s\n", (int)my_pid, wdata);

    /*
     * In a standalone backend, the next line (LOCK_FILE_LINE_LISTEN_ADDR)
     * will never receive data, so fill it in as empty now.
     */
    errno = 0;
    if (write(fd, buffer, strlen(buffer)) != strlen(buffer))
    {
        int save_errno = errno;

        close(fd);
        unlink(filename);
        /* if write didn't set errno, assume problem is no disk space */
        errno = save_errno ? save_errno : ENOSPC;
        elog(RLOG_ERROR, "could not write lock file %s", filename);
    }

    if (osal_file_sync(fd) != 0)
    {
        int save_errno = errno;

        close(fd);
        unlink(filename);
        errno = save_errno;
        elog(RLOG_ERROR, "could not write lock file %s", filename);
    }

    if (close(fd) != 0)
    {
        int save_errno = errno;

        unlink(filename);
        errno = save_errno;
        elog(RLOG_ERROR, "could not write lock file %s", filename);
    }

    m_lockfile = rstrdup(filename);
}

/* Get pid from lock file */
long misc_lockfiles_getpid(void)
{
    long        pid;
    FILE*       lockf = NULL;
    char*       wdata = NULL;
    struct stat statbuf;

    wdata = guc_getdata();
    /* Change working directory */
    chdir(wdata);
    if (0 != stat(LOCK_FILE, &statbuf))
    {
        if (errno == ENOENT)
        {
            return 0;
        }
        else
        {
            elog(RLOG_ERROR, "could not access file:%s/%s, error:%s", wdata, LOCK_FILE, strerror(errno));
        }
    }

    lockf = osal_file_fopen(LOCK_FILE, "r");
    if (NULL == lockf)
    {
        if (ENOENT == errno)
        {
            return 0;
        }
        else
        {
            elog(RLOG_ERROR, "open lock file %s.%s, error:%s", wdata, LOCK_FILE, strerror(errno));
        }
    }
    if (fscanf(lockf, "%ld", &pid) != 1)
    {
        /* Is the file empty? */
        elog(RLOG_ERROR, "read file error:%s/%s, error:%s", wdata, LOCK_FILE, strerror(errno));
    }

    osal_free_file(lockf);
    return pid;
}
