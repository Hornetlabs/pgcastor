#include "app_incl.h"
#include "port/file/file_perm.h"
#include "port/file/fd.h"
#include "utils/path/path.h"

/* Modes for creating directories and files in the data directory */
int g_dir_create_mode = DIR_MODE_OWNER;
int g_file_create_mode = FILE_MODE_OWNER;

#define FILE_CLOSED                (-1)

#define FileIsNotOpen(file)        (file == FILE_CLOSED)

#define IS_DIR_SEP(ch)             ((ch) == '/')

#define is_absolute_path(filename) (IS_DIR_SEP((filename)[0]))

/*----------------------------------FILE OPERATIONS-------------------------------------------*/
FILE* osal_allocate_file(const char* name, const char* mode)
{
    FILE* file;
    if ((file = fopen(name, mode)) != NULL)
    {
        return file;
    }

    if (EMFILE == errno || ENFILE == errno)
    {
        elog(RLOG_ERROR, "out of file descriptors");
    }
    else if (ENOENT == errno)
    {
        elog(RLOG_ERROR, "file %s not exist", name);
    }
    else if (ENOTDIR == errno)
    {
        elog(RLOG_ERROR, "%s not dir", name);
    }
    else
    {
        elog(RLOG_ERROR, "open file %s error, %s", name, strerror(errno));
    }

    return NULL;
}

FILE* osal_file_fopen(const char* name, const char* mode)
{
    return fopen(name, mode);
}

size_t osal_file_fwrite(FILE* fp, size_t size, size_t nmemb, const void* content)
{
    return fwrite(content, size, nmemb, fp);
}

char* osal_file_fgets(FILE* fp, int size, char* s)
{
    return fgets(s, size, fp);
}

int osal_free_file(FILE* file)
{
    return fclose(file);
}

/*----------------------------------FILE OPEN BY NAME
 * OPERATIONS-------------------------------------------*/
static int osal_basic_open_file_perm(const char* fileName, int fileFlags, mode_t fileMode)
{
    int fd;

    fd = open(fileName, fileFlags, fileMode);

    if (fd >= 0)
    {
        return fd; /* success! */
    }

    if (errno == EMFILE || errno == ENFILE)
    {
        elog(RLOG_ERROR, "out of file descriptors: %s; release and retry", fileName);
    }

    return -1; /* failure */
}

/*
 * Open a file with osal_basic_open_file_perm() and pass default file mode for the
 * fileMode parameter.
 */
int osal_basic_open_file(const char* fileName, int fileFlags)
{
    return osal_basic_open_file_perm(fileName, fileFlags, g_file_create_mode);
}

static int osal_open_transient_file_perm(const char* fileName, int fileFlags, mode_t fileMode)
{
    int fd;

    fd = osal_basic_open_file_perm(fileName, fileFlags, fileMode);

    if (fd >= 0)
    {
        return fd;
    }

    return -1; /* failure */
}

int osal_open_transient_file(const char* fileName, int fileFlags)
{
    return osal_open_transient_file_perm(fileName, fileFlags, g_file_create_mode);
}

int osal_close_transient_file(int fd)
{
    return close(fd);
}

static File osal_path_name_open_file_perm(const char* fileName, int fileFlags, mode_t fileMode)
{
    char* fnamecopy;
    File  file;

    /*
     * We need a malloc'd copy of the file name; fail cleanly if no room.
     */
    fnamecopy = rstrdup(fileName);
    if (fnamecopy == NULL)
    {
        elog(RLOG_ERROR, "out of memory");
    }

    file = osal_basic_open_file_perm(fileName, fileFlags, fileMode);

    if (0 > file)
    {
        return -1;
    }

    return file;
}

/*
 * Open a file with osal_path_name_open_file_perm() and pass default file mode for the
 * fileMode parameter.
 */
File osal_path_name_open_file(const char* fileName, int fileFlags)
{
    return osal_path_name_open_file_perm(fileName, fileFlags, g_file_create_mode);
}

File osal_file_open(const char* fileName, int oflag, mode_t mode)
{
    return open(fileName, oflag, mode);
}

int osal_file_close(File file)
{
    return close(file);
}

/* Read file */
int osal_file_pread(File file, char* buffer, int amount, off_t offset)
{
    int returnCode;

    if (FileIsNotOpen(file))
    {
        elog(RLOG_ERROR, "file not open");
    }

retry:
    returnCode = pread(file, buffer, amount, offset);

    if (returnCode < 0)
    {
        /* OK to retry if interrupted */
        if (errno == EINTR)
        {
            if (true != g_gotsigterm)
            {
                goto retry;
            }
        }
    }

    return returnCode;
}

int osal_file_read(File file, char* buffer, int amount)
{
    int returnCode;

    if (FileIsNotOpen(file))
    {
        elog(RLOG_ERROR, "file not open");
    }

retry:
    returnCode = read(file, buffer, amount);

    if (returnCode < 0)
    {
        /* OK to retry if interrupted */
        if (errno == EINTR)
        {
            goto retry;
        }
    }

    return returnCode;
}

/* Write file */
int osal_file_pwrite(File file, char* buffer, int amount, off_t offset)
{
    int returnCode;

    if (FileIsNotOpen(file))
    {
        elog(RLOG_ERROR, "file not open");
    }

retry:
    returnCode = pwrite(file, buffer, amount, offset);

    /* if write didn't set errno, assume problem is no disk space */
    if (returnCode != amount && errno == 0)
    {
        errno = ENOSPC;
    }

    if (returnCode < 0)
    {
        /* OK to retry if interrupted */
        if (errno == EINTR)
        {
            goto retry;
        }
    }

    return returnCode;
}

int osal_file_write(File file, char* buffer, int amount)
{
    int returnCode;

    if (FileIsNotOpen(file))
    {
        elog(RLOG_ERROR, "file not open");
    }

retry:
    returnCode = write(file, buffer, amount);

    /* if write didn't set errno, assume problem is no disk space */
    if (returnCode != amount && errno == 0)
    {
        errno = ENOSPC;
    }

    if (returnCode < 0)
    {
        /* OK to retry if interrupted */
        if (errno == EINTR)
        {
            goto retry;
        }
    }

    return returnCode;
}

int osal_file_sync(File file)
{
    if (FileIsNotOpen(file))
    {
        return -1;
    }

    return fsync(file);
}

int osal_file_data_sync(File file)
{
    if (FileIsNotOpen(file))
    {
        return 0;
    }

    return fdatasync(file);
}

/* Get file size */
off_t osal_file_size(File file)
{
    if (FileIsNotOpen(file))
    {
        return (off_t)-1;
    }

    return lseek(file, 0, SEEK_END);
}

/* Truncate file */
int osal_file_truncate(File file, off_t offset)
{
    if (FileIsNotOpen(file))
    {
        elog(RLOG_ERROR, "osal_file_truncate file not open");
    }

    return ftruncate(file, offset);
}

/* Set offset */
off_t osal_file_seek(File file, off_t offset)
{
    if (FileIsNotOpen(file))
    {
        return (off_t)-1;
    }

    return lseek(file, offset, SEEK_SET);
}

/* Check if file exists */
bool osal_file_exist(char* filepath)
{
    int fd = -1;

    /* Open file */
    fd = open(filepath, O_RDONLY | BINARY, 0);
    if (0 > fd)
    {
        return false;
    }
    close(fd);
    return true;
}

/*----------------------------------DIRECTORY
 * OPERATIONS-------------------------------------------*/

/* Open directory */
DIR* osal_open_dir(const char* dirname)
{
    DIR* dir = NULL;

    if ((dir = opendir(dirname)) != NULL)
    {
        return dir;
    }

    if (errno == EMFILE || errno == ENFILE)
    {
        elog(RLOG_ERROR, "out of file descriptors:%s", strerror(errno));
    }

    return NULL;
}

static struct dirent* osal_read_dir_extended(DIR* dir, const char* dirname, int elevel)
{
    struct dirent* dent;

    /* Give a generic message for AllocateDir failure, if caller didn't */
    if (dir == NULL)
    {
        elog(elevel, "could not open directory %s", dirname);
        return NULL;
    }

    errno = 0;
    if ((dent = readdir(dir)) != NULL)
    {
        return dent;
    }

    if (errno)
    {
        elog(elevel, "could not read directory %s", dirname);
    }
    return NULL;
}

struct dirent* osal_read_dir(DIR* dir, const char* dirname)
{
    return osal_read_dir_extended(dir, dirname, RLOG_ERROR);
}

int osal_free_dir(DIR* dir)
{
    /* Nothing to do if AllocateDir failed */
    if (dir == NULL)
    {
        return 0;
    }

    return closedir(dir);
}

int osal_make_dir(char* path)
{
    struct stat sb;
    mode_t      numask, oumask;
    int         last, retval;
    char*       p;

    retval = 0;
    p = path;

    /*
     * POSIX 1003.2: For each dir operand that does not name an existing
     * directory, effects equivalent to those caused by the following command
     * shall occur:
     *
     * mkdir -p -m $(umask -S),u+wx $(dirname dir) && mkdir [-m mode] dir
     *
     * We change the user's umask and then restore it, instead of doing
     * chmod's.  Note we assume umask() can't change errno.
     */
    oumask = umask(0);
    numask = oumask & ~(S_IWUSR | S_IXUSR);
    (void)umask(numask);

    /* Skip leading '/' */
    if (p[0] == '/')
    {
        ++p;
    }

    /* Iterate and create directories */
    for (last = 0; !last; ++p)
    {
        if (p[0] == '\0')
        {
            /* Mark as end */
            last = 1;
        }
        else if (p[0] != '/')
        {
            continue;
        }

        /* Append '\0' after directory */
        *p = '\0';
        if (!last && p[1] == '\0')
        {
            /* Peek one ahead */
            last = 1;
        }

        if (last)
        {
            (void)umask(oumask);
        }

        /* check for pre-existing directory */
        /* Check if directory exists, skip if already exists */
        if (stat(path, &sb) == 0)
        {
            /* Already exists, check if it's a directory */
            if (!S_ISDIR(sb.st_mode))
            {
                if (last)
                {
                    errno = EEXIST;
                }
                else
                {
                    errno = ENOTDIR;
                }
                retval = -1;
                break;
            }
        }
        else if (mkdir(path, last ? g_dir_create_mode : S_IRWXU | S_IRWXG | S_IRWXO) < 0)
        {
            /* If directory creation fails, check error reason - if already exists, continue */
            if (EEXIST != errno)
            {
                retval = -1;
                break;
            }
        }
        if (!last)
        {
            *p = '/';
        }
    }

    /* ensure we restored umask */
    (void)umask(oumask);

    return retval;
}

/*
 * Check if directory exists
 * true  exists
 * false does not exist
 */
bool osal_dir_exist(char* wdata)
{
    DIR* datadir = NULL;
    datadir = osal_open_dir(wdata);
    if (NULL == datadir)
    {
        if (errno == ENOENT)
        {
            return false;
        }
        else
        {
            elog(RLOG_ERROR, "open dir %s error:%s", wdata, strerror(errno));
        }
    }
    osal_free_dir(datadir);
    return true;
}

/*----------------------------------RENAME/UNLINK-------------------------------------------*/
int osal_durable_rename(const char* oldfile, const char* newfile, int elevel)
{
    int fd;

    fd = osal_open_transient_file(newfile, BINARY | O_RDWR);
    if (fd < 0)
    {
        if (errno != ENOENT)
        {
            elog(elevel, "could not open file %s, %s", newfile, strerror(errno));
            return -1;
        }
    }
    else
    {
        osal_file_sync(fd);
        osal_close_transient_file(fd);
    }

    /* Time to do the real deal... */
    if (rename(oldfile, newfile) < 0)
    {
        elog(elevel, "could not rename file %s to %s", oldfile, newfile);
        return -1;
    }

    return 0;
}

int osal_durable_unlink(const char* fname, int elevel)
{
    if (unlink(fname) < 0)
    {
        elog(elevel, "could not remove file %s, %s", fname, strerror(errno));
        return -1;
    }

    return 0;
}

char* osal_make_absolute_path(const char* path)
{
    char* new;

    /* Returning null for null input is convenient for some callers */
    if (path == NULL)
    {
        return NULL;
    }

    if (!is_absolute_path(path))
    {
        char*  buf;
        size_t buflen;

        buflen = MAXPGPATH;
        for (;;)
        {
            buf = rmalloc0(buflen);
            if (!buf)
            {
                elog(RLOG_ERROR, "out of memory");
            }
            if (getcwd(buf, buflen))
            {
                break;
            }
            else if (errno == ERANGE)
            {
                rfree(buf);
                buflen *= 2;
                continue;
            }
            else
            {
                int save_errno = errno;

                rfree(buf);
                errno = save_errno;
                elog(RLOG_ERROR, "could not get current working directory: %m");
            }
        }

        new = rmalloc1(strlen(buf) + strlen(path) + 2);
        if (!new)
        {
            rfree(buf);
            elog(RLOG_ERROR, "out of memory");
        }
        sprintf(new, "%s/%s", buf, path);
        rfree(buf);
    }
    else
    {
        new = rstrdup(path);
        if (!new)
        {
            elog(RLOG_ERROR, "out of memory");
        }
    }

    /* Make sure punctuation is canonical, too */
    path_canonicalize_path(new);

    return new;
}

/* Create file with specified size */
bool osal_create_file_with_size(char* filepath, int fileFlags, uint64_t filesize, uint32_t blksize, uint8* blkdata)
{
    int      fd = -1;
    uint64_t index = 0;
    uint64_t blockcnt = 0;
    char     tpath[1024] = {0};

    /* Create temp file */
    rmemset1(tpath, 0, '\0', 1024);
    snprintf(tpath, 1024, "%s.tmp", filepath);

    fd = osal_basic_open_file(tpath, fileFlags);
    if (0 > fd)
    {
        elog(RLOG_WARNING, "open file %s error:%s", tpath, strerror(errno));
        return false;
    }

    blockcnt = (filesize / (uint64_t)blksize);

    for (index = 0; index < blockcnt; index++)
    {
        if (blksize != osal_file_write(fd, (char*)blkdata, blksize))
        {
            osal_file_close(fd);
            unlink(tpath);
            elog(RLOG_WARNING, "can not write file %s, errno:%s", tpath, strerror(errno));
            return false;
        }
    }

    osal_file_sync(fd);
    osal_file_close(fd);

    /* Rename file */
    osal_durable_rename(tpath, filepath, RLOG_DEBUG);
    return true;
}

/* Check if path is a directory */
static bool osal_is_dir(const char* path)
{
    struct stat statbuf;
    if (lstat(path, &statbuf) == 0)
    {
        return S_ISDIR(statbuf.st_mode) != 0;
    }
    return false;
}

/* Check if path is a regular file */
static bool osal_is_file(const char* path)
{
    struct stat statbuf;
    if (lstat(path, &statbuf) == 0)
    {
        return S_ISREG(statbuf.st_mode) != 0;
    }
    return false;
}

/* Check if path is a special directory */
static bool osal_is_special_dir(const char* path)
{
    return strcmp(path, ".") == 0 || strcmp(path, "..") == 0;
}

/* Generate full file path */
static void osal_get_file_path(const char* path, const char* file_name, char* file_path)
{
    strcpy(file_path, path);
    if (file_path[strlen(path) - 1] != '/')
    {
        strcat(file_path, "/");
    }
    strcat(file_path, file_name);
}

/* Copy file */
bool osal_copy_file(char* srcfile, char* dstfile)
{
    size_t rlen = 0;
    FILE*  srcfd = NULL;
    FILE*  dstfd = NULL;
    char   buf[2048] = {'\0'};

    srcfd = fopen(srcfile, "rb");
    if (NULL == srcfd)
    {
        elog(RLOG_WARNING, "open %s file error, %s", srcfile, strerror(errno));
        return false;
    }

    dstfd = fopen(dstfile, "wb");
    if (NULL == dstfd)
    {
        elog(RLOG_WARNING, "open %s file error, %s", srcfile, strerror(errno));
        osal_free_file(srcfd);
        return false;
    }

    while ((rlen = fread(buf, 1, sizeof(buf), srcfd)) > 0)
    {
        if (fwrite(buf, 1, rlen, dstfd) != rlen)
        {
            osal_free_file(srcfd);
            osal_free_file(dstfd);
            return false;
        }
    }

    osal_free_file(srcfd);
    osal_free_file(dstfd);

    return true;
}

bool osal_remove_dir(const char* path)
{
    DIR*           dir;
    struct dirent* dir_info;
    char           file_path[1024];

    if (osal_is_file(path))
    {
        remove(path);
        return false;
    }

    if (osal_is_dir(path))
    {
        if ((dir = opendir(path)) == NULL)
        {
            return false;
        }

        while ((dir_info = readdir(dir)) != NULL)
        {
            osal_get_file_path(path, dir_info->d_name, file_path);

            if (osal_is_special_dir(dir_info->d_name))
            {
                continue;
            }

            osal_remove_dir(file_path);
        }
        remove(path);
        osal_free_dir(dir);
    }
    return true;
}
