#include "ripple_app_incl.h"
#include "port/file/file_perm.h"
#include "port/file/fd.h"
#include "utils/path/ripple_path.h"

/* Modes for creating directories and files in the data directory */
int g_dir_create_mode = RIPPLE_DIR_MODE_OWNER;
int g_file_create_mode = RIPPLE_FILE_MODE_OWNER;

#define FILE_CLOSED (-1)

#define FileIsNotOpen(file) (file== FILE_CLOSED)

#define IS_DIR_SEP(ch)	((ch) == '/')

#define is_absolute_path(filename) \
( \
	IS_DIR_SEP((filename)[0]) \
)

/*----------------------------------FILE 操作-------------------------------------------*/
FILE *
AllocateFile(const char *name, const char *mode)
{
	FILE	   *file;
	if ((file = fopen(name, mode)) != NULL)
	{
		return file;
	}

	if (EMFILE == errno || ENFILE == errno)
	{
		elog(RLOG_ERROR, "out of file descriptors");
	}
	else if(ENOENT == errno)
	{
		elog(RLOG_ERROR, "file %s not exist", name);
	}
	else if(ENOTDIR == errno)
	{
		elog(RLOG_ERROR, "%s not dir", name);
	}
	else
	{
		elog(RLOG_ERROR, "open file %s error, %s", name, strerror(errno));
	}

	return NULL;
}

FILE* FileFOpen(const char *name, const char *mode)
{
	return fopen(name, mode);
}

size_t FileFWrite(FILE* fp, size_t size, size_t nmemb, const void* content)
{
    return fwrite(content, size, nmemb, fp);
}

char* FileFGets(FILE* fp, int size, char* s)
{
    return fgets(s, size, fp);
}

int
FreeFile(FILE *file)
{
	return fclose(file);
}


/*----------------------------------基于文件名打开文件操作-------------------------------------------*/
static int
BasicOpenFilePerm(const char *fileName, int fileFlags, mode_t fileMode)
{
	int			fd;

	fd = open(fileName, fileFlags, fileMode);

	if (fd >= 0)
		return fd;				/* success! */

	if (errno == EMFILE || errno == ENFILE)
	{
		elog(RLOG_ERROR, "out of file descriptors: %s; release and retry", fileName);
	}

	return -1;					/* failure */
}

/*
 * Open a file with BasicOpenFilePerm() and pass default file mode for the
 * fileMode parameter.
 */
int
BasicOpenFile(const char *fileName, int fileFlags)
{
	return BasicOpenFilePerm(fileName, fileFlags, g_file_create_mode);
}

static int
OpenTransientFilePerm(const char *fileName, int fileFlags, mode_t fileMode)
{
	int			fd;

	fd = BasicOpenFilePerm(fileName, fileFlags, fileMode);

	if (fd >= 0)
	{

		return fd;
	}

	return -1;					/* failure */
}

int
OpenTransientFile(const char *fileName, int fileFlags)
{
	return OpenTransientFilePerm(fileName, fileFlags, g_file_create_mode);
}

int
CloseTransientFile(int fd)
{
	return close(fd);
}


static File
PathNameOpenFilePerm(const char *fileName, int fileFlags, mode_t fileMode)
{
	char	   *fnamecopy;
	File		file;

	/*
	 * We need a malloc'd copy of the file name; fail cleanly if no room.
	 */
	fnamecopy = rstrdup(fileName);
	if (fnamecopy == NULL)
		elog(RLOG_ERROR, "out of memory");

	file = BasicOpenFilePerm(fileName, fileFlags, fileMode);

	if (0 > file)
	{
		return -1;
	}

	return file;
}


/*
 * Open a file with PathNameOpenFilePerm() and pass default file mode for the
 * fileMode parameter.
 */
File
PathNameOpenFile(const char *fileName, int fileFlags)
{
	return PathNameOpenFilePerm(fileName, fileFlags, g_file_create_mode);
}

File
FileOpen(const char *fileName, int oflag, mode_t mode)
{
	return open(fileName, oflag, mode);
}

int FileClose(File file)
{
	return close(file);
}

/* 读文件 */
int
FilePRead(File file, char *buffer, int amount, off_t offset)
{
	int			returnCode;

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
			if(true != g_gotsigterm)
			{
				goto retry;
			}
		}
	}

	return returnCode;
}

int
FileRead(File file, char *buffer, int amount)
{
	int			returnCode;

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
			goto retry;
	}

	return returnCode;
}


/* 写文件 */
int
FilePWrite(File file, char *buffer, int amount, off_t offset)
{
	int			returnCode;

	if (FileIsNotOpen(file))
	{
		elog(RLOG_ERROR, "file not open");
	}

retry:
	returnCode = pwrite(file, buffer, amount, offset);

	/* if write didn't set errno, assume problem is no disk space */
	if (returnCode != amount && errno == 0)
		errno = ENOSPC;

	if (returnCode < 0)
	{
		/* OK to retry if interrupted */
		if (errno == EINTR)
			goto retry;
	}

	return returnCode;
}

int
FileWrite(File file, char *buffer, int amount)
{
	int			returnCode;

	if (FileIsNotOpen(file))
	{
		elog(RLOG_ERROR, "file not open");
	}

retry:
	returnCode = write(file, buffer, amount);

	/* if write didn't set errno, assume problem is no disk space */
	if (returnCode != amount && errno == 0)
		errno = ENOSPC;

	if (returnCode < 0)
	{
		/* OK to retry if interrupted */
		if (errno == EINTR)
			goto retry;
	}

	return returnCode;
}

int
FileSync(File file)
{
	if (FileIsNotOpen(file))
	{
		return -1;
	}

	return fsync(file);
}

int
FileDataSync(File file)
{
	if (FileIsNotOpen(file))
	{
		return 0;
	}

	return fdatasync(file);
}

/* 获取文件大小 */
off_t
FileSize(File file)
{
	if (FileIsNotOpen(file))
	{
		return (off_t) -1;
	}

	return lseek(file, 0, SEEK_END);
}

/* 截断文件 */
int
FileTruncate(File file, off_t offset)
{
	if (FileIsNotOpen(file))
	{
		elog(RLOG_ERROR, "FileTruncate file not open");
	}

	return ftruncate(file, offset);
}

/* 设置偏移 */
off_t FileSeek(File file, off_t offset)
{
	if (FileIsNotOpen(file))
	{
		return (off_t) -1;
	}

	return lseek(file, offset, SEEK_SET);
}

/* 文件是否存在检测 */
bool FileExist(char* filepath)
{
    int fd = -1;

    /* 打开文件 */
    fd = open(filepath, O_RDONLY | RIPPLE_BINARY, 0);
    if (0 > fd)
    {
        return false;
    }
    close(fd);
    return true;
}

/*----------------------------------文件夹相关-------------------------------------------*/

/* 打开文件 */
DIR *
OpenDir(const char *dirname)
{
	DIR *dir = NULL;

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

static struct dirent *
ReadDirExtended(DIR *dir, const char *dirname, int elevel)
{
	struct dirent *dent;

	/* Give a generic message for AllocateDir failure, if caller didn't */
	if (dir == NULL)
	{
		elog(elevel, "could not open directory %s", dirname);
		return NULL;
	}

	errno = 0;
	if ((dent = readdir(dir)) != NULL)
		return dent;

	if (errno)
	{
		elog(elevel, "could not read directory %s", dirname);
	}
	return NULL;
}

struct dirent *
ReadDir(DIR *dir, const char *dirname)
{
	return ReadDirExtended(dir, dirname, RLOG_ERROR);
}


int
FreeDir(DIR *dir)
{
	/* Nothing to do if AllocateDir failed */
	if (dir == NULL)
	{
		return 0;
	}

	return closedir(dir);
}

int
MakeDir(char *path)
{
	struct stat sb;
	mode_t		numask,
				oumask;
	int			last,
				retval;
	char	   *p;

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
	(void) umask(numask);

	/* 跳过首部的 '/' */
	if (p[0] == '/')			/* Skip leading '/'. */
	{
		++p;
	}

	/* 遍历创建目录 */
	for (last = 0; !last; ++p)
	{
		if (p[0] == '\0')
		{
			/* 标识为结束 */
			last = 1;
		}
		else if (p[0] != '/')
		{
			continue;
		}

		/* 在目录的后面添加 ‘\0’ */
		*p = '\0';
		if (!last && p[1] == '\0')
		{
			/* 向前多探一个 */
			last = 1;
		}

		if (last)
		{
			(void) umask(oumask);
		}

		/* check for pre-existing directory */
		/* 查看目录是否存在, 存在则不用创建 */
		if (stat(path, &sb) == 0)
		{
			/* 已经存在,那么判断是否为文件夹 */
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
			/* 创建文件夹出错,那么判断错误的原因，如果是已经存在，那么不做处理，继续后面的创建 */
			if(EEXIST != errno)
			{
				retval = -1;
				break;
			}
		}
		if (!last)
			*p = '/';
	}

	/* ensure we restored umask */
	(void) umask(oumask);

	return retval;
}

/*
 * 查看目录是否存在
 * true  存在
 * false 不存在
*/
bool DirExist(char* wdata)
{
	DIR* datadir = NULL;
	datadir = OpenDir(wdata);
	if(NULL == datadir)
	{
		if(errno == ENOENT)
		{
			return false;
		}
		else
		{
			elog(RLOG_ERROR, "open dir %s error:%s", wdata, strerror(errno));
		}
	}
	FreeDir(datadir);
	return true;
}

/*----------------------------------RENAME/UNLINK-------------------------------------------*/
int
durable_rename(const char *oldfile, const char *newfile, int elevel)
{
	int			fd;

	fd = OpenTransientFile(newfile, RIPPLE_BINARY | O_RDWR);
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
		FileSync(fd);
		CloseTransientFile(fd);
	}

	/* Time to do the real deal... */
	if (rename(oldfile, newfile) < 0)
	{
		elog(elevel,"could not rename file %s to %s", oldfile, newfile);
		return -1;
	}

	return 0;
}

int
durable_unlink(const char *fname, int elevel)
{
	if (unlink(fname) < 0)
	{
		elog(elevel, "could not remove file %s, %s", fname, strerror(errno));
		return -1;
	}

	return 0;
}

char *
ripple_make_absolute_path(const char *path)
{
	char	*new;

	/* Returning null for null input is convenient for some callers */
	if (path == NULL)
		return NULL;

	if (!is_absolute_path(path))
	{
		char		*buf;
		size_t		buflen;

		buflen = MAXPGPATH;
		for (;;)
		{
			buf = rmalloc0(buflen);
			if (!buf)
			{
				elog(RLOG_ERROR,"out of memory");
			}
			if (getcwd(buf, buflen))
				break;
			else if (errno == ERANGE)
			{
				rfree(buf);
				buflen *= 2;
				continue;
			}
			else
			{
				int			save_errno = errno;

				rfree(buf);
				errno = save_errno;
				elog(RLOG_ERROR, "could not get current working directory: %m");
			}
		}

		new = rmalloc1(strlen(buf) + strlen(path) + 2);
		if (!new)
		{
			rfree(buf);
			elog(RLOG_ERROR,"out of memory");

		}
		sprintf(new, "%s/%s", buf, path);
		rfree(buf);
	}
	else
	{
		new = rstrdup(path);
		if (!new)
		{
			elog(RLOG_ERROR,"out of memory");
		}
	}

	/* Make sure punctuation is canonical, too */
	ripple_path_canonicalize_path(new);

	return new;
}

/* 创建指定大小的文件 */
bool
CreateFileWithSize(char* filepath, int fileFlags, uint64_t filesize, uint32_t blksize, uint8* blkdata)
{
    int fd              = -1;
    uint64_t index      = 0;
    uint64_t blockcnt   = 0;
    char tpath[1024]    = { 0 };

    /* 创建临时文件 */
    rmemset1(tpath, 0, '\0', 1024);
    snprintf(tpath, 1024, "%s.tmp", filepath);

    fd = BasicOpenFile(tpath, fileFlags);
    if(0 > fd)
    {
        elog(RLOG_WARNING, "open file %s error:%s", tpath, strerror(errno));
        return false;
    }

    blockcnt = (filesize / (uint64_t)blksize);

    for(index = 0; index < blockcnt; index++)
    {
        if (blksize != FileWrite(fd, (char*)blkdata, blksize))
        {
            FileClose(fd);
            unlink(tpath);
            elog(RLOG_WARNING, "can not write file %s, errno:%s", tpath, strerror(errno));
            return false;
        }
    }

    FileSync(fd); 
    FileClose(fd);
    
    /* 重命名文件 */
    durable_rename(tpath, filepath, RLOG_DEBUG);
    return true;
}

/* 判断是否为目录 */
static bool is_dir(const char *path)
{
	struct stat statbuf;
	if (lstat(path, &statbuf) == 0)
	{
		return S_ISDIR(statbuf.st_mode) != 0;
	}
	return false;
}

/* 判断是否为常规文件 */
static bool is_file(const char *path)
{
	struct stat statbuf;
	if(lstat(path, &statbuf) == 0)
	{
		return S_ISREG(statbuf.st_mode) != 0;//判断文件是否为常规文件
	}
	return false;
}

/* 判断是否是特殊目录 */
static bool is_special_dir(const char *path)
{
	return strcmp(path, ".") == 0 || strcmp(path, "..") == 0;
}

/* 生成完整的文件路径 */
static void get_file_path(const char *path, const char *file_name,  char *file_path)
{
	strcpy(file_path, path);
	if(file_path[strlen(path) - 1] != '/')
		strcat(file_path, "/");
	strcat(file_path, file_name);
}

/* 复制文件 */
bool CopyFile(char* srcfile, char* dstfile)
{
	size_t rlen = 0;
	FILE *srcfd = NULL;
	FILE *dstfd = NULL;
	char buf [2048] = {'\0'};
	
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
		FreeFile(srcfd);
		return false;
	}

	while ((rlen = fread(buf, 1, sizeof(buf), srcfd)) > 0)
	{
		if (fwrite(buf, 1, rlen, dstfd) != rlen)
		{
			FreeFile(srcfd);
			FreeFile(dstfd);
			return false;
		}
	}

	FreeFile(srcfd);
	FreeFile(dstfd);

	return true;
}

bool RemoveDir(const char *path)
{
	DIR *dir;
	struct dirent *dir_info;
	char file_path[1024];

	if(is_file(path))
	{
		remove(path);
		return false;
	}

	if(is_dir(path))
	{
		if((dir = opendir(path)) == NULL)
		{
			return false;
		}

		while((dir_info = readdir(dir)) != NULL)
		{
			get_file_path(path, dir_info->d_name, file_path);

			if(is_special_dir(dir_info->d_name))
			{
				continue;
			}

			RemoveDir(file_path);
		}
		remove(path);
		FreeDir(dir);
	}
	return true;
}
