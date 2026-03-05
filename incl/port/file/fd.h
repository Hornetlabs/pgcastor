#ifndef _RIPPLE_FD_H
#define _RIPPLE_FD_H

typedef int File;

extern int g_file_create_mode;
extern int g_dir_create_mode;

int durable_rename(const char *oldfile, const char *newfile, int elevel);

int durable_unlink(const char *fname, int elevel);

FILE *
AllocateFile(const char *name, const char *mode);

FILE* FileFOpen(const char *name, const char *mode);

size_t FileFWrite(FILE* fp, size_t size, size_t nmemb, const void* content);

char* FileFGets(FILE* fp, int size, char* s);

int FreeFile(FILE *file);

int BasicOpenFile(const char *fileName, int fileFlags);

int OpenTransientFile(const char *fileName, int fileFlags);

int CloseTransientFile(int fd);

File PathNameOpenFile(const char *fileName, int fileFlags);

File FileOpen(const char *fileName, int oflag, mode_t mode);

int FileClose(File file);

int FilePRead(File file, char *buffer, int amount, off_t offset);


int FileRead(File file, char *buffer, int amount);

int FilePWrite(File file, char *buffer, int amount, off_t offset);

int FileWrite(File file, char *buffer, int amount);

int FileSync(File file);

int FileDataSync(File file);

off_t FileSize(File file);

int FileTruncate(File file, off_t offset);

off_t FileSeek(File file, off_t offset);

/* 创建指定大小的文件 */
bool
CreateFileWithSize(char* filepath, int fileFlags, uint64_t filesize, uint32_t blksize, uint8* blkdata);

/* 文件是否存在检测 */
bool FileExist(char* filepath);

/* 复制文件 */
bool CopyFile(char* srcfile, char* dstfile);

/*------------------------DIR 相关-----------------------------------*/
DIR *OpenDir(const char *dirname);

struct dirent *ReadDir(DIR *dir, const char *dirname);

int FreeDir(DIR *dir);

int MakeDir(char *path);

bool DirExist(char* wdata);

char* ripple_make_absolute_path(const char *path);

bool RemoveDir(const char *path);

#endif
