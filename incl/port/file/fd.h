#ifndef _FD_H
#define _FD_H

typedef int File;

extern int g_file_create_mode;
extern int g_dir_create_mode;

int osal_durable_rename(const char *oldfile, const char *newfile, int elevel);

int osal_durable_unlink(const char *fname, int elevel);

FILE *
osal_allocate_file(const char *name, const char *mode);

FILE* osal_file_fopen(const char *name, const char *mode);

size_t osal_file_fwrite(FILE* fp, size_t size, size_t nmemb, const void* content);

char* osal_file_fgets(FILE* fp, int size, char* s);

int osal_free_file(FILE *file);

int osal_basic_open_file(const char *fileName, int fileFlags);

int osal_open_transient_file(const char *fileName, int fileFlags);

int osal_close_transient_file(int fd);

File osal_path_name_open_file(const char *fileName, int fileFlags);

File osal_file_open(const char *fileName, int oflag, mode_t mode);

int osal_file_close(File file);

int osal_file_pread(File file, char *buffer, int amount, off_t offset);


int osal_file_read(File file, char *buffer, int amount);

int osal_file_pwrite(File file, char *buffer, int amount, off_t offset);

int osal_file_write(File file, char *buffer, int amount);

int osal_file_sync(File file);

int osal_file_data_sync(File file);

off_t osal_file_size(File file);

int osal_file_truncate(File file, off_t offset);

off_t osal_file_seek(File file, off_t offset);

/* 创建指定大小的文件 */
bool
osal_create_file_with_size(char* filepath, int fileFlags, uint64_t filesize, uint32_t blksize, uint8* blkdata);

/* 文件是否存在检测 */
bool osal_file_exist(char* filepath);

/* 复制文件 */
bool osal_copy_file(char* srcfile, char* dstfile);

/*------------------------DIR 相关-----------------------------------*/
DIR *osal_open_dir(const char *dirname);

struct dirent *osal_read_dir(DIR *dir, const char *dirname);

int osal_free_dir(DIR *dir);

int osal_make_dir(char *path);

bool osal_dir_exist(char* wdata);

char* osal_make_absolute_path(const char *path);

bool osal_remove_dir(const char *path);

#endif
