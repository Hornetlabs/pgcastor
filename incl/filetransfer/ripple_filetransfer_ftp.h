#ifndef _RIPPLE_FILETRANSFER_FTP_H
#define _RIPPLE_FILETRANSFER_FTP_H

typedef struct RIPPLE_FILETRANSFER_FTP_FILE 
{
    char            *filename;              /* 本地文件名，curl回调函数使用 */
    int             fd;                     /* 打开文件的文件描述符 */
}ripple_filetransfer_ftp_file;

/* 获取目录结构 */
typedef struct RIPPLE_FILETRANSFER_FTP_LIST 
{
    char            *memory;                /* 读取的目录结构 */
    uint64          size;                   /* 内容大小 */
}ripple_filetransfer_ftp_list;


typedef struct RIPPLE_FILETRANSFER_FTP
{
    ripple_filetransfer             base;
    bool                            ssl;                                        /* ssl加密 */
    bool                            verifypeer;                                 /* 服务器ssl证书验证 */
    bool                            verifyhost;                                 /* 服务器主机名验证 */
    char                            relativepath[MAXPGPATH];                    /* 根目录下服务器相对路径（包含文件名） */
}ripple_filetransfer_ftp;

ripple_filetransfer_ftp* ripple_filetransfer_ftp_init(void);

ripple_filetransfer_ftp_file* ripple_filetransfer_ftp_file_init(char* filename);

ripple_filetransfer_ftp_list* ripple_filetransfer_ftp_list_init(void);

bool ripple_filetransfer_ftp_download(ripple_filetransfer* filetransfer, char* file);

bool ripple_filetransfer_ftp_upload(ripple_filetransfer* filetransfer, char* file);

void ripple_filetransfer_ftp_removefile(ripple_filetransfer* filetransfer, char* file);

bool ripple_filetransfer_ftp_removedir(ripple_filetransfer* filetransfer, char* dir);

void ripple_filetransfer_ftp_relativepath_set(ripple_filetransfer_ftp* ftp, ripple_filetransfernode* node);

void ripple_filetransfer_ftp_free(ripple_filetransfer_ftp* filetransfer_ftp);

void ripple_filetransfer_ftp_file_free(ripple_filetransfer_ftp_file* ftp_file);

void ripple_filetransfer_ftp_list_free(ripple_filetransfer_ftp_list* ftp_list);

void ripple_filetransfer_ftp_global_init(void);

void ripple_filetransfer_ftp_global_cleanup(void);



#endif
