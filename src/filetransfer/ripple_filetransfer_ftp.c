#include "ripple_app_incl.h"
#include <curl/curl.h>
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "utils/md5/ripple_md5.h"
#include "utils/string/stringinfo.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "queue/ripple_queue.h"
#include "refresh/ripple_refresh_tables.h"
#include "filetransfer/ripple_filetransfer.h"
#include "filetransfer/ripple_filetransfer_ftp.h"
#include "parser/trail/ripple_parsertrail.h"

/* 接收到数据但不处理数据，ftp执行语句时,保证数据正确 */
static uint64 ripple_filetransfer_ftp_discard(void *ptr, uint64 size, uint64 nmemb, void *stream)
{
    /* make gcc happy */
    (void)ptr;
    (void)stream;
    /* 告诉 libcurl 已经"处理"了*/
    return size * nmemb; 
}

/* 返回下一行的指针和长度
 *   input 输入字符串起始位置
 *   line 拆分出来的字符串
 *   返回：下一行的起始位置（NULL 表示没有更多行）
*/
static char* ripple_filetransfer_ftp_nextline(char *input, char *line, uint64 maxlen)
{
    uint64 len = 0;
    char *end = NULL;
    if (!input || !*input)
    {
        return NULL;
    }

    /* 根基‘\n’ 拆分字符串 */
    end = strchr(input, '\n');

    /* 设置拆分出来的长度 */
    if (end) 
    {
        len = end - input;
    }
    else
    {
        len = strlen(input);
    }

    /* 去掉末尾的 '\r' */
    if (len > 0 && input[len - 1] == '\r') 
    {
        len--;
    }

    if (len >= maxlen) 
    {
        len = maxlen - 1;
    }
    /* 保存拆分出来的值 */
    rmemcpy1(line, 0, input, len);
    line[len] = '\0';

    /* 返回值偏过'\n' */
    return end ? end + 1 : NULL;
}

/* 读出文件夹下的所有子文件和文件夹 */
static uint64 ripple_filetransfer_ftp_getlist(void *contents, uint64 size, uint64 nmemb, void *stream)
{
    uint64 realsize = 0;
    char *ptr = NULL;
    ripple_filetransfer_ftp_list *mem = NULL;
    realsize = size * nmemb;

    if (NULL == contents || 0 == realsize || NULL == stream) 
    {
        return 0;
    }
    mem = (ripple_filetransfer_ftp_list *)stream;

    ptr = rmalloc0(mem->size + realsize + 1);
    if (!ptr)
    {
        elog(RLOG_WARNING,"oom");
        return 0;
    }

    if (mem->size > 0)
    {
        rmemcpy0(ptr, 0, mem->memory, mem->size);
    }
    
    rmemcpy0(ptr, mem->size, contents, realsize);
    mem->size += realsize;
    mem->memory = ptr;
    mem->memory[mem->size] = 0;

    return realsize;
}

/* 设置开启ssl加密 */
static void ripple_filetransfer_ftp_ssl_set(CURL *curl, bool ssl)
{
    if (true == ssl)
    {
        curl_easy_setopt(curl, CURLOPT_USE_SSL, CURLUSESSL_ALL);
        curl_easy_setopt(curl, CURLOPT_SSLVERSION, (long)CURL_SSLVERSION_TLSv1_3);
        return;
    }
    return;
}

/* 设置服务器ssl证书验证 */
static void ripple_filetransfer_ftp_verifypeer_set(CURL *curl, bool verifypeer)
{
    if (false == verifypeer)
    {
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
        return;
    }
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
    return;
}

/* 设置服务器主机名验证 */
static void ripple_filetransfer_ftp_verifyhost_set(CURL *curl, bool verifyhost)
{
    if (false == verifyhost)
    {
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
        return;
    }
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
    return;
}

/* 上传到服务器回调函数 */
static uint64 ripple_filetransfer_ftp_read(char *ptr, uint64 size, uint64 nmemb, void *stream)
{
    uint64 nread = 0;
    uint64 retcode = 0;

    retcode = fread(ptr, size, nmemb, stream);

    if(retcode > 0)
    {
        nread = (uint64)retcode;
        elog(RLOG_DEBUG, " ftp upload %lu bytes from file\n", nread);
    }

    return retcode;
}

/* 设置上传到服务器的文件描述符 */
static void ripple_filetransfer_ftp_file_set(CURL *curl, FILE *file)
{
    /* now specify which file to upload */
    curl_easy_setopt(curl, CURLOPT_READDATA, file);
}

/* 设置超时时间 */
static void ripple_filetransfer_ftp_timeout_set(CURL *curl)
{
    /* 连接超时 */
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 30L);
    /* 整体流程超时 */
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 90L);
}

/* 设置上传到服务器的文件大小 */
static void ripple_filetransfer_ftp_filesize_set(CURL *curl, uint64 filesize)
{
    /* now specify which file to upload */
    curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, (curl_off_t)filesize);
}

/* 上传连接信息设置 */
static void ripple_filetransfer_ftp_upload_set(CURL *curl, ripple_filetransfer_ftp* ftransfer_ftp)
{
    StringInfo ftpurl;

    ftpurl = makeStringInfo();

    appendStringInfo(ftpurl, "%s%s", ftransfer_ftp->base.prefixurl, ftransfer_ftp->relativepath);

    //删除
    elog(RLOG_DEBUG, "ftpurl:%s", ftpurl->data);

    curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);

    /* we want to use our own read function */
    curl_easy_setopt(curl, CURLOPT_READFUNCTION, ripple_filetransfer_ftp_read);

    /* 设置url */
    curl_easy_setopt(curl, CURLOPT_URL, ftpurl->data);

    deleteStringInfo(ftpurl);

    return;
}

/* 删除目录连接信息设置 */
static void ripple_filetransfer_ftp_removedir_set(CURL *curl, char* base_url, void *chunk)
{
    elog(RLOG_DEBUG, "ftpurl:%s", base_url);

    curl_easy_setopt(curl, CURLOPT_URL, base_url);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, ripple_filetransfer_ftp_getlist);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, chunk);

    return;
}

/* 下载到本地的回调函数 */
static uint64 ripple_filetransfer_ftp_fwrite(void *buffer, uint64 size, uint64 nmemb, void *stream)
{
    int amount = 0;
    uint64 written = 0;
    ripple_filetransfer_ftp_file *out = (ripple_filetransfer_ftp_file*)stream;
    
    if(-1 == out->fd)
    {
        /* 打开本地文件用于写入数据 */
        out->fd = BasicOpenFile(out->filename, O_RDWR | O_CREAT | RIPPLE_BINARY);
        if (out->fd < 0)
        {
            elog(RLOG_WARNING, "can not open file %s :%s", out->filename, strerror(errno));
            return 0;
        }
    }

    amount = size* nmemb;
    written = FileWrite(out->fd, buffer, amount);

    if(0 != FileSync(out->fd))
    {
        elog(RLOG_WARNING, "could not fsync file %s", out->filename);
        return 0;
    }

    if (written != amount) 
    {
        elog(RLOG_WARNING, "write failed: written=%zu, expected=%zu", written, nmemb);
    }
    return amount;

}

/* 下载连接信息设置 */
static void ripple_filetransfer_ftp_download_set(CURL *curl, ripple_filetransfer_ftp* ftransfer_ftp, ripple_filetransfer_ftp_file* ftp_file)
{
    StringInfo ftpurl;

    ftpurl = makeStringInfo();

    appendStringInfo(ftpurl, "%s%s", ftransfer_ftp->base.prefixurl, ftransfer_ftp->relativepath);

    //删除
    elog(RLOG_DEBUG, "ftpurl:%s", ftpurl->data);
    /* 设置url */
    curl_easy_setopt(curl, CURLOPT_URL, ftpurl->data);

    /* 设置读取函数 */
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, ripple_filetransfer_ftp_fwrite);

    /* 设置 */
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void*)ftp_file);

    deleteStringInfo(ftpurl);

    return;
}

/* 初始化 */
ripple_filetransfer_ftp_file* ripple_filetransfer_ftp_file_init(char* filename)
{
    ripple_filetransfer_ftp_file* ftransfer_ftp = NULL;
    ftransfer_ftp = (ripple_filetransfer_ftp_file*)rmalloc0(sizeof(ripple_filetransfer_ftp_file));
    if(NULL == ftransfer_ftp)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ftransfer_ftp, 0, '\0', sizeof(ripple_filetransfer_ftp_file));

    ftransfer_ftp->filename = rstrdup(filename);
    ftransfer_ftp->fd = -1;
    return ftransfer_ftp;
}

/* 初始化 */
ripple_filetransfer_ftp* ripple_filetransfer_ftp_init(void)
{
    ripple_filetransfer_ftp* ftransfer_ftp = NULL;
    ftransfer_ftp = (ripple_filetransfer_ftp*)rmalloc0(sizeof(ripple_filetransfer_ftp));
    if(NULL == ftransfer_ftp)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ftransfer_ftp, 0, '\0', sizeof(ripple_filetransfer_ftp));

    ftransfer_ftp->ssl = false;
    ftransfer_ftp->verifyhost = false;
    ftransfer_ftp->verifypeer = false;

    ripple_filetransfer_reset((ripple_filetransfer*)ftransfer_ftp);

    return ftransfer_ftp;
}

/* 初始化 */
ripple_filetransfer_ftp_list* ripple_filetransfer_ftp_list_init(void)
{
    ripple_filetransfer_ftp_list* ftp_list = NULL;
    ftp_list = (ripple_filetransfer_ftp_list*)rmalloc0(sizeof(ripple_filetransfer_ftp_list));
    if(NULL == ftp_list)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ftp_list, 0, '\0', sizeof(ripple_filetransfer_ftp_list));

    ftp_list->memory = NULL;
    ftp_list->size = 0;

    return ftp_list;
}

/* 根据类型生成相对路径 */
void ripple_filetransfer_ftp_relativepath_set(ripple_filetransfer_ftp* ftp, ripple_filetransfernode* node)
{
    rmemset1(ftp->relativepath, 0, '\0', MAXPGPATH);
    if(RIPPLE_FILETRANSFERNODE_TYPE_INCREAMENT == node->type)
    {
        ripple_filetransfer_increment* ftransfer_inc = NULL;
        ftransfer_inc = (ripple_filetransfer_increment*)node;
        snprintf(ftp->relativepath, MAXPGPATH, "%s/%s/%016lX", ftp->base.ftpdata,
                                                               ftransfer_inc->prefixpath,
                                                               ftransfer_inc->base.trail);
    }
    else if(RIPPLE_FILETRANSFERNODE_TYPE_REFRESH == node->type)
    {
        ripple_filetransfer_refresh* ftransfer_refresh = NULL;
        ftransfer_refresh = (ripple_filetransfer_refresh*)node;
        snprintf(ftp->relativepath, MAXPGPATH, "%s/%s/%s_%s_%u_%u", ftp->base.ftpdata,
                                                                    ftransfer_refresh->prefixpath,
                                                                    ftransfer_refresh->schema,
                                                                    ftransfer_refresh->table,
                                                                    ftransfer_refresh->shards,
                                                                    ftransfer_refresh->shardnum);
    }
    else if(RIPPLE_FILETRANSFERNODE_TYPE_ONLINEREFRESH_INC == node->type)
    {
        ripple_filetransfer_onlinerefreshinc* ftransfer_inc = NULL;
        ftransfer_inc = (ripple_filetransfer_onlinerefreshinc*)node;
        snprintf(ftp->relativepath, MAXPGPATH, "%s/%s/%016lX", ftp->base.ftpdata,
                                                               ftransfer_inc->prefixpath,
                                                               ftransfer_inc->base.trail);
    }
    else if(RIPPLE_FILETRANSFERNODE_TYPE_ONLINEREFRESH_SHARDING == node->type)
    {
        ripple_filetransfer_refresh* ftransfer_refresh = NULL;
        ftransfer_refresh = (ripple_filetransfer_refresh*)node;
        snprintf(ftp->relativepath, MAXPGPATH, "%s/%s/%s_%s_%u_%u", ftp->base.ftpdata,
                                                                    ftransfer_refresh->prefixpath,
                                                                    ftransfer_refresh->schema,
                                                                    ftransfer_refresh->table,
                                                                    ftransfer_refresh->shards,
                                                                    ftransfer_refresh->shardnum);
    }
    else if(RIPPLE_FILETRANSFERNODE_TYPE_REFRESHSHARDS == node->type)
    {
        ripple_filetransfer_refreshshards* refreshfile = NULL;
        refreshfile = (ripple_filetransfer_refreshshards*)node;
        snprintf(ftp->relativepath, MAXPGPATH, "%s/%s/%s_%s", ftp->base.ftpdata,
                                                              refreshfile->prefixpath,
                                                              refreshfile->schema,
                                                              refreshfile->table);
    }
    else if(RIPPLE_FILETRANSFERNODE_TYPE_ONLINEREFRESHSHARDS == node->type)
    {
        ripple_filetransfer_refreshshards* refreshfile = NULL;
        refreshfile = (ripple_filetransfer_refreshshards*)node;
        snprintf(ftp->relativepath, MAXPGPATH, "%s/%s/%s_%s", ftp->base.ftpdata,
                                                              refreshfile->prefixpath,
                                                              refreshfile->schema,
                                                              refreshfile->table);
    }
    else if(RIPPLE_FILETRANSFERNODE_TYPE_BIGTXN_INC == node->type)
    {
        ripple_filetransfer_bigtxninc* ftransfer_inc = NULL;
        ftransfer_inc = (ripple_filetransfer_bigtxninc*)node;
        snprintf(ftp->relativepath, MAXPGPATH, "%s/%s/%016lX", ftp->base.ftpdata,
                                                               ftransfer_inc->prefixpath,
                                                               ftransfer_inc->base.trail);
    }
    else if(RIPPLE_FILETRANSFERNODE_TYPE_DELETEFILE == node->type)
    {
        ripple_filetransfer_cleanpath* removefile = NULL;
        removefile = (ripple_filetransfer_cleanpath*)node;
        snprintf(ftp->relativepath, MAXPGPATH, "%s/%s/%016lX", ftp->base.ftpdata,
                                                               removefile->prefixpath,
                                                               removefile->base.trail);
    }
    else if(RIPPLE_FILETRANSFERNODE_TYPE_DELETEDIR == node->type)
    {
        ripple_filetransfer_cleanpath* removedir = NULL;
        removedir = (ripple_filetransfer_cleanpath*)node;
        snprintf(ftp->relativepath, MAXPGPATH, "%s/%s", ftp->base.ftpdata,
                                                        removedir->prefixpath);
    }

    //删除
    elog(RLOG_DEBUG, "ftp->relativepath:%s", ftp->relativepath);
    return;
}

/* 初始化 libcurl 全局环境 */
void ripple_filetransfer_ftp_global_init(void)
{
    curl_global_init(CURL_GLOBAL_ALL);
}

/* 清理libcurl 全局环境 */
void ripple_filetransfer_ftp_global_cleanup(void)
{
    curl_global_cleanup();
}


bool ripple_filetransfer_ftp_upload(ripple_filetransfer* filetransfer, char* file)
{
    bool result = true;
    uint64 fsize = 0;
    struct stat st;
    CURL *curl = NULL;
    FILE *fp = NULL;
    CURLcode res = {'\0'};
    ripple_filetransfer_ftp* ftp = NULL;

    ftp = (ripple_filetransfer_ftp*)filetransfer;

    /* 获取本地文件大小 */
    if(stat(file, &st))
    {
        elog(RLOG_WARNING, "file:%s does not exist: %s\n", file, strerror(errno));
        return false;
    }
    fsize = (uint64)st.st_size;

    elog(RLOG_DEBUG,"Local file size: %lu bytes.\n", fsize);

    /* 打开要上传的文件 */
    fp = FileFOpen(file, "rb");
    if(NULL == fp)
    {
        elog(RLOG_WARNING,"Couldn't open file: %s, %s", file, strerror(errno));
        return false;
    }

    /* 初始化CURL句柄 */
    curl = curl_easy_init();
    if(curl)
    {
        ripple_filetransfer_ftp_upload_set(curl, ftp);

        ripple_filetransfer_ftp_ssl_set(curl, ftp->ssl);

        ripple_filetransfer_ftp_verifypeer_set(curl, ftp->verifypeer);

        ripple_filetransfer_ftp_verifyhost_set(curl, ftp->verifyhost);

        /* 创建文件夹 */
        curl_easy_setopt(curl, CURLOPT_FTP_CREATE_MISSING_DIRS, (long)CURLFTP_CREATE_DIR_RETRY);

        curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);

        ripple_filetransfer_ftp_timeout_set(curl);

        ripple_filetransfer_ftp_file_set(curl, fp);

        ripple_filetransfer_ftp_filesize_set(curl, fsize);

        res = curl_easy_perform(curl);

        if(res != CURLE_OK)
        {
            elog(RLOG_WARNING, "ftp upload file failed:%d, %s\n", res, curl_easy_strerror(res));
            result = false;
        }
        /* 释放CURL */
        curl_easy_cleanup(curl);
    }

    if (fp)
    {
        fclose(fp);
    }

    return result;
}

bool ripple_filetransfer_ftp_download(ripple_filetransfer* filetransfer, char* file)
{
    bool result = true;
    CURL *curl;
    CURLcode res;
    ripple_filetransfer_ftp* ftp = NULL;
    ripple_filetransfer_ftp_file* ftp_file = NULL;

    ftp = (ripple_filetransfer_ftp*)filetransfer;

    ftp_file = ripple_filetransfer_ftp_file_init(file);
    elog(RLOG_DEBUG, "filename:%s", ftp_file->filename);

    curl = curl_easy_init();
    if(curl)
    {
        ripple_filetransfer_ftp_download_set(curl, ftp, ftp_file);

        ripple_filetransfer_ftp_ssl_set(curl, ftp->ssl);

        ripple_filetransfer_ftp_verifypeer_set(curl, ftp->verifypeer);

        ripple_filetransfer_ftp_verifyhost_set(curl, ftp->verifyhost);

        /* Switch on full protocol/debug output */
        curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);

        ripple_filetransfer_ftp_timeout_set(curl);

        res = curl_easy_perform(curl);

        if(CURLE_OK != res)
        {
            elog(RLOG_WARNING, "ftp download file failed:%d, %s", res, curl_easy_strerror(res));
            result = false;
        }
        curl_easy_cleanup(curl);
    }

    ripple_filetransfer_ftp_file_free(ftp_file);

    return result;
}

void ripple_filetransfer_ftp_removefile(ripple_filetransfer* filetransfer, char* file)
{
    CURL *curl;
    CURLcode res;
    struct curl_slist *headerlist = NULL;
    char dele_cmd[MAXPGPATH];

    // 构造 DELE 命令
    snprintf(dele_cmd, MAXPGPATH, "DELE %s", file);

    curl = curl_easy_init();
    if (curl) 
    {
        /* FTP URL，直接包含要进入的目录 */
        curl_easy_setopt(curl, CURLOPT_URL, filetransfer->prefixurl);

        /* 开启调试输出 */ 
        curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);

        ripple_filetransfer_ftp_timeout_set(curl);

        /* 添加 DELE 命令 */
        headerlist = curl_slist_append(headerlist, dele_cmd);
        curl_easy_setopt(curl, CURLOPT_QUOTE, headerlist);

        /* 5. 执行 */
        res = curl_easy_perform(curl);
        if (res != CURLE_OK)
        {
            elog(RLOG_WARNING, "ftp download file failed:%d, %s", res, curl_easy_strerror(res));
        }
        curl_slist_free_all(headerlist);
        curl_easy_cleanup(curl);
    }
    return;
}

/* 资源释放 */
void ripple_filetransfer_ftp_free(ripple_filetransfer_ftp* filetransfer_ftp)
{
    if (NULL == filetransfer_ftp)
    {
        return;
    }

    rfree(filetransfer_ftp);
}

/* 资源释放 */
void ripple_filetransfer_ftp_file_free(ripple_filetransfer_ftp_file* ftp_file)
{
    if (NULL == ftp_file)
    {
        return;
    }

    if (ftp_file->filename)
    {
        rfree(ftp_file->filename);
    }

    if (ftp_file->fd)
    {
        FileClose(ftp_file->fd);
    }
    
    rfree(ftp_file);
}

/* 资源释放 */
void ripple_filetransfer_ftp_list_free(ripple_filetransfer_ftp_list* ftp_list)
{
    if(ftp_list->memory)
    {
        rfree(ftp_list->memory);
        ftp_list->memory = NULL;
    }
    ftp_list->size = 0;
}

/* 执行 FTP 命令 */
static CURLcode ripple_filetransfer_ftp_command(CURL *curl, char *url, char *cmd)
{
    CURLcode res = 0;
    struct curl_slist *cmds = NULL;
    cmds = curl_slist_append(cmds, cmd);

    curl_easy_setopt(curl, CURLOPT_QUOTE, cmds);

    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, ripple_filetransfer_ftp_discard);

    res = curl_easy_perform(curl);
    if (res != CURLE_OK)
    {
        elog(RLOG_WARNING, "op :%s failure %s\n", cmd, curl_easy_strerror(res));
    }
    curl_slist_free_all(cmds);
    return res;
}

/* 递归删除文件夹内容，传入根目录不删除 */
static bool ripple_filetransfer_ftp_recursive_delete(CURL *curl, 
                                                     char *base_url,
                                                     char *path,
                                                     ripple_filetransfer_ftp_list* chunk,
                                                     bool is_root)
{
    CURLcode res;
    char *pos = NULL;
    char *name = NULL;
    char *tmpchunk = NULL;
    struct curl_slist *quote = NULL;
    char cmd[512] = {'\0'};
    char line[1024] = {'\0'};

    /* 保证函数和接收数据正确 */
    ripple_filetransfer_ftp_removedir_set(curl, base_url, (void*)chunk);

    /* 进入目录 */
    rmemset1(cmd, 0, '\0', 512);
    snprintf(cmd, sizeof(cmd), "CWD %s", path);
    quote = curl_slist_append(quote, cmd);
    curl_easy_setopt(curl, CURLOPT_QUOTE, quote);

    /* 获取详细目录列表 */
    curl_easy_setopt(curl, CURLOPT_DIRLISTONLY, 0L);
    res = curl_easy_perform(curl);
    if (res != CURLE_OK)
    {
        elog(RLOG_WARNING, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
        ripple_filetransfer_ftp_list_free(chunk);
        curl_slist_free_all(quote);
        return false;
    }

    if (chunk->size > 0)
    {
        tmpchunk = chunk->memory;
        pos = chunk->memory;
        chunk->memory = NULL;
        chunk->size = 0;
    }

    /* 拆分目录列表 */
    rmemset1(line, 0, '\0', 1024);
    while ((pos = ripple_filetransfer_ftp_nextline(pos, line, sizeof(line))) != NULL)
    {
        name = NULL;
        /* 目录--递归 */
        if (line[0] == 'd')
        {
            name = strrchr(line, ' ');
            if (name)
            {
                name++;
            }
            ripple_filetransfer_ftp_recursive_delete(curl, base_url, name, chunk, false);
        }
        else if (line[0] == '-') /* 文件--直接删除 */
        {
            name = strrchr(line, ' ');
            if (name)
            {
                name++;
            }
            rmemset1(cmd, 0, '\0', 512);
            snprintf(cmd, sizeof(cmd), "DELE %s", name);
            res = ripple_filetransfer_ftp_command(curl, base_url, cmd);
            if (res != CURLE_OK)
            {
                if(tmpchunk)
                {
                    rfree(tmpchunk);
                }
                curl_slist_free_all(quote);
                return false;
            }
        }
        rmemset1(line, 0, '\0', sizeof(line));
    }

    /* 返回上级并删除目录 */
    res = ripple_filetransfer_ftp_command(curl, base_url, "CDUP");
    if (res != CURLE_OK)
    {
        if(tmpchunk)
        {
            rfree(tmpchunk);
        }
        curl_slist_free_all(quote);
        return false;
    }

    /* 传入根目录不删除 */
    if (false == is_root)
    {
        rmemset1(cmd, 0, '\0', 512);
        snprintf(cmd, sizeof(cmd), "RMD %s", path);
        res = ripple_filetransfer_ftp_command(curl, base_url, cmd);
        if (res != CURLE_OK)
        {
            if(tmpchunk)
            {
                rfree(tmpchunk);
            }
            curl_slist_free_all(quote);
            return false;
        }
    }

    curl_slist_free_all(quote);
    if(tmpchunk)
    {
        rfree(tmpchunk);
    }

    return true;
}

/* 文件夹内容删除 */
bool ripple_filetransfer_ftp_removedir(ripple_filetransfer* filetransfer, char* dir)
{
    bool result = true;
    CURL *curl;
    ripple_filetransfer_ftp_list* chunk = NULL;

    curl = curl_easy_init();
    if (curl)
    {
        chunk = ripple_filetransfer_ftp_list_init();
        ripple_filetransfer_ftp_removedir_set(curl, filetransfer->prefixurl, (void*)chunk);

        ripple_filetransfer_ftp_timeout_set(curl);

        result = ripple_filetransfer_ftp_recursive_delete(curl, 
                                                          filetransfer->prefixurl,
                                                          dir,
                                                          chunk,
                                                          true);
        curl_easy_cleanup(curl);
        rfree(chunk);
    }

    return result;
}
