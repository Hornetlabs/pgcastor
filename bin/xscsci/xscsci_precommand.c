#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <unistd.h>
#include <sys/stat.h>
#include <time.h>
#include <fcntl.h>
#include <dirent.h>

#include "app_c.h"
#include "xsynch_exbufferdata.h"
#include "xsynch_fe.h"
#include "xscsci_output.h"
#include "xscsci.h"
#include "xscsci_precommand.h"

typedef void (*precommandfunc)(xsciscistat* xscisc, xsynch_cmd* cmd);

typedef struct XSCSCI_PRECOMMANDOPS
{
    xsynch_cmdtag                   type;
    char*                           desc;
    precommandfunc                  func;
} xscsci_precommandops;

/* 根据模板文件创建xmanager配置文件 */
static bool xscsci_precommand_createconf_fromsample(const char *src, const char *dst)
{
    size_t rlen = 0;
    char buf [8192] = {'\0'};
    FILE *fd = NULL;
    FILE *fs = NULL;

    fs = fopen(src, "rb");
    if (!fs) 
    {
        return false;
    }

    fd = fopen(dst, "wb");
    if (!fd) 
    {
        fclose(fs);
        return false;
    }

    while ((rlen = fread(buf, 1, sizeof(buf), fs)) > 0)
    {
        if (fwrite(buf, 1, rlen, fd) != rlen)
        {
            fclose(fs);
            fclose(fd);
            return false;
        }
    }

    fclose(fs);
    fclose(fd);
    return true;
}

/* 删除文件夹 */
static bool xscsci_precommand_removedir(const char *path)
{
    DIR *dir;
    struct stat statbuf;
    struct dirent *dir_info;
    char file_path[1024];

    if (0 != stat(path, &statbuf))
    {
        return false;
    }

    if(S_ISREG(statbuf.st_mode))
    {
        remove(path);
        return true;
    }

    if(S_ISDIR(statbuf.st_mode))
    {
        if((dir = opendir(path)) == NULL)
        {
            return false;
        }

        while((dir_info = readdir(dir)) != NULL)
        {
            strcpy(file_path, path);
            if(file_path[strlen(path) - 1] != '/')
            {
                strcat(file_path, "/");
            }
            strcat(file_path, dir_info->d_name);

            if(strcmp(dir_info->d_name, ".") == 0 
               || strcmp(dir_info->d_name, "..") == 0)
            {
                continue;
            }

            xscsci_precommand_removedir(file_path);
        }
        remove(path);
        closedir(dir);
    }
    return true;
}

/* 从配置文件中获取key--valve */
static bool xscsci_precommand_getdatafromcfgfile(const char *config_file, char* key, char* data)
{
    FILE *fp = NULL;
    char fline[1024];

    fp = fopen(config_file, "r");
    if (!fp)
    {
        printf("could not open configuration file:%s %s\n", config_file, strerror(errno));
        return false;
    }

    /* 读取一行数据 */
    memset(fline, '\0', sizeof(fline));
    while (fgets(fline, sizeof(fline), fp) != NULL)
    {
        bool quota = false;
        char* uptr = fline;
        int pos = 0;
        int len = 0;

        /* 跳过 开头的 空字符等信息 */
        while('\0' != *uptr)
        {
            if(' ' != *uptr
                && '\t' != *uptr
                && '\r' != *uptr
                && '\n' != *uptr)
            {
                break;
            }
            uptr++;
            pos++;
        }

        /* 跳过空行和注释行 */
        if('\0' == *uptr || '#' == *uptr)
        {
            memset(fline, '\0', sizeof(fline));
            continue;
        }

        /* 获取key */
        while('\0' != *uptr)
        {
            if(' ' == *uptr
                || '\t' == *uptr
                || '\r' == *uptr
                || '\n' == *uptr
                || '=' == *uptr)
            {
                break;
            }
            len++;
            uptr++;
        }

        /* 获取名称 */
        if ( len != strlen(key)
            || 0 != memcmp(key, fline + pos, len))
        {
            memset(fline, '\0', sizeof(fline));
            continue;
        }
        pos += len;

        /* 获取 value */
        len = 0;

        /* 跳过 SPACE TABE 和 换行 */
        while('\0'!= *uptr)
        {
            if(' ' != *uptr
                && '\t' != *uptr
                && '\r' != *uptr
                && '\n' != *uptr)
            {
                break;
            }
            pos++;
            uptr++;
        }

        //table 获取;
        if('=' != *uptr)
        {
            /* 结束了 */
            printf("config data error\n");
            return false;
        }

        /* 获取 value 值 */
        /* 跳过 '=' 字符 */
        pos++;
        uptr++;

        /* 跳过 空格 等字符 */
        while('\0' != *uptr)
        {
            if(' ' != *uptr
                && '\t' != *uptr
                && '\r' != *uptr
                && '\n' != *uptr)
            {
                break;
            }
            uptr++;
            pos++;
        }

        /* 跳过空行和注释行 */
        if('\0' == *uptr || '#' == *uptr)
        {
            /* 结束了 */
            printf("config data error\n");
            return false;
        }

        /* 跳过 空格 等字符 */
        /* 查看字符类型 */
        if(*uptr == '"')
        {
            uptr++;
            pos++;
            quota = true;
            /* 获取去下一个 " 字符 */
            while('\0' != *uptr)
            {
                if('"' == *uptr)
                {
                    quota = false;
                    break;
                }
                len++;
                uptr++;
            }

            if(true == quota)
            {
                 printf("configuration data is incorrect, missing double quotation marks\n");
                 return false;
            }
        }
        else
        {
            while('\0'!= *uptr)
            {
                if(' ' == *uptr
                    || '\t' == *uptr
                    || '\r' == *uptr
                    || '\n' == *uptr)
                {
                    break;
                }
                len++;
                uptr++;
            }
        }

        len += 1;
        memset(data, 0, len);
        memcpy(data, fline + pos, len - 1);
        break;
    }
    return true;
}

/* deit读取配置文件生成xsynch_cfgfilecmd->data */
static bool  xscsci_precommand_edit_setcmddata(FILE *fp, xsynch_cfgfilecmd* cfgcmd)
{
    size_t rlen         = 0;
    size_t offset       = 0;
    size_t filesize     = 0;
    char buf [8192]     = {'\0'};

    if (!fp || !cfgcmd)
    {
        return false;
    }

    if(0 != fseek(fp, 0, SEEK_END))
    {
        return false;
    }

    filesize = ftell(fp);

    if (filesize <= 0)
    {
        cfgcmd->data = NULL;
        return false;
    }

    if(0 != fseek(fp, 0, SEEK_SET))
    {
        return false;
    }

    cfgcmd->datalen = filesize;

    cfgcmd->data = malloc(filesize);
    memset(cfgcmd->data, '\0', filesize);

    while ((rlen = fread(buf, 1, sizeof(buf), fp)) > 0)
    {
        memcpy(cfgcmd->data + offset, buf, rlen);
        memset(buf, 0, sizeof(buf));
        offset += rlen;
    }

    return true;
}

static void xscsci_precommand_create(xsciscistat* xscisc, xsynch_cmd* cmd)
{
    struct stat st;
    int rownum                      = 0;
    char samplepath[512]            = {'\0'};
    char confpath[512]              = {'\0'};
    xsynchrow* rows                 = NULL;
    xsynch_createcmd* createcmd     = NULL;

    if (cmd == NULL)
    {
        return;
    }

    createcmd = (xsynch_createcmd*)cmd;

    if (XSYNCH_JOBKIND_MANAGER == createcmd->kind)
    {
        /* 根据模板文件创建xmanager配置文件 */
        snprintf(confpath, sizeof(confpath), "%s/%s/%s_%s.cfg",
                                              xscisc->xsynchhome,
                                              "config",
                                              "manager",
                                              createcmd->name);
        if(0 == stat(confpath, &st))
        {
            printf("xmanager %s already exists\n", createcmd->name);
            return ;
        }

        snprintf(samplepath, sizeof(samplepath), "%s/%s/%s/%s", xscisc->xsynchhome, "config", "sample", "manager.cfg.sample");

        if (false == xscsci_precommand_createconf_fromsample(samplepath, confpath))
        {
            printf("Create xmanager:%s configuration file from template file error \n", createcmd->name);
            return ;
        }

        printf("Create xmanager %s\n", createcmd->name);

        return ;
    }

    if (false == XSynchSendCmd(xscisc->conn, cmd))
    {
        printf("precommand send cmd failed %d\n", cmd->type);
        return;
    }

    XSynchGetResult(xscisc->conn, &rownum, &rows);

    xscsci_output(rownum, rows);

    return;

}

static void xscsci_precommand_alter(xsciscistat* xscisc, xsynch_cmd* cmd)
{
    int rownum                  = 0;
    xsynchrow* rows             = NULL;
    xsynch_altercmd* altercmd   = NULL;

    if (cmd == NULL)
    {
        return;
    }

    altercmd = (xsynch_altercmd*)cmd;

    if (XSYNCH_JOBKIND_PROCESS != altercmd->kind)
    {
        printf("Alter only supports progress\n");
        return;
    }

    if (NULL == altercmd->job)
    {
        printf("Alter not find valid progress job\n");
        return;
    }

    if (false == XSynchSendCmd(xscisc->conn, cmd))
    {
        printf("precommand send cmd failed %d\n", cmd->type);
        return;
    }

    XSynchGetResult(xscisc->conn, &rownum, &rows);

    xscsci_output(rownum, rows);

    return;

}

static void xscsci_precommand_remove(xsciscistat* xscisc, xsynch_cmd* cmd)
{
    int rownum                      = 0;
    char confpath[512]              = {'\0'};
    xsynchrow* rows                 = NULL;
    xsynch_removecmd* removecmd     = NULL;

    if (cmd == NULL)
    {
        return;
    }

    removecmd = (xsynch_removecmd*)cmd;

    if (XSYNCH_JOBKIND_PROCESS == removecmd->kind
        || XSYNCH_JOBKIND_ALL == removecmd->kind)
    {
        printf("Remove not supports all or progress\n");
        return;
    }

    if (XSYNCH_JOBKIND_MANAGER == removecmd->kind)
    {
        /* 删除配置文件 */
        snprintf(confpath, sizeof(confpath), "%s/%s/%s_%s.cfg", 
                                              xscisc->xsynchhome, 
                                              "config",
                                              "manager",
                                              removecmd->name);
        if (0 == unlink(confpath))
        {
            printf("Remove xmanager %s\n", removecmd->name);
        }
        else
        {
             printf(" %s remove failed: %s\n", removecmd->name, strerror(errno));
        }
        return;
    }

    if (false == XSynchSendCmd(xscisc->conn, cmd))
    {
        printf("precommand send cmd failed %d\n", cmd->type);
        return;
    }

    XSynchGetResult(xscisc->conn, &rownum, &rows);

    xscsci_output(rownum, rows);

    return;

}

static void xscsci_precommand_reload(xsciscistat* xscisc, xsynch_cmd* cmd)
{
    int rownum                      = 0;
    xsynchrow* rows                 = NULL;
    xsynch_reloadcmd* reloadcmd     = NULL;

    if (cmd == NULL)
    {
        return;
    }

    reloadcmd = (xsynch_reloadcmd*)cmd;

    if (XSYNCH_JOBKIND_PROCESS == reloadcmd->kind
        || XSYNCH_JOBKIND_ALL == reloadcmd->kind)
    {
        printf("Reload not supports all or progress\n");
        return;
    }

    if (false == XSynchSendCmd(xscisc->conn, cmd))
    {
        return;
    }

    XSynchGetResult(xscisc->conn, &rownum, &rows);

    xscsci_output(rownum, rows);

    return;

}

static void xscsci_precommand_drop(xsciscistat* xscisc, xsynch_cmd* cmd)
{
    int rownum                  = 0;
    xsynchrow* rows             = NULL;
    xsynch_dropcmd* dropcmd     = NULL;
    char data[512]              = {'\0'};
    char confpath[512]          = {'\0'};

    if (cmd == NULL)
    {
        return;
    }

    dropcmd = (xsynch_dropcmd*)cmd;

    if (XSYNCH_JOBKIND_ALL == dropcmd->kind)
    {
        printf("Drop not supports all or progress\n");
        return;
    }

    if (XSYNCH_JOBKIND_MANAGER == dropcmd->kind)
    {
        /* 删除data目录 */
        snprintf(confpath, sizeof(confpath), "%s/%s/%s_%s.cfg", 
                                              xscisc->xsynchhome, 
                                              "config",
                                              "manager",
                                              dropcmd->name);
        xscsci_precommand_getdatafromcfgfile(confpath, "data", data);

        if (false == xscsci_precommand_removedir(data))
        {
            printf("Drop xmanager %s data failed\n", dropcmd->name);
            return;
        }

        /* 删除配置文件 */
        if (0 == unlink(confpath))
        {
            printf("Drop xmanager %s\n", dropcmd->name);
        }
        else
        {
             printf(" Drop xmanager %s configure failed: %s\n", dropcmd->name, strerror(errno));
        }
        return;
    }

    if (false == XSynchSendCmd(xscisc->conn, cmd))
    {
        printf("precommand send cmd failed %d\n", cmd->type);
        return;
    }

    XSynchGetResult(xscisc->conn, &rownum, &rows);

    xscsci_output(rownum, rows);

    return;

}

static void xscsci_precommand_info(xsciscistat* xscisc, xsynch_cmd* cmd)
{
    int rownum                  = 0;
    xsynchrow* rows             = NULL;

    if (cmd == NULL)
    {
        return;
    }

    if (false == XSynchPing(xscisc->conn))
    {
        printf("xmanager not running\n");
        return;
    }

    if (false == XSynchSendCmd(xscisc->conn, cmd))
    {
        printf("precommand send cmd failed %d\n", cmd->type);
        return;
    }

    XSynchGetResult(xscisc->conn, &rownum, &rows);

    xscsci_output(rownum, rows);

    return;
}

/* refresh 命令 */
static void xscsci_precommand_refresh(xsciscistat* xscisc, xsynch_cmd* cmd)
{
    int rownum                  = 0;
    xsynchrow* rows             = NULL;

    if (false == XSynchSendCmd(xscisc->conn, cmd))
    {
        printf("precommand send cmd failed %d\n", cmd->type);
        return;
    }

    XSynchGetResult(xscisc->conn, &rownum, &rows);

    xscsci_output(rownum, rows);
    return;
}

/* list 命令 */
static void xscsci_precommand_list(xsciscistat* xscisc, xsynch_cmd* cmd)
{
    int rownum                  = 0;
    xsynchrow* rows             = NULL;

    if (false == XSynchSendCmd(xscisc->conn, cmd))
    {
        printf("precommand send cmd failed %d\n", cmd->type);
        return;
    }

    XSynchGetResult(xscisc->conn, &rownum, &rows);

    xscsci_output(rownum, rows);
    return;
}

static void xscsci_precommand_watch(xsciscistat* xscisc, xsynch_cmd* cmd)
{
    int rownum                  = 0;
    xsynchrow* rows             = NULL;
    xsynch_watchcmd* watch      = NULL;

    if (cmd == NULL)
    {
        return;
    }

    watch = (xsynch_watchcmd*)cmd;

    if (false == XSynchPing(xscisc->conn))
    {
        printf("xmanager not running\n");
        return;
    }

    while (true)
    {
        if (false == XSynchSendCmd(xscisc->conn, cmd))
        {
            printf("precommand send cmd failed %d\n", cmd->type);
            return;
        }

        XSynchGetResult(xscisc->conn, &rownum, &rows);

        xscsci_output(rownum, rows);
        rows = NULL;
        rownum = 0;

        sleep(watch->interval);
    }

    return;
}

static void xscsci_precommand_edit(xsciscistat* xscisc, xsynch_cmd* cmd)
{
    time_t now;
    int fd                      = -1;
    int rownum                  = 0;
    FILE *fp                    = NULL;
    struct tm *pcnow            = NULL;
    xsynchrow* rows             = NULL;
    xsynch_editcmd* editcmd     = NULL;
    xsynch_cfgfilecmd cfgcmd    = {{'\0'}};
    char cfgpath[512]           = {'\0'};
    char filename[512]          = {'\0'};
    char confpath[512]          = {'\0'};
    char syscmd[1024]           = {'\0'};

    if (cmd == NULL)
    {
        return;
    }

    editcmd = (xsynch_editcmd*)cmd;

    if (XSYNCH_JOBKIND_PROCESS == editcmd->kind
        || XSYNCH_JOBKIND_ALL == editcmd->kind)
    {
        printf("Edit not supports all or progress\n");
        return;
    }

    if (XSYNCH_JOBKIND_MANAGER == editcmd->kind)
    {
        snprintf(confpath, sizeof(confpath), "%s/%s/%s_%s.cfg", 
                                              xscisc->xsynchhome,
                                              "config",
                                              "manager",
                                              editcmd->name);
        sprintf(syscmd, "vim %s", confpath);
        system(syscmd);
        printf("Edit manager %s\n", editcmd->name);
        return;
    }

    if (false == XSynchSendCmd(xscisc->conn, cmd))
    {
        printf("precommand send cmd failed %d\n", cmd->type);
        return;
    }

    XSynchGetResult(xscisc->conn, &rownum, &rows);

    /* 出现错误 */
    if (0 == rownum)
    {
        printf("Edit %s get conf file failed\n", editcmd->name);
        return;
    }

    time(&now);
    pcnow = localtime(&now);
    if(pcnow == NULL)
    {
        printf("Edit %s get time failed\n", editcmd->name);
        return;
    }

    snprintf(cfgpath, 512, "%s/%s/.%s_%d-%d-%d-%02d%02d%02d.conf",
                                                xscisc->xsynchhome,
                                                "config",
                                                editcmd->name,
                                                pcnow->tm_year + 1900,
                                                pcnow->tm_mon + 1,
                                                pcnow->tm_mday,
                                                pcnow->tm_hour,
                                                pcnow->tm_min,
                                                pcnow->tm_sec);
    snprintf(filename, 512, "%s_%d-%d-%d-%02d%02d%02d.conf",
                                                editcmd->name,
                                                pcnow->tm_year + 1900,
                                                pcnow->tm_mon + 1,
                                                pcnow->tm_mday,
                                                pcnow->tm_hour,
                                                pcnow->tm_min,
                                                pcnow->tm_sec);
    fd = open(cfgpath, O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd < 0)
    {
        printf("Edit %s open file failed %s:%s\n", editcmd->name, cfgpath, strerror(errno));
        return;
    }
    close(fd);

    fp = fopen(cfgpath, "wb");
    if (!fp) 
    {
        printf("Edit %s open file failed %s\n", editcmd->name, strerror(errno));
        return ;
    }

    if (fwrite(rows->columns->value, 1, rows->columns->valuelen, fp) != (size_t)rows->columns->valuelen)
    {
        return;
    }

    fclose(fp);
    memset(syscmd, '\0', 1024);
    sprintf(syscmd, "vim %s", cfgpath);
    system(syscmd);

    fp = fopen(cfgpath, "rb");
    if (!fp) 
    {
        printf("Edit %s open file failed %s\n", editcmd->name, strerror(errno));
        return ;
    }

    cfgcmd.type.type = T_XSYNCH_CFGfILECMD;
    cfgcmd.kind = editcmd->kind;
    cfgcmd.name = editcmd->name;
    cfgcmd.filename = filename;

    if (false == xscsci_precommand_edit_setcmddata(fp, &cfgcmd))
    {
        printf("edit  %s setcmddata failed %s\n", editcmd->name, strerror(errno));
        fclose(fp);
        free(cfgcmd.data);
        return;
    }
    fclose(fp);

    if (false == XSynchSendCmd(xscisc->conn, &cfgcmd.type))
    {
        printf("precommand send cmd failed %d\n", cmd->type);
        return;
    }

    free(cfgcmd.data);
    unlink(cfgpath);

    printf("Edit %s success \n", editcmd->name);

    return;

}

static void xscsci_precommand_init(xsciscistat* xscisc, xsynch_cmd* cmd)
{
    int rownum = 0;
    xsynchrow* rows = NULL;
    xsynch_initcmd* initcmd = NULL;
    char cwd[512]           = {'\0'};
    char syscmd[1024]       = {'\0'};

    if (cmd == NULL)
    {
        return;
    }

    initcmd = (xsynch_initcmd*)cmd;

    if (XSYNCH_JOBKIND_PROCESS == initcmd->kind
        || XSYNCH_JOBKIND_ALL == initcmd->kind)
    {
        printf("Init not supports all or progress\n");
        return;
    }

    if (XSYNCH_JOBKIND_MANAGER == initcmd->kind)
    {
        /* system 初始化xmanager */

        if (getcwd(cwd, sizeof(cwd)) == NULL)
        {
            printf("Init xmanager %s getcwd failed \n", initcmd->name);
            return;
        }
        snprintf(syscmd, sizeof(syscmd), "%s/xmanager -f  %s/%s/%s_%s.cfg init", 
                                          cwd,
                                          xscisc->xsynchhome,
                                          "config",
                                          "manager",
                                          initcmd->name);
        if (0 == system(syscmd))
        {
            printf("Init xmanager %s\n", initcmd->name);
        }
        return;
    }

    if (false == XSynchSendCmd(xscisc->conn, cmd))
    {
        printf("precommand send cmd failed %d\n", cmd->type);
        return;
    }

    XSynchGetResult(xscisc->conn, &rownum, &rows);

    xscsci_output(rownum, rows);

    return;

}

static void xscsci_precommand_start(xsciscistat* xscisc, xsynch_cmd* cmd)
{
    int rownum                  = 0;
    xsynchrow* rows             = NULL;
    xsynch_startcmd* startcmd   = NULL;
    char cwd[512]               = {'\0'};
    char syscmd[1024]           = {'\0'};

    if (cmd == NULL)
    {
        return;
    }

    startcmd = (xsynch_startcmd*)cmd;

    if (XSYNCH_JOBKIND_PROCESS == startcmd->kind)
    {
        printf("Start not supports progress\n");
        return;
    }

    if (XSYNCH_JOBKIND_MANAGER == startcmd->kind)
    {
        /* system 启动xmanager */
        if (getcwd(cwd, sizeof(cwd)) == NULL)
        {
            printf("start xmanager %s getcwd failed \n", startcmd->name);
            return;
        }
        snprintf(syscmd, sizeof(syscmd), "%s/xmanager -f  %s/%s/%s_%s.cfg start", 
                                          cwd,
                                          xscisc->xsynchhome,
                                          "config",
                                          "manager",
                                          startcmd->name);
        if (0 == system(syscmd))
        {
            printf("start xmanager %s\n", startcmd->name);
        }
        return;
    }
    else if (XSYNCH_JOBKIND_ALL == startcmd->kind)
    {
        if (getcwd(cwd, sizeof(cwd)) == NULL)
        {
            printf("start xmanager %s getcwd failed \n", startcmd->name);
            return;
        }
        snprintf(syscmd, sizeof(syscmd), "%s/xmanager -f  %s/%s/%s_%s.cfg start", 
                                          cwd,
                                          xscisc->xsynchhome,
                                          "config",
                                          "manager",
                                          startcmd->name);
        if (0 == system(syscmd))
        {
            printf("start xmanager %s\n", startcmd->name);
        }
    }

    if (false == XSynchSendCmd(xscisc->conn, cmd))
    {
        printf("precommand send cmd failed %d\n", cmd->type);
        return;
    }

    XSynchGetResult(xscisc->conn, &rownum, &rows);

    xscsci_output(rownum, rows);

    return;

}

static void xscsci_precommand_stop(xsciscistat* xscisc, xsynch_cmd* cmd)
{
    int rownum                  = 0;
    xsynchrow* rows             = NULL;
    xsynch_stopcmd* stopcmd     = NULL;

    if (cmd == NULL)
    {
        return;
    }

    stopcmd = (xsynch_stopcmd*)cmd;

    if (XSYNCH_JOBKIND_PROCESS == stopcmd->kind)
    {
        printf("Stop not supports progress\n");
        return;
    }

    if (false == XSynchSendCmd(xscisc->conn, cmd))
    {
        printf("precommand send cmd failed %d\n", cmd->type);
        return;
    }

    XSynchGetResult(xscisc->conn, &rownum, &rows);

    xscsci_output(rownum, rows);

    return;
}

static xscsci_precommandops m_precommandops[] =
{
    {
        T_XSYNCH_NOP,
        "NOP",
        NULL
    },
    {
        T_XSYNCH_IDENTITYCMD,
        "IDENTITY COMMAND",
        NULL
    },
    {
        T_XSYNCH_CREATECMD,
        "CREATE COMMAND",
        xscsci_precommand_create
    },
    {
        T_XSYNCH_ALTERCMD,
        "ALTER COMMAND",
        xscsci_precommand_alter
    },
    {
        T_XSYNCH_REMOVECMD,
        "REMOVE COMMAND",
        xscsci_precommand_remove
    },
    {
        T_XSYNCH_DROPCMD,
        "DROP COMMAND",
        xscsci_precommand_drop
    },
    {
        T_XSYNCH_INITCMD,
        "INIT COMMAND",
        xscsci_precommand_init
    },
    {
        T_XSYNCH_EDITCMD,
        "EDIT COMMAND",
        xscsci_precommand_edit
    },
    {
        T_XSYNCH_STARTCMD,
        "START COMMAND",
        xscsci_precommand_start
    },
    {
        T_XSYNCH_STOPCMD,
        "STOP COMMAND",
        xscsci_precommand_stop
    },
    {
        T_XSYNCH_RELOADCMD,
        "RELOAD COMMAND",
        xscsci_precommand_reload
    },
    {
        T_XSYNCH_INFOCMD,
        "INFO COMMAND",
        xscsci_precommand_info
    },
    {
        T_XSYNCH_WATCHCMD,
        "WATCH COMMAND",
        xscsci_precommand_watch
    },
    {
        T_XSYNCH_CFGfILECMD,
        "never trigger",
        NULL
    },
    {
        T_XSYNCH_REFRESHCMD,
        "REFRESH COMMAND",
        xscsci_precommand_refresh
    },
    {
        T_XSYNCH_LISTCMD,
        "LIST COMMAND",
        xscsci_precommand_list
    },
    {
        T_XSYNCH_MAX,
        "MAX COMMAND",
        NULL
    }
};

void xscsci_precommand(xsciscistat* xscisc, xsynch_cmd* cmd)
{
    if(T_XSYNCH_MAX < cmd->type)
    {
        printf("precommand unknown cmd type %d\n", cmd->type);
        return;
    }

    if(NULL == m_precommandops[cmd->type].func)
    {
        printf("precommand unsupport %s\n", m_precommandops[cmd->type].desc);
        return ;
    }

    return m_precommandops[cmd->type].func(xscisc, cmd);

}