#include "app_incl.h"
#include "port/file/fd.h"
#include "port/net/net.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "command/cmd.h"
#include "queue/queue.h"
#include "net/netiomp/netiomp.h"
#include "net/netiomp/netiomp_poll.h"
#include "net/netpacket/netpacket.h"
#include "net/netpool.h"
#include "xmanager/xmanager_msg.h"
#include "xmanager/xmanager_metricnode.h"
#include "xmanager/xmanager_metric.h"
#include "xmanager/xmanager_metricmsgcreate.h"
#include "xmanager/xmanager_metricmsg.h"
#include "xmanager/xmanager_metricprogressnode.h"

static bool xmanager_metricmsg_parsecreateinplacejobname(char* srcpath,
                                                                char* dstpath,
                                                                char* jobname)
{
    bool bvalue         = false;
    int keylen          = 0;
    int keystart        = 0;
    int fd              = 0;
    int filesize        = 0;
    int fileoffset      = 0;
    int dfilesize       = 0;
    int dfileoffset     = 0;
    char* filedata      = NULL;
    char* dfiledata     = NULL;

    /* 
     * 读取文件
     * 找到 jobname 并替换为正确的名称 
     */
    fd = osal_file_open(srcpath, O_RDONLY, 0);
    if (-1 == fd)
    {
        elog(RLOG_WARNING, "can not open sample file:%s.", srcpath);
        return false;
    }

    filesize = osal_file_size(fd);
    if (-1 == filesize)
    {
        osal_file_close(fd);
        elog(RLOG_WARNING, "can not get file:%s size, error:%s.", srcpath, strerror(errno));
        return false;
    }

    filesize += 1;
    filedata = rmalloc0(filesize);
    if (NULL == filedata)
    {
        osal_file_close(fd);
        elog(RLOG_WARNING, "copy file, oom.");
        return false;
    }
    rmemset0(filedata, 0, '\0', filesize);
    filesize -= 1;

    /* 重新设置到文件头部*/
    osal_file_seek(fd, 0);

    /* 读取文件 */
    if (filesize != osal_file_read(fd, filedata, filesize))
    {
        rfree(filedata);
        osal_file_close(fd);
        elog(RLOG_WARNING, "read file %s error, %s", srcpath, strerror(errno));
        return false;
    }

    osal_file_close(fd);
    fd = -1;

    dfilesize = (2 * filesize);
    dfiledata = rmalloc0(dfilesize);
    if (NULL == dfiledata)
    {
        rfree(filedata);
        elog(RLOG_WARNING, "copy file, oom.");
        return false;
    }

    /* 替换 jobname */
    while (fileoffset < filesize)
    {
        bvalue = true;
        /* 行头部复制 */
        while (' ' == filedata[fileoffset]
            || '\t' == filedata[fileoffset]
            || '\f' == filedata[fileoffset]
            || '\r' == filedata[fileoffset]
            || '\n' == filedata[fileoffset])
        {
            /* 空行及空格等内容复制 */
            dfiledata[dfileoffset] = filedata[fileoffset];
            dfileoffset++;
            fileoffset++;
            continue;
        }

        if (fileoffset >= filesize)
        {
            break;
        }

        if ('#' == filedata[fileoffset])
        {
            /* 注释行, 复制到行尾部 */
            /* 复制 # 符号 */
            dfiledata[dfileoffset] = filedata[fileoffset];
            dfileoffset++;
            fileoffset++;

            /* 复制到尾部 */
            while (1)
            {
                if ('\r' == filedata[fileoffset])
                {
                    /* 遇到了行尾部, 复制 '\r' */
                    dfiledata[dfileoffset] = filedata[fileoffset];
                    dfileoffset++;
                    fileoffset++;
                    if (fileoffset < filesize)
                    {
                        if ('\n' == filedata[fileoffset])
                        {
                            /* windows */
                            dfiledata[dfileoffset] = filedata[fileoffset];
                            dfileoffset++;
                            fileoffset++;
                        }
                        else
                        {
                            /* macos 不做处理 */
                            ;
                        }
                    }
                    else
                    {
                        /* 解析到了尾部, 也是 macos */
                        ;
                    }

                    break;
                }
                else if ('\n' == filedata[fileoffset])
                {
                    /* linux 行尾 */
                    dfiledata[dfileoffset] = filedata[fileoffset];
                    dfileoffset++;
                    fileoffset++;
                    break;
                }

                dfiledata[dfileoffset] = filedata[fileoffset];
                dfileoffset++;
                fileoffset++;
            }
            continue;
        }

        /* 获取 key */
        keystart = fileoffset;
        while (' ' != filedata[fileoffset]&& '\t' != filedata[fileoffset])
        {
            dfiledata[dfileoffset] = filedata[fileoffset];
            dfileoffset++;
            fileoffset++;
            continue;
        }

        /* 计算 key 长度 */
        keylen = fileoffset - keystart;

        /* key 和 '=' 之间的空格 */
        while (' ' == filedata[fileoffset]
               || '\t' == filedata[fileoffset]
               || '\f' == filedata[fileoffset])
        {
            dfiledata[dfileoffset] = filedata[fileoffset];
            dfileoffset++;
            fileoffset++;
            continue;
        }

        /* 不是 ‘=’ 那么配置有问题 */
        if ('=' != filedata[fileoffset])
        {
            /* 配置有问题 */
            rfree(filedata);
            rfree(dfiledata);
            elog(RLOG_WARNING, "%s config error", srcpath);
            return false;
        }

        /* '=' 构建 */
        dfiledata[dfileoffset] = filedata[fileoffset];
        dfileoffset++;
        fileoffset++;

        if (keylen == strlen("jobname") && 0 == strncasecmp(filedata +keystart, "jobname", keylen))
        {
            /* jobname 替换 */
            dfiledata[dfileoffset] = ' ';
            dfileoffset++;
            rmemcpy1(dfiledata, dfileoffset, jobname, strlen(jobname));
            dfileoffset += strlen(jobname);
            dfiledata[dfileoffset] = ' ';
            dfileoffset++;

            /* 不需要 value */
            bvalue = false;
        }
        else
        {
            /* '=' 附加 */
            dfiledata[dfileoffset] = filedata[fileoffset];
            fileoffset++;
            dfileoffset++;
        }

        /* '=' 之后的空格 */
        while (' ' == filedata[fileoffset]
               || '\t' == filedata[fileoffset]
               || '\f' == filedata[fileoffset])
        {
            if (false == bvalue)
            {
                fileoffset++;
                continue;
            }

            dfiledata[dfileoffset] = filedata[fileoffset];
            dfileoffset++;
            fileoffset++;
            continue;
        }

        if ('\r' == filedata[fileoffset] || '\n' == filedata[fileoffset])
        {
            /* 配置有误 */
            rfree(filedata);
            rfree(dfiledata);
            elog(RLOG_WARNING, "%s config error", srcpath);
            return false;
        }

        /* value 值 */
        while ('\r' != filedata[fileoffset] && '\n' != filedata[fileoffset])
        {
            if (false == bvalue)
            {
                fileoffset++;
                continue;
            }

            dfiledata[dfileoffset] = filedata[fileoffset];
            dfileoffset++;
            fileoffset++;
            continue;
        }

        /* 换行处理 */
        if ('\r' == filedata[fileoffset])
        {
            /* 
             * Windows 使用 \r\n 为换行符
             * 早期 MacOS 使用 \r
             */
            dfiledata[dfileoffset] = filedata[fileoffset];
            dfileoffset++;
            fileoffset++;
            if (fileoffset < filesize)
            {
                if ('\n' == filedata[fileoffset])
                {
                    /* windows */
                    dfiledata[dfileoffset] = filedata[fileoffset];
                    dfileoffset++;
                    fileoffset++;
                }
                else
                {
                    /* macos 不做处理 */
                    ;
                }
            }
        }
        else if ('\n' == filedata[fileoffset])
        {
            /* linux */
            dfiledata[dfileoffset] = filedata[fileoffset];
            dfileoffset++;
            fileoffset++;
        }
        else
        {
            /* 配置有误 */
            rfree(filedata);
            rfree(dfiledata);
            elog(RLOG_WARNING, "%s config error", srcpath);
            return false;
        }
    }

    rfree(filedata);
    /* 打开目标文件 */
    fd = osal_file_open(dstpath, O_RDWR | O_CREAT, g_file_create_mode);
    if (-1 == fd)
    {
        rfree(dfiledata);
        elog(RLOG_WARNING, "can not open sample file:%s, %s.", dstpath, strerror(errno));
        return false;
    }

    if (dfileoffset != osal_file_write(fd, dfiledata, dfileoffset))
    {
        osal_file_close(fd);
        fd = -1;
        rfree(dfiledata);
        return false;
    }

    osal_file_close(fd);
    rfree(dfiledata);
    return true;
}

static bool xmanager_metricmsg_parsecreateprocess(xmanager_metric* xmetric,
                                                         netpoolentry* npoolentry,
                                                         uint8* uptr,
                                                         char* jobname)
{
    int len                                                 = 0;
    int jobcnt                                              = 0;
    int jobtype                                             = 0;
    int idx_jobcnt                                          = 0;
    char* name                                              = NULL;
    xmanager_metricnode* pxmetricnode                = NULL;
    xmanager_metricnode* jobxmetricnode              = NULL;
    xmanager_metricnode xmetricnode                  = { 0 };
    xmanager_metricprogressnode* xmetricprogressnode = NULL;

    xmetricprogressnode = (xmanager_metricprogressnode*)xmanager_metricprogressnode_init();
    if (NULL == xmetricprogressnode)
    {
        elog(RLOG_WARNING, "xmanager create progress error, out of memory");
        return false;
    }
    xmetricprogressnode->base.name = jobname;
    jobname = NULL;

    /* jobcnt */
    rmemcpy1(&jobcnt, 0, uptr, 4);
    jobcnt = r_ntoh32(jobcnt);
    uptr += 4;

    for (idx_jobcnt = 0; idx_jobcnt < jobcnt; idx_jobcnt++)
    {
        /* jobtype */
        rmemcpy1(&jobtype, 0, uptr, 4);
        jobtype = r_ntoh32(jobtype);
        uptr += 4;

        if (XMANAGER_METRICNODETYPE_INTEGRATE < jobtype)
        {
            elog(RLOG_WARNING, "xmanager recv create progress command, need jobtype less then HGRECEIVELOG");
            xmanager_metricprogressnode_destroy((xmanager_metricnode*)xmetricprogressnode);
            return false;
        }

        /* name len */
        rmemcpy1(&len, 0, uptr, 4);
        len = r_ntoh32(len);
        uptr += 4;
        len += 1;

        name = rmalloc0(len);
        if (NULL == name)
        {
            elog(RLOG_WARNING, "%s", "xmanager recv create progress command, oom");
            xmanager_metricprogressnode_destroy((xmanager_metricnode*)xmetricprogressnode);
            return false;
        }
        rmemset0(name, 0, '\0', len);
        len -= 1;
        rmemcpy0(name, 0, uptr, len);
        uptr += len;

        /* 查看是否存在 */
        xmetricnode.type = jobtype;
        xmetricnode.name = name;

        pxmetricnode = dlist_get(xmetric->metricnodes, &xmetricnode, xmanager_metricnode_cmp);
        if (NULL == pxmetricnode)
        {
            elog(RLOG_WARNING, "xmanager recv create progress command, not find %d.%s", jobtype, name);
            xmanager_metricprogressnode_destroy((xmanager_metricnode*)xmetricprogressnode);
            return false;
        }

        /* 创建新的 metricnode */
        jobxmetricnode = xmanager_metricnode_init(jobtype);
        if (NULL == jobxmetricnode)
        {
            elog(RLOG_WARNING, "%s", "xmanager create command, oom");
            xmanager_metricprogressnode_destroy((xmanager_metricnode*)xmetricprogressnode);
            return false;
        }

        jobxmetricnode->name = name;
        jobxmetricnode->conf = rstrdup(pxmetricnode->conf);
        name = NULL;
        pxmetricnode = NULL;

        xmetricprogressnode->progressjop = dlist_put(xmetricprogressnode->progressjop, jobxmetricnode);
        jobxmetricnode = NULL;
    }

    /* 设置为初始化状态 */
    xmetricprogressnode->base.stat = XMANAGER_METRICNODESTAT_ONLINE;

    /* 将 xmetricnode 加入到链表中 */
    xmetric->metricnodes = dlist_put(xmetric->metricnodes, (void*)&xmetricprogressnode->base);

    /* 将 metricnode 落盘 */
    xmanager_metricnode_flush(xmetric->metricnodes);
    return true;
}

/*
 * 处理 create 命令
 *  1、jobtype 需要小于 ALL
 *  2、校验 job 是否已经存在
 *  3、将 job 加入到 xmetric->metricnodes 中
*/
bool xmanager_metricmsg_parsecreate(xmanager_metric* xmetric,
                                           netpoolentry* npoolentry,
                                           netpacket* npacket)
{
    /* 错误码 */
    int errcode                                         = 0;
    int len                                             = 0;
    int jobtype                                         = 0;
    uint8* uptr                                         = NULL;
    char* jobname                                       = NULL;
    xmanager_metricnode* pxmetricnode            = NULL;
    xmanager_metricnode xmetricnode              = { 0 };
    char errormsg[512]                                  = { 0 };
    char srcpath[1024]                                  = { 0 };
    char dstpath[1024]                                  = { 0 };

    /* 获取作业类型 */
    uptr = npacket->data;

    /* msglen + crc32 + commandtype */
    uptr += 12;

    /* jobtype */
    rmemcpy1(&jobtype, 0, uptr, 4);
    jobtype = r_ntoh32(jobtype);
    uptr += 4;

    if (XMANAGER_METRICNODETYPE_ALL <= jobtype)
    {
        snprintf(errormsg, 512, "xmanager recv create command, need jobtype less then ALL");
        errcode = ERROR_MSGCOMMANDUNVALID;
        goto xmanager_metricmsg_parsecreate_error;
    }

    rmemcpy1(&len, 0, uptr, 4);
    len = r_ntoh32(len);
    uptr += 4;
    len += 1;

    jobname = rmalloc0(len);
    if (NULL == jobname)
    {
        snprintf(errormsg, 512, "%s", "xmanager recv create command, oom");
        errcode = ERROR_OOM;
        goto xmanager_metricmsg_parsecreate_error;
    }
    rmemset0(jobname, 0, '\0', len);
    len -= 1;
    rmemcpy0(jobname, 0, uptr, len);
    uptr += len;

    /* 查看是否存在 */
    xmetricnode.type = jobtype;
    xmetricnode.name = jobname;

    if (NULL != dlist_isexist(xmetric->metricnodes, &xmetricnode, xmanager_metricnode_cmp))
    {
        snprintf(errormsg, 512, "%s already exist.", jobname);
        errcode = ERROR_MSGEXIST;
        goto xmanager_metricmsg_parsecreate_error;
    }

    if (XMANAGER_METRICNODETYPE_PROCESS == jobtype)
    {
        if (false == xmanager_metricmsg_parsecreateprocess(xmetric,
                                                                  npoolentry,
                                                                  uptr,
                                                                  jobname))
        {
            snprintf(errormsg, 512, "xmanager create process command error ");
            errcode = ERROR_MSGCOMMAND;
            goto xmanager_metricmsg_parsecreate_error;
        }
        return xmanager_metricmsg_assemblecmdresult(xmetric, npoolentry, XMANAGER_MSG_CREATECMD);
    }

    /* 生成新的配置文件 */
    snprintf(srcpath,
             1024,
             "%s/sample/%s.cfg.sample",
             xmetric->configpath,
             xmanager_metricnode_getname(jobtype));

    snprintf(dstpath,
             1024,
             "%s/%s_%s.cfg",
             xmetric->configpath,
             xmanager_metricnode_getname(jobtype),
             jobname);

    if (false == xmanager_metricmsg_parsecreateinplacejobname(srcpath, dstpath, jobname))
    {
        snprintf(errormsg, 512, "%s", "xmanager create command generate config file error");
        errcode = ERROR_MSGCOMMAND;
        goto xmanager_metricmsg_parsecreate_error;
    }

    /* 创建新的 metricnode */
    pxmetricnode = xmanager_metricnode_init(jobtype);
    if (NULL == pxmetricnode)
    {
        snprintf(errormsg, 512, "%s", "xmanager create command, oom");
        errcode = ERROR_OOM;
        goto xmanager_metricmsg_parsecreate_error;
    }

    pxmetricnode->name = jobname;
    jobname = NULL;

    /* 设置 config 信息 */
    len = strlen(dstpath);
    len += 1;

    pxmetricnode->conf = rmalloc0(len);
    if (NULL == pxmetricnode->conf)
    {
        snprintf(errormsg, 512, "%s", "xmanager recv create command, oom");
        errcode = ERROR_OOM;
        goto xmanager_metricmsg_parsecreate_error;
    }
    rmemset0(pxmetricnode->conf, 0, '\0', len);
    len -= 1;
    rmemcpy0(pxmetricnode->conf, 0, dstpath, len);

    /* 设置为初始化状态 */
    pxmetricnode->stat = XMANAGER_METRICNODESTAT_INIT;

    /* 将 xmetricnode 加入到链表中 */
    xmetric->metricnodes = dlist_put(xmetric->metricnodes, pxmetricnode);

    /* 将 metricnode 落盘 */
    xmanager_metricnode_flush(xmetric->metricnodes);

    /* 构建返回消息 */
    return xmanager_metricmsg_assemblecmdresult(xmetric, npoolentry, XMANAGER_MSG_CREATECMD);

xmanager_metricmsg_parsecreate_error:

    if (NULL != jobname)
    {
        rfree(jobname);
    }

    if (NULL != pxmetricnode)
    {
        xmanager_metricnode_destroy(pxmetricnode);
    }

    elog(RLOG_WARNING, errormsg);
    return xmanager_metricmsg_assembleerrormsg(xmetric,
                                                      npoolentry->wpackets,
                                                      XMANAGER_MSG_CREATECMD,
                                                      errcode,
                                                      errormsg);
}
