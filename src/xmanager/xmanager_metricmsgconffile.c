#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "utils/dlist/dlist.h"
#include "utils/daemon/ripple_process.h"
#include "queue/ripple_queue.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netiomp/ripple_netiomp_poll.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netpool.h"
#include "xmanager/ripple_xmanager_msg.h"
#include "xmanager/ripple_xmanager_metricnode.h"
#include "xmanager/ripple_xmanager_metric.h"
#include "xmanager/ripple_xmanager_metricmsgconffile.h"
#include "xmanager/ripple_xmanager_metricmsg.h"

/*
 * 处理 conffile 命令
 *  1、jobtype 需要小于 PROCESS
 *  2、校验 job 是否已经存在
 *  3、覆盖conf文件
*/
bool ripple_xmanager_metricmsg_parseconffile(ripple_xmanager_metric* xmetric,
                                             ripple_netpoolentry* npoolentry,
                                             ripple_netpacket* npacket)
{
    int fd                                              = 0;
    int len                                             = 0;
    int jobtype                                         = 0;
    int errcode                                         = 0;
    uint32 fnamelen                                     = 0;
    uint32 filesize                                     = 0;
    uint8* uptr                                         = NULL;
    char* jobname                                       = NULL;
    ripple_xmanager_metricnode* pxmetricnode            = NULL;
    ripple_xmanager_metricnode xmetricnode              = { 0 };
    char tmppath[1024]                                  = { 0 };
    char errormsg[2048]                                 = { 0 };

    /* 获取作业类型 */
    uptr = npacket->data;

    /* msglen + crc32 + commandtype */
    uptr += 12;

    /* jobtype */
    rmemcpy1(&jobtype, 0, uptr, 4);
    jobtype = r_ntoh32(jobtype);
    uptr += 4;

    if (RIPPLE_XMANAGER_METRICNODETYPE_PROCESS <= jobtype)
    {
        errcode = RIPPLE_ERROR_MSGCOMMANDUNVALID;
        snprintf(errormsg, 2048, 
                 "xmanager parse conffile command, unsupport %s",
                 ripple_xmanager_metricnode_getname(jobtype));
        goto ripple_xmanager_metricmsg_parseconffile_error;
    }

    /* 获取 jobname */
    rmemcpy1(&len, 0, uptr, 4);
    len = r_ntoh32(len);
    uptr += 4;
    len += 1;

    jobname = rmalloc0(len);
    if (NULL == jobname)
    {
        errcode = RIPPLE_ERROR_OOM;
        snprintf(errormsg, 2048, "xmanager parse conffile command, out of memory.");
        goto ripple_xmanager_metricmsg_parseconffile_error;
    }
    rmemset0(jobname, 0, '\0', len);
    len -= 1;
    rmemcpy0(jobname, 0, uptr, len);
    uptr += len;

    /* 查看节点是否存在 */
    xmetricnode.type = jobtype;
    xmetricnode.name = jobname;

    pxmetricnode = dlist_get(xmetric->metricnodes, &xmetricnode, ripple_xmanager_metricnode_cmp);
    if (NULL == pxmetricnode)
    {
        errcode = RIPPLE_ERROR_NOENT;
        snprintf(errormsg, 2048, "ERROR: %s does not exist.", jobname);
        goto ripple_xmanager_metricmsg_parseconffile_error;
    }

    /* filename len */
    rmemcpy1(&fnamelen, 0, uptr, 4);
    fnamelen = r_ntoh32(fnamelen);
    uptr += 4;

    /* 暂时无用 */
    uptr += fnamelen;

    /* filename len */
    rmemcpy1(&filesize, 0, uptr, 4);
    filesize = r_ntoh32(filesize);
    uptr += 4;

    /* 临时文件 */
    snprintf(tmppath, 1024, "%s.tmp", pxmetricnode->conf);

    fd = FileOpen(tmppath, O_RDWR | O_CREAT, g_file_create_mode);
    if (-1 == fd)
    {
        errcode = RIPPLE_ERROR_OPENFILEERROR;
        snprintf(errormsg, 2048, "ERROR: %s does not creat tmpfile.", jobname);
        goto ripple_xmanager_metricmsg_parseconffile_error;
    }

    if (filesize != FileWrite(fd, (char*)uptr, filesize))
    {
        FileClose(fd);
        fd = -1;
        errcode = RIPPLE_ERROR_OPENFILEERROR;
        snprintf(errormsg, 2048, "ERROR: %s does not writer tmpfile.", jobname);
        goto ripple_xmanager_metricmsg_parseconffile_error;
    }

    /* 重命名文件 */
    if (durable_rename(tmppath, pxmetricnode->conf, RLOG_DEBUG)) 
    {
        errcode = RIPPLE_ERROR_OPENFILEERROR;
        snprintf(errormsg, 2048, "ERROR: %s does not renaming file %s:%s .", jobname, tmppath, strerror(errno));
        goto ripple_xmanager_metricmsg_parseconffile_error;
    }

    if (NULL != jobname)
    {
        rfree(jobname);
    }

    return ripple_xmanager_metricmsg_assemblecmdresult(xmetric, npoolentry, RIPPLE_XMANAGER_MSG_CONFFILECMD);

ripple_xmanager_metricmsg_parseconffile_error:

    if (NULL != jobname)
    {
        rfree(jobname);
    }

    elog(RLOG_WARNING, errormsg);
    return ripple_xmanager_metricmsg_assembleerrormsg(xmetric,
                                                      npoolentry->wpackets,
                                                      RIPPLE_XMANAGER_MSG_CONFFILECMD,
                                                      errcode,
                                                      errormsg);

}
