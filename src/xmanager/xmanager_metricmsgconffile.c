#include "app_incl.h"
#include "port/file/fd.h"
#include "utils/dlist/dlist.h"
#include "utils/daemon/process.h"
#include "queue/queue.h"
#include "net/netiomp/netiomp.h"
#include "net/netiomp/netiomp_poll.h"
#include "net/netpacket/netpacket.h"
#include "net/netpool.h"
#include "xmanager/xmanager_msg.h"
#include "xmanager/xmanager_metricnode.h"
#include "xmanager/xmanager_metric.h"
#include "xmanager/xmanager_metricmsgconffile.h"
#include "xmanager/xmanager_metricmsg.h"

/*
 * Handle conffile command
 *1. jobtype must be less than PROCESS
 *2. Verify job already exists
 *3. Overwrite conf file
 */
bool xmanager_metricmsg_parseconffile(xmanager_metric* xmetric, netpoolentry* npoolentry,
                                      netpacket* npacket)
{
    int                  fd = 0;
    int                  len = 0;
    int                  jobtype = 0;
    int                  errcode = 0;
    uint32               fnamelen = 0;
    uint32               filesize = 0;
    uint8*               uptr = NULL;
    char*                jobname = NULL;
    xmanager_metricnode* pxmetricnode = NULL;
    xmanager_metricnode  xmetricnode = {0};
    char                 tmppath[1024] = {0};
    char                 errormsg[2048] = {0};

    /* Get job type */
    uptr = npacket->data;

    /* msglen + crc32 + commandtype */
    uptr += 12;

    /* jobtype */
    rmemcpy1(&jobtype, 0, uptr, 4);
    jobtype = r_ntoh32(jobtype);
    uptr += 4;

    if (XMANAGER_METRICNODETYPE_PROCESS <= jobtype)
    {
        errcode = ERROR_MSGCOMMANDUNVALID;
        snprintf(errormsg, 2048, "xmanager parse conffile command, unsupport %s",
                 xmanager_metricnode_getname(jobtype));
        goto xmanager_metricmsg_parseconffile_error;
    }

    /* Get jobname */
    rmemcpy1(&len, 0, uptr, 4);
    len = r_ntoh32(len);
    uptr += 4;
    len += 1;

    jobname = rmalloc0(len);
    if (NULL == jobname)
    {
        errcode = ERROR_OOM;
        snprintf(errormsg, 2048, "xmanager parse conffile command, out of memory.");
        goto xmanager_metricmsg_parseconffile_error;
    }
    rmemset0(jobname, 0, '\0', len);
    len -= 1;
    rmemcpy0(jobname, 0, uptr, len);
    uptr += len;

    /* Check if node exists */
    xmetricnode.type = jobtype;
    xmetricnode.name = jobname;

    pxmetricnode = dlist_get(xmetric->metricnodes, &xmetricnode, xmanager_metricnode_cmp);
    if (NULL == pxmetricnode)
    {
        errcode = ERROR_NOENT;
        snprintf(errormsg, 2048, "ERROR: %s does not exist.", jobname);
        goto xmanager_metricmsg_parseconffile_error;
    }

    /*filename lengthgth */
    rmemcpy1(&fnamelen, 0, uptr, 4);
    fnamelen = r_ntoh32(fnamelen);
    uptr += 4;

    /* Temporarily unused */
    uptr += fnamelen;

    /*filename lengthgth */
    rmemcpy1(&filesize, 0, uptr, 4);
    filesize = r_ntoh32(filesize);
    uptr += 4;

    /* Temp file */
    snprintf(tmppath, 1024, "%s.tmp", pxmetricnode->conf);

    fd = osal_file_open(tmppath, O_RDWR | O_CREAT, g_file_create_mode);
    if (-1 == fd)
    {
        errcode = ERROR_OPENFILEERROR;
        snprintf(errormsg, 2048, "ERROR: %s does not creat tmpfile.", jobname);
        goto xmanager_metricmsg_parseconffile_error;
    }

    if (filesize != osal_file_write(fd, (char*)uptr, filesize))
    {
        osal_file_close(fd);
        fd = -1;
        errcode = ERROR_OPENFILEERROR;
        snprintf(errormsg, 2048, "ERROR: %s does not writer tmpfile.", jobname);
        goto xmanager_metricmsg_parseconffile_error;
    }

    /* Rename file */
    if (osal_durable_rename(tmppath, pxmetricnode->conf, RLOG_DEBUG))
    {
        errcode = ERROR_OPENFILEERROR;
        snprintf(errormsg, 2048, "ERROR: %s does not renaming file %s:%s .", jobname, tmppath,
                 strerror(errno));
        goto xmanager_metricmsg_parseconffile_error;
    }

    if (NULL != jobname)
    {
        rfree(jobname);
    }

    return xmanager_metricmsg_assemblecmdresult(xmetric, npoolentry, XMANAGER_MSG_CONFFILECMD);

xmanager_metricmsg_parseconffile_error:

    if (NULL != jobname)
    {
        rfree(jobname);
    }

    elog(RLOG_WARNING, errormsg);
    return xmanager_metricmsg_assembleerrormsg(xmetric, npoolentry->wpackets,
                                               XMANAGER_MSG_CONFFILECMD, errcode, errormsg);
}
