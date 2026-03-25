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
#include "xmanager/xmanager_metricmsgedit.h"
#include "xmanager/xmanager_metricmsg.h"

static bool xmanager_metricmsg_assembleedit(xmanager_metric* xmetric, netpoolentry* npoolentry,
                                            xmanager_metricnode* metricnode)
{
    uint8      u8value = 0;
    int        fd = 0;
    int        filesize = 0;
    int        rowlen = 0;
    int        msglen = 0;
    int        ivalue = 0;
    uint8*     rowuptr = 0;
    uint8*     uptr = 0;
    char*      conffile = NULL;
    netpacket* npacket = NULL;

    if (NULL == metricnode->conf)
    {
        elog(RLOG_WARNING, "edit %s error configure file is null: %s.", conffile);
        return false;
    }

    conffile = metricnode->conf;

    /*
     * Read file
     */
    fd = osal_file_open(conffile, O_RDONLY, 0);
    if (-1 == fd)
    {
        elog(RLOG_WARNING, "xmanager metric assemble edit msg not open file:%s.", conffile);
        return false;
    }

    /* Get file size */
    filesize = osal_file_size(fd);
    if (-1 == filesize)
    {
        osal_file_close(fd);
        elog(RLOG_WARNING, "xmanager metric assemble edit msg not get file:%s size, error:%s.",
             conffile, strerror(errno));
        return false;
    }

    /* 4 total length + 4 crc32 + 4 msgtype + 1 success or fail */
    msglen = 4 + 4 + 4 + 1;

    /* rowcnt */
    msglen += 4;

    /*rowlen 4 + datalen 4 + filedata */
    rowlen = (4 + 4 + filesize);

    msglen += rowlen;

    /* Allocate space */
    npacket = netpacket_init();
    if (NULL == npacket)
    {
        elog(RLOG_WARNING, "xmanager metric assemble edit msg out of memory");
        return false;
    }
    msglen += 1;
    npacket->data = netpacket_data_init(msglen);
    if (NULL == npacket->data)
    {
        elog(RLOG_WARNING, "xmanager metric assemble edit msg data, out of memory");
        netpacket_destroy(npacket);
        return false;
    }
    msglen -= 1;
    npacket->used = msglen;

    /* Assemble data */
    uptr = npacket->data;

    /* Total data length */
    ivalue = msglen;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* crc32 */
    uptr += 4;

    /* Type */
    ivalue = XMANAGER_MSG_EDITCMD;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* Type success flag */
    u8value = 0;
    rmemcpy1(uptr, 0, &u8value, 1);
    uptr += 1;

    /* rowcnt */
    ivalue = 1;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    rowlen = 0;
    rowuptr = uptr;

    /* Skip row length */
    uptr += 4;
    rowlen = 4;

    /* datalen */
    ivalue = filesize;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* Re-set to file header*/
    osal_file_seek(fd, 0);

    /* Read file, write message */
    if (filesize != osal_file_read(fd, (char*)uptr, filesize))
    {
        osal_file_close(fd);
        fd = -1;
        netpacket_destroy(npacket);
        elog(RLOG_WARNING, "xmanager metric assemble edit msg read file %s error, %s", conffile,
             strerror(errno));
        return false;
    }

    rowlen += filesize;

    /* Close file */
    osal_file_close(fd);
    fd = -1;

    /* Total row length */
    rowlen = r_hton32(rowlen);
    rmemcpy1(rowuptr, 0, &rowlen, 4);

    /* Mount netpacket to send queue */
    if (false == queue_put(npoolentry->wpackets, (void*)npacket))
    {
        elog(RLOG_WARNING, "xmanager metric assemble edit msg add message to queue error");
        netpacket_destroy(npacket);
        return false;
    }

    return true;
}

bool xmanager_metricmsg_parseedit(xmanager_metric* xmetric, netpoolentry* npoolentry,
                                  netpacket* npacket)
{
    int                  len = 0;
    int                  jobtype = 0;
    int                  errcode = 0;
    uint8*               uptr = NULL;
    char*                jobname = NULL;
    xmanager_metricnode* pxmetricnode = NULL;
    xmanager_metricnode  xmetricnode = {0};
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
        snprintf(errormsg, 2048, "ERROR: xmanager parse edit command, unsupport %s",
                 xmanager_metricnode_getname(jobtype));
        goto xmanager_metricmsg_parseedit_error;
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
        snprintf(errormsg, 2048, "ERROR: xmanager parse edit command, out of memory.");
        goto xmanager_metricmsg_parseedit_error;
    }
    rmemset0(jobname, 0, '\0', len);
    len -= 1;
    rmemcpy0(jobname, 0, uptr, len);

    /* Check if node exists */
    xmetricnode.type = jobtype;
    xmetricnode.name = jobname;

    pxmetricnode = dlist_get(xmetric->metricnodes, &xmetricnode, xmanager_metricnode_cmp);
    if (NULL == pxmetricnode)
    {
        errcode = ERROR_NOENT;
        snprintf(errormsg, 2048, "ERROR: %s does not exist.", jobname);
        goto xmanager_metricmsg_parseedit_error;
    }

    /* Assemble return message */
    if (false == xmanager_metricmsg_assembleedit(xmetric, npoolentry, pxmetricnode))
    {
        errcode = ERROR_OOM;
        snprintf(errormsg, 2048, "ERROR: %s does not assemble edit result.", jobname);
        goto xmanager_metricmsg_parseedit_error;
    }

    if (NULL != jobname)
    {
        rfree(jobname);
    }

    return xmanager_metricmsg_assemblecmdresult(xmetric, npoolentry, XMANAGER_MSG_EDITCMD);
    ;

xmanager_metricmsg_parseedit_error:

    if (NULL != jobname)
    {
        rfree(jobname);
    }

    elog(RLOG_WARNING, errormsg);
    return xmanager_metricmsg_assembleerrormsg(xmetric, npoolentry->wpackets, XMANAGER_MSG_EDITCMD,
                                               errcode, errormsg);
}
