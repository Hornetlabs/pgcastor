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
#include "xmanager/ripple_xmanager_metricmsgedit.h"
#include "xmanager/ripple_xmanager_metricmsg.h"


static bool ripple_xmanager_metricmsg_assembleedit(ripple_xmanager_metric* xmetric,
                                                   ripple_netpoolentry* npoolentry,
                                                   ripple_xmanager_metricnode* metricnode)
{
    uint8 u8value                                               = 0;
    int fd                                                      = 0;
    int filesize                                                = 0;
    int rowlen                                                  = 0;
    int msglen                                                  = 0;
    int ivalue                                                  = 0;
    uint8* rowuptr                                              = 0;
    uint8* uptr                                                 = 0;
    char* conffile                                              = NULL;
    ripple_netpacket* npacket                                   = NULL;

    if (NULL == metricnode->conf)
    {
        elog(RLOG_WARNING, "edit %s error configure file is null: %s.", conffile);
        return false;
    }

    conffile = metricnode->conf;
 
    /* 
     * 读取文件
     */
    fd = FileOpen(conffile, O_RDONLY, 0);
    if (-1 == fd)
    {
        elog(RLOG_WARNING, "xmanager metric assemble edit msg not open file:%s.", conffile);
        return false;
    }

    /* 获取文件大小 */
    filesize = FileSize(fd);
    if (-1 == filesize)
    {
        FileClose(fd);
        elog(RLOG_WARNING, "xmanager metric assemble edit msg not get file:%s size, error:%s.", conffile, strerror(errno));
        return false;
    }

    /* 4 总长度 + 4 crc32 + 4 msgtype + 1 成功/失败 */
    msglen = 4 + 4 + 4 + 1;

    /* rowcnt */
    msglen += 4;

    /*rowlen 4 + datalen 4 + filedata */
    rowlen = (4 + 4 + filesize);

    msglen += rowlen;

    /* 申请空间 */
    npacket = ripple_netpacket_init();
    if (NULL == npacket)
    {
        elog(RLOG_WARNING, "xmanager metric assemble edit msg out of memory");
        return false;
    }
    msglen += 1;
    npacket->data = ripple_netpacket_data_init(msglen);
    if (NULL == npacket->data)
    {
        elog(RLOG_WARNING, "xmanager metric assemble edit msg data, out of memory");
        ripple_netpacket_destroy(npacket);
        return false;
    }
    msglen -= 1;
    npacket->used = msglen;

    /* 组装数据 */
    uptr = npacket->data;

    /* 数据总长度 */
    ivalue = msglen;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* crc32 */
    uptr += 4;

    /* 类型 */
    ivalue = RIPPLE_XMANAGER_MSG_EDITCMD;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* 类型成功标识 */
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

    /* 偏过行长度 */
    uptr += 4;
    rowlen = 4;

    /* datalen */
    ivalue = filesize;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* 重新设置到文件头部*/
    FileSeek(fd, 0);

    /* 读取文件，写入消息 */
    if (filesize != FileRead(fd, (char*)uptr, filesize))
    {
        FileClose(fd);
        fd = -1;
        ripple_netpacket_destroy(npacket);
        elog(RLOG_WARNING, "xmanager metric assemble edit msg read file %s error, %s", conffile, strerror(errno));
        return false;
    }

    rowlen += filesize;

    /* 关闭文件 */
    FileClose(fd);
    fd = -1;

    /* 行总长度 */
    rowlen = r_hton32(rowlen);
    rmemcpy1(rowuptr, 0, &rowlen, 4);

    /* 将 netpacket 挂载到待发送队列中 */
    if (false == ripple_queue_put(npoolentry->wpackets, (void*)npacket))
    {
        elog(RLOG_WARNING, "xmanager metric assemble edit msg add message to queue error");
        ripple_netpacket_destroy(npacket);
        return false;
    }

    return true;
}

bool ripple_xmanager_metricmsg_parseedit(ripple_xmanager_metric* xmetric,
                                         ripple_netpoolentry* npoolentry,
                                         ripple_netpacket* npacket)
{
    int len                                             = 0;
    int jobtype                                         = 0;
    int errcode                                         = 0;
    uint8* uptr                                         = NULL;
    char* jobname                                       = NULL;
    ripple_xmanager_metricnode* pxmetricnode            = NULL;
    ripple_xmanager_metricnode xmetricnode              = { 0 };
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
                 "ERROR: xmanager parse edit command, unsupport %s",
                 ripple_xmanager_metricnode_getname(jobtype));
        goto ripple_xmanager_metricmsg_parseedit_error;
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
        snprintf(errormsg, 2048, "ERROR: xmanager parse edit command, out of memory.");
        goto ripple_xmanager_metricmsg_parseedit_error;
    }
    rmemset0(jobname, 0, '\0', len);
    len -= 1;
    rmemcpy0(jobname, 0, uptr, len);

    /* 查看节点是否存在 */
    xmetricnode.type = jobtype;
    xmetricnode.name = jobname;

    pxmetricnode = dlist_get(xmetric->metricnodes, &xmetricnode, ripple_xmanager_metricnode_cmp);
    if (NULL == pxmetricnode)
    {
        errcode = RIPPLE_ERROR_NOENT;
        snprintf(errormsg, 2048, "ERROR: %s does not exist.", jobname);
        goto ripple_xmanager_metricmsg_parseedit_error;
    }

    /* 组装返回消息 */
    if (false == ripple_xmanager_metricmsg_assembleedit(xmetric, npoolentry, pxmetricnode))
    {
        errcode = RIPPLE_ERROR_OOM;
        snprintf(errormsg, 2048, "ERROR: %s does not assemble edit result.", jobname);
        goto ripple_xmanager_metricmsg_parseedit_error;
    }

    if (NULL != jobname)
    {
        rfree(jobname);
    }

    return ripple_xmanager_metricmsg_assemblecmdresult(xmetric, npoolentry, RIPPLE_XMANAGER_MSG_EDITCMD);;

ripple_xmanager_metricmsg_parseedit_error:

    if (NULL != jobname)
    {
        rfree(jobname);
    }

    elog(RLOG_WARNING, errormsg);
    return ripple_xmanager_metricmsg_assembleerrormsg(xmetric,
                                                      npoolentry->wpackets,
                                                      RIPPLE_XMANAGER_MSG_EDITCMD,
                                                      errcode,
                                                      errormsg);

}
