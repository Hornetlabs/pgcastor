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

static bool xmanager_metricmsg_parsecreateinplacejobname(char* srcpath, char* dstpath, char* jobname)
{
    bool  bvalue = false;
    int   keylen = 0;
    int   keystart = 0;
    int   fd = 0;
    int   filesize = 0;
    int   fileoffset = 0;
    int   dfilesize = 0;
    int   dfileoffset = 0;
    char* filedata = NULL;
    char* dfiledata = NULL;

    /*
     * Read file
     * Find jobname and replace with correct name
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

    /* Reset to file head */
    osal_file_seek(fd, 0);

    /* Read file */
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

    /* Replace jobname */
    while (fileoffset < filesize)
    {
        bvalue = true;
        /* Copy line header */
        while (' ' == filedata[fileoffset] || '\t' == filedata[fileoffset] || '\f' == filedata[fileoffset] ||
               '\r' == filedata[fileoffset] || '\n' == filedata[fileoffset])
        {
            /* Copy empty lines and spaces */
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
            /* Comment line, copy to end of line */
            /* Copy # symbol */
            dfiledata[dfileoffset] = filedata[fileoffset];
            dfileoffset++;
            fileoffset++;

            /* Copy to end */
            while (1)
            {
                if ('\r' == filedata[fileoffset])
                {
                    /* Encountered line ending, copy '\r' */
                    dfiledata[dfileoffset] = filedata[fileoffset];
                    dfileoffset++;
                    fileoffset++;
                    if (fileoffset < filesize)
                    {
                        if ('\n' == filedata[fileoffset])
                        {
                            /* Windows */
                            dfiledata[dfileoffset] = filedata[fileoffset];
                            dfileoffset++;
                            fileoffset++;
                        }
                        else
                        {
                            /* macOS - no action needed */
                            ;
                        }
                    }
                    else
                    {
                        /* Reached end, also macOS */
                        ;
                    }

                    break;
                }
                else if ('\n' == filedata[fileoffset])
                {
                    /* Linux line ending */
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

        /* Get key */
        keystart = fileoffset;
        while (' ' != filedata[fileoffset] && '\t' != filedata[fileoffset])
        {
            dfiledata[dfileoffset] = filedata[fileoffset];
            dfileoffset++;
            fileoffset++;
            continue;
        }

        /* Calculate key length */
        keylen = fileoffset - keystart;

        /* Space between key and '=' */
        while (' ' == filedata[fileoffset] || '\t' == filedata[fileoffset] || '\f' == filedata[fileoffset])
        {
            dfiledata[dfileoffset] = filedata[fileoffset];
            dfileoffset++;
            fileoffset++;
            continue;
        }

        /* If not '=', config is invalid */
        if ('=' != filedata[fileoffset])
        {
            /* Invalid config */
            rfree(filedata);
            rfree(dfiledata);
            elog(RLOG_WARNING, "%s config error", srcpath);
            return false;
        }

        /* Build '=' */
        dfiledata[dfileoffset] = filedata[fileoffset];
        dfileoffset++;
        fileoffset++;

        if (keylen == strlen("jobname") && 0 == strncasecmp(filedata + keystart, "jobname", keylen))
        {
            /* Replace jobname */
            dfiledata[dfileoffset] = ' ';
            dfileoffset++;
            rmemcpy1(dfiledata, dfileoffset, jobname, strlen(jobname));
            dfileoffset += strlen(jobname);
            dfiledata[dfileoffset] = ' ';
            dfileoffset++;

            /* No value needed */
            bvalue = false;
        }
        else
        {
            /* Append '=' */
            dfiledata[dfileoffset] = filedata[fileoffset];
            fileoffset++;
            dfileoffset++;
        }

        /* Space after '=' */
        while (' ' == filedata[fileoffset] || '\t' == filedata[fileoffset] || '\f' == filedata[fileoffset])
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
            /* Config error */
            rfree(filedata);
            rfree(dfiledata);
            elog(RLOG_WARNING, "%s config error", srcpath);
            return false;
        }

        /* Value */
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

        /* Newline handling */
        if ('\r' == filedata[fileoffset])
        {
            /*
             * Windows uses \r\n as newline
             * Early MacOS uses \r
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
                    /* macOS - no action needed */
                    ;
                }
            }
        }
        else if ('\n' == filedata[fileoffset])
        {
            /* Linux */
            dfiledata[dfileoffset] = filedata[fileoffset];
            dfileoffset++;
            fileoffset++;
        }
        else
        {
            /* Config error */
            rfree(filedata);
            rfree(dfiledata);
            elog(RLOG_WARNING, "%s config error", srcpath);
            return false;
        }
    }

    rfree(filedata);
    /* Open target file */
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
                                                  netpoolentry*    npoolentry,
                                                  uint8*           uptr,
                                                  char*            jobname)
{
    int                          len = 0;
    int                          jobcnt = 0;
    int                          jobtype = 0;
    int                          idx_jobcnt = 0;
    char*                        name = NULL;
    xmanager_metricnode*         pxmetricnode = NULL;
    xmanager_metricnode*         jobxmetricnode = NULL;
    xmanager_metricnode          xmetricnode = {0};
    xmanager_metricprogressnode* xmetricprogressnode = NULL;

    /* For capture-integrate path validation */
    xmanager_metricnode*         capturenode = NULL;
    xmanager_metricnode*         integratenode = NULL;
    char                         capture_data[1024] = {'\0'};
    char                         integrate_traildir[1024] = {'\0'};
    int                          capture_data_len = 0;
    int                          integrate_traildir_len = 0;

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

    /* Validate: exactly one capture and one integrate */
    if (2 != jobcnt)
    {
        elog(RLOG_WARNING, "xmanager create progress command, need exactly one capture and one integrate");
        xmanager_metricprogressnode_destroy((xmanager_metricnode*)xmetricprogressnode);
        return false;
    }

    for (idx_jobcnt = 0; idx_jobcnt < jobcnt; idx_jobcnt++)
    {
        /* jobtype */
        rmemcpy1(&jobtype, 0, uptr, 4);
        jobtype = r_ntoh32(jobtype);
        uptr += 4;

        if (XMANAGER_METRICNODETYPE_INTEGRATE < jobtype)
        {
            elog(RLOG_WARNING, "xmanager recv create progress command, need jobtype less then PGRECEIVELOG");
            xmanager_metricprogressnode_destroy((xmanager_metricnode*)xmetricprogressnode);
            return false;
        }

        /* Only allow CAPTURE and INTEGRATE types */
        if (XMANAGER_METRICNODETYPE_CAPTURE != jobtype && XMANAGER_METRICNODETYPE_INTEGRATE != jobtype)
        {
            elog(RLOG_WARNING, "xmanager create progress command, only support capture and integrate");
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

        /* Check if it already exists */
        xmetricnode.type = jobtype;
        xmetricnode.name = name;

        pxmetricnode = dlist_get(xmetric->metricnodes, &xmetricnode, xmanager_metricnode_cmp);
        if (NULL == pxmetricnode)
        {
            elog(RLOG_WARNING, "xmanager recv create progress command, not find %d.%s", jobtype, name);
            xmanager_metricprogressnode_destroy((xmanager_metricnode*)xmetricprogressnode);
            return false;
        }

        /* Validate based on job type */
        if (XMANAGER_METRICNODETYPE_CAPTURE == jobtype)
        {
            /* Check if capture already exists */
            if (NULL != capturenode)
            {
                elog(RLOG_WARNING, "xmanager create progress command, only one capture allowed");
                xmanager_metricprogressnode_destroy((xmanager_metricnode*)xmetricprogressnode);
                return false;
            }

            /* Validate capture.data is not empty (trail write directory) */
            if (NULL == pxmetricnode->data || '\0' == pxmetricnode->data[0])
            {
                elog(RLOG_WARNING, "xmanager create progress command, capture %s data is empty", name);
                xmanager_metricprogressnode_destroy((xmanager_metricnode*)xmetricprogressnode);
                return false;
            }

            /* Validate capture.conf is not empty */
            if (NULL == pxmetricnode->conf || '\0' == pxmetricnode->conf[0])
            {
                elog(RLOG_WARNING, "xmanager create progress command, capture %s conf is empty", name);
                xmanager_metricprogressnode_destroy((xmanager_metricnode*)xmetricprogressnode);
                return false;
            }

            /* NOTE: capture.traildir is NOT validated - it's always NULL for capture jobs */

            /* Store capture data path for validation */
            capturenode = pxmetricnode;
            capture_data_len = strlen(pxmetricnode->data);
            if (capture_data_len >= sizeof(capture_data))
            {
                elog(RLOG_WARNING, "xmanager create progress command, capture data path too long");
                xmanager_metricprogressnode_destroy((xmanager_metricnode*)xmetricprogressnode);
                return false;
            }
            rmemcpy0(capture_data, 0, pxmetricnode->data, capture_data_len);

            /* Remove trailing slash for comparison */
            while (capture_data_len > 1 && '/' == capture_data[capture_data_len - 1])
            {
                capture_data[capture_data_len - 1] = '\0';
                capture_data_len--;
            }
        }
        else if (XMANAGER_METRICNODETYPE_INTEGRATE == jobtype)
        {
            /* Check if integrate already exists */
            if (NULL != integratenode)
            {
                elog(RLOG_WARNING, "xmanager create progress command, only one integrate allowed");
                xmanager_metricprogressnode_destroy((xmanager_metricnode*)xmetricprogressnode);
                return false;
            }

            /* Validate integrate.data is not empty */
            if (NULL == pxmetricnode->data || '\0' == pxmetricnode->data[0])
            {
                elog(RLOG_WARNING, "xmanager create progress command, integrate %s data is empty", name);
                xmanager_metricprogressnode_destroy((xmanager_metricnode*)xmetricprogressnode);
                return false;
            }

            /* Validate integrate.conf is not empty */
            if (NULL == pxmetricnode->conf || '\0' == pxmetricnode->conf[0])
            {
                elog(RLOG_WARNING, "xmanager create progress command, integrate %s conf is empty", name);
                xmanager_metricprogressnode_destroy((xmanager_metricnode*)xmetricprogressnode);
                return false;
            }

            /* Validate integrate.traildir is not empty (trail read directory) */
            if (NULL == pxmetricnode->traildir || '\0' == pxmetricnode->traildir[0])
            {
                elog(RLOG_WARNING, "xmanager create progress command, integrate %s traildir is empty", name);
                xmanager_metricprogressnode_destroy((xmanager_metricnode*)xmetricprogressnode);
                return false;
            }

            /* Store integrate traildir for validation */
            integratenode = pxmetricnode;
            integrate_traildir_len = strlen(pxmetricnode->traildir);
            if (integrate_traildir_len >= sizeof(integrate_traildir))
            {
                elog(RLOG_WARNING, "xmanager create progress command, integrate traildir path too long");
                xmanager_metricprogressnode_destroy((xmanager_metricnode*)xmetricprogressnode);
                return false;
            }
            rmemcpy0(integrate_traildir, 0, pxmetricnode->traildir, integrate_traildir_len);

            /* Remove trailing slash for comparison */
            while (integrate_traildir_len > 1 && '/' == integrate_traildir[integrate_traildir_len - 1])
            {
                integrate_traildir[integrate_traildir_len - 1] = '\0';
                integrate_traildir_len--;
            }
        }

        /* Create new metricnode */
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

    /* Validate capture-integrate path relationship */
    if (NULL == capturenode)
    {
        elog(RLOG_WARNING, "xmanager create progress command, capture not specified");
        xmanager_metricprogressnode_destroy((xmanager_metricnode*)xmetricprogressnode);
        return false;
    }

    if (NULL == integratenode)
    {
        elog(RLOG_WARNING, "xmanager create progress command, integrate not specified");
        xmanager_metricprogressnode_destroy((xmanager_metricnode*)xmetricprogressnode);
        return false;
    }

    /* Compare normalized paths */
    if (0 != strcmp(capture_data, integrate_traildir))
    {
        elog(RLOG_WARNING,
             "xmanager create progress command, path mismatch: capture.data=%s, integrate.traildir=%s",
             capture_data,
             integrate_traildir);
        xmanager_metricprogressnode_destroy((xmanager_metricnode*)xmetricprogressnode);
        return false;
    }

    /* Set to initialized state */
    xmetricprogressnode->base.stat = XMANAGER_METRICNODESTAT_ONLINE;

    /* Add xmetricnode to the linked list */
    xmetric->metricnodes = dlist_put(xmetric->metricnodes, (void*)&xmetricprogressnode->base);

    /* Persist metricnode to disk */
    xmanager_metricnode_flush(xmetric->metricnodes);
    return true;
}

/*
 * Process create command
 *  1. jobtype must be less than ALL
 *  2. Verify if job already exists
 *  3. Add job to xmetric->metricnodes
 */
bool xmanager_metricmsg_parsecreate(xmanager_metric* xmetric, netpoolentry* npoolentry, netpacket* npacket)
{
    /* Error code */
    int                  errcode = 0;
    int                  len = 0;
    int                  jobtype = 0;
    uint8*               uptr = NULL;
    char*                jobname = NULL;
    xmanager_metricnode* pxmetricnode = NULL;
    xmanager_metricnode  xmetricnode = {0};
    char                 errormsg[512] = {0};
    char                 srcpath[1024] = {0};
    char                 dstpath[1024] = {0};

    /* Get job type */
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

    /* Check if it already exists */
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
        if (false == xmanager_metricmsg_parsecreateprocess(xmetric, npoolentry, uptr, jobname))
        {
            snprintf(errormsg, 512, "xmanager create process command error ");
            errcode = ERROR_MSGCOMMAND;
            goto xmanager_metricmsg_parsecreate_error;
        }
        return xmanager_metricmsg_assemblecmdresult(xmetric, npoolentry, XMANAGER_MSG_CREATECMD);
    }

    /* Generate new configuration file */
    snprintf(srcpath, 1024, "%s/sample/%s.cfg.sample", xmetric->configpath, xmanager_metricnode_getname(jobtype));

    snprintf(dstpath, 1024, "%s/%s_%s.cfg", xmetric->configpath, xmanager_metricnode_getname(jobtype), jobname);

    if (false == xmanager_metricmsg_parsecreateinplacejobname(srcpath, dstpath, jobname))
    {
        snprintf(errormsg, 512, "%s", "xmanager create command generate config file error");
        errcode = ERROR_MSGCOMMAND;
        goto xmanager_metricmsg_parsecreate_error;
    }

    /* Create new metricnode */
    pxmetricnode = xmanager_metricnode_init(jobtype);
    if (NULL == pxmetricnode)
    {
        snprintf(errormsg, 512, "%s", "xmanager create command, oom");
        errcode = ERROR_OOM;
        goto xmanager_metricmsg_parsecreate_error;
    }

    pxmetricnode->name = jobname;
    jobname = NULL;

    /* Set config information */
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

    /* Set to initialized state */
    pxmetricnode->stat = XMANAGER_METRICNODESTAT_INIT;

    /* Add xmetricnode to the linked list */
    xmetric->metricnodes = dlist_put(xmetric->metricnodes, pxmetricnode);

    /* Persist metricnode to disk */
    xmanager_metricnode_flush(xmetric->metricnodes);

    /* Build response message */
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
