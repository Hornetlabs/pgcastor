#include "app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "port/net/net.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/conn/conn.h"
#include "utils/dttime/dttime.h"
#include "utils/dttime/dttimestamp.h"
#include "command/cmd.h"
#include "queue/queue.h"
#include "net/netiomp/netiomp.h"
#include "net/netiomp/netiomp_poll.h"
#include "net/netpacket/netpacket.h"
#include "net/netpool.h"
#include "xmanager/xmanager_msg.h"
#include "xmanager/xmanager_metricnode.h"
#include "xmanager/xmanager_metriccapturenode.h"
#include "xmanager/xmanager_metricintegratenode.h"
#include "xmanager/xmanager_metric.h"
#include "xmanager/xmanager_metricprogressnode.h"

/* Initialize node */
xmanager_metricnode* xmanager_metricprogressnode_init(void)
{
    xmanager_metricprogressnode* xprogressmetricnode = NULL;

    xprogressmetricnode = rmalloc0(sizeof(xmanager_metricprogressnode));
    if (NULL == xprogressmetricnode)
    {
        elog(RLOG_WARNING, "xmanager metric progress node init out of memory");
        return NULL;
    }
    rmemset0(xprogressmetricnode, 0, '\0', sizeof(xmanager_metricprogressnode));

    xmanager_metricnode_reset(&xprogressmetricnode->base);
    xprogressmetricnode->base.type = XMANAGER_METRICNODETYPE_PROCESS;
    xprogressmetricnode->progressjop = NULL;
    return (xmanager_metricnode*)xprogressmetricnode;
}

/* Free metric progress node memory */
void xmanager_metricprogressnode_destroy(xmanager_metricnode* metricnode)
{
    xmanager_metricprogressnode* xprogressmetricnode = NULL;
    if (NULL == metricnode)
    {
        return;
    }

    xprogressmetricnode = (xmanager_metricprogressnode*)metricnode;

    dlist_free(xprogressmetricnode->progressjop, xmanager_metricnode_destroyvoid);

    rfree(metricnode);
}

/* Serialize progress node */
bool xmanager_metricprogressnode_serial(xmanager_metricnode* metricnode, uint8** blk, int* blksize, int* blkstart)
{
    bool                         bnew = false;
    int                          len = 0;
    int                          freespace = 0;
    int                          ivalue = 0;
    int                          jobcnt = 0;
    uint8*                       uptr = NULL;
    dlistnode*                   dlnode = NULL;
    xmanager_metricnode*         xmetricnode = NULL;
    xmanager_metricprogressnode* xmetricprogressnode = NULL;

    if (NULL == metricnode)
    {
        return true;
    }

    xmetricprogressnode = (xmanager_metricprogressnode*)metricnode;

    /* Total length of node */
    len = 4;

    /*
     * Calculate total length
     *  1、metricnode length
     *  2、progressnode length
     */
    /* metricnode length */
    len += xmanager_metricnode_serialsize(metricnode);

    /* TODO: progress node private length */

    /* job cnt */
    len += 4;

    for (dlnode = xmetricprogressnode->progressjop->head; NULL != dlnode; dlnode = dlnode->next)
    {
        xmetricnode = (xmanager_metricnode*)dlnode->value;

        /* jobtype */
        len += 4;

        /* jobnamelen 4 */
        len += 4;

        /* jobname */
        if (NULL != xmetricnode->name && '\0' != xmetricnode->name[0])
        {
            len += strlen(xmetricnode->name);
        }

        /* conflen 4 */
        len += 4;

        /* conf */
        if (NULL != xmetricnode->conf && '\0' != xmetricnode->conf[0])
        {
            len += strlen(xmetricnode->conf);
        }

        jobcnt++;
    }

    /* Check if space is sufficient, allocate more if needed */
    uptr = *blk;
    freespace = *blksize - *blkstart;
    while (len > freespace)
    {
        /* Reallocate space */
        bnew = true;
        *blksize = (*blksize) * 2;
        freespace = *blksize - *blkstart;
        continue;
    }

    if (true == bnew)
    {
        uptr = rrealloc0(uptr, *blksize);
        if (NULL == uptr)
        {
            elog(RLOG_WARNING, "xmanager progress node serial error, out of memory");
            return false;
        }
        rmemset0(uptr, *blkstart, '\0', *blksize - *blkstart);
        *blk = uptr;
    }

    /* Serialize total length */
    uptr += *blkstart;
    ivalue = len;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    *blkstart += 4;

    /* Reset to head */
    uptr = *blk;

    /* Format common content */
    xmanager_metricnode_serial(&xmetricprogressnode->base, uptr, blkstart);

    /* Serialize progress node content */
    uptr += *blkstart;

    /* jobcnt */
    ivalue = jobcnt;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    *blkstart += 4;

    for (dlnode = xmetricprogressnode->progressjop->head; NULL != dlnode; dlnode = dlnode->next)
    {
        xmetricnode = (xmanager_metricnode*)dlnode->value;

        /* jobtype */
        ivalue = xmetricnode->type;
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;
        *blkstart += 4;

        /* jobnamelen 4 + jobname */
        uptr += 4;
        if (NULL == xmetricnode->name || '\0' == xmetricnode->name[0])
        {
            ivalue = len = 0;
        }
        else
        {
            len = ivalue = strlen(xmetricnode->name);
            rmemcpy1(uptr, 0, xmetricnode->name, ivalue);
        }
        uptr -= 4;
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;
        uptr += len;
        *blkstart += 4;
        *blkstart += len;

        /* conflen 4 + conf */
        uptr += 4;
        if (NULL == xmetricnode->conf || '\0' == xmetricnode->conf[0])
        {
            ivalue = len = 0;
        }
        else
        {
            len = ivalue = strlen(xmetricnode->conf);
            rmemcpy1(uptr, 0, xmetricnode->conf, ivalue);
        }
        uptr -= 4;
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;
        uptr += len;
        *blkstart += 4;
        *blkstart += len;
    }

    return true;
}

/* Deserialize to progress node */
xmanager_metricnode* xmanager_metricprogressnode_deserial(uint8* blk, int* blkstart)
{
    int                          ivalue = 0;
    int                          jobcnt = 0;
    int                          jobtype = 0;
    int                          idx_jobcnt = 0;
    char*                        jobname = NULL;
    uint8*                       uptr = NULL;
    xmanager_metricnode*         metricnode = NULL;
    xmanager_metricprogressnode* xmetricprogressnode = NULL;

    xmetricprogressnode = (xmanager_metricprogressnode*)xmanager_metricprogressnode_init();
    if (NULL == xmetricprogressnode)
    {
        elog(RLOG_WARNING, "xmanager metric progress deserial error, out of memory");
        return NULL;
    }

    /* Get base information */
    if (false == xmanager_metricnode_deserial(&xmetricprogressnode->base, blk, blkstart))
    {
        elog(RLOG_WARNING, "xmanager metric progress deserial error");
        xmanager_metricnode_destroy(&xmetricprogressnode->base);
        return NULL;
    }
    uptr = blk;
    uptr += *blkstart;

    /* jobcnt */
    rmemcpy1(&ivalue, 0, uptr, 4);
    jobcnt = r_ntoh32(ivalue);
    uptr += 4;
    *blkstart += 4;

    for (idx_jobcnt = 0; idx_jobcnt < jobcnt; idx_jobcnt++)
    {
        /* jobtype */
        rmemcpy1(&ivalue, 0, uptr, 4);
        jobtype = r_ntoh32(ivalue);
        uptr += 4;
        *blkstart += 4;

        /* jobnamelen */
        rmemcpy1(&ivalue, 0, uptr, 4);
        ivalue = r_ntoh32(ivalue);
        uptr += 4;
        *blkstart += 4;
        if (0 == ivalue)
        {
            continue;
        }

        jobname = (char*)rmalloc0(ivalue + 1);
        if (NULL == jobname)
        {
            elog(RLOG_WARNING, "xmanager metric progress deserial error, jobname out of memory");
            xmanager_metricnode_destroy(&xmetricprogressnode->base);
            return NULL;
        }
        rmemset0(jobname, 0, '\0', ivalue + 1);

        rmemcpy0(jobname, 0, uptr, ivalue);
        uptr += ivalue;
        *blkstart += ivalue;

        metricnode = xmanager_metricnode_init(jobtype);
        if (NULL == metricnode)
        {
            elog(RLOG_WARNING, "xmanager metric progress deserial error, out of memory");
            xmanager_metricnode_destroy(&xmetricprogressnode->base);
            return NULL;
        }
        metricnode->name = jobname;
        jobname = NULL;

        /* conflen */
        rmemcpy1(&ivalue, 0, uptr, 4);
        ivalue = r_ntoh32(ivalue);
        uptr += 4;
        *blkstart += 4;
        if (0 == ivalue)
        {
            xmetricprogressnode->progressjop = dlist_put(xmetricprogressnode->progressjop, metricnode);
            metricnode = NULL;
            continue;
        }

        metricnode->conf = (char*)rmalloc0(ivalue + 1);
        if (NULL == metricnode->conf)
        {
            elog(RLOG_WARNING, "xmanager metric progress deserial error, jobname out of memory");
            xmanager_metricnode_destroy(&xmetricprogressnode->base);
            return NULL;
        }
        rmemset0(metricnode->conf, 0, '\0', ivalue + 1);

        rmemcpy0(metricnode->conf, 0, uptr, ivalue);
        uptr += ivalue;
        *blkstart += ivalue;
        xmetricprogressnode->progressjop = dlist_put(xmetricprogressnode->progressjop, metricnode);
        metricnode = NULL;
    }

    return (xmanager_metricnode*)xmetricprogressnode;
}

/*
 * Format bytes to human readable format (GB/MB/KB/B)
 * Rules:
 *   - Auto-select largest unit
 *   - KB and above: keep 2 decimal places
 *   - Example: 1025B -> "1.00KB", 3758096384B -> "3.50GB"
 */
static void xmanager_format_lsnlag(uint64 bytes, char* buffer, size_t bufsize)
{
    const uint64 KB = 1024;
    const uint64 MB = 1024 * 1024;
    const uint64 GB = 1024 * 1024 * 1024;

    if (bytes >= GB)
    {
        double value = (double)bytes / GB;
        snprintf(buffer, bufsize, "%.2fGB", value);
    }
    else if (bytes >= MB)
    {
        double value = (double)bytes / MB;
        snprintf(buffer, bufsize, "%.2fMB", value);
    }
    else if (bytes >= KB)
    {
        double value = (double)bytes / KB;
        snprintf(buffer, bufsize, "%.2fKB", value);
    }
    else
    {
        snprintf(buffer, bufsize, "%luB", bytes);
    }
}

/*
 * Calculate delay information between capture and integrate
 *
 * Delay calculation:
 *   - LSN Lag: capture.flushlsn - integrate.synclsn (formatted as GB/MB/KB/B)
 *   - Trail Lag: capture.trailno - integrate.synctrailno
 *   - Time Lag: capture.flushtimestamp - integrate.synctimestamp (formatted as Xh Xm Xs)
 *
 * Requirement: One-to-one relationship only (one capture, one integrate)
 */
void* xmanager_metricmsg_assembleprogress(xmanager_metric* xmetric, xmanager_metricnode* pxmetricnode)
{
    uint8                         u8value = 0;
    uint16                        u16value = 0;
    int                           rowlen = 0;
    int                           msglen = 0;
    int                           ivalue = 0;
    int                           len = 0;
    uint8*                        rowuptr = NULL;
    uint8*                        uptr = NULL;
    netpacket*                    npacket = NULL;
    dlistnode*                    dlnode = NULL;
    xmanager_metricnode*          capturenode = NULL;
    xmanager_metricnode*          integratenode = NULL;
    xmanager_metriccapturenode*   capturemetric = NULL;
    xmanager_metricintegratenode* integratemetric = NULL;
    xmanager_metricprogressnode*  xmetricprogressnode = NULL;
    xmanager_metricnode           tmpmetricnode = {0};

    /* Delay data */
    char                          lsnlag[64] = {'\0'};
    char                          traillag[64] = {'\0'};
    char                          timelag[128] = {'\0'};
    uint64                        lsnlag_value = 0;
    uint64                        traillag_value = 0;
    int64                         timelag_value = 0;
    int64                         seconds = 0;
    int64                         microseconds = 0;

    xmetricprogressnode = (xmanager_metricprogressnode*)pxmetricnode;

    if (true == dlist_isnull(xmetricprogressnode->progressjop))
    {
        elog(RLOG_WARNING, "xmanager progress job is null");
        return NULL;
    }

    /* Find capture and integrate nodes */
    for (dlnode = xmetricprogressnode->progressjop->head; NULL != dlnode; dlnode = dlnode->next)
    {
        xmanager_metricnode* pmetricnode = (xmanager_metricnode*)dlnode->value;

        if (XMANAGER_METRICNODETYPE_CAPTURE == pmetricnode->type)
        {
            /* Ensure only one capture is allowed */
            if (NULL != capturenode)
            {
                elog(RLOG_WARNING,
                     "progress %s multiple capture not allowed, only one-to-one supported",
                     xmetricprogressnode->base.name);
                return NULL;
            }
            capturenode = pmetricnode;
        }
        else if (XMANAGER_METRICNODETYPE_INTEGRATE == pmetricnode->type)
        {
            /* Ensure only one integrate is allowed */
            if (NULL != integratenode)
            {
                elog(RLOG_WARNING,
                     "progress %s multiple integrate not allowed, only one-to-one supported",
                     xmetricprogressnode->base.name);
                return NULL;
            }
            integratenode = pmetricnode;
        }
    }

    /* Both capture and integrate must exist */
    if (NULL == capturenode)
    {
        elog(RLOG_WARNING, "progress %s not find capture", xmetricprogressnode->base.name);
        return NULL;
    }

    if (NULL == integratenode)
    {
        elog(RLOG_WARNING, "progress %s not find integrate", xmetricprogressnode->base.name);
        return NULL;
    }

    /* Get actual metricnode for capture (with runtime metrics) */
    tmpmetricnode.type = XMANAGER_METRICNODETYPE_CAPTURE;
    tmpmetricnode.name = capturenode->name;

    capturenode = dlist_get(xmetric->metricnodes, &tmpmetricnode, xmanager_metricnode_cmp);
    if (NULL == capturenode)
    {
        elog(RLOG_WARNING,
             "progress %s capture %s not found in metricnodes",
             xmetricprogressnode->base.name,
             tmpmetricnode.name);
        return NULL;
    }

    /* Check if capture is running */
    if (XMANAGER_METRICNODESTAT_ONLINE > capturenode->stat)
    {
        elog(RLOG_WARNING, "progress %s capture %s not running", xmetricprogressnode->base.name, capturenode->name);
        return NULL;
    }

    /* Get actual metricnode for integrate (with runtime metrics) */
    tmpmetricnode.type = XMANAGER_METRICNODETYPE_INTEGRATE;
    tmpmetricnode.name = integratenode->name;

    integratenode = dlist_get(xmetric->metricnodes, &tmpmetricnode, xmanager_metricnode_cmp);
    if (NULL == integratenode)
    {
        elog(RLOG_WARNING,
             "progress %s integrate %s not found in metricnodes",
             xmetricprogressnode->base.name,
             tmpmetricnode.name);
        return NULL;
    }

    /* Check if integrate is running */
    if (XMANAGER_METRICNODESTAT_ONLINE > integratenode->stat)
    {
        elog(RLOG_WARNING, "progress %s integrate %s not running", xmetricprogressnode->base.name, integratenode->name);
        return NULL;
    }

    /* Cast to get detailed metrics */
    capturemetric = (xmanager_metriccapturenode*)capturenode;
    integratemetric = (xmanager_metricintegratenode*)integratenode;

    /* Calculate LSN Lag */
    if (capturemetric->flushlsn >= integratemetric->synclsn)
    {
        lsnlag_value = capturemetric->flushlsn - integratemetric->synclsn;
        xmanager_format_lsnlag(lsnlag_value, lsnlag, sizeof(lsnlag));
    }
    else
    {
        snprintf(lsnlag, sizeof(lsnlag), "N/A");
    }

    /* Calculate Trail Lag */
    if (capturemetric->trailno >= integratemetric->synctrailno)
    {
        traillag_value = capturemetric->trailno - integratemetric->synctrailno;
        snprintf(traillag, sizeof(traillag), "%lu", traillag_value);
    }
    else
    {
        snprintf(traillag, sizeof(traillag), "N/A");
    }

    /* Calculate Time Lag */
    if (capturemetric->flushtimestamp > 0 && integratemetric->synctimestamp > 0)
    {
        timelag_value = capturemetric->flushtimestamp - integratemetric->synctimestamp;

        /* Convert to human readable format */
        seconds = timelag_value / 1000000;
        microseconds = timelag_value % 1000000;

        if (seconds >= 3600)
        {
            snprintf(timelag, sizeof(timelag), "%ldh %ldm %lds", seconds / 3600, (seconds % 3600) / 60, seconds % 60);
        }
        else if (seconds >= 60)
        {
            snprintf(timelag, sizeof(timelag), "%ldm %lds", seconds / 60, seconds % 60);
        }
        else
        {
            snprintf(timelag, sizeof(timelag), "%lds %ldus", seconds, microseconds);
        }
    }
    else
    {
        snprintf(timelag, sizeof(timelag), "N/A");
    }

    /* Build response message */
    /* 4(total) + 4(crc) + 4(msgtype) + 1(success) + 4(rowcnt) + 4(rowlen) */
    msglen = 4 + 4 + 4 + 1 + 4 + 4;

    /* Column names */
    msglen += (4 + strlen("name"));
    msglen += (4 + strlen("capture"));
    msglen += (4 + strlen("integrate"));
    msglen += (4 + strlen("lsnlag"));
    msglen += (4 + strlen("traillag"));
    msglen += (4 + strlen("timelag"));

    /* Row data: rowlen(4) + nullmapcnt(2) + nullmap(1) */
    rowlen = 4 + 2 + 1;

    /* name value */
    rowlen += 4 + strlen(xmetricprogressnode->base.name);

    /* capture value */
    rowlen += 4 + strlen(capturemetric->base.name);

    /* integrate value */
    rowlen += 4 + strlen(integratemetric->base.name);

    /* lsnlag value */
    rowlen += 4 + strlen(lsnlag);

    /* traillag value */
    rowlen += 4 + strlen(traillag);

    /* timelag value */
    rowlen += 4 + strlen(timelag);

    msglen += rowlen;

    /* Allocate memory */
    npacket = netpacket_init();
    if (NULL == npacket)
    {
        elog(RLOG_WARNING, "xmanager metric assemble progress msg out of memory");
        return NULL;
    }

    npacket->data = netpacket_data_init(msglen);
    if (NULL == npacket->data)
    {
        elog(RLOG_WARNING, "xmanager metric assemble progress msg data, out of memory");
        netpacket_destroy(npacket);
        return NULL;
    }

    /* Assemble data */
    uptr = npacket->data;

    /* Total length */
    uptr += 4;
    npacket->used += 4;

    /* crc32 */
    uptr += 4;
    npacket->used += 4;

    /* Message type */
    ivalue = XMANAGER_MSG_INFOCMD;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    npacket->used += 4;

    /* Success flag */
    u8value = 0;
    rmemcpy1(uptr, 0, &u8value, 1);
    uptr += 1;
    npacket->used += 1;

    /* Row count */
    ivalue = 2;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    npacket->used += 4;

    rowuptr = uptr;

    /* Skip row length */
    uptr += 4;
    rowlen = 4;
    npacket->used += 4;

    /* Column name: name */
    len = strlen("name");
    ivalue = r_hton32(len);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;
    rmemcpy1(uptr, 0, "name", len);
    uptr += len;
    rowlen += len;
    npacket->used += len;

    /* Column name: capture */
    len = strlen("capture");
    ivalue = r_hton32(len);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;
    rmemcpy1(uptr, 0, "capture", len);
    uptr += len;
    rowlen += len;
    npacket->used += len;

    /* Column name: integrate */
    len = strlen("integrate");
    ivalue = r_hton32(len);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;
    rmemcpy1(uptr, 0, "integrate", len);
    uptr += len;
    rowlen += len;
    npacket->used += len;

    /* Column name: lsnlag */
    len = strlen("lsnlag");
    ivalue = r_hton32(len);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;
    rmemcpy1(uptr, 0, "lsnlag", len);
    uptr += len;
    rowlen += len;
    npacket->used += len;

    /* Column name: traillag */
    len = strlen("traillag");
    ivalue = r_hton32(len);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;
    rmemcpy1(uptr, 0, "traillag", len);
    uptr += len;
    rowlen += len;
    npacket->used += len;

    /* Column name: timelag */
    len = strlen("timelag");
    ivalue = r_hton32(len);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;
    rmemcpy1(uptr, 0, "timelag", len);
    uptr += len;
    rowlen += len;
    npacket->used += len;

    /* Total row length for header row */
    rowlen = r_hton32(rowlen);
    rmemcpy1(rowuptr, 0, &rowlen, 4);

    /* ========== DATA ROW ========== */
    rowuptr = uptr;

    /* Skip row length for data row */
    uptr += 4;
    rowlen = 4;
    npacket->used += 4;

    /* Null column map count (2 bytes, matching capture/integrate format) */
    u16value = 1;
    u16value = r_hton16(u16value);
    rmemcpy1(uptr, 0, &u16value, 2);
    uptr += 2;
    rowlen += 2;
    npacket->used += 2;

    /* Null column map (1 byte for 6 columns, all not null) */
    u8value = 0;
    rmemcpy1(uptr, 0, &u8value, 1);
    uptr += 1;
    rowlen += 1;
    npacket->used += 1;

    /* Row data: name */
    len = strlen(xmetricprogressnode->base.name);
    ivalue = r_hton32(len);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;
    rmemcpy1(uptr, 0, xmetricprogressnode->base.name, len);
    uptr += len;
    rowlen += len;
    npacket->used += len;

    /* Row data: capture */
    len = strlen(capturemetric->base.name);
    ivalue = r_hton32(len);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;
    rmemcpy1(uptr, 0, capturemetric->base.name, len);
    uptr += len;
    rowlen += len;
    npacket->used += len;

    /* Row data: integrate */
    len = strlen(integratemetric->base.name);
    ivalue = r_hton32(len);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;
    rmemcpy1(uptr, 0, integratemetric->base.name, len);
    uptr += len;
    rowlen += len;
    npacket->used += len;

    /* Row data: lsnlag */
    len = strlen(lsnlag);
    ivalue = r_hton32(len);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;
    rmemcpy1(uptr, 0, lsnlag, len);
    uptr += len;
    rowlen += len;
    npacket->used += len;

    /* Row data: traillag */
    len = strlen(traillag);
    ivalue = r_hton32(len);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;
    rmemcpy1(uptr, 0, traillag, len);
    uptr += len;
    rowlen += len;
    npacket->used += len;

    /* Row data: timelag */
    len = strlen(timelag);
    ivalue = r_hton32(len);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;
    rmemcpy1(uptr, 0, timelag, len);
    uptr += len;
    rowlen += len;
    npacket->used += len;

    /* Total row length for data row */
    rowlen = r_hton32(rowlen);
    rmemcpy1(rowuptr, 0, &rowlen, 4);

    /* Total data length */
    ivalue = npacket->used;
    ivalue = r_hton32(ivalue);
    rmemcpy1(npacket->data, 0, &ivalue, 4);

    return npacket;
}
