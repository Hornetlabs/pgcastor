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
bool xmanager_metricprogressnode_serial(xmanager_metricnode* metricnode, uint8** blk, int* blksize,
                                        int* blkstart)
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
            xmetricprogressnode->progressjop =
                dlist_put(xmetricprogressnode->progressjop, metricnode);
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

/* Get key-value pairs from configuration file */
static bool xmanager_metricprogressnode_getdatafromcfgfile(const char* config_file, char* key,
                                                           char* data)
{
    FILE* fp = NULL;
    char  fline[1024];

    fp = osal_file_fopen(config_file, "rb");
    if (!fp)
    {
        elog(RLOG_WARNING, "could not open configuration file:%s %s", config_file, strerror(errno));
        return false;
    }

    /* Read a line of data */
    rmemset1(fline, 0, '\0', sizeof(fline));
    while (osal_file_fgets(fp, sizeof(fline), fline) != NULL)
    {
        bool  quota = false;
        char* uptr = fline;
        int   pos = 0;
        int   len = 0;

        /* Skip leading whitespace characters */
        while ('\0' != *uptr)
        {
            if (' ' != *uptr && '\t' != *uptr && '\r' != *uptr && '\n' != *uptr)
            {
                break;
            }
            uptr++;
            pos++;
        }

        /* Skip empty lines and comments */
        if ('\0' == *uptr || '#' == *uptr)
        {
            rmemset1(fline, 0, '\0', sizeof(fline));
            continue;
        }

        /* Get key */
        while ('\0' != *uptr)
        {
            if (' ' == *uptr || '\t' == *uptr || '\r' == *uptr || '\n' == *uptr || '=' == *uptr)
            {
                break;
            }
            len++;
            uptr++;
        }

        /* Get key/name */
        if (len != strlen(key) || 0 != memcmp(key, fline + pos, len))
        {
            rmemset1(fline, 0, '\0', sizeof(fline));
            continue;
        }
        pos += len;

        /* Get value */
        len = 0;

        /* Skip spaces, tabs, and newline characters */
        while ('\0' != *uptr)
        {
            if (' ' != *uptr && '\t' != *uptr && '\r' != *uptr && '\n' != *uptr)
            {
                break;
            }
            pos++;
            uptr++;
        }

        if ('=' != *uptr)
        {
            /* End of data */
            elog(RLOG_WARNING, "config data error");
            return false;
        }

        /* Get value */
        /* Skip '=' character */
        pos++;
        uptr++;

        /* Skip spaces and other characters */
        while ('\0' != *uptr)
        {
            if (' ' != *uptr && '\t' != *uptr && '\r' != *uptr && '\n' != *uptr)
            {
                break;
            }
            uptr++;
            pos++;
        }

        /* Skip empty lines and comment lines */
        if ('\0' == *uptr || '#' == *uptr)
        {
            /* End of data */
            elog(RLOG_WARNING, "config data error");
            return false;
        }

        /* Skip spaces and other characters */
        /* Check character type */
        if (*uptr == '"')
        {
            uptr++;
            pos++;
            quota = true;
            /* Get the next '"' character */
            while ('\0' != *uptr)
            {
                if ('"' == *uptr)
                {
                    quota = false;
                    break;
                }
                len++;
                uptr++;
            }

            if (true == quota)
            {
                elog(RLOG_WARNING,
                     "configuration data is incorrect, missing double quotation marks");
                return false;
            }
        }
        else
        {
            while ('\0' != *uptr)
            {
                if (' ' == *uptr || '\t' == *uptr || '\r' == *uptr || '\n' == *uptr)
                {
                    break;
                }
                len++;
                uptr++;
            }
        }

        len += 1;
        rmemset1(data, 0, 0, len);
        rmemcpy1(data, 0, fline + pos, len - 1);
        break;
    }
    return true;
}

/* Assemble progress capture info */
static void* xmanager_metricmsg_assembleprogresscapture(xmanager_metric*     xmetric,
                                                        xmanager_metricnode* xmetricnode,
                                                        dlist*               job)
{
    bool       find = false;
    uint8      u8value = 0;
    int        rowlen = 0;
    int        msglen = 0;
    int        ivalue = 0;
    int        rowcnt = 0;
    uint32     hi = 0;
    uint32     lo = 0;
    int64      dbtime = 0;
    uint8*     rowuptr = NULL;
    uint8*     uptr = NULL;
    PGconn*    conn = NULL;
    PGresult*  res = NULL;
    netpacket* npacket = NULL;
    char       conninfo[512] = {'\0'};
    char       sql_exec[1024] = {'\0'};

    rmemset1(conninfo, 0, 0, 512);
    xmanager_metricprogressnode_getdatafromcfgfile(xmetricnode->conf, CFG_KEY_URL, conninfo);

    conn = conn_get(conninfo);
    if (NULL == conn)
    {
        return NULL;
    }

    rmemset1(sql_exec, 0, '\0', 1024);
    sprintf(sql_exec, "SELECT pg_current_wal_lsn();");
    res = conn_exec(conn, sql_exec);
    if (NULL == res)
    {
        return NULL;
    }

    if (PQnfields(res) != 1 && PQntuples(res) != 1)
    {
        PQfinish(conn);
        PQclear(res);
        elog(RLOG_WARNING, "failed get excute SQL result: SELECT pg_current_wal_lsn()");
        return NULL;
    }

    /* Read data from redolsn in "%X/%X" format and assign values to hi/lo */
    if (sscanf(PQgetvalue(res, 0, 0), "%X/%X", &hi, &lo) != 2)
    {
        PQfinish(conn);
        PQclear(res);
        elog(RLOG_WARNING, " could not parse end position ");
        return NULL;
    }
    PQclear(res);

    rmemset1(sql_exec, 0, '\0', 1024);
    sprintf(sql_exec,
            "SELECT (EXTRACT(EPOCH\n"
            "FROM (CURRENT_TIMESTAMP - TIMESTAMPTZ '2000-01-01 00:00:00+00') ) * 1000000 )::int8\n"
            "AS pg_ts_usec;");
    res = conn_exec(conn, sql_exec);
    if (NULL == res)
    {
        return NULL;
    }

    if (PQnfields(res) != 1 && PQntuples(res) != 1)
    {
        PQfinish(conn);
        PQclear(res);
        elog(RLOG_WARNING, "failed get excute SQL result: SELECT CURRENT_TIMESTAMP");
        return NULL;
    }

    /* Read dbtime */
    if (sscanf(PQgetvalue(res, 0, 0), "%ld", &dbtime) != 1)
    {
        PQfinish(conn);
        PQclear(res);
        elog(RLOG_WARNING, "dbtime could not parse end position ");
        return NULL;
    }

    PQfinish(conn);
    PQclear(res);

    /* 4 total length + 4 crc32 + 4 msgtype + 1 success/failure + 4 rowcnt */
    msglen = 4 + 4 + 4 + 1 + 4;

    /* rowlen */
    msglen += 4;

    /* name */
    msglen += (4 + strlen("name"));

    /* lsnlag */
    msglen += (4 + strlen("lsnlag"));

    /* timelag */
    msglen += (4 + strlen("timelag"));

    /* traillag */
    msglen += (4 + strlen("traillag"));

    rowcnt = job->length;

    /* rowlen 4 + nullmapcnt 2 + nullmap 1 */
    rowlen += (4 + 2 + 1);

    /* name */
    rowlen += 4;
    rowlen += 128;

    /* lsnlag */
    rowlen += 4;
    rowlen += 32;

    /* timelag */
    rowlen += 4;
    rowlen += 128;

    /* traillag */
    rowlen += 4;
    rowlen += 32;

    msglen += (rowlen * (rowcnt - 1));

    /* Allocate memory */
    npacket = netpacket_init();
    if (NULL == npacket)
    {
        elog(RLOG_WARNING, "xmanager metric assemble info msg out of memory");
        return NULL;
    }
    msglen += 1;
    npacket->data = netpacket_data_init(msglen);
    if (NULL == npacket->data)
    {
        elog(RLOG_WARNING, "xmanager metric assemble info msg data, out of memory");
        netpacket_destroy(npacket);
        return NULL;
    }
    msglen -= 1;

    /* Assemble data */
    uptr = npacket->data;

    /* Total data length */
    uptr += 4;
    npacket->used += 4;

    /* crc32 */
    uptr += 4;
    npacket->used += 4;

    /* Type */
    ivalue = XMANAGER_MSG_STARTCMD;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    npacket->used += 4;

    /* Type success flag */
    u8value = 0;
    rmemcpy1(uptr, 0, &u8value, 1);
    uptr += 1;
    npacket->used += 1;

    /* rowcnt */
    ivalue = rowcnt;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    npacket->used += 4;

    rowlen = 0;
    rowuptr = uptr;

    /* Skip row length */
    uptr += 4;
    rowlen = 4;
    npacket->used += 4;

    /* Assemble column header */
    /* name len */
    ivalue = strlen("name");
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;

    /* name */
    ivalue = strlen("name");
    rmemcpy1(uptr, 0, "name", ivalue);
    uptr += ivalue;
    rowlen += ivalue;
    npacket->used += ivalue;

    /* lsnlag len */
    ivalue = strlen("lsnlag");
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;

    /* lsnlag */
    ivalue = strlen("lsnlag");
    rmemcpy1(uptr, 0, "lsnlag", ivalue);
    uptr += ivalue;
    rowlen += ivalue;
    npacket->used += ivalue;

    /* timelag len */
    ivalue = strlen("timelag");
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;

    /* timelag */
    ivalue = strlen("timelag");
    rmemcpy1(uptr, 0, "timelag", ivalue);
    uptr += ivalue;
    rowlen += ivalue;
    npacket->used += ivalue;

    /* traillag len */
    ivalue = strlen("traillag");
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;

    /* traillag */
    ivalue = strlen("traillag");
    rmemcpy1(uptr, 0, "traillag", ivalue);
    uptr += ivalue;
    rowlen += ivalue;
    npacket->used += ivalue;

    /* Row total length */
    rowlen = r_hton32(rowlen);
    rmemcpy1(rowuptr, 0, &rowlen, 4);

    if (false == find)
    {
        elog(RLOG_WARNING, "xmanager metric assemble progress msg no valid found");
        netpacket_destroy(npacket);
        return NULL;
    }

    /* Total data length */
    ivalue = npacket->used;
    ivalue = r_hton32(ivalue);
    rmemcpy1(npacket->data, 0, &ivalue, 4);

    return npacket;
}

/* Assemble progress info */
void* xmanager_metricmsg_assembleprogress(xmanager_metric*     xmetric,
                                          xmanager_metricnode* pxmetricnode)
{
    dlistnode*                   dlnode = NULL;
    xmanager_metricnode*         pmetricnode = NULL;
    xmanager_metricprogressnode* xmetricprogressnode = NULL;
    xmanager_metricnode          tmpmetricnode = {0};

    xmetricprogressnode = (xmanager_metricprogressnode*)pxmetricnode;

    if (true == dlist_isnull(xmetricprogressnode->progressjop))
    {
        elog(RLOG_WARNING, "xmanager progress job is null");
        return NULL;
    }

    for (dlnode = xmetricprogressnode->progressjop->head; NULL != dlnode; dlnode = dlnode->next)
    {
        pmetricnode = (xmanager_metricnode*)dlnode->value;
        if (XMANAGER_METRICNODETYPE_CAPTURE == pmetricnode->type)
        {
            break;
        }
        pmetricnode = NULL;
    }

    if (NULL == pmetricnode)
    {
        elog(RLOG_WARNING, "not find valid information in progress %s ",
             xmetricprogressnode->base.name);
        return NULL;
    }

    tmpmetricnode.name = pmetricnode->name;
    tmpmetricnode.type = pmetricnode->type;

    /* Get metricnode */
    pmetricnode = dlist_get(xmetric->metricnodes, &tmpmetricnode, xmanager_metricnode_cmp);
    if (NULL == pmetricnode)
    {
        elog(RLOG_WARNING, "not find valid progress job %s in metricnodes ", pmetricnode->name);
        return NULL;
    }

    /* Check if running, do not print info if not running */
    if (XMANAGER_METRICNODESTAT_ONLINE > pmetricnode->stat)
    {
        elog(RLOG_WARNING, "progress job %s not start", pmetricnode->name);
        return NULL;
    }

    if (XMANAGER_METRICNODETYPE_CAPTURE == pmetricnode->type)
    {
        return xmanager_metricmsg_assembleprogresscapture(xmetric, pmetricnode,
                                                          xmetricprogressnode->progressjop);
    }
    else
    {
        elog(RLOG_WARNING, "find invalid information in progress %s ",
             xmetricprogressnode->base.name);
        return NULL;
    }
}
