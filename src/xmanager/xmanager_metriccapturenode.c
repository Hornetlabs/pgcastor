#include "app_incl.h"
#include "port/file/fd.h"
#include "port/net/net.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/dttime/dttimestamp.h"
#include "command/cmd.h"
#include "net/netpacket/netpacket.h"
#include "xmanager/xmanager_msg.h"
#include "xmanager/xmanager_metricnode.h"
#include "xmanager/xmanager_metriccapturenode.h"

/*
 * metriccapture column header length
 * Row length 4
 * namelen 4 + redolsn
 * namelen 4 + restartlsn
 * namelen 4 + confirmlsn
 * namelen 4 + loadlsn
 * namelen 4 + parselsn
 * namelen 4 + flushlsn
 * namelen 4 + trailno
 * namelen 4 + trailstart
 * namelen 4 + parsetimestamp
 * namelen 4 + parsetimestamp
 */
#define REFRESH_METRICCAPTURE_KEY_LEN                                                                              \
    (4 + 4 + strlen("redolsn") + 4 + strlen("restartlsn") + 4 + strlen("confirmlsn") + 4 + strlen("loadlsn") + 4 + \
     strlen("parselsn") + 4 + strlen("flushlsn") + 4 + strlen("trailno") + 4 + strlen("trailstart") + 4 +          \
     strlen("parsetimestamp") + 4 + strlen("parsetimestamp"))

/* Initialize node */
xmanager_metricnode* xmanager_metriccapturenode_init(void)
{
    xmanager_metriccapturenode* xcapturemetricnode = NULL;

    xcapturemetricnode = rmalloc0(sizeof(xmanager_metriccapturenode));
    if (NULL == xcapturemetricnode)
    {
        elog(RLOG_WARNING, "xmanager metric capture node init out of memory");
        return NULL;
    }
    rmemset0(xcapturemetricnode, 0, '\0', sizeof(xmanager_metriccapturenode));

    xmanager_metricnode_reset(&xcapturemetricnode->base);
    xcapturemetricnode->base.type = XMANAGER_METRICNODETYPE_CAPTURE;
    return (xmanager_metricnode*)xcapturemetricnode;
}

/* Clean up metric capture node memory */
void xmanager_metriccapturenode_destroy(xmanager_metricnode* metricnode)
{
    rfree(metricnode);
}

/* Serialize capture node */
bool xmanager_metriccapturenode_serial(xmanager_metricnode* metricnode, uint8** blk, int* blksize, int* blkstart)
{
    bool                        bnew = false;
    int                         len = 0;
    int                         freespace = 0;
    int                         ivalue = 0;
    int64                       i64value = 0;
    uint64                      uvalue = 0;
    uint8*                      uptr = NULL;
    xmanager_metriccapturenode* xmetriccapturenode = NULL;

    if (NULL == metricnode)
    {
        return true;
    }

    xmetriccapturenode = (xmanager_metriccapturenode*)metricnode;

    /* Total node length */
    len = 4;

    /*
     * Calculate total length
     *  1、metricnode length
     *  2、capturenode length
     */
    /* metricnode length */
    len += xmanager_metricnode_serialsize(metricnode);

    /* capture node private length */
    len += (8 + /* redolsn */
            8 + /* restartlsn */
            8 + /* confirmlsn */
            8 + /* loadlsn */
            8 + /* parselsn */
            8 + /* flushlsn */
            8 + /* trailno */
            8 + /* trailstart */
            8 + /* parsetimestamp */
            8 /* flushtimestamp */);

    /* Check space sufficient, allocate if needed */
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
            elog(RLOG_WARNING, "xmanager capture node serial error, out of memory");
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

    /* Re-point to header */
    uptr = *blk;

    /* Format common content */
    xmanager_metricnode_serial(&xmetriccapturenode->base, uptr, blkstart);

    /* Serialize capture node content */
    uptr += *blkstart;

    /* redolsn */
    uvalue = xmetriccapturenode->redolsn;
    uvalue = r_hton64(uvalue);
    rmemcpy1(uptr, 0, &uvalue, 8);
    uptr += 8;
    *blkstart += 8;

    /* restartlsn */
    uvalue = xmetriccapturenode->restartlsn;
    uvalue = r_hton64(uvalue);
    rmemcpy1(uptr, 0, &uvalue, 8);
    uptr += 8;
    *blkstart += 8;

    /* confirmlsn */
    uvalue = xmetriccapturenode->confirmlsn;
    uvalue = r_hton64(uvalue);
    rmemcpy1(uptr, 0, &uvalue, 8);
    uptr += 8;
    *blkstart += 8;

    /* loadlsn */
    uvalue = xmetriccapturenode->loadlsn;
    uvalue = r_hton64(uvalue);
    rmemcpy1(uptr, 0, &uvalue, 8);
    uptr += 8;
    *blkstart += 8;

    /* parselsn */
    uvalue = xmetriccapturenode->parselsn;
    uvalue = r_hton64(uvalue);
    rmemcpy1(uptr, 0, &uvalue, 8);
    uptr += 8;
    *blkstart += 8;

    /* flushlsn */
    uvalue = xmetriccapturenode->flushlsn;
    uvalue = r_hton64(uvalue);
    rmemcpy1(uptr, 0, &uvalue, 8);
    uptr += 8;
    *blkstart += 8;

    /* trailno */
    uvalue = xmetriccapturenode->trailno;
    uvalue = r_hton64(uvalue);
    rmemcpy1(uptr, 0, &uvalue, 8);
    uptr += 8;
    *blkstart += 8;

    /* trailstart */
    uvalue = xmetriccapturenode->trailstart;
    uvalue = r_hton64(uvalue);
    rmemcpy1(uptr, 0, &uvalue, 8);
    uptr += 8;
    *blkstart += 8;

    /* parsetimestamp */
    i64value = xmetriccapturenode->parsetimestamp;
    i64value = r_hton64(i64value);
    rmemcpy1(uptr, 0, &i64value, 8);
    uptr += 8;
    *blkstart += 8;

    /* flushtimestamp */
    i64value = xmetriccapturenode->flushtimestamp;
    i64value = r_hton64(i64value);
    rmemcpy1(uptr, 0, &i64value, 8);
    uptr += 8;
    *blkstart += 8;

    return true;
}

/* Deserialize to capture node */
xmanager_metricnode* xmanager_metriccapturenode_deserial(uint8* blk, int* blkstart)
{
    int64                       i64value = 0;
    uint64                      u64value = 0;
    uint8*                      uptr = NULL;
    xmanager_metriccapturenode* xmetriccapturenode = NULL;
    xmetriccapturenode = (xmanager_metriccapturenode*)xmanager_metriccapturenode_init();
    if (NULL == xmetriccapturenode)
    {
        elog(RLOG_WARNING, "xmanager metric capture deserial error, out of memory");
        return NULL;
    }

    /* Get basic info */
    if (false == xmanager_metricnode_deserial(&xmetriccapturenode->base, blk, blkstart))
    {
        elog(RLOG_WARNING, "xmanager metric capture deserial error");
        xmanager_metricnode_destroy(&xmetriccapturenode->base);
        return NULL;
    }
    uptr = blk;
    uptr += *blkstart;

    /* redolsn */
    rmemcpy1(&u64value, 0, uptr, 8);
    xmetriccapturenode->redolsn = r_ntoh64(u64value);
    uptr += 8;
    *blkstart += 8;

    /* restartlsn */
    rmemcpy1(&u64value, 0, uptr, 8);
    xmetriccapturenode->restartlsn = r_ntoh64(u64value);
    uptr += 8;
    *blkstart += 8;

    /* confirmlsn */
    rmemcpy1(&u64value, 0, uptr, 8);
    xmetriccapturenode->confirmlsn = r_ntoh64(u64value);
    uptr += 8;
    *blkstart += 8;

    /* loadlsn */
    rmemcpy1(&u64value, 0, uptr, 8);
    xmetriccapturenode->loadlsn = r_ntoh64(u64value);
    uptr += 8;
    *blkstart += 8;

    /* parselsn */
    rmemcpy1(&u64value, 0, uptr, 8);
    xmetriccapturenode->parselsn = r_ntoh64(u64value);
    uptr += 8;
    *blkstart += 8;

    /* flushlsn */
    rmemcpy1(&u64value, 0, uptr, 8);
    xmetriccapturenode->flushlsn = r_ntoh64(u64value);
    uptr += 8;
    *blkstart += 8;

    /* trailno */
    rmemcpy1(&u64value, 0, uptr, 8);
    xmetriccapturenode->trailno = r_ntoh64(u64value);
    uptr += 8;
    *blkstart += 8;

    /* trailstart */
    rmemcpy1(&u64value, 0, uptr, 8);
    xmetriccapturenode->trailstart = r_ntoh64(u64value);
    uptr += 8;
    *blkstart += 8;

    /* parsetimestamp */
    rmemcpy1(&i64value, 0, uptr, 8);
    xmetriccapturenode->parsetimestamp = r_ntoh64(i64value);
    uptr += 8;
    *blkstart += 8;

    /* flushtimestamp */
    rmemcpy1(&i64value, 0, uptr, 8);
    xmetriccapturenode->flushtimestamp = r_ntoh64(i64value);
    uptr += 8;
    *blkstart += 8;

    return (xmanager_metricnode*)xmetriccapturenode;
}

/* Assemble capture info */
void* xmanager_metricmsg_assemblecapture(xmanager_metricnode* pxmetricnode)
{
    uint8                       u8value = 0;
    uint16                      u16value = 0;
    int                         rowlen = 0;
    int                         msglen = 0;
    int                         ivalue = 0;
    size_t                      idx_col = 0;
    uint8*                      nullmap = NULL;
    uint8*                      rowuptr = NULL;
    uint8*                      uptr = NULL;
    netpacket*                  npacket = NULL;
    xmanager_metriccapturenode* xmetriccapturenode = NULL;
    char                        state[32] = {'\0'};
    char                        values[REFRESH_METRICCAPTURE_INFOCNT][128] = {{0}};
    int32                       valuelen[REFRESH_METRICCAPTURE_INFOCNT] = {0};

    xmetriccapturenode = (xmanager_metriccapturenode*)pxmetricnode;

    rmemset1(state, 0, 0, 32);
    if (XMANAGER_METRICNODESTAT_NOP == pxmetricnode->stat)
    {
        snprintf(state, 32, "%s", "NOP");
    }
    else if (XMANAGER_METRICNODESTAT_INIT == pxmetricnode->stat)
    {
        snprintf(state, 32, "%s", "INIT");
    }
    else if (XMANAGER_METRICNODESTAT_ONLINE == pxmetricnode->stat)
    {
        snprintf(state, 32, "%s", "ONLINE");
    }
    else if (XMANAGER_METRICNODESTAT_OFFLINE == pxmetricnode->stat)
    {
        snprintf(state, 32, "%s", "OFFLINE");
    }
    else
    {
        elog(RLOG_WARNING, "xmanager metric assemble info capture msg data, invalid metricnode stat");
        return NULL;
    }

    /* 4 total length + 4 crc32 + 4 msgtype + 1 success or fail + 4 rowcnt */
    msglen = 4 + 4 + 4 + 1 + 4;

    /* First row length info + state (4 + strlen(state)) */
    msglen += (REFRESH_METRICCAPTURE_KEY_LEN + 4 + strlen("state"));

    /* Row length */
    msglen += 4;

    /* nullmap count */
    msglen += 2;

    /* nullmap */
    msglen += 2;

    /* Calculate and save column value length */
    /* redolsn + len */
    msglen += 4;
    valuelen[0] = snprintf(values[0],
                           32,
                           "%X/%X",
                           (uint32)(xmetriccapturenode->redolsn >> 32),
                           (uint32)(xmetriccapturenode->redolsn));
    msglen += valuelen[0];

    /* restartlsn + len */
    msglen += 4;
    valuelen[1] = snprintf(values[1],
                           32,
                           "%X/%X",
                           (uint32)(xmetriccapturenode->restartlsn >> 32),
                           (uint32)(xmetriccapturenode->restartlsn));
    msglen += valuelen[1];

    /* confirmlsn + len */
    msglen += 4;
    valuelen[2] = snprintf(values[2],
                           32,
                           "%X/%X",
                           (uint32)(xmetriccapturenode->confirmlsn >> 32),
                           (uint32)(xmetriccapturenode->confirmlsn));
    msglen += valuelen[2];

    /* loadlsn + len */
    msglen += 4;
    valuelen[3] = snprintf(values[3],
                           32,
                           "%X/%X",
                           (uint32)(xmetriccapturenode->loadlsn >> 32),
                           (uint32)(xmetriccapturenode->loadlsn));
    msglen += valuelen[3];

    /* parselsn + len */
    msglen += 4;
    valuelen[4] = snprintf(values[4],
                           32,
                           "%X/%X",
                           (uint32)(xmetriccapturenode->parselsn >> 32),
                           (uint32)(xmetriccapturenode->parselsn));
    msglen += valuelen[4];

    /* flushlsn + len */
    msglen += 4;
    valuelen[5] = snprintf(values[5],
                           32,
                           "%X/%X",
                           (uint32)(xmetriccapturenode->flushlsn >> 32),
                           (uint32)(xmetriccapturenode->flushlsn));
    msglen += valuelen[5];

    /* trailno + len */
    msglen += 4;
    valuelen[6] = snprintf(values[6], 32, "%" PRIu64, xmetriccapturenode->trailno);
    msglen += valuelen[6];

    /* trailstart + len */
    msglen += 4;
    valuelen[7] = snprintf(values[7], 32, "%" PRIu64, xmetriccapturenode->trailstart);
    msglen += valuelen[7];

    /* parsetimestamp + len */
    msglen += 4;
    dt_timestamptz_to_string((TimestampTz)xmetriccapturenode->parsetimestamp, values[8]);
    valuelen[8] = strlen(values[8]);
    msglen += valuelen[8];

    /* flushtimestamp + len */
    msglen += 4;
    dt_timestamptz_to_string((TimestampTz)xmetriccapturenode->flushtimestamp, values[9]);
    valuelen[9] = strlen(values[9]);
    msglen += valuelen[9];

    /* state */
    msglen += 4;
    msglen += strlen(state);

    /* Allocate space */
    npacket = netpacket_init();
    if (NULL == npacket)
    {
        elog(RLOG_WARNING, "xmanager metric assemble info capture msg out of memory");
        return NULL;
    }
    msglen += 1;
    npacket->data = netpacket_data_init(msglen);
    if (NULL == npacket->data)
    {
        elog(RLOG_WARNING, "xmanager metric assemble info capture msg data, out of memory");
        netpacket_destroy(npacket);
        return NULL;
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
    ivalue = XMANAGER_MSG_STARTCMD;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* Type success flag */
    u8value = 0;
    rmemcpy1(uptr, 0, &u8value, 1);
    uptr += 1;

    /* rowcnt */
    ivalue = 2;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    rowlen = 0;
    rowuptr = uptr;

    /* Skip row length */
    uptr += 4;
    rowlen = 4;

    /* Assemble column header */
    /* redolsn len */
    ivalue = strlen("redolsn");
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;

    /* redolsn */
    ivalue = strlen("redolsn");
    rmemcpy1(uptr, 0, "redolsn", ivalue);
    uptr += ivalue;
    rowlen += ivalue;

    /* restartlsn len */
    ivalue = strlen("restartlsn");
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;

    /* redolsn */
    ivalue = strlen("restartlsn");
    rmemcpy1(uptr, 0, "restartlsn", ivalue);
    uptr += ivalue;
    rowlen += ivalue;

    /* confirmlsn len */
    ivalue = strlen("confirmlsn");
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;

    /* confirmlsn */
    ivalue = strlen("confirmlsn");
    rmemcpy1(uptr, 0, "confirmlsn", ivalue);
    uptr += ivalue;
    rowlen += ivalue;

    /* loadlsn len */
    ivalue = strlen("loadlsn");
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;

    /* loadlsn */
    ivalue = strlen("loadlsn");
    rmemcpy1(uptr, 0, "loadlsn", ivalue);
    uptr += ivalue;
    rowlen += ivalue;

    /* loadlsn len */
    ivalue = strlen("parselsn");
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;

    /* loadlsn */
    ivalue = strlen("parselsn");
    rmemcpy1(uptr, 0, "parselsn", ivalue);
    uptr += ivalue;
    rowlen += ivalue;

    /* flushlsn len */
    ivalue = strlen("flushlsn");
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;

    /* flushlsn */
    ivalue = strlen("flushlsn");
    rmemcpy1(uptr, 0, "flushlsn", ivalue);
    uptr += ivalue;
    rowlen += ivalue;

    /* trailno len */
    ivalue = strlen("trailno");
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;

    /* trailno */
    ivalue = strlen("trailno");
    rmemcpy1(uptr, 0, "trailno", ivalue);
    uptr += ivalue;
    rowlen += ivalue;

    /* trailstart len */
    ivalue = strlen("trailstart");
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;

    /* trailstart */
    ivalue = strlen("trailstart");
    rmemcpy1(uptr, 0, "trailstart", ivalue);
    uptr += ivalue;
    rowlen += ivalue;

    /* parsetimestamp len */
    ivalue = strlen("parsetimestamp");
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;

    /* parsetimestamp */
    ivalue = strlen("parsetimestamp");
    rmemcpy1(uptr, 0, "parsetimestamp", ivalue);
    uptr += ivalue;
    rowlen += ivalue;

    /* flushtimestamp len */
    ivalue = strlen("flushtimestamp");
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;

    /* flushtimestamp */
    ivalue = strlen("flushtimestamp");
    rmemcpy1(uptr, 0, "flushtimestamp", ivalue);
    uptr += ivalue;
    rowlen += ivalue;

    /* state len */
    ivalue = strlen("state");
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;

    /* state */
    ivalue = strlen("state");
    rmemcpy1(uptr, 0, "state", ivalue);
    uptr += ivalue;
    rowlen += ivalue;

    /* Total row length */
    rowlen = r_hton32(rowlen);
    rmemcpy1(rowuptr, 0, &rowlen, 4);

    rowlen = 0;
    rowuptr = uptr;

    /* Skip row length */
    uptr += 4;
    rowlen = 4;

    /* Null column map count */
    u16value = 2;
    u16value = r_hton16(u16value);
    rmemcpy1(uptr, 0, &u16value, 2);
    uptr += 2;
    rowlen += 2;

    /* Null column map */
    u16value = 2;
    uptr += u16value;
    rowlen += u16value;

    nullmap = rmalloc0(u16value);
    if (NULL == nullmap)
    {
        elog(RLOG_WARNING, "xmanager metric assemble info capture nullmap, out of memory");
        netpacket_destroy(npacket);
        return NULL;
    }
    rmemset0(nullmap, 0, 0, u16value);

    for (idx_col = 0; idx_col < REFRESH_METRICCAPTURE_INFOCNT; idx_col++)
    {
        if (values[idx_col][0] == '\0')
        {
            nullmap[idx_col / 8] |= (1U << (idx_col % 8));
            continue;
        }

        /* Column length */
        ivalue = valuelen[idx_col];
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;
        rowlen += 4;

        /* Column content */
        rmemcpy1(uptr, 0, values[idx_col], valuelen[idx_col]);
        uptr += valuelen[idx_col];
        rowlen += valuelen[idx_col];
    }

    /* state length */
    ivalue = strlen(state);
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;

    /* Column content */
    rmemcpy1(uptr, 0, state, strlen(state));
    uptr += strlen(state);
    rowlen += strlen(state);

    /* Total row length */
    rowlen = r_hton32(rowlen);
    rmemcpy1(rowuptr, 0, &rowlen, 4);
    rowuptr += 4;

    rowuptr += 2;
    rmemcpy1(rowuptr, 0, nullmap, u16value);

    rfree(nullmap);
    nullmap = NULL;

    return (void*)npacket;
}
