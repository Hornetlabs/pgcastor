#include "app_incl.h"
#include "utils/dlist/dlist.h"
#include "utils/dttime/dttimestamp.h"
#include "net/netpacket/netpacket.h"
#include "xmanager/xmanager_msg.h"
#include "xmanager/xmanager_metricnode.h"
#include "xmanager/xmanager_metricintegratenode.h"

/*
 * metricintegrate column header length
 * Row length 4
 * namelen 4 + loadlsn
 * namelen 4 + synclsn
 * namelen 4 + loadtrailno
 * namelen 4 + loadtrailstart
 * namelen 4 + synctrailno
 * namelen 4 + synctrailstart
 * namelen 4 + loadtimestamp
 * namelen 4 + synctimestamp
 */
#define REFRESH_METRICINTEGRATE_KEY_LEN                                                        \
    (4 + 4 + strlen("loadlsn") + 4 + strlen("synclsn") + 4 + strlen("loadtrailno") + 4 +       \
     strlen("loadtrailstart") + 4 + strlen("synctrailno") + 4 + strlen("synctrailstart") + 4 + \
     strlen("loadtimestamp") + 4 + strlen("synctimestamp"))

/* Initialize */
xmanager_metricnode* xmanager_metricintegratenode_init(void)
{
    xmanager_metricintegratenode* xintegratemetricnode = NULL;

    xintegratemetricnode = rmalloc0(sizeof(xmanager_metricintegratenode));
    if (NULL == xintegratemetricnode)
    {
        elog(RLOG_WARNING, "xmanager metric integrate node init out of memory");
        return NULL;
    }
    rmemset0(xintegratemetricnode, 0, '\0', sizeof(xmanager_metricintegratenode));

    xmanager_metricnode_reset(&xintegratemetricnode->base);
    xintegratemetricnode->base.type = XMANAGER_METRICNODETYPE_INTEGRATE;
    return (xmanager_metricnode*)xintegratemetricnode;
}

/* Clean up resources */
void xmanager_metricintegratenode_destroy(xmanager_metricnode* metricnode)
{
    rfree(metricnode);
}

/* Serialize integrate node */
bool xmanager_metricintegratenode_serial(xmanager_metricnode* metricnode,
                                         uint8**              blk,
                                         int*                 blksize,
                                         int*                 blkstart)
{
    bool                          bnew = false;
    int                           len = 0;
    int                           freespace = 0;
    int                           ivalue = 0;
    int64                         i64value = 0;
    uint64                        uvalue = 0;
    uint8*                        uptr = NULL;
    xmanager_metricintegratenode* xmetricintegratenode = NULL;

    if (NULL == metricnode)
    {
        return true;
    }

    xmetricintegratenode = (xmanager_metricintegratenode*)metricnode;

    /* Total node length */
    len = 4;

    /*
     * Calculate total length
     *  1. metricnode length
     *  2. integratenode length
     */
    /* metricnode length */
    len += xmanager_metricnode_serialsize(metricnode);

    /* integrate node private length */
    len += (8 + /*loadlsn */
            8 + /*synclsn */
            8 + /*loadtrailno */
            8 + /*loadtrailstart */
            8 + /*synctrailno */
            8 + /*synctrailstart */
            8 + /*loadtimestamp */
            8 /*synctimestamp */);

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
            elog(RLOG_WARNING, "xmanager integrate node serial error, out of memory");
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
    xmanager_metricnode_serial(&xmetricintegratenode->base, uptr, blkstart);

    /* Serialize integrate node content */
    uptr += *blkstart;

    /* loadlsn */
    uvalue = xmetricintegratenode->loadlsn;
    uvalue = r_hton64(uvalue);
    rmemcpy1(uptr, 0, &uvalue, 8);
    uptr += 8;
    *blkstart += 8;

    /* synclsn */
    uvalue = xmetricintegratenode->synclsn;
    uvalue = r_hton64(uvalue);
    rmemcpy1(uptr, 0, &uvalue, 8);
    uptr += 8;
    *blkstart += 8;

    /* loadtrailno */
    uvalue = xmetricintegratenode->loadtrailno;
    uvalue = r_hton64(uvalue);
    rmemcpy1(uptr, 0, &uvalue, 8);
    uptr += 8;
    *blkstart += 8;

    /* loadtrailstart */
    uvalue = xmetricintegratenode->loadtrailstart;
    uvalue = r_hton64(uvalue);
    rmemcpy1(uptr, 0, &uvalue, 8);
    uptr += 8;
    *blkstart += 8;

    /* synctrailno */
    uvalue = xmetricintegratenode->synctrailno;
    uvalue = r_hton64(uvalue);
    rmemcpy1(uptr, 0, &uvalue, 8);
    uptr += 8;
    *blkstart += 8;

    /* synctrailstart */
    uvalue = xmetricintegratenode->synctrailstart;
    uvalue = r_hton64(uvalue);
    rmemcpy1(uptr, 0, &uvalue, 8);
    uptr += 8;
    *blkstart += 8;

    /* loadtimestamp */
    i64value = xmetricintegratenode->loadtimestamp;
    i64value = r_hton64(i64value);
    rmemcpy1(uptr, 0, &i64value, 8);
    uptr += 8;
    *blkstart += 8;

    /* synctimestamp */
    i64value = xmetricintegratenode->synctimestamp;
    i64value = r_hton64(i64value);
    rmemcpy1(uptr, 0, &i64value, 8);
    uptr += 8;
    *blkstart += 8;

    return true;
}

/* Deserialize to integrate node */
xmanager_metricnode* xmanager_metricintegratenode_deserial(uint8* blk, int* blkstart)
{
    int64                         i64value = 0;
    uint64                        u64value = 0;
    uint8*                        uptr = NULL;
    xmanager_metricintegratenode* xmetricintegratenode = NULL;
    xmetricintegratenode = (xmanager_metricintegratenode*)xmanager_metricintegratenode_init();
    if (NULL == xmetricintegratenode)
    {
        elog(RLOG_WARNING, "xmanager metric integrate deserial error, out of memory");
        return NULL;
    }

    /* Get basic info */
    if (false == xmanager_metricnode_deserial(&xmetricintegratenode->base, blk, blkstart))
    {
        elog(RLOG_WARNING, "xmanager metric integrate deserial error");
        xmanager_metricnode_destroy(&xmetricintegratenode->base);
        return NULL;
    }
    uptr = blk;
    uptr += *blkstart;

    /* loadlsn */
    rmemcpy1(&u64value, 0, uptr, 8);
    xmetricintegratenode->loadlsn = r_ntoh64(u64value);
    uptr += 8;
    *blkstart += 8;

    /* synclsn */
    rmemcpy1(&u64value, 0, uptr, 8);
    xmetricintegratenode->synclsn = r_ntoh64(u64value);
    uptr += 8;
    *blkstart += 8;

    /* loadtrailno */
    rmemcpy1(&u64value, 0, uptr, 8);
    xmetricintegratenode->loadtrailno = r_ntoh64(u64value);
    uptr += 8;
    *blkstart += 8;

    /* loadtrailstart */
    rmemcpy1(&u64value, 0, uptr, 8);
    xmetricintegratenode->loadtrailstart = r_ntoh64(u64value);
    uptr += 8;
    *blkstart += 8;

    /* synctrailno */
    rmemcpy1(&u64value, 0, uptr, 8);
    xmetricintegratenode->synctrailno = r_ntoh64(u64value);
    uptr += 8;
    *blkstart += 8;

    /* synctrailstart */
    rmemcpy1(&u64value, 0, uptr, 8);
    xmetricintegratenode->synctrailstart = r_ntoh64(u64value);
    uptr += 8;
    *blkstart += 8;

    /* loadtimestamp */
    rmemcpy1(&i64value, 0, uptr, 8);
    xmetricintegratenode->loadtimestamp = r_ntoh64(i64value);
    uptr += 8;
    *blkstart += 8;

    /* synctimestamp */
    rmemcpy1(&i64value, 0, uptr, 8);
    xmetricintegratenode->synctimestamp = r_ntoh64(i64value);
    uptr += 8;
    *blkstart += 8;

    return (xmanager_metricnode*)xmetricintegratenode;
}

/* Assemble integrate info */
void* xmanager_metricmsg_assembleintegrate(xmanager_metricnode* pxmetricnode)
{
    uint8                         u8value = 0;
    uint16                        u16value = 0;
    int                           rowlen = 0;
    int                           msglen = 0;
    int                           ivalue = 0;
    size_t                        idx_col = 0;
    uint8*                        nullmap = NULL;
    uint8*                        rowuptr = NULL;
    uint8*                        uptr = NULL;
    netpacket*                    npacket = NULL;
    xmanager_metricintegratenode* xmetricintegratenode = NULL;
    char                          state[32] = {'\0'};
    char                          values[REFRESH_METRICINTEGRATE_INFOCNT][32] = {{0}};
    int32                         valuelen[REFRESH_METRICINTEGRATE_INFOCNT] = {0};

    xmetricintegratenode = (xmanager_metricintegratenode*)pxmetricnode;

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
        elog(RLOG_WARNING,
             "xmanager metric assemble info capture msg data, invalid metricnode stat");
        return NULL;
    }

    /* 4 total length + 4 crc32 + 4 msgtype + 1 success or fail + 4 rowcnt */
    msglen = 4 + 4 + 4 + 1 + 4;

    /* First row length info + state length */
    msglen += (REFRESH_METRICINTEGRATE_KEY_LEN + 4 + strlen("state"));

    /* Row length */
    msglen += 4;

    /* nullmap count */
    msglen += 2;

    /* nullmap */
    msglen += 2;

    /* Calculate column value length */

    /* loadlsn + len */
    msglen += 4;
    valuelen[0] = snprintf(values[0],
                           32,
                           "%X/%X",
                           (uint32)(xmetricintegratenode->loadlsn >> 32),
                           (uint32)(xmetricintegratenode->loadlsn));
    msglen += valuelen[0];

    /* synclsn + len */
    msglen += 4;
    valuelen[1] = snprintf(values[1],
                           32,
                           "%X/%X",
                           (uint32)(xmetricintegratenode->synclsn >> 32),
                           (uint32)(xmetricintegratenode->synclsn));
    msglen += valuelen[1];

    /* loadtrailno + len */
    msglen += 4;
    valuelen[2] = snprintf(values[2], 32, "%" PRIu64, xmetricintegratenode->loadtrailno);
    msglen += valuelen[2];

    /* loadtrailstart + len */
    msglen += 4;
    valuelen[3] = snprintf(values[3], 32, "%" PRIu64, xmetricintegratenode->loadtrailstart);
    msglen += valuelen[3];

    /* synctrailno + len */
    msglen += 4;
    valuelen[4] = snprintf(values[4], 32, "%" PRIu64, xmetricintegratenode->synctrailno);
    msglen += valuelen[4];

    /* synctrailstart + len */
    msglen += 4;
    valuelen[5] = snprintf(values[5], 32, "%" PRIu64, xmetricintegratenode->synctrailstart);
    msglen += valuelen[5];

    /* loadtimestamp + len */
    msglen += 4;
    dt_timestamptz_to_string((TimestampTz)xmetricintegratenode->loadtimestamp, values[6]);
    valuelen[6] = strlen(values[6]);
    msglen += valuelen[6];

    /* synctimestamp + len */
    msglen += 4;
    dt_timestamptz_to_string((TimestampTz)xmetricintegratenode->synctimestamp, values[7]);
    valuelen[7] = strlen(values[7]);
    msglen += valuelen[7];

    /* state */
    msglen += 4;
    msglen += strlen(state);

    /* Allocate space */
    npacket = netpacket_init();
    if (NULL == npacket)
    {
        elog(RLOG_WARNING, "xmanager metric assemble info integrate msg out of memory");
        return NULL;
    }
    msglen += 1;
    npacket->data = netpacket_data_init(msglen);
    if (NULL == npacket->data)
    {
        elog(RLOG_WARNING, "xmanager metric assemble info integrate msg data, out of memory");
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

    /* synclsn len */
    ivalue = strlen("synclsn");
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;

    /* synclsn */
    ivalue = strlen("synclsn");
    rmemcpy1(uptr, 0, "synclsn", ivalue);
    uptr += ivalue;
    rowlen += ivalue;

    /* loadtrailno len */
    ivalue = strlen("loadtrailno");
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;

    /* loadtrailno */
    ivalue = strlen("loadtrailno");
    rmemcpy1(uptr, 0, "loadtrailno", ivalue);
    uptr += ivalue;
    rowlen += ivalue;

    /* loadtrailstart len */
    ivalue = strlen("loadtrailstart");
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;

    /* loadtrailstart */
    ivalue = strlen("loadtrailstart");
    rmemcpy1(uptr, 0, "loadtrailstart", ivalue);
    uptr += ivalue;
    rowlen += ivalue;

    /* synctrailno len */
    ivalue = strlen("synctrailno");
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;

    /* synctrailno */
    ivalue = strlen("synctrailno");
    rmemcpy1(uptr, 0, "synctrailno", ivalue);
    uptr += ivalue;
    rowlen += ivalue;

    /* synctrailstart len */
    ivalue = strlen("synctrailstart");
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;

    /* synctrailstart */
    ivalue = strlen("synctrailstart");
    rmemcpy1(uptr, 0, "synctrailstart", ivalue);
    uptr += ivalue;
    rowlen += ivalue;

    /* loadtimestamp len */
    ivalue = strlen("loadtimestamp");
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;

    /* loadtimestamp */
    ivalue = strlen("loadtimestamp");
    rmemcpy1(uptr, 0, "loadtimestamp", ivalue);
    uptr += ivalue;
    rowlen += ivalue;

    /* synctimestamp len */
    ivalue = strlen("synctimestamp");
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;

    /* synctimestamp */
    ivalue = strlen("synctimestamp");
    rmemcpy1(uptr, 0, "synctimestamp", ivalue);
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
        elog(RLOG_WARNING, "xmanager metric assemble info integrate nullmap, out of memory");
        netpacket_destroy(npacket);
        return NULL;
    }
    rmemset0(nullmap, 0, 0, u16value);

    for (idx_col = 0; idx_col < REFRESH_METRICINTEGRATE_INFOCNT; idx_col++)
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
