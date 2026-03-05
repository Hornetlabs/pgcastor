#include "ripple_app_incl.h"
#include "utils/dlist/dlist.h"
#include "utils/dttime/dttimestamp.h"
#include "net/netpacket/ripple_netpacket.h"
#include "xmanager/ripple_xmanager_msg.h"
#include "xmanager/ripple_xmanager_metricnode.h"
#include "xmanager/ripple_xmanager_metricintegratenode.h"

/*
 * metricintegrate  列头长度
 * rowlen 4
 * namelen 4 + loadlsn
 * namelen 4 + synclsn
 * namelen 4 + loadtrailno
 * namelen 4 + loadtrailstart
 * namelen 4 + synctrailno
 * namelen 4 + synctrailstart
 * namelen 4 + loadtimestamp
 * namelen 4 + synctimestamp
 */
#define REFRESH_METRICINTEGRATE_KEY_LEN ( 4 + 4 + strlen("loadlsn") + 4 + strlen("synclsn") + 4 + strlen("loadtrailno") + 4 + strlen("loadtrailstart") + 4 + strlen("synctrailno") + 4 + strlen("synctrailstart") + 4 + strlen("loadtimestamp") + 4 + strlen("synctimestamp"))


/* 初始化 */
ripple_xmanager_metricnode* ripple_xmanager_metricintegratenode_init(void)
{
    ripple_xmanager_metricintegratenode* xintegratemetricnode = NULL;

    xintegratemetricnode = rmalloc0(sizeof(ripple_xmanager_metricintegratenode));
    if (NULL == xintegratemetricnode)
    {
        elog(RLOG_WARNING, "xmanager metric integrate node init out of memory");
        return NULL;
    }
    rmemset0(xintegratemetricnode, 0, '\0', sizeof(ripple_xmanager_metricintegratenode));

    ripple_xmanager_metricnode_reset(&xintegratemetricnode->base);
    xintegratemetricnode->base.type = RIPPLE_XMANAGER_METRICNODETYPE_INTEGRATE;
    return (ripple_xmanager_metricnode*)xintegratemetricnode;
}

/* 资源清理 */
void ripple_xmanager_metricintegratenode_destroy(ripple_xmanager_metricnode* metricnode)
{
    rfree(metricnode);
}

/* 将 integrate node 节点序列化 */
bool ripple_xmanager_metricintegratenode_serial(ripple_xmanager_metricnode* metricnode,
                                                uint8** blk,
                                                int* blksize,
                                                int* blkstart)
{
    bool bnew                                                   = false;
    int len                                                     = 0;
    int freespace                                               = 0;
    int ivalue                                                  = 0;
    int64 i64value                                              = 0;
    uint64 uvalue                                               = 0;
    uint8* uptr                                                 = NULL;
    ripple_xmanager_metricintegratenode* xmetricintegratenode   = NULL;

    if (NULL == metricnode)
    {
        return true;
    }

    xmetricintegratenode = (ripple_xmanager_metricintegratenode*)metricnode;

    /* node 节点的总长度 */
    len = 4;

    /* 
     * 计算总长度 
     *  1、metricnode 长度
     *  2、integratenode 长度
     */
    /* metricnode 长度 */
    len += ripple_xmanager_metricnode_serialsize(metricnode);

    /* integrate node 私有长度 */
    len += (8 +                 /*loadlsn */
            8 +                 /*synclsn */
            8 +                 /*loadtrailno */
            8 +                 /*loadtrailstart */
            8 +                 /*synctrailno */
            8 +                 /*synctrailstart */
            8 +                 /*loadtimestamp */
            8                   /*synctimestamp */);
    
    /* 查看空间是否足够, 不够那么申请空间 */
    uptr = *blk;
    freespace = *blksize - *blkstart;
    while (len > freespace)
    {
        /* 重新申请空间 */
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

    /* 总长度序列化 */
    uptr += *blkstart;
    ivalue = len;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    *blkstart += 4;
    
    /* 重新指向头部 */
    uptr = *blk;

    /* 通用内容格式化 */
    ripple_xmanager_metricnode_serial(&xmetricintegratenode->base, uptr, blkstart);

    /* 将 integrate node 节点的内容序列化 */
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

/* 反序列化为 integrate node 节点 */
ripple_xmanager_metricnode* ripple_xmanager_metricintegratenode_deserial(uint8* blk, int* blkstart)
{
    int64 i64value                                              = 0;
    uint64 u64value                                             = 0;
    uint8* uptr                                                 = NULL;
    ripple_xmanager_metricintegratenode* xmetricintegratenode   = NULL;
    xmetricintegratenode = (ripple_xmanager_metricintegratenode*)ripple_xmanager_metricintegratenode_init();
    if (NULL == xmetricintegratenode)
    {
        elog(RLOG_WARNING, "xmanager metric integrate deserial error, out of memory");
        return NULL;
    }

    /* 获取基础信息 */
    if (false == ripple_xmanager_metricnode_deserial(&xmetricintegratenode->base, blk, blkstart))
    {
        elog(RLOG_WARNING, "xmanager metric integrate deserial error");
        ripple_xmanager_metricnode_destroy(&xmetricintegratenode->base);
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

    return (ripple_xmanager_metricnode*)xmetricintegratenode;
}

/* integrate info 组装 */
void* ripple_xmanager_metricmsg_assembleintegrate(ripple_xmanager_metricnode* pxmetricnode)
{
    uint8 u8value                                               = 0;
    uint16 u16value                                             = 0;
    int rowlen                                                  = 0;
    int msglen                                                  = 0;
    int ivalue                                                  = 0;
    size_t idx_col                                              = 0;
    uint8* nullmap                                              = NULL;
    uint8* rowuptr                                              = NULL;
    uint8* uptr                                                 = NULL;
    ripple_netpacket* npacket                                   = NULL;
    ripple_xmanager_metricintegratenode* xmetricintegratenode   = NULL;
    char state[32]                                              = {'\0'};
    char values[REFRESH_METRICINTEGRATE_INFOCNT][32]            = {{0}};
    int32 valuelen[REFRESH_METRICINTEGRATE_INFOCNT]             = {0};

    xmetricintegratenode = (ripple_xmanager_metricintegratenode*) pxmetricnode;

    rmemset1(state, 0, 0, 32);
    if (RIPPLE_XMANAGER_METRICNODESTAT_NOP == pxmetricnode->stat)
    {
        snprintf(state, 32, "%s", "NOP");
    }
    else if (RIPPLE_XMANAGER_METRICNODESTAT_INIT == pxmetricnode->stat)
    {
        snprintf(state, 32, "%s", "INIT");
    }
    else if (RIPPLE_XMANAGER_METRICNODESTAT_ONLINE == pxmetricnode->stat)
    {
        snprintf(state, 32, "%s", "ONLINE");
    }
    else if (RIPPLE_XMANAGER_METRICNODESTAT_OFFLINE == pxmetricnode->stat)
    {
        snprintf(state, 32, "%s", "OFFLINE");
    }
    else
    {
        elog(RLOG_WARNING, "xmanager metric assemble info capture msg data, invalid metricnode stat");
        return NULL;
    }

    /* 4 总长度 + 4 crc32 + 4 msgtype + 1 成功/失败 + 4 rowcnt */
    msglen = 4 + 4 + 4 + 1 + 4;

    /* 第一行 长度 info REFRESH_METRICCAPTURE_KEY_LEN + state (4 + strlen(state)) 长度 */
    msglen += (REFRESH_METRICINTEGRATE_KEY_LEN + 4 + strlen("state"));

    /* rowlen */
    msglen += 4;

    /* nullmapcnt */
    msglen += 2;

    /* nullmap */
    msglen += 2;

    /* 计算列值长度 */

    /* loadlsn + len */
    msglen += 4;
    valuelen[0] = snprintf(values[0], 32, "%X/%X", (uint32)(xmetricintegratenode->loadlsn >> 32), (uint32)(xmetricintegratenode->loadlsn));
    msglen += valuelen[0];

    /* synclsn + len */
    msglen += 4;
    valuelen[1] = snprintf(values[1], 32, "%X/%X", (uint32)(xmetricintegratenode->synclsn >> 32), (uint32)(xmetricintegratenode->synclsn));
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

    /* 申请空间 */
    npacket = ripple_netpacket_init();
    if (NULL == npacket)
    {
        elog(RLOG_WARNING, "xmanager metric assemble info integrate msg out of memory");
        return NULL;
    }
    msglen += 1;
    npacket->data = ripple_netpacket_data_init(msglen);
    if (NULL == npacket->data)
    {
        elog(RLOG_WARNING, "xmanager metric assemble info integrate msg data, out of memory");
        ripple_netpacket_destroy(npacket);
        return NULL;
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
    ivalue = RIPPLE_XMANAGER_MSG_STARTCMD;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* 类型成功标识 */
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

    /* 偏过行长度 */
    uptr += 4;
    rowlen = 4;

    /* 列头组装 */
    /* loadlsn len */
    ivalue = strlen("loadlsn") ;
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
    ivalue = strlen("synclsn") ;
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
    ivalue = strlen("loadtrailno") ;
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
    ivalue = strlen("loadtrailstart") ;
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
    ivalue = strlen("synctrailno") ;
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
    ivalue = strlen("synctrailstart") ;
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
    ivalue = strlen("loadtimestamp") ;
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
    ivalue = strlen("synctimestamp") ;
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
    ivalue = strlen("state") ;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;

    /* state */
    ivalue = strlen("state");
    rmemcpy1(uptr, 0, "state", ivalue);
    uptr += ivalue;
    rowlen += ivalue;

    /* 行总长度 */
    rowlen = r_hton32(rowlen);
    rmemcpy1(rowuptr, 0, &rowlen, 4);

    rowlen = 0;
    rowuptr = uptr;

    /* 跳过行长度 */
    uptr += 4;
    rowlen = 4;

    /* 空列map的个数 */
    u16value = 2;
    u16value = r_hton16(u16value);
    rmemcpy1(uptr, 0, &u16value, 2);
    uptr += 2;
    rowlen += 2;

    /* 空列 map */
    u16value = 2;
    uptr += u16value;
    rowlen += u16value;

    nullmap = rmalloc0(u16value);
    if (NULL == nullmap)
    {
        elog(RLOG_WARNING, "xmanager metric assemble info integrate nullmap, out of memory");
        ripple_netpacket_destroy(npacket);
        return NULL;
    }
    rmemset0(nullmap, 0, 0, u16value);

    for (idx_col = 0; idx_col < REFRESH_METRICINTEGRATE_INFOCNT; idx_col++)
    {

        if (values[idx_col] == NULL || values[idx_col][0] == '\0')
        {
            nullmap[idx_col / 8] |= (1U << (idx_col % 8));
            continue;
        }

        /* 列 长度 */
        ivalue = valuelen[idx_col];
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;
        rowlen += 4;

        /* 列内容 */
        rmemcpy1(uptr, 0, values[idx_col], valuelen[idx_col]);
        uptr += valuelen[idx_col];
        rowlen += valuelen[idx_col];
    }

    /* state 长度 */
    ivalue = strlen(state);
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;

    /* 列内容 */
    rmemcpy1(uptr, 0, state, strlen(state));
    uptr += strlen(state);
    rowlen += strlen(state);

    /* 行总长度 */
    rowlen = r_hton32(rowlen);
    rmemcpy1(rowuptr, 0, &rowlen, 4);
    rowuptr += 4;

    rowuptr += 2;
    rmemcpy1(rowuptr, 0, nullmap, u16value);

    rfree(nullmap);
    nullmap = NULL;

    return (void*)npacket;
}
