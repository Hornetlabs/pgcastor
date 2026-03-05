#include "ripple_app_incl.h"
#include "utils/dlist/dlist.h"
#include "utils/dttime/dttimestamp.h"
#include "net/netpacket/ripple_netpacket.h"
#include "xmanager/ripple_xmanager_msg.h"
#include "xmanager/ripple_xmanager_metricnode.h"
#include "xmanager/ripple_xmanager_metriccollectornode.h"

/*
 * metriccollector   列头长度
 * rowlen 4
 * namlen 4 + pumpname
 * namlen 4 + recvlsn
 * namlen 4 + flushlsn
 * namlen 4 + recvtrailno
 * namlen 4 + recvtrailstart
 * namlen 4 + flushtrailno
 * namlen 4 + flushtrailstart
 * namlen 4 + recvtimestamp
 * namlen 4 + flushtimestamp
 */
#define REFRESH_METRICCOLLECTOR_KEY_LEN ( 4 + 4 + strlen("pumpname") + 4 + strlen("recvlsn") + 4 + strlen("flushlsn") + 4 + strlen("recvtrailno") + 4 + strlen("recvtrailstart") + 4 + strlen("flushtrailno") + 4 + strlen("flushtrailstart") + 4 + strlen("recvtimestamp") + 4 + strlen("flushtimestamp"))


/* 初始化collectorinfo */
ripple_xmanager_metriccollectorinfo* ripple_xmanager_metriccollectorinfo_init(void)
{
    ripple_xmanager_metriccollectorinfo* xcollectormetricinfo = NULL;

    xcollectormetricinfo = rmalloc0(sizeof(ripple_xmanager_metriccollectorinfo));
    if (NULL == xcollectormetricinfo)
    {
        elog(RLOG_WARNING, "xmanager metric collector node init out of memory");
        return NULL;
    }
    rmemset0(xcollectormetricinfo, 0, '\0', sizeof(ripple_xmanager_metriccollectorinfo));
    rmemset0(xcollectormetricinfo->pumpname, 0, '\0', 128);
    xcollectormetricinfo->recvlsn = InvalidXLogRecPtr;
    xcollectormetricinfo->flushlsn = InvalidXLogRecPtr;
    xcollectormetricinfo->recvtrailno = 0;
    xcollectormetricinfo->recvtrailstart = 0;
    xcollectormetricinfo->flushtrailno = 0;
    xcollectormetricinfo->flushtrailstart = 0;
    xcollectormetricinfo->recvtimestamp = 0;
    xcollectormetricinfo->flushtimestamp = 0;

    return xcollectormetricinfo;
}

/* 初始化collectornode */
ripple_xmanager_metricnode* ripple_xmanager_metriccollectornode_init(void)
{
    ripple_xmanager_metriccollectornode* xcollectormetricnode = NULL;

    xcollectormetricnode = rmalloc0(sizeof(ripple_xmanager_metriccollectornode));
    if (NULL == xcollectormetricnode)
    {
        elog(RLOG_WARNING, "xmanager metric collector node init out of memory");
        return NULL;
    }
    rmemset0(xcollectormetricnode, 0, '\0', sizeof(ripple_xmanager_metriccollectornode));

    ripple_xmanager_metricnode_reset(&xcollectormetricnode->base);
    xcollectormetricnode->collectorinfo = NULL;
    xcollectormetricnode->base.type = RIPPLE_XMANAGER_METRICNODETYPE_COLLECTOR;
    return (ripple_xmanager_metricnode*)xcollectormetricnode;
}

/*  metriccollectorinfo pump比较函数 */
int ripple_xmanager_metriccollectorinfo_cmp(void* s1, void* s2)
{
    char* pumpname                                      = NULL;
    ripple_xmanager_metriccollectorinfo* collectorinfo  = NULL;

    pumpname = (char*)s1;
    collectorinfo = (ripple_xmanager_metriccollectorinfo*)s2;

    if (0 == strcmp(pumpname, collectorinfo->pumpname))
    {
        return 0;
    }
     return 1;
}

/* 资源清理 */
void ripple_xmanager_metriccollectorinfo_destroy(void* args)
{
    ripple_xmanager_metriccollectorinfo* metriccollectorinfo = NULL;

    metriccollectorinfo = (ripple_xmanager_metriccollectorinfo* )args;
    if (NULL == metriccollectorinfo)
    {
        return;
    }
    
    rfree(metriccollectorinfo);
}

/* 资源清理 */
void ripple_xmanager_metriccollectornode_destroy(ripple_xmanager_metricnode* metricnode)
{
    ripple_xmanager_metriccollectornode* xcollectormetricnode = NULL;

    if (NULL == metricnode)
    {
        return;
    }

    xcollectormetricnode = (ripple_xmanager_metriccollectornode* )metricnode;

    dlist_free(xcollectormetricnode->collectorinfo, ripple_xmanager_metriccollectorinfo_destroy);

    rfree(metricnode);
}

/* 将 collector node 节点序列化 */
bool ripple_xmanager_metriccollectornode_serial(ripple_xmanager_metricnode* metricnode,
                                                uint8** blk,
                                                int* blksize,
                                                int* blkstart)
{
    bool bnew                                                   = false;
    int len                                                     = 0;
    int infolen                                                 = 0;
    int freespace                                               = 0;
    int ivalue                                                  = 0;
    int64 i64value                                              = 0;
    uint64 uvalue                                               = 0;
    uint64 pumpcnt                                              = 0;
    uint8* uptr                                                 = NULL;
    dlistnode* dlnode                                           = NULL;
    ripple_xmanager_metriccollectornode* xmetriccollectornode   = NULL;
    ripple_xmanager_metriccollectorinfo* xmetriccollectorinfo   = NULL;

    if (NULL == metricnode)
    {
        return true;
    }

    xmetriccollectornode = (ripple_xmanager_metriccollectornode*)metricnode;

    if (true == dlist_isnull(xmetriccollectornode->collectorinfo))
    {
        pumpcnt = 0;
    }
    else
    {
        pumpcnt = xmetriccollectornode->collectorinfo->length;
    }

    /* node 节点的总长度 */
    len = 4;

    /* 
     * 计算总长度 
     *  1、metricnode 长度
     *  2、collectornode 长度
     */
    /* metricnode 长度 */
    len += ripple_xmanager_metricnode_serialsize(metricnode);

    /* pumpname个数 */
    len += 8;

    /* collector info 私有长度 */
    infolen = (128 +                /* pumpname[128] */
               8   +                /* recvlsn */
               8   +                /* flushlsn */
               8   +                /* recvtrailno */
               8   +                /* recvtrailstart */
               8   +                /* flushtrailno */
               8   +                /* flushtrailstar */
               8   +                /* recvtimestamp */
               8                    /* flushtimestamp */);

    /* collector node 总私有长度 */
    len += (infolen * pumpcnt);
    
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
            elog(RLOG_WARNING, "xmanager collector node serial error, out of memory");
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
    ripple_xmanager_metricnode_serial(&xmetriccollectornode->base, uptr, blkstart);

    /* 将 collector node 节点的内容序列化 */
    uptr += *blkstart;

    /* pumpnamecnt */
    uvalue = pumpcnt;
    uvalue = r_hton64(uvalue);
    rmemcpy1(uptr, 0, &uvalue, 8);
    uptr += 8;
    *blkstart += 8;

    if (0 == pumpcnt)
    {
        return true;
    }

    for(dlnode = xmetriccollectornode->collectorinfo->head; NULL != dlnode; dlnode = dlnode->next)
    {
        xmetriccollectorinfo = (ripple_xmanager_metriccollectorinfo* )dlnode->value;

        /* pumpname */
        rmemcpy1(uptr, 0, xmetriccollectorinfo->pumpname, 128);
        uptr += 128;
        *blkstart += 128;

        /* recvlsn */
        uvalue = xmetriccollectorinfo->recvlsn;
        uvalue = r_hton64(uvalue);
        rmemcpy1(uptr, 0, &uvalue, 8);
        uptr += 8;
        *blkstart += 8;

        /* flushlsn */
        uvalue = xmetriccollectorinfo->flushlsn;
        uvalue = r_hton64(uvalue);
        rmemcpy1(uptr, 0, &uvalue, 8);
        uptr += 8;
        *blkstart += 8;

        /* recvtrailno */
        uvalue = xmetriccollectorinfo->recvtrailno;
        uvalue = r_hton64(uvalue);
        rmemcpy1(uptr, 0, &uvalue, 8);
        uptr += 8;
        *blkstart += 8;

        /* recvtrailstart */
        uvalue = xmetriccollectorinfo->recvtrailstart;
        uvalue = r_hton64(uvalue);
        rmemcpy1(uptr, 0, &uvalue, 8);
        uptr += 8;
        *blkstart += 8;

        /* flushtrailno */
        uvalue = xmetriccollectorinfo->flushtrailno;
        uvalue = r_hton64(uvalue);
        rmemcpy1(uptr, 0, &uvalue, 8);
        uptr += 8;
        *blkstart += 8;

        /* flushtrailstart */
        uvalue = xmetriccollectorinfo->flushtrailstart;
        uvalue = r_hton64(uvalue);
        rmemcpy1(uptr, 0, &uvalue, 8);
        uptr += 8;
        *blkstart += 8;

        /* recvtimestamp */
        i64value = xmetriccollectorinfo->recvtimestamp;
        i64value = r_hton64(i64value);
        rmemcpy1(uptr, 0, &i64value, 8);
        uptr += 8;
        *blkstart += 8;

        /* flushtimestamp */
        i64value = xmetriccollectorinfo->flushtimestamp;
        i64value = r_hton64(i64value);
        rmemcpy1(uptr, 0, &i64value, 8);
        uptr += 8;
        *blkstart += 8;
    }

    return true;
}

/* 反序列化为 collector node 节点 */
ripple_xmanager_metricnode* ripple_xmanager_metriccollectornode_deserial(uint8* blk, int* blkstart)
{
    int64 pumpcnt                                               = 0;
    int64 idx_pump                                              = 0;
    int64 i64value                                              = 0;
    uint64 u64value                                             = 0;
    uint8* uptr                                                 = NULL;
    ripple_xmanager_metriccollectornode* xmetriccollectornode   = NULL;
    ripple_xmanager_metriccollectorinfo* xmetriccollectorinfo   = NULL;
    xmetriccollectornode = (ripple_xmanager_metriccollectornode*)ripple_xmanager_metriccollectornode_init();
    if (NULL == xmetriccollectornode)
    {
        elog(RLOG_WARNING, "xmanager metric collector deserial error, out of memory");
        return NULL;
    }

    /* 获取基础信息 */
    if (false == ripple_xmanager_metricnode_deserial(&xmetriccollectornode->base, blk, blkstart))
    {
        elog(RLOG_WARNING, "xmanager metric collector deserial error");
        ripple_xmanager_metricnode_destroy(&xmetriccollectornode->base);
        return NULL;
    }
    uptr = blk;
    uptr += *blkstart;

    /* pumpcnt */
    rmemcpy1(&u64value, 0, uptr, 8);
    pumpcnt = r_ntoh64(u64value);
    uptr += 8;
    *blkstart += 8;

    for (idx_pump = 0; idx_pump < pumpcnt; idx_pump++)
    {
        xmetriccollectorinfo = ripple_xmanager_metriccollectorinfo_init();
        xmetriccollectornode->collectorinfo = dlist_put(xmetriccollectornode->collectorinfo, xmetriccollectorinfo);

        /* pumpname */
        rmemcpy1(xmetriccollectorinfo->pumpname, 0, uptr, 128);
        uptr += 128;
        *blkstart += 128;

        /* recvlsn */
        rmemcpy1(&u64value, 0, uptr, 8);
        xmetriccollectorinfo->recvlsn = r_ntoh64(u64value);
        uptr += 8;
        *blkstart += 8;

        /* flushlsn */
        rmemcpy1(&u64value, 0, uptr, 8);
        xmetriccollectorinfo->flushlsn = r_ntoh64(u64value);
        uptr += 8;
        *blkstart += 8;

        /* recvtrailno */
        rmemcpy1(&u64value, 0, uptr, 8);
        xmetriccollectorinfo->recvtrailno = r_ntoh64(u64value);
        uptr += 8;
        *blkstart += 8;

        /* recvtrailstart */
        rmemcpy1(&u64value, 0, uptr, 8);
        xmetriccollectorinfo->recvtrailstart = r_ntoh64(u64value);
        uptr += 8;
        *blkstart += 8;

        /* flushtrailno */
        rmemcpy1(&u64value, 0, uptr, 8);
        xmetriccollectorinfo->flushtrailno = r_ntoh64(u64value);
        uptr += 8;
        *blkstart += 8;

        /* flushtrailstart */
        rmemcpy1(&u64value, 0, uptr, 8);
        xmetriccollectorinfo->flushtrailstart = r_ntoh64(u64value);
        uptr += 8;
        *blkstart += 8;

        /* recvtimestamp */
        rmemcpy1(&i64value, 0, uptr, 8);
        xmetriccollectorinfo->recvtimestamp = r_ntoh64(i64value);
        uptr += 8;
        *blkstart += 8;

        /* flushtimestamp */
        rmemcpy1(&i64value, 0, uptr, 8);
        xmetriccollectorinfo->flushtimestamp = r_ntoh64(i64value);
        uptr += 8;
        *blkstart += 8;
    }

    return (ripple_xmanager_metricnode*)xmetriccollectornode;
}

/* collector info 组装 */
void* ripple_xmanager_metricmsg_assemblecollector(ripple_xmanager_metricnode* pxmetricnode)
{
    uint8 u8value                                               = 0;
    uint16 u16value                                             = 0;
    int rowlen                                                  = 0;
    int msglen                                                  = 0;
    int ivalue                                                  = 0;
    uint32 valuelen                                             = 0;
    uint8* nullmap                                              = NULL;
    uint8* rowuptr                                              = NULL;
    uint8* uptr                                                 = NULL;
    dlistnode* dlnode                                           = NULL;
    ripple_netpacket* npacket                                   = NULL;
    ripple_xmanager_metriccollectornode* xmetriccollectornode   = NULL;
    ripple_xmanager_metriccollectorinfo* xmetriccollectorinfo   = NULL;
    char state[32]                                              = {'\0'};
    char values[128]                                            = {'\0'};

    xmetriccollectornode = (ripple_xmanager_metriccollectornode*) pxmetricnode;

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

    if (true == dlist_isnull(xmetriccollectornode->collectorinfo))
    {
        elog(RLOG_WARNING, "xmanager metric collector assemble info collector msg, collectorinfo is null");
        return NULL;
    }

    /* 4 总长度 + 4 crc32 + 4 msgtype + 1 成功/失败 + 4 rowcnt */
    msglen = 4 + 4 + 4 + 1 + 4;

    /* 第一行 长度 info REFRESH_METRICCAPTURE_KEY_LEN + state (4 + strlen(state)) */
    msglen += (REFRESH_METRICCOLLECTOR_KEY_LEN + 4 + strlen("state"));

    /* 计算列值长度 */

    rowlen = 0;

    /* rowlen 4 + nullmapcnt 2 + nullmap 2 */
    rowlen += (4 + 2 + 2);
    
    /* pumpname */
    rowlen += 4;
    rowlen += 128;

    /* recvlsn */
    rowlen += 4;
    rowlen += 32;

    /* flushlsn长度 */
    rowlen += 4;
    rowlen += 32;

    /* recvtrailno长度 */
    rowlen += 4;
    rowlen += 32;

    /* recvtrailstart长度 */
    rowlen += 4;
    rowlen += 32;

    /* flushtrailno长度 */
    rowlen += 4;
    rowlen += 32;

    /* flushtrailstart长度 */
    rowlen += 4;
    rowlen += 32;

    /* recvtimestamp长度 */
    rowlen += 4;
    rowlen += 32;

    /* flushtimestamp长度 */
    rowlen += 4;
    rowlen += 128;

    /* parsetimestamp + len */
    rowlen += 4;
    rowlen += 128;

    /* flushtimestamp + len */
    rowlen += 4;
    rowlen += 128;

    /* state */
    rowlen += 4;
    rowlen += strlen(state);

    msglen += (rowlen * xmetriccollectornode->collectorinfo->length);


    /* 申请空间 */
    npacket = ripple_netpacket_init();
    if (NULL == npacket)
    {
        elog(RLOG_WARNING, "xmanager metric assemble info collector msg out of memory");
        return NULL;
    }
    msglen += 1;
    npacket->data = ripple_netpacket_data_init(msglen);
    if (NULL == npacket->data)
    {
        elog(RLOG_WARNING, "xmanager metric assemble info collector msg data, out of memory");
        ripple_netpacket_destroy(npacket);
        return NULL;
    }
    msglen -= 1;

    /* 组装数据 */
    uptr = npacket->data;

    /* 数据总长度 */
    uptr += 4;
    npacket->used += 4;

    /* crc32 */
    uptr += 4;
    npacket->used += 4;

    /* 类型 */
    ivalue = RIPPLE_XMANAGER_MSG_STARTCMD;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    npacket->used += 4;

    /* 类型成功标识 */
    u8value = 0;
    rmemcpy1(uptr, 0, &u8value, 1);
    uptr += 1;
    npacket->used += 1;

    /* rowcnt */
    ivalue = 1;
    ivalue += xmetriccollectornode->collectorinfo->length;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    npacket->used += 4;

    rowlen = 0;
    rowuptr = uptr;

    /* 偏过行长度 */
    uptr += 4;
    npacket->used += 4;
    rowlen = 4;

    /* 列头组装 */
    /* pumpname len */
    ivalue = strlen("pumpname") ;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;

    /* pumpname */
    ivalue = strlen("pumpname");
    rmemcpy1(uptr, 0, "pumpname", ivalue);
    uptr += ivalue;
    rowlen += ivalue;
    npacket->used += ivalue;

    /* recvlsn len */
    ivalue = strlen("recvlsn") ;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;

    /* recvlsn */
    ivalue = strlen("recvlsn");
    rmemcpy1(uptr, 0, "recvlsn", ivalue);
    uptr += ivalue;
    rowlen += ivalue;
    npacket->used += ivalue;

    /* flushlsn len */
    ivalue = strlen("flushlsn") ;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;

    /* flushlsn */
    ivalue = strlen("flushlsn");
    rmemcpy1(uptr, 0, "flushlsn", ivalue);
    uptr += ivalue;
    rowlen += ivalue;
    npacket->used += ivalue;

    /* recvtrailno len */
    ivalue = strlen("recvtrailno") ;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;

    /* recvtrailno */
    ivalue = strlen("recvtrailno");
    rmemcpy1(uptr, 0, "recvtrailno", ivalue);
    uptr += ivalue;
    rowlen += ivalue;
    npacket->used += ivalue;

    /* recvtrailstart len */
    ivalue = strlen("recvtrailstart") ;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;

    /* recvtrailstart */
    ivalue = strlen("recvtrailstart");
    rmemcpy1(uptr, 0, "recvtrailstart", ivalue);
    uptr += ivalue;
    rowlen += ivalue;
    npacket->used += ivalue;

    /* flushtrailno len */
    ivalue = strlen("flushtrailno") ;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;

    /* flushtrailno */
    ivalue = strlen("flushtrailno");
    rmemcpy1(uptr, 0, "flushtrailno", ivalue);
    uptr += ivalue;
    rowlen += ivalue;
    npacket->used += ivalue;

    /* flushtrailstart len */
    ivalue = strlen("flushtrailstart") ;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;

    /* flushtrailstart */
    ivalue = strlen("flushtrailstart");
    rmemcpy1(uptr, 0, "flushtrailstart", ivalue);
    uptr += ivalue;
    rowlen += ivalue;
    npacket->used += ivalue;

    /* recvtimestamp len */
    ivalue = strlen("recvtimestamp") ;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;

    /* recvtimestamp */
    ivalue = strlen("recvtimestamp");
    rmemcpy1(uptr, 0, "recvtimestamp", ivalue);
    uptr += ivalue;
    rowlen += ivalue;
    npacket->used += ivalue;

    /* flushtimestamp len */
    ivalue = strlen("flushtimestamp") ;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;

    /* flushtimestamp */
    ivalue = strlen("flushtimestamp");
    rmemcpy1(uptr, 0, "flushtimestamp", ivalue);
    uptr += ivalue;
    rowlen += ivalue;
    npacket->used += ivalue;

    /* state len */
    ivalue = strlen("state") ;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;

    /* state */
    ivalue = strlen("state");
    rmemcpy1(uptr, 0, "state", ivalue);
    uptr += ivalue;
    rowlen += ivalue;
    npacket->used += ivalue;

    /* 行总长度 */
    rowlen = r_hton32(rowlen);
    rmemcpy1(rowuptr, 0, &rowlen, 4);

    for(dlnode = xmetriccollectornode->collectorinfo->head; NULL != dlnode; dlnode = dlnode->next)
    {
        xmetriccollectorinfo = (ripple_xmanager_metriccollectorinfo* )dlnode->value;

        rowlen = 0;
        rowuptr = uptr;

        /* 跳过行长度 */
        uptr += 4;
        rowlen = 4;
        npacket->used += 4;

        /* 空列map的个数 */
        u16value = 2;
        u16value = r_hton16(u16value);
        rmemcpy1(uptr, 0, &u16value, 2);
        uptr += 2;
        rowlen += 2;
        npacket->used += 2;

        /* 空列 map */
        u16value = 2;
        uptr += u16value;
        rowlen += u16value;
        npacket->used += 2;

        nullmap = rmalloc0(u16value);
        if (NULL == nullmap)
        {
            elog(RLOG_WARNING, "xmanager metric assemble info collector nullmap, out of memory");
            ripple_netpacket_destroy(npacket);
            return NULL;
        }
        rmemset0(nullmap, 0, 0, u16value);
        /* pumpname len */
        if (xmetriccollectorinfo->pumpname[0] != '\0'
            && NULL != xmetriccollectorinfo->pumpname)
        {
            valuelen = strlen(xmetriccollectorinfo->pumpname);
            ivalue = valuelen;
            ivalue = r_hton32(ivalue);
            rmemcpy1(uptr, 0, &ivalue, 4);
            uptr += 4;
            rowlen += 4;
            npacket->used += 4;

            /* pumpname len */
            rmemcpy1(uptr, 0, xmetriccollectorinfo->pumpname, valuelen);
            uptr += valuelen;
            rowlen += valuelen;
            npacket->used += valuelen;
        }
        else
        {
            nullmap[0 / 8] |= (1U << (0 % 8));
            continue;
        }

        /* recvlsn len */
        valuelen = snprintf(values, 128, "%X/%X", (uint32)(xmetriccollectorinfo->recvlsn >> 32), (uint32)(xmetriccollectorinfo->recvlsn));
        ivalue = valuelen;
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;
        rowlen += 4;
        npacket->used += 4;

        /* recvlsn */
        rmemcpy1(uptr, 0, values, valuelen);
        uptr += valuelen;
        rowlen += valuelen;
        npacket->used += valuelen;

        /* flushlsn len */
        valuelen = snprintf(values, 128, "%X/%X", (uint32)(xmetriccollectorinfo->flushlsn >> 32), (uint32)(xmetriccollectorinfo->flushlsn));
        ivalue = valuelen;
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;
        rowlen += 4;
        npacket->used += 4;

        /* flushlsn */
        rmemcpy1(uptr, 0, values, valuelen);
        uptr += valuelen;
        rowlen += valuelen;
        npacket->used += valuelen;

        /* recvtrailno len */
        valuelen = snprintf(values, 128, "%" PRIu64, xmetriccollectorinfo->recvtrailno);
        ivalue = valuelen;
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;
        rowlen += 4;
        npacket->used += 4;

        /* recvtrailno */
        rmemcpy1(uptr, 0, values, valuelen);
        uptr += valuelen;
        rowlen += valuelen;
        npacket->used += valuelen;

        /* recvtrailstart len */
        valuelen = snprintf(values, 128, "%" PRIu64, xmetriccollectorinfo->recvtrailstart);
        ivalue = valuelen;
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;
        rowlen += 4;
        npacket->used += 4;

        /* recvtrailstart */
        rmemcpy1(uptr, 0, values, valuelen);
        uptr += valuelen;
        rowlen += valuelen;
        npacket->used += valuelen;

        /* flushtrailno len */
        valuelen = snprintf(values, 128, "%" PRIu64, xmetriccollectorinfo->flushtrailno);
        ivalue = valuelen;
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;
        rowlen += 4;
        npacket->used += 4;

        /* flushtrailno */
        rmemcpy1(uptr, 0, values, valuelen);
        uptr += valuelen;
        rowlen += valuelen;
        npacket->used += valuelen;

        /* flushtrailstart len */
        valuelen = snprintf(values, 128, "%" PRIu64, xmetriccollectorinfo->flushtrailstart);
        ivalue = valuelen;
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;
        rowlen += 4;
        npacket->used += 4;

        /* flushtrailstart */
        rmemcpy1(uptr, 0, values, valuelen);
        uptr += valuelen;
        rowlen += valuelen;
        npacket->used += valuelen;

        /* recvtimestamp len */
        dt_timestamptz_to_string((TimestampTz)xmetriccollectorinfo->recvtimestamp, values);
        valuelen = strlen(values);
        ivalue = valuelen;
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;
        rowlen += 4;
        npacket->used += 4;

        /* recvtimestamp */
        rmemcpy1(uptr, 0, values, valuelen);
        uptr += valuelen;
        rowlen += valuelen;
        npacket->used += valuelen;

        /* flushtimestamp len */
        dt_timestamptz_to_string((TimestampTz)xmetriccollectorinfo->recvtimestamp, values);
        valuelen = strlen(values);
        ivalue = valuelen;
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;
        rowlen += 4;
        npacket->used += 4;

        /* flushtimestamp */
        rmemcpy1(uptr, 0, values, valuelen);
        uptr += valuelen;
        rowlen += valuelen;
        npacket->used += valuelen;

        /* state 长度 */
        ivalue = strlen(state);
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;
        rowlen += 4;
        npacket->used += 4;

        /* 列内容 */
        valuelen = strlen(state);
        rmemcpy1(uptr, 0, state, valuelen);
        uptr += valuelen;
        rowlen += valuelen;
        npacket->used += valuelen;

        /* 行总长度 */
        rowlen = r_hton32(rowlen);
        rmemcpy1(rowuptr, 0, &rowlen, 4);
        rowuptr += 4;

        rowuptr += 2;
        rmemcpy1(rowuptr, 0, nullmap, u16value);

        rfree(nullmap);
        nullmap = NULL;
    }

    /* 数据总长度 */
    ivalue = npacket->used;
    ivalue = r_hton32(ivalue);
    rmemcpy1(npacket->data, 0, &ivalue, 4);

    return (void*)npacket;
}
