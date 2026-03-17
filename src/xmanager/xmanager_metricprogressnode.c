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
/* 初始化 node 节点 */
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

/* 清理 metric progress 节点内存 */
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

/* 将 progress node 节点序列化 */
bool xmanager_metricprogressnode_serial(xmanager_metricnode* metricnode,
                                               uint8** blk,
                                               int* blksize,
                                               int* blkstart)
{
    bool bnew                                               = false;
    int len                                                 = 0;
    int freespace                                           = 0;
    int ivalue                                              = 0;
    int jobcnt                                              = 0;
    uint8* uptr                                             = NULL;
    dlistnode* dlnode                                       = NULL;
    xmanager_metricnode* xmetricnode                 = NULL;
    xmanager_metricprogressnode* xmetricprogressnode = NULL;

    if (NULL == metricnode)
    {
        return true;
    }

    xmetricprogressnode = (xmanager_metricprogressnode*)metricnode;

    /* node 节点的总长度 */
    len = 4;

    /* 
     * 计算总长度 
     *  1、metricnode 长度
     *  2、progressnode 长度
     */
    /* metricnode 长度 */
    len += xmanager_metricnode_serialsize(metricnode);

    /* todo progress node 私有长度 */

    /* job cnt */
    len += 4;

    for(dlnode = xmetricprogressnode->progressjop->head; NULL != dlnode; dlnode = dlnode->next)
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
            elog(RLOG_WARNING, "xmanager progress node serial error, out of memory");
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
    xmanager_metricnode_serial(&xmetricprogressnode->base, uptr, blkstart);

    /* 将 progress node 节点的内容序列化 */
    uptr += *blkstart;

    /* jobcnt */
    ivalue = jobcnt;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    *blkstart += 4;

    for(dlnode = xmetricprogressnode->progressjop->head; NULL != dlnode; dlnode = dlnode->next)
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

/* 反序列化为 progress node 节点 */
xmanager_metricnode* xmanager_metricprogressnode_deserial(uint8* blk, int* blkstart)
{
    int ivalue                                              = 0;
    int jobcnt                                              = 0;
    int jobtype                                             = 0;
    int idx_jobcnt                                          = 0;
    char* jobname                                           = NULL;
    uint8* uptr                                             = NULL;
    xmanager_metricnode* metricnode                  = NULL;
    xmanager_metricprogressnode* xmetricprogressnode = NULL;

    xmetricprogressnode = (xmanager_metricprogressnode*)xmanager_metricprogressnode_init();
    if (NULL == xmetricprogressnode)
    {
        elog(RLOG_WARNING, "xmanager metric progress deserial error, out of memory");
        return NULL;
    }

    /* 获取基础信息 */
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

/* 从配置文件中获取key--valve */
static bool xmanager_metricprogressnode_getdatafromcfgfile(const char *config_file, char* key, char* data)
{
    FILE *fp = NULL;
    char fline[1024];

    fp = osal_file_fopen(config_file, "rb");
    if (!fp)
    {
        elog(RLOG_WARNING, "could not open configuration file:%s %s", config_file, strerror(errno));
        return false;
    }

    /* 读取一行数据 */
    rmemset1(fline, 0, '\0', sizeof(fline));
    while (osal_file_fgets(fp, sizeof(fline), fline) != NULL)
    {
        bool quota = false;
        char* uptr = fline;
        int pos = 0;
        int len = 0;

        /* 跳过 开头的 空字符等信息 */
        while('\0' != *uptr)
        {
            if(' ' != *uptr
                && '\t' != *uptr
                && '\r' != *uptr
                && '\n' != *uptr)
            {
                break;
            }
            uptr++;
            pos++;
        }

        /* 跳过空行和注释行 */
        if('\0' == *uptr || '#' == *uptr)
        {
            rmemset1(fline, 0, '\0', sizeof(fline));
            continue;
        }

        /* 获取key */
        while('\0' != *uptr)
        {
            if(' ' == *uptr
                || '\t' == *uptr
                || '\r' == *uptr
                || '\n' == *uptr
                || '=' == *uptr)
            {
                break;
            }
            len++;
            uptr++;
        }

        /* 获取名称 */
        if ( len != strlen(key)
            || 0 != memcmp(key, fline + pos, len))
        {
            rmemset1(fline, 0, '\0', sizeof(fline));
            continue;
        }
        pos += len;

        /* 获取 value */
        len = 0;

        /* 跳过 SPACE TABE 和 换行 */
        while('\0'!= *uptr)
        {
            if(' ' != *uptr
                && '\t' != *uptr
                && '\r' != *uptr
                && '\n' != *uptr)
            {
                break;
            }
            pos++;
            uptr++;
        }

        if('=' != *uptr)
        {
            /* 结束了 */
            elog(RLOG_WARNING, "config data error");
            return false;
        }

        /* 获取 value 值 */
        /* 跳过 '=' 字符 */
        pos++;
        uptr++;

        /* 跳过 空格 等字符 */
        while('\0' != *uptr)
        {
            if(' ' != *uptr
                && '\t' != *uptr
                && '\r' != *uptr
                && '\n' != *uptr)
            {
                break;
            }
            uptr++;
            pos++;
        }

        /* 跳过空行和注释行 */
        if('\0' == *uptr || '#' == *uptr)
        {
            /* 结束了 */
            elog(RLOG_WARNING, "config data error");
            return false;
        }

        /* 跳过 空格 等字符 */
        /* 查看字符类型 */
        if(*uptr == '"')
        {
            uptr++;
            pos++;
            quota = true;
            /* 获取去下一个 " 字符 */
            while('\0' != *uptr)
            {
                if('"' == *uptr)
                {
                    quota = false;
                    break;
                }
                len++;
                uptr++;
            }

            if(true == quota)
            {
                elog(RLOG_WARNING, "configuration data is incorrect, missing double quotation marks");
                return false;
            }
        }
        else
        {
            while('\0'!= *uptr)
            {
                if(' ' == *uptr
                    || '\t' == *uptr
                    || '\r' == *uptr
                    || '\n' == *uptr)
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

/* progress capture info 组装 */
static void* xmanager_metricmsg_assembleprogresscapture(xmanager_metric* xmetric,
                                                               xmanager_metricnode* xmetricnode,
                                                               dlist* job)
{
    bool find                                               = false;
    uint8 u8value                                           = 0;
    int rowlen                                              = 0;
    int msglen                                              = 0;
    int ivalue                                              = 0;
    int rowcnt                                              = 0;
    uint32 hi                                               = 0;
    uint32 lo                                               = 0;
    int64 dbtime                                            = 0;
    uint8* rowuptr                                          = NULL;
    uint8* uptr                                             = NULL;
    PGconn *conn                                            = NULL;
    PGresult *res                                           = NULL;
    netpacket* npacket                               = NULL;
    char conninfo[512]                                      = {'\0'};
    char sql_exec[1024]                                     = {'\0'};

    rmemset1(conninfo, 0, 0, 512);
    xmanager_metricprogressnode_getdatafromcfgfile(xmetricnode->conf, CFG_KEY_URL, conninfo);

    conn = conn_get(conninfo);
    if (NULL == conn)
    {
        return NULL;
    }

    rmemset1(sql_exec, 0, '\0', 1024);
    sprintf(sql_exec,"SELECT pg_current_wal_lsn();" );
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

    /*以"%X/%X"格式读取redolsn中数据，并把值赋给hi，lo*/
    if (sscanf(PQgetvalue(res, 0, 0), "%X/%X", &hi, &lo) != 2)
    {
        PQfinish(conn);
        PQclear(res);
        elog(RLOG_WARNING," could not parse end position ");
        return NULL;
    }
    PQclear(res);

    rmemset1(sql_exec, 0, '\0', 1024);
    sprintf(sql_exec, "SELECT (EXTRACT(EPOCH\n"
                      "FROM (CURRENT_TIMESTAMP - TIMESTAMPTZ '2000-01-01 00:00:00+00') ) * 1000000 )::int8\n"
                      "AS pg_ts_usec;" );
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

    /* 读取dbtime */
    if (sscanf(PQgetvalue(res, 0, 0), "%ld", &dbtime) != 1)
    {
        PQfinish(conn);
        PQclear(res);
        elog(RLOG_WARNING,"dbtime could not parse end position ");
        return NULL;
    }

    PQfinish(conn);
    PQclear(res);

    /* 4 总长度 + 4 crc32 + 4 msgtype + 1 成功/失败 + 4 rowcnt */
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

    /* 申请空间 */
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

    /* 组装数据 */
    uptr = npacket->data;

    /* 数据总长度 */
    uptr += 4;
    npacket->used += 4;

    /* crc32 */
    uptr += 4;
    npacket->used += 4;

    /* 类型 */
    ivalue = XMANAGER_MSG_STARTCMD;
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
    ivalue = rowcnt;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    npacket->used += 4;

    rowlen = 0;
    rowuptr = uptr;

    /* 偏过行长度 */
    uptr += 4;
    rowlen = 4;
    npacket->used += 4;

    /* 列头组装 */
    /* name len */
    ivalue = strlen("name") ;
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
    ivalue = strlen("lsnlag") ;
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
    ivalue = strlen("timelag") ;
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

    /* 行总长度 */
    rowlen = r_hton32(rowlen);
    rmemcpy1(rowuptr, 0, &rowlen, 4);

    if (false == find)
    {
        elog(RLOG_WARNING, "xmanager metric assemble progress msg no valid found");
        netpacket_destroy(npacket);
        return NULL;
    }

    /* 数据总长度 */
    ivalue = npacket->used ;
    ivalue = r_hton32(ivalue);
    rmemcpy1(npacket->data, 0, &ivalue, 4);

    return npacket;
}

/* progress info 组装 */
void* xmanager_metricmsg_assembleprogress(xmanager_metric* xmetric, xmanager_metricnode* pxmetricnode)
{
    dlistnode* dlnode                                       = NULL;
    xmanager_metricnode* pmetricnode                 = NULL;
    xmanager_metricprogressnode* xmetricprogressnode = NULL;
    xmanager_metricnode tmpmetricnode                = {0};

    xmetricprogressnode = (xmanager_metricprogressnode*) pxmetricnode;

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
        elog(RLOG_WARNING, "not find valid information in progress %s ", xmetricprogressnode->base.name);
        return NULL;
    }

    tmpmetricnode.name = pmetricnode->name;
    tmpmetricnode.type = pmetricnode->type;

    /* 获取 metricnode */
    pmetricnode = dlist_get(xmetric->metricnodes, &tmpmetricnode, xmanager_metricnode_cmp);
    if (NULL == pmetricnode)
    {
        elog(RLOG_WARNING, "not find valid progress job %s in metricnodes ", pmetricnode->name);
        return NULL;
    }

    /* 检验是否在运行中，没有运行不打印信息 */
    if (XMANAGER_METRICNODESTAT_ONLINE > pmetricnode->stat)
    {
        elog(RLOG_WARNING, "progress job %s not start", pmetricnode->name);
        return NULL;
    }

    if (XMANAGER_METRICNODETYPE_CAPTURE == pmetricnode->type)
    {
        return xmanager_metricmsg_assembleprogresscapture(xmetric, pmetricnode, xmetricprogressnode->progressjop);
    }
    else
    {
        elog(RLOG_WARNING, "find invalid information in progress %s ", xmetricprogressnode->base.name);
        return NULL;
    }
}

