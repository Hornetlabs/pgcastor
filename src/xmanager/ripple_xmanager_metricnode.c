#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/net/ripple_net.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "queue/ripple_queue.h"
#include "command/ripple_cmd.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netiomp/ripple_netiomp_poll.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netpool.h"
#include "xmanager/ripple_xmanager_msg.h"
#include "xmanager/ripple_xmanager_metricasyncmsg.h"
#include "xmanager/ripple_xmanager_metricnode.h"
#include "xmanager/ripple_xmanager_metricxscscinode.h"
#include "xmanager/ripple_xmanager_metriccapturenode.h"
#include "xmanager/ripple_xmanager_metricintegratenode.h"
#include "xmanager/ripple_xmanager_metric.h"
#include "xmanager/ripple_xmanager_metricprogressnode.h"

typedef struct RIPPLE_XMANAGER_METRICNODEOP
{
    ripple_xmanager_metricnodetype                  type;
    char*                                           name;
    char*                                           desc;
    ripple_xmanager_metricnode*                     (*init)(void);
    bool                                            (*serial)(ripple_xmanager_metricnode* metricnode,
                                                              uint8** blk,
                                                              int* blksize,
                                                              int* blkstart);
    ripple_xmanager_metricnode*                     (*deserial)(uint8* blk,
                                                                int* blkstart);
    int                                             (*cmp)(void* s1, void* s2);
    void                                            (*destroy)(ripple_xmanager_metricnode* xmetricnode);
} ripple_xmanager_metricnodeop;


/*----------------------------metricnode begin----------------------------*/

void ripple_xmanager_metricnode_reset(ripple_xmanager_metricnode* metricnode)
{
    metricnode->type = RIPPLE_XMANAGER_METRICNODETYPE_NOP;
    metricnode->conf = NULL;
    metricnode->data = NULL;
    metricnode->traildir = NULL;
    metricnode->name = NULL;
    metricnode->remote = false;
    metricnode->stat = RIPPLE_XMANAGER_METRICNODESTAT_NOP;
}

/* 计算 metricnode 所占用的内存 */
int ripple_xmanager_metricnode_serialsize(ripple_xmanager_metricnode* metricnode)
{
    int len = 0;
    if (NULL == metricnode)
    {
        return 0;
    }

    /* 计算总长度 */
    /* type 4 + remote 1 + stat 4 + namelen 4 + datalen 4 + conflen 4 + traillen 4*/
    len = 4 + 1 + 4 + 4 + 4 + 4 + 4;
    if (NULL != metricnode->name)
    {
        len += strlen(metricnode->name);
    }

    if (NULL != metricnode->data)
    {
        len += strlen(metricnode->data);
    }

    if (NULL != metricnode->conf)
    {
        len += strlen(metricnode->conf);
    }

    if (NULL != metricnode->traildir)
    {
        len += strlen(metricnode->traildir);
    }

    return len;
}

/* 序列化 metricnode */
void ripple_xmanager_metricnode_serial(ripple_xmanager_metricnode* metricnode,
                                       uint8* blk,
                                       int* blkstart)
{
    int len                 = 0;
    int ivalue              = 0;
    uint8* uptr             = NULL;

    uptr = blk + *blkstart;

    /* 类型 */
    ivalue = metricnode->type;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    *blkstart += 4;

    /* remote */
    rmemcpy1(uptr, 0, &metricnode->remote, 1);
    uptr += 1;
    *blkstart += 1;

    /* 状态 */
    ivalue = metricnode->stat;
    if (RIPPLE_XMANAGER_METRICNODESTAT_ONLINE == ivalue)
    {
        ivalue = RIPPLE_XMANAGER_METRICNODESTAT_OFFLINE;
    }

    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    *blkstart += 4;

    /* 
     * 名称
     *  1、先构建名称
     *  2、再构建长度
     */
    uptr += 4;
    if (NULL == metricnode->name || '\0' == metricnode->name[0])
    {
        ivalue = len = 0;
    }
    else
    {
        len = ivalue = strlen(metricnode->name);
        rmemcpy1(uptr, 0, metricnode->name, ivalue);
    }
    uptr -= 4;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    uptr += len;
    *blkstart += 4;
    *blkstart += len;

    /* data 目录 */
    uptr += 4;
    if (NULL == metricnode->data || '\0' == metricnode->data[0])
    {
        ivalue = len = 0;
    }
    else
    {
        len = ivalue = strlen(metricnode->data);
        rmemcpy1(uptr, 0, metricnode->data, ivalue);
    }
    uptr -= 4;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    uptr += len;
    *blkstart += 4;
    *blkstart += len;

    /* conf */
    uptr += 4;
    if (NULL == metricnode->conf || '\0' == metricnode->conf[0])
    {
        ivalue = len = 0;
    }
    else
    {
        len = ivalue = strlen(metricnode->conf);
        rmemcpy1(uptr, 0, metricnode->conf, ivalue);
    }
    uptr -= 4;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    uptr += len;
    *blkstart += 4;
    *blkstart += len;

    /* trail */
    uptr += 4;
    if (NULL == metricnode->traildir || '\0' == metricnode->traildir[0])
    {
        ivalue = len = 0;
    }
    else
    {
        len = ivalue = strlen(metricnode->traildir);
        rmemcpy1(uptr, 0, metricnode->traildir, ivalue);
    }
    uptr -= 4;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    uptr += len;
    *blkstart += 4;
    *blkstart += len;

    return;
}

/* 反序列化 */
bool ripple_xmanager_metricnode_deserial(ripple_xmanager_metricnode* metricnode,
                                         uint8* blk,
                                         int* blkstart)
{
    int ivalue          = 0;
    uint8* uptr         = NULL;

    uptr = blk;
    uptr += *blkstart;

    /* 类型 */
    rmemcpy1(&ivalue, 0, uptr, 4);
    metricnode->type = r_ntoh32(ivalue);
    uptr += 4;
    *blkstart += 4;

    /* remote */
    rmemcpy1(&metricnode->remote, 0, uptr, 1);
    uptr += 1;
    *blkstart += 1;

    /* 状态 */
    rmemcpy1(&ivalue, 0, uptr, 4);
    metricnode->stat = r_ntoh32(ivalue);
    uptr += 4;
    *blkstart += 4;

    /* 
     * 名称
     *  1、获取长度
     *  2、获取名称
     */
    rmemcpy1(&ivalue, 0, uptr, 4);
    ivalue = r_ntoh32(ivalue);
    uptr += 4;
    *blkstart += 4;

    if (0 != ivalue)
    {
        ivalue += 1;
        metricnode->name = rmalloc0(ivalue);
        if (NULL == metricnode->name)
        {
            elog(RLOG_WARNING, "xmanager metric node deserial error, out of memory");
            return false;
        }
        rmemset0(metricnode->name, 0, '\0', ivalue);
        ivalue -= 1;
        rmemcpy0(metricnode->name, 0, uptr, ivalue);
        uptr += ivalue;
        *blkstart += ivalue;
    }

    /* 
     * data目录
     *  1、获取长度
     *  2、获取内容
     */
    rmemcpy1(&ivalue, 0, uptr, 4);
    ivalue = r_ntoh32(ivalue);
    uptr += 4;
    *blkstart += 4;

    if (0 != ivalue)
    {
        ivalue += 1;
        metricnode->data = rmalloc0(ivalue);
        if (NULL == metricnode->data)
        {
            elog(RLOG_WARNING, "xmanager metric node deserial error, out of memory");
            return false;
        }
        rmemset0(metricnode->data, 0, '\0', ivalue);
        ivalue -= 1;
        rmemcpy0(metricnode->data, 0, uptr, ivalue);
        uptr += ivalue;
        *blkstart += ivalue;
    }

    /* 
     * conf目录
     *  1、获取长度
     *  2、获取内容
     */
    rmemcpy1(&ivalue, 0, uptr, 4);
    ivalue = r_ntoh32(ivalue);
    uptr += 4;
    *blkstart += 4;

    if (0 != ivalue)
    {
        ivalue += 1;
        metricnode->conf = rmalloc0(ivalue);
        if (NULL == metricnode->conf)
        {
            elog(RLOG_WARNING, "xmanager metric node deserial error, out of memory");
            return false;
        }
        rmemset0(metricnode->conf, 0, '\0', ivalue);
        ivalue -= 1;
        rmemcpy0(metricnode->conf, 0, uptr, ivalue);
        uptr += ivalue;
        *blkstart += ivalue;
    }

    /* 
     * trail目录
     *  1、获取长度
     *  2、获取内容
     */
    rmemcpy1(&ivalue, 0, uptr, 4);
    ivalue = r_ntoh32(ivalue);
    uptr += 4;
    *blkstart += 4;

    if (0 != ivalue)
    {
        ivalue += 1;
        metricnode->traildir = rmalloc0(ivalue);
        if (NULL == metricnode->traildir)
        {
            elog(RLOG_WARNING, "xmanager metric node deserial error, out of memory");
            return false;
        }
        rmemset0(metricnode->traildir, 0, '\0', ivalue);
        ivalue -= 1;
        rmemcpy0(metricnode->traildir, 0, uptr, ivalue);
        uptr += ivalue;
        *blkstart += ivalue;
    }
    return true;
}

static ripple_xmanager_metricnodeop     m_xmetricnodeops[] =
{
    {
        RIPPLE_XMANAGER_METRICNODETYPE_NOP,
        "nop",
        "XManager Metric Node NOP",
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
    },
    {
        RIPPLE_XMANAGER_METRICNODETYPE_CAPTURE,
        "capture",
        "XManager Metric Capture Node",
        ripple_xmanager_metriccapturenode_init,
        ripple_xmanager_metriccapturenode_serial,
        ripple_xmanager_metriccapturenode_deserial,
        NULL,
        ripple_xmanager_metriccapturenode_destroy
    },
    {
        RIPPLE_XMANAGER_METRICNODETYPE_INTEGRATE,
        "integrate",
        "XManager Metric Integrate Node",
        ripple_xmanager_metricintegratenode_init,
        ripple_xmanager_metricintegratenode_serial,
        ripple_xmanager_metricintegratenode_deserial,
        NULL,
        ripple_xmanager_metricintegratenode_destroy
    },
    {
        RIPPLE_XMANAGER_METRICNODETYPE_PGRECEIVELOG,
        "pgreceivelog",
        "XManager Metric PGReceivelog Node",
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
    },
    {
        RIPPLE_XMANAGER_METRICNODETYPE_PROCESS,
        "process",
        "XManager Metric Process Node",
        ripple_xmanager_metricprogressnode_init,
        ripple_xmanager_metricprogressnode_serial,
        ripple_xmanager_metricprogressnode_deserial,
        NULL,
        ripple_xmanager_metricprogressnode_destroy
    },
    {
        RIPPLE_XMANAGER_METRICNODETYPE_ALL,
        "all",
        "XManager Metric ALL Node",
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
    },
    {
        RIPPLE_XMANAGER_METRICNODETYPE_MANAGER,
        "manager",
        "XManager Metric Xmanager Node",
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
    },
    {
        RIPPLE_XMANAGER_METRICNODETYPE_XSCSCI,
        "xscsci",
        "XManager Metric XScsci Node",
        ripple_xmanager_metricxscscinode_init,
        NULL,
        NULL,
        ripple_xmanager_metricxscscinode_cmp,
        ripple_xmanager_metricxscscinode_destroy
    },
    {
        RIPPLE_XMANAGER_METRICNODETYPE_MAX,
        "max",
        "XManager Metric Node Max",
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
    }
};

ripple_xmanager_metricnode* ripple_xmanager_metricnode_init(ripple_xmanager_metricnodetype nodetype)
{
    if (NULL == m_xmetricnodeops[nodetype].init)
    {
        elog(RLOG_WARNING, "%s unsupport init", m_xmetricnodeops[nodetype].desc);
        return NULL;
    }

    if (nodetype != m_xmetricnodeops[nodetype].type)
    {
        elog(RLOG_WARNING,
             "metric node init need type %d, but now type:%d ",
             m_xmetricnodeops[nodetype].type, nodetype);
        return NULL;
    }

    return m_xmetricnodeops[nodetype].init();
}

char* ripple_xmanager_metricnode_getname(ripple_xmanager_metricnodetype nodetype)
{
    return m_xmetricnodeops[nodetype].name;
}

/* metricnode 比较函数 */
int ripple_xmanager_metricnode_cmp(void* s1, void* s2)
{
    ripple_xmanager_metricnode* mnode1 = NULL;
    ripple_xmanager_metricnode* mnode2 = NULL;

    mnode1 = (ripple_xmanager_metricnode*)s1;
    mnode2 = (ripple_xmanager_metricnode*)s2;

    if (mnode1->type != mnode2->type)
    {
        return 1;
    }

    if (NULL == mnode1->name && NULL != mnode2->name)
    {
        return 1;
    }

    if (NULL == mnode2->name && NULL != mnode1->name)
    {
        return 1;
    }

    if (0 != strcmp(mnode1->name, mnode2->name))
    {
        return 1;
    }

    if (NULL != m_xmetricnodeops[mnode1->type].cmp)
    {
        return m_xmetricnodeops[mnode1->type].cmp(s1, s2);
    }
    return 0;
}

void ripple_xmanager_metricnode_destroy(ripple_xmanager_metricnode* metricnode)
{
    if (NULL == metricnode)
    {
        return;
    }

    if (NULL != metricnode->conf)
    {
        rfree(metricnode->conf);
    }

    if (NULL != metricnode->data)
    {
        rfree(metricnode->data);
    }

    if (NULL != metricnode->name)
    {
        rfree(metricnode->name);
    }

    if (NULL != metricnode->traildir)
    {
        rfree(metricnode->traildir);
    }

    if (NULL == m_xmetricnodeops[metricnode->type].destroy)
    {
        elog(RLOG_WARNING, "%s unsupport destroy", m_xmetricnodeops[metricnode->type].desc);
        return;
    }

    m_xmetricnodeops[metricnode->type].destroy(metricnode);
}

void ripple_xmanager_metricnode_destroyvoid(void* args)
{
    return ripple_xmanager_metricnode_destroy((ripple_xmanager_metricnode*)args);
}

/* 将 metricnode 落盘 */
void ripple_xmanager_metricnode_flush(dlist* dlmetricnodes)
{
    bool bfailed                            = false;
    int fd                                  = -1;
    int blkstart                            = 0;
    int blksize                             = RIPPLE_XMANAGER_METRICNODEBLKSIZE;
    uint8* blk                              = NULL;
    dlistnode* dlnode                       = NULL;
    ripple_xmanager_metricnode* xmetricnode = NULL;
    char metricfile[]                       = "metric/xmetricnode.dat";
    char metricfiletmp[]                    = "metric/xmetricnode.dat.tmp";

    if (dlist_isnull(dlmetricnodes))
    {
        return;
    }

    for (dlnode = dlmetricnodes->head; NULL != dlnode; dlnode = dlnode->next)
    {
        xmetricnode = (ripple_xmanager_metricnode*)dlnode->value;
        if (RIPPLE_XMANAGER_METRICNODETYPE_PROCESS < xmetricnode->type)
        {
            continue;
        }

        if (NULL == blk)
        {
            blk = rmalloc0(blksize);
            if (NULL == blk)
            {
                return;
            }
            rmemset0(blk, 0, '\0', blksize);
            blkstart = 0;
        }

        if (NULL == m_xmetricnodeops[xmetricnode->type].serial)
        {
            bfailed = true;
            elog(RLOG_WARNING, "%s unsupport serial", m_xmetricnodeops[xmetricnode->type].desc);
            continue;
        }

        /* 执行序列化 */
        if (false == m_xmetricnodeops[xmetricnode->type].serial(xmetricnode,
                                                                &blk,
                                                                &blksize,
                                                                &blkstart))
        {
            bfailed = true;
            elog(RLOG_WARNING, "%s serial error", m_xmetricnodeops[xmetricnode->type].desc);
            break;
        }

        if (-1 == fd)
        {
            fd = FileOpen(metricfiletmp, O_RDWR | O_CREAT, g_file_create_mode);
            if (-1 == fd)
            {
                bfailed = true;
                elog(RLOG_WARNING, "can not open file %s, %s", metricfiletmp, strerror(errno));
                break;
            }
        }

        /* 将数据落盘 */
        if (blkstart != FileWrite(fd, (char*)blk, blkstart))
        {
            bfailed = true;
            elog(RLOG_WARNING, "write metric node to file error, %s", strerror(errno));
            break;
        }

        rmemset0(blk, 0, '\0', blkstart);
        blkstart = 0;
    }

    if (-1 != fd)
    {
        FileClose(fd);
        fd = -1;
    }

    if (NULL != blk)
    {
        rfree(blk);
    }

    if (true == bfailed)
    {
        return;
    }

    /* 重命名 */
    durable_rename(metricfiletmp, metricfile, RLOG_DEBUG);
    return;
}

/* 加载 metircnode.dat 文件 */
bool ripple_xmanager_metricnode_load(dlist** pdlmetricnodes)
{
    bool benlarge                           = false;
    int fd                                  = -1;
    int nodelen                             = 0;
    int nodetype                            = 0;

    /* 读取的长度 */
    int rlen                                = 0;
    /* 文件总长度 */
    int filesize                            = 0;
    /* 解析到基于文件头的偏移 */
    int fileoffset                          = 0;
    /* blk 中解析到的位置 */
    int blkstart                            = 0;
    /* blk 中数据结束的位置 */
    int blkend                              = 0;
    /* blk 中的空间 */
    int blksize                             = RIPPLE_XMANAGER_METRICNODEBLKSIZE;
    uint8* blk                              = NULL;
    uint8* uptr                             = NULL;
    dlist* dlmetricnodes                    = NULL;
    ripple_xmanager_metricnode* xmetricnode = NULL;
    char metricfile[]                       = "metric/xmetricnode.dat";
    
    /*
     * 查看文件是否存在, 文件不存在证明是首次
     */
    if (false == FileExist(metricfile))
    {
        elog(RLOG_WARNING, "metric node file not exist, %s", metricfile);
        return true;
    }

    /* 打开文件获取文件大小 */
    fd = FileOpen(metricfile, O_RDONLY, 0);
    if (-1 == fd)
    {
        elog(RLOG_WARNING, "open file %s error, %s", metricfile, strerror(errno));
        return false;
    }

    /* 获取文件总长度 */
    filesize = FileSize(fd);

    /* 重置文件指针到头部 */
    FileSeek(fd, 0);

    /* 预先申请空间 */
    blk = rmalloc0(blksize);
    if (NULL == blk)
    {
        elog(RLOG_WARNING, "load metric file error, out of memory");
        goto ripple_xmanager_metricnode_load_error;
    }
    rmemset0(blk, 0, '\0', blksize);

    /* 没有解析完 */
    while (filesize > fileoffset)
    {
        benlarge = false;
        /* 加载数据并解析 */
        if (blksize > (filesize - fileoffset))
        {
            rlen = filesize - fileoffset;
        }
        else
        {
            rlen = blksize;
        }
        fileoffset += rlen;

        /* 校验是否需要扩容 */
        while ((rlen + blkend) > blksize)
        {
            benlarge = true;
            blksize *= 2;
            continue;
        }

        if (true == benlarge)
        {
            /* 扩容并对扩容后的空间初始化 */
            uptr = rrealloc0(blk, blksize);
            if (NULL == uptr)
            {
                elog(RLOG_WARNING, "realloc error, out of memory");
                goto ripple_xmanager_metricnode_load_error;
            }
            rmemset0(uptr, blkend, '\0', blksize - blkend);
            blk = uptr;
        }

        if (rlen != FileRead(fd, (char*)(blk + blkend), rlen))
        {
            elog(RLOG_WARNING, "read metricnode file error, %s", strerror(errno));
            goto ripple_xmanager_metricnode_load_error;
        }

        /* blk 中数据的总长度 */
        blkend += rlen;
        while (blkstart != blkend)
        {
            uptr = blk + blkstart;
            /* 
             * 8 字节的含义为, 4 字节的总长度, 4 字节的类型
             *  4 字节总长度查看数据是否足够
             *  4 字节的类型用于分发
            */
            if (8 >= (blkend - blkstart))
            {
                if (0 == blkstart)
                {
                    /* 为0,那么证明需要更多的空间 */
                    break;
                }

                /* 
                 * 不为0, 说明还有未解析的数据
                 *  1、重置 blkend 的尾部长度
                 *  2、搬运剩余空间
                 *  3、设置 blkstart 为 0
                 */
                blkend = blkend -blkstart;
                memmove(blk, uptr, blkend);
                blkstart = 0;
                rmemset0(blk, blkend, '\0', blksize - blkend);
                break;
            }

            /* 获取 4 字节节点总长度 */
            rmemcpy1(&nodelen, 0, uptr, 4);
            nodelen = r_ntoh32(nodelen);

            /* 节点的长度 */
            if (nodelen > (blkend - blkstart))
            {
                if (0 == blkstart)
                {
                    break;
                }

                /* 搬运数据到头部 */
                blkend = blkend -blkstart;
                memmove(blk, uptr, blkend);
                blkstart = 0;
                rmemset0(blk, blkend, '\0', blksize - blkend);
                break;
            }
            uptr += 4;

            /* 获取 4 字节类型 */
            rmemcpy1(&nodetype, 0, uptr, 4);
            nodetype = r_ntoh32(nodetype);

            if (NULL == m_xmetricnodeops[nodetype].deserial)
            {
                elog(RLOG_WARNING, "%s unsupport deserial.", m_xmetricnodeops[nodetype].desc);
                goto ripple_xmanager_metricnode_load_error;
            }

            /* blkstart 需要跳过 4 字节的总长度 */
            blkstart += 4;
            xmetricnode = m_xmetricnodeops[nodetype].deserial(blk, &blkstart);
            if (NULL == xmetricnode)
            {
                elog(RLOG_WARNING, "%s deserial error.", m_xmetricnodeops[nodetype].desc);
                goto ripple_xmanager_metricnode_load_error;
            }


            dlmetricnodes = dlist_put(dlmetricnodes, xmetricnode);
        }
    }

    if (blk)
    {
        rfree(blk);
    }

    if (-1 != fd)
    {
        FileClose(fd);
    }

    *pdlmetricnodes = dlmetricnodes;
    return true;
ripple_xmanager_metricnode_load_error:

    if (blk)
    {
        rfree(blk);
    }

    if (-1 != fd)
    {
        FileClose(fd);
    }

    dlist_free(dlmetricnodes, ripple_xmanager_metricnode_destroyvoid);
    return false;
}

/*----------------------------metricnode   end----------------------------*/

/* 初始化 */
ripple_xmanager_metricregnode* ripple_xmanager_metricregnode_init(void)
{
    ripple_xmanager_metricregnode* mregnode = NULL;

    mregnode = rmalloc0(sizeof(ripple_xmanager_metricregnode));
    if (NULL == mregnode)
    {
        elog(RLOG_WARNING, "metric ");
        return NULL;
    }
    rmemset0(mregnode, 0, '\0', sizeof(ripple_xmanager_metricregnode));
    mregnode->errcode = 0;
    mregnode->metricfd2node = NULL;
    mregnode->msg = NULL;
    mregnode->nodetype = RIPPLE_XMANAGER_METRICNODETYPE_NOP;
    mregnode->result = 0;
    mregnode->msgtype = RIPPLE_XMANAGER_MSG_NOP;
    return mregnode;
}

/* 释放 */
void ripple_xmanager_metricregnode_destroy(ripple_xmanager_metricregnode* mregnode)
{
    if (NULL == mregnode)
    {
        return;
    }

    ripple_xmanager_metricfd2node_destroy(mregnode->metricfd2node);

    if (NULL != mregnode->msg)
    {
        rfree(mregnode->msg);
    }

    rfree(mregnode);
}

ripple_xmanager_metricfd2node* ripple_xmanager_metricfd2node_init(void)
{
    ripple_xmanager_metricfd2node* fd2node = NULL;

    fd2node = rmalloc0(sizeof(ripple_xmanager_metricfd2node));
    if (NULL == fd2node)
    {
        elog(RLOG_WARNING, "metricfd2node init out of memory");
        return NULL;
    }

    fd2node->fd = -1;
    fd2node->metricnode = NULL;
    return fd2node;
}

int ripple_xmanager_metricfd2node_cmp(void* s1, void* s2)
{
    int fd = -1;
    ripple_xmanager_metricfd2node* fd2node = NULL;

    /* 比较 */
    fd = (int)((uintptr_t)s1);
    fd2node =  (ripple_xmanager_metricfd2node*)s2;
    if (fd != fd2node->fd)
    {
        return 1;
    }
    return 0;
}

/* 用 metricnode 进行比较 */
int ripple_xmanager_metricfd2node_cmp2(void* s1, void* s2)
{
    ripple_xmanager_metricnode* metricnode = NULL;
    ripple_xmanager_metricfd2node* fd2node = NULL;

    metricnode = (ripple_xmanager_metricnode*)s1;
    fd2node = (ripple_xmanager_metricfd2node*)s2;

    return ripple_xmanager_metricnode_cmp(metricnode, fd2node->metricnode);
}

void ripple_xmanager_metricfd2node_destroy(ripple_xmanager_metricfd2node* metricfd2node)
{
    if (NULL == metricfd2node)
    {
        return;
    }

    metricfd2node->fd = -1;
    rfree(metricfd2node);
}

void ripple_xmanager_metricfd2node_destroyvoid(void* args)
{
    ripple_xmanager_metricfd2node_destroy((ripple_xmanager_metricfd2node*)args);
}
