#include "app_incl.h"
#include "port/file/fd.h"
#include "port/net/net.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "queue/queue.h"
#include "command/cmd.h"
#include "net/netiomp/netiomp.h"
#include "net/netiomp/netiomp_poll.h"
#include "net/netpacket/netpacket.h"
#include "net/netpool.h"
#include "xmanager/xmanager_msg.h"
#include "xmanager/xmanager_metricasyncmsg.h"
#include "xmanager/xmanager_metricnode.h"
#include "xmanager/xmanager_metricxscscinode.h"
#include "xmanager/xmanager_metriccapturenode.h"
#include "xmanager/xmanager_metricintegratenode.h"
#include "xmanager/xmanager_metric.h"
#include "xmanager/xmanager_metricprogressnode.h"

typedef struct XMANAGER_METRICNODEOP
{
    xmanager_metricnodetype type;
    char*                   name;
    char*                   desc;
    xmanager_metricnode* (*init)(void);
    bool (*serial)(xmanager_metricnode* metricnode, uint8** blk, int* blksize, int* blkstart);
    xmanager_metricnode* (*deserial)(uint8* blk, int* blkstart);
    int (*cmp)(void* s1, void* s2);
    void (*destroy)(xmanager_metricnode* xmetricnode);
} xmanager_metricnodeop;

/*----------------------------metricnode begin----------------------------*/

void xmanager_metricnode_reset(xmanager_metricnode* metricnode)
{
    metricnode->type = XMANAGER_METRICNODETYPE_NOP;
    metricnode->conf = NULL;
    metricnode->data = NULL;
    metricnode->traildir = NULL;
    metricnode->name = NULL;
    metricnode->remote = false;
    metricnode->stat = XMANAGER_METRICNODESTAT_NOP;
}

/* Calculate memory usage of metricnode */
int xmanager_metricnode_serialsize(xmanager_metricnode* metricnode)
{
    int len = 0;
    if (NULL == metricnode)
    {
        return 0;
    }

    /* Calculate total length */
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

/* Serialize metricnode */
void xmanager_metricnode_serial(xmanager_metricnode* metricnode, uint8* blk, int* blkstart)
{
    int    len = 0;
    int    ivalue = 0;
    uint8* uptr = NULL;

    uptr = blk + *blkstart;

    /* Type */
    ivalue = metricnode->type;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    *blkstart += 4;

    /* remote */
    rmemcpy1(uptr, 0, &metricnode->remote, 1);
    uptr += 1;
    *blkstart += 1;

    /* Status */
    ivalue = metricnode->stat;
    if (XMANAGER_METRICNODESTAT_ONLINE == ivalue)
    {
        ivalue = XMANAGER_METRICNODESTAT_OFFLINE;
    }

    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    *blkstart += 4;

    /*
     * Name
     *  1. First build the name
     *  2. Then build the length
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

    /* data directory */
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

/* Deserialization */
bool xmanager_metricnode_deserial(xmanager_metricnode* metricnode, uint8* blk, int* blkstart)
{
    int    ivalue = 0;
    uint8* uptr = NULL;

    uptr = blk;
    uptr += *blkstart;

    /* Type */
    rmemcpy1(&ivalue, 0, uptr, 4);
    metricnode->type = r_ntoh32(ivalue);
    uptr += 4;
    *blkstart += 4;

    /* remote */
    rmemcpy1(&metricnode->remote, 0, uptr, 1);
    uptr += 1;
    *blkstart += 1;

    /* Status */
    rmemcpy1(&ivalue, 0, uptr, 4);
    metricnode->stat = r_ntoh32(ivalue);
    uptr += 4;
    *blkstart += 4;

    /*
     * Name
     *  1. Get length
     *  2. Get name
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
     * data directory
     *  1. Get length
     *  2. Get content
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
     * conf directory
     *  1. Get length
     *  2. Get content
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
     * trail directory
     *  1. Get length
     *  2. Get content
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

static xmanager_metricnodeop m_xmetricnodeops[] = {
    {XMANAGER_METRICNODETYPE_NOP, "nop", "XManager Metric Node NOP", NULL, NULL, NULL, NULL, NULL},
    {XMANAGER_METRICNODETYPE_CAPTURE, "capture", "XManager Metric Capture Node",
     xmanager_metriccapturenode_init, xmanager_metriccapturenode_serial,
     xmanager_metriccapturenode_deserial, NULL, xmanager_metriccapturenode_destroy},
    {XMANAGER_METRICNODETYPE_INTEGRATE, "integrate", "XManager Metric Integrate Node",
     xmanager_metricintegratenode_init, xmanager_metricintegratenode_serial,
     xmanager_metricintegratenode_deserial, NULL, xmanager_metricintegratenode_destroy},
    {XMANAGER_METRICNODETYPE_PGRECEIVELOG, "pgreceivelog", "XManager Metric PGReceivelog Node",
     NULL, NULL, NULL, NULL, NULL},
    {XMANAGER_METRICNODETYPE_PROCESS, "process", "XManager Metric Process Node",
     xmanager_metricprogressnode_init, xmanager_metricprogressnode_serial,
     xmanager_metricprogressnode_deserial, NULL, xmanager_metricprogressnode_destroy},
    {XMANAGER_METRICNODETYPE_ALL, "all", "XManager Metric ALL Node", NULL, NULL, NULL, NULL, NULL},
    {XMANAGER_METRICNODETYPE_MANAGER, "manager", "XManager Metric Xmanager Node", NULL, NULL, NULL,
     NULL, NULL},
    {XMANAGER_METRICNODETYPE_XSCSCI, "xscsci", "XManager Metric XScsci Node",
     xmanager_metricxscscinode_init, NULL, NULL, xmanager_metricxscscinode_cmp,
     xmanager_metricxscscinode_destroy},
    {XMANAGER_METRICNODETYPE_MAX, "max", "XManager Metric Node Max", NULL, NULL, NULL, NULL, NULL}};

xmanager_metricnode* xmanager_metricnode_init(xmanager_metricnodetype nodetype)
{
    if (NULL == m_xmetricnodeops[nodetype].init)
    {
        elog(RLOG_WARNING, "%s unsupport init", m_xmetricnodeops[nodetype].desc);
        return NULL;
    }

    if (nodetype != m_xmetricnodeops[nodetype].type)
    {
        elog(RLOG_WARNING, "metric node init need type %d, but now type:%d ",
             m_xmetricnodeops[nodetype].type, nodetype);
        return NULL;
    }

    return m_xmetricnodeops[nodetype].init();
}

char* xmanager_metricnode_getname(xmanager_metricnodetype nodetype)
{
    return m_xmetricnodeops[nodetype].name;
}

/* Metricnode comparison function */
int xmanager_metricnode_cmp(void* s1, void* s2)
{
    xmanager_metricnode* mnode1 = NULL;
    xmanager_metricnode* mnode2 = NULL;

    mnode1 = (xmanager_metricnode*)s1;
    mnode2 = (xmanager_metricnode*)s2;

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

void xmanager_metricnode_destroy(xmanager_metricnode* metricnode)
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

void xmanager_metricnode_destroyvoid(void* args)
{
    return xmanager_metricnode_destroy((xmanager_metricnode*)args);
}

/* Persist metricnode to disk */
void xmanager_metricnode_flush(dlist* dlmetricnodes)
{
    bool                 bfailed = false;
    int                  fd = -1;
    int                  blkstart = 0;
    int                  blksize = XMANAGER_METRICNODEBLKSIZE;
    uint8*               blk = NULL;
    dlistnode*           dlnode = NULL;
    xmanager_metricnode* xmetricnode = NULL;
    char                 metricfile[] = "metric/xmetricnode.dat";
    char                 metricfiletmp[] = "metric/xmetricnode.dat.tmp";

    if (dlist_isnull(dlmetricnodes))
    {
        return;
    }

    for (dlnode = dlmetricnodes->head; NULL != dlnode; dlnode = dlnode->next)
    {
        xmetricnode = (xmanager_metricnode*)dlnode->value;
        if (XMANAGER_METRICNODETYPE_PROCESS < xmetricnode->type)
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

        /* Perform serialization */
        if (false ==
            m_xmetricnodeops[xmetricnode->type].serial(xmetricnode, &blk, &blksize, &blkstart))
        {
            bfailed = true;
            elog(RLOG_WARNING, "%s serial error", m_xmetricnodeops[xmetricnode->type].desc);
            break;
        }

        if (-1 == fd)
        {
            fd = osal_file_open(metricfiletmp, O_RDWR | O_CREAT, g_file_create_mode);
            if (-1 == fd)
            {
                bfailed = true;
                elog(RLOG_WARNING, "can not open file %s, %s", metricfiletmp, strerror(errno));
                break;
            }
        }

        /* Write data to disk */
        if (blkstart != osal_file_write(fd, (char*)blk, blkstart))
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
        osal_file_close(fd);
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

    /* Rename */
    osal_durable_rename(metricfiletmp, metricfile, RLOG_DEBUG);
    return;
}

/* Load metricnode.dat file */
bool xmanager_metricnode_load(dlist** pdlmetricnodes)
{
    bool benlarge = false;
    int  fd = -1;
    int  nodelen = 0;
    int  nodetype = 0;

    /* Length read */
    int rlen = 0;
    /* Total file length */
    int filesize = 0;
    /* Offset from file header */
    int fileoffset = 0;
    /* Position parsed in blk */
    int blkstart = 0;
    /* End position of data in blk */
    int blkend = 0;
    /* Space in blk */
    int                  blksize = XMANAGER_METRICNODEBLKSIZE;
    uint8*               blk = NULL;
    uint8*               uptr = NULL;
    dlist*               dlmetricnodes = NULL;
    xmanager_metricnode* xmetricnode = NULL;
    char                 metricfile[] = "metric/xmetricnode.dat";

    /*
     * Check if file exists, if not, this is the first run
     */
    if (false == osal_file_exist(metricfile))
    {
        elog(RLOG_WARNING, "metric node file not exist, %s", metricfile);
        return true;
    }

    /* Open file and get file size */
    fd = osal_file_open(metricfile, O_RDONLY, 0);
    if (-1 == fd)
    {
        elog(RLOG_WARNING, "open file %s error, %s", metricfile, strerror(errno));
        return false;
    }

    /* Get total file length */
    filesize = osal_file_size(fd);

    /* Reset file pointer to beginning */
    osal_file_seek(fd, 0);

    /* Pre-allocate space */
    blk = rmalloc0(blksize);
    if (NULL == blk)
    {
        elog(RLOG_WARNING, "load metric file error, out of memory");
        goto xmanager_metricnode_load_error;
    }
    rmemset0(blk, 0, '\0', blksize);

    /* Not yet fully parsed */
    while (filesize > fileoffset)
    {
        benlarge = false;
        /* Load data and parse */
        if (blksize > (filesize - fileoffset))
        {
            rlen = filesize - fileoffset;
        }
        else
        {
            rlen = blksize;
        }
        fileoffset += rlen;

        /* Check if expansion is needed */
        while ((rlen + blkend) > blksize)
        {
            benlarge = true;
            blksize *= 2;
            continue;
        }

        if (true == benlarge)
        {
            /* Expand and initialize the expanded space */
            uptr = rrealloc0(blk, blksize);
            if (NULL == uptr)
            {
                elog(RLOG_WARNING, "realloc error, out of memory");
                goto xmanager_metricnode_load_error;
            }
            rmemset0(uptr, blkend, '\0', blksize - blkend);
            blk = uptr;
        }

        if (rlen != osal_file_read(fd, (char*)(blk + blkend), rlen))
        {
            elog(RLOG_WARNING, "read metricnode file error, %s", strerror(errno));
            goto xmanager_metricnode_load_error;
        }

        /* Total length of data in blk */
        blkend += rlen;
        while (blkstart != blkend)
        {
            uptr = blk + blkstart;
            /*
             * 8 bytes meaning: 4 bytes total length, 4 bytes type
             *  4 bytes total length to check if data is sufficient
             *  4 bytes type for dispatch
             */
            if (8 >= (blkend - blkstart))
            {
                if (0 == blkstart)
                {
                    /* If 0, more space is needed */
                    break;
                }

                /*
                 * If not 0, there is unparsed data
                 *  1. Reset blkend tail length
                 *  2. Move remaining space
                 *  3. Set blkstart to 0
                 */
                blkend = blkend - blkstart;
                memmove(blk, uptr, blkend);
                blkstart = 0;
                rmemset0(blk, blkend, '\0', blksize - blkend);
                break;
            }

            /* Get 4 bytes node total length */
            rmemcpy1(&nodelen, 0, uptr, 4);
            nodelen = r_ntoh32(nodelen);

            /* Node length */
            if (nodelen > (blkend - blkstart))
            {
                if (0 == blkstart)
                {
                    break;
                }

                /* Move data to beginning */
                blkend = blkend - blkstart;
                memmove(blk, uptr, blkend);
                blkstart = 0;
                rmemset0(blk, blkend, '\0', blksize - blkend);
                break;
            }
            uptr += 4;

            /* Get 4 bytes type */
            rmemcpy1(&nodetype, 0, uptr, 4);
            nodetype = r_ntoh32(nodetype);

            if (NULL == m_xmetricnodeops[nodetype].deserial)
            {
                elog(RLOG_WARNING, "%s unsupport deserial.", m_xmetricnodeops[nodetype].desc);
                goto xmanager_metricnode_load_error;
            }

            /* blkstart needs to skip 4 bytes total length */
            blkstart += 4;
            xmetricnode = m_xmetricnodeops[nodetype].deserial(blk, &blkstart);
            if (NULL == xmetricnode)
            {
                elog(RLOG_WARNING, "%s deserial error.", m_xmetricnodeops[nodetype].desc);
                goto xmanager_metricnode_load_error;
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
        osal_file_close(fd);
    }

    *pdlmetricnodes = dlmetricnodes;
    return true;
xmanager_metricnode_load_error:

    if (blk)
    {
        rfree(blk);
    }

    if (-1 != fd)
    {
        osal_file_close(fd);
    }

    dlist_free(dlmetricnodes, xmanager_metricnode_destroyvoid);
    return false;
}

/*----------------------------metricnode   end----------------------------*/

/* Initialize */
xmanager_metricregnode* xmanager_metricregnode_init(void)
{
    xmanager_metricregnode* mregnode = NULL;

    mregnode = rmalloc0(sizeof(xmanager_metricregnode));
    if (NULL == mregnode)
    {
        elog(RLOG_WARNING, "metric ");
        return NULL;
    }
    rmemset0(mregnode, 0, '\0', sizeof(xmanager_metricregnode));
    mregnode->errcode = 0;
    mregnode->metricfd2node = NULL;
    mregnode->msg = NULL;
    mregnode->nodetype = XMANAGER_METRICNODETYPE_NOP;
    mregnode->result = 0;
    mregnode->msgtype = XMANAGER_MSG_NOP;
    return mregnode;
}

/* Destroy */
void xmanager_metricregnode_destroy(xmanager_metricregnode* mregnode)
{
    if (NULL == mregnode)
    {
        return;
    }

    xmanager_metricfd2node_destroy(mregnode->metricfd2node);

    if (NULL != mregnode->msg)
    {
        rfree(mregnode->msg);
    }

    rfree(mregnode);
}

xmanager_metricfd2node* xmanager_metricfd2node_init(void)
{
    xmanager_metricfd2node* fd2node = NULL;

    fd2node = rmalloc0(sizeof(xmanager_metricfd2node));
    if (NULL == fd2node)
    {
        elog(RLOG_WARNING, "metricfd2node init out of memory");
        return NULL;
    }

    fd2node->fd = -1;
    fd2node->metricnode = NULL;
    return fd2node;
}

int xmanager_metricfd2node_cmp(void* s1, void* s2)
{
    int                     fd = -1;
    xmanager_metricfd2node* fd2node = NULL;

    /* Compare */
    fd = (int)((uintptr_t)s1);
    fd2node = (xmanager_metricfd2node*)s2;
    if (fd != fd2node->fd)
    {
        return 1;
    }
    return 0;
}

/* Compare using metricnode */
int xmanager_metricfd2node_cmp2(void* s1, void* s2)
{
    xmanager_metricnode*    metricnode = NULL;
    xmanager_metricfd2node* fd2node = NULL;

    metricnode = (xmanager_metricnode*)s1;
    fd2node = (xmanager_metricfd2node*)s2;

    return xmanager_metricnode_cmp(metricnode, fd2node->metricnode);
}

void xmanager_metricfd2node_destroy(xmanager_metricfd2node* metricfd2node)
{
    if (NULL == metricfd2node)
    {
        return;
    }

    metricfd2node->fd = -1;
    rfree(metricfd2node);
}

void xmanager_metricfd2node_destroyvoid(void* args)
{
    xmanager_metricfd2node_destroy((xmanager_metricfd2node*)args);
}
