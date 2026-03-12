/*
 * 组装待发送数据
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <memory.h>
#include <errno.h>

#include "ripple_c.h"
#include "xsynch_exbufferdata.h"
#include "xsynch_fe.h"
#include "xsynch_int.h"
#include "xsynch_febuildmsg.h"

typedef struct XSYNCH_FEBUILDMSG_ASSEMBLE
{
    xsynch_cmdtag           cmd;
    char*                   desc;

    bool (*assemble)(xsynch_cmd* cmd, xsynch_exbuffer msg);
} xsynch_febuildmsg_assemble;

/* 身份标识消息 */
static bool xsynch_febuildmsg_identitycmdassemble(xsynch_cmd* cmd, xsynch_exbuffer msg)
{
    int msglen = 0;
    int msgjobtype = 0;
    int msgjobnamelen = 0;
    int msgtype = T_XSYNCH_IDENTITYCMD;
    char* cptr = NULL;
    xsynch_identitycmd* icmd = (xsynch_identitycmd*)cmd;

    /* 字节序转换 */
    msglen = 8;
    msgtype = r_hton32(msgtype);
    msglen += 4;

    msgjobtype = icmd->kind;
    msgjobtype = r_hton32(msgjobtype);
    msglen += 4;

    if (NULL == icmd->jobname || '\0' == icmd->jobname[0])
    {
        msgjobnamelen = 0;
    }
    else
    {
        msgjobnamelen = strlen(icmd->jobname);
    }
    msglen += 4;
    msglen += msgjobnamelen;

    if (false == xsynch_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    /* 
     * 将数据加入到待发送缓存中
     */
    cptr = msg->data + msg->len;

    /* 总长度 */
    msglen = r_hton32(msglen);
    memcpy(cptr, &msglen, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, 暂时不用 */
    msg->len += 4;
    cptr += 4;

    /* 消息类型 */
    memcpy(cptr, &msgtype, 4);
    msg->len += 4;
    cptr += 4;

    /* 作业类型 */
    memcpy(cptr, &msgjobtype, 4);
    msg->len += 4;
    cptr += 4;

    /* 作业名称 */
    if (0 == msgjobnamelen)
    {
        memcpy(cptr, &msgjobnamelen, 4);
        msg->len += 4;
        cptr += 4;
        return true;
    }

    /* 先填充名称 */
    cptr += 4;
    memcpy(cptr, icmd->jobname, msgjobnamelen);
    msg->len += msgjobnamelen;

    /* 再次填充长度 */
    cptr -= 4;
    msgjobnamelen = r_hton32(msgjobnamelen);
    memcpy(cptr, &msgjobnamelen, 4);
    msg->len += 4;

    return true;
}

/* create标识消息 */
static bool xsynch_febuildmsg_createcmdassemble(xsynch_cmd* cmd, xsynch_exbuffer msg)
{
    int msglen = 0;
    int ivalue = 0;
    int valuelen = 0;
    int msgjobtype = 0;
    int msgjobnamelen = 0;
    int msgtype = T_XSYNCH_CREATECMD;
    char* cptr = NULL;
    ListCell* lc = NULL;
    xsynch_job* job = NULL;
    xsynch_createcmd* createcmd = (xsynch_createcmd*)cmd;

    msglen = 4 + 4;
    msgtype = r_hton32(msgtype);
    msglen += 4;

    msgjobtype = createcmd->kind;
    msgjobtype = r_hton32(createcmd->kind);
    msglen += 4;

    if (NULL == createcmd->name || '\0' == createcmd->name[0])
    {
        msgjobnamelen = 0;
    }
    else
    {
        msgjobnamelen = strlen(createcmd->name);
    }
    msglen += 4;
    msglen += msgjobnamelen;

    /* progress job len */
    if (NULL != createcmd->job)
    {
        /* job cnt */
        msglen += 4;

        foreach(lc, createcmd->job)
        {
            job = (xsynch_job*)lfirst(lc);

            /* jobtype 4 + jobnamelen 4 */
            msglen += (4 + 4);
            msglen += strlen(job->jobname);
        }
    }

    if (false == xsynch_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    cptr = msg->data + msg->len;

    /* 总长度 */
    msglen = r_hton32(msglen);
    memcpy(cptr, &msglen, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, 暂时不用 */
    msg->len += 4;
    cptr += 4;

    /* 消息类型 */
    memcpy(cptr, &msgtype, 4);
    msg->len += 4;
    cptr += 4;

    /* 作业类型 */
    memcpy(cptr, &msgjobtype, 4);
    msg->len += 4;
    cptr += 4;

    /* 作业名称 */
    if (0 == msgjobnamelen)
    {
        memcpy(cptr, &msgjobnamelen, 4);
        msg->len += 4;
        cptr += 4;
        return true;
    }

    /* 先填充名称 */
    cptr += 4;
    memcpy(cptr, createcmd->name, msgjobnamelen);
    msg->len += msgjobnamelen;

    /* 再次填充长度 */
    cptr -= 4;
    ivalue = msgjobnamelen;
    ivalue = r_hton32(ivalue);
    memcpy(cptr, &ivalue, 4);
    msg->len += 4;
    cptr += 4;
    cptr += msgjobnamelen;

    if (NULL != createcmd->job)
    {
        /* job cnt */
        valuelen = createcmd->job->length;
        valuelen = r_hton32(valuelen);
        memcpy(cptr, &valuelen, 4);
        msg->len += 4;
        cptr += 4;

        foreach(lc, createcmd->job)
        {
            job = (xsynch_job*)lfirst(lc);

            /* job type */
            ivalue  = job->kind;
            ivalue = r_hton32(ivalue);
            memcpy(cptr, &ivalue, 4);
            msg->len += 4;
            cptr += 4;

            /* jobnamelen */
            valuelen  = strlen(job->jobname);
            if (0 == valuelen)
            {
                memcpy(cptr, &msgjobnamelen, 4);
                msg->len += 4;
                cptr += 4;
                continue;
            }
            valuelen  = strlen(job->jobname);
            valuelen = r_hton32(valuelen);
            memcpy(cptr, &valuelen, 4);
            msg->len += 4;
            cptr += 4;

            /* jobname */
            valuelen  = strlen(job->jobname);
            memcpy(cptr, job->jobname, valuelen);
            msg->len += valuelen;
            cptr += valuelen;
        }
    }

    return true;
}

/* alter标识消息 */
static bool xsynch_febuildmsg_altercmdassemble(xsynch_cmd* cmd, xsynch_exbuffer msg)
{
    int8 action = 0;
    int msglen = 0;
    int ivalue = 0;
    int valuelen = 0;
    int msgjobtype = 0;
    int msgjobnamelen = 0;
    int msgtype = T_XSYNCH_ALTERCMD;
    char* cptr = NULL;
    ListCell* lc = NULL;
    xsynch_job* job = NULL;
    xsynch_altercmd* altercmd = (xsynch_altercmd*)cmd;

    msglen = 4 + 4;
    msgtype = r_hton32(msgtype);
    msglen += 4;

    msgjobtype = altercmd->kind;
    msgjobtype = r_hton32(altercmd->kind);
    msglen += 4;

    if (NULL == altercmd->name || '\0' == altercmd->name[0])
    {
        msgjobnamelen = 0;
    }
    else
    {
        msgjobnamelen = strlen(altercmd->name);
    }
    msglen += 4;
    msglen += msgjobnamelen;

    action = (int8)altercmd->action;
    msglen += 1;

    if (NULL != altercmd->job)
    {
        /* job cnt */
        msglen += 4;

        foreach(lc, altercmd->job)
        {
            job = (xsynch_job*)lfirst(lc);

            /* jobtype 4 + jobnamelen 4 */
            msglen += (4 + 4);
            msglen += strlen(job->jobname);
        }
    }

    if (false == xsynch_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    cptr = msg->data + msg->len;

    /* 总长度 */
    msglen = r_hton32(msglen);
    memcpy(cptr, &msglen, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, 暂时不用 */
    msg->len += 4;
    cptr += 4;

    /* 消息类型 */
    memcpy(cptr, &msgtype, 4);
    msg->len += 4;
    cptr += 4;

    /* 作业类型 */
    memcpy(cptr, &msgjobtype, 4);
    msg->len += 4;
    cptr += 4;

    /* 作业名称 */
    if (0 == msgjobnamelen)
    {
        memcpy(cptr, &msgjobnamelen, 4);
        msg->len += 4;
        cptr += 4;
        return true;
    }

    /* name */
    valuelen = msgjobnamelen;
    valuelen = r_hton32(valuelen);
    memcpy(cptr, &valuelen, 4);
    msg->len += 4;
    cptr += 4;

    /* jobname */
    memcpy(cptr, altercmd->name, msgjobnamelen);
    msg->len += msgjobnamelen;
    cptr += msgjobnamelen;

    /* 操作类型 */
    memcpy(cptr, &action, 1);
    msg->len += 1;
    cptr += 1;

    if (NULL != altercmd->job)
    {
        /* job cnt */
        valuelen  = altercmd->job->length;
        valuelen = r_hton32(valuelen);
        memcpy(cptr, &valuelen, 4);
        msg->len += 4;
        cptr += 4;

        foreach(lc, altercmd->job)
        {
            job = (xsynch_job*)lfirst(lc);

            /* job cnt */
            ivalue  = job->kind;
            ivalue = r_hton32(ivalue);
            memcpy(cptr, &ivalue, 4);
            msg->len += 4;
            cptr += 4;

            /* jobtype */
            valuelen  = strlen(job->jobname);
            if (0 == valuelen)
            {
                memcpy(cptr, &msgjobnamelen, 4);
                msg->len += 4;
                cptr += 4;
                continue;
            }

            /* jobnamelen */
            valuelen = r_hton32(valuelen);
            memcpy(cptr, &valuelen, 4);
            msg->len += 4;
            cptr += 4;

            /* jobname */
            valuelen  = strlen(job->jobname);
            memcpy(cptr, job->jobname, valuelen);
            msg->len += valuelen;
            cptr += valuelen;
        }
    }

    return true;
}

/* remove标识消息 */
static bool xsynch_febuildmsg_removecmdassemble(xsynch_cmd* cmd, xsynch_exbuffer msg)
{
    int msglen = 0;
    int msgjobtype = 0;
    int msgjobnamelen = 0;
    int msgtype = T_XSYNCH_REMOVECMD;
    char* cptr = NULL;
    xsynch_removecmd* removecmd = (xsynch_removecmd*)cmd;

    msglen = 4 + 4;
    msgtype = r_hton32(msgtype);
    msglen += 4;

    msgjobtype = removecmd->kind;
    msgjobtype = r_hton32(removecmd->kind);
    msglen += 4;

    if (NULL == removecmd->name || '\0' == removecmd->name[0])
    {
        msgjobnamelen = 0;
    }
    else
    {
        msgjobnamelen = strlen(removecmd->name);
    }
    msglen += 4;
    msglen += msgjobnamelen;

    if (false == xsynch_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    cptr = msg->data + msg->len;

    /* 总长度 */
    msglen = r_hton32(msglen);
    memcpy(cptr, &msglen, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, 暂时不用 */
    msg->len += 4;
    cptr += 4;

    /* 消息类型 */
    memcpy(cptr, &msgtype, 4);
    msg->len += 4;
    cptr += 4;

    /* 作业类型 */
    memcpy(cptr, &msgjobtype, 4);
    msg->len += 4;
    cptr += 4;

    /* 作业名称 */
    if (0 == msgjobnamelen)
    {
        memcpy(cptr, &msgjobnamelen, 4);
        msg->len += 4;
        cptr += 4;
        return true;
    }

    /* 先填充名称 */
    cptr += 4;
    memcpy(cptr, removecmd->name, msgjobnamelen);
    msg->len += msgjobnamelen;

    /* 再次填充长度 */
    cptr -= 4;
    msgjobnamelen = r_hton32(msgjobnamelen);
    memcpy(cptr, &msgjobnamelen, 4);
    msg->len += 4;

    return true;
}

/* drop标识消息 */
static bool xsynch_febuildmsg_dropcmdassemble(xsynch_cmd* cmd, xsynch_exbuffer msg)
{
    int msglen = 0;
    int msgjobtype = 0;
    int msgjobnamelen = 0;
    int msgtype = T_XSYNCH_DROPCMD;
    char* cptr = NULL;
    xsynch_dropcmd* dropcmd = (xsynch_dropcmd*)cmd;

    msglen = 4 + 4;
    msgtype = r_hton32(msgtype);
    msglen += 4;

    msgjobtype = dropcmd->kind;
    msgjobtype = r_hton32(dropcmd->kind);
    msglen += 4;

    if (NULL == dropcmd->name || '\0' == dropcmd->name[0])
    {
        msgjobnamelen = 0;
    }
    else
    {
        msgjobnamelen = strlen(dropcmd->name);
    }
    msglen += 4;
    msglen += msgjobnamelen;

    if (false == xsynch_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    cptr = msg->data + msg->len;

    /* 总长度 */
    msglen = r_hton32(msglen);
    memcpy(cptr, &msglen, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, 暂时不用 */
    msg->len += 4;
    cptr += 4;

    /* 消息类型 */
    memcpy(cptr, &msgtype, 4);
    msg->len += 4;
    cptr += 4;

    /* 作业类型 */
    memcpy(cptr, &msgjobtype, 4);
    msg->len += 4;
    cptr += 4;

    /* 作业名称 */
    if (0 == msgjobnamelen)
    {
        memcpy(cptr, &msgjobnamelen, 4);
        msg->len += 4;
        cptr += 4;
        return true;
    }

    /* 先填充名称 */
    cptr += 4;
    memcpy(cptr, dropcmd->name, msgjobnamelen);
    msg->len += msgjobnamelen;

    /* 再次填充长度 */
    cptr -= 4;
    msgjobnamelen = r_hton32(msgjobnamelen);
    memcpy(cptr, &msgjobnamelen, 4);
    msg->len += 4;

    return true;
}

/* init标识消息 */
static bool xsynch_febuildmsg_initcmdassemble(xsynch_cmd* cmd, xsynch_exbuffer msg)
{
    int msglen = 0;
    int msgjobtype = 0;
    int msgjobnamelen = 0;
    int msgtype = T_XSYNCH_INITCMD;
    char* cptr = NULL;
    xsynch_initcmd* initcmd = (xsynch_initcmd*)cmd;

    msglen = 4 + 4;
    msgtype = r_hton32(msgtype);
    msglen += 4;

    msgjobtype = initcmd->kind;
    msgjobtype = r_hton32(initcmd->kind);
    msglen += 4;

    if (NULL == initcmd->name || '\0' == initcmd->name[0])
    {
        msgjobnamelen = 0;
    }
    else
    {
        msgjobnamelen = strlen(initcmd->name);
    }
    msglen += 4;
    msglen += msgjobnamelen;

    if (false == xsynch_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    cptr = msg->data + msg->len;

    /* 总长度 */
    msglen = r_hton32(msglen);
    memcpy(cptr, &msglen, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, 暂时不用 */
    msg->len += 4;
    cptr += 4;

    /* 消息类型 */
    memcpy(cptr, &msgtype, 4);
    msg->len += 4;
    cptr += 4;

    /* 作业类型 */
    memcpy(cptr, &msgjobtype, 4);
    msg->len += 4;
    cptr += 4;

    /* 作业名称 */
    if (0 == msgjobnamelen)
    {
        memcpy(cptr, &msgjobnamelen, 4);
        msg->len += 4;
        cptr += 4;
        return true;
    }

    /* 先填充名称 */
    cptr += 4;
    memcpy(cptr, initcmd->name, msgjobnamelen);
    msg->len += msgjobnamelen;

    /* 再次填充长度 */
    cptr -= 4;
    msgjobnamelen = r_hton32(msgjobnamelen);
    memcpy(cptr, &msgjobnamelen, 4);
    msg->len += 4;

    return true;
}

/* edit标识消息 */
static bool xsynch_febuildmsg_editcmdassemble(xsynch_cmd* cmd, xsynch_exbuffer msg)
{
    int msglen = 0;
    int msgjobtype = 0;
    int msgjobnamelen = 0;
    int msgtype = T_XSYNCH_EDITCMD;
    char* cptr = NULL;
    xsynch_editcmd* editcmd = (xsynch_editcmd*)cmd;

    msglen = 4 + 4;
    msgtype = r_hton32(msgtype);
    msglen += 4;

    msgjobtype = editcmd->kind;
    msgjobtype = r_hton32(editcmd->kind);
    msglen += 4;

    if (NULL == editcmd->name || '\0' == editcmd->name[0])
    {
        msgjobnamelen = 0;
    }
    else
    {
        msgjobnamelen = strlen(editcmd->name);
    }
    msglen += 4;
    msglen += msgjobnamelen;

    if (false == xsynch_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    cptr = msg->data + msg->len;

    /* 总长度 */
    msglen = r_hton32(msglen);
    memcpy(cptr, &msglen, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, 暂时不用 */
    msg->len += 4;
    cptr += 4;

    /* 消息类型 */
    memcpy(cptr, &msgtype, 4);
    msg->len += 4;
    cptr += 4;

    /* 作业类型 */
    memcpy(cptr, &msgjobtype, 4);
    msg->len += 4;
    cptr += 4;

    /* 作业名称 */
    if (0 == msgjobnamelen)
    {
        memcpy(cptr, &msgjobnamelen, 4);
        msg->len += 4;
        cptr += 4;
        return true;
    }

    /* 先填充名称 */
    cptr += 4;
    memcpy(cptr, editcmd->name, msgjobnamelen);
    msg->len += msgjobnamelen;

    /* 再次填充长度 */
    cptr -= 4;
    msgjobnamelen = r_hton32(msgjobnamelen);
    memcpy(cptr, &msgjobnamelen, 4);
    msg->len += 4;

    return true;
}

/* start标识消息 */
static bool xsynch_febuildmsg_startcmdassemble(xsynch_cmd* cmd, xsynch_exbuffer msg)
{
    int msglen = 0;
    int msgjobtype = 0;
    int msgjobnamelen = 0;
    int msgtype = T_XSYNCH_STARTCMD;
    char* cptr = NULL;
    xsynch_startcmd* startcmd = (xsynch_startcmd*)cmd;

    msglen = 4 + 4;
    msgtype = r_hton32(msgtype);
    msglen += 4;

    msgjobtype = startcmd->kind;
    msgjobtype = r_hton32(startcmd->kind);
    msglen += 4;

    if (NULL == startcmd->name || '\0' == startcmd->name[0])
    {
        msgjobnamelen = 0;
    }
    else
    {
        msgjobnamelen = strlen(startcmd->name);
    }
    msglen += 4;
    msglen += msgjobnamelen;

    if (false == xsynch_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    cptr = msg->data + msg->len;

    /* 总长度 */
    msglen = r_hton32(msglen);
    memcpy(cptr, &msglen, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, 暂时不用 */
    msg->len += 4;
    cptr += 4;

    /* 消息类型 */
    memcpy(cptr, &msgtype, 4);
    msg->len += 4;
    cptr += 4;

    /* 作业类型 */
    memcpy(cptr, &msgjobtype, 4);
    msg->len += 4;
    cptr += 4;

    /* 作业名称 */
    if (0 == msgjobnamelen)
    {
        memcpy(cptr, &msgjobnamelen, 4);
        msg->len += 4;
        cptr += 4;
        return true;
    }

    /* 先填充名称 */
    cptr += 4;
    memcpy(cptr, startcmd->name, msgjobnamelen);
    msg->len += msgjobnamelen;

    /* 再次填充长度 */
    cptr -= 4;
    msgjobnamelen = r_hton32(msgjobnamelen);
    memcpy(cptr, &msgjobnamelen, 4);
    msg->len += 4;

    return true;
}

/* stop标识消息 */
static bool xsynch_febuildmsg_stopcmdassemble(xsynch_cmd* cmd, xsynch_exbuffer msg)
{
    int msglen = 0;
    int msgjobtype = 0;
    int msgjobnamelen = 0;
    int msgtype = T_XSYNCH_STOPCMD;
    char* cptr = NULL;
    xsynch_stopcmd* stopcmd = (xsynch_stopcmd*)cmd;

    msglen = 4 + 4;
    msgtype = r_hton32(msgtype);
    msglen += 4;

    msgjobtype = stopcmd->kind;
    msgjobtype = r_hton32(stopcmd->kind);
    msglen += 4;

    if (NULL == stopcmd->name || '\0' == stopcmd->name[0])
    {
        msgjobnamelen = 0;
    }
    else
    {
        msgjobnamelen = strlen(stopcmd->name);
    }
    msglen += 4;
    msglen += msgjobnamelen;

    if (false == xsynch_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    cptr = msg->data + msg->len;

    /* 总长度 */
    msglen = r_hton32(msglen);
    memcpy(cptr, &msglen, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, 暂时不用 */
    msg->len += 4;
    cptr += 4;

    /* 消息类型 */
    memcpy(cptr, &msgtype, 4);
    msg->len += 4;
    cptr += 4;

    /* 作业类型 */
    memcpy(cptr, &msgjobtype, 4);
    msg->len += 4;
    cptr += 4;

    /* 作业名称 */
    if (0 == msgjobnamelen)
    {
        memcpy(cptr, &msgjobnamelen, 4);
        msg->len += 4;
        cptr += 4;
        return true;
    }

    /* 先填充名称 */
    cptr += 4;
    memcpy(cptr, stopcmd->name, msgjobnamelen);
    msg->len += msgjobnamelen;

    /* 再次填充长度 */
    cptr -= 4;
    msgjobnamelen = r_hton32(msgjobnamelen);
    memcpy(cptr, &msgjobnamelen, 4);
    msg->len += 4;

    return true;
}

/* reload标识消息 */
static bool xsynch_febuildmsg_reloadcmdassemble(xsynch_cmd* cmd, xsynch_exbuffer msg)
{
    int msglen = 0;
    int msgjobtype = 0;
    int msgjobnamelen = 0;
    int msgtype = T_XSYNCH_RELOADCMD;
    char* cptr = NULL;
    xsynch_reloadcmd* reloadcmd = (xsynch_reloadcmd*)cmd;

    msglen = 4 + 4;
    msgtype = r_hton32(msgtype);
    msglen += 4;

    msgjobtype = reloadcmd->kind;
    msgjobtype = r_hton32(reloadcmd->kind);
    msglen += 4;

    if (NULL == reloadcmd->name || '\0' == reloadcmd->name[0])
    {
        msgjobnamelen = 0;
    }
    else
    {
        msgjobnamelen = strlen(reloadcmd->name);
    }
    msglen += 4;
    msglen += msgjobnamelen;

    if (false == xsynch_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    cptr = msg->data + msg->len;

    /* 总长度 */
    msglen = r_hton32(msglen);
    memcpy(cptr, &msglen, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, 暂时不用 */
    msg->len += 4;
    cptr += 4;

    /* 消息类型 */
    memcpy(cptr, &msgtype, 4);
    msg->len += 4;
    cptr += 4;

    /* 作业类型 */
    memcpy(cptr, &msgjobtype, 4);
    msg->len += 4;
    cptr += 4;

    /* 作业名称 */
    if (0 == msgjobnamelen)
    {
        memcpy(cptr, &msgjobnamelen, 4);
        msg->len += 4;
        cptr += 4;
        return true;
    }

    /* 先填充名称 */
    cptr += 4;
    memcpy(cptr, reloadcmd->name, msgjobnamelen);
    msg->len += msgjobnamelen;

    /* 再次填充长度 */
    cptr -= 4;
    msgjobnamelen = r_hton32(msgjobnamelen);
    memcpy(cptr, &msgjobnamelen, 4);
    msg->len += 4;

    return true;
}

/* info标识消息 */
static bool xsynch_febuildmsg_infocmdassemble(xsynch_cmd* cmd, xsynch_exbuffer msg)
{
    int msglen = 0;
    int msgjobtype = 0;
    int msgjobnamelen = 0;
    int msgtype = T_XSYNCH_INFOCMD;
    char* cptr = NULL;
    xsynch_infocmd* infocmd = (xsynch_infocmd*)cmd;

    msglen = 4 + 4;
    msgtype = r_hton32(msgtype);
    msglen += 4;

    msgjobtype = infocmd->kind;
    msgjobtype = r_hton32(infocmd->kind);
    msglen += 4;

    if (NULL == infocmd->name || '\0' == infocmd->name[0])
    {
        msgjobnamelen = 0;
    }
    else
    {
        msgjobnamelen = strlen(infocmd->name);
    }
    msglen += 4;
    msglen += msgjobnamelen;

    if (false == xsynch_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    cptr = msg->data + msg->len;

    /* 总长度 */
    msglen = r_hton32(msglen);
    memcpy(cptr, &msglen, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, 暂时不用 */
    msg->len += 4;
    cptr += 4;

    /* 消息类型 */
    memcpy(cptr, &msgtype, 4);
    msg->len += 4;
    cptr += 4;

    /* 作业类型 */
    memcpy(cptr, &msgjobtype, 4);
    msg->len += 4;
    cptr += 4;

    /* 作业名称 */
    if (0 == msgjobnamelen)
    {
        memcpy(cptr, &msgjobnamelen, 4);
        msg->len += 4;
        cptr += 4;
        return true;
    }

    /* 先填充名称 */
    cptr += 4;
    memcpy(cptr, infocmd->name, msgjobnamelen);
    msg->len += msgjobnamelen;

    /* 再次填充长度 */
    cptr -= 4;
    msgjobnamelen = r_hton32(msgjobnamelen);
    memcpy(cptr, &msgjobnamelen, 4);
    msg->len += 4;

    return true;
}

/* watch标识消息 */
static bool xsynch_febuildmsg_watchcmdassemble(xsynch_cmd* cmd, xsynch_exbuffer msg)
{
    int msglen = 0;
    int msgjobtype = 0;
    int msgjobnamelen = 0;
    int msgtype = T_XSYNCH_WATCHCMD;
    char* cptr = NULL;
    xsynch_watchcmd* watchcmd = (xsynch_watchcmd*)cmd;

    msglen = 4 + 4;
    msgtype = r_hton32(msgtype);
    msglen += 4;

    msgjobtype = watchcmd->kind;
    msgjobtype = r_hton32(watchcmd->kind);
    msglen += 4;

    if (NULL == watchcmd->name || '\0' == watchcmd->name[0])
    {
        msgjobnamelen = 0;
    }
    else
    {
        msgjobnamelen = strlen(watchcmd->name);
    }
    msglen += 4;
    msglen += msgjobnamelen;

    if (false == xsynch_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    cptr = msg->data + msg->len;

    /* 总长度 */
    msglen = r_hton32(msglen);
    memcpy(cptr, &msglen, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, 暂时不用 */
    msg->len += 4;
    cptr += 4;

    /* 消息类型 */
    memcpy(cptr, &msgtype, 4);
    msg->len += 4;
    cptr += 4;

    /* 作业类型 */
    memcpy(cptr, &msgjobtype, 4);
    msg->len += 4;
    cptr += 4;

    /* 作业名称 */
    if (0 == msgjobnamelen)
    {
        memcpy(cptr, &msgjobnamelen, 4);
        msg->len += 4;
        cptr += 4;
        return true;
    }

    /* 先填充名称 */
    cptr += 4;
    memcpy(cptr, watchcmd->name, msgjobnamelen);
    msg->len += msgjobnamelen;

    /* 再次填充长度 */
    cptr -= 4;
    msgjobnamelen = r_hton32(msgjobnamelen);
    memcpy(cptr, &msgjobnamelen, 4);
    msg->len += 4;

    return true;
}

/* cfgfile标识消息 */
static bool xsynch_febuildmsg_cfgfilecmdassemble(xsynch_cmd* cmd, xsynch_exbuffer msg)
{
    int msglen = 0;
    int msgjobtype = 0;
    int msgjobnamelen = 0;
    int msgfilenamelen = 0;
    int msgtype = T_XSYNCH_CFGfILECMD;
    int tmplen = 0;
    int datalen = 0;
    char* cptr = NULL;
    xsynch_cfgfilecmd* cfgfilecmd = (xsynch_cfgfilecmd*)cmd;

    msglen = 4 + 4;
    msgtype = r_hton32(msgtype);
    msglen += 4;

    msgjobtype = cfgfilecmd->kind;
    msgjobtype = r_hton32(cfgfilecmd->kind);
    msglen += 4;

    if (NULL == cfgfilecmd->name || '\0' == cfgfilecmd->name[0])
    {
        msgjobnamelen = 0;
    }
    else
    {
        msgjobnamelen = strlen(cfgfilecmd->name);
    }

    if (NULL == cfgfilecmd->filename || '\0' == cfgfilecmd->filename[0])
    {
        msgfilenamelen = 0;
    }
    else
    {
        msgfilenamelen = strlen(cfgfilecmd->filename);
    }
    datalen = cfgfilecmd->datalen;

    msglen += 4;
    msglen += msgjobnamelen;
    msglen += 4;
    msglen += msgfilenamelen;
    msglen += 4;
    msglen += datalen;

    if (false == xsynch_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    cptr = msg->data + msg->len;

    /* 总长度 */
    msglen = r_hton32(msglen);
    memcpy(cptr, &msglen, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, 暂时不用 */
    msg->len += 4;
    cptr += 4;

    /* 消息类型 */
    memcpy(cptr, &msgtype, 4);
    msg->len += 4;
    cptr += 4;

    /* 作业类型 */
    memcpy(cptr, &msgjobtype, 4);
    msg->len += 4;
    cptr += 4;

    /* 先填充名称 */
    tmplen = msgjobnamelen;
    msgjobnamelen = r_hton32(msgjobnamelen);
    memcpy(cptr, &msgjobnamelen, 4);
    msg->len += 4;
    cptr += 4;

    if (tmplen != 0)
    {
        memcpy(cptr, cfgfilecmd->name, tmplen);
        msg->len += tmplen;
        cptr += tmplen;
    }

    /* 填充filename */
    tmplen = msgfilenamelen;
    msgfilenamelen = r_hton32(msgfilenamelen);
    memcpy(cptr, &msgfilenamelen, 4);
    msg->len += 4;
    cptr += 4;

    if (tmplen != 0)
    {
        memcpy(cptr, cfgfilecmd->filename, tmplen);
        msg->len += tmplen;
        cptr += tmplen;
    }

    /* 填充data */
    tmplen = datalen;
    datalen = r_hton32(datalen);
    memcpy(cptr, &datalen, 4);
    msg->len += 4;
    cptr += 4;

    if (tmplen != 0)
    {
        memcpy(cptr, cfgfilecmd->data, tmplen);
        msg->len += tmplen;
        cptr += tmplen;
    }

    return true;
}


/* refresh消息 */
static bool xsynch_febuildmsg_refreshcmdassemble(xsynch_cmd* cmd, xsynch_exbuffer msg)
{
    /*
     * msglen
     * crc32
     * cmdtype
     * jobname 
     * 行数
     *  schema.table
     */
    int ivalue                      = 0;
    int msglen                      = 0;
    char* cptr                      = NULL;
    ListCell* lc                    = NULL;
    xsynch_rangevar* rvar           = NULL;
    xsynch_refreshcmd* refreshcmd   = NULL;

    refreshcmd = (xsynch_refreshcmd*)cmd;

    /* 算总长度 */
    /* 总长度 + crc32 */
    msglen = 4 + 4;

    /* cmdtype */
    msglen += 4;

    /* jobname */
    msglen += 4;
    msglen += strlen(refreshcmd->name);

    /* 总行数 */
    msglen += 4;

    foreach(lc, refreshcmd->tables)
    {
        rvar = (xsynch_rangevar*)lfirst(lc);

        /* schema */
        msglen += 4;
        msglen += strlen(rvar->schema);

        /* table */
        msglen += 4;
        msglen += strlen(rvar->table);
    }

    /* 申请空间 */
    msg->len = 0;
    if (false == xsynch_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    /* 填充数据 */
    cptr = msg->data + msg->len;

    /* 总长度 */
    ivalue = msglen;
    ivalue = r_hton32(ivalue);
    memcpy(cptr, &ivalue, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, 暂时不用 */
    msg->len += 4;
    cptr += 4;

    /* 消息类型 */
    ivalue = T_XSYNCH_REFRESHCMD;
    ivalue = r_hton32(ivalue);
    memcpy(cptr, &ivalue, 4);
    msg->len += 4;
    cptr += 4;

    /* jobnamelen */
    ivalue = strlen(refreshcmd->name);
    ivalue = r_hton32(ivalue);
    memcpy(cptr, &ivalue, 4);
    msg->len += 4;
    cptr += 4;

    /* jobname */
    ivalue = strlen(refreshcmd->name);
    memcpy(cptr, refreshcmd->name, ivalue);
    cptr += ivalue;
    msg->len += ivalue;

    /* 行数 */
    ivalue = refreshcmd->tables->length;
    ivalue = r_hton32(ivalue);
    memcpy(cptr, &ivalue, 4);
    msg->len += 4;
    cptr += 4;

    foreach(lc, refreshcmd->tables)
    {
        rvar = (xsynch_rangevar*)lfirst(lc);

        /* schema */
        ivalue = strlen(rvar->schema);
        ivalue = r_hton32(ivalue);
        memcpy(cptr, &ivalue, 4);
        msg->len += 4;
        cptr += 4;

        ivalue = strlen(rvar->schema);
        memcpy(cptr, rvar->schema, ivalue);
        cptr += ivalue;
        msg->len += ivalue;

        /* table */
        ivalue = strlen(rvar->table);
        ivalue = r_hton32(ivalue);
        memcpy(cptr, &ivalue, 4);
        msg->len += 4;
        cptr += 4;

        ivalue = strlen(rvar->table);
        memcpy(cptr, rvar->table, ivalue);
        cptr += ivalue;
        msg->len += ivalue;
    }

    return true;
}

/* list消息 */
static bool xsynch_febuildmsg_listcmdassemble(xsynch_cmd* cmd, xsynch_exbuffer msg)
{
    int msglen = 0;
    int msgtype = T_XSYNCH_LISTCMD;
    char* cptr = NULL;

    msglen = 4 + 4;
    msgtype = r_hton32(msgtype);
    msglen += 4;

    if (false == xsynch_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    cptr = msg->data + msg->len;

    /* 总长度 */
    msglen = r_hton32(msglen);
    memcpy(cptr, &msglen, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, 暂时不用 */
    msg->len += 4;
    cptr += 4;

    /* 消息类型 */
    memcpy(cptr, &msgtype, 4);
    msg->len += 4;
    cptr += 4;
    return true;
}

static xsynch_febuildmsg_assemble m_cmd2msgmap[] =
{
    {
        T_XSYNCH_NOP,
        "unknown command",
        NULL
    },
    {
        T_XSYNCH_IDENTITYCMD,
        "identity command",
        xsynch_febuildmsg_identitycmdassemble
    },
    {
        T_XSYNCH_CREATECMD,
        "create command",
        xsynch_febuildmsg_createcmdassemble
    },
    {
        T_XSYNCH_ALTERCMD,
        "alter command",
        xsynch_febuildmsg_altercmdassemble
    },
    {
        T_XSYNCH_REMOVECMD,
        "remove command",
        xsynch_febuildmsg_removecmdassemble
    },
    {
        T_XSYNCH_DROPCMD,
        "drop command",
        xsynch_febuildmsg_dropcmdassemble
    },
    {
        T_XSYNCH_INITCMD,
        "init command",
        xsynch_febuildmsg_initcmdassemble
    },
    {
        T_XSYNCH_EDITCMD,
        "edit command",
        xsynch_febuildmsg_editcmdassemble
    },
    {
        T_XSYNCH_STARTCMD,
        "start command",
        xsynch_febuildmsg_startcmdassemble
    },
    {
        T_XSYNCH_STOPCMD,
        "stop command",
        xsynch_febuildmsg_stopcmdassemble
    },
    {
        T_XSYNCH_RELOADCMD,
        "reload command",
        xsynch_febuildmsg_reloadcmdassemble
    },
    {
        T_XSYNCH_INFOCMD,
        "info command",
        xsynch_febuildmsg_infocmdassemble
    },
    {
        T_XSYNCH_WATCHCMD,
        "watch command",
        xsynch_febuildmsg_watchcmdassemble
    },
    {
        T_XSYNCH_CFGfILECMD,
        "config file command",
        xsynch_febuildmsg_cfgfilecmdassemble
    },
    {
        T_XSYNCH_REFRESHCMD,
        "refresh command",
        xsynch_febuildmsg_refreshcmdassemble
    },
    {
        T_XSYNCH_LISTCMD,
        "list command",
        xsynch_febuildmsg_listcmdassemble
    },

    /* 在此之前添加 */
    {
        T_XSYNCH_MAX,
        "max command",
        NULL
    }
};

/*
 * 根据 command 的类型组装数据到缓存中
*/
bool xsynch_febuildmsg_cmd2msg(xsynch_cmd* cmd, xsynch_exbuffer msg)
{
    /*
     * 组装 长度 等信息数据时，需要将主机字节序转换为网络字节序
     *  在 ripple_c.h 中含有转换函数:
     *      r_hton16 r_hton32 r_hton64
     */
    if (NULL == m_cmd2msgmap[cmd->type].assemble)
    {
        return false;
    }

    return m_cmd2msgmap[cmd->type].assemble(cmd, msg);
}
