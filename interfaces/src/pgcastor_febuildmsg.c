/*
 * assemble data to send
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <memory.h>
#include <errno.h>

#include "app_c.h"
#include "pgcastor_exbufferdata.h"
#include "pgcastor_fe.h"
#include "pgcastor_int.h"
#include "pgcastor_febuildmsg.h"

typedef struct PGCASTOR_FEBUILDMSG_ASSEMBLE
{
    pgcastor_cmdtag cmd;
    char*         desc;

    bool          (*assemble)(pgcastor_cmd* cmd, pgcastor_exbuffer msg);
} pgcastor_febuildmsg_assemble;

/* identity message */
static bool pgcastor_febuildmsg_identitycmdassemble(pgcastor_cmd* cmd, pgcastor_exbuffer msg)
{
    int                 msglen = 0;
    int                 msgjobtype = 0;
    int                 msgjobnamelen = 0;
    int                 msgtype = T_PGCASTOR_IDENTITYCMD;
    char*               cptr = NULL;
    pgcastor_identitycmd* icmd = (pgcastor_identitycmd*)cmd;

    /* byte order conversion */
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

    if (false == pgcastor_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    /*
     * add data to send buffer
     */
    cptr = msg->data + msg->len;

    /* total length */
    msglen = r_hton32(msglen);
    memcpy(cptr, &msglen, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, not used for now */
    msg->len += 4;
    cptr += 4;

    /* message type */
    memcpy(cptr, &msgtype, 4);
    msg->len += 4;
    cptr += 4;

    /* job type */
    memcpy(cptr, &msgjobtype, 4);
    msg->len += 4;
    cptr += 4;

    /* job name */
    if (0 == msgjobnamelen)
    {
        memcpy(cptr, &msgjobnamelen, 4);
        msg->len += 4;
        cptr += 4;
        return true;
    }

    /* fill name first */
    cptr += 4;
    memcpy(cptr, icmd->jobname, msgjobnamelen);
    msg->len += msgjobnamelen;

    /* fill length again */
    cptr -= 4;
    msgjobnamelen = r_hton32(msgjobnamelen);
    memcpy(cptr, &msgjobnamelen, 4);
    msg->len += 4;

    return true;
}

/* create identifier message */
static bool pgcastor_febuildmsg_createcmdassemble(pgcastor_cmd* cmd, pgcastor_exbuffer msg)
{
    int               msglen = 0;
    int               ivalue = 0;
    int               valuelen = 0;
    int               msgjobtype = 0;
    int               msgjobnamelen = 0;
    int               msgtype = T_PGCASTOR_CREATECMD;
    char*             cptr = NULL;
    ListCell*         lc = NULL;
    pgcastor_job*       job = NULL;
    pgcastor_createcmd* createcmd = (pgcastor_createcmd*)cmd;

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

        foreach (lc, createcmd->job)
        {
            job = (pgcastor_job*)lfirst(lc);

            /* jobtype 4 + jobnamelen 4 */
            msglen += (4 + 4);
            msglen += strlen(job->jobname);
        }
    }

    if (false == pgcastor_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    cptr = msg->data + msg->len;

    /* total length */
    msglen = r_hton32(msglen);
    memcpy(cptr, &msglen, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, not used for now */
    msg->len += 4;
    cptr += 4;

    /* message type */
    memcpy(cptr, &msgtype, 4);
    msg->len += 4;
    cptr += 4;

    /* job type */
    memcpy(cptr, &msgjobtype, 4);
    msg->len += 4;
    cptr += 4;

    /* job name */
    if (0 == msgjobnamelen)
    {
        memcpy(cptr, &msgjobnamelen, 4);
        msg->len += 4;
        cptr += 4;
        return true;
    }

    /* fill name first */
    cptr += 4;
    memcpy(cptr, createcmd->name, msgjobnamelen);
    msg->len += msgjobnamelen;

    /* fill length again */
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

        foreach (lc, createcmd->job)
        {
            job = (pgcastor_job*)lfirst(lc);

            /* job type */
            ivalue = job->kind;
            ivalue = r_hton32(ivalue);
            memcpy(cptr, &ivalue, 4);
            msg->len += 4;
            cptr += 4;

            /* jobnamelen */
            valuelen = strlen(job->jobname);
            if (0 == valuelen)
            {
                memcpy(cptr, &msgjobnamelen, 4);
                msg->len += 4;
                cptr += 4;
                continue;
            }
            valuelen = strlen(job->jobname);
            valuelen = r_hton32(valuelen);
            memcpy(cptr, &valuelen, 4);
            msg->len += 4;
            cptr += 4;

            /* jobname */
            valuelen = strlen(job->jobname);
            memcpy(cptr, job->jobname, valuelen);
            msg->len += valuelen;
            cptr += valuelen;
        }
    }

    return true;
}

/* alter identifier message */
static bool pgcastor_febuildmsg_altercmdassemble(pgcastor_cmd* cmd, pgcastor_exbuffer msg)
{
    int8             action = 0;
    int              msglen = 0;
    int              ivalue = 0;
    int              valuelen = 0;
    int              msgjobtype = 0;
    int              msgjobnamelen = 0;
    int              msgtype = T_PGCASTOR_ALTERCMD;
    char*            cptr = NULL;
    ListCell*        lc = NULL;
    pgcastor_job*      job = NULL;
    pgcastor_altercmd* altercmd = (pgcastor_altercmd*)cmd;

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

        foreach (lc, altercmd->job)
        {
            job = (pgcastor_job*)lfirst(lc);

            /* jobtype 4 + jobnamelen 4 */
            msglen += (4 + 4);
            msglen += strlen(job->jobname);
        }
    }

    if (false == pgcastor_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    cptr = msg->data + msg->len;

    /* total length */
    msglen = r_hton32(msglen);
    memcpy(cptr, &msglen, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, not used for now */
    msg->len += 4;
    cptr += 4;

    /* message type */
    memcpy(cptr, &msgtype, 4);
    msg->len += 4;
    cptr += 4;

    /* job type */
    memcpy(cptr, &msgjobtype, 4);
    msg->len += 4;
    cptr += 4;

    /* job name */
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

    /* operation type */
    memcpy(cptr, &action, 1);
    msg->len += 1;
    cptr += 1;

    if (NULL != altercmd->job)
    {
        /* job cnt */
        valuelen = altercmd->job->length;
        valuelen = r_hton32(valuelen);
        memcpy(cptr, &valuelen, 4);
        msg->len += 4;
        cptr += 4;

        foreach (lc, altercmd->job)
        {
            job = (pgcastor_job*)lfirst(lc);

            /* job cnt */
            ivalue = job->kind;
            ivalue = r_hton32(ivalue);
            memcpy(cptr, &ivalue, 4);
            msg->len += 4;
            cptr += 4;

            /* jobtype */
            valuelen = strlen(job->jobname);
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
            valuelen = strlen(job->jobname);
            memcpy(cptr, job->jobname, valuelen);
            msg->len += valuelen;
            cptr += valuelen;
        }
    }

    return true;
}

/* remove identifier message */
static bool pgcastor_febuildmsg_removecmdassemble(pgcastor_cmd* cmd, pgcastor_exbuffer msg)
{
    int               msglen = 0;
    int               msgjobtype = 0;
    int               msgjobnamelen = 0;
    int               msgtype = T_PGCASTOR_REMOVECMD;
    char*             cptr = NULL;
    pgcastor_removecmd* removecmd = (pgcastor_removecmd*)cmd;

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

    if (false == pgcastor_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    cptr = msg->data + msg->len;

    /* total length */
    msglen = r_hton32(msglen);
    memcpy(cptr, &msglen, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, not used for now */
    msg->len += 4;
    cptr += 4;

    /* message type */
    memcpy(cptr, &msgtype, 4);
    msg->len += 4;
    cptr += 4;

    /* job type */
    memcpy(cptr, &msgjobtype, 4);
    msg->len += 4;
    cptr += 4;

    /* job name */
    if (0 == msgjobnamelen)
    {
        memcpy(cptr, &msgjobnamelen, 4);
        msg->len += 4;
        cptr += 4;
        return true;
    }

    /* fill name first */
    cptr += 4;
    memcpy(cptr, removecmd->name, msgjobnamelen);
    msg->len += msgjobnamelen;

    /* fill length again */
    cptr -= 4;
    msgjobnamelen = r_hton32(msgjobnamelen);
    memcpy(cptr, &msgjobnamelen, 4);
    msg->len += 4;

    return true;
}

/* drop identifier message */
static bool pgcastor_febuildmsg_dropcmdassemble(pgcastor_cmd* cmd, pgcastor_exbuffer msg)
{
    int             msglen = 0;
    int             msgjobtype = 0;
    int             msgjobnamelen = 0;
    int             msgtype = T_PGCASTOR_DROPCMD;
    char*           cptr = NULL;
    pgcastor_dropcmd* dropcmd = (pgcastor_dropcmd*)cmd;

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

    if (false == pgcastor_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    cptr = msg->data + msg->len;

    /* total length */
    msglen = r_hton32(msglen);
    memcpy(cptr, &msglen, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, not used for now */
    msg->len += 4;
    cptr += 4;

    /* message type */
    memcpy(cptr, &msgtype, 4);
    msg->len += 4;
    cptr += 4;

    /* job type */
    memcpy(cptr, &msgjobtype, 4);
    msg->len += 4;
    cptr += 4;

    /* job name */
    if (0 == msgjobnamelen)
    {
        memcpy(cptr, &msgjobnamelen, 4);
        msg->len += 4;
        cptr += 4;
        return true;
    }

    /* fill name first */
    cptr += 4;
    memcpy(cptr, dropcmd->name, msgjobnamelen);
    msg->len += msgjobnamelen;

    /* fill length again */
    cptr -= 4;
    msgjobnamelen = r_hton32(msgjobnamelen);
    memcpy(cptr, &msgjobnamelen, 4);
    msg->len += 4;

    return true;
}

/* init identifier message */
static bool pgcastor_febuildmsg_initcmdassemble(pgcastor_cmd* cmd, pgcastor_exbuffer msg)
{
    int             msglen = 0;
    int             msgjobtype = 0;
    int             msgjobnamelen = 0;
    int             msgtype = T_PGCASTOR_INITCMD;
    char*           cptr = NULL;
    pgcastor_initcmd* initcmd = (pgcastor_initcmd*)cmd;

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

    if (false == pgcastor_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    cptr = msg->data + msg->len;

    /* total length */
    msglen = r_hton32(msglen);
    memcpy(cptr, &msglen, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, not used for now */
    msg->len += 4;
    cptr += 4;

    /* message type */
    memcpy(cptr, &msgtype, 4);
    msg->len += 4;
    cptr += 4;

    /* job type */
    memcpy(cptr, &msgjobtype, 4);
    msg->len += 4;
    cptr += 4;

    /* job name */
    if (0 == msgjobnamelen)
    {
        memcpy(cptr, &msgjobnamelen, 4);
        msg->len += 4;
        cptr += 4;
        return true;
    }

    /* fill name first */
    cptr += 4;
    memcpy(cptr, initcmd->name, msgjobnamelen);
    msg->len += msgjobnamelen;

    /* fill length again */
    cptr -= 4;
    msgjobnamelen = r_hton32(msgjobnamelen);
    memcpy(cptr, &msgjobnamelen, 4);
    msg->len += 4;

    return true;
}

/* edit identifier message */
static bool pgcastor_febuildmsg_editcmdassemble(pgcastor_cmd* cmd, pgcastor_exbuffer msg)
{
    int             msglen = 0;
    int             msgjobtype = 0;
    int             msgjobnamelen = 0;
    int             msgtype = T_PGCASTOR_EDITCMD;
    char*           cptr = NULL;
    pgcastor_editcmd* editcmd = (pgcastor_editcmd*)cmd;

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

    if (false == pgcastor_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    cptr = msg->data + msg->len;

    /* total length */
    msglen = r_hton32(msglen);
    memcpy(cptr, &msglen, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, not used for now */
    msg->len += 4;
    cptr += 4;

    /* message type */
    memcpy(cptr, &msgtype, 4);
    msg->len += 4;
    cptr += 4;

    /* job type */
    memcpy(cptr, &msgjobtype, 4);
    msg->len += 4;
    cptr += 4;

    /* job name */
    if (0 == msgjobnamelen)
    {
        memcpy(cptr, &msgjobnamelen, 4);
        msg->len += 4;
        cptr += 4;
        return true;
    }

    /* fill name first */
    cptr += 4;
    memcpy(cptr, editcmd->name, msgjobnamelen);
    msg->len += msgjobnamelen;

    /* fill length again */
    cptr -= 4;
    msgjobnamelen = r_hton32(msgjobnamelen);
    memcpy(cptr, &msgjobnamelen, 4);
    msg->len += 4;

    return true;
}

/* start identifier message */
static bool pgcastor_febuildmsg_startcmdassemble(pgcastor_cmd* cmd, pgcastor_exbuffer msg)
{
    int              msglen = 0;
    int              msgjobtype = 0;
    int              msgjobnamelen = 0;
    int              msgtype = T_PGCASTOR_STARTCMD;
    char*            cptr = NULL;
    pgcastor_startcmd* startcmd = (pgcastor_startcmd*)cmd;

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

    if (false == pgcastor_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    cptr = msg->data + msg->len;

    /* total length */
    msglen = r_hton32(msglen);
    memcpy(cptr, &msglen, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, not used for now */
    msg->len += 4;
    cptr += 4;

    /* message type */
    memcpy(cptr, &msgtype, 4);
    msg->len += 4;
    cptr += 4;

    /* job type */
    memcpy(cptr, &msgjobtype, 4);
    msg->len += 4;
    cptr += 4;

    /* job name */
    if (0 == msgjobnamelen)
    {
        memcpy(cptr, &msgjobnamelen, 4);
        msg->len += 4;
        cptr += 4;
        return true;
    }

    /* fill name first */
    cptr += 4;
    memcpy(cptr, startcmd->name, msgjobnamelen);
    msg->len += msgjobnamelen;

    /* fill length again */
    cptr -= 4;
    msgjobnamelen = r_hton32(msgjobnamelen);
    memcpy(cptr, &msgjobnamelen, 4);
    msg->len += 4;

    return true;
}

/* stop identifier message */
static bool pgcastor_febuildmsg_stopcmdassemble(pgcastor_cmd* cmd, pgcastor_exbuffer msg)
{
    int             msglen = 0;
    int             msgjobtype = 0;
    int             msgjobnamelen = 0;
    int             msgtype = T_PGCASTOR_STOPCMD;
    char*           cptr = NULL;
    pgcastor_stopcmd* stopcmd = (pgcastor_stopcmd*)cmd;

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

    if (false == pgcastor_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    cptr = msg->data + msg->len;

    /* total length */
    msglen = r_hton32(msglen);
    memcpy(cptr, &msglen, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, not used for now */
    msg->len += 4;
    cptr += 4;

    /* message type */
    memcpy(cptr, &msgtype, 4);
    msg->len += 4;
    cptr += 4;

    /* job type */
    memcpy(cptr, &msgjobtype, 4);
    msg->len += 4;
    cptr += 4;

    /* job name */
    if (0 == msgjobnamelen)
    {
        memcpy(cptr, &msgjobnamelen, 4);
        msg->len += 4;
        cptr += 4;
        return true;
    }

    /* fill name first */
    cptr += 4;
    memcpy(cptr, stopcmd->name, msgjobnamelen);
    msg->len += msgjobnamelen;

    /* fill length again */
    cptr -= 4;
    msgjobnamelen = r_hton32(msgjobnamelen);
    memcpy(cptr, &msgjobnamelen, 4);
    msg->len += 4;

    return true;
}

/* reload identifier message */
static bool pgcastor_febuildmsg_reloadcmdassemble(pgcastor_cmd* cmd, pgcastor_exbuffer msg)
{
    int               msglen = 0;
    int               msgjobtype = 0;
    int               msgjobnamelen = 0;
    int               msgtype = T_PGCASTOR_RELOADCMD;
    char*             cptr = NULL;
    pgcastor_reloadcmd* reloadcmd = (pgcastor_reloadcmd*)cmd;

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

    if (false == pgcastor_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    cptr = msg->data + msg->len;

    /* total length */
    msglen = r_hton32(msglen);
    memcpy(cptr, &msglen, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, not used for now */
    msg->len += 4;
    cptr += 4;

    /* message type */
    memcpy(cptr, &msgtype, 4);
    msg->len += 4;
    cptr += 4;

    /* job type */
    memcpy(cptr, &msgjobtype, 4);
    msg->len += 4;
    cptr += 4;

    /* job name */
    if (0 == msgjobnamelen)
    {
        memcpy(cptr, &msgjobnamelen, 4);
        msg->len += 4;
        cptr += 4;
        return true;
    }

    /* fill name first */
    cptr += 4;
    memcpy(cptr, reloadcmd->name, msgjobnamelen);
    msg->len += msgjobnamelen;

    /* fill length again */
    cptr -= 4;
    msgjobnamelen = r_hton32(msgjobnamelen);
    memcpy(cptr, &msgjobnamelen, 4);
    msg->len += 4;

    return true;
}

/* info identifier message */
static bool pgcastor_febuildmsg_infocmdassemble(pgcastor_cmd* cmd, pgcastor_exbuffer msg)
{
    int             msglen = 0;
    int             msgjobtype = 0;
    int             msgjobnamelen = 0;
    int             msgtype = T_PGCASTOR_INFOCMD;
    char*           cptr = NULL;
    pgcastor_infocmd* infocmd = (pgcastor_infocmd*)cmd;

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

    if (false == pgcastor_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    cptr = msg->data + msg->len;

    /* total length */
    msglen = r_hton32(msglen);
    memcpy(cptr, &msglen, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, not used for now */
    msg->len += 4;
    cptr += 4;

    /* message type */
    memcpy(cptr, &msgtype, 4);
    msg->len += 4;
    cptr += 4;

    /* job type */
    memcpy(cptr, &msgjobtype, 4);
    msg->len += 4;
    cptr += 4;

    /* job name */
    if (0 == msgjobnamelen)
    {
        memcpy(cptr, &msgjobnamelen, 4);
        msg->len += 4;
        cptr += 4;
        return true;
    }

    /* fill name first */
    cptr += 4;
    memcpy(cptr, infocmd->name, msgjobnamelen);
    msg->len += msgjobnamelen;

    /* fill length again */
    cptr -= 4;
    msgjobnamelen = r_hton32(msgjobnamelen);
    memcpy(cptr, &msgjobnamelen, 4);
    msg->len += 4;

    return true;
}

/* watch identifier message */
static bool pgcastor_febuildmsg_watchcmdassemble(pgcastor_cmd* cmd, pgcastor_exbuffer msg)
{
    int              msglen = 0;
    int              msgjobtype = 0;
    int              msgjobnamelen = 0;
    int              msgtype = T_PGCASTOR_WATCHCMD;
    char*            cptr = NULL;
    pgcastor_watchcmd* watchcmd = (pgcastor_watchcmd*)cmd;

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

    if (false == pgcastor_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    cptr = msg->data + msg->len;

    /* total length */
    msglen = r_hton32(msglen);
    memcpy(cptr, &msglen, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, not used for now */
    msg->len += 4;
    cptr += 4;

    /* message type */
    memcpy(cptr, &msgtype, 4);
    msg->len += 4;
    cptr += 4;

    /* job type */
    memcpy(cptr, &msgjobtype, 4);
    msg->len += 4;
    cptr += 4;

    /* job name */
    if (0 == msgjobnamelen)
    {
        memcpy(cptr, &msgjobnamelen, 4);
        msg->len += 4;
        cptr += 4;
        return true;
    }

    /* fill name first */
    cptr += 4;
    memcpy(cptr, watchcmd->name, msgjobnamelen);
    msg->len += msgjobnamelen;

    /* fill length again */
    cptr -= 4;
    msgjobnamelen = r_hton32(msgjobnamelen);
    memcpy(cptr, &msgjobnamelen, 4);
    msg->len += 4;

    return true;
}

/* cfgfile identifier message */
static bool pgcastor_febuildmsg_cfgfilecmdassemble(pgcastor_cmd* cmd, pgcastor_exbuffer msg)
{
    int                msglen = 0;
    int                msgjobtype = 0;
    int                msgjobnamelen = 0;
    int                msgfilenamelen = 0;
    int                msgtype = T_PGCASTOR_CFGfILECMD;
    int                tmplen = 0;
    int                datalen = 0;
    char*              cptr = NULL;
    pgcastor_cfgfilecmd* cfgfilecmd = (pgcastor_cfgfilecmd*)cmd;

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

    if (false == pgcastor_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    cptr = msg->data + msg->len;

    /* total length */
    msglen = r_hton32(msglen);
    memcpy(cptr, &msglen, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, not used for now */
    msg->len += 4;
    cptr += 4;

    /* message type */
    memcpy(cptr, &msgtype, 4);
    msg->len += 4;
    cptr += 4;

    /* job type */
    memcpy(cptr, &msgjobtype, 4);
    msg->len += 4;
    cptr += 4;

    /* fill name first */
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

    /* fill filename */
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

    /* fill data */
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

/* refresh message */
static bool pgcastor_febuildmsg_refreshcmdassemble(pgcastor_cmd* cmd, pgcastor_exbuffer msg)
{
    /*
     * msglen
     * crc32
     * cmdtype
     * jobname
     * row count
     *  schema.table
     */
    int                ivalue = 0;
    int                msglen = 0;
    char*              cptr = NULL;
    ListCell*          lc = NULL;
    pgcastor_rangevar*   rvar = NULL;
    pgcastor_refreshcmd* refreshcmd = NULL;

    refreshcmd = (pgcastor_refreshcmd*)cmd;

    /* calculate total length */
    /* total length + crc32 */
    msglen = 4 + 4;

    /* cmdtype */
    msglen += 4;

    /* jobname */
    msglen += 4;
    msglen += strlen(refreshcmd->name);

    /* total row count */
    msglen += 4;

    foreach (lc, refreshcmd->tables)
    {
        rvar = (pgcastor_rangevar*)lfirst(lc);

        /* schema */
        msglen += 4;
        msglen += strlen(rvar->schema);

        /* table */
        msglen += 4;
        msglen += strlen(rvar->table);
    }

    /* allocate space */
    msg->len = 0;
    if (false == pgcastor_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    /* fill data */
    cptr = msg->data + msg->len;

    /* total length */
    ivalue = msglen;
    ivalue = r_hton32(ivalue);
    memcpy(cptr, &ivalue, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, not used for now */
    msg->len += 4;
    cptr += 4;

    /* message type */
    ivalue = T_PGCASTOR_REFRESHCMD;
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

    /* row count */
    ivalue = refreshcmd->tables->length;
    ivalue = r_hton32(ivalue);
    memcpy(cptr, &ivalue, 4);
    msg->len += 4;
    cptr += 4;

    foreach (lc, refreshcmd->tables)
    {
        rvar = (pgcastor_rangevar*)lfirst(lc);

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

/* list message */
static bool pgcastor_febuildmsg_listcmdassemble(pgcastor_cmd* cmd, pgcastor_exbuffer msg)
{
    int   msglen = 0;
    int   msgtype = T_PGCASTOR_LISTCMD;
    char* cptr = NULL;

    msglen = 4 + 4;
    msgtype = r_hton32(msgtype);
    msglen += 4;

    if (false == pgcastor_exbufferdata_enlarge(msg, msglen))
    {
        return false;
    }

    cptr = msg->data + msg->len;

    /* total length */
    msglen = r_hton32(msglen);
    memcpy(cptr, &msglen, 4);
    msg->len += 4;
    cptr += 4;

    /* crc, not used for now */
    msg->len += 4;
    cptr += 4;

    /* message type */
    memcpy(cptr, &msgtype, 4);
    msg->len += 4;
    cptr += 4;
    return true;
}

static pgcastor_febuildmsg_assemble m_cmd2msgmap[] = {
    {T_PGCASTOR_NOP,         "unknown command",     NULL                                 },
    {T_PGCASTOR_IDENTITYCMD, "identity command",    pgcastor_febuildmsg_identitycmdassemble},
    {T_PGCASTOR_CREATECMD,   "create command",      pgcastor_febuildmsg_createcmdassemble  },
    {T_PGCASTOR_ALTERCMD,    "alter command",       pgcastor_febuildmsg_altercmdassemble   },
    {T_PGCASTOR_REMOVECMD,   "remove command",      pgcastor_febuildmsg_removecmdassemble  },
    {T_PGCASTOR_DROPCMD,     "drop command",        pgcastor_febuildmsg_dropcmdassemble    },
    {T_PGCASTOR_INITCMD,     "init command",        pgcastor_febuildmsg_initcmdassemble    },
    {T_PGCASTOR_EDITCMD,     "edit command",        pgcastor_febuildmsg_editcmdassemble    },
    {T_PGCASTOR_STARTCMD,    "start command",       pgcastor_febuildmsg_startcmdassemble   },
    {T_PGCASTOR_STOPCMD,     "stop command",        pgcastor_febuildmsg_stopcmdassemble    },
    {T_PGCASTOR_RELOADCMD,   "reload command",      pgcastor_febuildmsg_reloadcmdassemble  },
    {T_PGCASTOR_INFOCMD,     "info command",        pgcastor_febuildmsg_infocmdassemble    },
    {T_PGCASTOR_WATCHCMD,    "watch command",       pgcastor_febuildmsg_watchcmdassemble   },
    {T_PGCASTOR_CFGfILECMD,  "config file command", pgcastor_febuildmsg_cfgfilecmdassemble },
    {T_PGCASTOR_REFRESHCMD,  "refresh command",     pgcastor_febuildmsg_refreshcmdassemble },
    {T_PGCASTOR_LISTCMD,     "list command",        pgcastor_febuildmsg_listcmdassemble    },

    /* add before this */
    {T_PGCASTOR_MAX,         "max command",         NULL                                 }
};

/*
 * assemble data to buffer according to command type
 */
bool pgcastor_febuildmsg_cmd2msg(pgcastor_cmd* cmd, pgcastor_exbuffer msg)
{
    /*
     * when assembling length and other info data, need to convert host byte order to network byte
     * order c.h contains conversion functions: r_hton16 r_hton32 r_hton64
     */
    if (NULL == m_cmd2msgmap[cmd->type].assemble)
    {
        return false;
    }

    return m_cmd2msgmap[cmd->type].assemble(cmd, msg);
}
