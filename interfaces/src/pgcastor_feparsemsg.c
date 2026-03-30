/*
 * parse data sent by server
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <memory.h>
#include <errno.h>
#include <inttypes.h>

#include "app_c.h"
#include "pgcastor_exbufferdata.h"
#include "pgcastor_fe.h"
#include "pgcastor_int.h"
#include "pgcastor_feparsemsg.h"

typedef struct PGCASTOR_FEPARSEMSG_PARSER
{
    pgcastor_cmdtag cmd;
    char*         desc;

    bool          (*parser)(pgcastor_exbuffer msg, pgcastor_conn* conn);
} pgcastor_feparsemsg_parser;

/* edit message parser */
static bool pgcastor_feparsemsg_editmsgparser(pgcastor_exbuffer msg, pgcastor_conn* conn)
{
    int    datalen = 0;
    uint32 rowcnt = 0;
    uint32 rowlen = 0;
    char*  cptr = NULL;

    if (0 == msg->len || NULL == msg->data)
    {
        return false;
    }

    cptr = msg->data;

    cptr += 13;

    /* get row count */
    memcpy(&rowcnt, cptr, 4);
    rowcnt = r_ntoh32(rowcnt);
    cptr += 4;

    /* get data length */
    memcpy(&rowlen, cptr, 4);
    rowlen = r_ntoh32(rowlen);
    cptr += 4;

    /* get data length */
    memcpy(&datalen, cptr, 4);
    datalen = r_ntoh32(datalen);
    cptr += 4;

    if (datalen <= 0)
    {
        pgcastor_exbufferdata_append(conn->errmsg, "get wrong data length");
        return false;
    }

    /* write information to rows */
    conn->result->rowcnt = rowcnt;
    conn->result->rows = PGCastorRowInit(conn->result->rowcnt);

    conn->result->rows->columncnt = 1;
    conn->result->rows->columns = PGCastorPairInit(conn->result->rows->columncnt);

    conn->result->rows->columns->valuelen = datalen;

    conn->result->rows->columns->value = (char*)malloc(datalen);
    if (NULL == conn->result->rows->columns->value)
    {
        pgcastor_exbufferdata_append(conn->errmsg, "malloc colvalue oom");
        return false;
    }
    memset(conn->result->rows->columns->value, '\0', datalen);
    memcpy(conn->result->rows->columns->value, cptr, datalen);

    return true;
}

/* row data message parsing */
static bool pgcastor_feparsemsg_rowsmsgparser(pgcastor_exbuffer msg, pgcastor_conn* conn)
{
    uint16      nullmapcnt = 0;
    uint32      rowlen = 0;
    uint32      rowcnt = 0;
    uint32      idx_row = 0;
    uint32      idx_col = 0;
    uint32      namelen = 0;
    uint32      colcnt = 0;
    uint32      offset = 0;
    uint8*      nullmap = NULL;
    char*       cptr = NULL;
    pgcastorrow*  rows = NULL;
    pgcastorpair* keys = NULL;
    pgcastorpair* columns = NULL;

    cptr = msg->data;

    cptr += 13;

    /* get row count */
    memcpy(&rowcnt, cptr, 4);
    rowcnt = r_ntoh32(rowcnt);
    cptr += 4;

    if (rowcnt <= 0)
    {
        pgcastor_exbufferdata_append(conn->errmsg, "get wrong rowcnt");
        return false;
    }

    /* get first row data length */
    memcpy(&rowlen, cptr, 4);
    rowlen = r_ntoh32(rowlen);
    cptr += 4;

    /* calculate column count from first row */
    rowlen -= 4;
    while (rowlen > offset)
    {
        memcpy(&namelen, cptr + offset, 4);
        namelen = r_ntoh32(namelen);
        offset += 4;
        offset += namelen;
        colcnt++;
    }

    /* initialize first row structure */
    keys = PGCastorPairInit(colcnt);

    /* generate column header data */
    for (idx_col = 0; idx_col < colcnt; idx_col++)
    {
        columns = (keys + idx_col);

        memcpy(&namelen, cptr, 4);
        namelen = r_ntoh32(namelen);
        cptr += 4;

        columns->keylen = namelen + 1;
        columns->key = (char*)malloc(columns->keylen);
        if (NULL == columns->key)
        {
            pgcastor_exbufferdata_append(conn->errmsg, "malloc colkey oom");
            return false;
        }
        memset(columns->key, '\0', columns->keylen);
        memcpy(columns->key, cptr, namelen);
        cptr += namelen;
    }

    /* generate column data */
    conn->result->rowcnt = rowcnt - 1;
    conn->result->rows = PGCastorRowInit(conn->result->rowcnt);

    for (idx_row = 0; idx_row < conn->result->rowcnt; idx_row++)
    {
        rows = &conn->result->rows[idx_row];
        rows->columncnt = colcnt;
        rows->columns = PGCastorPairInit(conn->result->rows->columncnt);

        memcpy(&rowlen, cptr, 4);
        rowlen = r_ntoh32(rowlen);
        cptr += 4;

        /* get nullmap length */
        rowlen -= 4;
        memcpy(&nullmapcnt, cptr, 2);
        nullmapcnt = r_ntoh16(nullmapcnt);
        cptr += 2;

        /* get nullmap content */
        if (0 < nullmapcnt)
        {
            nullmap = malloc(nullmapcnt);
            if (NULL == nullmap)
            {
                pgcastor_exbufferdata_append(conn->errmsg, "malloc nullmap oom");
                return false;
            }
            memcpy(nullmap, cptr, nullmapcnt);
            cptr += nullmapcnt;
        }

        /* fill column values */
        for (idx_col = 0; idx_col < rows->columncnt; idx_col++)
        {
            columns = rows->columns + idx_col;
            /* fill column header info */
            columns->keylen = keys[idx_col].keylen;
            columns->key = strdup(keys[idx_col].key);

            /* check if column is null based on nullmap, continue if null */
            if (NULL != nullmap && (nullmap[idx_col / 8] & (1U << (idx_col % 8))))
            {
                columns->value = NULL;
                columns->valuelen = 0;
                continue;
            }

            /* get column value length */
            memcpy(&columns->valuelen, cptr, 4);
            columns->valuelen = r_ntoh32(columns->valuelen);
            cptr += 4;

            /* read character type directly into value */
            columns->valuelen += 1;
            columns->value = (char*)malloc(columns->valuelen);

            if (NULL == columns->value)
            {
                pgcastor_exbufferdata_append(conn->errmsg, "malloc colvalue oom");
                return false;
            }

            memset(columns->value, '\0', columns->valuelen);
            columns->valuelen -= 1;
            memcpy(columns->value, cptr, columns->valuelen);
            cptr += columns->valuelen;
        }
        if (nullmap)
        {
            free(nullmap);
            nullmap = NULL;
        }
    }

    PGCastorPairFree(colcnt, keys);
    return true;
}

static pgcastor_feparsemsg_parser m_msg2rows[] = {
    {T_PGCASTOR_NOP,         "unknown command",     NULL                           },
    {T_PGCASTOR_IDENTITYCMD, "identity command",    NULL                           },
    {T_PGCASTOR_CREATECMD,   "create command",      NULL                           },
    {T_PGCASTOR_ALTERCMD,    "alter command",       NULL                           },
    {T_PGCASTOR_REMOVECMD,   "remove command",      NULL                           },
    {T_PGCASTOR_DROPCMD,     "drop command",        NULL                           },
    {T_PGCASTOR_INITCMD,     "init command",        NULL                           },
    {T_PGCASTOR_EDITCMD,     "edit command",        pgcastor_feparsemsg_editmsgparser},
    {T_PGCASTOR_STARTCMD,    "start command",       pgcastor_feparsemsg_rowsmsgparser},
    {T_PGCASTOR_STOPCMD,     "stop command",        pgcastor_feparsemsg_rowsmsgparser},
    {T_PGCASTOR_RELOADCMD,   "reload command",      NULL                           },
    {T_PGCASTOR_INFOCMD,     "info command",        pgcastor_feparsemsg_rowsmsgparser},
    {T_PGCASTOR_WATCHCMD,    "watch command",       pgcastor_feparsemsg_rowsmsgparser},
    {T_PGCASTOR_CFGfILECMD,  "config file command", NULL                           },
    {T_PGCASTOR_REFRESHCMD,  "refresh command",     NULL                           },
    {T_PGCASTOR_LISTCMD,     "list command",        pgcastor_feparsemsg_rowsmsgparser},

    /* add before this */
    {T_PGCASTOR_MAX,         "max command",         NULL                           }
};

/*
 * convert received descriptor to parse result
 */
bool pgcastor_feparsemsg_msg2result(pgcastor_exbuffer msg, pgcastor_conn* conn)
{
    uint8 success = 0;
    int   cmdtype = 0;
    int   errlen = 0;
    char* cptr = NULL;

    if (0 == msg->len)
    {
        return false;
    }

    cptr = msg->data;

    /* skip total length, crc */
    cptr += 8;

    /* get operation type */
    memcpy(&cmdtype, cptr, 4);
    cmdtype = r_ntoh32(cmdtype);
    cptr += 4;

    /* get success/failure */
    memcpy(&success, cptr, 1);
    cptr += 1;

    PGCastorResultReset(conn->result);

    /* reset error message */
    conn->errcode = 0;
    if (false == pgcastor_exbufferdata_reset(conn->errmsg))
    {
        return false;
    }

    /* failure handling */
    if (1 == success)
    {
        /* get error message length */
        memcpy(&errlen, cptr, 4);
        errlen = r_ntoh32(errlen);
        cptr += 4;

        /* get error code */
        memcpy(&conn->errcode, cptr, 4);
        conn->errcode = r_ntoh32(conn->errcode);
        cptr += 4;

        /* write error message */
        if (false == pgcastor_exbufferdata_appendbinary(conn->errmsg, cptr, (errlen - 4)))
        {
            return false;
        }
        return true;
    }

    if (T_PGCASTOR_MAX < cmdtype)
    {
        return false;
    }

    if (NULL == m_msg2rows[cmdtype].parser)
    {
        return true;
    }

    if (false == m_msg2rows[cmdtype].parser(msg, conn))
    {
        return false;
    }
    return true;
}
