/*
 * 解析服务端发送的数据
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <memory.h>
#include <errno.h>
#include <inttypes.h>

#include "ripple_c.h"
#include "xsynch_exbufferdata.h"
#include "xsynch_fe.h"
#include "xsynch_int.h"
#include "xsynch_feparsemsg.h"

typedef struct XSYNCH_FEPARSEMSG_PARSER
{
    xsynch_cmdtag           cmd;
    char*                   desc;

    bool (*parser)(xsynch_exbuffer msg, xsynch_conn* conn);
} xsynch_feparsemsg_parser;

/* edit消息解析 */
static bool xsynch_feparsemsg_editmsgparser(xsynch_exbuffer msg, xsynch_conn* conn)
{
    int datalen         = 0;
    uint32 rowcnt       = 0;
    uint32 rowlen       = 0;
    char* cptr          = NULL;

    if (0 == msg->len || NULL == msg->data)
    {
        return false;
    }

    cptr = msg->data;

    cptr += 13;

    /* 获取行数量 */
    memcpy(&rowcnt, cptr , 4);
    rowcnt = r_ntoh32(rowcnt);
    cptr += 4;

    /* 获取数据长度 */
    memcpy(&rowlen, cptr , 4);
    rowlen = r_ntoh32(rowlen);
    cptr += 4;

    /* 获取数据长度 */
    memcpy(&datalen, cptr , 4);
    datalen = r_ntoh32(datalen);
    cptr += 4;

    if (datalen <= 0)
    {
        xsynch_exbufferdata_append(conn->errmsg, "get wrong data length");
        return false;
    }

    /* 写入信息到rows */
    conn->result->rowcnt = rowcnt;
    conn->result->rows = XsynchRowInit(conn->result->rowcnt);

    conn->result->rows->columncnt = 1;
    conn->result->rows->columns = XsynchPairInit(conn->result->rows->columncnt);

    conn->result->rows->columns->valuelen = datalen;

    conn->result->rows->columns->value = (char* )malloc(datalen);
    if (NULL == conn->result->rows->columns->value)
    {
        xsynch_exbufferdata_append(conn->errmsg, "malloc colvalue oom");
        return false;
    }
    memset(conn->result->rows->columns->value, '\0', datalen);
    memcpy(conn->result->rows->columns->value, cptr, datalen);

    return true;
}

/* 行数据消息消息解析 */
static bool xsynch_feparsemsg_rowsmsgparser(xsynch_exbuffer msg, xsynch_conn* conn)
{
    uint16 nullmapcnt   = 0;
    uint32 rowlen       = 0;
    uint32 rowcnt       = 0;
    uint32 idx_row      = 0;
    uint32 idx_col      = 0;
    uint32 namelen      = 0;
    uint32 colcnt       = 0;
    uint32 offset       = 0;
    uint8* nullmap      = NULL;
    char* cptr          = NULL;
    xsynchrow* rows     = NULL;
    xsynchpair* keys    = NULL;
    xsynchpair* columns = NULL;

    cptr = msg->data;

    cptr += 13;

    /* 获取行数量 */
    memcpy(&rowcnt, cptr , 4);
    rowcnt = r_ntoh32(rowcnt);
    cptr += 4;

    if (rowcnt <= 0)
    {
        xsynch_exbufferdata_append(conn->errmsg, "get wrong rowcnt");
        return false;
    }

    /* 获取首行数据长度 */
    memcpy(&rowlen, cptr , 4);
    rowlen = r_ntoh32(rowlen);
    cptr += 4;

    /* 根据第一行计算列数 */
    rowlen -= 4;
    while (rowlen > offset)
    {
        memcpy(&namelen, cptr + offset , 4);
        namelen = r_ntoh32(namelen);
        offset += 4;
        offset += namelen;
        colcnt ++;
    }

    /* 初始化第一行结构 */
    keys = XsynchPairInit(colcnt);

    /* 生成列头数据 */
    for (idx_col = 0; idx_col < colcnt; idx_col++)
    {
        columns = (keys + idx_col);

        memcpy(&namelen, cptr , 4);
        namelen = r_ntoh32(namelen);
        cptr += 4;

        columns->keylen = namelen + 1;
        columns->key = (char* )malloc(columns->keylen);
        if (NULL == columns->key)
        {
            xsynch_exbufferdata_append(conn->errmsg, "malloc colkey oom");
            return false;
        }
        memset(columns->key, '\0', columns->keylen);
        memcpy(columns->key, cptr, namelen);
        cptr += namelen;
    }

    /* 生成列数据 */
    conn->result->rowcnt = rowcnt - 1;
    conn->result->rows = XsynchRowInit(conn->result->rowcnt);

    for (idx_row = 0; idx_row < conn->result->rowcnt; idx_row++)
    {
        rows = &conn->result->rows[idx_row];
        rows->columncnt = colcnt;
        rows->columns = XsynchPairInit(conn->result->rows->columncnt);

        memcpy(&rowlen, cptr , 4);
        rowlen = r_ntoh32(rowlen);
        cptr += 4;

        /* 获取nullmap长度 */
        rowlen -= 4;
        memcpy(&nullmapcnt, cptr , 2);
        nullmapcnt = r_ntoh16(nullmapcnt);
        cptr += 2;

        /* 获取nullmap内容 */
        if (0 < nullmapcnt)
        {
            nullmap = malloc(nullmapcnt);
            if (NULL == nullmap)
            {
                xsynch_exbufferdata_append(conn->errmsg, "malloc nullmap oom");
                return false;
            }
            memcpy(nullmap, cptr , nullmapcnt);
            cptr += nullmapcnt;
        }

        /* 填充列值 */
        for (idx_col = 0; idx_col < rows->columncnt; idx_col++)
        {
            columns = rows->columns + idx_col;
             /* 填充列头信息 */
            columns->keylen = keys[idx_col].keylen;
            columns->key = strdup(keys[idx_col].key);

            /* 根据nullmap判断该列是否为空，为空继续下一次 */
            if (NULL != nullmap && (nullmap[idx_col / 8] & (1U << (idx_col % 8))))
            {
                columns->value = NULL;
                columns->valuelen = 0;
                continue;
            }

            /* 获取列值长度 */
            memcpy(&columns->valuelen, cptr , 4);
            columns->valuelen = r_ntoh32(columns->valuelen);
            cptr += 4;

            /* 字符类型直接读取到value中 */
            columns->valuelen += 1;
            columns->value = (char* )malloc(columns->valuelen);

            if (NULL == columns->value)
            {
                xsynch_exbufferdata_append(conn->errmsg, "malloc colvalue oom");
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

    XsynchPairFree(colcnt, keys);
    return true;
}


static xsynch_feparsemsg_parser m_msg2rows[] =
{
    {
        T_XSYNCH_NOP,
        "unknown command",
        NULL
    },
    {
        T_XSYNCH_IDENTITYCMD,
        "identity command",
        NULL
    },
    {
        T_XSYNCH_CREATECMD,
        "create command",
        NULL
    },
    {
        T_XSYNCH_ALTERCMD,
        "alter command",
        NULL
    },
    {
        T_XSYNCH_REMOVECMD,
        "remove command",
        NULL
    },
    {
        T_XSYNCH_DROPCMD,
        "drop command",
        NULL
    },
    {
        T_XSYNCH_INITCMD,
        "init command",
        NULL
    },
    {
        T_XSYNCH_EDITCMD,
        "edit command",
        xsynch_feparsemsg_editmsgparser
    },
    {
        T_XSYNCH_STARTCMD,
        "start command",
        xsynch_feparsemsg_rowsmsgparser
    },
    {
        T_XSYNCH_STOPCMD,
        "stop command",
        xsynch_feparsemsg_rowsmsgparser
    },
    {
        T_XSYNCH_RELOADCMD,
        "reload command",
        NULL
    },
    {
        T_XSYNCH_INFOCMD,
        "info command",
        xsynch_feparsemsg_rowsmsgparser
    },
    {
        T_XSYNCH_WATCHCMD,
        "watch command",
        xsynch_feparsemsg_rowsmsgparser
    },
    {
        T_XSYNCH_CFGfILECMD,
        "config file command",
        NULL
    },
    {
        T_XSYNCH_REFRESHCMD,
        "refresh command",
        NULL
    },
    {
        T_XSYNCH_LISTCMD,
        "list command",
        xsynch_feparsemsg_rowsmsgparser
    },

    /* 在此之前添加 */
    {
        T_XSYNCH_MAX,
        "max command",
        NULL
    }
};

/*
 * 将接收到的描述符转化为解析结果
 */
bool xsynch_feparsemsg_msg2result(xsynch_exbuffer msg, xsynch_conn* conn)
{
    uint8 success       = 0;
    int cmdtype         = 0;
    int errlen          = 0;
    char* cptr          = NULL;

    if (0 == msg->len)
    {
        return false;
    }

    cptr = msg->data;

    /* 跳过 总长度，crc */
    cptr += 8;

    /* 获取操作类型 */
    memcpy(&cmdtype, cptr , 4);
    cmdtype = r_ntoh32(cmdtype);
    cptr += 4;

    /* 获取成功/失败 */
    memcpy(&success, cptr , 1);
    cptr += 1;

    XsynchResultReset(conn->result);

    /* 重置错误信息 */
    conn->errcode = 0;
    if (false == xsynch_exbufferdata_reset(conn->errmsg))
    {
        return false;
    }

    /* 失败处理 */
    if (1 == success)
    {
        /* 获取错误信息长度 */
        memcpy(&errlen, cptr , 4);
        errlen = r_ntoh32(errlen);
        cptr += 4;

        /* 获取错误码 */
        memcpy(&conn->errcode, cptr , 4);
        conn->errcode = r_ntoh32(conn->errcode);
        cptr += 4;

        /*  写入错误信息 */
        if (false == xsynch_exbufferdata_appendbinary(conn->errmsg, cptr, (errlen - 4)))
        {
            return false;
        }
        return true;
    }

    if (T_XSYNCH_MAX < cmdtype)
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
