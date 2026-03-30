#ifndef _XMANAGER_MSG_H_
#define _XMANAGER_MSG_H_

typedef enum XMANAGER_MSG
{
    XMANAGER_MSG_NOP = 0,
    XMANAGER_MSG_IDENTITYCMD,
    XMANAGER_MSG_CREATECMD,
    XMANAGER_MSG_ALTERCMD,
    XMANAGER_MSG_REMOVECMD,
    XMANAGER_MSG_DROPCMD,
    XMANAGER_MSG_INITCMD,
    XMANAGER_MSG_EDITCMD,
    XMANAGER_MSG_STARTCMD,
    XMANAGER_MSG_STOPCMD,
    XMANAGER_MSG_RELOADCMD,
    XMANAGER_MSG_INFOCMD,
    XMANAGER_MSG_WATCHCMD,
    XMANAGER_MSG_CONFFILECMD,
    XMANAGER_MSG_REFRESHCMD,
    XMANAGER_MSG_LISTCMD,

    /* xmanager internal use with capture/integrate/receivelog */
    /*-----------capture begin------------*/
    XMANAGER_MSG_CAPTUREINCREMENT,
    XMANAGER_MSG_CAPTUREREFRESH,
    XMANAGER_MSG_CAPTUREBIGTXN,

    /*-----------capture   end------------*/

    /*-----------integrate begin----------*/
    XMANAGER_MSG_INTEGRATEINCREMENT,
    XMANAGER_MSG_INTEGRATEONLINEREFRESH,
    XMANAGER_MSG_INTEGRATEBIGTXN,

    /*-----------integrate   end----------*/

    /*-----------pgreceivelog begin-------*/
    XMANAGER_MSG_PGRECEIVELOG,

    /*-----------pgreceivelog   end-------*/

    /* Add before this */
    XMANAGER_MSG_MAX
} xmanager_msg;

typedef enum XMANAGER_METRICNODETYPE
{
    XMANAGER_METRICNODETYPE_NOP = 0x00,

    /* capture */
    XMANAGER_METRICNODETYPE_CAPTURE,

    /* integrate */
    XMANAGER_METRICNODETYPE_INTEGRATE,

    /* pgreceivelog */
    XMANAGER_METRICNODETYPE_PGRECEIVELOG,

    /* process command */
    XMANAGER_METRICNODETYPE_PROCESS,

    /*---------Add specific business types above ------*/

    /* all command */
    XMANAGER_METRICNODETYPE_ALL,

    /* manager */
    XMANAGER_METRICNODETYPE_MANAGER,

    /* xscsci */
    XMANAGER_METRICNODETYPE_XSCSCI,

    /* Add before this */
    XMANAGER_METRICNODETYPE_MAX
} xmanager_metricnodetype;

typedef enum XMANAGER_METRICNODEACTION
{
    XMANAGER_METRICNODEACTION_NOP = 0x00,
    XMANAGER_METRICNODEACTION_ADD,
    XMANAGER_METRICNODEACTION_REMOVE,

    /* Add before this */
    XMANAGER_METRICNODEACTION_MAX
} xmanager_metricnodeaction;

typedef enum XMANAGER_MSGVALUETYPE
{
    XMANAGER_MSGVALUETYPE_UINT8 = 0x00,
    XMANAGER_MSGVALUETYPE_UINT16,
    XMANAGER_MSGVALUETYPE_UINT32,
    XMANAGER_MSGVALUETYPE_UIN64,
    XMANAGER_MSGVALUETYPE_CHAR,
    XMANAGER_MSGVALUETYPE_STR
} xmanager_msgvaluetype;

#endif
