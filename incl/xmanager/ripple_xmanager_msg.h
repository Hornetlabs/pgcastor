#ifndef _RIPPLE_XMANAGER_MSG_H_
#define _RIPPLE_XMANAGER_MSG_H_

typedef enum RIPPLE_XMANAGER_MSG
{
    RIPPLE_XMANAGER_MSG_NOP                             = 0,
    RIPPLE_XMANAGER_MSG_IDENTITYCMD                     ,
    RIPPLE_XMANAGER_MSG_CREATECMD                       ,
    RIPPLE_XMANAGER_MSG_ALTERCMD                        ,
    RIPPLE_XMANAGER_MSG_REMOVECMD                       ,
    RIPPLE_XMANAGER_MSG_DROPCMD                         ,
    RIPPLE_XMANAGER_MSG_INITCMD                         ,
    RIPPLE_XMANAGER_MSG_EDITCMD                         ,
    RIPPLE_XMANAGER_MSG_STARTCMD                        ,
    RIPPLE_XMANAGER_MSG_STOPCMD                         ,
    RIPPLE_XMANAGER_MSG_RELOADCMD                       ,
    RIPPLE_XMANAGER_MSG_INFOCMD                         ,
    RIPPLE_XMANAGER_MSG_WATCHCMD                        ,
    RIPPLE_XMANAGER_MSG_CONFFILECMD                     ,
    RIPPLE_XMANAGER_MSG_REFRESHCMD                      ,
    RIPPLE_XMANAGER_MSG_LISTCMD                         ,

    /* xmanager 与 capture/integrate/receivelog 内部使用 */
    /*-----------capture begin------------*/
    RIPPLE_XMANAGER_MSG_CAPTUREINCREMENT                ,
    RIPPLE_XMANAGER_MSG_CAPTUREREFRESH            ,
    RIPPLE_XMANAGER_MSG_CAPTUREBIGTXN                   ,

    /*-----------capture   end------------*/

    /*-----------integrate begin----------*/
    RIPPLE_XMANAGER_MSG_INTEGRATEINCREMENT              ,
    RIPPLE_XMANAGER_MSG_INTEGRATEONLINEREFRESH          ,
    RIPPLE_XMANAGER_MSG_INTEGRATEBIGTXN                 ,

    /*-----------integrate   end----------*/

    /*-----------hgreceivelog begin-------*/
    RIPPLE_XMANAGER_MSG_HGRECEIVELOG                    ,

    /*-----------hgreceivelog   end-------*/

    /*-----------pgreceivelog begin-------*/
    RIPPLE_XMANAGER_MSG_PGRECEIVELOG                    ,

    /*-----------pgreceivelog   end-------*/

    /* 在此前添加 */
    RIPPLE_XMANAGER_MSG_MAX                            
} ripple_xmanager_msg;


typedef enum RIPPLE_XMANAGER_METRICNODETYPE
{
    RIPPLE_XMANAGER_METRICNODETYPE_NOP                  = 0x00,

    /* capture */
    RIPPLE_XMANAGER_METRICNODETYPE_CAPTURE              ,

    /* integrate */
    RIPPLE_XMANAGER_METRICNODETYPE_INTEGRATE            ,

    /* hgreceivelog */
    RIPPLE_XMANAGER_METRICNODETYPE_HGRECEIVELOG         ,

    /* pgreceivelog */
    RIPPLE_XMANAGER_METRICNODETYPE_PGRECEIVELOG         ,

    /* process 命令 */
    RIPPLE_XMANAGER_METRICNODETYPE_PROCESS              ,

    /*---------有具体业务类型的在此上添加 ------*/

    /* all 命令 */
    RIPPLE_XMANAGER_METRICNODETYPE_ALL                  ,

    /* manager */
    RIPPLE_XMANAGER_METRICNODETYPE_MANAGER              ,

    /* xscsci */
    RIPPLE_XMANAGER_METRICNODETYPE_XSCSCI               ,

    /* 在此之前添加 */
    RIPPLE_XMANAGER_METRICNODETYPE_MAX
} ripple_xmanager_metricnodetype;

typedef enum RIPPLE_XMANAGER_METRICNODEACTION
{
    RIPPLE_XMANAGER_METRICNODEACTION_NOP                = 0x00,
    RIPPLE_XMANAGER_METRICNODEACTION_ADD                ,
    RIPPLE_XMANAGER_METRICNODEACTION_REMOVE             ,

    /* 在此前添加 */
    RIPPLE_XMANAGER_METRICNODEACTION_MAX
} ripple_xmanager_metricnodeaction;

typedef enum RIPPLE_XMANAGER_MSGVALUETYPE
{
    RIPPLE_XMANAGER_MSGVALUETYPE_UINT8                  = 0x00,
    RIPPLE_XMANAGER_MSGVALUETYPE_UINT16                 ,
    RIPPLE_XMANAGER_MSGVALUETYPE_UINT32                 ,
    RIPPLE_XMANAGER_MSGVALUETYPE_UIN64                  ,
    RIPPLE_XMANAGER_MSGVALUETYPE_CHAR                   ,
    RIPPLE_XMANAGER_MSGVALUETYPE_STR
} ripple_xmanager_msgvaluetype;

#endif
