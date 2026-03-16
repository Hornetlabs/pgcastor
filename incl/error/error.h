#ifndef _RIPPLE_ERROR_H_
#define _RIPPLE_ERROR_H_

typedef enum RIPPLE_ERROR
{
    RIPPLE_ERROR_SUCCESS                        = 0x00,

    RIPPLE_ERROR_APPENDMSG                      ,

    /* 超时 */
    RIPPLE_ERROR_TIMEOUT                        ,

    /* retry */
    RIPPLE_ERROR_RETRY                          ,

    /* 接收到了源端发送的 end replication */
    RIPPLE_ERROR_ENDREPLICATION                 ,

    /* 接收源端的流复制错误 */
    RIPPLE_ERROR_REPLICATION                    ,

    /* 文件被删除 */
    RIPPLE_ERROR_FILEREMOVED                    ,

    /*----------在下面添加错误码-----------------*/
    /*                   |                      */
    /*                   |                      */
    /*                   |                      */
    /*                   |                      */
    /*                  \|/                     */

    /* BLOCK 块不完整 */
    RIPPLE_ERROR_BLK_INCOMPLETE                 ,

    /* 队列已满 */
    RIPPLE_ERROR_QUEUE_FULL                     ,

    /* 内存问题 */
    RIPPLE_ERROR_OOM                            ,

    /* 文件不存在 */
    RIPPLE_ERROR_NOENT                          ,

    /* 打开文件失败 */
    RIPPLE_ERROR_OPENFILEERROR                  ,

    /* 读取文件失败 */
    RIPPLE_ERROR_READFILEERROR                  ,

    /* 消息不支持 */
    RIPPLE_ERROR_MSGUNSPPORT                    ,

    /* 命令执行失败 */
    RIPPLE_ERROR_MSGCOMMAND                     ,

    /* 命令不合法 */
    RIPPLE_ERROR_MSGCOMMANDUNVALID              ,

    /* 已存在 */
    RIPPLE_ERROR_MSGEXIST                       ,

    /* 未连接 */
    RIPPLE_ERROR_DISCONN                        ,

    /* 工作线程启动失败 */
    RIPPLE_ERROR_STARTTHREAD                    ,

    /* 在此前添加 */
    RIPPLE_ERROR_MAX
} ripple_error;

#endif
