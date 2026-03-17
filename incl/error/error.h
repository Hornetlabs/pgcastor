#ifndef _ERROR_H_
#define _ERROR_H_

typedef enum ERROR
{
    ERROR_SUCCESS                        = 0x00,

    ERROR_APPENDMSG                      ,

    /* 超时 */
    ERROR_TIMEOUT                        ,

    /* retry */
    ERROR_RETRY                          ,

    /* 接收到了源端发送的 end replication */
    ERROR_ENDREPLICATION                 ,

    /* 接收源端的流复制错误 */
    ERROR_REPLICATION                    ,

    /* 文件被删除 */
    ERROR_FILEREMOVED                    ,

    /*----------在下面添加错误码-----------------*/
    /*                   |                      */
    /*                   |                      */
    /*                   |                      */
    /*                   |                      */
    /*                  \|/                     */

    /* BLOCK 块不完整 */
    ERROR_BLK_INCOMPLETE                 ,

    /* 队列已满 */
    ERROR_QUEUE_FULL                     ,

    /* 内存问题 */
    ERROR_OOM                            ,

    /* 文件不存在 */
    ERROR_NOENT                          ,

    /* 打开文件失败 */
    ERROR_OPENFILEERROR                  ,

    /* 读取文件失败 */
    ERROR_READFILEERROR                  ,

    /* 消息不支持 */
    ERROR_MSGUNSPPORT                    ,

    /* 命令执行失败 */
    ERROR_MSGCOMMAND                     ,

    /* 命令不合法 */
    ERROR_MSGCOMMANDUNVALID              ,

    /* 已存在 */
    ERROR_MSGEXIST                       ,

    /* 未连接 */
    ERROR_DISCONN                        ,

    /* 工作线程启动失败 */
    ERROR_STARTTHREAD                    ,

    /* 在此前添加 */
    ERROR_MAX
} error;

#endif
