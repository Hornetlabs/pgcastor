#ifndef _ERROR_H_
#define _ERROR_H_

typedef enum ERROR
{
    ERROR_SUCCESS = 0x00,

    ERROR_APPENDMSG,

    /* timeout */
    ERROR_TIMEOUT,

    /* retry */
    ERROR_RETRY,

    /* received end replication from source */
    ERROR_ENDREPLICATION,

    /* streaming replication error from source */
    ERROR_REPLICATION,

    /* file was removed */
    ERROR_FILEREMOVED,

    /*----------add error codes below-----------------*/
    /*                   |                      */
    /*                   |                      */
    /*                   |                      */
    /*                   |                      */
    /*                  \|/                     */

    /* block is incomplete */
    ERROR_BLK_INCOMPLETE,

    /* queue is full */
    ERROR_QUEUE_FULL,

    /* out of memory */
    ERROR_OOM,

    /* file does not exist */
    ERROR_NOENT,

    /* failed to open file */
    ERROR_OPENFILEERROR,

    /* failed to read file */
    ERROR_READFILEERROR,

    /* unsupported message */
    ERROR_MSGUNSPPORT,

    /* command execution failed */
    ERROR_MSGCOMMAND,

    /* invalid command */
    ERROR_MSGCOMMANDUNVALID,

    /* already exists */
    ERROR_MSGEXIST,

    /* disconnected */
    ERROR_DISCONN,

    /* worker thread failed to start */
    ERROR_STARTTHREAD,

    /* add new codes before this line */
    ERROR_MAX
} error;

#endif
