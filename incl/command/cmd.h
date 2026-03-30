#ifndef _CMD_STOP_H
#define _CMD_STOP_H

typedef enum OPTYPE
{
    OPTYPE_NOP = 0x00,
    OPTYPE_INIT = 0x01,
    OPTYPE_START = 0x02,
    OPTYPE_STOP = 0x03,
    OPTYPE_STATUS = 0x04,
    OPTYPE_RELOAD = 0x05,
    OPTYPE_ONLINEREFRESH = 0x06
} optype;

bool cmd(optype type, void* extra_config);

bool cmd_init(void* extra_config);

bool cmd_start(void* extra_config);

bool cmd_stop(void* extra_config);

bool cmd_status(void* extra_config);

bool cmd_reload(void* extra_config);

bool cmd_onlinerefresh(void* extra_config);

char* cmd_getdesc(optype type);

void cmd_printmsg(const char* msg);

#endif
