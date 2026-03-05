#ifndef _RIPPLE_CMD_STOP_H
#define _RIPPLE_CMD_STOP_H

typedef enum RIPPLE_OPTYPE
{
    RIPPLE_OPTYPE_NOP           = 0x00,
    RIPPLE_OPTYPE_INIT          = 0x01,
    RIPPLE_OPTYPE_START         = 0x02,
    RIPPLE_OPTYPE_STOP          = 0x03,
    RIPPLE_OPTYPE_STATUS        = 0x04,
    RIPPLE_OPTYPE_RELOAD        = 0x05,
    RIPPLE_OPTYPE_ONLINEREFRESH = 0x06
} ripple_optype;

bool ripple_cmd(ripple_optype type, void *extra_config);

bool ripple_cmd_init(void *extra_config);

bool ripple_cmd_start(void *extra_config);

bool ripple_cmd_stop(void *extra_config);

bool ripple_cmd_status(void *extra_config);

bool ripple_cmd_reload(void *extra_config);

bool ripple_cmd_onlinerefresh(void *extra_config);

char* ripple_cmd_getdesc(ripple_optype type);

void ripple_cmd_printmsg(const char *msg);


#endif
