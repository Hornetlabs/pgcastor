#ifndef _RIPPLE_ONLINEREFRESH_INTEGRATESPLITTRAIL_H
#define _RIPPLE_ONLINEREFRESH_INTEGRATESPLITTRAIL_H

typedef struct RIPPLE_ONLINEREFRESH_INTEGRATESPLITTRAIL
{
    ripple_increment_integratesplittrail*   splittrailctx;
}ripple_onlinerefresh_integratesplittrail;

ripple_onlinerefresh_integratesplittrail* ripple_onlinerefresh_integratesplittrail_init(void);

void *ripple_onlinerefresh_integratesplittrail_main(void *args);

void ripple_onlinerefresh_integratesplittrail_free(void *args);

#endif
