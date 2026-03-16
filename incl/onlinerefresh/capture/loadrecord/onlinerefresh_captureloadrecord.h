#ifndef RIPPLE_ONLINEREFRESH_CAPTURELOADRECORD_H
#define RIPPLE_ONLINEREFRESH_CAPTURELOADRECORD_H

typedef struct RIPPLE_ONLINEREFRESH_CAPTURELOADRECORD
{
    ripple_splitwalcontext *splitwalctx;
} ripple_onlinerefresh_captureloadrecord;

extern ripple_onlinerefresh_captureloadrecord *ripple_onlinerefresh_captureloadrecord_init(void);

#endif
