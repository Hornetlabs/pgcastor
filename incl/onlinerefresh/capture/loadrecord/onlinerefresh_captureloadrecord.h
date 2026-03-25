#ifndef ONLINEREFRESH_CAPTURELOADRECORD_H
#define ONLINEREFRESH_CAPTURELOADRECORD_H

typedef struct ONLINEREFRESH_CAPTURELOADRECORD
{
    splitwalcontext* splitwalctx;
} onlinerefresh_captureloadrecord;

extern onlinerefresh_captureloadrecord* onlinerefresh_captureloadrecord_init(void);

#endif
