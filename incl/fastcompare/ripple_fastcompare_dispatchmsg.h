#ifndef RIPPLE_FASTCOMPARE_DISPATCHMSG_H
#define RIPPLE_FASTCOMPARE_DISPATCHMSG_H

void ripple_fastcompare_dispatchmsg_beginslice(void* privdata, void* buffer);

void ripple_fastcompare_dispatchmsg_beginchunk(void* privdata, void* buffer);

bool ripple_fastcompare_dispatchmsg_simpledatachunk(void* privdata, void* buffer);

void ripple_fastcompare_dispatchmsg_s2dcorrectdata(void* privdata, void* buffer);

bool ripple_fastcompare_dispatchmsg_netmsg(void* privdata, uint8* msg);


#endif
