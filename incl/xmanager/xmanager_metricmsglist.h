#ifndef _XMANAGER_METRICMSGLIST_H_
#define _XMANAGER_METRICMSGLIST_H_

/*
 * Handle list command
 *  1. Return metricnode information
 */
extern bool xmanager_metricmsg_parselist(xmanager_metric* xmetric,
                                         netpoolentry*    npoolentry,
                                         netpacket*       npacket);

#endif
