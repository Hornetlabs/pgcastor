#ifndef _XMANAGER_METRICMSGALTER_H_
#define _XMANAGER_METRICMSGALTER_H_

/*
 * Handle alter command
 *  1、jobtype equals PROCESS
 *  2、If capture, check if there is same type job
 *  3、add/remove job
 *  4. Assemble return message
 */
extern bool xmanager_metricmsg_parsealter(xmanager_metric* xmetric,
                                          netpoolentry*    npoolentry,
                                          netpacket*       npacket);

#endif
