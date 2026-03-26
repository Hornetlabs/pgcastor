#ifndef _XMANAGER_METRICMSGWATCH_H_
#define _XMANAGER_METRICMSGWATCH_H_

/*
 * Handle watch command
 *  1. jobtype must be less than ALL
 *  2. Verify if job already exists
 *  3. Get information and return
 */
extern bool xmanager_metricmsg_parsewatch(xmanager_metric* xmetric,
                                          netpoolentry*    npoolentry,
                                          netpacket*       npacket);

#endif
