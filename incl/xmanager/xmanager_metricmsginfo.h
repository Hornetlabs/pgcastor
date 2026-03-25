#ifndef _XMANAGER_METRICMSGINFO_H_
#define _XMANAGER_METRICMSGINFO_H_

/*
 * Handle info command
 *  1. jobtype must be less than ALL
 *  2. Verify if job already exists
 *  3. Get information and return
 */
extern bool xmanager_metricmsg_parseinfo(xmanager_metric* xmetric, netpoolentry* npoolentry,
                                         netpacket* npacket);

#endif
