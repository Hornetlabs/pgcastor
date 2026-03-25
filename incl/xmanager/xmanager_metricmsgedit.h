#ifndef _XMANAGER_METRICMSGEDIT_H_
#define _XMANAGER_METRICMSGEDIT_H_

/*
 * Handle edit command
 *  1. jobtype must be less than PROCESS
 *  2. Verify if job exists, report error if not
 *  3. Get config file content
 *  4. Assemble return message
 */
extern bool xmanager_metricmsg_parseedit(xmanager_metric* xmetric, netpoolentry* npoolentry,
                                         netpacket* npacket);

#endif
