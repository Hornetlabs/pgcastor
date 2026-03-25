#ifndef _XMANAGER_METRICMSGINIT_H_
#define _XMANAGER_METRICMSGINIT_H_

/*
 * Handle init command
 *  1. jobtype must be less than PROCESS
 *  2. Verify if job exists, report error if not
 *  3. Create async message and mount to xscsci node
 *  4. Execute initialization command
 */
extern bool xmanager_metricmsg_parseinit(xmanager_metric* xmetric, netpoolentry* npoolentry,
                                         netpacket* npacket);

/*
 * Assemble init return message
 */
extern bool xmanager_metricmsg_assembleinit(xmanager_metric* xmetric, netpoolentry* npoolentry,
                                            dlist* dlmsgs);

#endif
