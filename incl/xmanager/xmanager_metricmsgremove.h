#ifndef _XMANAGER_METRICMSGREMOVE_H_
#define _XMANAGER_METRICMSGREMOVE_H_

/*
 * Handle remove command
 *  1. jobtype must be less than PROCESS
 *  2. Verify if job already exists
 *  3. Check if running
 *  4. Delete conf file
 *  5. Return success message
 */
extern bool xmanager_metricmsg_parseremove(xmanager_metric* xmetric, netpoolentry* npoolentry,
                                           netpacket* npacket);

#endif
