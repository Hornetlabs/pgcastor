#ifndef _XMANAGER_METRICMSGDROP_H_
#define _XMANAGER_METRICMSGDROP_H_

/*
 * Handle drop command
 *  1. jobtype must be less than PROCESS
 *  2. Verify if job already exists
 *  3. Check if running
 *  4. Delete data and conf files
 *  5. Return success message
 */
extern bool xmanager_metricmsg_parsedrop(xmanager_metric* xmetric, netpoolentry* npoolentry, netpacket* npacket);

#endif
