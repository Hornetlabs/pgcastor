#ifndef _XMANAGER_METRICMSGSTART_H_
#define _XMANAGER_METRICMSGSTART_H_

/*
 * Handle start command
 *  1. jobtype must be less than PROCESS
 *  2. Verify if job exists, report error if not
 *  3. Create async message and mount to xscsci node
 *  4. Execute initialization command
 */
extern bool xmanager_metricmsg_parsestart(xmanager_metric* xmetric,
                                          netpoolentry*    npoolentry,
                                          netpacket*       npacket);

/*
 * Assemble start return message
 */
extern bool xmanager_metricmsg_assemblestart(xmanager_metric* xmetric,
                                             netpoolentry*    npoolentry,
                                             dlist*           dlmsgs);
#endif
