/*
 * All Copyright (c) 2024-2024, Byte Sync Development Group
 *
 */

#ifndef _PROCESS_H
#define _PROCESS_H

/* Close standard input/output */
void closestd(void);

/* Set to background execution */
void makedaemon(void);

/* Execute background command */
bool execcommand(char* cmd, void* args, void (*childdestroy)(void* args));

#endif
