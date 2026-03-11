/*-------------------------------------------------------------------------
 *
 * hash_utils.h
 *      exported definitions for src/utils/hash/hash_utils.c
 *
 *
 * All Copyright (c) 2024-2024, Byte Sync Development Group
 *
 *
 * incl/utils/hash/hash_utils.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HASH_UTILS_H
#define HASH_UTILS_H

extern uint32 uint32_hash(const void* key, Size keysize);
extern uint32 tag_hash(const void* key, Size keysize);
extern uint32 string_hash(const void* key, Size keysize);

#endif
