#ifndef _RIPPLE_SINVAL_H
#define _RIPPLE_SINVAL_H

typedef struct
{
	int8		id;				/* cache ID --- must be first */
	Oid			dbId;			/* database ID, or 0 if a shared relation */
	uint32		hashValue;		/* hash value of key for this catcache */
} SharedInvalCatcacheMsg;

#define SHAREDINVALCATALOG_ID	(-1)

typedef struct
{
	int8		id;				/* type field --- must be first */
	Oid			dbId;			/* database ID, or 0 if a shared catalog */
	Oid			catId;			/* ID of catalog whose contents are invalid */
} SharedInvalCatalogMsg;


#define SHAREDINVALRELCACHE_ID	(-2)

typedef struct
{
	int8		id;				/* type field --- must be first */
	Oid			dbId;			/* database ID, or 0 if a shared relation */
	Oid			relId;			/* relation ID, or 0 if whole relcache */
} SharedInvalRelcacheMsg;

#define SHAREDINVALSMGR_ID		(-3)

typedef struct
{
	/* note: field layout chosen to pack into 16 bytes */
	int8		id;				/* type field --- must be first */
	int8		backend_hi;		/* high bits of backend ID, if temprel */
	uint16		backend_lo;		/* low bits of backend ID, if temprel */
	RelFileNode rnode;			/* spcNode, dbNode, relNode */
} SharedInvalSmgrMsg;

#define SHAREDINVALRELMAP_ID	(-4)

typedef struct
{
	int8		id;				/* type field --- must be first */
	Oid			dbId;			/* database ID, or 0 for shared catalogs */
} SharedInvalRelmapMsg;

#define SHAREDINVALSNAPSHOT_ID	(-5)

typedef struct
{
	int8		id;				/* type field --- must be first */
	Oid			dbId;			/* database ID, or 0 if a shared relation */
	Oid			relId;			/* relation ID */
} SharedInvalSnapshotMsg;

typedef union
{
	int8		id;				/* type field --- must be first */
	SharedInvalCatcacheMsg cc;
	SharedInvalCatalogMsg cat;
	SharedInvalRelcacheMsg rc;
	SharedInvalSmgrMsg sm;
	SharedInvalRelmapMsg rm;
	SharedInvalSnapshotMsg sn;
} SharedInvalidationMessage;

#endif
