#ifndef _RIPPLE_FILE_PERM_H
#define _RIPPLE_FILE_PERM_H

/*
 * Mode mask for data directory permissions that only allows the owner to
 * read/write directories and files.
 *
 * This is the default.
 */
#define RIPPLE_MODE_MASK_OWNER		    (S_IRWXG | S_IRWXO)

/*
 * Mode mask for data directory permissions that also allows group read/execute.
 */
#define RIPPLE_MODE_MASK_GROUP			(S_IWGRP | S_IRWXO)

/* Default mode for creating directories */
#define RIPPLE_DIR_MODE_OWNER			S_IRWXU

/* Mode for creating directories that allows group read/execute */
#define RIPPLE_DIR_MODE_GROUP			(S_IRWXU | S_IRGRP | S_IXGRP)

/* Default mode for creating files */
#define RIPPLE_FILE_MODE_OWNER		    (S_IRUSR | S_IWUSR)

/* Mode for creating files that allows group read */
#define RIPPLE_FILE_MODE_GROUP			(S_IRUSR | S_IWUSR | S_IRGRP)


#endif
