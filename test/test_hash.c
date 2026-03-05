#include "ripple_app_incl.h"
#include "utils/hash/hash_search.h"

typedef struct test_hash_key
{
    int32_t     key1;
    int32_t     key2;
}test_hash_key;

typedef struct test_hash_entry
{
    test_hash_key key;
    int32_t     value1;
    char       *value2;
}test_hash_entry;

static void hash_seq_scan_test(HTAB *hashTb)
{
    HASH_SEQ_STATUS hash_seq;
    test_hash_entry *entry = NULL;
    int num = 0;

    hash_seq_init(&hash_seq, hashTb);

    while ((entry = hash_seq_search(&hash_seq)) != NULL)
    {
        printf("find seq hash\n");
        printf("        |-------->value1: %d\n", entry->value1);
        printf("        |-------->value2: %s\n\n", entry->value2);
        free(entry->value2);
        entry->value2 = NULL;
        num++;
    }
    printf("----- hash entry num: %d -----\n\n", num);
}

static void hash_insert_or_find_test(HTAB *hashTb)
{
    int i = 0;

    for (i = 0; i < 10; i++)
    {
        test_hash_entry *entry = NULL;
        test_hash_key key = {'\0'};
        bool isfind = false;

        key.key1 = i;
        key.key2 = i + 1;

        entry = hash_search(hashTb, &key, HASH_ENTER, &isfind);
        if (isfind)
        {
            printf("find hash\n");
            printf("        |-------->value1: %d\n", entry->value1);
            printf("        |-------->value2: %s\n\n", entry->value2);
        }
        else
        {
            char str[64] = {'\0'};
            printf("insert hash\n");
            printf("        |-------->key1: %d\n", key.key1);
            printf("        |-------->key2: %d\n", key.key2);
            sprintf(str, "HASH TEST %d", i + 1);
            entry->value1 = i + 1;
            entry->value2 = malloc(strlen(str) + 1);
            strcpy(entry->value2, str);
            entry->value2[strlen(str)] = '\0';
        }
    }
}

int main(void)
{
    HASHCTL hashCtl = {'\0'};
    HTAB   *hashTb = NULL;
    hashCtl.keysize = sizeof(test_hash_key);
    hashCtl.entrysize = sizeof(test_hash_entry);
    //char *test_str = malloc(10);
    //memset(test_str, 1, 100);
    //char *s = (char*)malloc(100);

    hashTb = hash_create("HASH_TEST",
                          128,
                         &hashCtl,
                          HASH_ELEM | HASH_BLOBS);

    /* 第一次做insert操作 */
    printf(">>>>>>>>>> begin insert hash entry <<<<<<<<<<\n");
    hash_insert_or_find_test(hashTb);

    printf(">>>>>>>>>> begin search hash entry <<<<<<<<<<\n");
    hash_insert_or_find_test(hashTb);

    /* 顺序查找输出 */
    printf(">>>>>>>>>> begin seqscan hash entry <<<<<<<<<<\n");
    hash_seq_scan_test(hashTb);

    hash_destroy(hashTb);
    return 0;
}
