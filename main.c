#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <sys/random.h>
#include <sys/time.h>

#include "merkledb.h"

#define COUNT 5000000

int main() {
    MerkleDB* db = openDB("test");
    uint8_t* key = malloc(32);

    uint8_t* val = malloc(104);

    struct timeval begin, end;

    gettimeofday(&begin, NULL);
    for (size_t i = 0; i < COUNT; i++) {
        memcpy(key, (uint8_t*)&i, sizeof(size_t));
        update(db, key, val, 104);
    }
    gettimeofday(&end, NULL);

    free(key);
    free(val);

    free(db);

    printf("Took %lu nanoseconds to insert %d values.\n",
            ((end.tv_sec - begin.tv_sec) * 1000000) + (end.tv_usec - begin.tv_usec),
            COUNT);
    printf("%lu nanoseconds each.", ((((end.tv_sec - begin.tv_sec) * 1000000) + (end.tv_usec - begin.tv_usec)) / COUNT));
}