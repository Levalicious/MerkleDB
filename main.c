#include <stdio.h>
#include <stdint.h>
#include <sys/random.h>

#include "merkledb.h"

int main() {
    MerkleDB* db = openDB("test");
    uint8_t* key = malloc(32);
    getrandom(key, 32, 0);

    uint8_t* val = malloc(104);
    getrandom(val, 104, 0);

    for (int i = 0; i < 5; i++) {
        update(db, key, val, 104);
        getrandom(key, 32, 0);
        getrandom(val, 104, 0);
    }

    free(key);
    free(val);

    free(db);

    printf("\n");
}