#ifndef merkledbH
#define merkledbH

#include <stdint.h>
#include <stdlib.h>

#define NODE_SIZE sizeof(InternalNode)
#define PAGE_TABLE_SIZE 8192

#define DAT_FILE_SIZE 65536
#define TRI_FILE_SIZE 2147483648

typedef struct {
    uint8_t hash[32]; /* 32 */
    uint32_t left_pos; /* 32 + 4 = 36 */
    uint32_t right_pos; /* 36 + 4 = 40 */
    uint16_t left_file; /* 40 + 2 = 42 */
    uint16_t right_file; /* 42 + 2 = 44 */
} InternalNode;

typedef struct {
    uint8_t root[32];
    uint16_t rootFile;
    uint32_t rootPos;
    uint16_t triCurrent;
    uint16_t datCurrent;
    uint8_t currentNodeBytes[NODE_SIZE];
    InternalNode* currentNode;
    uint32_t triLen[PAGE_TABLE_SIZE];
    uint32_t datLen[PAGE_TABLE_SIZE];
    char *name;
} MerkleDB;

typedef struct {
    uint16_t len;
    uint8_t * val;
} Slice;

char* to_string(uint8_t* arr, size_t len);
Slice from_string(char* str);
Slice* get(MerkleDB* db, uint8_t* key);
void update(MerkleDB* db, uint8_t* key, uint8_t * val, uint16_t len);
void initDB(MerkleDB* db, char* name);
MerkleDB* openDB(char* name);



#endif