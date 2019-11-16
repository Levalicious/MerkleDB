#include "merkledb.h"
#include "blake2/blake2.h"

#include <dirent.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

typedef struct {
    uint16_t file;
    uint32_t pos;
} Loc;

char* to_string(uint8_t* arr, size_t len) {
    char* out = calloc(1, (len * 2) + 1);
    for (size_t i = 0; i < len; i++) {
        sprintf(out, "%02x", arr[i]);
        out += 2;
    }

    out -= (len * 2);

    return out;
}

Slice from_string(char* str) {
    Slice out;
    char* copy = str;
    out.len = strlen(str) / 2;
    out.val = malloc(out.len);
    for (int i = 0; i < out.len; i++) {
        sscanf(copy, "%2hhx", &out.val[i]);
        copy += 2;
    }
    free(copy);
    return out;
}

FILE* getTriFile(MerkleDB* db, uint16_t file) {
    char *path = malloc(256);
    path[0] = '\0';
    sprintf(path, "%s/trie/%d.tri", db->name, file);
    FILE* out = fopen(path, "rb+");
    free(path);
    return out;
}

FILE* currentTriFile(MerkleDB* db) {
    return getTriFile(db, db->triCurrent);
}

FILE* getValFile(MerkleDB* db, uint16_t file) {
    char *path = malloc(256);
    path[0] = '\0';
    sprintf(path, "%s/data/%d.dat", db->name, file);
    FILE* out = fopen(path, "rb+");
    free(path);
    return out;
}

FILE* currentValFile(MerkleDB* db) {
    return getValFile(db, db->datCurrent);
}

void addTriFile(MerkleDB* db) {
    char* path = calloc(1, 512);
    sprintf(path, "%s/trie/%d.tri", db->name, db->triCurrent);
    FILE* curr = fopen(path, "wb+");
    free(path);
    fflush(curr);
    fclose(curr);
    db->triLen[db->triCurrent] = 0;
    db->triCurrent++;
}

void addValFile(MerkleDB* db) {
    char* path = calloc(1, 512);
    sprintf(path, "%s/data/%d.dat", db->name, db->datCurrent);
    FILE* curr = fopen(path, "wb+");
    free(path);
    fflush(curr);
    fclose(curr);
    db->datLen[db->datCurrent] = 0;
    db->datCurrent++;
}

uint32_t fileSize(FILE* curr) {
    fseek(curr, 0, SEEK_END);
    return (uint32_t)ftell(curr);
}

InternalNode* readNode(MerkleDB* db, uint16_t file, uint32_t pos) {
    FILE* curr = getTriFile(db, file);
    fseek(curr, pos, SEEK_SET);
    fread(db->currentNodeBytes, 1, NODE_SIZE, curr);
    fclose(curr);
    return (InternalNode*) db->currentNodeBytes;
}

Slice* readVal(MerkleDB* db, uint16_t file, uint32_t pos) {
    FILE* curr = getValFile(db, file);
    fseek(curr, pos, SEEK_SET);
    Slice* out = calloc(1, sizeof(Slice));
    fread((uint8_t*)&out->len, 1, 2, curr);
    out->val = malloc(out->len);
    fread(out->val, 1, out->len, curr);
    fclose(curr);
    return out;
}

void delVal(MerkleDB* db, uint16_t file, uint32_t pos) {
    FILE* curr = getValFile(db, file);
    fseek(curr, pos, SEEK_SET);
    uint16_t* len = malloc(2);
    fread((uint8_t*)len, 1, 2, curr);

    uint32_t next = pos + *len + 2;

    uint32_t size = fileSize(curr);

    uint8_t* arr = malloc(size - next);
    fseek(curr, next, SEEK_SET);
    fread(arr, 1, size - next, curr);
    fseek(curr, pos, SEEK_SET);
    fwrite(arr, 1, size - next, curr);
    ftruncate(fileno(curr), size - (*len + 2));
    fflush(curr);
    fclose(curr);

    db->datLen[file] = size - (*len + 2);
}

Loc appendVal(MerkleDB* db, Slice* val) {
    uint16_t i = 0;

    for (i = 0; i < 8192; i++) {
        if (db->datLen[i] == 0) { addValFile(db); break; }
        if (db->datLen[i] < DAT_FILE_SIZE) break;
    }

    FILE* curr = getValFile(db, i);
    Loc out = { i, db->datLen[i]};
    fseek(curr, 0, SEEK_END);
    fwrite((uint8_t*)&val->len, 1, 2, curr);
    fwrite(val->val, 1, val->len, curr);
    db->datLen[i] += val->len + 2;
    fflush(curr);
    fclose(curr);
    return out;
}

Loc appendNode(MerkleDB* db, InternalNode* node) {
    uint16_t i = 0;

    for (i = 0; i < 8192; i++) {
        if (db->triLen[i] == 0) { addTriFile(db); break; }
        if (db->triLen[i] < TRI_FILE_SIZE) break;
    }

    FILE* curr = getTriFile(db, i);
    fseek(curr, 0, SEEK_END);
    Loc out = { i, db->triLen[i]};
    fwrite((uint8_t*)node, 1, sizeof(InternalNode), curr);
    db->triLen[i] += sizeof(InternalNode);
    fflush(curr);
    fclose(curr);
    return out;
}

void overwriteNode(MerkleDB* db, uint16_t file, uint32_t pos, InternalNode* node) {
    FILE* curr = getTriFile(db, file);
    fseek(curr, pos, SEEK_SET);
    fwrite((uint8_t*)node, 1, NODE_SIZE, curr);
    fflush(curr);
    fclose(curr);
}

uint8_t* getHash(MerkleDB* db, uint16_t file, uint32_t pos) {
    FILE* curr = getTriFile(db, file);
    fseek(curr, pos, SEEK_SET);
    uint8_t* out = malloc(32);
    fread(out, 1, 32, curr);
    fclose(curr);
    return out;
}

bool* keyToPath(const uint8_t * const key) {
    bool* out = malloc(sizeof(bool) * 256);
    for (int i = 0; i < 32; i++) {
        uint8_t current = key[i];
        for (int j = 7; j >= 0; j--) {
            out[i * 8 + j] = !!(current & 1U);
            current >>= 1U;
        }
    }

    return out;
}

bool isZero(const uint8_t * const arr) {
    for (int i = 0; i < 32; i++) {
        if (arr[i] != 0) return false;
    }

    return true;
}

uint8_t* hash2(uint8_t* in, uint8_t* in2) {
    if (isZero(in) && isZero(in2)) return calloc(1, 32);

    uint8_t* merge = malloc(64);
    memcpy(merge, in, 32);
    memcpy(merge + 32, in2, 32);
    uint8_t* hash = calloc(1, 32);
    blake2(hash, 32, merge, 64, NULL, 0);
    free(merge);
    return hash;
}

uint8_t* hash(uint8_t* in, uint16_t len) {
    bool zero = true;
    if (len != 32) zero = false;
    for (int i = 0; i < 32; i++) {
        if (in[i] != 0) { zero = false; break; }
    }

    if (zero) return calloc(1, 32);

    uint8_t* out = malloc(32);
    blake2(out, 32, in, len, NULL, 0);
    return out;
}

uint8_t* copy(const uint8_t * const val, uint16_t len) {
    uint8_t* out = malloc(len);
    for (int i = 0; i < len; i++) {
        out[i] = val[i];
    }
    return out;
}

void hashUpSlices(MerkleDB* db, InternalNode* node) {
    Slice* c = readVal(db, node->left_file, node->left_pos);
    Slice* d = readVal(db, node->right_file, node->right_pos);

    uint8_t* hLeft = hash(c->val, c->len);
    uint8_t* hRight = hash(d->val, d->len);

    uint8_t* h = hash2(hLeft, hRight);
    for (int i = 0; i < 32; i++) {
        node->hash[i] = h[i];
    }
    free(c->val);
    free(d->val);
    free(c);
    free(d);
    free(hLeft);
    free(hRight);
    free(h);
}

bool isZerohash(MerkleDB* db) {
    for (int i = 0; i < 32; i++) {
        if (db->currentNodeBytes[i] != 0) return false;
    }

    return true;
}

Loc getLoc(InternalNode* node, bool path) {
    Loc out;
    if (!path) {
        out.file = node->left_file;
        out.pos = node->left_pos;
    } else {
        out.file = node->right_file;
        out.pos = node->right_pos;
    }
    return out;
}

Slice* get(MerkleDB* db, uint8_t* key) {
    /* Read only */
    readNode(db, db->rootFile, db->rootPos);

    bool* path = keyToPath(key);

    for (int i = 0; i < 255; i++) {
        if (!path[i]) {
            readNode(db, db->currentNode->left_file, db->currentNode->left_pos);
        } else {
            readNode(db, db->currentNode->right_file, db->currentNode->right_pos);
        }
    }

    uint16_t valFile;
    uint32_t valPos;

    if (!path[255]) {
        valFile = db->currentNode->left_file;
        valPos = db->currentNode->left_pos;
    } else {
        valFile = db->currentNode->right_file;
        valPos = db->currentNode->right_pos;
    }

    return readVal(db, valFile, valPos);
}

void writeState(MerkleDB* db) {
    char* path = calloc(1, 512);
    sprintf(path, "%s/state.dbconf", db->name);
    FILE* state = fopen(path, "rb+");
    ftruncate(fileno(state), 0);
    free(path);
    fseek(state, 0, SEEK_SET);
    fwrite((uint8_t*) db, 1, sizeof(MerkleDB), state);
    fwrite(db->name, 1, strlen(db->name), state);
    fflush(state);
    fclose(state);
}

void update(MerkleDB* db, uint8_t* key, uint8_t * val, uint16_t len) {
    /* Read only zone */
    readNode(db, db->rootFile, db->rootPos);

    InternalNode* pathNodes = malloc(sizeof(InternalNode) * 256);
    uint8_t** sideHashes = malloc(sizeof(uint8_t*) * 256);

    bool* path = keyToPath(key);

    for (int i = 0; i < 255; i++) {
        pathNodes[i] = *db->currentNode;
        if (!path[i]) {
            sideHashes[i + 1] = getHash(db, db->currentNode->right_file, db->currentNode->right_pos);
            readNode(db, db->currentNode->left_file, db->currentNode->left_pos);
        } else {
            sideHashes[i + 1] = getHash(db, db->currentNode->left_file, db->currentNode->left_pos);
            readNode(db, db->currentNode->right_file, db->currentNode->right_pos);
        }
    }

    pathNodes[255] = *db->currentNode;

    uint16_t valFile;
    uint32_t valPos;

    if (!path[255]) {
        valFile = db->currentNode->left_file;
        valPos = db->currentNode->left_pos;
    } else {
        valFile = db->currentNode->right_file;
        valPos = db->currentNode->right_pos;
    }

    /* Write zone begins */
    if (!(valFile == 0 && valPos == 0)) {
        /* If it's *not* a zerohash val, delete it */
        delVal(db, valFile, valPos);
    }

    /* Append the new value */
    Slice dat = { len, val };
    Loc valLoc = appendVal(db, &dat);

    if (!path[255]) {
        pathNodes[255].left_file = valLoc.file;
        pathNodes[255].left_pos = valLoc.pos;
    } else {
        pathNodes[255].right_file = valLoc.file;
        pathNodes[255].right_pos = valLoc.pos;
    }

    Loc loc;
    if (isZerohash(db)) {
        hashUpSlices(db, &pathNodes[255]);
        loc = appendNode(db, &pathNodes[255]);
    } else {
        loc = getLoc(&pathNodes[254], path[254]);
        hashUpSlices(db, &pathNodes[255]);
        overwriteNode(db, loc.file, loc.pos, &pathNodes[255]);
    }

    for (int i = 254; i >= 0; i--) {
        InternalNode* currNode = &pathNodes[i];

        /* Update hash & pointers */
        uint8_t* newHash;
        if (!path[i]) {
            newHash = hash2(pathNodes[i + 1].hash, sideHashes[i + 1]);
            currNode->left_file = loc.file;
            currNode->left_pos = loc.pos;
        } else {
            newHash = hash2(sideHashes[i + 1], pathNodes[i + 1].hash);
            currNode->right_file = loc.file;
            currNode->right_pos = loc.pos;
        }

        if (isZero(currNode->hash)) {
            for (int j = 0; j < 32; j++) {
                currNode->hash[j] = newHash[j];
            }
            if (i != 0) {
                loc = appendNode(db, currNode);
            } else {
                loc = appendNode(db, currNode);
                db->rootFile = loc.file;
                db->rootPos = loc.pos;
                for (int j = 0; j < 32; j++) {
                    db->root[j] = currNode->hash[j];
                }
            }
        } else {
            for (int j = 0; j < 32; j++) {
                currNode->hash[j] = newHash[j];
            }
            if (i != 0) {
                loc = getLoc(&pathNodes[i - 1], path[i - 1]);
                overwriteNode(db, loc.file, loc.pos, currNode);
            } else {
                overwriteNode(db, db->rootFile, db->rootPos, currNode);
                for (int j = 0; j < 32; j++) {
                    db->root[j] = currNode->hash[j];
                }
            }
        }
        free(newHash);
    }
    free(path);
    free(pathNodes);
    for (int i = 1; i < 256; i++) {
        free(sideHashes[i]);
    }
    free(sideHashes);
    writeState(db);
}

int isDir(const char *path) {
    struct stat path_stat;
    lstat(path, &path_stat);
    return S_ISDIR(path_stat.st_mode);
}

void readState(MerkleDB* db) {
    char* path = calloc(1, 512);
    sprintf(path, "%s/state.dbconf", db->name);
    FILE* state = fopen(path, "rb+");
    free(path);
    fseek(state, 0, SEEK_SET);
    fread((uint8_t*) db, 1, sizeof(MerkleDB), state);
    fseek(state, 0, SEEK_END);
    uint16_t len = ftell(state) - sizeof(MerkleDB);
    db->name = malloc(len);
    fread(db->name, 1, len, state);
    db->name = realloc(db->name, len + 1);
    db->name[len] = '\0';
    fclose(state);
}

void zeroDB(MerkleDB* db) {
    for (int i = 0; i < 32; i++) {
        db->root[i] = 0;
    }

    db->rootFile = 0;
    db->rootPos = 0;

    db->triCurrent = 0;

    db->datCurrent = 0;


    for (size_t i = 0; i < NODE_SIZE; i++) {
        db->currentNodeBytes[i] = 0;
    }

    db->currentNode = (InternalNode*) db->currentNodeBytes;

    for (size_t i = 0; i < PAGE_TABLE_SIZE; i++) {
        db->triLen[i] = 0;
        db->datLen[i] = 0;
    }
}

void initDB(MerkleDB* db, char* name) {
    db->name = name;

    mkdir(db->name, 0700);

    char* path = calloc(1, 512);

    sprintf(path, "%s/data", db->name);
    mkdir(path, 0700);
    memset(path, 0, 512);

    sprintf(path, "%s/trie", db->name);
    mkdir(path, 0700);
    memset(path, 0, 512);

    sprintf(path, "%s/state.dbconf", db->name);
    FILE* curr = fopen(path, "wb+");
    fclose(curr);
    free(path);
    db->datLen[db->datCurrent] = 0;

    zeroDB(db);

    uint8_t *arr = calloc(1, 32);
    Slice zeroVal = { 32, arr };

    for (size_t i = 0; i < NODE_SIZE; i++) {
        db->currentNodeBytes[i] = 0;
    }

    db->currentNode = (InternalNode*) db->currentNodeBytes;

    db->triLen[0] = 0;
    db->datLen[0] = 0;
    appendVal(db, &zeroVal);
    appendNode(db, db->currentNode);

    free(arr);

    writeState(db);
}

MerkleDB* openDB(char* name) {
    DIR* dir = opendir(name);
    if (dir) {
        MerkleDB* db = malloc(sizeof(MerkleDB));
        db->currentNode = (InternalNode*) db->currentNodeBytes;
        db->name = name;
        free(name);
        readState(db);
        closedir(dir);
        return db;
    } else {
        MerkleDB* db = malloc(sizeof(MerkleDB));
        zeroDB(db);
        initDB(db, name);
        db->namelen = strlen(name);
        closedir(dir);
        return db;
    }
}

