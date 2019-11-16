# MerkleDB
A Radix-2 Merkle Patricia, implemented as a sparse merkle tree stored in an on-disk datastore.

It seems to work, however, it may have bugs I haven't found yet. Also, I've put it through Valgrind a few times and haven't found any memory leaks.

Each internal node for the tree is exactly 44 bytes, which is as low as I could push it.

This is in no way or shape ACID compliant, capable of surviving any sort of unplanned shutdown mid-operation or anything nice like that. Eventually I might be able to integrate that.

Pull requests are always welcome!
