package byodb

import (
	"bytes"
	"encoding/binary"
)

/*

Wire format for the BTree nodes

A node consists of:

1. A fixed-sized header containing the type of the node (leaf node or internal node) and the number of keys.
2. A list of pointers to the child nodes. (Used by internal nodes).
3. A list of offsets pointing to each key-value pair.
4. Packed KV pairs.


|----header----|
| type | nkeys |  pointers  |   offsets  | key-values
|  2B  |   2B  | nkeys * 8B | nkeys * 2B | ...


Format of the KV pair -- lengths followed by data
| klen | vlen | key | val |
|  2B  |  2B  | ... | ... |

*/

type BNode struct {
	data []byte // can be dumped to disk
}

type BTree struct {
	// pointer (a non zero page number)
	// we can't use in-memory pointers. The pointers are 64-bit integers
	// referencing disk pages instead of in-memory nodes
	root uint64
	// callbacks for managing on-disk pages
	get func(uint64) BNode // dereference a pointer
	new func(BNode) uint64 // allocate a new page
	del func(uint64)       // deallocate a page
}

const (
	BNODE_NODE = 1 // internal nodes without values
	BNODE_LEAF = 2 // leaf nodes with values
)

const HEADER = 4                // the type of node and the number of keys
const BTREE_PAGE_SIZE = 4096    // page size is 4kb
const BTREE_MAX_KEY_SIZE = 1000 // the largest size a key can be
const BTREE_MAX_VAL_SIZE = 3000 // the largest size a value can be
// if bigger keys or values are needed, you have to allocate extra pages for them. More complex

func init() {

	// make sure that a node with a single KV pair always fits on a single page
	// HEADER = 4 = 2B + 2B = Type + nkeys
	// 8 --> pointers
	// 2 --> offsets
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	if !(node1max <= BTREE_PAGE_SIZE) {
		panic("node1max should be <= BTREE_PAGE_SIZE")
	}
}

/////////////////
// Decoding the B-Tree node
/////////////////

// helper functions to access the contents of a node, which is an array of bytes
// dealing with the header (btype and nkeys)
func (node BNode) btype() uint16 {
	// node.data is []byte which is an alias for uint8
	// range of uint8 is 0 to 255. This is an array of uint8 (bytes)
	// reads the stuff in the byte array as little endian and combines it to form Uint16
	// see example here: https://go.dev/play/p/3TCANxD0uRCZ
	// ex: [0xe8, 0x03] --> combine to form [0x03e8] (little endian, lower byte comes first)
	return binary.LittleEndian.Uint16(node.data)
}

func (node BNode) nkeys() uint16 {
	// refer to the wire format diagram
	// first two bytes are type --> node.data[0:2]
	// nkeys are the next two bytes --> node.data[2:4]
	return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	// Put encodes a uint16 in this case into a buf and returns the number of bytes written
	// if the buffer is too small, PutUint16 will panic
	// here we put btype into node.data[0:2]
	// and we put nkeys into node.data[2:4]
	// example: https://go.dev/play/p/yRUoUooVrHm
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// handling the pointers
func (node BNode) getPtr(idx uint16) uint64 {
	if !(idx < node.nkeys()) {
		panic("idx should be < node.nkeys()")
	}

	// idx is one of the keys, and each is 8B long --> idx*8
	// offset by the HEADER
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.data[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	if !(idx < node.nkeys()) {
		panic("idx should be < node.nkeys()")
	}
	pos := HEADER + 8*idx
	// put val into node.data[pos:]
	binary.LittleEndian.PutUint64(node.data[pos:], val)
}

// offset list
func offsetPos(node BNode, idx uint16) uint16 {
	if !(1 <= idx && idx <= node.nkeys()) {
		panic("error with idx")
	}
	// offset is relative to the position of the first KV pair
	// navigate past HEADER and the pointers
	// the next block are the offsets which are 2B each
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

func (node BNode) getOffset(idx uint16) uint16 {
	// the first KV pair offset is zero, so it is not stored in the list
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.data[offsetPos(node, idx):])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	// store the offset to the end of the last KV pair in the offset list
	binary.LittleEndian.PutUint16(node.data[offsetPos(node, idx):], offset)
}

// key-values
func (node BNode) kvPos(idx uint16) uint16 {
	if !(idx <= node.nkeys()) {
		panic("idx should be <= node.nkeys")
	}

	// HEADER + pointers + offsets + whatever offset we calculate to get to the KV pair
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	if !(idx < node.nkeys()) {
		panic("idx should be < node.nkeys")
	}

	pos := node.kvPos(idx) // start of the kv pair
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	// pos+4 to skip the "header" of the kv pair (the klen and vlen)
	// and then :klen to read up to the klen which is the key
	return node.data[pos+4:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	if !(idx < node.nkeys()) {
		panic("idx should be < node.nkeys")
	}
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos+0:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	// skip the "header" of the kv pair
	// then skip the key
	// start reading the value up to vlen
	return node.data[pos+4+klen:][:vlen]
}

// node size in bytes
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

// ////////////////////
// B-Tree Insertion
// ////////////////////

// Step 1: Look Up the Key

// in order to insert a key into a leaf node, we need to first look up the key's position in the sorted KV list
// returns the first child node whose range intersects the key (child[i] <= key)
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	found := uint16(0)

	// linear search to find the key. Iterate through nkeys until you find
	// the matching key
	// the first key is a copy from the parent node -- so we start from i := 1
	// thus it's always less than or equal to the key
	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}

// Step 2: Update Leaf nodes

// update leaf nodes
// after looking up the position to insert, we need to create a copy of the node with the new key in it
// add a new key to a leaf node
func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

// copy keys from an old node to a new node
func nodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
	if !(srcOld+n <= old.nkeys()) {
		panic("srcOld+n should be <= old.nkeys")
	}
	if !(dstNew+n <= new.nkeys()) {
		panic("dstNew+n should be <= new.nkeys")
	}
	if n == 0 {
		return
	}

	// copy the pointers from old to new node
	for i := uint16(0); i < n; i++ {
		new.setPtr(dstNew+i, old.getPtr(srcOld+i))
	}

	// offsets
	dstBegin := new.getOffset(dstNew)
	srcBegin := old.getOffset(srcOld)
	for i := uint16(1); i <= n; i++ {
		offset := dstBegin + old.getOffset(srcOld+i) - srcBegin
		new.setOffset(dstNew+i, offset)
	}

	// KVs
	begin := old.kvPos(srcOld)
	end := old.kvPos(srcOld + n)
	copy(new.data[new.kvPos(dstNew):], old.data[begin:end])
}

// copy a KV pair to the new node
func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// ptrs
	new.setPtr(idx, ptr)
	// KVs
	pos := new.kvPos(idx)

	// add the KV header
	binary.LittleEndian.PutUint16(new.data[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new.data[pos+2:], uint16(len(val)))
	// copy the data
	copy(new.data[pos+4:], key)
	copy(new.data[pos+4+uint16(len(key)):], val)
	// store the offset to the end of the last KV pair in the offset list
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16((len(key)+len(val))))
}

// Step 3: Recursive Insertion
