#include <stdint.h>
#include <stdbool.h>
#include <byteswap.h>
#ifndef WOART_H
#define WOART_H

#ifdef __cplusplus
extern "C" {
#endif

#define NODE4		1
#define NODE16		2
#define NODE48		3
#define NODE256		4

#define BITS_PER_LONG		64
#define CACHE_LINE_SIZE 	64

/* If you want to change the number of entries, 
 * change the values of NODE_BITS & MAX_DEPTH */
#define NODE_BITS			8
#define MAX_DEPTH			7
#define NUM_NODE_ENTRIES 	(0x1UL << NODE_BITS)
#define LOW_BIT_MASK		((0x1UL << NODE_BITS) - 1)

#define MAX_PREFIX_LEN		6
#define MAX_HEIGHT			(MAX_DEPTH + 1)

#if defined(__GNUC__) && !defined(__clang__)
# if __STDC_VERSION__ >= 199901L && 402 == (__GNUC__ * 100 + __GNUC_MINOR__)
/*
 * GCC 4.2.2's C99 inline keyword support is pretty broken; avoid. Introduced in
 * GCC 4.2.something, fixed in 4.3.0. So checking for specific major.minor of
 * 4.2 is fine.
 */
#  define BROKEN_GCC_C99_INLINE
# endif
#endif

static inline unsigned long __ffs(unsigned long word)
{
	asm("rep; bsf %1,%0"
		: "=r" (word)
		: "rm" (word));
	return word;
}

static inline unsigned long ffz(unsigned long word)
{
	asm("rep; bsf %1,%0"
		: "=r" (word)
		: "r" (~word));
	return word;
}

typedef int(*art_callback)(void *data, const unsigned char *key, uint32_t key_len, void *value);

/**
 * path compression
 * partial_len: Optimistic
 * partial: Pessimistic
 */
typedef struct {
	unsigned char depth;
	unsigned char partial_len;
	unsigned char partial[MAX_PREFIX_LEN];
} path_comp;

/**
 * This struct is included as part
 * of all the various node sizes
 */
typedef struct {
    uint8_t type;
	path_comp path;
} art_node;

typedef struct {
	unsigned char key;
	char i_ptr;
} slot_array;

/**
 * Small node with only 4 children, but
 * 8byte slot array field.
 */
typedef struct {
    art_node n;
	slot_array slot[4];
    art_node *children[4];
} art_node4;

/**
 * Node with 16 keys and 16 children, and
 * a 8byte bitmap field
 */
typedef struct {
    art_node n;
	unsigned long bitmap;
    unsigned char keys[16];
    art_node *children[16];
} art_node16;

/**
 * Node with 48 children and a full 256 byte field,
 */
typedef struct {
    art_node n;
    unsigned char keys[256];
    art_node *children[48];
} art_node48;

/**
 * Full node with 256 children
 */
typedef struct {
    art_node n;
    art_node *children[256];
} art_node256;

/**
 * Represents a leaf. These are
 * of arbitrary size, as they include the key.
 */
typedef struct {
    void *value;
    uint32_t key_len;	
	unsigned long key;
} art_leaf;

/**
 * Main struct, points to root.
 */
typedef struct {
    art_node *root;
    uint64_t size;
} art_tree;

/*
 * For range lookup in NODE16
 */
typedef struct {
	unsigned char key;
	art_node *child;
} key_pos;

/**
 * Initializes an ART tree
 * @return 0 on success.
 */
int art_tree_init(art_tree *t);

/**
 * DEPRECATED
 * Initializes an ART tree
 * @return 0 on success.
 */
#define init_art_tree(...) art_tree_init(__VA_ARGS__)

/**
 * Inserts a new value into the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @arg value Opaque value.
 * @return NULL if the item was newly inserted, otherwise
 * the old value pointer is returned.
 */
void* art_insert(art_tree *t, const unsigned long key, int key_len, void *value);

/**
 * Searches for a value in the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* art_search(const art_tree *t, const unsigned long key, int key_len);

#ifdef __cplusplus
}
#endif
#endif
