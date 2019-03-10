#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <emmintrin.h>
#include <assert.h>
#include <x86intrin.h>
#include <math.h>
#include "wort.h"

/**
 * Macros to manipulate pointer tags
 */
#define IS_LEAF(x) (((uintptr_t)x & 1))
#define SET_LEAF(x) ((void*)((uintptr_t)x | 1))
#define LEAF_RAW(x) ((art_leaf*)((void*)((uintptr_t)x & ~1)))

#define LATENCY			0
#define CPU_FREQ_MHZ	2100
#define CACHE_LINE_SIZE 64

static inline void cpu_pause()
{
	__asm__ volatile ("pause" ::: "memory");
}

static inline unsigned long read_tsc(void)
{
	unsigned long var;
	unsigned int hi, lo;

	asm volatile ("rdtsc" : "=a" (lo), "=d" (hi));
	var = ((unsigned long long int) hi << 32) | lo;

	return var;
}

static inline void mfence() {
    asm volatile("mfence" ::: "memory");
}

static void flush_buffer(void *buf, unsigned long len, bool fence)
{
	unsigned long i, etsc;
	len = len + ((unsigned long)(buf) & (CACHE_LINE_SIZE - 1));
	if (fence) {
		mfence();
		for (i = 0; i < len; i += CACHE_LINE_SIZE) {
			etsc = read_tsc() + (unsigned long)(LATENCY * CPU_FREQ_MHZ / 1000);
			asm volatile ("clflush %0\n" : "+m" (*(char *)(buf+i)));
			while (read_tsc() < etsc)
				cpu_pause();
		}
		mfence();
	} else {
		for (i = 0; i < len; i += CACHE_LINE_SIZE) {
			etsc = read_tsc() + (unsigned long)(LATENCY * CPU_FREQ_MHZ / 1000);
			asm volatile ("clflush %0\n" : "+m" (*(char *)(buf+i)));
			while (read_tsc() < etsc)
				cpu_pause();
		}
	}
}

static int get_index(unsigned long key, int depth)
{
	int index;

	index = ((key >> ((MAX_DEPTH - depth) * NODE_BITS)) & LOW_BIT_MASK);
	return index;
}

/**
 * Allocates a node of the given type,
 * initializes to zero and sets the type.
 */
static art_node* alloc_node() {
	art_node* n;
    void *ret;
	posix_memalign(&ret, 64, sizeof(art_node16));
    n = ret;
	memset(n, 0, sizeof(art_node16));
	return n;
}

/**
 * Initializes an ART tree
 * @return 0 on success.
 */
int art_tree_init(art_tree *t) {
	t->root = NULL;
	t->size = 0;
	return 0;
}

static art_node** find_child(art_node *n, unsigned char c) {
	art_node16 *p;

	p = (art_node16 *)n;
	if (p->children[c])
		return &p->children[c];

	return NULL;
}

// Simple inlined if
static inline int min(int a, int b) {
	return (a < b) ? a : b;
}

/**
 * Returns the number of prefix characters shared between
 * the key and node.
 */
static int check_prefix(const art_node *n, const unsigned long key, int key_len, int depth) {
//	int max_cmp = min(min(n->partial_len, MAX_PREFIX_LEN), (key_len * INDEX_BITS) - depth);
	int max_cmp = min(min(n->partial_len, MAX_PREFIX_LEN), MAX_HEIGHT - depth);
	int idx;
	for (idx=0; idx < max_cmp; idx++) {
		if (n->partial[idx] != get_index(key, depth + idx))
			return idx;
	}
	return idx;
}

/**
 * Checks if a leaf matches
 * @return 0 on success.
 */
static int leaf_matches(const art_leaf *n, unsigned long key, int key_len, int depth) {
	(void)depth;
	// Fail if the key lengths are different
	if (n->key_len != (uint32_t)key_len) return 1;

	// Compare the keys starting at the depth
//	return memcmp(n->key, key, key_len);
	return !(n->key == key);
}

// Find the minimum leaf under a node
static art_leaf* minimum(const art_node *n) {
	// Handle base cases
	if (!n) return NULL;
	if (IS_LEAF(n)) return LEAF_RAW(n);

	int idx = 0;

	while (!((art_node16 *)n)->children[idx]) idx++;
	return minimum(((art_node16 *)n)->children[idx]);
}

static int longest_common_prefix(art_leaf *l1, art_leaf *l2, int depth) {
//	int idx, max_cmp = (min(l1->key_len, l2->key_len) * INDEX_BITS) - depth;
	int idx, max_cmp = MAX_HEIGHT - depth;

	for (idx=0; idx < max_cmp; idx++) {
		if (get_index(l1->key, depth + idx) != get_index(l2->key, depth + idx))
			return idx;
	}
	return idx;
}

/**
 * Searches for a value in the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* art_search(const art_tree *t, const unsigned long key, int key_len) {
	art_node **child;
	art_node *n = t->root;
	int prefix_len, depth = 0;

	while (n) {
		// Might be a leaf
		if (IS_LEAF(n)) {
			n = (art_node*)LEAF_RAW(n);
			// Check if the expanded path matches
			if (!leaf_matches((art_leaf*)n, key, key_len, depth)) {
				return ((art_leaf*)n)->value;
			}
			return NULL;
		}

		if (n->depth == depth) {
			// Bail if the prefix does not match
			if (n->partial_len) {
				prefix_len = check_prefix(n, key, key_len, depth);
				if (prefix_len != min(MAX_PREFIX_LEN, n->partial_len))
					return NULL;
				depth = depth + n->partial_len;
			}
		} else {
			art_leaf *leaf[2];
			int cnt, pos, i;

			for (pos = 0, cnt = 0; pos < 16; pos++) {
				if (((art_node16*)n)->children[pos]) {
					leaf[cnt] = minimum(((art_node16*)n)->children[pos]);
					cnt++;
					if (cnt == 2)
						break;
				}
			}

			int prefix_diff = longest_common_prefix(leaf[0], leaf[1], depth);			  
			art_node old_path;
			old_path.partial_len = prefix_diff;
			for (i = 0; i < min(MAX_PREFIX_LEN, prefix_diff); i++)
				old_path.partial[i] = get_index(leaf[1]->key, depth + i);

			prefix_len = check_prefix(&old_path, key, key_len, depth);
			if (prefix_len != min(MAX_PREFIX_LEN, old_path.partial_len))
				return NULL;
			depth = depth + old_path.partial_len;
		}

		// Recursively search
		child = find_child(n, get_index(key, depth));
		n = (child) ? *child : NULL;
		depth++;
	}
	return NULL;
}

static art_leaf* make_leaf(const unsigned long key, int key_len, void *value, bool flush) {
	//art_leaf *l = (art_leaf*)malloc(sizeof(art_leaf));
	art_leaf *l;
    void *ret;
	posix_memalign(&ret, 64, sizeof(art_leaf));
    l = ret;
	l->value = value;
	l->key_len = key_len;
	l->key = key;

    if (flush == true)
        flush_buffer(l, sizeof(art_leaf), true);
	return l;
}

static void add_child(art_node16 *n, art_node **ref, unsigned char c, void *child) {
	(void)ref;
	n->children[c] = (art_node*)child;
}

/**
 * Calculates the index at which the prefixes mismatch
 */
static int prefix_mismatch(const art_node *n, const unsigned long key, int key_len, int depth, art_leaf **l) {
	int max_cmp = min(min(MAX_PREFIX_LEN, n->partial_len), MAX_HEIGHT - depth);
	int idx;
	for (idx=0; idx < max_cmp; idx++) {
		if (n->partial[idx] != get_index(key, depth + idx))
			return idx;
	}

	// If the prefix is short we can avoid finding a leaf
	if (n->partial_len > MAX_PREFIX_LEN) {
		// Prefix is longer than what we've checked, find a leaf
		*l = minimum(n);
		max_cmp = MAX_HEIGHT - depth;
		for (; idx < max_cmp; idx++) {
			if (get_index((*l)->key, idx + depth) != get_index(key, depth + idx))
				return idx;
		}
	}
	return idx;
}

void recovery_prefix(art_node *n, int depth) {
	art_leaf *leaf[2];
	int cnt, pos, i, j;

	for (pos = 0, cnt = 0; pos < 16; pos++) {
		if (((art_node16*)n)->children[pos]) {
			leaf[cnt] = minimum(((art_node16*)n)->children[pos]);
			cnt++;
			if (cnt == 2)
				break;
		}
	}

	int prefix_diff = longest_common_prefix(leaf[0], leaf[1], depth);
	art_node old_path;
	old_path.partial_len = prefix_diff;
	for (i = 0; i < min(MAX_PREFIX_LEN, prefix_diff); i++)
		old_path.partial[i] = get_index(leaf[1]->key, depth + i);
	old_path.depth = depth;
	*((uint64_t *)n) = *((uint64_t *)&old_path);
	flush_buffer(n, sizeof(art_node), true);
}

static void* recursive_insert(art_node *n, art_node **ref, const unsigned long key,
		int key_len, void *value, int depth, int *old)
{
	// If we are at a NULL node, inject a leaf
	if (!n) {
		*ref = (art_node*)SET_LEAF(make_leaf(key, key_len, value, true));
		flush_buffer(ref, sizeof(uintptr_t), true);
		return NULL;
	}

	// If we are at a leaf, we need to replace it with a node
	if (IS_LEAF(n)) {
		art_leaf *l = LEAF_RAW(n);

		// Check if we are updating an existing value
		if (!leaf_matches(l, key, key_len, depth)) {
			*old = 1;
			void *old_val = l->value;
			l->value = value;
			flush_buffer(&l->value, sizeof(uintptr_t), true);
			return old_val;
		}

		// New value, we must split the leaf into a node4
		art_node16 *new_node = (art_node16 *)alloc_node();
		new_node->n.depth = depth;

		// Create a new leaf
		art_leaf *l2 = make_leaf(key, key_len, value, false);

		// Determine longest prefix
		int i, longest_prefix = longest_common_prefix(l, l2, depth);
		new_node->n.partial_len = longest_prefix;
		for (i = 0; i < min(MAX_PREFIX_LEN, longest_prefix); i++)
			new_node->n.partial[i] = get_index(key, depth + i);

		// Add the leafs to the new node4
		add_child(new_node, ref, get_index(l->key, depth + longest_prefix), SET_LEAF(l));
		add_child(new_node, ref, get_index(l2->key, depth + longest_prefix), SET_LEAF(l2));

        mfence();
		flush_buffer(new_node, sizeof(art_node16), false);
		flush_buffer(l2, sizeof(art_leaf), false);
        mfence();

		*ref = (art_node*)new_node;
		flush_buffer(ref, 8, true);
		return NULL;
	}

	if (n->depth != depth) {
		recovery_prefix(n, depth);
	}

	// Check if given node has a prefix
	if (n->partial_len) {
		// Determine if the prefixes differ, since we need to split
		art_leaf *l = NULL;
		int prefix_diff = prefix_mismatch(n, key, key_len, depth, &l);
		if ((uint32_t)prefix_diff >= n->partial_len) {
			depth += n->partial_len;
			goto RECURSE_SEARCH;
		}

		// Create a new node
		art_node16 *new_node = (art_node16 *)alloc_node();
		new_node->n.depth = depth;
		new_node->n.partial_len = prefix_diff;
		memcpy(new_node->n.partial, n->partial, min(MAX_PREFIX_LEN, prefix_diff));

		// Adjust the prefix of the old node
        art_node temp_path;
        if (n->partial_len <= MAX_PREFIX_LEN) {
			add_child(new_node, ref, n->partial[prefix_diff], n);
			temp_path.partial_len = n->partial_len - (prefix_diff + 1);
			temp_path.depth = (depth + prefix_diff + 1);
			memcpy(temp_path.partial, n->partial + prefix_diff + 1,
					min(MAX_PREFIX_LEN, temp_path.partial_len));
		} else {
			int i;
			if (l == NULL)
				l = minimum(n);
			add_child(new_node, ref, get_index(l->key, depth + prefix_diff), n);
			temp_path.partial_len = n->partial_len - (prefix_diff + 1);
			for (i = 0; i < min(MAX_PREFIX_LEN, temp_path.partial_len); i++)
				temp_path.partial[i] = get_index(l->key, depth + prefix_diff + 1 +i);
			temp_path.depth = (depth + prefix_diff + 1);
		}

		// Insert the new leaf
		l = make_leaf(key, key_len, value, false);
		add_child(new_node, ref, get_index(key, depth + prefix_diff), SET_LEAF(l));

        mfence();
		flush_buffer(new_node, sizeof(art_node16), false);
		flush_buffer(l, sizeof(art_leaf), false);
        mfence();

        *ref = (art_node*)new_node;
        *((uint64_t *)n) = *((uint64_t *)&temp_path);

        mfence();
		flush_buffer(n, sizeof(art_node), false);
		flush_buffer(ref, sizeof(uintptr_t), false);
        mfence();

		return NULL;
	}

RECURSE_SEARCH:;

	// Find a child to recurse to
	art_node **child = find_child(n, get_index(key, depth));
	if (child) {
		return recursive_insert(*child, child, key, key_len, value, depth + 1, old);
	}

	// No child, node goes within us
	art_leaf *l = make_leaf(key, key_len, value, true);

	add_child((art_node16 *)n, ref, get_index(key, depth), SET_LEAF(l));
	flush_buffer(&((art_node16 *)n)->children[get_index(key, depth)], sizeof(uintptr_t), true);
	return NULL;
}

/**
 * Inserts a new value into the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @arg value Opaque value.
 * @return NULL if the item was newly inserted, otherwise
 * the old value pointer is returned.
 */
void* art_insert(art_tree *t, const unsigned long key, int key_len, void *value) {
	int old_val = 0;
	void *old = recursive_insert(t->root, &t->root, key, key_len, value, 0, &old_val);
	if (!old_val) t->size++;
	return old;
}
