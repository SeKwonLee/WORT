#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <emmintrin.h>
#include <assert.h>
#include <x86intrin.h>
#include "woart.h"

#define mfence() asm volatile("mfence":::"memory")
#define BITOP_WORD(nr)	((nr) / BITS_PER_LONG)

/**
 * Macros to manipulate pointer tags
 */
#define IS_LEAF(x) (((uintptr_t)x & 1))
#define SET_LEAF(x) ((void*)((uintptr_t)x | 1))
#define LEAF_RAW(x) ((art_leaf*)((void*)((uintptr_t)x & ~1)))

#define LATENCY			0
#define CPU_FREQ_MHZ	2100

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

void flush_buffer(void *buf, unsigned long len, bool fence)
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

/*
 * Find the next set bit in a memory region.
 */
unsigned long find_next_bit(const unsigned long *addr, unsigned long size,
			    unsigned long offset)
{
	const unsigned long *p = addr + BITOP_WORD(offset);
	unsigned long result = offset & ~(BITS_PER_LONG-1);
	unsigned long tmp;

	if (offset >= size)
		return size;
	size -= result;
	offset %= BITS_PER_LONG;
	if (offset) {
		tmp = *(p++);
		tmp &= (~0UL << offset);
		if (size < BITS_PER_LONG)
			goto found_first;
		if (tmp)
			goto found_middle;
		size -= BITS_PER_LONG;
		result += BITS_PER_LONG;
	}
	while (size & ~(BITS_PER_LONG-1)) {
		if ((tmp = *(p++)))
			goto found_middle;
		result += BITS_PER_LONG;
		size -= BITS_PER_LONG;
	}
	if (!size)
		return result;
	tmp = *p;

found_first:
	tmp &= (~0UL >> (BITS_PER_LONG - size));
	if (tmp == 0UL)		/* Are any bits set? */
		return result + size;	/* Nope. */
found_middle:
	return result + __ffs(tmp);
}

/*
 * Find the next zero bit in a memory region
 */
unsigned long find_next_zero_bit(const unsigned long *addr, unsigned long size,
		unsigned long offset)
{
	const unsigned long *p = addr + BITOP_WORD(offset);
	unsigned long result = offset & ~(BITS_PER_LONG - 1);
	unsigned long tmp;

	if (offset >= size)
		return size;
	size -= result;
	offset %= BITS_PER_LONG;
	if (offset) {
		tmp = *(p++);
		tmp |= ~0UL >> (BITS_PER_LONG - offset);
		if (size < BITS_PER_LONG)
			goto found_first;
		if (~tmp)
			goto found_middle;
		size -= BITS_PER_LONG;
		result += BITS_PER_LONG;
	}
	while (size & ~(BITS_PER_LONG - 1)) {
		if (~(tmp = *(p++)))
			goto found_middle;
		result += BITS_PER_LONG;
		size -= BITS_PER_LONG;
	}
	if (!size)
		return result;
	tmp = *p;

found_first:
	tmp |= ~0UL << size;
	if (tmp == ~0UL)
		return result + size;
found_middle:
	return result + ffz(tmp);
}

/**
 * Allocates a node of the given type,
 * initializes to zero and sets the type.
 */
static art_node* alloc_node(uint8_t type) {
	art_node* n;
    void *ret;
	int i;
	switch (type) {
		case NODE4:
			posix_memalign(&ret, 64, sizeof(art_node4));
            n = ret;
			for (i = 0; i < 4; i++)
				((art_node4 *)n)->slot[i].i_ptr = -1;
			break;
		case NODE16:
			posix_memalign(&ret, 64, sizeof(art_node16));
            n = ret;
			((art_node16 *)n)->bitmap = 0;
			break;
		case NODE48:
			posix_memalign(&ret, 64, sizeof(art_node48));
            n = ret;
			memset(n, 0, sizeof(art_node48));
			break;
		case NODE256:
			posix_memalign(&ret, 64, sizeof(art_node256));
            n = ret;
			memset(n, 0, sizeof(art_node256));
			break;
		default:
			abort();
	}
	n->type = type;
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
	int i;
	union {
		art_node4 *p1;
		art_node16 *p2;
		art_node48 *p3;
		art_node256 *p4;
	} p;
	switch (n->type) {
		case NODE4:
			p.p1 = (art_node4 *)n;
			for (i = 0; (i < 4 && (p.p1->slot[i].i_ptr != -1)); i++) {
				if (p.p1->slot[i].key == c)
					return &p.p1->children[p.p1->slot[i].i_ptr];
			}
			break;
		case NODE16:
			p.p2 = (art_node16 *)n;
			for (i = 0; i < 16; i++) {
				i = find_next_bit(&p.p2->bitmap, 16, i);
				if (i < 16 && p.p2->keys[i] == c)
					return &p.p2->children[i];
			}
			break;
		case NODE48:
			p.p3 = (art_node48 *)n;
			i = p.p3->keys[c];
			if (i)
				return &p.p3->children[i - 1];
			break;
		case NODE256:
			p.p4 = (art_node256 *)n;
			if (p.p4->children[c])
				return &p.p4->children[c];
			break;
		default:
			abort();
	}
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
	int max_cmp = min(min(n->path.partial_len, MAX_PREFIX_LEN), MAX_HEIGHT - depth);
	int idx;
	for (idx=0; idx < max_cmp; idx++) {
		if (n->path.partial[idx] != get_index(key, depth + idx))
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

		if (n->path.depth == depth) {
			// Bail if the prefix does not match
			if (n->path.partial_len) {
				prefix_len = check_prefix(n, key, key_len, depth);
				if (prefix_len != min(MAX_PREFIX_LEN, n->path.partial_len))
					return NULL;
				depth = depth + n->path.partial_len;
			}
		} else {
			printf("Search: Crash occured\n");
			exit(0);
		}

		// Recursively search
		child = find_child(n, get_index(key, depth));
		n = (child) ? *child : NULL;
		depth++;
	}
	return NULL;
}

// Find the minimum leaf under a node
static art_leaf* minimum(const art_node *n) {
	// Handle base cases
	if (!n) return NULL;
	if (IS_LEAF(n)) return LEAF_RAW(n);

	int i, j, idx, min;
	switch (n->type) {
		case NODE4:
			return minimum(((art_node4 *)n)->children[((art_node4 *)n)->slot[0].i_ptr]);
		case NODE16:
			i = find_next_bit(&((art_node16 *)n)->bitmap, 16, 0);
			min = ((art_node16 *)n)->keys[i];
			idx = i;
			for (i = i + 1; i < 16; i++) {
				i = find_next_bit(&((art_node16 *)n)->bitmap, 16, i);
				if(((art_node16 *)n)->keys[i] < min && i < 16) {
					min = ((art_node16 *)n)->keys[i];
					idx = i;
				}
			}
			return minimum(((art_node16 *)n)->children[idx]);
		case NODE48:
			idx = 0;
			while (!((art_node48*)n)->keys[idx]) idx++;
			idx = ((art_node48*)n)->keys[idx] - 1;
			return minimum(((art_node48 *)n)->children[idx]);
		case NODE256:
			idx = 0;
			while (!((art_node256 *)n)->children[idx]) idx++;
			return minimum(((art_node256 *)n)->children[idx]);
		default:
			abort();
	}
}

static art_leaf* make_leaf(const unsigned long key, int key_len, void *value, bool flush) {
	//art_leaf *l = (art_leaf*)malloc(sizeof(art_leaf));
	art_leaf *l;
    void *ret;
	posix_memalign(&ret, 64, sizeof(art_leaf));
	l->value = value;
	l->key_len = key_len;
	l->key = key;

    if (flush == true)
        flush_buffer(l, sizeof(art_leaf), true);
	return l;
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

static void copy_header(art_node *dest, art_node *src) {
	memcpy(&dest->path, &src->path, sizeof(path_comp));
}

static void add_child256(art_node256 *n, art_node **ref, unsigned char c, void *child) {
	(void)ref;
	n->children[c] = (art_node *)child;
	flush_buffer(&n->children[c], 8, true);
}

static void add_child256_noflush(art_node256 *n, art_node **ref, unsigned char c, void *child) {
	(void)ref;
	n->children[c] = (art_node *)child;
}

static void add_child48(art_node48 *n, art_node **ref, unsigned char c, void *child) {
	unsigned long bitmap = 0;
	int i, num = 0;

	for (i = 0; i < 256; i++) {
		if (n->keys[i]) {
			bitmap += (0x1UL << (n->keys[i] - 1));
			num++;
			if (num == 48)
				break;
		}
	}

	if (num < 48) {
		unsigned long pos = find_next_zero_bit(&bitmap, 48, 0);
		n->children[pos] = (art_node *)child;
		flush_buffer(&n->children[pos], 8, true);
		n->keys[c] = pos + 1;
		flush_buffer(&n->keys[c], sizeof(unsigned char), true);
	} else {
		art_node256 *new_node = (art_node256 *)alloc_node(NODE256);
		for (i = 0; i < 256; i++) {
			if (n->keys[i]) {
				new_node->children[i] = n->children[n->keys[i] - 1];
				num--;
				if (num == 0)
					break;
			}
		}		
		copy_header((art_node *)new_node, (art_node *)n);
		add_child256_noflush(new_node, ref, c, child);
		flush_buffer(new_node, sizeof(art_node256), true);

		*ref = (art_node *)new_node;
		flush_buffer(ref, 8, true);

		free(n);
	}
}

static void add_child16(art_node16 *n, art_node **ref, unsigned char c, void *child) {
	if (n->bitmap != ((0x1UL << 16) - 1)) {
		int empty_idx;

		empty_idx = find_next_zero_bit(&n->bitmap, 16, 0);
		if (empty_idx == 16) {
			printf("find next zero bit error add_child16\n");
			abort();
		}

		n->keys[empty_idx] = c;
		n->children[empty_idx] = child;
        mfence();
		flush_buffer(&n->keys[empty_idx], sizeof(unsigned char), false);
		flush_buffer(&n->children[empty_idx], sizeof(uintptr_t), false);
        mfence();

		n->bitmap += (0x1UL << empty_idx);
		flush_buffer(&n->bitmap, sizeof(unsigned long), true);
	} else {
		int idx;
		art_node48 *new_node = (art_node48 *)alloc_node(NODE48);

		memcpy(new_node->children, n->children,
				sizeof(void *) * 16);
		for (idx = 0; idx < 16; idx++) {
			new_node->keys[n->keys[idx]] = idx + 1;
		}
		copy_header((art_node *)new_node, (art_node *)n);

		new_node->keys[c] = 17;
		new_node->children[16] = child;
		flush_buffer(new_node, sizeof(art_node48), true);

		*ref = (art_node *)new_node;
		flush_buffer(ref, sizeof(uintptr_t), true);

		free(n);
	}
}

static void add_child4(art_node4 *n, art_node **ref, unsigned char c, void *child) {
	if (n->slot[3].i_ptr == -1) {
		slot_array temp_slot[4];
		int i, idx, mid = -1;
		unsigned long p_idx = 0;

		for (idx = 0; (idx < 4 && (n->slot[idx].i_ptr != -1)); idx++) {
			p_idx = p_idx + (0x1UL << n->slot[idx].i_ptr);
			if (mid == -1 && c < n->slot[idx].key)
				mid = idx;
		}

		if (mid == -1)
			mid = idx;

		p_idx = find_next_zero_bit(&p_idx, 4, 0);
		if (p_idx == 4) {
			printf("find next zero bit error in child4\n");
			abort();
		}
		n->children[p_idx] = child;
		flush_buffer(&n->children[p_idx], sizeof(uintptr_t), true);

		for (i = idx - 1; i >= mid; i--) {
			temp_slot[i + 1].key = n->slot[i].key;
			temp_slot[i + 1].i_ptr = n->slot[i].i_ptr;
		}

		if (idx < 3) {
			for (i = idx + 1; i < 4; i++)
				temp_slot[i].i_ptr = -1;
		}

		temp_slot[mid].key = c;
		temp_slot[mid].i_ptr = p_idx;

		for (i = mid - 1; i >=0; i--) {
			temp_slot[i].key = n->slot[i].key;
			temp_slot[i].i_ptr = n->slot[i].i_ptr;
		}

		*((uint64_t *)n->slot) = *((uint64_t *)temp_slot);
		flush_buffer(n->slot, sizeof(uintptr_t), true);
	} else {
		int idx;
		art_node16 *new_node = (art_node16 *)alloc_node(NODE16);

		for (idx = 0; idx < 4; idx++) {
			new_node->keys[n->slot[idx].i_ptr] = n->slot[idx].key;
			new_node->children[n->slot[idx].i_ptr] = n->children[n->slot[idx].i_ptr];
			new_node->bitmap += (0x1UL << n->slot[idx].i_ptr);
		}
		copy_header((art_node *)new_node, (art_node *)n);

		new_node->keys[4] = c;
		new_node->children[4] = child;
		new_node->bitmap += (0x1UL << 4);
		flush_buffer(new_node, sizeof(art_node16), true);

		*ref = (art_node *)new_node;
		flush_buffer(ref, 8, true);

		free(n);
	}
}

static void add_child4_noflush(art_node4 *n, art_node **ref, unsigned char c, void *child) {
	slot_array temp_slot[4];
	int i, idx, mid = -1;
	unsigned long p_idx = 0;

	for (idx = 0; (idx < 4 && (n->slot[idx].i_ptr != -1)); idx++) {
		p_idx = p_idx + (0x1UL << n->slot[idx].i_ptr);
		if (mid == -1 && c < n->slot[idx].key)
			mid = idx;
	}

	if (mid == -1)
		mid = idx;

	p_idx = find_next_zero_bit(&p_idx, 4, 0);
	if (p_idx == 4) {
		printf("find next zero bit error in child4\n");
		abort();
	}

	n->children[p_idx] = child;

	for (i = idx - 1; i >= mid; i--) {
		temp_slot[i + 1].key = n->slot[i].key;
		temp_slot[i + 1].i_ptr = n->slot[i].i_ptr;
	}

	if (idx < 3) {
		for (i = idx + 1; i < 4; i++)
			temp_slot[i].i_ptr = -1;
	}

	temp_slot[mid].key = c;
	temp_slot[mid].i_ptr = p_idx;

	for (i = mid - 1; i >=0; i--) {
		temp_slot[i].key = n->slot[i].key;
		temp_slot[i].i_ptr = n->slot[i].i_ptr;
	}

	*((uint64_t *)n->slot) = *((uint64_t *)temp_slot);
}

static void add_child(art_node *n, art_node **ref, unsigned char c, void *child) {
	switch (n->type) {
		case NODE4:
			return add_child4((art_node4 *)n, ref, c, child);
		case NODE16:
			return add_child16((art_node16 *)n, ref, c, child);
		case NODE48:
			return add_child48((art_node48 *)n, ref, c, child);
		case NODE256:
			return add_child256((art_node256 *)n, ref, c, child);
		default:
			abort();
	}
}

/**
 * Calculates the index at which the prefixes mismatch
 */
static int prefix_mismatch(const art_node *n, const unsigned long key, int key_len, int depth, art_leaf **l) {
//	int max_cmp = min(min(MAX_PREFIX_LEN, n->partial_len), (key_len * INDEX_BITS) - depth);
	int max_cmp = min(min(MAX_PREFIX_LEN, n->path.partial_len), MAX_HEIGHT - depth);
	int idx;
	for (idx=0; idx < max_cmp; idx++) {
		if (n->path.partial[idx] != get_index(key, depth + idx))
			return idx;
	}

	// If the prefix is short we can avoid finding a leaf
	if (n->path.partial_len > MAX_PREFIX_LEN) {
		// Prefix is longer than what we've checked, find a leaf
		*l = minimum(n);
//		max_cmp = (min((*l)->key_len, key_len) * INDEX_BITS) - depth;
		max_cmp = MAX_HEIGHT - depth;
		for (; idx < max_cmp; idx++) {
			if (get_index((*l)->key, idx + depth) != get_index(key, depth + idx))
				return idx;
		}
	}
	return idx;
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
		art_node4 *new_node = (art_node4 *)alloc_node(NODE4);
		new_node->n.path.depth = depth;

		// Create a new leaf
		art_leaf *l2 = make_leaf(key, key_len, value, false);

		// Determine longest prefix
		int i, longest_prefix = longest_common_prefix(l, l2, depth);
		new_node->n.path.partial_len = longest_prefix;
		for (i = 0; i < min(MAX_PREFIX_LEN, longest_prefix); i++)
			new_node->n.path.partial[i] = get_index(key, depth + i);

		add_child4_noflush(new_node, ref, get_index(l->key, depth + longest_prefix), SET_LEAF(l));
		add_child4_noflush(new_node, ref, get_index(l2->key, depth + longest_prefix), SET_LEAF(l2));

        mfence();
		flush_buffer(new_node, sizeof(art_node4), false);
		flush_buffer(l2, sizeof(art_leaf), false);
        mfence();

		// Add the leafs to the new node4
		*ref = (art_node*)new_node;
		flush_buffer(ref, sizeof(uintptr_t), true);
		return NULL;
	}

	if (n->path.depth != depth) {
		printf("Insert: system is previously crashed!!\n");
		exit(0);
	}

	// Check if given node has a prefix
	if (n->path.partial_len) {
		// Determine if the prefixes differ, since we need to split
		art_leaf *l = NULL;
		int prefix_diff = prefix_mismatch(n, key, key_len, depth, &l);
		if ((uint32_t)prefix_diff >= n->path.partial_len) {
			depth += n->path.partial_len;
			goto RECURSE_SEARCH;
		}

		// Create a new node
		art_node4 *new_node = (art_node4*)alloc_node(NODE4);
		new_node->n.path.depth = depth;
		new_node->n.path.partial_len = prefix_diff;
		memcpy(new_node->n.path.partial, n->path.partial, min(MAX_PREFIX_LEN, prefix_diff));

		// Adjust the prefix of the old node
        path_comp temp_path;
        if (n->path.partial_len <= MAX_PREFIX_LEN) {
			add_child4_noflush(new_node, ref, n->path.partial[prefix_diff], n);
			temp_path.partial_len = n->path.partial_len - (prefix_diff + 1);
			temp_path.depth = (depth + prefix_diff + 1);
			memmove(temp_path.partial, n->path.partial + prefix_diff + 1,
					min(MAX_PREFIX_LEN, temp_path.partial_len));
		} else {
			int i;
			if (l == NULL)
				l = minimum(n);
			add_child4_noflush(new_node, ref, get_index(l->key, depth + prefix_diff), n);
			temp_path.partial_len = n->path.partial_len - (prefix_diff + 1);
			for (i = 0; i < min(MAX_PREFIX_LEN, temp_path.partial_len); i++)
				temp_path.partial[i] = get_index(l->key, depth + prefix_diff + 1 + i);
			temp_path.depth = (depth + prefix_diff + 1);
		}

		// Insert the new leaf
		l = make_leaf(key, key_len, value, false);
		add_child4_noflush(new_node, ref, get_index(key, depth + prefix_diff), SET_LEAF(l));

        mfence();
		flush_buffer(new_node, sizeof(art_node4), false);
		flush_buffer(l, sizeof(art_leaf), false);
        mfence();

		*ref = (art_node*)new_node;
        *((uint64_t *)&n->path) = *((uint64_t *)&temp_path);

        mfence();
		flush_buffer(&n->path, sizeof(path_comp), false);
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

	add_child(n, ref, get_index(key, depth), SET_LEAF(l));

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
