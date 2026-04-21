// tree.c — Tree object serialization and construction
//
// PROVIDED functions: get_file_mode, tree_parse, tree_serialize
// TODO functions:     tree_from_index
//
// Binary tree format (per entry, concatenated with no separators):
//   "<mode-as-ascii-octal> <name>\0<32-byte-binary-hash>"
//
// Example single entry (conceptual):
//   "100644 hello.txt\0" followed by 32 raw bytes of SHA-256
#include "index.h"
#include "tree.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
// Forward declarations
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);
int index_load(Index *index);

// ─── Mode Constants ─────────────────────────────────────────────────────────

#define MODE_FILE      0100644
#define MODE_EXEC      0100755
#define MODE_DIR       0040000

// ─── PROVIDED ───────────────────────────────────────────────────────────────

// Determine the object mode for a filesystem path.
uint32_t get_file_mode(const char *path) {
    struct stat st;
    if (lstat(path, &st) != 0) return 0;

    if (S_ISDIR(st.st_mode))  return MODE_DIR;
    if (st.st_mode & S_IXUSR) return MODE_EXEC;
    return MODE_FILE;
}

// Parse binary tree data into a Tree struct safely.
// Returns 0 on success, -1 on parse error.
int tree_parse(const void *data, size_t len, Tree *tree_out) {
    tree_out->count = 0;
    const uint8_t *ptr = (const uint8_t *)data;
    const uint8_t *end = ptr + len;

    while (ptr < end && tree_out->count < MAX_TREE_ENTRIES) {
        TreeEntry *entry = &tree_out->entries[tree_out->count];

        // 1. Safely find the space character for the mode
        const uint8_t *space = memchr(ptr, ' ', end - ptr);
        if (!space) return -1;

        char mode_str[16] = {0};
        size_t mode_len = space - ptr;
        if (mode_len >= sizeof(mode_str)) return -1;
        memcpy(mode_str, ptr, mode_len);
        entry->mode = strtol(mode_str, NULL, 8);

        ptr = space + 1;

        // 2. Safely find the null terminator for the name
        const uint8_t *null_byte = memchr(ptr, '\0', end - ptr);
        if (!null_byte) return -1;

        size_t name_len = null_byte - ptr;
        if (name_len >= sizeof(entry->name)) return -1;
        memcpy(entry->name, ptr, name_len);
        entry->name[name_len] = '\0';

        ptr = null_byte + 1;

        // 3. Read the 32-byte binary hash
        if (ptr + HASH_SIZE > end) return -1;
        memcpy(entry->hash.hash, ptr, HASH_SIZE);
        ptr += HASH_SIZE;

        tree_out->count++;
    }
    return 0;
}

// Helper for qsort to ensure consistent tree hashing
static int compare_tree_entries(const void *a, const void *b) {
    return strcmp(((const TreeEntry *)a)->name, ((const TreeEntry *)b)->name);
}

// Serialize a Tree struct into binary format for storage.
// Caller must free(*data_out).
// Returns 0 on success, -1 on error.
int tree_serialize(const Tree *tree, void **data_out, size_t *len_out) {
    size_t max_size = tree->count * 296;
    uint8_t *buffer = malloc(max_size);
    if (!buffer) return -1;

    Tree sorted_tree = *tree;
    qsort(sorted_tree.entries, sorted_tree.count, sizeof(TreeEntry), compare_tree_entries);

    size_t offset = 0;
    for (int i = 0; i < sorted_tree.count; i++) {
        const TreeEntry *entry = &sorted_tree.entries[i];

        int written = sprintf((char *)buffer + offset, "%o %s", entry->mode, entry->name);
        offset += written + 1; // +1 for the null terminator written by sprintf

        memcpy(buffer + offset, entry->hash.hash, HASH_SIZE);
        offset += HASH_SIZE;
    }

    *data_out = buffer;
    *len_out = offset;
    return 0;
}

// ─── IMPLEMENTATION ──────────────────────────────────────────────────────────

// Recursive helper: builds a tree from a slice of index entries
// that all share the same directory prefix at the given depth.
//
// entries     : array of IndexEntry pointers for this subtree
// count       : number of entries in the slice
// prefix      : the directory path prefix at this depth (e.g., "src/")
// prefix_len  : length of prefix
// id_out      : where to write the resulting tree ObjectID
//
// Returns 0 on success, -1 on error.
static int write_tree_level(IndexEntry **entries, int count,
                            const char *prefix, size_t prefix_len,
                            ObjectID *id_out)
{
    Tree tree;
    tree.count = 0;

    int i = 0;
    while (i < count) {
        // Get the path relative to the current prefix
        const char *rel = entries[i]->path + prefix_len;

        // Look for a '/' — if found, this entry lives in a subdirectory
        const char *slash = strchr(rel, '/');

        if (!slash) {
            // ── Leaf file entry ──────────────────────────────────────────
            // rel is just the filename, no more subdirectories
            TreeEntry *te = &tree.entries[tree.count];

            // Use the stored mode from the index
            te->mode = entries[i]->mode;

            // Copy just the filename (rel) as the entry name
            strncpy(te->name, rel, sizeof(te->name) - 1);
            te->name[sizeof(te->name) - 1] = '\0';

            // Copy the blob hash that was stored at pes-add time
            memcpy(te->hash.hash, entries[i]->hash.hash, HASH_SIZE);

            tree.count++;
            i++;

        } else {
            // ── Subdirectory entry ───────────────────────────────────────
            // All entries that start with the same directory name belong
            // to the same subtree — collect them.

            // Extract the directory component name (e.g., "src")
            size_t dir_name_len = (size_t)(slash - rel);
            char dir_name[256];
            if (dir_name_len >= sizeof(dir_name)) return -1;
            memcpy(dir_name, rel, dir_name_len);
            dir_name[dir_name_len] = '\0';

            // Build the full prefix for the subtree (e.g., "src/")
            char sub_prefix[512];
            snprintf(sub_prefix, sizeof(sub_prefix), "%.*s%s/",
                     (int)prefix_len, prefix, dir_name);
            size_t sub_prefix_len = strlen(sub_prefix);

            // Find how many consecutive entries share this subdirectory
            int j = i;
            while (j < count &&
                   strncmp(entries[j]->path, sub_prefix, sub_prefix_len) == 0) {
                j++;
            }

            // Recursively build the subtree for entries[i..j-1]
            ObjectID sub_id;
            if (write_tree_level(entries + i, j - i,
                                 sub_prefix, sub_prefix_len, &sub_id) != 0) {
                return -1;
            }

            // Add a directory entry pointing to the subtree object
            TreeEntry *te = &tree.entries[tree.count];
            te->mode = MODE_DIR;
            strncpy(te->name, dir_name, sizeof(te->name) - 1);
            te->name[sizeof(te->name) - 1] = '\0';
            memcpy(te->hash.hash, sub_id.hash, HASH_SIZE);

            tree.count++;
            i = j; // Skip past all entries we just handled
        }
    }

    // Serialize the tree struct to binary and write to the object store
    void *tree_data = NULL;
    size_t tree_len = 0;
    if (tree_serialize(&tree, &tree_data, &tree_len) != 0) return -1;

    int ret = object_write(OBJ_TREE, tree_data, tree_len, id_out);
    free(tree_data);
    return ret;
}

// Helper: compare index entry pointers by path (for qsort)
static int compare_index_entries(const void *a, const void *b) {
    const IndexEntry *ea = *(const IndexEntry **)a;
    const IndexEntry *eb = *(const IndexEntry **)b;
    return strcmp(ea->path, eb->path);
}

// Build a tree hierarchy from the current index and write all tree
// objects to the object store.
//
// Returns 0 on success, -1 on error.
int tree_from_index(ObjectID *id_out) {
    // Step 1: Load the index (staged files)
    Index idx;
    if (index_load(&idx) != 0) return -1;

    if (idx.count == 0) {
        // Empty tree — serialize a tree with zero entries
        Tree empty;
        empty.count = 0;
        void *tree_data = NULL;
        size_t tree_len = 0;
        if (tree_serialize(&empty, &tree_data, &tree_len) != 0) return -1;
        int ret = object_write(OBJ_TREE, tree_data, tree_len, id_out);
        free(tree_data);
        return ret;
    }

    // Step 2: Build a sorted array of pointers to index entries
    IndexEntry **sorted = malloc(sizeof(IndexEntry *) * (size_t)idx.count);
    if (!sorted) return -1;

    for (int i = 0; i < idx.count; i++) {
        sorted[i] = &idx.entries[i];
    }

    // Sort by path so subdirectory grouping works correctly
    qsort(sorted, (size_t)idx.count, sizeof(IndexEntry *), compare_index_entries);

    // Step 3: Recursively build the root tree (prefix = "", depth = root)
    int ret = write_tree_level(sorted, idx.count, "", 0, id_out);

    free(sorted);
    return ret;
}
