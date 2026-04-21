// index.c — Staging area implementation
//
// Text format of .pes/index (one entry per line, sorted by path):
//
//   <mode-octal> <64-char-hex-hash> <mtime-seconds> <size> <path>
//
// Example:
//   100644 a1b2c3d4e5f6...  1699900000 42 README.md
//   100644 f7e8d9c0b1a2...  1699900100 128 src/main.c
//
// This is intentionally a simple text format. No magic numbers, no
// binary parsing. The focus is on the staging area CONCEPT (tracking
// what will go into the next commit) and ATOMIC WRITES (temp+rename).
//
// PROVIDED functions: index_find, index_remove, index_status
// TODO functions:     index_load, index_save, index_add

#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

// Find an index entry by path (linear scan).
IndexEntry* index_find(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0)
            return &index->entries[i];
    }
    return NULL;
}

// Remove a file from the index.
// Returns 0 on success, -1 if path not in index.
int index_remove(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0) {
            int remaining = index->count - i - 1;
            if (remaining > 0)
                memmove(&index->entries[i], &index->entries[i + 1],
                        remaining * sizeof(IndexEntry));
            index->count--;
            return index_save(index);
        }
    }
    fprintf(stderr, "error: '%s' is not in the index\n", path);
    return -1;
}

// Print the status of the working directory.
int index_status(const Index *index) {
    printf("Staged changes:\n");
    int staged_count = 0;
    for (int i = 0; i < index->count; i++) {
        printf("  staged:     %s\n", index->entries[i].path);
        staged_count++;
    }
    if (staged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Unstaged changes:\n");
    int unstaged_count = 0;
    for (int i = 0; i < index->count; i++) {
        struct stat st;
        if (stat(index->entries[i].path, &st) != 0) {
            printf("  deleted:    %s\n", index->entries[i].path);
            unstaged_count++;
        } else {
            if (st.st_mtime != (time_t)index->entries[i].mtime_sec ||
                st.st_size  != (off_t)index->entries[i].size) {
                printf("  modified:   %s\n", index->entries[i].path);
                unstaged_count++;
            }
        }
    }
    if (unstaged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Untracked files:\n");
    int untracked_count = 0;
    DIR *dir = opendir(".");
    if (dir) {
        struct dirent *ent;
        while ((ent = readdir(dir)) != NULL) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
            if (strcmp(ent->d_name, ".pes") == 0) continue;
            if (strcmp(ent->d_name, "pes") == 0) continue;
            if (strstr(ent->d_name, ".o") != NULL) continue;

            int is_tracked = 0;
            for (int i = 0; i < index->count; i++) {
                if (strcmp(index->entries[i].path, ent->d_name) == 0) {
                    is_tracked = 1;
                    break;
                }
            }

            if (!is_tracked) {
                struct stat st;
                stat(ent->d_name, &st);
                if (S_ISREG(st.st_mode)) {
                    printf("  untracked:  %s\n", ent->d_name);
                    untracked_count++;
                }
            }
        }
        closedir(dir);
    }
    if (untracked_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    return 0;
}

// ─── IMPLEMENTATION ───────────────────────────────────────────────────────────

// Helper: compare IndexEntry by path for qsort
static int compare_entries_by_path(const void *a, const void *b) {
    return strcmp(((const IndexEntry *)a)->path,
                  ((const IndexEntry *)b)->path);
}

// Load the index from .pes/index.
//
// Format per line:
//   <mode-octal> <64-hex-hash> <mtime> <size> <path>
//
// Returns 0 on success, -1 on error.
int index_load(Index *index) {
    index->count = 0;

    FILE *f = fopen(INDEX_FILE, "r");
    if (!f) {
        // File doesn't exist yet — empty index, not an error
        return 0;
    }

    char hex[HASH_HEX_SIZE + 2]; // +2 for safety
    unsigned int mode;
    unsigned long mtime;
    unsigned long size;
    char path[512];

    while (index->count < MAX_INDEX_ENTRIES) {
        // Read one line: mode hex mtime size path
        int matched = fscanf(f, "%o %64s %lu %lu %511s",
                             &mode, hex, &mtime, &size, path);
        if (matched == EOF || matched < 5) break;

        IndexEntry *e = &index->entries[index->count];

        e->mode     = mode;
        e->mtime_sec = (uint64_t)mtime;
        e->size      = (uint64_t)size;
        strncpy(e->path, path, sizeof(e->path) - 1);
        e->path[sizeof(e->path) - 1] = '\0';

        // Convert 64-char hex string → ObjectID
        if (hex_to_hash(hex, &e->id) != 0) {
            fclose(f);
            return -1;
        }

        index->count++;
    }

    fclose(f);
    return 0;
}

// Save the index to .pes/index atomically (temp file + rename).
//
// Returns 0 on success, -1 on error.
int index_save(const Index *index) {
    // Step 1: Sort entries by path for a deterministic, readable index
    Index sorted = *index;
    qsort(sorted.entries, (size_t)sorted.count,
          sizeof(IndexEntry), compare_entries_by_path);

    // Step 2: Write to a temp file in the same directory as INDEX_FILE
    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp_XXXXXX", INDEX_FILE);

    int fd = mkstemp(tmp_path);
    if (fd < 0) return -1;

    FILE *f = fdopen(fd, "w");
    if (!f) {
        close(fd);
        unlink(tmp_path);
        return -1;
    }

    // Step 3: Write each entry as a text line
    for (int i = 0; i < sorted.count; i++) {
        const IndexEntry *e = &sorted.entries[i];

        char hex[HASH_HEX_SIZE + 1];
        hash_to_hex(&e->id, hex);

        fprintf(f, "%o %s %lu %lu %s\n",
                e->mode,
                hex,
                (unsigned long)e->mtime_sec,
                (unsigned long)e->size,
                e->path);
    }

    // Step 4: Flush userspace buffers then fsync to disk
    if (fflush(f) != 0) {
        fclose(f);
        unlink(tmp_path);
        return -1;
    }
    if (fsync(fileno(f)) != 0) {
        fclose(f);
        unlink(tmp_path);
        return -1;
    }
    fclose(f);

    // Step 5: Atomically replace the old index with the new one
    if (rename(tmp_path, INDEX_FILE) != 0) {
        unlink(tmp_path);
        return -1;
    }

    return 0;
}

// Stage a file for the next commit.
//
// Steps:
//   1. Read the file contents
//   2. Write a blob object to the object store
//   3. Gather file metadata (mode, mtime, size)
//   4. Add or update the entry in the in-memory index
//   5. Save the index atomically
//
// Returns 0 on success, -1 on error.
int index_add(Index *index, const char *path) {
    // Step 1: Open and read the file contents
    FILE *f = fopen(path, "rb");
    if (!f) {
        fprintf(stderr, "error: cannot open '%s'\n", path);
        return -1;
    }

    fseek(f, 0, SEEK_END);
    long file_size = ftell(f);
    fseek(f, 0, SEEK_SET);

    if (file_size < 0) {
        fclose(f);
        return -1;
    }

    uint8_t *contents = malloc((size_t)file_size);
    if (!contents) {
        fclose(f);
        return -1;
    }

    if (fread(contents, 1, (size_t)file_size, f) != (size_t)file_size) {
        free(contents);
        fclose(f);
        return -1;
    }
    fclose(f);

    // Step 2: Store contents as a blob object → get its hash
    ObjectID blob_id;
    if (object_write(OBJ_BLOB, contents, (size_t)file_size, &blob_id) != 0) {
        free(contents);
        fprintf(stderr, "error: failed to write blob for '%s'\n", path);
        return -1;
    }
    free(contents);

    // Step 3: Gather file metadata
    struct stat st;
    if (lstat(path, &st) != 0) {
        fprintf(stderr, "error: cannot stat '%s'\n", path);
        return -1;
    }

    uint32_t mode;
    if (S_ISDIR(st.st_mode))       mode = 0040000;
    else if (st.st_mode & S_IXUSR) mode = 0100755;
    else                            mode = 0100644;

    // Step 4: Update existing entry or add a new one
    IndexEntry *existing = index_find(index, path);
    if (existing) {
        // Update in place
        existing->id        = blob_id;
        existing->mode      = mode;
        existing->mtime_sec = (uint64_t)st.st_mtime;
        existing->size      = (uint64_t)st.st_size;
    } else {
        // Add new entry
        if (index->count >= MAX_INDEX_ENTRIES) {
            fprintf(stderr, "error: index is full\n");
            return -1;
        }
        IndexEntry *e = &index->entries[index->count];
        e->id        = blob_id;
        e->mode      = mode;
        e->mtime_sec = (uint64_t)st.st_mtime;
        e->size      = (uint64_t)st.st_size;
        strncpy(e->path, path, sizeof(e->path) - 1);
        e->path[sizeof(e->path) - 1] = '\0';
        index->count++;
    }

    // Step 5: Persist the updated index atomically
    return index_save(index);
}
