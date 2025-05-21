# Project Phase 3 Report

## Detailed Explanation of Commands

### INSERT

-   **Syntax:** `INSERT INTO table_name ( col1 = val1, col2 = val2, ... )`
-   **Core Logic:**
    1.  The command no longer appends to the source CSV file or calls `table->reload()`.
    2.  It identifies the target data page (`../data/temp/<tableName>_Page<ID>`) for the new row. This is typically the last page unless the table is empty (Page 0) or the last page is full (requiring a new page to be created).
    3.  **Page Writing:**
        -   If a new page is needed, an empty buffer is prepared, the new row is added, and `bufferManager.writePage` is called to create the new page file with a row count of 1. The table's `blockCount` is incremented, and the new page's row count (1) is added to `table->rowsPerBlockCount`.
        -   If appending to an existing page, that page is loaded using `bufferManager.getPage`. Its existing rows are read, the new row is appended to this in-memory list, and the entire updated list of rows is written back to the _same_ page file using `bufferManager.writePage`. The page's entry in `table->rowsPerBlockCount` is updated.
    4.  The table's total `rowCount` is incremented.
-   **Index Interaction:**
    1.  After the row is successfully written to a data page, the function checks if the table is indexed (`table->indexed && table->index != nullptr`).
    2.  If indexed, it retrieves the key value from the newly inserted row, specifically from the column designated by `table->indexedColumn`.
    3.  It determines the `RecordPointer` (pair of `{pageIndex, rowIndexInPage}`) where the row was physically inserted.
    4.  It calls `table->index->insertKey(key, recordPointer)` to add the new entry to the B+ Tree, potentially triggering node splits within the index structure.
-   **Efficiency:** Avoids full table rewrites. Modifies at most one data page and performs a logarithmic insertion into the index tree.

### DELETE

-   **Syntax:** `DELETE FROM table_name WHERE column_name binary_operator value`
-   **Core Logic - Row Identification:**
    1.  The implementation first checks if the `WHERE` clause is suitable for index-based lookup:
        -   Is the table indexed (`table->indexed && table->index != nullptr`)?
        -   Is the condition column (`parsedQuery.deleteCondColumn`) the indexed column (`table->indexedColumn`)?
        -   Is the operator equality (`parsedQuery.deleteCondOperator == EQUAL`)?
    2.  **Index Lookup:** If all conditions above are met, `table->index->searchKey(value)` is called to efficiently retrieve a vector of `RecordPointer`s for rows matching the value. A validation step filters out any pointers that fall outside the known bounds of the table's pages or row counts based on current metadata.
    3.  **Table Scan:** If index lookup is not possible, the implementation falls back to a full table scan using a `Cursor`. It iterates through each row, evaluates the `WHERE` condition using `evaluateBinOp`, and collects the `RecordPointer` of each matching row.
-   **Core Logic - Row Removal & Page Modification:**
    1.  The collected `RecordPointer`s (either from index or scan) are grouped by their `pageIndex`.
    2.  The code iterates through each affected page index.
    3.  For a given page:
        -   The original page is loaded using `bufferManager.getPage`.
        -   A list of rows to _keep_ is built by iterating through the original rows and skipping those whose `rowIndexInPage` matches one marked for deletion on this page.
        -   The page file is rewritten using `bufferManager.writePage`, containing only the _kept_ rows.
        -   The corresponding entry in `table->rowsPerBlockCount` is updated to reflect the new, smaller row count for that page.
-   **Index Interaction:**
    1.  Crucially, _during_ the page processing step (before the page is rewritten), for each row that is identified for deletion:
        -   If the table is indexed, the key value from the `table->indexedColumn` of that specific row is extracted.
        -   `table->index->deleteKey(key)` is called to remove the corresponding entry from the B+ Tree. (Assumes `deleteKey` handles duplicates appropriately based on the BTree implementation). This might trigger node merges/borrows within the index.
-   **Metadata:** The table's total `rowCount` is decremented by the number of rows deleted. `blockCount` remains unchanged in this implementation (empty pages are not removed).
-   **Efficiency:** Leverages index lookup for `==` conditions on indexed columns. Modifies only the data pages containing deleted rows. Performs logarithmic deletions from the index tree.

### UPDATE

-   **Syntax:** `UPDATE table_name WHERE condition SET column_name = value` (Note: `WHERE` precedes `SET`)
-   **Core Logic - Row Identification:**
    1.  Similar to `DELETE`, it checks if the `WHERE` clause condition (`updateCondColumn == indexedColumn && updateCondOperator == EQUAL`) allows for index usage.
    2.  **Index Lookup:** If applicable, `table->index->searchKey(updateCondValue)` is used to get `RecordPointer`s, followed by pointer validation.
    3.  **Table Scan:** Otherwise, a full table scan using `Cursor` and `evaluateBinOp` is performed to find matching `RecordPointer`s.
-   **Core Logic - Row Modification & Page Update:**
    1.  The code iterates through the identified `RecordPointer`s.
    2.  For each pointer `{pageIndex, rowIndexInPage}`:
        -   The corresponding page is loaded via `bufferManager.getPage`.
        -   The original row data at `rowIndexInPage` is retrieved.
        -   A copy of the row is made, and the value in the target column (`parsedQuery.updateTargetColumn`) is changed to the new literal (`parsedQuery.updateLiteral`).
        -   The _entire_ page is rewritten using `bufferManager.writePage`. To do this, all original rows are loaded into memory, the row at `rowIndexInPage` is replaced with the modified version, and this complete set of rows is written back. The page's row count remains the same.
-   **Index Interaction (Conditional):**
    1.  After modifying a row and rewriting its page, index maintenance occurs **only if all** the following are true:
        -   The table is indexed (`table->indexed && table->index != nullptr`).
        -   The column being updated (`parsedQuery.updateTargetColumn`) _is_ the indexed column (`table->indexedColumn`).
        -   The actual value in the indexed column _changed_ (i.e., `oldValue != newValue`).
    2.  If maintenance is required:
        -   `table->index->deleteKey(oldValue)` is called (using the value from the original row).
        -   `table->index->insertKey(newValue, pointer)` is called (using the value from the modified row and the same `RecordPointer`).
-   **Metadata:** `table->rowCount` and `table->rowsPerBlockCount` remain unchanged by `UPDATE`.
-   **Efficiency:** Uses index for `WHERE` lookup when possible. Modifies only affected data pages. Index maintenance is conditional and logarithmic, avoiding unnecessary updates if the indexed key didn't change.

### INDEX

**Syntax:** `INDEX ON <column_name> FROM <table_name> USING BTREE`

**Functionality:** Creates a B+ Tree index on the specified column of the given table.

**Implementation Details:**

1.  **Structure:**
    *   A B+ Tree structure is used, consisting of internal nodes and leaf nodes.
    *   Internal nodes store keys and pointers (page indices) to child nodes.
    *   Leaf nodes store keys and `RecordPointer`s (`{pageIndex, rowIndex}`) pointing to the actual data rows in the table's pages.
    *   Leaf nodes are linked together sequentially using `nextLeafPageIndex` to facilitate efficient range queries.
    *   The tree's `order` (p, max pointers in internal nodes) and `leafOrder` (Pleaf, max record pointers in leaf nodes) are calculated based on the global `BLOCK_SIZE` and the size of keys (int) and pointers (int or `RecordPointer`), aiming to fit nodes within a single block/page ([`BTree::BTree`](src/index.cpp)).

2.  **Node Representation (`BTreeNode`):**
    *   The [`BTreeNode`](src/index.h) class represents both internal and leaf nodes.
    *   Members include `isLeaf`, `keys`, `childrenPageIndices` (for internal), `recordPointers` (for leaf), `nextLeafPageIndex` (for leaf), `parentPageIndex`, `pageIndex`, and `keyCount` ([`BTreeNode`](src/index.h)).

3.  **Storage and I/O:**
    *   Each `BTreeNode` is stored as a separate file in the `../data/temp/` directory, named `<indexName>_Node<pageIndex>` (e.g., `STUDENTS2_math_index_Node0`).
    *   Serialization ([`BTreeNode::serialize`](src/index.cpp)) writes the node's state into a 3-row format:
        *   Row 0: Metadata (`isLeaf`, `keyCount`, `parentPageIndex`, `nextLeafPageIndex` if leaf).
        *   Row 1: Keys (`std::vector<int>`).
        *   Row 2: Pointers (`std::vector<int>` for children or flattened `std::vector<RecordPointer>` for data).
    *   Deserialization ([`BTreeNode::deserialize`](src/index.cpp)) reads this file format.
    *   Node I/O is handled directly using `std::ofstream` ([`BTree::writeNode`](src/index.cpp)) and `std::ifstream` ([`BTree::fetchNode`](src/index.cpp)), bypassing the `BufferManager` used for table pages.
    *   New node pages are allocated sequentially by incrementing a counter ([`BTree::allocateNewNodePage`](src/index.cpp)).

4.  **Core Operations:**
    *   **Build (`BTree::buildIndex`):** Iterates through all rows of the source table using a `Cursor`. For each row, extracts the key and calculates the `RecordPointer`. Calls `insertKey` to add the entry to the tree ([`BTree::buildIndex`](src/index.cpp)).
    *   **Insertion (`BTree::insertKey`):** Finds the appropriate leaf node using `findLeafNodePageIndex`. Calls `insertIntoLeaf` ([`BTree::insertKey`](src/index.cpp)). If the tree is empty, it calls `startNewTree` ([`BTree::startNewTree`](src/index.cpp)).
    *   **Leaf Insertion (`BTree::insertIntoLeaf`):** Inserts the key and `RecordPointer` into the sorted leaf node. If the leaf is full ([`BTreeNode::isFull`](src/index.cpp)), it splits the leaf: temporary vectors hold combined keys/pointers, the node is split at the midpoint, the new right node is created and written, the linked list pointers (`nextLeafPageIndex`) are updated, and `insertIntoParent` is called with the middle key ([`BTree::insertIntoLeaf`](src/index.cpp)).
    *   **Parent Insertion (`BTree::insertIntoParent`):** Inserts a key (from a child split) and the new child pointer into the correct position in the parent node. If the parent is full, it splits recursively, potentially creating a new root node if the original root splits ([`BTree::insertIntoParent`](src/index.cpp)). Correctly handles setting keys and child pointers when creating a new root ([`BTree::insertIntoParent`](src/index.cpp)).
    *   **Deletion (`BTree::deleteKey`):** Finds the leaf node containing the key ([`BTree::findLeafNodePageIndex`](src/index.cpp)). Removes *all* occurrences of the key and associated `RecordPointer`s from the leaf ([`BTreeNode::removeLeafEntry`](src/index.cpp)). If the node becomes underflowed ([`BTreeNode::isMinimal`](src/index.cpp)), `handleUnderflow` is called. Finally, `adjustRoot` is called to potentially shrink the tree height ([`BTree::deleteKey`](src/index.cpp)).
    *   **Underflow Handling (`BTree::handleUnderflow`):** Finds a sibling node ([`BTree::findSiblingPageIndex`](src/index.cpp)). Attempts to borrow an entry from the sibling if the sibling has more than the minimum required entries ([`BTree::borrowFromLeafSibling`](src/index.cpp)). If borrowing is not possible, it merges the node with a sibling ([`BTree::mergeLeafNodes`](src/index.cpp)), removing an entry from the parent. If the parent underflows due to the merge, `handleUnderflow` is called recursively on the parent ([`BTree::handleUnderflow`](src/index.cpp)). *Note: Borrowing and merging for internal nodes are currently stubbed ([`BTree::borrowFromInternalSibling`](src/index.cpp), [`BTree::mergeInternalNodes`](src/index.cpp)).*
    *   **Root Adjustment (`BTree::adjustRoot`):** Checks if the root node is an internal node with only one child after a deletion/merge. If so, the single child becomes the new root, and the old root page is deleted ([`BTree::adjustRoot`](src/index.cpp)). Also handles the case where the tree becomes completely empty.
    *   **Search (`BTree::searchKey`, `BTree::searchRange`):** Finds the starting leaf node using `findLeafNodePageIndex`. For `searchKey`, it uses `std::lower_bound` to find the key(s) in the leaf. For `searchRange`, it iterates from the `startKey` position and follows the `nextLeafPageIndex` pointers until keys exceed `endKey` ([`BTree::searchKey`](src/index.cpp), [`BTree::searchRange`](src/index.cpp)).

5.  **Index Dropping (`BTree::dropIndex`):** Iterates from 0 up to the current `nodeCount` and attempts to delete each corresponding node file (`../data/temp/<indexName>_Node<i>`). Resets `rootPageIndex` and `nodeCount`.

**Why B+ Tree:**

*   **Efficient Disk I/O:** B+ Trees are optimized for disk-based storage. Their high branching factor (determined by `order`) minimizes the number of disk reads (node fetches) required to locate data, resulting in logarithmic time complexity for search, insertion, and deletion.
*   **Range Queries:** The linked list connecting leaf nodes allows for efficient sequential scanning of data within a specified range without traversing back up the tree.
*   **Balanced Structure:** Insertion and deletion algorithms maintain tree balance, ensuring worst-case performance guarantees.

**Assumptions:**

1.  **Index Node I/O:** Index nodes (`BTreeNode`) are read from and written to individual files directly using `ifstream`/`ofstream`, not managed by the `BufferManager`.
2.  **Data Types:** Keys stored in the index are assumed to be integers. `RecordPointer`s store integer page and row indices.
3.  **Order Calculation:** The `order` and `leafOrder` are calculated based on `BLOCK_SIZE` and the `sizeof(int)` and `sizeof(RecordPointer)`. Assumes these sizes are consistent and the calculation provides reasonable node sizes.
4.  **Concurrency:** Assumes a single-user environment; no locking or concurrency control mechanisms are implemented for index operations.
5.  **Storage Location:** Index node files are stored in the fixed relative path `../data/temp/`.
6.  **Deletion Semantics:** `deleteKey(key)` removes *all* entries matching the key within the found leaf node.
7.  **Incomplete Features:** Borrowing and merging operations for *internal* nodes during deletion underflow are not fully implemented (stubs exist).
8.  **Persistence:** The index structure (root page index, node count) is not persisted between program executions. The index must be rebuilt using the `INDEX` command each time the program starts.

### SEARCH

-   **Syntax:** `R <- SEARCH FROM T WHERE col bin_op literal`
-   **Functionality:** Selects rows from table `T` where the condition `col bin_op literal` is true and stores the result in a new table `R`. Crucially, this command **always** attempts to use a B+ Tree index on the specified `col`. If an index does not exist on `col`, it implicitly *creates* one before performing the search. If index creation fails, the command aborts. Table scans are *not* used for this command.
-   **Supported Operators:** `==`, `<`, `>`, `<=`, `>=`, `!=`.
-   **Core Logic & Index Interaction:**
    1.  **Index Check/Creation:**
        -   The system first checks if a B+ Tree index already exists on the specified `searchColumnName` for the `searchRelationName` using `table->getIndex()`.
        -   **If Index Exists:** It retrieves the pointer to the existing `BTree` object.
        -   **If Index Doesn't Exist:** It attempts to *implicitly create* a B+ Tree index on `searchColumnName` by temporarily setting the query type to `INDEX` and calling `executeINDEX()`. After creation, it attempts to retrieve the pointer to the newly created index. If creation or retrieval fails, the `SEARCH` operation aborts.
    2.  **Index-Based Search:** Assuming a valid `BTree*` (`indexToUse`) was obtained (either existing or implicitly created):
        -   The appropriate index search method is called based on `parsedQuery.searchOperator`:
            -   `==`: `indexToUse->searchKey(literal)`
            -   `<`: `indexToUse->searchRange(MIN_INT, literal - 1)`
            -   `>`: `indexToUse->searchRange(literal + 1, MAX_INT)`
            -   `<=`: `indexToUse->searchRange(MIN_INT, literal)`
            -   `>=`: `indexToUse->searchRange(literal, MAX_INT)`
            -   `!=`: Combines results from two range scans: `searchRange(MIN_INT, literal - 1)` and `searchRange(literal + 1, MAX_INT)`.
        -   This yields a `vector<RecordPointer>` containing the locations (`{pageIndex, rowIndexInPage}`) of potentially matching rows within the source table's data pages.
    3.  **Row Fetching & Filtering:**
        -   The code iterates through the obtained `RecordPointer`s.
        -   **Pointer Validation:** Each pointer is validated against the source table's metadata (`blockCount`, `rowsPerBlockCount`) to ensure it points to a valid location. Invalid pointers are logged and skipped.
        -   **Row Retrieval:** For valid pointers, the corresponding page is fetched using `bufferManager.getPage()`, and the specific row data is retrieved using `page.getRow()`.
        -   **Result Writing:** The retrieved row data is written to the `resultTable`.
    4.  **Result Finalization:**
        -   After processing all pointers, `resultTable->blockify()` is called to create the page files for the result table.
        -   The `resultTable` is inserted into the `tableCatalogue`.
-   **Efficiency:** Relies entirely on the B+ Tree index for row identification, providing logarithmic time complexity for finding matching row pointers (O(log_p N) where p is tree order, N is number of keys). The subsequent row fetching depends on the number of matching rows and their distribution across pages. Avoids costly full table scans. Implicit index creation adds an initial O(M log_p M) cost (where M is total rows) if the index wasn't pre-built.
-   **Assumptions:**
    *   The `WHERE` clause condition always compares a column against an integer literal.
    *   Implicit index creation uses the BTREE strategy.
    *   Index node I/O happens directly, not via `BufferManager`.
    *   Integer data types.
-   **Error Handling:**
    *   Syntax errors during parsing.
    *   Semantic errors (source table doesn't exist, result table exists, search column doesn't exist).
    *   Aborts if implicit index creation fails.
    *   Handles and logs invalid `RecordPointer`s returned by the index (e.g., due to stale index data if not properly maintained by INSERT/DELETE/UPDATE).
    *   Handles cases where fetched pages or rows are unexpectedly empty.


## Overall Assumptions

1.  **Page Modification:** Assumes rewriting an entire page via `bufferManager.writePage` after modifying rows in memory is the accepted method for simulating in-place updates/deletions in the absence of finer-grained page modification capabilities (like a `Page::updateRow` method).
3.  **Data Types:** Assumes all table data and query literals involved in conditions and updates are integers.
4.  **Concurrency:** Assumes a single-user environment with no concurrent operations.
5.  **Function Availability:** Assumes helper functions like `evaluateBinOp` are correctly accessible.

## Contributions

will fill in at the end
