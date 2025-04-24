# Project Phase 3 Report

## Indexing Structure Used

KARWAL/PAUL FILL IN HERE (AND ANY OTHER SECTIONS IF NEEDED)

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

## Assumptions

1.  **B+ Tree Implementation:** Assumes the provided `BTree` class (`index.h`, `index.cpp`) correctly implements `insertKey`, `deleteKey`, and `searchKey` operations, managing its own node splits/merges and adhering to memory limits internally. Assumes `deleteKey(key)` correctly handles potential duplicate keys as intended for this application (either removing all or requiring pointer-specific deletion, which the current executor logic would need adjustment for if the latter is true).
2.  **Page Modification:** Assumes rewriting an entire page via `bufferManager.writePage` after modifying rows in memory is the accepted method for simulating in-place updates/deletions in the absence of finer-grained page modification capabilities (like a `Page::updateRow` method).
3.  **Data Types:** Assumes all table data and query literals involved in conditions and updates are integers.
4.  **Concurrency:** Assumes a single-user environment with no concurrent operations.
5.  **Function Availability:** Assumes helper functions like `evaluateBinOp` are correctly accessible.

## Contributions

will fill in at the end
