## ORDERBY

**Files of interest**:

-   `orderBy.cpp` (new executor file for your command)
-   `executor.cpp` (calls `executeORDERBY()`)
-   `syntacticParser.cpp` (contains `syntacticParseORDERBY()`)
-   `semanticParser.cpp` (contains `semanticParseORDERBY()`)

**Logic**

-   **Command syntax**:

    ```plaintext
    <newTable> <- ORDER BY <columnName> ASC|DESC ON <existingTable>
    ```

    -   The `<existingTable>` must already exist in the table catalogue.
    -   The user supplies a valid `<columnName>` to sort on.
    -   `ASC` or `DESC` is mandatory, indicating ascending or descending.

-   **Implementation details**:

    1. **Syntactic Parsing** (`syntacticParseORDERBY()`):

        - Checks that the tokens match the expected pattern:
            - 8 tokens total, e.g.
                ```
                SortedData <- ORDER BY myColumn ASC ON InputTable
                ```
        - Captures the output table name, the source table name, the sort column, and the direction (ASC or DESC).
        - Sets `parsedQuery.queryType = ORDERBY`.

    2. **Semantic Parsing** (`semanticParseORDERBY()`):

        - Ensures the `<existingTable>` actually exists.
        - Ensures `<newTable>` does _not_ already exist in the catalogue.
        - Checks that `<columnName>` is a valid column in `<existingTable>`.

    3. **Execution** (`executeORDERBY()`):
        - Reads all rows from `<existingTable>` into a vector of rows (`allRows`).
        - Uses `std::sort` or `std::stable_sort` (with a lambda) to sort `allRows` by the specified column index.
        - Creates a new table `<newTable>` with the same column schema, writes out sorted rows to `<newTable>`, and calls `blockify()`.
        - Inserts `<newTable>` into the table catalogue.

**Page Design and Block Access**

-   Just like `SORT`, `CROSS`, or `JOIN`, `ORDER BY` uses a purely in‐memory approach: it loads all rows from the source table (via a `Cursor` that reads pages from disk) into a single `vector<vector<int>>`.
-   After sorting, it writes rows into `<newTable>`’s CSV and calls `blockify()` to split them into pages.

**Error Handling**

-   If `<existingTable>` does not exist or `<newTable>` already exists, a “SEMANTIC ERROR” is printed.
-   If `<columnName>` is not found in `<existingTable>`, a “SEMANTIC ERROR” is printed.
-   If the user types anything other than `ASC` or `DESC`, a “SYNTAX ERROR” is printed.
-   For extremely large tables (more rows than can fit in memory), this in‐memory sort could run out of RAM. Handling that would require an external sort approach, but for typical assignment/test data, the in‐memory solution suffices.

**Sample Usage**

```plaintext
LOAD EMPLOYEE
SortedEmp <- ORDER BY Salary ASC ON EMPLOYEE
PRINT SortedEmp
```

This produces a new table named `SortedEmp` with rows sorted on `Salary` in ascending order.

## PARTITION HASH JOIN

**Files of interest**:

-   `join.cpp`
-   `executor.cpp` (which calls `executeJOIN()`).
-   `syntacticParser.cpp` (which implements `syntacticParseJOIN()`).
-   `semanticParser.cpp` (which implements `semanticParseJOIN()`).

**Logic**

-   **Command syntax**:

    ```plaintext
    <newTableName> <- JOIN <table1>, <table2> ON <column1> == <column2>
    ```

    -   This command performs an **EQUI‐JOIN** of `<table1>` and `<table2>` on `<column1> == <column2>`.
    -   The resulting joined table has **all** columns from `<table1>` (in original order) followed by **all** columns from `<table2>`.

-   **Implementation details**:

    1. **Syntactic Parsing** (`syntacticParseJOIN()`):

        - Verifies the query matches the pattern:
            ```
            <resultTable> <- JOIN <table1>, <table2> ON <col1> == <col2>
            ```
        - **Only** accepts the `==` operator (rejects `<`, `>`, `!=`, etc.).
        - Populates `parsedQuery.joinFirstRelationName`, `parsedQuery.joinSecondRelationName`, etc.

    2. **Semantic Parsing** (`semanticParseJOIN()`):

        - Ensures both `<table1>` and `<table2>` exist in the table catalogue.
        - Ensures `<resultTable>` **does not** already exist.
        - Checks `<col1>` is a valid column of `<table1>`, `<col2>` a valid column of `<table2>`.
        - Confirms only `==` is used (EQUI‐JOIN).

    3. **Execution** (`executeJOIN()`):
        - Implements a **two‐phase partition hash join** under a 10‐block memory constraint (similar to external sort constraints):
            1. **Phase 1**: Partition `<table1>` and `<table2>` into `numBuckets` bucket files on disk. Typically, `numBuckets = 9` if there are 10 buffers total.
            2. **Phase 2**: For each bucket _i_:
                - **Load** bucket _i_ from `<table1>` into memory, building an in‐memory hash table keyed on `<col1>`.
                - **Scan** bucket _i_ from `<table2>`, probing the hash table, and output matching rows directly to `<resultTable>`’s CSV.
        - Then calls `blockify()` to physically split the final joined data into pages.
        - Inserts `<resultTable>` into the table catalogue.

**Page Design and Block Access**

-   Since the join is done in streaming fashion, we only load **one bucket** of `<table1>` (or `<table2>`) at a time. Each bucket is significantly smaller than the full table, keeping memory usage under the 10‐block limit.

-   Once the final joined rows are appended to `<resultTable>`’s CSV, we call `blockify()` on `<resultTable>`, which uses the standard “split into pages” approach (similar to `CROSS`, `SORT`, etc.).

**Error Handling**

-   Throws “SYNTAX ERROR” if the user tries `<`, `>`, or any operator besides `==`.
-   Throws “SEMANTIC ERROR” if `<table1>` or `<table2>` do not exist, if `<resultTable>` already exists, or if `<col1>/<col2>` are not valid columns of the respective tables.
-   If no matching rows exist, `<resultTable>` still gets created but may end up empty (or blockify to zero blocks).

**Sample Usage**

```plaintext
LOAD EMPLOYEE
LOAD DEPARTMENT
JoinedEmpDept <- JOIN EMPLOYEE, DEPARTMENT ON Dno == Dnumber
PRINT JoinedEmpDept
```

# Assumptions

1. **Integer‐only data**: All matrix CSV files contain strictly integer data in n×n format.
2. **No partial columns**: Each row in the CSV is assumed to have exactly `n` columns, matching the dimension. Any mismatch causes an error.
3. **Block Size**: A `BLOCK_SIZE` set to 1 (and scaled to 1KB) is used as the basis for `maxRowsPerBlock`. If the matrix dimension is large, `maxRowsPerBlock` might become `1`.
4. **Page Files**: Matrix data is split row‐by‐row into `../data/temp/<matrixName>_Page<i>`. These are removed when the matrix is unloaded (e.g., upon program quit or if the user explicitly removes it).
5. **Dimension**: The code uses the line count of the CSV to define `n`. There is no separate check that the row length also equals `n`, beyond reading columns in each row line.

# Contributions

Below is an overview of which team member(s) contributed to each command:

-   **SOURCE**: Lakshya Shastri
-   **LOAD MATRIX**: Aditya Tejpaul
-   **PRINT MATRIX**: Harshit Karwal
-   **EXPORT MATRIX**: Lakshya Shastri and Aditya Tejpaul
-   **ROTATE**: Aditya Tejpaul and Harshit Karwal
-   **CROSSTRANSPOSE**: Lakshya Shastri and Harshit Karwal
-   **CHECKANTISYM**: Lakshya Shastri, Aditya Tejpaul, and Harshit Karwal
-   **ORDERBY**: Lakshya Shastri
-   **GROUPBY**: Harshit Karwal
