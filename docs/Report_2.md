## SORT

**Files of interest**:

-   `sort.cpp` (implements the SORT command functionality)
-   `executor.cpp` (calls `executeSORT()`)
-   `syntacticParser.cpp` (contains `syntacticParseSORT()`)
-   `semanticParser.cpp` (contains `semanticParseSORT()`)

**Logic**

-   **Command syntax**:

    ```plaintext
    SORT <table-name> BY <col1>,<col2>,<col3> IN <ASC|DESC>,<ASC|DESC>,<ASC|DESC>
    ```

    -   The `<table-name>` must already exist in the table catalogue.
    -   Users can specify multiple columns to sort by, establishing a priority order (leftmost column has highest priority).
    -   For each column, users must specify sort direction (ASC for ascending, DESC for descending).
    -   The number of columns must match the number of sort directions.

-   **Implementation details**:

    1. **Syntactic Parsing** (`syntacticParseSORT()`):

        - Verifies the query matches the pattern with "SORT", "BY", and "IN" keywords properly placed.
        - Parses the columns between "BY" and "IN", handling comma-separated lists.
        - Ensures that each direction specified after "IN" is either "ASC" or "DESC".
        - Verifies the number of columns matches the number of sort directions.
        - Stores column-direction pairs for later processing.

    2. **Semantic Parsing** (`semanticParseSORT()`):

        - Ensures the table to be sorted exists in the catalogue.
        - Checks that all specified columns exist in the table.
        - Returns error messages for non-existent tables or columns.

    3. **Execution** (`executeSORT()`):
        - Implements a **two-phase external merge sort** algorithm, maintaining a 10-block memory constraint:
            1. **Phase 1 - Initial runs creation**:
                - Reads chunks of rows (up to 10 blocks worth) from the table.
                - Sorts each chunk in memory using the specified columns and directions.
                - Writes each sorted chunk to a temporary run file.
            2. **Phase 2 - Merge phase**:
                - Iteratively merges up to 9 runs at a time (reserving 1 buffer for output).
                - Uses a priority queue approach to efficiently merge multiple sorted streams.
                - Continues merging until only one run remains (the final sorted table).
        - The final sorted run replaces the original table content, maintaining the same table name and schema.
        - Performs an in-place sort, avoiding the creation of a new table.

**Page Design and Block Access**

-   The implementation strictly adheres to the 10-block memory constraint:
    - During the initial run creation, loads at most 10 blocks of data at once.
    - During merge phase, uses at most 9 blocks for input and 1 for output.
-   Uses temporary run files to store intermediate results, which are stored as tables in the catalogue.
-   For very large tables, it may require multiple merge passes, each time combining up to 9 runs until only one remains.
-   When writing the final sorted data back to the original table, it clears all existing blocks and regenerates them with the sorted content.

**Error Handling**

-   Prints "SYNTAX ERROR" if the command structure is invalid or if an unsupported sort direction is specified.
-   Prints "SEMANTIC ERROR" if the table doesn't exist or if any of the specified columns don't exist in the table.
-   Handles edge cases like empty tables gracefully.
-   Ensures cleanup of temporary run files, even if the sort operation encounters an error.

**Sample Usage**

```plaintext
LOAD EMPLOYEE
SORT EMPLOYEE BY Sex, Salary IN ASC, DESC
PRINT EMPLOYEE
```

This sorts the EMPLOYEE table first by GENDER in ascending order, then by SALARY in descending order within each gender group. The operation modifies the table in-place, so the sorted result is available under the original table name (EMPLOYEE).


## ORDERBY

**Files of interest**:

-   `orderBy.cpp` (new executor file for the command)
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

## GROUPBY

**Files of interest:**

-   `groupBy.cpp` (new executor file)
-   `executor.cpp` (calls `executeGROUPBY()`)
-   `syntacticParser.cpp` (contains `syntacticParseGROUPBY()`)
-   `semanticParser.cpp` (contains `semanticParseGROUPBY()`)
-   `syntacticParser.h` (contains `AggregateFunction` enum and GROUP BY fields)

**Logic**

-   **Command syntax**:

    ```plaintext
    <resultTable> <- GROUP BY <attribute1>
    FROM <table>
    HAVING <Aggregate-Func1(attribute2)> <bin-op> <attribute-value>
    RETURN <Aggregate-Func2(attribute3)>
    ```

    Where:

    -   `<attribute1>` is the column to group by
    -   `<Aggregate-Func1>` can be MAX, MIN, COUNT, SUM, or AVG
    -   `<bin-op>` can be >, <, >=, <=, or ==
    -   `<attribute-value>` is an integer constant
    -   `<Aggregate-Func2>` can be MAX, MIN, COUNT, SUM, or AVG

-   **Implementation details**:

    1. **Syntactic Parsing** (`syntacticParseGROUPBY()`):

        - Parses the command into its components
        - Validates the syntax of the HAVING and RETURN clauses
        - Checks that the aggregate functions are valid
        - Extracts the binary operator and comparison value

    2. **Semantic Parsing** (`semanticParseGROUPBY()`):

        - Verifies the source table exists
        - Verifies the result table doesn't already exist
        - Ensures all referenced columns exist in the source table

    3. **Execution** (`executeGROUPBY()`):
        - First sorts the input table using the existing SORT command on the grouping attribute
        - Processes the sorted table in a streaming fashion to identify groups
        - For each group:
            - Computes the required aggregates for HAVING and RETURN clauses
            - Filters groups based on the HAVING condition
            - Writes qualifying groups with computed return values to the result table
        - Result table contains two columns: the grouping attribute and the return aggregate value
        - Column headers include the aggregate function name, e.g., "DepartmentID, AVG(Salary)"

**Page Design and Block Access**

-   Utilizes the existing SORT functionality to order rows by the grouping attribute
-   Processes data in a streaming fashion after sorting, avoiding loading the entire table into memory
-   Respects the 10-buffer limit by reading and processing one page at a time
-   Groups are identified by detecting changes in the grouping attribute as the cursor iterates through sorted rows

**Error Handling**

-   Prints "SYNTAX ERROR" for malformed queries
-   Prints "SEMANTIC ERROR" if the source table doesn't exist
-   Prints "SEMANTIC ERROR" if the result table already exists
-   Prints "SEMANTIC ERROR" if referenced columns don't exist in the source table
-   Handles the case where no groups match the HAVING condition, creating an empty result table

**Sample Usage**

```plaintext
LOAD EMPLOYEE
Result <- GROUP BY Super_ssn FROM EMPLOYEE HAVING AVG(Salary) > 30000 RETURN MAX(Salary)
PRINT Result
```

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

-   **SORT**: Aditya Tejpaul
-   **ORDERBY**: Lakshya Shastri
-   **GROUPBY**: Harshit Karwal
-   **PARTIAL HASH JOIN**: Lakshya Shastri, Harshit Karwal, Aditya Tejpaul
