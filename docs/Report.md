# Table of Contents

- [Table of Contents](#table-of-contents)
- [Commands](#commands)
  - [SOURCE](#source)
  - [LOAD MATRIX](#load-matrix)
  - [PRINT MATRIX](#print-matrix)
  - [EXPORT MATRIX](#export-matrix)
  - [ROTATE](#rotate)
  - [CROSSTRANSPOSE](#crosstranspose)
  - [CHECKANTISYM](#checkantisym)
  - [ORDERBY](#orderby)
  - [GROUPBY](#groupby)
- [Assumptions](#assumptions)
- [Contributions](#contributions)

# Commands

Below is an explanation of how each matrix command (implemented by us) works in this codebase.

## SOURCE

**Files of interest:**

-   `source.cpp`
-   `executor.cpp` (calls `executeSOURCE()`)

**Logic**

-   **Command syntax**:
    ```plaintext
    SOURCE <filename>
    ```

The `<filename>` is expected to be in the `/data` directory and to end with a `.ra` extension.

-   **Implementation details**:
    1. **Syntactic Parsing** (`syntacticParseSOURCE()`):
        - Checks that the query has exactly two tokens: `SOURCE` and the `<filename>`.
        - If valid, sets `parsedQuery.queryType = SOURCE`, and captures the `<filename>` in `parsedQuery.sourceFileName`.
    2. **Semantic Parsing** (`semanticParseSOURCE()`):
        - Verifies that the `<filename>.ra` actually exists in `../data/`.
        - If not found, prints an error.
    3. **Execution** (`executeSOURCE()`):
        - Opens the file `../data/<filename>.ra`.
        - Reads each line and treats it as a command, re‐running the parsing/semantic checks/execute pipeline for each line.
        - Hence, it effectively “sources” a script of queries line by line.

**Page Design and Block Access**

-   The `SOURCE` command itself does not create or read data blocks.
-   Its main job is to read lines (queries) from a file and pass them to the standard pipeline (`syntacticParse` → `semanticParse` → `executeCommand`).

**Error Handling**

-   Checks if the file exists before executing.
-   Any syntactic or semantic error in the lines inside that file is handled the same way as if the user typed them directly.

## LOAD MATRIX

**Files of interest:**

-   `loadMatrix.cpp`
-   `matrix.cpp`
-   `matrix.h`
-   `matrixCatalogue.cpp`
-   `matrixCatalogue.h`
-   `matrixHelpers.cpp/h` (used indirectly)

**Logic**

-   **Command syntax**:
    ```plaintext
    LOAD MATRIX <matrixName>
    ```
-   **Implementation details**:
    1. **Syntactic Parsing** (`syntacticParseLOADMATRIX()`):
        - Expects exactly three tokens: `LOAD`, `MATRIX`, `<matrixName>`.
        - Sets `queryType = LOADMATRIX`, captures `<matrixName>`.
    2. **Semantic Parsing** (`semanticParseLOADMATRIX()`):
        - Checks that no matrix with that `<matrixName>` is already in memory.
        - Checks that a corresponding file `<matrixName>.csv` exists in `../data/`.
    3. **Execution** (`executeLOADMATRIX()`):
        - Constructs a `Matrix *matrix = new Matrix(matrixName)`.
        - Calls `matrix->load()`, which:
            1. **Determines dimension** via `determineMatrixDimension()`: reads lines in the CSV to find how many lines there are, meaning it’s an _n×n_ matrix with `n` = line count.
            2. **Splits (blockify)** the matrix row by row into “pages” in `../data/temp/`. This is similar to how the system handles tables, but using `matrix->maxRowsPerBlock` based on `BLOCK_SIZE` (converted to bytes) divided by `(sizeof(int)*dimension)`.
        - If successful, inserts the new matrix into `matrixCatalogue` (so future commands can reference it).
        - Prints out success message with the dimension.

**Page Design**

-   Each matrix row has `dimension` integers.
-   The code attempts to store as many full rows as possible in one “page file” in `../data/temp/<matrixName>_Page<i>`.
-   `Matrix::maxRowsPerBlock` is computed from the block size (1 KB _ BLOCK_SIZE) / (dimension _ 4 bytes). A fallback ensures at least 1 row if the dimension is large.

**Block Access**

-   The `bufferManager` writes each block (page) to `../data/temp/<matrixName>_Page<i>` when finishing that block.
-   Each block records how many rows (lines) ended up inside it, tracked in `rowsPerBlockCount[i]`.

**Error Handling**

-   If the CSV does not exist, or the matrix is already loaded, it prints a semantic error.
-   If a line in the CSV is malformed (i.e., doesn’t have enough columns), the code prints an error and aborts.

## PRINT MATRIX

**Files of interest:**

-   `printMatrix.cpp`
-   `matrix.cpp`
-   `matrix.h`
-   (plus the general buffer/page mechanism)

**Logic**

-   **Command syntax**:
    ```plaintext
    PRINT MATRIX <matrixName>
    ```
-   **Implementation details**:
    1. **Syntactic** (`syntacticParsePRINTMATRIX()`):
        - Checks for exactly three tokens: `PRINT`, `MATRIX`, `<matrixName>`.
        - Sets `parsedQuery.queryType = PRINTMATRIX` and so forth.
    2. **Semantic** (`semanticParsePRINTMATRIX()`):
        - Verifies that `<matrixName>` is indeed loaded in the `matrixCatalogue`.
    3. **Execution** (`executePRINTMATRIX()`):
        - Retrieves the matrix from `matrixCatalogue`.
        - Invokes `matrix->print()`.
            - This function reads each page from block 0 onward, up to the point we have read at most 20 total rows (the limit).
            - Each row is fetched via `BufferManager::getPage(...)` and `Page::getRow(r)`.
            - Prints only the first 20 columns if `dimension > 20` (though code also sets `limit` to 20, so we see up to 20×20 if dimension is bigger).
        - Outputs the matrix dimension.

**Page Design**

-   The matrix is stored as blocks of full rows.
-   The print routine loops over each block, loads it from the BufferManager, and prints rows row-by‐row until 20 have been printed or until the entire matrix is exhausted.

**Error Handling**

-   If the matrix does not exist, prints an error message.
-   If the matrix dimension is 0 or block reading fails, nothing is shown.

## EXPORT MATRIX

**Files of interest:**

-   `exportMatrix.cpp`
-   `matrix.cpp`
-   `matrix.h`

**Logic**

-   **Command syntax**:
    ```plaintext
    EXPORT MATRIX <matrixName>
    ```
-   **Implementation details**:
    1. **Syntactic** (`syntacticParseEXPORTMATRIX()`):
        - Expects exactly three tokens: `EXPORT`, `MATRIX`, `<matrixName>`.
        - Sets `queryType = EXPORTMATRIX` with the name captured.
    2. **Semantic** (`semanticParseEXPORTMATRIX()`):
        - Ensures that `<matrixName>` is in the `matrixCatalogue`.
    3. **Execution** (`executeEXPORTMATRIX()`):
        - Fetches the Matrix pointer from the catalogue.
        - Calls `matrix->makePermanent()`, which:
            - Creates (or overwrites) a new file `../data/<matrixName>.csv`.
            - Iterates over each block, reading the block’s rows from the buffer manager, and writes them out line by line (each line has `dimension` comma‐separated integers).
        - Prints a confirmation message.

**Page Design / Block Access**

-   Similar to printing, each block is read from `temp/`, then exported line by line to the final file.
-   This reconstitutes the full matrix in row‐major order.

**Error Handling**

-   Prints an error if the matrix does not exist or if the file can’t be opened for writing.

## ROTATE

**Files of interest**:

-   `rotate.cpp`
-   `matrixHelpers.cpp/h`
-   `matrix.cpp`

**Logic**

-   **Command syntax**:

    ```plaintext
    ROTATE <matrixName>
    ```

    Rotates a matrix 90 degrees _in place_ (clockwise).

-   **Implementation details**:
    1. **Syntactic** (`syntacticParseROTATEMATRIX()`):
        - Checks for two tokens: `ROTATE`, `<matrixName>`.
        - If valid, sets `queryType = ROTATEMATRIX`.
    2. **Semantic** (`semanticParseROTATEMATRIX()`):
        - Checks that `<matrixName>` is actually in `matrixCatalogue`.
    3. **Execution** (`executeROTATEMATRIX()`):
        - Gets a pointer to the matrix and obtains its dimension `n`.
        - Uses the standard “rotate matrix in layers” approach:
            - For each layer from outer to inner:
                - For each element in that layer, swaps the “top” with “left,” “left” with “bottom,” etc.
                - These reads/writes use `readMatrixElement()` and `writeMatrixElement()`, which operate on the block/page data in `temp/`.
        - Prints a confirmation message.

**Page Design / Block Access**

-   All reading and writing occurs cell by cell. For a cell at `(r, c)`, the code:
    -   Identifies which block the row `r` belongs to.
    -   Loads that block via `bufferManager.getPage(...)`.
    -   Accesses row `offsetInBlock = (r % maxRowsPerBlock)`.
    -   Finally accesses the `col`th element in that row.
-   Writes are done similarly, forcibly rewriting the relevant page.

**Error Handling**

-   If `<matrixName>` not found, or if dimension is zero, it prints a semantic error or does nothing.
-   If out‐of‐bound issues arise, they would typically indicate malformed data.

## CROSSTRANSPOSE

**Files of interest**:

-   `crossTranspose.cpp`
-   `matrixHelpers.cpp/h`
-   `matrix.cpp`

**Logic**

-   **Command syntax**:

    ```plaintext
    CROSSTRANSPOSE <matrixName1> <matrixName2>
    ```

    Steps:

    1. Transpose `<matrixName1>` in place.
    2. Transpose `<matrixName2>` in place.
    3. Swap the contents of `<matrixName1>` and `<matrixName2>` cell‐by‐cell.

-   **Implementation details**:
    1. **Syntactic** (`syntacticParseCROSSTRANSPOSE()`):
        - Requires exactly three tokens: `CROSSTRANSPOSE`, `<matrixName1>`, `<matrixName2>`.
        - Sets `queryType = CROSSTRANSPOSE`.
    2. **Semantic** (`semanticParseCROSSTRANSPOSE()`):
        - Checks both matrix names exist in `matrixCatalogue`.
        - Checks they have the same dimension. Otherwise prints an error.
    3. **Execution** (`executeCROSSTRANSPOSE()`):
        - Defines a helper `transposeMatrixInPlace(<matrixName>)` that:
            - Iterates over the upper triangular `(i < j)` region and swaps the `(i, j)` and `(j, i)` entries.
        - Calls `transposeMatrixInPlace(mat1)` and `transposeMatrixInPlace(mat2)`.
        - Then does a second pass over **all** cells `(i, j)` to swap the transposed contents of `mat1` with `mat2`.
        - Prints a message indicating the final result.

**Page / Block Access**

-   Just like `ROTATE`, each individual cell is fetched and updated via `readMatrixElement()` and `writeMatrixElement()`.
-   The swapping is done row by row, reading from the block in memory, rewriting in memory, etc.

**Error Handling**

-   If the two matrix dimensions differ, or if one is missing from the catalogue, a semantic error is triggered.

## CHECKANTISYM

**Files of interest**:

-   `checkAntiSym.cpp`
-   `matrixHelpers.cpp/h`
-   `matrix.cpp`

**Logic**

-   **Command syntax**:

    ```plaintext
    CHECKANTISYM <matrixName1> <matrixName2>
    ```

    The command checks whether `matrixName1` = −( `matrixName2` )^T. If true, prints `"True"`, else `"False"`.

-   **Implementation details**:
    1. **Syntactic** (`syntacticParseCHECKANTISYM()`):
        - Expects three tokens: `CHECKANTISYM`, `<mat1>`, `<mat2>`.
        - Sets `queryType = CHECKANTISYM`.
    2. **Semantic** (`semanticParseCHECKANTISYM()`):
        - Ensures both `<mat1>` and `<mat2>` exist and have the same dimension.
    3. **Execution** (`executeCHECKANTISYM()`):
        - Reads both matrices’ dimensions, checks them.
        - Loops `i` in `[0..n)` and `j` in `[0..n)`, verifying:
            ```cpp
            readMatrixElement(mat1, i, j) == - readMatrixElement(mat2, j, i)
            ```
            If a mismatch is found, sets a flag `isAntiSym = false`.
        - Prints `"True"` or `"False"` accordingly.

**Page Design / Block Access**

-   Similar approach to the other matrix commands, uses cell‐by‐cell reads.
-   No writes are done here.

**Error Handling**

-   If either matrix is missing or their dimensions differ, prints a semantic error.
-   If the data mismatch occurs, simply returns “False.”

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

# Assumptions

1. **Integer‐only data**: All matrix CSV files contain strictly integer data in n×n format.
2. **No partial columns**: Each row in the CSV is assumed to have exactly `n` columns, matching the dimension. Any mismatch causes an error.
3. **Block Size**: A `BLOCK_SIZE` set to 1 (and scaled to 1KB) is used as the basis for `maxRowsPerBlock`. If the matrix dimension is large, `maxRowsPerBlock` might become `1`.
4. **Page Files**: Matrix data is split row‐by‐row into `../data/temp/<matrixName>_Page<i>`. These are removed when the matrix is unloaded (e.g., upon program quit or if the user explicitly removes it).
5. **Dimension**: The code uses the line count of the CSV to define `n`. There is no separate check that the row length also equals `n`, beyond reading columns in each row line.

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
    - `<attribute1>` is the column to group by
    - `<Aggregate-Func1>` can be MAX, MIN, COUNT, SUM, or AVG
    - `<bin-op>` can be >, <, >=, <=, or ==
    - `<attribute-value>` is an integer constant
    - `<Aggregate-Func2>` can be MAX, MIN, COUNT, SUM, or AVG

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
Result <- GROUP BY DepartmentID FROM EMPLOYEE HAVING AVG(Salary) > 30000 RETURN MAX(Salary)
PRINT Result

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
