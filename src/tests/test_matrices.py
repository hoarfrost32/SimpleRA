#!/usr/bin/env python3
"""
A single comprehensive script for testing SimpleRA with NxN matrices, including
some smaller and larger ones. Commands tested:
    - SOURCE
    - LOAD MATRIX
    - PRINT MATRIX
    - EXPORT MATRIX
    - ROTATE
    - CROSSTRANSPOSE
    - CHECKANTISYM

We handle:
1) 1x1, 2x2, 3x3, 5x5, and (larger) 20x20 square matrices.
2) Dimension mismatches (e.g. 20x20 vs 21x21).
3) Ensuring that we LOAD each matrix before referencing it.
4) Avoiding lines starting with "#" so we don't trigger SYNTAX ERROR.

Usage:
  cd src
  chmod +x test_matrices.py
  ./test_matrices.py
"""

import os
import random
import subprocess
import sys

DATA_DIR = "../data"
RA_TEST_SCRIPT = "matrix_tests"  # We'll create ../data/matrix_tests.ra
LARGE_N = 20                     # Adjust this if you want even larger tests (e.g., 100, 1000, etc.)

def generate_matrix_csv(file_path, matrix_data):
    """Writes matrix_data (list of lists) to file_path in CSV format."""
    with open(file_path, "w") as f:
        for row in matrix_data:
            f.write(",".join(str(x) for x in row) + "\n")


def main():
    print("=== SimpleRA Square-Matrix Commands Test Script (with Large Tests) ===")

    # -------------------------------------------------------------------------
    # 1) Generate NxN matrices in ../data/
    # -------------------------------------------------------------------------
    print("[INFO] Generating CSV files in", DATA_DIR)
    os.makedirs(DATA_DIR, exist_ok=True)

    # M1: 3x3
    M1 = [
        [0,  1,  2],
        [3,  4,  5],
        [6,  7,  8]
    ]
    generate_matrix_csv(f"{DATA_DIR}/M1.csv", M1)
    print("[OK] Created M1.csv (3×3)")

    # M2: 3x3
    M2 = [
        [ 0, 10, 20],
        [30, 40, 50],
        [60, 70, 80]
    ]
    generate_matrix_csv(f"{DATA_DIR}/M2.csv", M2)
    print("[OK] Created M2.csv (3×3)")

    # M3: 1x1
    M3 = [[0]]
    generate_matrix_csv(f"{DATA_DIR}/M3.csv", M3)
    print("[OK] Created M3.csv (1×1)")

    # M4: 1x1
    M4 = [[1]]
    generate_matrix_csv(f"{DATA_DIR}/M4.csv", M4)
    print("[OK] Created M4.csv (1×1)")

    # M5: 2×2
    M5 = [[1, 2], [3, 4]]
    generate_matrix_csv(f"{DATA_DIR}/M5.csv", M5)
    print("[OK] Created M5.csv (2×2)")

    # M6: 2×2 => M5 = - (M6^T) => checkAntiSym => True
    M6 = [[-1, -3], [-2, -4]]
    generate_matrix_csv(f"{DATA_DIR}/M6.csv", M6)
    print("[OK] Created M6.csv (2×2)")

    # M7: 2×2
    M7 = [[9, 10], [11, 12]]
    generate_matrix_csv(f"{DATA_DIR}/M7.csv", M7)
    print("[OK] Created M7.csv (2×2)")

    # M8: 3×3 => mismatch with M7
    M8 = [
        [100, 200, 300],
        [400, 500, 600],
        [700, 800, 900]
    ]
    generate_matrix_csv(f"{DATA_DIR}/M8.csv", M8)
    print("[OK] Created M8.csv (3×3)")

    # M9: 5×5 random
    random.seed(42)
    M9 = [[random.randint(-5, 5) for _ in range(5)] for __ in range(5)]
    generate_matrix_csv(f"{DATA_DIR}/M9.csv", M9)
    print("[OK] Created M9.csv (5×5)")

    # M10, M11: 20×20 (larger test)
    # M12: 21×21 => mismatch with M10, M11
    # We do random values in [-10, 10] here.
    M10 = []
    for _ in range(LARGE_N):
        row = [random.randint(-10, 10) for __ in range(LARGE_N)]
        M10.append(row)
    generate_matrix_csv(f"{DATA_DIR}/M10.csv", M10)
    print(f"[OK] Created M10.csv ({LARGE_N}×{LARGE_N})")

    M11 = []
    for _ in range(LARGE_N):
        row = [random.randint(-10, 10) for __ in range(LARGE_N)]
        M11.append(row)
    generate_matrix_csv(f"{DATA_DIR}/M11.csv", M11)
    print(f"[OK] Created M11.csv ({LARGE_N}×{LARGE_N})")

    M12_dim = LARGE_N + 1  # e.g. 21 if LARGE_N=20 => mismatch
    M12 = []
    for _ in range(M12_dim):
        row = [random.randint(-10, 10) for __ in range(M12_dim)]
        M12.append(row)
    generate_matrix_csv(f"{DATA_DIR}/M12.csv", M12)
    print(f"[OK] Created M12.csv ({M12_dim}×{M12_dim})")

    # -------------------------------------------------------------------------
    # 2) Create the .ra script (no lines start with '#') and ensure we LOAD
    #    each matrix before referencing it, so we get correct dimension checks.
    # -------------------------------------------------------------------------
    script_path = f"{DATA_DIR}/{RA_TEST_SCRIPT}.ra"
    print("[INFO] Creating RA script =>", script_path)

    with open(script_path, "w") as f:
        # ---- M1 Tests ----
        f.write("LOAD MATRIX M1\n")          # 1
        f.write("PRINT MATRIX M1\n")         # 2
        f.write("EXPORT MATRIX M1\n")        # 3
        f.write("ROTATE M1\n")               # 4
        f.write("PRINT MATRIX M1\n\n")       # 5

        # ---- M2 + crossTranspose with M1 ----
        f.write("LOAD MATRIX M2\n")          # 6
        f.write("CROSSTRANSPOSE M1 M2\n")    # 7
        f.write("PRINT MATRIX M1\n")         # 8
        f.write("PRINT MATRIX M2\n\n")       # 9

        # ---- Load M7, M8 then mismatch crosstranspose
        f.write("LOAD MATRIX M7\n")          # 10
        f.write("LOAD MATRIX M8\n")          # 11
        f.write("CROSSTRANSPOSE M7 M8\n\n")  # 12 => mismatch

        # ---- M5, M6 => checkAntiSym => True
        f.write("LOAD MATRIX M5\n")          # 13
        f.write("LOAD MATRIX M6\n")          # 14
        f.write("CHECKANTISYM M5 M6\n\n")    # 15 => True

        # ---- M7, M8 => checkAntiSym => mismatch
        f.write("CHECKANTISYM M7 M8\n\n")    # 16 => mismatch

        # ---- M3, M4 => checkAntiSym => False
        f.write("LOAD MATRIX M3\n")          # 17
        f.write("LOAD MATRIX M4\n")          # 18
        f.write("CHECKANTISYM M3 M4\n\n")    # 19 => false

        f.write("ROTATE M3\n")               # 20
        f.write("PRINT MATRIX M3\n\n")       # 21

        # ---- M9 => bigger 5×5
        f.write("LOAD MATRIX M9\n")          # 22
        f.write("PRINT MATRIX M9\n")         # 23
        f.write("EXPORT MATRIX M9\n\n")      # 24

        # ---- Non-existent => error
        f.write("LOAD MATRIX M99\n")         # 25

        # ---- Larger NxN tests: M10, M11, M12
        f.write("LOAD MATRIX M10\n")         # 26
        f.write("PRINT MATRIX M10\n")        # 27 (will show partial)
        f.write("ROTATE M10\n")              # 28
        f.write("PRINT MATRIX M10\n")        # 29
        f.write("EXPORT MATRIX M10\n\n")     # 30

        f.write("LOAD MATRIX M11\n")         # 31
        f.write("CROSSTRANSPOSE M10 M11\n")  # 32 => both 20×20 => success
        f.write("PRINT MATRIX M10\n")        # 33
        f.write("PRINT MATRIX M11\n\n")      # 34
        f.write("CHECKANTISYM M10 M11\n\n")  # 35 => likely "False"

        f.write("LOAD MATRIX M12\n")         # 36 => 21×21
        f.write("CROSSTRANSPOSE M10 M12\n\n")# 37 => mismatch
        f.write("CHECKANTISYM M10 M12\n")    # 38 => mismatch

    # -------------------------------------------------------------------------
    # 3) We define test_scenarios consistent with the new .ra script
    # -------------------------------------------------------------------------
    test_scenarios = [
        # 1
        {
            "name": "LOAD MATRIX M1",
            "expect": "success",
            "pattern_success": "Loaded Matrix. Dimensions: 3 x 3",
        },
        # 2
        {
            "name": "PRINT MATRIX M1",
            "expect": "success",
            "pattern_success": "Matrix dimension: 3 x 3",
        },
        # 3
        {
            "name": "EXPORT MATRIX M1",
            "expect": "success",
            "pattern_success": "Exported matrix M1 to file: M1.csv",
        },
        # 4
        {
            "name": "ROTATE M1",
            "expect": "success",
            "pattern_success": "Matrix M1 rotated 90 degrees",
        },
        # 5
        {
            "name": "PRINT MATRIX M1 (rotated)",
            "expect": "success",
            "pattern_success": "Matrix dimension: 3 x 3",
        },
        # 6
        {
            "name": "LOAD MATRIX M2",
            "expect": "success",
            "pattern_success": "Loaded Matrix. Dimensions: 3 x 3",
        },
        # 7
        {
            "name": "CROSSTRANSPOSE M1 M2",
            "expect": "success",
            "pattern_success": "CROSSTRANSPOSE done",
        },
        # 8
        {
            "name": "PRINT MATRIX M1 (after crosstranspose)",
            "expect": "success",
            "pattern_success": "Matrix dimension: 3 x 3",
        },
        # 9
        {
            "name": "PRINT MATRIX M2 (after crosstranspose)",
            "expect": "success",
            "pattern_success": "Matrix dimension: 3 x 3",
        },
        # 10
        {
            "name": "LOAD MATRIX M7",
            "expect": "success",
            "pattern_success": "Loaded Matrix. Dimensions: 2 x 2",
        },
        # 11
        {
            "name": "LOAD MATRIX M8",
            "expect": "success",
            "pattern_success": "Loaded Matrix. Dimensions: 3 x 3",
        },
        # 12
        {
            "name": "CROSSTRANSPOSE M7 M8 => mismatch",
            "expect": "error",
            "pattern_error": "SEMANTIC ERROR: Matrices must have the same dimensions",
        },
        # 13
        {
            "name": "LOAD MATRIX M5",
            "expect": "success",
            "pattern_success": "Loaded Matrix. Dimensions: 2 x 2",
        },
        # 14
        {
            "name": "LOAD MATRIX M6",
            "expect": "success",
            "pattern_success": "Loaded Matrix. Dimensions: 2 x 2",
        },
        # 15
        {
            "name": "CHECKANTISYM M5 M6 => True",
            "expect": "success",
            "pattern_success": "True",
        },
        # 16
        {
            "name": "CHECKANTISYM M7 M8 => mismatch",
            "expect": "error",
            "pattern_error": "SEMANTIC ERROR: Matrices have different dimensions",
        },
        # 17
        {
            "name": "LOAD MATRIX M3",
            "expect": "success",
            "pattern_success": "Loaded Matrix. Dimensions: 1 x 1",
        },
        # 18
        {
            "name": "LOAD MATRIX M4",
            "expect": "success",
            "pattern_success": "Loaded Matrix. Dimensions: 1 x 1",
        },
        # 19
        {
            "name": "CHECKANTISYM M3 M4 => False",
            "expect": "success",
            "pattern_success": "False",
        },
        # 20
        {
            "name": "ROTATE M3 (single element)",
            "expect": "success",
            "pattern_success": "Matrix M3 rotated 90 degrees",
        },
        # 21
        {
            "name": "PRINT MATRIX M3",
            "expect": "success",
            "pattern_success": "Matrix dimension: 1 x 1",
        },
        # 22
        {
            "name": "LOAD MATRIX M9",
            "expect": "success",
            "pattern_success": "Loaded Matrix. Dimensions: 5 x 5",
        },
        # 23
        {
            "name": "PRINT MATRIX M9",
            "expect": "success",
            "pattern_success": "Matrix dimension: 5 x 5",
        },
        # 24
        {
            "name": "EXPORT MATRIX M9",
            "expect": "success",
            "pattern_success": "Exported matrix M9 to file: M9.csv",
        },
        # 25
        {
            "name": "LOAD MATRIX M99 => error",
            "expect": "error",
            "pattern_error": "SEMANTIC ERROR: File doesn't exist",
        },
        # 26
        {
            "name": "LOAD MATRIX M10 (20x20)",
            "expect": "success",
            "pattern_success": "Loaded Matrix. Dimensions: 20 x 20",
        },
        # 27
        {
            "name": "PRINT MATRIX M10",
            "expect": "success",
            "pattern_success": "Matrix dimension: 20 x 20",
        },
        # 28
        {
            "name": "ROTATE M10",
            "expect": "success",
            "pattern_success": "Matrix M10 rotated 90 degrees",
        },
        # 29
        {
            "name": "PRINT MATRIX M10 (rotated)",
            "expect": "success",
            "pattern_success": "Matrix dimension: 20 x 20",
        },
        # 30
        {
            "name": "EXPORT MATRIX M10",
            "expect": "success",
            "pattern_success": "Exported matrix M10 to file: M10.csv",
        },
        # 31
        {
            "name": "LOAD MATRIX M11 (20x20)",
            "expect": "success",
            "pattern_success": "Loaded Matrix. Dimensions: 20 x 20",
        },
        # 32
        {
            "name": "CROSSTRANSPOSE M10 M11 => both 20x20 => success",
            "expect": "success",
            "pattern_success": "CROSSTRANSPOSE done",
        },
        # 33
        {
            "name": "PRINT MATRIX M10 (post crosstranspose)",
            "expect": "success",
            "pattern_success": "Matrix dimension: 20 x 20",
        },
        # 34
        {
            "name": "PRINT MATRIX M11 (post crosstranspose)",
            "expect": "success",
            "pattern_success": "Matrix dimension: 20 x 20",
        },
        # 35
        {
            "name": "CHECKANTISYM M10 M11 => likely False",
            "expect": "success",
            "pattern_success": "False",  
            # or "True" if random chance leads to antisym, but 99.999% it won't.
            # We'll assume "False" will appear. If it doesn't, the script will fail.
        },
        # 36
        {
            "name": "LOAD MATRIX M12 (21x21)",
            "expect": "success",
            "pattern_success": "Loaded Matrix. Dimensions: 21 x 21",
        },
        # 37
        {
            "name": "CROSSTRANSPOSE M10 M12 => mismatch",
            "expect": "error",
            "pattern_error": "SEMANTIC ERROR: Matrices must have the same dimensions",
        },
        # 38
        {
            "name": "CHECKANTISYM M10 M12 => mismatch",
            "expect": "error",
            "pattern_error": "SEMANTIC ERROR: Matrices have different dimensions",
        },
    ]

    results = [{"name": t["name"], "status": "NOT_RUN"} for t in test_scenarios]
    current_test_index = 0

    # -------------------------------------------------------------------------
    # 4) Run "SOURCE matrix_tests" in the server, capture output
    # -------------------------------------------------------------------------
    print("[INFO] Running SimpleRA server with: SOURCE", RA_TEST_SCRIPT)
    cmd_input = f"SOURCE {RA_TEST_SCRIPT}\nQUIT\n"

    try:
        proc = subprocess.run(
            ["./server"],
            input=cmd_input.encode("utf-8"),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False
        )
        out = proc.stdout.decode("utf-8", errors="replace")
        err = proc.stderr.decode("utf-8", errors="replace")

        print("=== SERVER OUTPUT START ===")
        print(out)
        print("=== SERVER OUTPUT END   ===")

        if err.strip():
            print("=== SERVER ERRORS/WARNINGS ===")
            print(err)
            print("================================")

        # ---------------------------------------------------------------------
        # 5) Parse server output for pass/fail
        # ---------------------------------------------------------------------
        lines = out.splitlines()
        for line in lines:
            if current_test_index >= len(test_scenarios):
                break
            test = test_scenarios[current_test_index]
            expect = test["expect"]
            line_lc = line.lower()

            if expect == "success":
                if test["pattern_success"].lower() in line_lc:
                    results[current_test_index]["status"] = "PASSED"
                    current_test_index += 1
                elif "semantic error" in line_lc or "syntax error" in line_lc:
                    results[current_test_index]["status"] = "FAILED (got error instead of success)"
                    current_test_index += 1

            else:  # expect == "error"
                if test["pattern_error"].lower() in line_lc:
                    results[current_test_index]["status"] = "PASSED"
                    current_test_index += 1
                elif any(s in line_lc for s in [
                        "loaded matrix", "exported matrix", "crosstranspose done",
                        "rotated 90 degrees", "true", "false"]):
                    # If we see success-like strings, it's a fail
                    results[current_test_index]["status"] = "FAILED (got success instead of error)"
                    current_test_index += 1

        # Mark leftover tests as failed
        for i in range(current_test_index, len(test_scenarios)):
            results[i]["status"] = "FAILED (pattern not found)"

    except FileNotFoundError:
        print("[ERROR] Could not find ./server binary. Make sure you compiled it (via `make`).")
        print("Exiting without further cleanup.")
        return

    # -------------------------------------------------------------------------
    # 6) Cleanup
    # -------------------------------------------------------------------------
    print("[INFO] Cleaning up generated CSV and RA files.")
    to_remove = [
        "M1.csv", "M2.csv", "M3.csv", "M4.csv",
        "M5.csv", "M6.csv", "M7.csv", "M8.csv", "M9.csv",
        "M10.csv", "M11.csv", "M12.csv",
        f"{RA_TEST_SCRIPT}.ra"
    ]
    for fname in to_remove:
        path = os.path.join(DATA_DIR, fname)
        if os.path.exists(path):
            try:
                os.remove(path)
            except OSError as e:
                print(f"[WARN] Could not remove {path}: {e}")

    # -------------------------------------------------------------------------
    # 7) Final Report
    # -------------------------------------------------------------------------
    print("\n=== Test Case Final Report (with Large NxN) ===")
    width = max(len(t["name"]) for t in results)
    for t in results:
        print(f"{t['name'].ljust(width)}  =>  {t['status']}")
    print("=== End of Report ===\n")

    print("[OK] Test script finished!")


if __name__ == "__main__":
    main()

