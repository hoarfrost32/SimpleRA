#ifndef INDEX_H
#define INDEX_H

#pragma once

#include "bufferManager.h"
#include "page.h" // Need Page for reading table data during build and node deserialization
#include "table.h" // Need Table for metadata
#include <vector>
#include <string>
#include <optional> // For optional return values
#include <cmath>    // For ceil
#include <algorithm> // For lower_bound etc.

// Define a structure for data pointers in leaf nodes
// pageIndex: The index of the page file in the TABLE's storage
// rowIndex: The index of the row within that page
using RecordPointer = std::pair<int, int>; // {pageIndex, rowIndex}

/**
 * @brief Represents a node in the B+ Tree.
 * Each node corresponds to one page in the buffer manager.
 */
class BTreeNode {
public:
    bool isLeaf;
    std::vector<int> keys;
    std::vector<int> childrenPageIndices; // Page indices of children (for internal nodes)
    std::vector<RecordPointer> recordPointers; // Data pointers (for leaf nodes)
    int nextLeafPageIndex; // Page index of the next leaf node (-1 if none)
    int parentPageIndex; // Page index of the parent node (-1 if root)
    int pageIndex;       // Page index of this node itself
    int keyCount;        // Number of keys currently in the node

    // --- Node Metadata stored within the page ---
    // Offsets assume metadata is stored as the first "row" in the page's data vector
    static const int IS_LEAF_OFFSET = 0;
    static const int KEY_COUNT_OFFSET = 1;
    static const int PARENT_PAGE_INDEX_OFFSET = 2;
    static const int NEXT_LEAF_PAGE_INDEX_OFFSET = 3; // Only used if isLeaf = true
    // Determine METADATA_INTS based on the maximum needed (leaf nodes need one more)
    static const int METADATA_INTS_INTERNAL = 3; // isLeaf, keyCount, parentPageIndex
    static const int METADATA_INTS_LEAF = 4;     // isLeaf, keyCount, parentPageIndex, nextLeafPageIndex


    // Constructor for creating a new node
    BTreeNode(int order, int leafOrder, bool leaf = false);
    // Constructor for loading an existing node from a page object
    // TAKES Page* now
    BTreeNode(Page* page, int order, int leafOrder);

    // --- Serialization / Deserialization ---
    // Fills a rows vector suitable for BufferManager::writePage
    void serialize(std::vector<std::vector<int>>& pageData, int order, int leafOrder);
    // Parses data from a Page object read by BufferManager::getPage
    // TAKES Page* now
    void deserialize(Page* page, int order, int leafOrder);

    // --- Node Operations ---
    bool isFull(int order, int leafOrder) const;
    bool isMinimal(int order, int leafOrder) const;
    int findKeyIndex(int key) const; // Helper to find exact key index
    int findChildIndex(int key) const; // For internal nodes: find pointer index for a key

    // Helper methods for insertion/deletion within node
    void insertLeafEntry(int key, RecordPointer pointer, int pos);
    void removeLeafEntry(int pos);
    void insertInternalEntry(int key, int childPageIndex, int pos);
    void removeInternalEntry(int pos); // Removes key[pos] and child[pos+1]

    // Debugging helper
    void printNode() const;
};


/**
 * @brief The B+ Tree index implementation.
 */
class BTree {
private:
    std::string indexName; // Unique name, e.g., <tableName>_<columnName>_index
    std::string tableName;
    std::string columnName;
    int columnIndex;
    int rootPageIndex;
    int nodeCount; // Tracks the total number of nodes (used for allocating new page indices)
    int order; // Max pointers in internal node (p)
    int leafOrder; // Max record pointers in leaf node (Pleaf)

    // --- Helper Methods ---
    BTreeNode* fetchNode(int pageIndex); // Reads node page from buffer manager - NOW PRIVATE
    void writeNode(BTreeNode* node); // Writes node page back to buffer manager - NOW PRIVATE
    int allocateNewNodePage(); // Gets the next available page index for a new node - NOW PRIVATE

    // Recursive search to find the leaf node for a given key - NOW PRIVATE
    int findLeafNodePageIndex(int key, int currentRootPageIndex);

    // Insertion helpers - NOW PRIVATE
    void startNewTree(int key, RecordPointer pointer);
    void insertIntoLeaf(int leafPageIndex, int key, RecordPointer pointer);
    void insertIntoParent(int leftChildPageIndex, int key, int rightChildPageIndex);

    // Splitting helpers - NOW PRIVATE
    void splitLeafNode(BTreeNode* leafNode, int& splitKey, int& newRightNodePageIndex);
    void splitInternalNode(BTreeNode* internalNode, int& splitKey, int& newRightNodePageIndex);

    // --- Deletion Helpers ---
    // Handles underflow after deletion - NOW PRIVATE
    void handleUnderflow(int nodePageIndex);
    // Borrowing operations - NOW PRIVATE
    bool borrowFromLeafSibling(BTreeNode* node, BTreeNode* sibling, bool isRightSibling, BTreeNode* parent);
    bool borrowFromInternalSibling(BTreeNode* node, BTreeNode* sibling, bool isRightSibling, BTreeNode* parent);
    // Merging operations - NOW PRIVATE
    void mergeLeafNodes(BTreeNode* leftNode, BTreeNode* rightNode, BTreeNode* parent, int parentKeyIndex);
    void mergeInternalNodes(BTreeNode* leftNode, BTreeNode* rightNode, BTreeNode* parent, int parentKeyIndex);
    // Find sibling page index - NOW PRIVATE
    int findSiblingPageIndex(int nodePageIndex, int parentPageIndex, bool& isRightSibling);
    // Adjust root if needed - NOW PRIVATE
    void adjustRoot();

    // Recursive print helper - NOW PRIVATE
    void printSubtree(int pageIndex, int level);


public:
    // Constructor: Creates or loads a B+ tree index
    BTree(const std::string& tableName, const std::string& columnName, int columnIndex);
    ~BTree(); // Destructor

    // --- Core Index Operations ---

    // Build the index from scratch based on the table data
    bool buildIndex(Table* table);

    // Remove the index and its associated files
    bool dropIndex();

    // Insert a key-value pair (key, {pageIndex, rowIndex})
    bool insertKey(int key, RecordPointer recordPointer);

    // Delete *all* entries matching the key. Returns true if any deletion occurred.
    bool deleteKey(int key);

    // Search for a specific key, returns vector of record pointers
    std::vector<RecordPointer> searchKey(int key);

    // Search for keys within a range [startKey, endKey], returns vector of record pointers
    std::vector<RecordPointer> searchRange(int startKey, int endKey);

    // --- Getters ---
    int getRootPageIndex() const { return rootPageIndex; }
    int getOrder() const { return order; }
    int getLeafOrder() const { return leafOrder; }
    std::string getIndexName() const { return indexName; }

    // Debugging
    void printTree(); // Helper to print the tree structure (optional) - NOW PUBLIC
    void printLeafChain(); // Helper to print the linked list of leaves - NOW PUBLIC
};


#endif // INDEX_H