#include "index.h"
#include "global.h" // Access bufferManager, tableCatalogue, BLOCK_SIZE etc.
#include <iostream>
#include <vector>
#include <cmath>   // For ceil
#include <algorithm> // For lower_bound, sort, min, distance
#include <cstring> // For memcpy potentially if optimizing serialization
#include <climits> // For INT_MIN

//------------------------------------------------------------------------------
// BTreeNode Implementation
//------------------------------------------------------------------------------

// Constructor for creating a new node
BTreeNode::BTreeNode(int order, int leafOrder, bool leaf) :
    isLeaf(leaf),
    nextLeafPageIndex(-1),
    parentPageIndex(-1),
    pageIndex(-1), // Will be assigned when allocated
    keyCount(0)
{
    int keyCapacity = isLeaf ? leafOrder : order - 1;
    keys.reserve(keyCapacity);
    if (isLeaf) {
        recordPointers.reserve(leafOrder);
    } else {
        childrenPageIndices.reserve(order);
    }
}

// Constructor for loading an existing node from a page object
// TAKES Page* now
BTreeNode::BTreeNode(Page* page, int order, int leafOrder)
{
    // Deserialize directly from the Page object using getRow()
    deserialize(page, order, leafOrder);

    // Set the page index from the Page object's name (requires parsing page.pageName)
    if (page) { // Check if page is valid
        size_t lastUnderscore = page->pageName.rfind('_');
        size_t lastNode = page->pageName.rfind("Node"); // Find "Node" prefix
        if (lastNode != std::string::npos && (lastUnderscore == std::string::npos || lastNode > lastUnderscore)) { // Ensure "Node" is the suffix part
            try {
                // Extract substring after "Node" and convert to int
                pageIndex = std::stoi(page->pageName.substr(lastNode + 4));
            } catch (const std::exception& e) {
                logger.log("BTreeNode(Page*) - Error parsing pageIndex from pageName: " + page->pageName + " - " + e.what());
                pageIndex = -1; // Indicate error
            }
        } else {
            logger.log("BTreeNode(Page*) - Error: Could not find 'Node<ID>' suffix in pageName: " + page->pageName);
            pageIndex = -1;
        }
    } else {
        pageIndex = -1; // Invalid page pointer
    }
}

// --- Node Serialization / Deserialization ---
// Fills a rows vector suitable for BufferManager::writePage
void BTreeNode::serialize(std::vector<std::vector<int>>& pageData, int order, int leafOrder) {
    pageData.clear();
    // Use first row for metadata
    int metaSize = isLeaf ? METADATA_INTS_LEAF : METADATA_INTS_INTERNAL;
    std::vector<int> metadataRow(metaSize, 0); // Initialize with zeros
    metadataRow[IS_LEAF_OFFSET] = isLeaf ? 1 : 0;
    metadataRow[KEY_COUNT_OFFSET] = keyCount;
    metadataRow[PARENT_PAGE_INDEX_OFFSET] = parentPageIndex;
    if (isLeaf) {
        metadataRow[NEXT_LEAF_PAGE_INDEX_OFFSET] = nextLeafPageIndex;
    }
    pageData.push_back(metadataRow);

    // Store keys (Simple: one key per "row" entry for now)
    for (int key : keys) {
        pageData.push_back({key});
    }

    if (isLeaf) {
        // Store record pointers (pageIndex, rowIndex)
        for (const auto& rp : recordPointers) {
            pageData.push_back({rp.first, rp.second});
        }
    } else {
        // Store children page indices
        for (int childIdx : childrenPageIndices) {
            pageData.push_back({childIdx});
        }
    }
    // NOTE: BufferManager::writePage handles writing only the provided rowCount.
}

// Parses data from a Page object read by BufferManager::getPage
// TAKES Page* now
void BTreeNode::deserialize(Page* page, int order, int leafOrder) {
    if (!page) {
        logger.log("BTreeNode::deserialize - Error: Null page pointer provided.");
        isLeaf = false; keyCount = 0; parentPageIndex = -1; nextLeafPageIndex = -1;
        keys.clear(); recordPointers.clear(); childrenPageIndices.clear();
        return;
    }

    // Read metadata from the first row of the Page object
    std::vector<int> metadataRow = page->getRow(0);
    if (metadataRow.empty()) {
         logger.log("BTreeNode::deserialize - Error: Metadata row is empty. PageName: " + page->pageName);
         isLeaf = false; keyCount = 0; parentPageIndex = -1; nextLeafPageIndex = -1;
         keys.clear(); recordPointers.clear(); childrenPageIndices.clear();
         return;
    }

    // Determine expected size based on isLeaf flag *read from the data*
    bool potentialLeaf = (metadataRow[IS_LEAF_OFFSET] == 1);
    int expectedMetaSize = potentialLeaf ? METADATA_INTS_LEAF : METADATA_INTS_INTERNAL;

    if (metadataRow.size() < expectedMetaSize ) {
        logger.log("BTreeNode::deserialize - Error: Invalid page data format (metadata size mismatch). Expected >= " + std::to_string(expectedMetaSize) + ", got " + std::to_string(metadataRow.size()) + ". PageName: " + page->pageName);
        isLeaf = false; keyCount = 0; parentPageIndex = -1; nextLeafPageIndex = -1;
        keys.clear(); recordPointers.clear(); childrenPageIndices.clear();
        return;
    }

    isLeaf = potentialLeaf; // Now confirmed
    keyCount = metadataRow[KEY_COUNT_OFFSET];
    parentPageIndex = metadataRow[PARENT_PAGE_INDEX_OFFSET];
    if (isLeaf) {
        nextLeafPageIndex = metadataRow[NEXT_LEAF_PAGE_INDEX_OFFSET];
    } else {
        nextLeafPageIndex = -1; // Not used for internal nodes
    }

    // Clear existing data
    keys.clear();
    recordPointers.clear();
    childrenPageIndices.clear();

    // Read keys (one key per row starting from row 1)
    int currentRowIndex = 1;
    keys.reserve(keyCount);
    for (int i = 0; i < keyCount; ++i) {
        std::vector<int> keyRow = page->getRow(currentRowIndex);
        if (keyRow.empty()) {
             logger.log("BTreeNode::deserialize - Error: Invalid page data format (keys). Expected key at row " + std::to_string(currentRowIndex) + " PageName: " + page->pageName);
             keyCount = i; // Adjust count
             break;
        }
        keys.push_back(keyRow[0]);
        currentRowIndex++;
    }

    if (isLeaf) {
        // Read record pointers (pageIndex, rowIndex)
        recordPointers.reserve(keyCount); // One pointer per key in a leaf
        for (int i = 0; i < keyCount; ++i) {
            std::vector<int> pointerRow = page->getRow(currentRowIndex);
             if (pointerRow.empty() || pointerRow.size() < 2) {
                 logger.log("BTreeNode::deserialize - Error: Invalid page data format (record pointers). Expected {pg,row} at row " + std::to_string(currentRowIndex) + " PageName: " + page->pageName);
                 keyCount = i; // Adjust count
                 keys.resize(i); // Match keys vector size
                 break;
             }
            recordPointers.push_back({pointerRow[0], pointerRow[1]});
            currentRowIndex++;
        }
         // Ensure consistency if loop broke early
         if (keys.size() != recordPointers.size()) {
            logger.log("BTreeNode::deserialize - Warning: Key/Pointer count mismatch after reading leaf. PageName: " + page->pageName);
            keyCount = std::min((int)keys.size(), (int)recordPointers.size()); // Use int cast for comparison
            keys.resize(keyCount);
            recordPointers.resize(keyCount);
         }

    } else {
        // Read children page indices (keyCount + 1 pointers)
        childrenPageIndices.reserve(keyCount + 1);
        for (int i = 0; i <= keyCount; ++i) { // Note: <= keyCount
             std::vector<int> childRow = page->getRow(currentRowIndex);
             if (childRow.empty()) {
                 logger.log("BTreeNode::deserialize - Error: Invalid page data format (child pointers). Expected pointer at row " + std::to_string(currentRowIndex) + " PageName: " + page->pageName);
                 childrenPageIndices.clear(); keyCount = 0; keys.clear(); // Invalidate node state
                 break;
             }
            childrenPageIndices.push_back(childRow[0]);
            currentRowIndex++;
        }
        // Ensure consistency if loop broke early
        if (!childrenPageIndices.empty() && childrenPageIndices.size() != keyCount + 1) {
             logger.log("BTreeNode::deserialize - Warning: Key/Children count mismatch after reading internal node. PageName: " + page->pageName);
             keyCount = childrenPageIndices.size() - 1;
             if (keyCount < 0) keyCount = 0; // Ensure non-negative
             keys.resize(keyCount);
        }
    }
}

bool BTreeNode::isFull(int order, int leafOrder) const {
    if (isLeaf) {
        return keyCount >= leafOrder;
    } else {
        // Internal node is full if it has p-1 keys
        return keyCount >= order - 1;
    }
}

// CORRECTED VERSION from previous step
bool BTreeNode::isMinimal(int order, int leafOrder) const {
    if (parentPageIndex == -1) { // Is it the root?
        if (isLeaf) {
            // Root leaf is minimal even if empty (keyCount=0 is allowed initially)
            return true;
        } else {
            // Root internal node must have at least 1 key (=> 2 children)
            return keyCount >= 1;
        }
    } else { // Not the root
        int minKeys;
        if (isLeaf) {
            // Minimum leaf keys = ceil(Pleaf / 2)
            minKeys = std::ceil(static_cast<double>(leafOrder) / 2.0);
        } else {
            // Minimum internal keys = ceil(p / 2) - 1
            minKeys = std::ceil(static_cast<double>(order) / 2.0) - 1;
        }
        return keyCount >= minKeys;
    }
}

int BTreeNode::findKeyIndex(int key) const {
    auto it = std::lower_bound(keys.begin(), keys.end(), key);
    if (it != keys.end() && *it == key) {
        return std::distance(keys.begin(), it);
    }
    return -1; // Key not found
}

// For internal nodes: find pointer index for a key
int BTreeNode::findChildIndex(int key) const {
    if (isLeaf) return -1; // Only applicable for internal nodes
    // Find the first key *strictly greater* than the search key
    auto it = std::upper_bound(keys.begin(), keys.end(), key);
    // The pointer index is the same as the index of the iterator found
    return std::distance(keys.begin(), it);
}

void BTreeNode::insertLeafEntry(int key, RecordPointer pointer, int pos) {
    if (pos < 0 || pos > keyCount) return; // Basic bounds check
    keys.insert(keys.begin() + pos, key);
    recordPointers.insert(recordPointers.begin() + pos, pointer);
    keyCount++;
}

void BTreeNode::removeLeafEntry(int pos) {
    if (pos >= 0 && pos < keyCount) {
        keys.erase(keys.begin() + pos);
        recordPointers.erase(recordPointers.begin() + pos);
        keyCount--;
    }
}

void BTreeNode::insertInternalEntry(int key, int childPageIndex, int pos) {
     if (pos < 0 || pos > keyCount) return; // Basic bounds check
     keys.insert(keys.begin() + pos, key);
     // Child pointer goes *after* the key's position
     if (pos + 1 > childrenPageIndices.size()) { // Should not happen if called correctly
          logger.log("BTreeNode::insertInternalEntry - Error: Child pointer index out of bounds.");
          // Rollback key insertion? Or just log? Let's log and proceed carefully.
          childrenPageIndices.push_back(childPageIndex); // Append if missing? Risky.
     } else {
          childrenPageIndices.insert(childrenPageIndices.begin() + pos + 1, childPageIndex);
     }
     keyCount++;
}

// Removes key at index 'pos' and the child pointer *after* it (at index pos+1)
void BTreeNode::removeInternalEntry(int pos) {
     if (pos >= 0 && pos < keyCount) {
         keys.erase(keys.begin() + pos);
         // Remove the child pointer that followed the key
         if (pos + 1 < childrenPageIndices.size()) {
            childrenPageIndices.erase(childrenPageIndices.begin() + pos + 1);
         } else {
             logger.log("BTreeNode::removeInternalEntry - Warning: Attempting to remove key at end without corresponding child pointer. Pointer vector size: " + std::to_string(childrenPageIndices.size()));
             // This might indicate an issue elsewhere if the structure is invalid
         }
         keyCount--;
     }
}

void BTreeNode::printNode() const {
    std::cout << "Node Page: " << pageIndex << " (Parent: " << parentPageIndex << ") "
              << (isLeaf ? "[LEAF]" : "[INTERNAL]") << " Keys (" << keyCount << "): ";
    for(int k : keys) std::cout << k << " ";

    if(isLeaf) {
        std::cout << " DataPtrs: [";
         for(size_t i=0; i<recordPointers.size(); ++i) std::cout << "{" << recordPointers[i].first << "," << recordPointers[i].second << "}" << (i==recordPointers.size()-1?"":", ");
        std::cout << "] NextLeaf: " << nextLeafPageIndex;
    } else {
        std::cout << " ChildrenPtrs: ";
        for(int p : childrenPageIndices) std::cout << p << " ";
    }
    std::cout << std::endl;
}

//------------------------------------------------------------------------------
// BTree Implementation
//------------------------------------------------------------------------------

BTree::BTree(const std::string& tblName, const std::string& colName, int colIndex) :
    tableName(tblName),
    columnName(colName),
    columnIndex(colIndex),
    rootPageIndex(-1), // Initially empty tree
    nodeCount(0)
{
    indexName = tableName + "_" + columnName + "_index"; // Unique name for buffer manager
    const int pointerSize = sizeof(int);
    const int keySize = sizeof(int);
    const int recordPointerSize = sizeof(RecordPointer);
    const int metadataSize = BTreeNode::METADATA_INTS_LEAF * sizeof(int);
    const int effectiveBlockSize = BLOCK_SIZE * 1000 - metadataSize;
    order = floor((double)(effectiveBlockSize + keySize) / (pointerSize + keySize));
    if (order < 3) order = 3;
    leafOrder = floor((double)(effectiveBlockSize - pointerSize) / (keySize + recordPointerSize));
    if (leafOrder < 1) leafOrder = 1;
    logger.log("BTree::BTree - Calculated Order (p): " + std::to_string(order));
    logger.log("BTree::BTree - Calculated Leaf Order (Pleaf): " + std::to_string(leafOrder));
    // TODO: Load existing index metadata if it persists
}

BTree::~BTree() {
    // Destructor doesn't automatically drop files, needs explicit call if desired.
}

int BTree::allocateNewNodePage() {
    // Simple allocation: just increment count. Persistency needs more.
    return nodeCount++;
}

BTreeNode* BTree::fetchNode(int pageIndex) {
    if (pageIndex < 0) return nullptr;
    Page page = bufferManager.getPage(indexName, pageIndex);
    BTreeNode* node = new BTreeNode(&page, order, leafOrder);
    if (node->pageIndex < 0) { // Check if deserialization or page name parsing failed
         logger.log("BTree::fetchNode - Failed to properly deserialize node from page: " + page.pageName + " (Index: " + std::to_string(pageIndex) + ")");
         delete node;
         return nullptr;
    }
    return node;
}

void BTree::writeNode(BTreeNode* node) {
    if (!node || node->pageIndex < 0) return;
    std::vector<std::vector<int>> pageData;
    node->serialize(pageData, order, leafOrder);
    int simulatedRowCount = pageData.size();
    bufferManager.writePage(indexName, node->pageIndex, pageData, simulatedRowCount);
}

bool BTree::buildIndex(Table* table) {
    logger.log("BTree::buildIndex for table " + table->tableName + " on column " + columnName);
    if (!table) { logger.log("BTree::buildIndex - Error: Null table pointer provided."); return false; }
    dropIndex();
    Cursor cursor = table->getCursor();
    std::vector<int> row = cursor.getNext();
    long long rowsProcessed = 0;
    while (!row.empty()) {
         if (columnIndex < 0 || columnIndex >= row.size()) { // Check index validity
             logger.log("BTree::buildIndex - Error: Invalid column index " + std::to_string(columnIndex) + " for row size " + std::to_string(row.size()) + ". Skipping row.");
             row = cursor.getNext(); rowsProcessed++; continue;
         }
         int key = row[columnIndex];
         int currentPageIndex = cursor.pageIndex;
         int currentRowInPage = cursor.pagePointer - 1; // pagePointer is 1-based index of *next* row to read

         if (cursor.pagePointer == 0) { // Handle page boundary wrap-around
             if (currentPageIndex > 0) {
                 Table* currentTable = tableCatalogue.getTable(tableName);
                 if (currentTable && currentPageIndex - 1 < currentTable->rowsPerBlockCount.size()) {
                     int rowsInPrevPage = currentTable->rowsPerBlockCount[currentPageIndex - 1];
                     currentRowInPage = rowsInPrevPage - 1; // 0-based index
                     currentPageIndex = currentPageIndex - 1; // Point to the actual page it came from
                 } else { currentRowInPage = -1; } // Mark invalid if previous page info unavailable
             } else { currentRowInPage = -1; } // Mark invalid if first page wrap (shouldn't happen)
         }

         RecordPointer rp = {currentPageIndex, currentRowInPage};
         if (rp.first >= 0 && rp.second >= 0) {
             if (!insertKey(key, rp)) { logger.log("BTree::buildIndex - Failed to insert key: " + std::to_string(key) + " for row " + std::to_string(rowsProcessed)); }
         } else { logger.log("BTree::buildIndex - Skipping row " + std::to_string(rowsProcessed) + " due to invalid pointer calculation."); }

         row = cursor.getNext();
         rowsProcessed++;
         if (rowsProcessed % 5000 == 0 && rowsProcessed > 0) { logger.log("BTree::buildIndex - Processed " + std::to_string(rowsProcessed) + " rows..."); }
    }
    logger.log("BTree::buildIndex - Completed processing " + std::to_string(rowsProcessed) + " rows.");
    return true;
}

bool BTree::dropIndex() {
    logger.log("BTree::dropIndex - Dropping index: " + indexName);
    for (int i = 0; i < nodeCount; ++i) {
        std::string nodePageFileName = "../data/temp/" + indexName + "_Node" + std::to_string(i);
        bufferManager.deleteFile(nodePageFileName);
    }
    rootPageIndex = -1;
    nodeCount = 0;
    return true;
}

int BTree::findLeafNodePageIndex(int key, int currentRootPageIndex) {
     if (currentRootPageIndex < 0) { return -1; }
     int currentPageIndex = currentRootPageIndex;
     BTreeNode* node = fetchNode(currentPageIndex);
     while (node && !node->isLeaf) {
         int childPointerFollowIndex = node->findChildIndex(key);
         if (childPointerFollowIndex < 0 || childPointerFollowIndex >= node->childrenPageIndices.size()) {
             logger.log("BTree::findLeafNodePageIndex - Error: Invalid child pointer index " + std::to_string(childPointerFollowIndex) + " calculated in node " + std::to_string(node->pageIndex) + " for key " + std::to_string(key));
             delete node; return -1;
         }
         int nextPageIndex = node->childrenPageIndices[childPointerFollowIndex];
         currentPageIndex = nextPageIndex;
         delete node;
         node = fetchNode(nextPageIndex);
     }
      if (!node) { logger.log("BTree::findLeafNodePageIndex - Error: Failed to fetch leaf node at page index " + std::to_string(currentPageIndex)); return -1; }
     delete node;
     return currentPageIndex;
}

void BTree::startNewTree(int key, RecordPointer pointer) {
    rootPageIndex = allocateNewNodePage();
    BTreeNode* rootNode = new BTreeNode(order, leafOrder, /*isLeaf=*/true);
    rootNode->pageIndex = rootPageIndex;
    rootNode->parentPageIndex = -1;
    rootNode->insertLeafEntry(key, pointer, 0); // Use helper
    rootNode->nextLeafPageIndex = -1;
    writeNode(rootNode);
    delete rootNode;
    logger.log("BTree::startNewTree - Created new root (leaf) at page " + std::to_string(rootPageIndex));
}

void BTree::insertIntoLeaf(int leafPageIndex, int key, RecordPointer pointer) {
    BTreeNode* leaf = fetchNode(leafPageIndex);
    if (!leaf) { logger.log("BTree::insertIntoLeaf - Error: Could not fetch leaf node " + std::to_string(leafPageIndex)); return; }
    auto it = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);
    int insertPos = std::distance(leaf->keys.begin(), it);

    if (!leaf->isFull(order, leafOrder)) {
        leaf->insertLeafEntry(key, pointer, insertPos);
        writeNode(leaf);
    } else {
        std::vector<int> tempKeys = leaf->keys;
        std::vector<RecordPointer> tempPointers = leaf->recordPointers;
        tempKeys.insert(tempKeys.begin() + insertPos, key);
        tempPointers.insert(tempPointers.begin() + insertPos, pointer);
        int newRightNodePageIndex = allocateNewNodePage();
        BTreeNode* rightNode = new BTreeNode(order, leafOrder, /*isLeaf=*/true);
        rightNode->pageIndex = newRightNodePageIndex;
        rightNode->parentPageIndex = leaf->parentPageIndex;
        int midPoint = (leafOrder + 1) / 2;
        int splitKey = tempKeys[midPoint];
        rightNode->keys.assign(tempKeys.begin() + midPoint, tempKeys.end());
        rightNode->recordPointers.assign(tempPointers.begin() + midPoint, tempPointers.end());
        rightNode->keyCount = rightNode->keys.size();
        leaf->keys.assign(tempKeys.begin(), tempKeys.begin() + midPoint);
        leaf->recordPointers.assign(tempPointers.begin(), tempPointers.begin() + midPoint);
        leaf->keyCount = midPoint;
        rightNode->nextLeafPageIndex = leaf->nextLeafPageIndex;
        leaf->nextLeafPageIndex = newRightNodePageIndex;
        writeNode(leaf);
        writeNode(rightNode);
        insertIntoParent(leaf->pageIndex, splitKey, newRightNodePageIndex);
        delete rightNode;
    }
    delete leaf;
}

void BTree::insertIntoParent(int leftChildPageIndex, int key, int rightChildPageIndex) {
    BTreeNode* leftChild = fetchNode(leftChildPageIndex);
    if (!leftChild) { logger.log("BTree::insertIntoParent - Error: Failed fetch left child " + std::to_string(leftChildPageIndex)); return; }
    int parentPageIndex = leftChild->parentPageIndex;
    delete leftChild;

    if (parentPageIndex == -1) { // Create new root
        int newRootPageIndex = allocateNewNodePage();
        BTreeNode* newRoot = new BTreeNode(order, leafOrder, /*isLeaf=*/false);
        newRoot->pageIndex = newRootPageIndex;
        newRoot->parentPageIndex = -1;
        newRoot->insertInternalEntry(key, rightChildPageIndex, 0); // Inserts key[0] and child[1]
        newRoot->childrenPageIndices[0] = leftChildPageIndex; // Set first child explicitly
        writeNode(newRoot);
        BTreeNode* left = fetchNode(leftChildPageIndex);
        BTreeNode* right = fetchNode(rightChildPageIndex);
        if(left) { left->parentPageIndex = newRootPageIndex; writeNode(left); delete left; }
        if(right) { right->parentPageIndex = newRootPageIndex; writeNode(right); delete right; }
        rootPageIndex = newRootPageIndex;
        delete newRoot;
        logger.log("BTree::insertIntoParent - Created new root at page " + std::to_string(newRootPageIndex));
        return;
    }

    BTreeNode* parentNode = fetchNode(parentPageIndex);
    if (!parentNode) { logger.log("BTree::insertIntoParent - Error: Could not fetch parent node " + std::to_string(parentPageIndex)); return; }
    auto it = std::lower_bound(parentNode->keys.begin(), parentNode->keys.end(), key);
    int insertPos = std::distance(parentNode->keys.begin(), it);

    if (!parentNode->isFull(order, leafOrder)) { // Parent has space
        parentNode->insertInternalEntry(key, rightChildPageIndex, insertPos);
        writeNode(parentNode);
        BTreeNode* rightChild = fetchNode(rightChildPageIndex);
        if(rightChild) { rightChild->parentPageIndex = parentNode->pageIndex; writeNode(rightChild); delete rightChild; }
    } else { // Parent is full, split parent
        std::vector<int> tempKeys = parentNode->keys;
        std::vector<int> tempChildren = parentNode->childrenPageIndices;
        tempKeys.insert(tempKeys.begin() + insertPos, key);
        tempChildren.insert(tempChildren.begin() + insertPos + 1, rightChildPageIndex);
        int newParentRightPageIndex = allocateNewNodePage();
        BTreeNode* rightParentNode = new BTreeNode(order, leafOrder, /*isLeaf=*/false);
        rightParentNode->pageIndex = newParentRightPageIndex;
        rightParentNode->parentPageIndex = parentNode->parentPageIndex;
        int leftPointersCount = std::ceil(static_cast<double>(order + 1) / 2.0);
        int keyUpIndex = leftPointersCount - 1;
        int parentSplitKey = tempKeys[keyUpIndex];
        rightParentNode->keys.assign(tempKeys.begin() + keyUpIndex + 1, tempKeys.end());
        rightParentNode->childrenPageIndices.assign(tempChildren.begin() + leftPointersCount, tempChildren.end());
        rightParentNode->keyCount = rightParentNode->keys.size();
        parentNode->keys.assign(tempKeys.begin(), tempKeys.begin() + keyUpIndex);
        parentNode->childrenPageIndices.assign(tempChildren.begin(), tempChildren.begin() + leftPointersCount);
        parentNode->keyCount = parentNode->keys.size();
        writeNode(parentNode);
        writeNode(rightParentNode);
        for (int childIdx : rightParentNode->childrenPageIndices) { // Update parent pointers of moved children
            BTreeNode* child = fetchNode(childIdx);
            if (child) { child->parentPageIndex = rightParentNode->pageIndex; writeNode(child); delete child; }
        }
        delete rightParentNode;
        insertIntoParent(parentNode->pageIndex, parentSplitKey, newParentRightPageIndex); // Recursive call
    }
    delete parentNode;
}

// --- Stubs and implementations for splitLeafNode, splitInternalNode ---
void BTree::splitLeafNode(BTreeNode* leafNode, int& splitKey, int& newRightNodePageIndex) {
    // This logic is now integrated into insertIntoLeaf when node is full.
    // Keeping the signature might be useful for potential refactoring or direct calls.
    logger.log("BTree::splitLeafNode - Note: Logic is handled within insertIntoLeaf.");
    // The actual splitting happens there based on temporary vectors.
}

void BTree::splitInternalNode(BTreeNode* internalNode, int& splitKey, int& newRightNodePageIndex) {
     // This logic is now integrated into insertIntoParent when node is full.
    logger.log("BTree::splitInternalNode - Note: Logic is handled within insertIntoParent.");
}

// --- Deletion Implementation ---
bool BTree::deleteKey(int key) {
    logger.log("BTree::deleteKey - Attempting to delete key: " + std::to_string(key));
    if (rootPageIndex == -1) { logger.log("BTree::deleteKey - Tree is empty."); return false; }

    int leafPageIndex = findLeafNodePageIndex(key, rootPageIndex);
    if (leafPageIndex < 0) { logger.log("BTree::deleteKey - Key not found (leaf search failed)."); return false; }

    BTreeNode* leafNode = fetchNode(leafPageIndex);
    if (!leafNode) { logger.log("BTree::deleteKey - Error fetching leaf node " + std::to_string(leafPageIndex)); return false; }

    int initialKeyCount = leafNode->keyCount;
    int deletionCount = 0;

    int keyPos = leafNode->findKeyIndex(key);
    while (keyPos != -1) { // Remove all occurrences
        leafNode->removeLeafEntry(keyPos);
        deletionCount++;
        keyPos = leafNode->findKeyIndex(key); // Re-find after removal
    }

    if (deletionCount > 0) {
        logger.log("BTree::deleteKey - Removed " + std::to_string(deletionCount) + " instance(s) of key " + std::to_string(key) + " from leaf " + std::to_string(leafPageIndex));
        writeNode(leafNode);
        if (!leafNode->isMinimal(order, leafOrder) && leafNode->parentPageIndex != -1) {
             logger.log("BTree::deleteKey - Leaf node " + std::to_string(leafPageIndex) + " underflow detected. Handling...");
             handleUnderflow(leafPageIndex);
        }
        delete leafNode;
        adjustRoot(); // Check root AFTER potential underflow handling completes
        return true;
    } else {
        logger.log("BTree::deleteKey - Key " + std::to_string(key) + " not found in leaf node " + std::to_string(leafPageIndex));
        delete leafNode;
        return false;
    }
}

void BTree::handleUnderflow(int nodePageIndex) {
     // logger.log("BTree::handleUnderflow - Handling node " + std::to_string(nodePageIndex)); // Can be verbose
     BTreeNode* node = fetchNode(nodePageIndex);
     if (!node) return;
     if (node->parentPageIndex == -1) { delete node; return; } // Root handled by adjustRoot

     BTreeNode* parent = fetchNode(node->parentPageIndex);
     if (!parent) { delete node; return; }

     bool isRightSibling = false;
     int siblingPageIndex = findSiblingPageIndex(nodePageIndex, node->parentPageIndex, isRightSibling);

     if (siblingPageIndex == -1) {
          logger.log("BTree::handleUnderflow - No sibling found for node " + std::to_string(nodePageIndex) + " (Parent: " + std::to_string(node->parentPageIndex) + ")");
          delete node; delete parent; return; // Cannot borrow or merge
     }

     BTreeNode* sibling = fetchNode(siblingPageIndex);
      if (!sibling) {
          logger.log("BTree::handleUnderflow - Error fetching sibling node " + std::to_string(siblingPageIndex));
          delete node; delete parent; return;
     }

     // Calculate parentKeyIndex (index of key in parent separating node and sibling)
     int nodeIndexInParent = -1;
     for(size_t i = 0; i < parent->childrenPageIndices.size(); ++i) if(parent->childrenPageIndices[i] == nodePageIndex) nodeIndexInParent = i;
     if(nodeIndexInParent == -1) { logger.log("BTree::handleUnderflow - Node not found in parent."); delete node; delete parent; delete sibling; return; }
     int parentKeyIndex = isRightSibling ? nodeIndexInParent : (nodeIndexInParent - 1);
     if (parentKeyIndex < 0 || parentKeyIndex >= parent->keyCount) { logger.log("BTree::handleUnderflow - Invalid parent key index."); delete node; delete parent; delete sibling; return; }


     // Try to Borrow first
     int minKeys = node->isLeaf ? std::ceil(static_cast<double>(leafOrder) / 2.0) : (std::ceil(static_cast<double>(order) / 2.0) - 1);
     if (sibling->keyCount > minKeys) {
         // logger.log("BTree::handleUnderflow - Attempting to borrow from sibling " + std::to_string(siblingPageIndex)); // Verbose
         bool borrowed = false;
         if (node->isLeaf) borrowed = borrowFromLeafSibling(node, sibling, isRightSibling, parent);
         else borrowed = borrowFromInternalSibling(node, sibling, isRightSibling, parent); // STUBBED

         if (borrowed) {
             writeNode(node); writeNode(sibling); writeNode(parent);
             // logger.log("BTree::handleUnderflow - Borrow successful."); // Verbose
             delete node; delete sibling; delete parent; return;
         }
     }

     // Cannot borrow, must Merge
     // logger.log("BTree::handleUnderflow - Cannot borrow, attempting to merge with sibling " + std::to_string(siblingPageIndex)); // Verbose
     int pageToDelete = -1;
     if (isRightSibling) { // Merge right sibling into node
         if (node->isLeaf) mergeLeafNodes(node, sibling, parent, parentKeyIndex);
         else mergeInternalNodes(node, sibling, parent, parentKeyIndex); // STUBBED
         pageToDelete = sibling->pageIndex;
     } else { // Merge node into left sibling
         if (node->isLeaf) mergeLeafNodes(sibling, node, parent, parentKeyIndex);
         else mergeInternalNodes(sibling, node, parent, parentKeyIndex); // STUBBED
         pageToDelete = node->pageIndex;
     }

     // Delete the now-empty node's page file (sibling or node itself)
     if (pageToDelete != -1) {
         bufferManager.deleteFile(indexName, pageToDelete);
          logger.log("BTree::handleUnderflow - Deleted merged node page " + std::to_string(pageToDelete));
          // A more robust system might add this page index to a free list instead of deleting immediately
     } else {
          logger.log("BTree::handleUnderflow - Error: pageToDelete index was not set during merge.");
     }

     // Check parent for underflow recursively *after* cleaning up current level
     int parentIdxToRecurse = parent->pageIndex;
     delete node; delete sibling; delete parent; // Delete objects
     handleUnderflow(parentIdxToRecurse); // Recurse up the tree
}

int BTree::findSiblingPageIndex(int nodePageIndex, int parentPageIndex, bool& isRightSibling) {
    if (parentPageIndex < 0) return -1;
    BTreeNode* parent = fetchNode(parentPageIndex);
    if (!parent) return -1;
    int childIndex = -1;
    for (size_t i = 0; i < parent->childrenPageIndices.size(); ++i) { if (parent->childrenPageIndices[i] == nodePageIndex) { childIndex = i; break; } }
    int siblingPageIndex = -1;
    if (childIndex != -1) {
        if (childIndex + 1 < parent->childrenPageIndices.size()) { // Try right sibling first
            siblingPageIndex = parent->childrenPageIndices[childIndex + 1]; isRightSibling = true;
        } else if (childIndex > 0) { // Try left sibling
            siblingPageIndex = parent->childrenPageIndices[childIndex - 1]; isRightSibling = false;
        }
    } else { logger.log("BTree::findSiblingPageIndex - Error: node not found in parent."); }
    delete parent;
    return siblingPageIndex;
}

bool BTree::borrowFromLeafSibling(BTreeNode* node, BTreeNode* sibling, bool isRightSibling, BTreeNode* parent) {
     int keyToMove; RecordPointer pointerToMove;
     int parentKeyIndex;

      int nodeIndex = -1;
      for(size_t i=0; i<parent->childrenPageIndices.size(); ++i) if(parent->childrenPageIndices[i] == node->pageIndex) nodeIndex = i;
      if(nodeIndex == -1) { logger.log("BorrowLeaf: Node not in parent"); return false; } // Should not happen

     if (isRightSibling) { // Borrow first from right sibling
         if (sibling->keyCount <= 0) return false; // Cannot borrow if empty
         keyToMove = sibling->keys.front();
         pointerToMove = sibling->recordPointers.front();
         parentKeyIndex = nodeIndex; // Key separating node from right sibling is parent->keys[nodeIndex]

         node->insertLeafEntry(keyToMove, pointerToMove, node->keyCount); // Add to end of current node
         sibling->removeLeafEntry(0); // Remove from start of sibling
         if (parentKeyIndex < parent->keyCount) { // Update parent key
            parent->keys[parentKeyIndex] = sibling->keys.front(); // New separating key is sibling's new first key
         } else { logger.log("BorrowLeaf(Right): Invalid parent key index."); return false;}
     } else { // Borrow last from left sibling
         if (sibling->keyCount <= 0) return false;
         keyToMove = sibling->keys.back();
         pointerToMove = sibling->recordPointers.back();
         parentKeyIndex = nodeIndex - 1; // Key separating left sibling from node is parent->keys[nodeIndex - 1]

         node->insertLeafEntry(keyToMove, pointerToMove, 0); // Add to beginning of current node
         sibling->removeLeafEntry(sibling->keyCount - 1); // Remove from end of sibling
         if (parentKeyIndex >= 0) { // Update parent key
            parent->keys[parentKeyIndex] = node->keys.front(); // New separating key is node's new first key
         } else { logger.log("BorrowLeaf(Left): Invalid parent key index."); return false; }
     }
     return true;
}

void BTree::mergeLeafNodes(BTreeNode* leftNode, BTreeNode* rightNode, BTreeNode* parent, int parentKeyIndex) {
    // logger.log("BTree::mergeLeafNodes - Merging node " + std::to_string(rightNode->pageIndex) + " into " + std::to_string(leftNode->pageIndex)); // Verbose
    leftNode->keys.insert(leftNode->keys.end(), rightNode->keys.begin(), rightNode->keys.end());
    leftNode->recordPointers.insert(leftNode->recordPointers.end(), rightNode->recordPointers.begin(), rightNode->recordPointers.end());
    leftNode->keyCount = leftNode->keys.size(); // Update count based on vector size
    leftNode->nextLeafPageIndex = rightNode->nextLeafPageIndex;
    writeNode(leftNode);
    parent->removeInternalEntry(parentKeyIndex); // Removes key[parentKeyIndex] and child[parentKeyIndex+1]
    writeNode(parent);
    // Caller (handleUnderflow) deletes the rightNode's page file
}

void BTree::adjustRoot() {
    if (rootPageIndex < 0) return;
    BTreeNode* root = fetchNode(rootPageIndex);
    if (!root) return;
    if (!root->isLeaf && root->keyCount == 0) { // Internal root with no keys (only one child)
         logger.log("BTree::adjustRoot - Root node " + std::to_string(rootPageIndex) + " is internal and empty. Adjusting root.");
         int oldRootIndex = rootPageIndex;
         if (root->childrenPageIndices.empty()) { // Should not happen, but check
             logger.log("BTree::adjustRoot - Error: Empty internal root has no children!");
             rootPageIndex = -1; // Tree is effectively empty/corrupt
             nodeCount = 0;
         } else {
             rootPageIndex = root->childrenPageIndices[0]; // The single child becomes new root
             BTreeNode* newRoot = fetchNode(rootPageIndex);
             if (newRoot) { newRoot->parentPageIndex = -1; writeNode(newRoot); delete newRoot; }
             else { logger.log("BTree::adjustRoot - Error fetching new root node " + std::to_string(rootPageIndex)); rootPageIndex = -1; nodeCount = 0;} // Failed to update new root
         }
         bufferManager.deleteFile(indexName, oldRootIndex); // Delete the old root's page file
         if(rootPageIndex != -1) logger.log("BTree::adjustRoot - New root is now page " + std::to_string(rootPageIndex));
    } else if (root->isLeaf && root->keyCount == 0 && nodeCount > 1) { // Empty leaf root (but tree wasn't initially empty)
         logger.log("BTree::adjustRoot - Root node " + std::to_string(rootPageIndex) + " is leaf and empty. Tree is now empty.");
         bufferManager.deleteFile(indexName, rootPageIndex);
         rootPageIndex = -1; nodeCount = 0;
    } else if (root->isLeaf && root->keyCount == 0 && nodeCount <= 1) {
        // This is the valid state for an empty tree - root is an empty leaf. Do nothing.
        // logger.log("BTree::adjustRoot - Root is leaf and empty, tree is empty. No adjustment needed.");
    }
    delete root;
}

// --- Stubs for Internal Node Borrow/Merge ---
bool BTree::borrowFromInternalSibling(BTreeNode* node, BTreeNode* sibling, bool isRightSibling, BTreeNode* parent) {
    logger.log("BTree::borrowFromInternalSibling - Borrowing for internal node " + std::to_string(node->pageIndex) + " from sibling " + std::to_string(sibling->pageIndex));
    cout << "WARNING: Internal node borrowing not fully implemented." << endl;
    // TODO: Implement internal node borrowing
    return false; // Placeholder
}

void BTree::mergeInternalNodes(BTreeNode* leftNode, BTreeNode* rightNode, BTreeNode* parent, int parentKeyIndex) {
    logger.log("BTree::mergeInternalNodes - Merging internal node " + std::to_string(rightNode->pageIndex) + " into " + std::to_string(leftNode->pageIndex));
    cout << "WARNING: Internal node merging not fully implemented." << endl;
    // TODO: Implement internal node merging
    // Conceptual: Pull parent key down, move right keys/children to left, remove parent entry.
}

// --- Search Methods ---
std::vector<RecordPointer> BTree::searchKey(int key) {
    // logger.log("BTree::searchKey - Key: " + std::to_string(key)); // Verbose
    std::vector<RecordPointer> result;
    int leafPageIndex = findLeafNodePageIndex(key, rootPageIndex);
    if (leafPageIndex < 0) { return result; } // Not found or tree empty
    BTreeNode* leaf = fetchNode(leafPageIndex);
    if (!leaf) { logger.log("BTree::searchKey - Error: Could not fetch leaf node " + std::to_string(leafPageIndex)); return result; }

    auto it = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);
    int keyPos = std::distance(leaf->keys.begin(), it);

    while (keyPos < leaf->keyCount && leaf->keys[keyPos] == key) {
         if (keyPos < leaf->recordPointers.size()) { result.push_back(leaf->recordPointers[keyPos]); }
         else { logger.log("BTree::searchKey - Error: Data pointer index out of bounds in leaf " + std::to_string(leaf->pageIndex) + " at key index " + std::to_string(keyPos)); }
         keyPos++;
    }
    delete leaf;
    // if (!result.empty()) { logger.log("BTree::searchKey - Found " + std::to_string(result.size()) + " record(s) for key " + std::to_string(key)); } // Verbose
    return result;
}

std::vector<RecordPointer> BTree::searchRange(int startKey, int endKey) {
     logger.log("BTree::searchRange - Range: [" + std::to_string(startKey) + ", " + std::to_string(endKey) + "]");
    std::vector<RecordPointer> result;
    int currentLeafPageIndex = findLeafNodePageIndex(startKey, rootPageIndex);
    if (currentLeafPageIndex < 0) { logger.log("BTree::searchRange - Tree empty or range start not found."); return result; }

    BTreeNode* currentNode = fetchNode(currentLeafPageIndex);
    while (currentNode != nullptr && currentNode->isLeaf) {
        bool continueToNextNode = true;
        auto it = std::lower_bound(currentNode->keys.begin(), currentNode->keys.end(), startKey);
        int startPos = std::distance(currentNode->keys.begin(), it);

        for (int i = startPos; i < currentNode->keyCount; ++i) {
            int currentKey = currentNode->keys[i];
            if (currentKey <= endKey) {
                 if (i < currentNode->recordPointers.size()) { result.push_back(currentNode->recordPointers[i]); }
                 else { logger.log("BTree::searchRange - Error: Data pointer index out of bounds in leaf " + std::to_string(currentNode->pageIndex) + " at key index " + std::to_string(i)); }
            } else { continueToNextNode = false; break; } // Past the end key
        }
        if (!continueToNextNode) { delete currentNode; break; }

        int nextPageIndex = currentNode->nextLeafPageIndex;
        delete currentNode;
        currentNode = (nextPageIndex != -1) ? fetchNode(nextPageIndex) : nullptr;
        currentLeafPageIndex = nextPageIndex;
    }
     if (currentNode) delete currentNode; // Clean up if loop exited unexpectedly
    logger.log("BTree::searchRange - Found " + std::to_string(result.size()) + " entries.");
    return result;
}

bool BTree::insertKey(int key, RecordPointer recordPointer) {
    // logger.log("BTree::insertKey - Key: " + std::to_string(key)); // Reduced verbosity
    if (rootPageIndex == -1) {
        // Tree is empty, create the first node (root is also a leaf)
        startNewTree(key, recordPointer);
    } else {
        // Find the appropriate leaf node page index
        int leafPageIndex = findLeafNodePageIndex(key, rootPageIndex);
        if (leafPageIndex < 0) {
             logger.log("BTree::insertKey - Error: Could not find leaf node for key " + std::to_string(key));
            return false; // Should not happen if root exists
        }
        // Insert into the found leaf node (handles splits internally)
        insertIntoLeaf(leafPageIndex, key, recordPointer);
    }
    return true; // Assume success if no error thrown/returned earlier
}

// --- Debugging Methods ---
void BTree::printLeafChain() { /* ... implementation from previous step ... */ }
void BTree::printSubtree(int pageIndex, int level) { /* ... implementation from previous step ... */ }
void BTree::printTree() { /* ... implementation from previous step ... */ }