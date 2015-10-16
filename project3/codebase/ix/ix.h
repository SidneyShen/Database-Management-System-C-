#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

class IX_ScanIterator;
class IXFileHandle;

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file
        RC createFile(const string &fileName);

        // Delete an index file
        RC destroyFile(const string &fileName);

        // Open an index and return a file handle
        RC openFile(const string &fileName, IXFileHandle &ixFileHandle);

        // Close a file handle for an index. 
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given fileHandle
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to supports a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree JSON record in pre-order
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;


        //-----------------------For Insert-----------------------------
        RC newRootPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);
        int searchForInsert(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const int pageNum, const int fatherPageNum);
        RC newLeftLeaf(IXFileHandle &ixfileHandle, const int rightLeaf);
        RC split(IXFileHandle &ixfileHandle, const Attribute &attribute, const int pageNum, const int fatherPageNum, const bool isLeaf);
        RC insertIntoLeafNode(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, const int pageNum);
        RC newLeafPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid,const bool append);
        RC writeToIndex(void *index, const Attribute &attribute, const void *newKey, const int newPageNum);
        RC writeToLeaf(void *leaf, const Attribute &attribute, const void *newKey, const RID &rid);
        bool isEqual(const Attribute &attribute, const void *a, const void *b);

        //-----------------------For Delete---------------------------------
        RC search(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const int pageNum);
        RC deleteRID(IXFileHandle &ixfileHandle, const Attribute &attribute, const int pageNum, const void *key, const RID rid);
        RC searchAndChange(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const int pageNum, const int fatherPageNum);
        RC searchPtrPosInIndex(void *data, const Attribute &attribute, const void *key);
        RC searchAndDeleteKey(void *data, const Attribute &attribute, const void *key);

        // bool isSmaller(const Attribute &attribute,const void *a,const void *b);

        // int getAttributeLength(Attribute attribute,const void *key);

        int getFreeSpaceOffset(IXFileHandle &ixfileHandle, int pageNum);

        int* getFreeSpacePointer(void* pageStartPosPointer);

        int getFreeSpaceOffset(void* pageStartPosPointer);

        RC setFreeSpaceOffset(IXFileHandle &ixfileHandle, int pageNum, int offset);

        RC setFreeSpaceOffset(void* pageStartPosPointer, int offset);

        int getKeysCount(IXFileHandle &ixfileHandle, int pageNum);

        int* getKeysCountPointer(void* pageStartPosPointer) const;

        int getKeysCount(void* pageStartPosPointer) const;

        int getFreeSpace(void* pageStartPosPointer, bool isLeaf);

        RC setKeysCount(IXFileHandle &ixfileHandle, int pageNum, int count);

        RC setKeysCount(void* pageStartPosPointer, int count);

        RC checkLeaf(IXFileHandle &ixfileHandle, int pageNum);

        int* getIsLeafPointer(void* pageStartPosPointer) const;

        RC checkLeaf(void* pageStartPosPointer) const;

        RC setLeaf(IXFileHandle &ixfileHandle, int pageNum);

        RC setLeaf(void* pageStartPosPointer);

        RC setIndex(IXFileHandle &ixfileHandle, int pageNum);

        RC setIndex(void* pageStartPosPointer);

        int* getLeftPointer(void* pageStartPosPointer);

        int getLeftNode(void* pageStartPosPointer);

        RC setLeftNode(void* pageStartPosPointer, int pageNum);

        int* getRightPointer(void* pageStartPosPointer);

        int getRightNode(void* pageStartPosPointer);

        RC setRightNode(void* pageStartPosPointer, int pageNum);

        //--------------------------For GetNextRid()------------------------------
        int getNextPageNumInIndexPage(void* pageStartPosPointer, int offset);
        int searchTillLeafPage(IXFileHandle &ixfileHandle,const Attribute attribute,const void * key,int pageNum);
        int findLeftMostLeafPage(IXFileHandle &ixfileHandle, int pageNum);
        int gotoLeftMostLeafPageSpecialCase(IXFileHandle &ixfileHandle,const Attribute attribute,const void * key,int pageNum);
        void printHelper(IXFileHandle &ixfileHandle, const Attribute &attribute, void * node, int level, bool lastChild) const;
        void printLeafNode(const Attribute &attribute, void * data, int level, bool lastChild) const;
        void printLeafNodeKeyWithRID(const Attribute &attribute, void * data, int keyID) const;
        void printIndexNodeHeader(const Attribute &attribute, void * data, int level) const;
        void printIndexNodeTailer(const Attribute &attribute, void * data, int level, bool lastChild) const;
    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
};

class IXFileHandle {
    public:
        // Put the current counter values of associated PF FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

        IXFileHandle();                             // Constructor
        ~IXFileHandle();                            // Destructor

        RC openFile(const string &fileName);
        RC closeFile();
        unsigned getNumberOfPages();
        RC readPage(PageNum pageNum, void *data);
        RC writePage(PageNum pageNum, const void *data);
        RC appendPage(const void *data);
        FILE* getFile();
    private:
        FileHandle _fileHandle;
};

class IX_ScanIterator {
    public:
        IX_ScanIterator();  							// Constructor
        ~IX_ScanIterator(); 							// Destructor

        RC getNextEntry(RID &rid, void *key);  		// Get next matching entry
        RC close();             						// Terminate index scan

        IXFileHandle ixfileHandle;
        Attribute attribute;
        void * lowKey,* highKey;
        bool lowKeyInclusive,highKeyInclusive;
        void * nextPageData;
        int nextPageNum;
        void * nextKey;
        int nextKeyNum;
        int nextRIDNumOfNextKey;

        // int getOffsetOfSpecificKey(const Attribute &attribute, int keyID, void * pageData);
        bool validateKey();
        void getRID(IndexManager * ixm, const Attribute &attribute, int keyID, int RidID, RID &rid, void * pageData, void * key);
        void printBasicInfo();
};


// print out the error message for a given return code
void IX_PrintError (RC rc);

#endif
