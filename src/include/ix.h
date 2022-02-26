#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <queue>

#include "pfm.h"
#include "rbfm.h" // for some type declarations only, e.g., RID and Attribute

# define IX_EOF (-1)  // end of the index scan

namespace PeterDB {
    namespace IX {
        // IX File Common
        const int32_t FILE_HIDDEN_PAGE_NUM = 1;
        const int32_t FILE_ROOT_PAGE_NUM = 1;
        const int32_t FILE_COUNTER_NUM = 3;
        const int32_t FILE_COUNTER_LEN = 4;
        const int32_t FILE_ROOTPAGE_PTR_LEN = 4;
        const int32_t FILE_ROOT_LEN = 4;

        // IX Page Common
        const int16_t PAGE_TYPE_LEN = 2;
        const int16_t PAGE_TYPE_UNKNOWN = 0;
        const int16_t PAGE_TYPE_INDEX = 1;
        const int16_t PAGE_TYPE_LEAF = 2;
        const int16_t PAGE_FREEBYTE_PTR_LEN = 2;
        const int16_t PAGE_COUNTER_LEN = 2;
        const int16_t PAGE_PARENT_PTR_LEN = 4;

        // Index Page
        const int16_t INDEXPAGE_CHILD_PTR_LEN = 4;

        // Leaf Page
        const int16_t LEAFPAGE_NEXT_PTR_LEN = 4;
        const int16_t LEAFPAGE_ENTRY_PAGE_LEN = 4;
        const int16_t LEAFPAGE_ENTRY_SLOT_LEN = 2;

        // Page Pointer
        const uint32_t PAGE_PTR_NULL = 0;
    }

    class IX_ScanIterator;

    class IXFileHandle;

    class IndexManager {

    public:
        static IndexManager &instance();

        // Create an index file.
        RC createFile(const std::string &fileName);

        // Delete an index file.
        RC destroyFile(const std::string &fileName);

        // Open an index and return an ixFileHandle.
        RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

        // Close an ixFileHandle for an index.
        RC closeFile(IXFileHandle &ixFileHandle);

        // Insert an entry into the given index that is indicated by the given ixFileHandle.
        RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attr, const void *key, const RID &rid);
        RC insertEntryRecur(IXFileHandle &ixFileHandle, uint32_t pageNum, const Attribute &attr, const uint8_t *key, const RID &rid,
                            uint8_t* middleKey, uint32_t& newChildPage, bool& isNewChildNull);

        // Delete an entry from the given index that is indicated by the given ixFileHandle.
        RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixFileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        RC findTargetLeafNode(IXFileHandle &ixFileHandle, uint32_t& leafPageNum, const uint8_t* key, const Attribute& attr);

        // Print the B+ tree in pre-order (in a JSON record format)
        RC printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const;

    protected:
        IndexManager() = default;                                                   // Prevent construction
        ~IndexManager() = default;                                                  // Prevent unwanted destruction
        IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
        IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment
    private:
        bool isFileExists(std::string fileName);
    };

    class IXFileHandle {
    public:
        // variables to keep counter for each operation
        unsigned ixReadPageCounter;
        unsigned ixWritePageCounter;
        unsigned ixAppendPageCounter;

        std::string fileName;
        std::fstream* fs;

        uint32_t rootPagePtr;
        uint32_t root;
    public:
        IXFileHandle();
        ~IXFileHandle();

        RC open(const std::string& filename);
        RC close();

        RC readPage(uint32_t pageNum, void* data);
        RC writePage(uint32_t pageNum, const void* data);
        RC appendPage(const void* data);
        RC appendEmptyPage();

        // Put the current counter values of associated PF FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

        RC readMetaData();
        RC flushMetaData();

        RC createRootPage();

        RC readRoot();
        RC flushRoot();

        uint32_t getRoot();
        RC setRoot(uint32_t newRoot);

        std::string getFileName();
        bool isOpen();
        bool isRootPageExist();
        bool isRootNull();

        uint32_t getPageCounter();
        uint32_t getLastPageIndex();
    };

    class IXPageHandle {
    public:
        IXFileHandle& ixFileHandle;
        uint32_t pageNum;

        int16_t pageType;
        int16_t freeBytePtr;
        int16_t counter;
        uint32_t parentPtr;

        uint8_t data[PAGE_SIZE] = {};
        uint8_t origin[PAGE_SIZE] = {};

        bool isForCheck;

    public:
        // Existed page
        IXPageHandle(IXFileHandle& fileHandle, uint32_t page);
        // New page with data
        IXPageHandle(IXFileHandle& fileHandle, uint8_t* newData, int16_t dataLen, uint32_t page, int16_t type, int16_t freeByte, int16_t counter, uint32_t parent);
        // New Page
        IXPageHandle(IXFileHandle& fileHandle, uint32_t page, int16_t type, int16_t freeByte, int16_t counter, uint32_t parent);
        IXPageHandle(IXPageHandle& pageHandle);
        ~IXPageHandle();

        bool isOpen();
        bool isTypeIndex();
        bool isTypeLeaf();

        bool isCompositeKeyMeetCompCondition(const uint8_t* key1, const RID& rid1, const uint8_t* key2, const RID& rid2, const Attribute& attr, const CompOp op);
        bool isKeyMeetCompCondition(const uint8_t* key1, const uint8_t* key2, const Attribute& attr, const CompOp op);
        bool isEntryMeetCompCondition(const RID& rid1, const RID& rid2, const CompOp op);

        RC shiftRecordLeft(int16_t dataNeedShiftStartPos, int16_t dist);
        RC shiftRecordRight(int16_t dataNeedShiftStartPos, int16_t dist);

    public:
        int16_t getKeyLen(const uint8_t* key, const Attribute &attr);
        int32_t getKeyInt(const uint8_t* key);
        float getKeyReal(const uint8_t* key);
        std::string getKeyString(const uint8_t* key);

        int16_t getHeaderLen();
        void flushHeader();

        int16_t getPageType();
        void setPageType(int16_t type);

        int16_t getFreeBytePointerOffset();
        int16_t getFreeBytePointer();
        void setFreeBytePointer(int16_t ptr);

        int16_t getCounterOffset();
        int16_t getCounter();
        void setCounter(int16_t counter);
        void addCounterByOne();

        int16_t getParentPtrOffset();
        uint32_t getParentPtr();
        void setParentPtr(uint32_t parent);
        bool isParentPtrNull();

    protected:
        int16_t getPageTypeFromData();
        int16_t getFreeBytePointerFromData();
        int16_t getCounterFromData();
        uint32_t getParentPtrFromData();
    };

    class IndexPageHandle: public IXPageHandle {
    public:
        // For existed page
        IndexPageHandle(IXPageHandle& pageHandle);
        IndexPageHandle(IXFileHandle& fileHandle, uint32_t page);
        // Initialize new page containing one entry
        IndexPageHandle(IXFileHandle& fileHandle, uint32_t page, uint32_t parent, uint32_t leftPage, uint8_t* key, uint32_t rightPage, const Attribute &attr);
        // Initialize new page with existing entries
        IndexPageHandle(IXFileHandle& fileHandle, uint32_t page, uint32_t parent, uint8_t* entryData, int16_t dataLen, int16_t entryCounter);
        ~IndexPageHandle();

        // Get target child page, if not exist, append one
        RC getTargetChild(uint32_t& childPtr, const uint8_t* key, const Attribute &attr);

        RC findPosToInsertKey(int16_t& keyPos, const uint8_t* key, const Attribute& attr);
        RC insertIndex(uint8_t* middleKey, uint32_t& newChildPage, bool& isNewChildNull, const uint8_t* key, const Attribute& attr, uint32_t newChildPtr);
        RC insertIndexWithEnoughSpace(const uint8_t* key, const Attribute& attr, uint32_t child);
        RC writeIndex(int16_t pos, const uint8_t* key, const Attribute& attr, uint32_t newPageNum);

        RC splitPageAndInsertIndex(uint8_t * middleKey, uint32_t & newIndexPage, const uint8_t* key, const Attribute& attr, uint32_t child);

        RC print(const Attribute &attr, std::ostream &out);

        int16_t getEntryLen(const uint8_t* key, const Attribute& attr);

        bool hasEnoughSpace(const uint8_t* key, const Attribute &attr);
        int16_t getIndexHeaderLen();
        int16_t getFreeSpace();

    };

    class LeafPageHandle: public IXPageHandle {
    public:
        uint32_t nextPtr;
    public:
        // For existed page
        LeafPageHandle(IXPageHandle& pageHandle);
        LeafPageHandle(IXFileHandle& fileHandle, uint32_t page);
        // For new page
        LeafPageHandle(IXFileHandle& fileHandle, uint32_t page, uint32_t parent, uint32_t next);
        // For split page
        LeafPageHandle(IXFileHandle& fileHandle, uint32_t page, uint32_t parent, uint32_t next, uint8_t* entryData, int16_t dataLen, int16_t entryCounter);
        ~LeafPageHandle();

        RC insertEntry(const uint8_t* key, const RID& entry, const Attribute& attr, uint8_t* middleKey, uint32_t& newChild, bool& isNewChildNull);
        RC insertEntryWithEnoughSpace(const uint8_t* key, const RID& entry, const Attribute& attr);
        RC writeEntry(int16_t pos, const uint8_t* key, const RID& entry, const Attribute& attr);
        RC findFirstKeyMeetCompCondition(int16_t& pos, const uint8_t* key, const Attribute& attr, CompOp op);
        RC findFirstCompositeKeyMeetCompCondition(int16_t& pos, const uint8_t* key, const RID& rid, const Attribute& attr, CompOp op);

        RC deleteEntry(const uint8_t* key, const RID& entry, const Attribute& attr);

        RC getFirstKey(uint8_t* keyData, const Attribute& attr);

        RC splitPageAndInsertEntry(uint8_t* middleKey, uint32_t& newLeafNum, uint8_t* key, const RID& entry, const Attribute& attr);

        RC print(const Attribute &attr, std::ostream &out);

        bool hasEnoughSpace(const uint8_t* key, const Attribute& attr);
        int16_t getEntryLen(const uint8_t* key, const Attribute& attr);
        void getRid(const uint8_t* keyStartPos, const Attribute& attr, RID& rid);

        // For Scan
        int16_t getNextEntryPos(int16_t curEntryPos, const Attribute &attr);
        RC getEntry(int16_t pos, uint8_t* key, RID& rid, const Attribute& attr);

        // MetaData
        int16_t getLeafHeaderLen();

        int16_t getFreeSpace();
        bool isEmpty();

        int16_t getNextPtrOffset();
        uint32_t getNextPtr();
        uint32_t getNextPtrFromData();
        void setNextPtr(uint32_t next);
    };

    class IX_ScanIterator {
    public:
        IXFileHandle* ixFileHandlePtr;
        Attribute attr;
        const uint8_t* lowKey;
        bool lowKeyInclusive;
        const uint8_t* highKey;
        bool highKeyInclusive;

        uint32_t curLeafPage;
        int16_t curSlotPos;
        bool entryExceedUpperBound;
    public:
        // Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        RC open(IXFileHandle* ixFileHandle, const Attribute& attr,
                const uint8_t* lowKey, const uint8_t* highKey,
                bool lowKeyInclusive, bool highKeyInclusive);

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
    };

}// namespace PeterDB
#endif // _ix_h_
