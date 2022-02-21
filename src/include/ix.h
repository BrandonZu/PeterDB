#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "pfm.h"
#include "rbfm.h" // for some type declarations only, e.g., RID and Attribute

# define IX_EOF (-1)  // end of the index scan

namespace PeterDB {
    namespace IX {
        const int32_t FILE_COUNTER_NUM = 3;
        const int32_t FILE_COUNTER_LEN = 4;
        const int32_t FILE_ROOT_LEN = 4;
        const int32_t FILE_TYPE_LEN = 4;
        const uint32_t FILE_ROOT_NULL = 0;

        const int16_t PAGE_TYPE_LEN = 2;
        const int16_t PAGE_TYPE_UNKNOWN = 0;
        const int16_t PAGE_TYPE_INDEX = 1;
        const int16_t PAGE_TYPE_LEAF = 2;
        const int16_t PAGE_FREEBYTE_PTR_LEN = 2;
        const int16_t PAGE_COUNTER_LEN = 2;
        const int16_t PAGE_PARENT_PTR_LEN = 4;

        const int16_t LEAFPAGE_NEXT_PTR_LEN = 4;
        const int16_t LEAFPAGE_ENTRY_PAGE_LEN = 4;
        const int16_t LEAFPAGE_ENTRY_SLOT_LEN = 2;

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
        RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);
        RC insertEntryRecur(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid, uint32_t pageNum);

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

    class IX_ScanIterator {
    public:

        // Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
    };

    class IXFileHandle {
    public:
        // variables to keep counter for each operation
        unsigned ixReadPageCounter;
        unsigned ixWritePageCounter;
        unsigned ixAppendPageCounter;

        std::string fileName;
        std::fstream* fs;

        int32_t root;
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

        RC insertRootPage();

        std::string getFileName();
        bool isOpen();
        bool isRootNull();
        uint32_t getPageCounter();
        uint32_t getLastPageIndex();
    };

    class IXPageHandle {
    public:
        IXFileHandle& fh;
        uint32_t pageNum;

        int16_t pageType;
        int16_t freeBytePtr;
        int16_t counter;
        uint32_t parentPtr;

        uint8_t data[PAGE_SIZE] = {};

    public:
        IXPageHandle(IXFileHandle& fileHandle, uint32_t page);
        IXPageHandle(IXPageHandle& pageHandle);
        ~IXPageHandle();

        bool isTypeIndex();
        bool isTypeLeaf();

        bool isKeyGreater(const uint8_t* key1, const uint8_t* key2, const Attribute& attr);

        RC shiftRecordLeft(int16_t dataNeedShiftStartPos, int16_t dist);
        RC shiftRecordRight(int16_t dataNeedShiftStartPos, int16_t dist);
    protected:
        int16_t getHeaderLen();

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
    };

    class IndexPageHandle: IXPageHandle {
    public:
        // For existed page
        IndexPageHandle(IXPageHandle& pageHandle);
        // For new page
        IndexPageHandle(IXFileHandle& fileHandle, uint32_t page, uint32_t parent);
        ~IndexPageHandle();

        int16_t getIndexHeaderLen();
        int16_t getFreeSpace();

        RC insertIndex();

    };

    class LeafPageHandle: IXPageHandle {
    public:
        uint32_t nextPtr;
    public:
        // For existed page
        LeafPageHandle(IXPageHandle& pageHandle);
        // For new page
        LeafPageHandle(IXFileHandle& fileHandle, uint32_t page, uint32_t parent, uint32_t next);
        ~LeafPageHandle();

        RC insertEntry(const uint8_t* key, const RID& entry, const Attribute& attr);
        RC writeEntry(int16_t pos, const uint8_t* key, const RID& entry, const Attribute& attr);

        // TODO split page
        RC splitPage();

        bool hasEnoughSpace(const uint8_t* key, const Attribute &attr);
        int16_t getEntryLen(const uint8_t* key, const Attribute& attr);
        int16_t getKeyLen(const uint8_t* key, const Attribute &attr);
        int16_t getLeafHeaderLen();
        int16_t getFreeSpace();

        int16_t getNextPtrOffset();
        uint32_t getNextPtr();
        void setNextPtr(uint32_t next);
    };

}// namespace PeterDB
#endif // _ix_h_
