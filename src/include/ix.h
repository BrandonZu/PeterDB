#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "pfm.h"
#include "rbfm.h" // for some type declarations only, e.g., RID and Attribute

# define IX_EOF (-1)  // end of the index scan

namespace PeterDB {
    namespace IX {
        const int16_t PAGE_TYPE_LEN = 2;
        const int16_t TYPE_INDEX_PAGE = 1;
        const int16_t TYPE_LEAF_PAGE = 2;

        const int16_t PAGE_FREEBYTE_PTR_LEN = 2;

        const int16_t PAGE_COUNTER_LEN = 2;
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
        FileHandle fh;
    public:
        // variables to keep counter for each operation
        unsigned ixReadPageCounter;
        unsigned ixWritePageCounter;
        unsigned ixAppendPageCounter;

        int32_t root;
        AttrType keyType;

        IXFileHandle();
        ~IXFileHandle();

        RC open(const std::string& filename);
        RC close();

        RC readPage(uint32_t pageNum, void* data);
        RC writePage(uint32_t pageNum, const void* data);
        RC appendPage(const void* data);

        // Put the current counter values of associated PF FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

        std::string getFileName();
        bool isOpen();
    };

    class IXPageHandle {
        IXFileHandle& fh;
        uint32_t pageNum;
        int16_t pageType;
        int16_t freeBytePtr;
        uint8_t data[PAGE_SIZE] = {};
    public:
        IXPageHandle(IXFileHandle& fileHandle, uint32_t page);
        ~IXPageHandle();

        bool isTypeIndex();
        void setTypeIndex();
        bool isTypeLeaf();
        void setTypeLeaf();

        // Index Page


        // Leaf Page


    private:
        int16_t getPageType();
        void setPageType(int16_t type);

        int16_t getFreeBytePointerOffset();
        int16_t getFreeBytePointer();
        void setFreeBytePointer(int16_t ptr);

        int16_t getCounterOffset();
        int16_t getCounter();
        void setCounter(int16_t counter);
    };

}// namespace PeterDB
#endif // _ix_h_
