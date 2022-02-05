#ifndef _pfm_h_
#define _pfm_h_

#define PAGE_SIZE 4096

#include <string>
#include <fstream>
#include <cstdio>
#include <memory>
#include <vector>
#include <cstring>
#include <sys/stat.h>

#include "glog/logging.h"
#include "src/include/ErrorCode.h"

namespace PeterDB {

    typedef unsigned PageNum;
    typedef int RC;

    class FileHandle;

    class PagedFileManager {
    public:
        static PagedFileManager &instance();                                // Access to the singleton instance

        RC createFile(const std::string &fileName);                         // Create a new file
        RC destroyFile(const std::string &fileName);                        // Destroy a file
        RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a file
        RC closeFile(FileHandle &fileHandle);                               // Close a file
        bool isFileExists(std::string fileName);
    protected:
        PagedFileManager();                                                 // Prevent construction
        ~PagedFileManager();                                                // Prevent unwanted destruction
        PagedFileManager(const PagedFileManager &);                         // Prevent construction by copying
        PagedFileManager &operator=(const PagedFileManager &);              // Prevent assignment

    };

    class FileHandle {
    public:
        // variables to keep the counter for each operation
        // Stored in the hidden page, i.e. 1st page
        uint32_t readPageCounter;
        uint32_t writePageCounter;
        uint32_t appendPageCounter;
        uint32_t pageCounter;

        std::string fileName;
        std::fstream* fs;

        RC readMetadata();
        RC flushMetadata();

        static int getCounterNum(); // Get Number of Counters
        void setCounters(const uint32_t counters[]);
        void getCounters(unsigned counters[]) const;
        bool isOpen();

    public:
        FileHandle();                                                       // Default constructor
        ~FileHandle();                                                      // Destructor

        RC open(const std::string& tmpFileName);
        RC close();

        RC readPage(PageNum pageNum, void *data);                           // Get a specific page
        RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
        RC appendPage(const void *data);                                    // Append a specific page
        uint32_t getNumberOfPages();                                        // Get the number of pages in the file
        RC collectCounterValues(uint32_t &readPageCount, uint32_t &writePageCount,
                                uint32_t &appendPageCount);                 // Put current counter values into variables
    };

} // namespace PeterDB

#endif // _pfm_h_