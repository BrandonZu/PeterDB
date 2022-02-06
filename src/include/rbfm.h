#ifndef _rbfm_h_
#define _rbfm_h_

#include <vector>
#include <cmath>
#include <iostream>
#include <algorithm>
#include <unordered_set>
#include <unordered_map>

#include "pfm.h"

namespace PeterDB {
    // Record ID
    typedef struct {
        unsigned pageNum;           // page number
        unsigned short slotNum;     // slot number in the page
    } RID;

    // Attribute
    typedef enum {
        TypeInt = 0, TypeReal, TypeVarChar
    } AttrType;

    typedef unsigned AttrLength;

    typedef struct Attribute {
        std::string name;  // attribute name
        AttrType type;     // attribute type
        AttrLength length; // attribute length
    } Attribute;

    // Comparison Operator (NOT needed for part 1 of the project)
    typedef enum {
        EQ_OP = 0, // no condition// =
        LT_OP,      // <
        LE_OP,      // <=
        GT_OP,      // >
        GE_OP,      // >=
        NE_OP,      // !=
        NO_OP       // no condition
    } CompOp;

    /********************************************************************
    * The scan iterator is NOT required to be implemented for Project 1 *
    ********************************************************************/

    # define RBFM_EOF (-1)  // end of a scan operator
    //  RBFM_ScanIterator is an iterator to go through records
    //  The way to use it is like the following:
    //  RBFM_ScanIterator rbfmScanIterator;
    //  rbfm.open(..., rbfmScanIterator);
    //  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
    //    process the data;
    //  }
    //  rbfmScanIterator.close();

    const uint16_t MASK_RECORD = 0;
    const uint16_t MASK_RECORD_POINTER = 1;

    const int16_t RECORD_MASK_LEN = 2;
    const int16_t RECORD_ATTRNUM_LEN = 2;
    const int16_t RECORD_ATTR_ENDPOS_LEN = 2;
    const int16_t RECORD_ATTR_NULL_ENDPOS = -1;
    const int16_t PTRRECORD_RECORD_PAGE_INDEX_LEN = 4;
    const int16_t PTRRECORD_SLOT_INDEX_LEN = 2;
    const int16_t MIN_RECORD_LEN = RECORD_MASK_LEN + PTRRECORD_RECORD_PAGE_INDEX_LEN + PTRRECORD_SLOT_INDEX_LEN;

    const int16_t SLOT_INDEX_START = 1;
    const int16_t SLOT_RECORD_POINTER_LEN = 2;
    const int16_t SLOT_RECORD_LEN_LEN = 2;
    const int16_t EMPTY_SLOT_OFFSET = -1;
    const int16_t EMPTY_SLOT_LEN = 0;

    const int16_t ATTR_NULL_LEN = -1;

    class RBFM_ScanIterator {
    public:
        FileHandle fileHandle;
        std::vector<Attribute> recordDesc;
        std::vector<uint32_t> selectedAttrIndex;

        CompOp compOp;
        Attribute conditionAttr;
        int32_t conditionAttrIndex;
        uint8_t* conditionAttrValue;
        int32_t conditionStrLen;    // Only use when condition attr is string

        uint32_t curPageIndex;
        uint16_t curSlotIndex;

        // Store record byte sequence
        uint8_t recordByteSeq[PAGE_SIZE];
        int16_t recordLen;

    public:
        RBFM_ScanIterator();
        ~RBFM_ScanIterator();

        RC open(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                const std::string &conditionAttribute, const CompOp compOp, const void *value,
                const std::vector<std::string> &attributeNames);
        RC close();

        // Never keep the results in the memory. When getNextRecord() is called,
        // a satisfying record needs to be fetched from the file.
        // "data" follows the same format as RecordBasedFileManager::insertRecord().
        RC getNextRecord(RID &recordRid, void *data);

        bool isRecordMeetCondition(uint8_t attrData[], int16_t attrLen);
    };

    class RecordBasedFileManager {
    public:
        static RecordBasedFileManager &instance();                          // Access to the singleton instance

        RC createFile(const std::string &fileName);                         // Create a new record-based file

        RC destroyFile(const std::string &fileName);                        // Destroy a record-based file

        RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a record-based file

        RC closeFile(FileHandle &fileHandle);                               // Close a record-based file

        //  Format of the data passed into the function is the following:
        //  [n byte-null-indicators for y fields] [actual conditionAttrValue for the first field] [actual conditionAttrValue for the second field] ...
        //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
        //     The conditionAttrValue n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
        //     Each bit represents whether each field conditionAttrValue is null or not.
        //     If k-th bit from the left is set to 1, k-th field conditionAttrValue is null. We do not include anything in the actual data part.
        //     If k-th bit from the left is set to 0, k-th field contains non-null values.
        //     If there are more than 8 fields, then you need to find the corresponding byte first,
        //     then find a corresponding bit inside that byte.
        //  2) Actual data is a concatenation of values of the attributes.
        //  3) For Int and Real: use 4 bytes to store the conditionAttrValue;
        //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
        //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
        // For example, refer to the Q8 of Project 1 wiki page.

        // Insert a record into a file
        RC insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                        RID &rid);

        // Read a record identified by the given rid.
        RC
        readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, void *data);

        // Print the record that is passed to this utility method.
        // This method will be mainly used for debugging/testing.
        // The format is as follows:
        // field1-name: field1-conditionAttrValue  field2-name: field2-conditionAttrValue ... \n
        // (e.g., age: 24  height: 6.1  salary: 9000
        //        age: NULL  height: 7.5  salary: 7500)
        RC printRecord(const std::vector<Attribute> &recordDescriptor, const void *data, std::ostream &out);

        /*****************************************************************************************************
        * IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) *
        * are NOT required to be implemented for Project 1                                                   *
        *****************************************************************************************************/
        // Delete a record identified by the given rid.
        RC deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid);

        // Assume the RID does not change after an update
        RC updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                        const RID &rid);

        // Read an attribute given its name and the rid.
        RC readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid,
                         const std::string &attributeName, void *data);

        // Scan returns an iterator to allow the caller to go through the results one by one.
        RC scan(FileHandle &fileHandle,
                const std::vector<Attribute> &recordDescriptor,
                const std::string &conditionAttribute,
                const CompOp compOp,                  // comparison type such as "<" and "="
                const void *value,                    // used in the comparison
                const std::vector<std::string> &attributeNames, // a list of projected attributes
                RBFM_ScanIterator &rbfm_ScanIterator);

    protected:
        RecordBasedFileManager();                                                   // Prevent construction
        ~RecordBasedFileManager();                                                  // Prevent unwanted destruction
        RecordBasedFileManager(const RecordBasedFileManager &);                     // Prevent construction by copying
        RecordBasedFileManager &operator=(const RecordBasedFileManager &);          // Prevent assignment

    private:
        // TODO: Create a PageOrganizer class to organize pages
        RC findAvailPage(FileHandle& fileHandle, int16_t recordLen, PageNum& availPageIndex);
    };

    class RecordPageHandle {
    public:
        FileHandle& fh;
        PageNum pageNum;

        int16_t freeBytePointer;
        int16_t slotCounter;
        uint8_t data[PAGE_SIZE] = {};

    public:
        RecordPageHandle(FileHandle& fileHandle, PageNum pageNum);
        ~RecordPageHandle();

        // Read Record
        // Record Format Described in report
        RC getRecordByteSeq(int16_t slotNum, uint8_t recordByteSeq[], int16_t& recordLen);
        RC getRecordPointerTarget(int16_t curSlotNum, int& ptrPageNum, int16_t& ptrSlotNum);

        RC getRecordAttr(int16_t slotNum, int16_t attrIndex, uint8_t* attrData);
        RC getNextRecord(uint16_t& slotIndex, uint8_t* byteSeq, int16_t& recordLen);

        // Insert Record
        RC insertRecord(uint8_t byteSeq[], int16_t byteSeqLen, RID& rid);

        // Delete Record
        RC deleteRecord(int16_t slotIndex);

        // Update Record
        RC updateRecord(int16_t slotIndex, uint8_t byteSeq[], int16_t recordLen);
        RC setRecordPointToNewRecord(int16_t curSlotIndex, const RID& newRecordPos);

        // Helper Functions
        // Shift records left to avoid empty holes between records
        RC shiftRecord(int16_t dataNeedShiftStartPos, int16_t dist, bool shiftLeft);

        int16_t getFreeSpace();
        bool hasEnoughSpaceForRecord(int16_t recordLen);

        int16_t getAvailSlot();

        int16_t getHeaderLen();
        int16_t getSlotListLen();

        int16_t getSlotCounterOffset();
        int16_t getFreeBytePointerOffset();

        // For Specific Record
        // Slot Index start from 1 !!!
        int16_t getSlotOffset(int16_t slotIndex);
        int16_t getRecordOffset(int16_t slotIndex);
        int16_t getRecordLen(int16_t slotIndex);

        bool isRecordPointer(int16_t slotNum);
        bool isRecordReadable(uint16_t slotIndex);
        bool isRecordDeleted(int16_t slotIndex);

        int16_t getAttrNum(int16_t slotIndex);  // Attr offset relative to the start point of record
        int16_t getAttrBeginPos(int16_t slotIndex, int16_t attrIndex);  // Get begin position relative to record offset
        int16_t getAttrEndPos(int16_t slotIndex, int16_t attrIndex);   // Get end position relative to record offset
        int16_t getAttrLen(int16_t slotIndex, int16_t attrIndex);
        bool isAttrNull(int16_t slotIndex, int16_t attrIndex);
    };

    // Based on Record Format
    class RecordHelper {
    public:
        static RC APIFormatToRecordByteSeq(uint8_t * apiData, const std::vector<Attribute> &attrs, uint8_t* byteSeq, int16_t & recordLen);
        static RC recordByteSeqToAPIFormat(uint8_t record[], const std::vector<Attribute> &recordDescriptor, std::vector<uint32_t> &selectedAttrIndex, uint8_t* apiData);

        static bool isAttrNull(uint8_t* data, uint32_t index);
        static void setAttrNull(uint8_t* data, uint32_t index);
    };

} // namespace PeterDB

#endif // _rbfm_h_
