#include "src/include/rbfm.h"

namespace PeterDB {
    PageHandle::PageHandle(FileHandle& fileHandle, PageNum pageNum):fh(fileHandle), pageNum(pageNum) {
        fileHandle.readPage(pageNum, data);
        memcpy(&freeBytePointer, data + PAGE_SIZE - sizeof(short), sizeof(short));
        memcpy(&slotNum, data + PAGE_SIZE - sizeof(short) * 2, sizeof(short));
    }

    PageHandle::~PageHandle() = default;

    short PageHandle::getFreeSpace() {
        return PAGE_SIZE - freeBytePointer - getSlotLen() - getHeaderLen();
    }

    bool PageHandle::hasEnoughSpaceForRecord(int recordLen) {
        short freeSpace = getFreeSpace();
        return freeSpace >= (recordLen + 2 * sizeof(short));
    }

    RC PageHandle::insertRecordByteSeq(char byteSeq[], RecordLen recordLen, RID& rid) {
        RC ret = 0;

        // Update Total Slot Num
        slotNum++;
        memcpy(data + getSlotNumOffset(), &slotNum, sizeof(short));
        // Add Slot
        memcpy(data + getSlotOffset(slotNum), &freeBytePointer, sizeof(short));
        memcpy(data + getSlotOffset(slotNum) + sizeof(short), &recordLen, sizeof(short));

        // Insert Byte Seq
        memcpy(data + freeBytePointer, byteSeq, recordLen);
        // Update Free Space Pointer
        freeBytePointer += recordLen;
        memcpy(data + getFreeBytePointerOffset(), &freeBytePointer, sizeof(short));

        // Flush page
        ret = fh.writePage(pageNum, data);

        rid.pageNum = this->pageNum;
        rid.slotNum = this->slotNum;

        if(ret) {
            std::cout << "Fail to write new data into file while inserting record! @ PageHandle::insertRecordByteSeq" << std::endl;
            return ret;
        }
        return 0;
    }

    RC PageHandle::getRecordByteSeq(unsigned short slotNum, char *recordByteSeq, short& recordLen) {
        RC ret = 0;
        short slotOffset = getSlotOffset(slotNum);
        short recordOffset;

        memcpy(&recordOffset, data + slotOffset, sizeof(short));
        memcpy(&recordLen, data + slotOffset + sizeof(short), sizeof(short));

        // Read Record Byte Seq
        memcpy(recordByteSeq, data + recordOffset, recordLen);

        return 0;
    }



    short PageHandle::getHeaderLen() {
        return 2 * sizeof(short);
    }

    short PageHandle::getSlotLen() {
        return slotNum * sizeof(short);
    }

    short PageHandle::getSlotNumOffset() {
        return PAGE_SIZE - 2 * sizeof(short);
    }

    short PageHandle::getFreeBytePointerOffset() {
        return PAGE_SIZE - sizeof(short);
    }

    short PageHandle::getSlotOffset(unsigned short slotNum) {
        return getSlotNumOffset() - 2 * sizeof(short) * slotNum;
    }
}
