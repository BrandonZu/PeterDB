#include "src/include/rbfm.h"

namespace PeterDB {
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() = default;

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        return PagedFileManager::instance().createFile(fileName);
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        return PagedFileManager::instance().destroyFile(fileName);
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        return PagedFileManager::instance().openFile(fileName, fileHandle);
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        return PagedFileManager::instance().closeFile(fileHandle);
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        RC ret = 0;
        if(!fileHandle.isOpen()) {
            LOG(ERROR) << "FileHandle NOT bound to a file! @ RecordBasedFileManager::insertRecord" << std::endl;
            return 1;
        }

        // 1. Transform Record to Byte Sequence
        short recordLen = 0;
        char buffer[PAGE_SIZE] = {};
        ret = RecordHelper::rawDataToRecordByteSeq((char *) data, recordDescriptor, buffer, recordLen);
        if(ret) {
            LOG(ERROR) << "Fail to Transform Record to Byte Seq @ RecordBasedFileManager::insertRecord" << std::endl;
            return ret;
        }
        char byteSeq[recordLen];
        memcpy(byteSeq, buffer, recordLen);

        // 2. Find an available page
        PageNum pageIndex;
        ret = findAvailPage(fileHandle, recordLen, pageIndex);
        if(ret) {
            LOG(ERROR) << "Fail to find an Available Page! @ RecordBasedFileManager::insertRecord" << std::endl;
            return ret;
        }

        // 3. Insert Record via Record Page Handle
        RecordPageHandle recordPageHandle(fileHandle, pageIndex);
        ret = recordPageHandle.insertRecordByteSeq(byteSeq, recordLen, rid);
        if(ret) {
            LOG(ERROR) << "Fail to insert record's byte sequence via RecordPageHandle @ RecordBasedFileManager::insertRecord" << std::endl;
            return ret;
        }
        return 0;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        RC ret = 0;
        if(!fileHandle.isOpen()) {
            LOG(ERROR) << "FileHandle NOT bound to a file! @ RecordBasedFileManager::readRecord" << std::endl;
            return 1;
        }
        if(rid.pageNum >= fileHandle.getNumberOfPages()) {
            LOG(ERROR) << "Target Page not exist! @ RecordBasedFileManager::readRecord" << std::endl;
            return 2;
        }
        // 1. Follow the pointer to find the real record
        int curPage = rid.pageNum;
        short curSlot = rid.slotNum;
        while(true) {
            RecordPageHandle curPageHandle(fileHandle, curPage);
            if(curPageHandle.isRecordDeleted(curSlot)) {
                LOG(INFO) << "Fail to read deleted record @ RecordBasedFileManager::readRecord" << std::endl;
                return 3;
            }
            // Break the loop when real record is found
            if(curPageHandle.isRecordPointer(curSlot) == false) {
                break;
            }
            curPageHandle.getRecordPointerTarget(curSlot, curPage, curSlot);
        }

        // 2. Read Record Byte Seq
        RecordPageHandle pageHandle(fileHandle, curPage);
        char recordBuffer[PAGE_SIZE] = {};
        short recordLen = 0;

        ret = pageHandle.getRecordByteSeq(curSlot, recordBuffer, recordLen);
        if(ret) {
            LOG(ERROR) << "Fail to Get Record Byte Seq @ RecordBasedFileManager::readRecord" << std::endl;
            return ret;
        }

        // 3. Transform Record Byte Seq to raw data(output format)
        ret = RecordHelper::recordByteSeqToRawData(recordBuffer, recordLen, recordDescriptor, (char*)data);
        if(ret) {
            LOG(ERROR) << "Fail to transform Record to Raw Data @ RecordBasedFileManager::readRecord" << std::endl;
            return ret;
        }
        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        RC ret = 0;
        if(!fileHandle.isOpen()) {
            LOG(ERROR) << "FileHandle NOT bound to a file! @ RecordBasedFileManager::deleteRecord" << std::endl;
            return 1;
        }

        // 1. Follow the pointer to find the real record
        int curPage = rid.pageNum;
        short curSlot = rid.slotNum;
        while(true) {
            RecordPageHandle curPageHandle(fileHandle, curPage);
            if(curPageHandle.isRecordDeleted(curSlot)) {
                LOG(INFO) << "Record already deleted @ RecordBasedFileManager::deleteRecord" << std::endl;
                return 2;
            }
            // Break the loop when real record is found
            if(curPageHandle.isRecordPointer(curSlot) == false) {
                break;
            }
            curPageHandle.getRecordPointerTarget(curSlot, curPage, curSlot);
            ret = curPageHandle.deleteRecord(curSlot);
            if(ret) {
                LOG(ERROR) << "Fail to delete record @ RecordBasedFileManager::deleteRecord" << std::endl;
                return ret;
            }
        }

        // 2. Delete the real record
        RecordPageHandle pageContainTargetRecord(fileHandle, curPage);
        ret = pageContainTargetRecord.deleteRecord(curSlot);

        if(ret) {
            LOG(ERROR) << "Fail to delete record via RecordPageHandle @ RecordBasedFileManager::deleteRecord" << std::endl;
            return ret;
        }
        return ret;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        RC ret = 0;
        const std::string deli = " ";
        char strBuffer[PAGE_SIZE] = {};
        unsigned dataPos = ceil(recordDescriptor.size() / 8.0);
        for(short i = 0; i < recordDescriptor.size(); i++) {
            // Print Name
            out << recordDescriptor[i].name << ":" << deli;
            // Print Value
            if(RecordHelper::isAttrNull((char*)data, i)) {
                out << "NULL";
            }
            else {
                switch (recordDescriptor[i].type) {
                    case TypeInt:
                        int intVal;
                        memcpy(&intVal, (char *)data + dataPos, sizeof(int));
                        dataPos += sizeof(int);
                        out << intVal;
                        break;
                    case TypeReal:
                        float floatVal;
                        memcpy(&floatVal, (char *)data + dataPos, sizeof(float));
                        dataPos += sizeof(float);
                        out << floatVal;
                        break;
                    case TypeVarChar:
                        unsigned strLen;
                        // Get String Len
                        memcpy(&strLen, (char *)data + dataPos, sizeof(unsigned));
                        dataPos += sizeof(unsigned);
                        memcpy(strBuffer, (char *)data + dataPos, strLen);
                        strBuffer[strLen] = '\0';
                        dataPos += strLen;
                        out << strBuffer;
                        break;
                    default:
                        LOG(ERROR) << "DataType not supported @ RecordBasedFileManager::printRecord" << std::endl;
                        break;
                }
            }
            // Print Deli or Endl
            if(i != recordDescriptor.size() - 1) {
                out << ',' << deli;
            }
            else {
                out << std::endl;
            }
        }
        return 0;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        RC ret = 0;
        if(!fileHandle.isOpen()) {
            LOG(ERROR) << "FileHandle NOT bound to a file! @ RecordBasedFileManager::updateRecord" << std::endl;
            return 1;
        }

        // 1. Follow the pointer to find real record
        int curPageIndex = rid.pageNum;
        short curSlotIndex = rid.slotNum;
        while(true) {
            RecordPageHandle curPageHandle(fileHandle, curPageIndex);
            if(curPageHandle.isRecordDeleted(curSlotIndex)) {
                LOG(ERROR) << "Record already deleted @ RecordBasedFileManager::updateRecord" << std::endl;
                return 2;
            }
            // Break the loop when real record is found
            if(curPageHandle.isRecordPointer(curSlotIndex) == false) {
                break;
            }
            curPageHandle.getRecordPointerTarget(curSlotIndex, curPageIndex, curSlotIndex);
        }

        // 2. Transform Record Data to Byte Sequence
        short recordLen = 0;
        char buffer[PAGE_SIZE] = {};
        ret = RecordHelper::rawDataToRecordByteSeq((char *) data, recordDescriptor, buffer, recordLen);
        if(ret) {
            LOG(ERROR) << "Fail to Transform Record to Byte Seq @ RecordBasedFileManager::insertRecord" << std::endl;
            return ret;
        }
        char byteSeq[recordLen];
        memcpy(byteSeq, buffer, recordLen);

        // 3. Find available space to store new record and update
        // Case 1: new byte seq is shorter -> shift records left && update slots and free byte pointer
        // Case 2: new byte seq is longer, cur page has enough space -> shift records right && update slots and free byte pointer
        // Case 3: new byte seq is longer, no enough space in cur page -> insert record in a new page && update old record to a pointer
        PageNum pageToStore;
        RecordPageHandle curPageHandle(fileHandle, curPageIndex);
        short oldRecordOffset = curPageHandle.getRecordOffset(curSlotIndex);
        short oldRecordLen = curPageHandle.getRecordLen(curSlotIndex);

        if(oldRecordLen >= recordLen ||
           (recordLen > oldRecordLen && (recordLen - oldRecordLen) <= curPageHandle.getFreeSpace())) {
            // Current Page can store new record
            ret = curPageHandle.updateRecord(curSlotIndex, byteSeq, recordLen);
            if(ret) {
                LOG(ERROR) << "Fail to update record @ RecordBasedFileManager::updateRecord"<< std::endl;
                return ret;
            }
        }
        else {
            // No enough space in current page
            // Fina an available page and insert new record to it
            ret = findAvailPage(fileHandle, recordLen, pageToStore);
            if(ret) {
                LOG(ERROR) << "Fail to find an available page @ RecordBasedFileManager::updateRecord" << std::endl;
                return ret;
            }
            RecordPageHandle newPageHandle(fileHandle, pageToStore);
            RID newRecordRID;
            ret = newPageHandle.insertRecordByteSeq(byteSeq, recordLen, newRecordRID);
            if(ret) {
                LOG(ERROR) << "Fail to store new record's byte sequence @ RecordBasedFileManager::updateRecord" << std::endl;
                return ret;
            }
            // Update old record to a record pointer
            curPageHandle.setRecordPointToNewRecord(curSlotIndex, newRecordRID);
        }

        return ret;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        return -1;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        return -1;
    }

    // Page Organizer Functions
    RC RecordBasedFileManager::findAvailPage(FileHandle& fileHandle, short recordLen, PageNum& availPageIndex) {
        RC ret = 0;
        char buffer[PAGE_SIZE] = {};
        unsigned pageCount = fileHandle.getNumberOfPages();

        if(pageCount > 0) {
            // Try to insert into the lastest page
            RecordPageHandle lastPage(fileHandle, pageCount - 1);
            if (lastPage.hasEnoughSpaceForRecord(recordLen)) {
                availPageIndex = pageCount - 1;
                return 0;
            }

            // Traverse all pages to find an available page
            for (PageNum i = 0; i < pageCount - 1; i++) {
                RecordPageHandle page(fileHandle, i);
                // Find an available page
                if (page.hasEnoughSpaceForRecord(recordLen)) {
                    availPageIndex = i;
                    return 0;
                }
            }
        }

        // No available page. Append a new page
        ret = fileHandle.appendPage(buffer);
        if(ret) {
            LOG(ERROR) << "Fail to append a new page @ RecordBasedFileManager::findAvailPage" << std::endl;
            return ret;
        }
        availPageIndex = fileHandle.getNumberOfPages() - 1;
        return 0;
    }

} // namespace PeterDB

