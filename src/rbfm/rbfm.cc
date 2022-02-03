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
        int16_t recordLen = 0;
        uint8_t buffer[PAGE_SIZE] = {};
        ret = RecordHelper::APIFormatToRecordByteSeq((uint8_t *) data, recordDescriptor, buffer, recordLen);
        if(ret) {
            LOG(ERROR) << "Fail to Transform Record to Byte Seq @ RecordBasedFileManager::insertRecord" << std::endl;
            return ret;
        }
        uint8_t byteSeq[recordLen];
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
        ret = recordPageHandle.insertRecord(byteSeq, recordLen, rid);
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
            return ERR_FILE_NOT_OPEN;
        }
        if(rid.pageNum >= fileHandle.getNumberOfPages()) {
            LOG(ERROR) << "Target Page not exist! @ RecordBasedFileManager::readRecord" << std::endl;
            return ERR_PAGE_NOT_EXIST;
        }

        // 1. Follow the pointer to find the real record
        int curPage = rid.pageNum;
        int16_t curSlot = rid.slotNum;
        while(true) {
            RecordPageHandle curPageHandle(fileHandle, curPage);
            if(!curPageHandle.isRecordReadable(curSlot)) {
                return ERR_SLOT_NOT_EXIST_OR_DELETED;
            }

            if(!curPageHandle.isRecordPointer(curSlot)) {
                break;
            }
            curPageHandle.getRecordPointerTarget(curSlot, curPage, curSlot);
        }

        // 2. Read Record Byte Seq
        RecordPageHandle pageHandle(fileHandle, curPage);
        uint8_t recordBuffer[PAGE_SIZE] = {};
        int16_t recordLen = 0;

        ret = pageHandle.getRecordByteSeq(curSlot, recordBuffer, recordLen);
        if(ret) {
            LOG(ERROR) << "Fail to Get Record Byte Seq @ RecordBasedFileManager::readRecord" << std::endl;
            return ret;
        }

        // 3. Transform Record Byte Seq to api format
        // Select all attributes
        std::vector<uint32_t> selectedAttrIndex(recordDescriptor.size());
        for(uint32_t i = 0; i < recordDescriptor.size(); i++) {
            selectedAttrIndex[i] = i;
        }
        ret = RecordHelper::recordByteSeqToAPIFormat(recordBuffer, recordDescriptor, selectedAttrIndex, (uint8_t *) data);
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
            return ERR_FILE_NOT_OPEN;
        }
        if(rid.pageNum >= fileHandle.getNumberOfPages()) {
            LOG(ERROR) << "Page not exist in this file @ RecordBasedFileManager::deleteRecord" << std::endl;
            return ERR_PAGE_NOT_EXIST;
        }

        // 1. Follow the pointer to find the real record
        int curPage = rid.pageNum;
        int16_t curSlot = rid.slotNum;
        while(true) {
            RecordPageHandle curPageHandle(fileHandle, curPage);
            // Break the loop when real record is found
            if(!curPageHandle.isRecordReadable(curSlot)) {
                return ERR_SLOT_NOT_EXIST_OR_DELETED;
            }

            if(!curPageHandle.isRecordPointer(curSlot)) {
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
        uint8_t strBuffer[PAGE_SIZE] = {};
        unsigned dataPos = ceil(recordDescriptor.size() / 8.0);
        for(int16_t i = 0; i < recordDescriptor.size(); i++) {
            // Print Name
            out << recordDescriptor[i].name << ":" << deli;
            // Print Value
            if(RecordHelper::isAttrNull((uint8_t*)data, i)) {
                out << "NULL";
            }
            else {
                switch (recordDescriptor[i].type) {
                    case TypeInt:
                        int intVal;
                        memcpy(&intVal, (uint8_t *)data + dataPos, sizeof(int));
                        dataPos += sizeof(int);
                        out << intVal;
                        break;
                    case TypeReal:
                        float floatVal;
                        memcpy(&floatVal, (uint8_t *)data + dataPos, sizeof(float));
                        dataPos += sizeof(float);
                        out << floatVal;
                        break;
                    case TypeVarChar:
                        unsigned strLen;
                        // Get String Len
                        memcpy(&strLen, (uint8_t *)data + dataPos, sizeof(unsigned));
                        dataPos += sizeof(unsigned);
                        memcpy(strBuffer, (uint8_t *)data + dataPos, strLen);
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
            return ERR_FILE_NOT_OPEN;
        }
        if(rid.pageNum >= fileHandle.getNumberOfPages()) {
            LOG(ERROR) << "Page not exist in this file @ RecordBasedFileManager::updateRecord" << std::endl;
            return ERR_PAGE_NOT_EXIST;
        }

        // 1. Follow the pointer to find real record
        int curPageIndex = rid.pageNum;
        int16_t curSlotIndex = rid.slotNum;
        while(true) {
            RecordPageHandle curPageHandle(fileHandle, curPageIndex);
            if(!curPageHandle.isRecordReadable(curSlotIndex)) {
                return ERR_SLOT_NOT_EXIST_OR_DELETED;
            }

            if(!curPageHandle.isRecordPointer(curSlotIndex)) {
                break;
            }
            curPageHandle.getRecordPointerTarget(curSlotIndex, curPageIndex, curSlotIndex);
        }

        // 2. Transform Record Data to Byte Sequence
        int16_t recordLen = 0;
        uint8_t buffer[PAGE_SIZE] = {};
        ret = RecordHelper::APIFormatToRecordByteSeq((uint8_t *) data, recordDescriptor, buffer, recordLen);
        if(ret) {
            LOG(ERROR) << "Fail to Transform Record to Byte Seq @ RecordBasedFileManager::insertRecord" << std::endl;
            return ret;
        }
        uint8_t byteSeq[recordLen];
        memcpy(byteSeq, buffer, recordLen);

        // 3. Find available space to store new record and update
        // Case 1: new byte seq is shorter -> shift records left && update slots and free byte pointer
        // Case 2: new byte seq is longer, cur page has enough space -> shift records right && update slots and free byte pointer
        // Case 3: new byte seq is longer, no enough space in cur page -> insert record in a new page && update old record to a pointer
        PageNum pageToStore;
        RecordPageHandle curPageHandle(fileHandle, curPageIndex);
        int16_t oldRecordOffset = curPageHandle.getRecordOffset(curSlotIndex);
        int16_t oldRecordLen = curPageHandle.getRecordLen(curSlotIndex);

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
            ret = newPageHandle.insertRecord(byteSeq, recordLen, newRecordRID);
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
        RC ret = 0;
        if(!fileHandle.isOpen()) {
            LOG(ERROR) << "FileHandle NOT bound to a file! @ RecordBasedFileManager::readAttribute" << std::endl;
            return ERR_FILE_NOT_OPEN;
        }
        if(rid.pageNum >= fileHandle.getNumberOfPages()) {
            LOG(ERROR) << "Page not exist in this file @ RecordBasedFileManager::readAttribute" << std::endl;
            return ERR_PAGE_NOT_EXIST;
        }

        int attrIndex;
        for(attrIndex = 0; attrIndex < recordDescriptor.size(); attrIndex++) {
            if(recordDescriptor[attrIndex].name == attributeName) {
                break;
            }
        }
        if(attrIndex >= recordDescriptor.size()) {
            LOG(ERROR) << "Attribute not exist in recordDescripter @ RecordBasedFileManager::readAttribute" << std::endl;
            return 3;
        }

        RecordPageHandle pageHandle(fileHandle, rid.pageNum);
        ret = pageHandle.getRecordAttr(rid.slotNum, attrIndex, (uint8_t*)data);
        if(ret) {
            LOG(ERROR) << "Fail to read attribute @ RecordBasedFileManager::readAttribute" << std::endl;
            return ret;
        }
        return 0;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        RC ret = 0;
        ret = rbfm_ScanIterator.open(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
        if(ret) {
            LOG(ERROR) << "Fail to open a scanner! @ RecordBasedFileManager::scan" << std::endl;
        }
        return 0;
    }

    // Page Organizer Functions
    RC RecordBasedFileManager::findAvailPage(FileHandle& fileHandle, int16_t recordLen, PageNum& availPageIndex) {
        RC ret = 0;
        uint8_t buffer[PAGE_SIZE] = {};
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

