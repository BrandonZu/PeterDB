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
        RecordLen recordLen = 0;
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

        // 1. Get Record Byte Seq via RecordPageHandle
        RecordPageHandle pageHandle(fileHandle, rid.pageNum);
        char recordBuffer[PAGE_SIZE] = {};
        short recordLen = 0;
        ret = pageHandle.getRecordByteSeq(rid.slotNum, recordBuffer, recordLen);
        if(ret) {
            LOG(ERROR) << "Fail to Get Record Byte Seq via RecordPageHandle @ RecordBasedFileManager::readRecord" << std::endl;
            return ret;
        }
        // 2. Transform Record Byte Seq to raw data(output format)
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
        // Get Record Page Handle
        RecordPageHandle recordPageHandle(fileHandle, rid.pageNum);
        ret = recordPageHandle.deleteRecord(recordDescriptor, rid);
        if(ret) {
            LOG(ERROR) << "Fail to delete record via RecordPageHandle @ RecordBasedFileManager::deleteRecord" << std::endl;
        }
        return 0;
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
        return -1;
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
    RC RecordBasedFileManager::findAvailPage(FileHandle& fileHandle, RecordLen recordLen, PageNum& availPageIndex) {
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

