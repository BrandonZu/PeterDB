#include "src/include/rbfm.h"

namespace PeterDB {
    RBFM_ScanIterator::RBFM_ScanIterator() {
        conditionAttrValue = new uint8_t[4096];
    }

    RBFM_ScanIterator::~RBFM_ScanIterator() {
        if(conditionAttrValue) {
            delete[] conditionAttrValue;
        }
    }

    RC RBFM_ScanIterator::open(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                               const std::string &conditionAttribute, const CompOp compOp, const void *value,
                               const std::vector<std::string> &attributeNames) {
        this->fileHandle = fileHandle;
        this->recordDesc = recordDescriptor;

        this->selectedAttrIndex.clear();    // In case not closed since last time
        for(const std::string& attrName: attributeNames) {
            for(uint32_t i = 0; i < recordDescriptor.size(); i++) {
                if(recordDescriptor[i].name == attrName) {
                    this->selectedAttrIndex.push_back(i);
                }
            }
        }

        this->curPageIndex = 0;     // Page index starts from 0
        this->curSlotIndex = 0;     // Set slot index to 0; Next round it will begin searching at 1

        this->compOp = compOp;

        if(compOp == NO_OP)  {
            conditionAttrIndex = -1;
        }
        else {
            conditionAttrIndex = -1;
            for(uint32_t i = 0; i < recordDescriptor.size(); i++) {
                if(recordDescriptor[i].name == conditionAttribute) {
                    conditionAttr = recordDescriptor[i];
                    conditionAttrIndex = i;
                    break;
                }
            }

            if(conditionAttrIndex == -1 || !value) {
                return ERR_SCAN_INVALID_CONDITION_ATTR;
            }

            switch (conditionAttr.type) {
                case TypeInt:
                    memcpy(conditionAttrValue, (uint8_t *)value, sizeof(int32_t));
                    break;
                case TypeReal:
                    memcpy(conditionAttrValue, (uint8_t *)value, sizeof(float));
                    break;
                case TypeVarChar:
                    memcpy(&conditionStrLen, (uint8_t *)value, sizeof(conditionStrLen));
                    memcpy(conditionAttrValue, (uint8_t *)value + sizeof(conditionStrLen), conditionStrLen);
                    break;
                default:
                    LOG(ERROR) << "Attribute Type not supported!" << std::endl;
                    return ERR_ATTRIBUTE_NOT_SUPPORT;
            }
        }

        return 0;
    }

    RC RBFM_ScanIterator::close() {
        recordDesc.clear();
        selectedAttrIndex.clear();
        bzero(recordByteSeq, PAGE_SIZE);
        return 0;
    }

    RC RBFM_ScanIterator::getNextRecord(RID &recordRid, void *data) {
        RC ret = 0;
        if(curPageIndex >= fileHandle.getNumberOfPages()) {
            LOG(ERROR) << "Page not exist! @ RBFM_ScanIterator::getNextRecord" << std::endl;
            return RBFM_EOF;
        }
        // Try to find next record
        uint8_t attrData[PAGE_SIZE];
        int16_t attrLen;
        while(curPageIndex < fileHandle.getNumberOfPages()) {
            RecordPageHandle curPageHandle(fileHandle, curPageIndex);
            ret = curPageHandle.getNextRecord(curSlotIndex, recordByteSeq, recordLen);
            if(ret) {
                curPageIndex++;     // No next record in current page, go to next page
                curSlotIndex = 0;   // Reset slot index to 0, so that next round it will begin searching at 1
                continue;
            }

            if(compOp == NO_OP) {
                break;      // No comparison
            }
            else if(curPageHandle.isAttrNull(curSlotIndex, conditionAttrIndex)) {
                continue;   // All comparison operations with NULL is FALSE
            }
            else {
                // Get comparison attribute value and check if attribute meets condition
                curPageHandle.getRecordAttr(curSlotIndex, conditionAttrIndex, attrData);
                attrLen = curPageHandle.getAttrLen(curSlotIndex, conditionAttrIndex);
                if (isRecordMeetCondition(attrData, attrLen)) {
                    break;
                }
            }
        }
        if(curPageIndex >= fileHandle.getNumberOfPages()) {
            return RBFM_EOF;
        }

        // Transform selected attributes byte seq into api format
        recordRid.pageNum = curPageIndex;
        recordRid.slotNum = curSlotIndex;

        ret = RecordHelper::recordByteSeqToAPIFormat(recordByteSeq, recordDesc, selectedAttrIndex, (uint8_t *)data);
        if(ret) {
            LOG(ERROR) << "Fail to tranform byte seq to api format @ RBFM_ScanIterator::getNextRecord" << std::endl;
            return RBFM_EOF;
        }
        return 0;
    }

    bool RBFM_ScanIterator::isRecordMeetCondition(uint8_t attrData[], int16_t attrLen) {
        if(compOp == NO_OP) {
            return true;
        }
        if(attrLen == RECORD_ATTR_NULL_ENDPOS) {
            return false;
        }

        bool meetCondition = false;
        if(conditionAttr.type == TypeInt) {
            int oper1, oper2;
            memcpy(&oper1, attrData, sizeof(int32_t));
            memcpy(&oper2, conditionAttrValue, sizeof(int32_t));
            switch (compOp) {
                case EQ_OP:
                    meetCondition = oper1 == oper2;
                    break;
                case LT_OP:
                    meetCondition = oper1 < oper2;
                    break;
                case LE_OP:
                    meetCondition = oper1 <= oper2;
                    break;
                case GT_OP:
                    meetCondition = oper1 > oper2;
                    break;
                case GE_OP:
                    meetCondition = oper1 >= oper2;
                    break;
                case NE_OP:
                    meetCondition = oper1 != oper2;
                    break;
                default:
                    LOG(ERROR) << "Comparison Operator Not Supported!" << std::endl;
                    return false;
            }
        }
        else if(conditionAttr.type == TypeReal) {
            float oper1, oper2;
            memcpy(&oper1, attrData, sizeof(float));
            memcpy(&oper2, conditionAttrValue, sizeof(float));
            switch (compOp) {
                case EQ_OP:
                    meetCondition = oper1 == oper2;
                    break;
                case LT_OP:
                    meetCondition = oper1 < oper2;
                    break;
                case LE_OP:
                    meetCondition = oper1 <= oper2;
                    break;
                case GT_OP:
                    meetCondition = oper1 > oper2;
                    break;
                case GE_OP:
                    meetCondition = oper1 >= oper2;
                    break;
                case NE_OP:
                    meetCondition = oper1 != oper2;
                    break;
                default:
                    LOG(ERROR) << "Comparison Operator Not Supported!" << std::endl;
                    return false;
            }
        }
        else if(conditionAttr.type == TypeVarChar) {
            std::string oper1((char *)attrData, attrLen);
            std::string oper2((char *)conditionAttrValue, conditionStrLen);
            switch (compOp) {
                case EQ_OP:
                    meetCondition = oper1 == oper2;
                    break;
                case LT_OP:
                    meetCondition = oper1 < oper2;
                    break;
                case LE_OP:
                    meetCondition = oper1 <= oper2;
                    break;
                case GT_OP:
                    meetCondition = oper1 > oper2;
                    break;
                case GE_OP:
                    meetCondition = oper1 >= oper2;
                    break;
                case NE_OP:
                    meetCondition = oper1 != oper2;
                    break;
                default:
                    LOG(ERROR) << "Comparison Operator Not Supported!" << std::endl;
                    return false;
            }
        }
        return meetCondition;
    }

}
