#include "src/include/rbfm.h"

namespace PeterDB {
    RBFM_ScanIterator::RBFM_ScanIterator() {
        conditionAttrValue = nullptr;
    }

    RBFM_ScanIterator::~RBFM_ScanIterator() {
        if(conditionAttrValue)
            free(conditionAttrValue);
    }

    RC RBFM_ScanIterator::open(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                               const std::string &conditionAttribute, const CompOp compOp, const void *value,
                               const std::vector<std::string> &attributeNames) {

        this->fileHandle = fileHandle;
        this->recordDesc = recordDescriptor;
        for(const std::string& attrName: attributeNames) {
            for(uint32_t i = 0; i < recordDescriptor.size(); i++) {
                if(recordDescriptor[i].name == attrName) {
                    this->selectedAttrIndex.push_back(i);
                }
            }
        }

        this->curPageIndex = 0;
        this->curSlotIndex = 1;    // Slot index start from 1

        this->compOp = compOp;
        for(uint32_t i = 0; i < recordDescriptor.size(); i++) {
            if(recordDescriptor[i].name == conditionAttribute) {
                conditionAttr = recordDescriptor[i];
                conditionAttrIndex = i;
            }
        }

        if(compOp != NO_OP) {
            switch (conditionAttr.type) {
                case TypeInt:
                    conditionAttrValue = new uint8_t(sizeof(int32_t));
                    memcpy(conditionAttrValue, value, sizeof(int32_t));
                    break;
                case TypeReal:
                    conditionAttrValue = new uint8_t(sizeof(int32_t));
                    memcpy(conditionAttrValue, value, sizeof(float));
                    break;
                case TypeVarChar:
                    uint32_t attrLen;
                    memcpy(&attrLen, value, sizeof(attrLen));
                    memcpy(conditionAttrValue, (uint8_t *)value + sizeof(attrLen), attrLen);
                    break;
                default:
                    LOG(ERROR) << "Attribute Type not supported!" << std::endl;
            }
        }

        return 0;
    }

    RC RBFM_ScanIterator::close() {
        if(this->conditionAttrValue)
            free(conditionAttrValue);
        return 0;
    }

    RC RBFM_ScanIterator::getNextRecord(RID &recordRid, void *data) {
        RC ret = 0;
        if(curPageIndex >= fileHandle.getNumberOfPages()) {
            LOG(ERROR) << "Page not exist! @ RBFM_ScanIterator::getNextRecord" << std::endl;
            return ERR_PAGE_NOT_EXIST;
        }
        // Try to find next record
        uint8_t attrData[PAGE_SIZE];
        int16_t attrLen;
        while(curPageIndex < fileHandle.getNumberOfPages()) {
            RecordPageHandle curPageHandle(fileHandle, curPageIndex);
            ret = curPageHandle.getNextRecord(curSlotIndex, recordByteSeq, recordLen);
            if(ret) {
                curPageIndex++;     // No next record in current page, go to next page
                curSlotIndex = 1;   // Reset slot index to 1
                continue;
            }
            // Store attribute value and check if attribute meets condition
            curPageHandle.getRecordAttr(curSlotIndex, conditionAttrIndex, attrData);
            attrLen = curPageHandle.getAttrLen(curSlotIndex, conditionAttrIndex);
            if(isRecordMeetCondition(attrData, attrLen)) {
                break;
            }
        }
        if(curPageIndex >= fileHandle.getNumberOfPages()) {
            LOG(ERROR) << "Next Record not exist! @ RBFM_ScanIterator::getNextRecord" << std::endl;
            return ERR_NEXT_RECORD_NOT_EXIST;
        }

        // Transform selected attributes byte seq into api format
        recordRid.pageNum = curPageIndex;
        recordRid.slotNum = curSlotIndex;

        ret = RecordHelper::recordByteSeqToAPIFormat((char *)recordByteSeq, recordDesc, selectedAttrIndex, (char *)data);
        if(ret) {
            LOG(ERROR) << "Fail to tranform byte seq to api format @ RBFM_ScanIterator::getNextRecord" << std::endl;
            return ERR_TRANSFORM_BYTESEQ_TO_APIFORMAT;
        }
        return 0;
    }

    bool RBFM_ScanIterator::isRecordMeetCondition(uint8_t attrData[], int16_t attrLen) {
        if(compOp == NO_OP) {
            return true;
        }
        if(attrLen == ATTR_NULL_LEN) {
            return false;
        }

        bool meetCondition = false;
        if(conditionAttr.type == TypeInt) {
            int oper1, oper2;
            memcpy(&oper1, attrData, sizeof(TypeInt));
            memcpy(&oper2, conditionAttrValue, sizeof(TypeInt));
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
            memcpy(&oper1, attrData, sizeof(TypeReal));
            memcpy(&oper2, conditionAttrValue, sizeof(TypeReal));
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
            std::string oper2((char *)conditionAttrValue, conditionAttr.length);
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

    RC RBFM_ScanIterator::APIFormatToAttrDict(uint8_t* apiData, std::unordered_map<std::string, void*>& attrDataList) {
        RC ret = 0;
        int16_t attrNum = selectedAttrIndex.size();
        int16_t nullByteNum = ceil(attrNum / 8.0);
        for(uint16_t i = 0; i < selectedAttrIndex.size(); i++) {
            if(RecordHelper::isAttrNull(apiData, i)) {

            }
        }
        return ret;
    }

}
