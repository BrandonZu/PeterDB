#include "src/include/rbfm.h"

namespace PeterDB {
    // Record Format
    // Mask(1) | Version(1) | AttrNum(2) | Directory:Attr End Postions, 2 * AttrNum | Attr1(2) | Attr2(2) | Attr3(2) | ... | AttrN(2) |
    RC RecordHelper::APIFormatToRecordByteSeq(uint8_t* apiData, const std::vector<Attribute> &attrs,
                                              uint8_t* byteSeq, int16_t& recordLen) {
        return APIFormatToRecordByteSeq(RECORD_VERSION_INITIAL, apiData, attrs, byteSeq, recordLen);
    }

    RC RecordHelper::APIFormatToRecordByteSeq(int8_t version, uint8_t * apiData, const std::vector<Attribute> &attrs,
                                              uint8_t* byteSeq, int16_t& recordLen) {
        // 0. Write Mask: 0x0 Record; 0x1 Pointer
        int8_t mask = RECORD_MASK_REALRECORD;
        memcpy(byteSeq, &mask, RECORD_MASK_LEN);

        // 1. Write Version
        memcpy(byteSeq + RECORD_MASK_LEN, &version, RECORD_VERSION_LEN);

        // 2. Write Attribute Number
        int16_t attrNum = attrs.size();
        memcpy(byteSeq + RECORD_MASK_LEN + RECORD_VERSION_LEN, &attrNum, RECORD_ATTRNUM_LEN);

        // 3. Write Directory
        int16_t dictOffset = RECORD_DICT_BEGIN, dictLen = attrNum * RECORD_DICT_SLOT_LEN;
        int16_t valOffset = dictOffset + dictLen;
        int16_t dictPos = dictOffset, valPos = valOffset;
        int16_t rawDataPos = ceil(attrNum / 8.0);

        // 4. Fill each attribute's dict slot
        for(int16_t i = 0; i < attrNum; i++, dictPos += RECORD_DICT_SLOT_LEN) {
            if(isAttrNull(apiData, i)) {
                int16_t attrEndPos = RECORD_ATTR_NULL_ENDPOS;
                memcpy(byteSeq + dictPos, &attrEndPos, RECORD_DICT_SLOT_LEN);
                continue;
            }
            switch(attrs[i].type) {
                case TypeInt:
                case TypeReal:
                    // Copy Attr Value
                    memcpy(byteSeq + valPos, apiData + rawDataPos, attrs[i].length);
                    rawDataPos += attrs[i].length;
                    valPos += attrs[i].length;
                    // Write the end position into the dict slot
                    memcpy(byteSeq + dictPos, &valPos, RECORD_DICT_SLOT_LEN);
                    break;

                case TypeVarChar:
                    int32_t strLen;
                    // Get String Length
                    memcpy(&strLen, apiData + rawDataPos, APIRECORD_STRLEN_LEN);
                    rawDataPos += APIRECORD_STRLEN_LEN;
                    // Copy String Value
                    memcpy(byteSeq + valPos, apiData + rawDataPos, strLen);
                    rawDataPos += strLen;
                    valPos += strLen;
                    // Set Attr Directory
                    memcpy(byteSeq + dictPos, &valPos, RECORD_DICT_SLOT_LEN);
                    break;

                default:
                    LOG(ERROR) << "Attribute Type not supported." << std::endl;
                    return ERR_ATTRIBUTE_NOT_SUPPORT;
            }
        }
        recordLen = valPos;
        return 0;
    }

    RC RecordHelper::recordByteSeqToAPIFormat(uint8_t byteSeq[], const std::vector<Attribute> &recordDescriptor,
                                              std::vector<uint32_t> &selectedAttrIndex, uint8_t* apiData) {
        int16_t allAttrNum = recordDescriptor.size();
        int16_t selectedAttrNum = selectedAttrIndex.size();
        if(selectedAttrNum == 0) {
            return 0;
        }

        // 1. Set Null Byte
        int16_t nullByteNum = ceil(selectedAttrNum / 8.0);
        uint8_t nullByte[nullByteNum];
        bzero(nullByte, nullByteNum);
        memcpy(apiData, nullByte, nullByteNum);

        // 2. Write Attr Values
        int16_t apiDataPos = nullByteNum;
        // Last Not Null Attr's End Pos - Initial Value: attrValOffset
        for(int16_t i = 0; i < selectedAttrNum; i++) {
            int16_t attrIndex = selectedAttrIndex[i];
            int16_t attrEndPos = getAttrEndPos(byteSeq, attrIndex);
            int16_t attrBeginPos = getAttrBeginPos(byteSeq, attrIndex);

            if(attrEndPos == RECORD_ATTR_NULL_ENDPOS) {
                setAttrNull(apiData, i);    // Null Attr -> Set Null bit to 1
                continue;
            }

            switch (recordDescriptor[attrIndex].type) {
                case TypeInt:
                    memcpy(apiData + apiDataPos, byteSeq + attrBeginPos, sizeof(int32_t));
                    apiDataPos += sizeof(int32_t);
                    break;
                case TypeReal:
                    memcpy(apiData + apiDataPos, byteSeq + attrBeginPos, sizeof(float));
                    apiDataPos += sizeof(float);
                    break;
                case TypeVarChar:
                    // Write String Len
                    uint32_t strLen;
                    strLen = attrEndPos - attrBeginPos;
                    memcpy(apiData + apiDataPos, &strLen, APIRECORD_STRLEN_LEN);
                    apiDataPos += APIRECORD_STRLEN_LEN;
                    // Write String Val
                    memcpy(apiData + apiDataPos, byteSeq + attrBeginPos, strLen);
                    apiDataPos += strLen;
                    break;
                default:
                    LOG(ERROR) << "Data Type Not Supported" << std::endl;
                    break;
            }
        }
        return 0;
    }

    bool RecordHelper::isAttrNull(uint8_t* data, uint32_t index) {
        uint32_t byteIndex = index / 8;
        uint32_t bitIndex = index % 8;
        uint8_t tmp = data[byteIndex];
        return (tmp >> (7 - bitIndex)) & 0x1;
    }

    void RecordHelper::setAttrNull(uint8_t* data, uint32_t index) {
        uint32_t byteIndex = index / 8;
        uint32_t bitIndex = index % 8;
        data[byteIndex] = data[byteIndex] | (0x1 << (7 - bitIndex));
    }

    int16_t RecordHelper::getRecordAttrNum(uint8_t* byteSeq) {
        int16_t attrNum;
        memcpy(&attrNum, byteSeq + RECORD_MASK_LEN + RECORD_VERSION_LEN, RECORD_ATTRNUM_LEN);
        return attrNum;
    }

    int16_t RecordHelper::getAttrBeginPos(uint8_t* byteSeq, int16_t attrIndex) {
        int16_t curOffset = getAttrEndPos(byteSeq, attrIndex);
        if(curOffset == RECORD_ATTR_NULL_ENDPOS) {   // Null attribute
            return ERR_RECORD_NULL;
        }
        int16_t prevOffset = -1;
        // Looking for previous attribute that is not null
        for(int i = attrIndex - 1; i >= 0; i--) {
            prevOffset = getAttrEndPos(byteSeq, i);
            if(prevOffset != RECORD_ATTR_NULL_ENDPOS) {
                break;
            }
        }
        if(prevOffset == -1) {
            prevOffset = RECORD_DICT_BEGIN + getRecordAttrNum(byteSeq) * RECORD_DICT_SLOT_LEN;
        }
        return prevOffset;
    }

    int16_t RecordHelper::getAttrEndPos(uint8_t* byteSeq, int16_t attrIndex) {
        int16_t dictOffset = RECORD_DICT_BEGIN + attrIndex * RECORD_DICT_SLOT_LEN;
        int16_t attrEndPos;
        memcpy(&attrEndPos, byteSeq + dictOffset, RECORD_DICT_SLOT_LEN);
        return attrEndPos;
    }

}
