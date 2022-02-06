#include "src/include/rbfm.h"

namespace PeterDB {
    // Record Format
    // Mask(2) | AttrNum(2) | Directory:Attr End Postions, 2 * AttrNum | Attr1(2) | Attr2(2) | Attr3(2) | ... | AttrN(2) |
    RC RecordHelper::APIFormatToRecordByteSeq(uint8_t* apiData, const std::vector<Attribute> &attrs,
                                              uint8_t* byteSeq, int16_t& recordLen) {
        // 0. Write Mask: 0x00 Record; 0x01 Pointer
        int16_t mask = RECORD_MASK_REALRECORD;
        memcpy(byteSeq, &mask, RECORD_MASK_LEN);

        // 1. Write Attribute Number
        int16_t attrNum = attrs.size();
        memcpy(byteSeq + RECORD_MASK_LEN, &attrNum, RECORD_ATTRNUM_LEN);

        // 2. Write Directory
        int16_t dictOffset = RECORD_DICT_BEGIN, dictLen = attrNum * RECORD_DICT_SLOT_LEN;
        int16_t valOffset = dictOffset + dictLen;
        int16_t dictPos = dictOffset, valPos = valOffset;
        int16_t rawDataPos = ceil(attrNum / 8.0);

        // Fill each attribute's dict slot
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

    RC RecordHelper::recordByteSeqToAPIFormat(uint8_t record[], const std::vector<Attribute> &recordDescriptor,
                                              std::vector<uint32_t> &selectedAttrIndex, uint8_t* apiData) {
        int16_t allAttrNum = recordDescriptor.size();
        int16_t selectedAttrNum = selectedAttrIndex.size();
        if(selectedAttrNum == 0) {
            return 0;
        }
        std::unordered_set<uint32_t> selectedAttrSet(selectedAttrIndex.begin(), selectedAttrIndex.end());

        // 1. Set Null Byte
        int16_t nullByteNum = ceil(selectedAttrNum / 8.0);
        uint8_t nullByte[nullByteNum];
        bzero(nullByte, nullByteNum);
        memcpy(apiData, nullByte, nullByteNum);

        // 2. Write Attr Values
        int16_t apiDataPos = nullByteNum;

        int16_t attrDictOffset = RECORD_MASK_LEN + RECORD_ATTRNUM_LEN, attrDictPos = attrDictOffset;
        int16_t attrValOffset = attrDictOffset + allAttrNum * RECORD_ATTR_ENDPOS_LEN;

        // Last Not Null Attr's End Pos - Initial Value: attrValOffset
        int16_t prevAttrEndPos = attrValOffset;
        for(int16_t i = 0; i < allAttrNum; i++, attrDictPos += RECORD_ATTR_ENDPOS_LEN) {
            int16_t curAttrEndPos;
            memcpy(&curAttrEndPos, record + attrDictPos, RECORD_ATTR_ENDPOS_LEN);

            if(selectedAttrSet.find(i) == selectedAttrSet.end()) {
                if(curAttrEndPos != RECORD_ATTR_NULL_ENDPOS) {
                    prevAttrEndPos = curAttrEndPos;
                }
                continue;
            }

            if(curAttrEndPos == RECORD_ATTR_NULL_ENDPOS) {
                setAttrNull(apiData, i);    // Null Attr -> Set Null bit to 1
                continue;
            }

            switch (recordDescriptor[i].type) {
                case TypeInt:
                    memcpy(apiData + apiDataPos, record + prevAttrEndPos, sizeof(TypeInt));
                    apiDataPos += sizeof(TypeReal);
                    break;
                case TypeReal:
                    memcpy(apiData + apiDataPos, record + prevAttrEndPos, sizeof(TypeReal));
                    apiDataPos += sizeof(TypeReal);
                    break;
                case TypeVarChar:
                    // Write String Len
                    uint32_t strLen;
                    strLen = curAttrEndPos - prevAttrEndPos;
                    memcpy(apiData + apiDataPos, &strLen, APIRECORD_STRLEN_LEN);
                    apiDataPos += APIRECORD_STRLEN_LEN;
                    // Write String Val
                    memcpy(apiData + apiDataPos, record + prevAttrEndPos, strLen);
                    apiDataPos += strLen;
                    break;
                default:
                    LOG(ERROR) << "Data Type Not Supported" << std::endl;
                    break;
            }
            // Update previous attr end pos pointer
            prevAttrEndPos = curAttrEndPos;
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

}
