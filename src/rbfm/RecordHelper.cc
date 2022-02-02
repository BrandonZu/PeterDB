#include "src/include/rbfm.h"

namespace PeterDB {
    // Record Format
    // Mask(short) | AttrNum(short) | Directory:Attr Offsets, 2 * AttrNum | Attr1 | Attr2 | Attr3 | ... | AttrN |
    RC RecordHelper::APIFormatToRecordByteSeq(char* apiData, const std::vector<Attribute> &attrs,
                                              char* byteSeq, short& recordLen) {
        // 0. Write Mask: 0x00 Record; 0x01 Pointer
        short mask = MASK_RECORD;
        memcpy(byteSeq, &mask, sizeof(mask));

        // 1. Write Attribute Number
        short attrNum = attrs.size();
        memcpy(byteSeq + sizeof(mask), &attrNum, sizeof(attrNum));

        // 2. Write Directory
        short dictOffset = sizeof(mask) + sizeof(attrNum), dictLen = attrNum * sizeof(short);
        short valOffset = dictOffset + dictLen;
        short dictPos = dictOffset, valPos = valOffset;
        short rawDataPos = ceil(attrNum / 8.0);

        // Fill each attribute's dict
        for(short i = 0; i < attrNum; i++, dictPos += sizeof(short)) {
            if(isAttrNull(apiData, i)) {
                short attrEndPos = RECORD_ATTR_NULL_ENDPOS;
                memcpy(byteSeq + dictPos, &attrEndPos, sizeof(attrEndPos));
                continue;
            }
            switch(attrs[i].type) {
                case TypeInt:
                case TypeReal:
                    // Copy Attr Value
                    memcpy(byteSeq + valPos, apiData + rawDataPos, attrs[i].length);
                    rawDataPos += attrs[i].length;
                    valPos += attrs[i].length;
                    // Set Attr Directory
                    memcpy(byteSeq + dictPos, &valPos, sizeof(short));
                    break;

                case TypeVarChar:
                    unsigned strLen;
                    // Get String Length
                    memcpy(&strLen, apiData + rawDataPos, sizeof(unsigned));
                    rawDataPos += sizeof(unsigned);
                    // Copy String Value
                    memcpy(byteSeq + valPos, apiData + rawDataPos, strLen);
                    rawDataPos += strLen;
                    valPos += strLen;
                    // Set Attr Directory
                    memcpy(byteSeq + dictPos, &valPos, sizeof(short));
                    break;

                default:
                    LOG(ERROR) << "Attribute Type not supported." << std::endl;
                    return ERR_ATTRIBUTE_NOT_SUPPORT;
            }
        }
        recordLen = valPos;
        return 0;
    }

    RC RecordHelper::recordByteSeqToAPIFormat(char record[], const std::vector<Attribute> &recordDescriptor,
                                              std::vector<uint32_t> &selectedAttrIndex, char* apiData) {
        int16_t attrNum = selectedAttrIndex.size();
        std::unordered_set<uint32_t> selectedAttrSet(selectedAttrIndex.begin(), selectedAttrIndex.end());

        // 1. Set Null Byte
        int16_t nullByteNum = ceil(attrNum / 8.0);
        uint8_t nullByte[nullByteNum];
        for(int16_t i = 0; i < nullByteNum; i++) {
            nullByte[i] = 0;
        }
        memcpy(apiData, nullByte, nullByteNum);

        // 2. Write Attr Values
        int16_t apiDataPos = nullByteNum;

        int16_t attrDictOffset = RECORD_MASK_LEN + RECORD_ATTRNUM_LEN, attrDictPos = attrDictOffset;
        int16_t attrValOffset = attrDictOffset + attrNum * RECORD_ATTR_ENDPOS_LEN, attrValPos = attrValOffset;

        // Last Not Null Attr's End Pos - Initial Value: attrValOffset
        int16_t prevAttrEndPos = attrValOffset;
        int16_t curAttrEndPos;
        for(int16_t i = 0; i < attrNum; i++, attrDictPos += RECORD_ATTR_ENDPOS_LEN) {
            if(selectedAttrSet.find(i) == selectedAttrSet.end()) {
                continue;   // Ignore attributes not in selected list
            }

            memcpy(&curAttrEndPos, record + attrDictPos, RECORD_ATTR_ENDPOS_LEN);

            if(curAttrEndPos == RECORD_ATTR_NULL_ENDPOS) {
                setAttrNull(apiData, i);    // Null Attr -> Set Null bit to 1
                continue;
            }

            // Attributes not null
            switch (recordDescriptor[i].type) {
                case TypeInt:
                    memcpy(apiData + apiDataPos, record + attrValPos, sizeof(TypeInt));
                    attrValPos += sizeof(TypeReal);
                    apiDataPos += sizeof(TypeReal);
                    break;
                case TypeReal:
                    memcpy(apiData + apiDataPos, record + attrValPos, sizeof(TypeReal));
                    attrValPos += sizeof(TypeReal);
                    apiDataPos += sizeof(TypeReal);
                    break;
                case TypeVarChar:
                    // Write String Len
                    uint32_t strLen;
                    strLen = curAttrEndPos - prevAttrEndPos;
                    memcpy(apiData + apiDataPos, &strLen, sizeof(uint32_t));
                    apiDataPos += sizeof(uint32_t);
                    // Write String Val
                    memcpy(apiData + apiDataPos, record + attrValPos, strLen);
                    attrValPos += strLen;
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

    bool RecordHelper::isAttrNull(char* data, unsigned index) {
        unsigned byteIndex = index / 8;
        unsigned bitIndex = index % 8;
        char tmp = data[byteIndex];
        return (tmp >> (7 - bitIndex)) & 0x1;
    }

    void RecordHelper::setAttrNull(char* data, unsigned index) {
        unsigned byteIndex = index / 8;
        unsigned bitIndex = index % 8;
        data[byteIndex] = data[byteIndex] | (0x1 << (7 - bitIndex));
    }

}
