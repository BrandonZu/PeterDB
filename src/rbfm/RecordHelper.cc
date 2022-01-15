#include "src/include/rbfm.h"

namespace PeterDB {
    // Record Format
    // Mask(short) | AttrNum(short) | Directory:Attr Offsets, 2 * AttrNum | Attr1 | Attr2 | Attr3 | ... | AttrN |
    RC RecordHelper::rawDataToRecordByteSeq(char* rawData, const std::vector<Attribute> &attrs,
                                            char* byteSeq, RecordLen& recordLen) {
        // 0. Write Mask(Reserved)
        short mask = 0x0;
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
            if(isAttrNull(rawData, i)) {
                short attrEndPos = -1;
                memcpy(byteSeq + dictPos, &attrEndPos, sizeof(attrEndPos));
                continue;
            }
            switch(attrs[i].type) {
                case TypeInt:
                case TypeReal:
                    // Copy Attr Value
                    memcpy(byteSeq + valPos, rawData + rawDataPos, attrs[i].length);
                    rawDataPos += attrs[i].length;
                    valPos += attrs[i].length;
                    // Set Attr Directory
                    memcpy(byteSeq + dictPos, &valPos, sizeof(short));
                    break;

                case TypeVarChar:
                    unsigned strLen;
                    // Get String Length
                    memcpy(&strLen, rawData + rawDataPos, sizeof(unsigned));
                    rawDataPos += sizeof(unsigned);
                    // Copy String Value
                    memcpy(byteSeq + valPos, rawData + rawDataPos, strLen);
                    rawDataPos += strLen;
                    valPos += strLen;
                    // Set Attr Directory
                    memcpy(byteSeq + dictPos, &valPos, sizeof(short));
                    break;

                default:
                    std::cout << "Attribute Type not supported." << std::endl;
                    return -1;
            }
        }
        recordLen = valPos;
        return 0;
    }

    RC RecordHelper::recordByteSeqToRawData(char record[], const short recordLen,
                                            const std::vector<Attribute> &recordDescriptor, char* rawData) {
        short attrNum = recordDescriptor.size();
        // 1. Set NullByte
        short nullByteNum = ceil(attrNum / 8.0);
        char nullByte[nullByteNum];
        for(int i = 0; i < nullByteNum; i++) {
            nullByte[i] = 0;
        }
        memcpy(rawData, nullByte, nullByteNum);

        // 2. Write Attr Values
        short rawDataPos = nullByteNum;

        short attrDictOffset = sizeof(short) + sizeof(short), attrDictPos = attrDictOffset;
        short attrValOffset = attrDictOffset + attrNum * sizeof(short), attrValPos = attrValOffset;

        // Last Not Null Attr's End Pos - Initial Value: attrValOffset
        short prevAttrEndPos = attrValOffset;
        short curAttrEndPos;
        for(int i = 0; i < attrNum; i++, attrDictPos += sizeof(short)) {
            // Get Attr End Pos
            memcpy(&curAttrEndPos, record + attrDictPos, sizeof(short));
            // Write Attr Data
            // NUll - Don't Write
            if(curAttrEndPos < 0) {
                setAttrNull(rawData, i);
            }
            else {
                switch (recordDescriptor[i].type) {
                    case TypeInt:
                        memcpy(rawData + rawDataPos, record + attrValPos, sizeof(int));
                        attrValPos += sizeof(int);
                        rawDataPos += sizeof(int);
                        break;
                    case TypeReal:
                        memcpy(rawData + rawDataPos, record + attrValPos, sizeof(float));
                        attrValPos += sizeof(float);
                        rawDataPos += sizeof(float);
                        break;
                    case TypeVarChar:
                        // Write String Len
                        unsigned strLen;
                        strLen = curAttrEndPos - prevAttrEndPos;
                        memcpy(rawData + rawDataPos, &strLen, sizeof(unsigned));
                        rawDataPos += sizeof(unsigned);
                        // Write String Val
                        memcpy(rawData + rawDataPos, record + attrValPos, strLen);
                        attrValPos += strLen;
                        rawDataPos += strLen;
                        break;
                    default:
                        std::cout << "Data Type Not Supported" << std::endl;
                        break;
                }
            }
            // Update prevAttrEndPos if Current Attr is not NULL
            if(curAttrEndPos >= 0) {
                prevAttrEndPos = curAttrEndPos;
            }
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
