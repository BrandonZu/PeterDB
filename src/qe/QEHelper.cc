#include "src/include/qe.h"

namespace PeterDB {
    RC QEHelper::concatRecords(uint8_t* output, uint8_t* outerRecord, const std::vector<Attribute>& outerAttr,
                               uint8_t* innerRecord, const std::vector<Attribute>& innerAttr) {
        int16_t nullByteLen = ceil((outerAttr.size() + innerAttr.size()) / 8.0);
        int16_t pos = nullByteLen;
        bzero((uint8_t *)output, nullByteLen);
        for(int16_t i = 0; i < outerAttr.size(); i++) {
            if(RecordHelper::isAttrNull(outerRecord, i)) {
                RecordHelper::setAttrNull((uint8_t *)output, i);
            }
        }
        for(int16_t i = 0 ; i < innerAttr.size(); i++) {
            if(RecordHelper::isAttrNull(innerRecord, i)) {
                RecordHelper::setAttrNull((uint8_t *)output, i + outerAttr.size());
            }
        }
        int16_t outerNullByteLen = ceil(outerAttr.size() / 8.0);
        int16_t outerCopyLen = ApiDataHelper::getDataLen(outerRecord, outerAttr) - outerNullByteLen;
        int16_t innerNullByteLen = ceil(innerAttr.size() / 8.0);
        int16_t innerCopyLen = ApiDataHelper::getDataLen(innerRecord, innerAttr) - innerNullByteLen;
        memcpy((uint8_t *)output + pos, outerRecord + outerNullByteLen, outerCopyLen);
        pos += outerCopyLen;
        memcpy((uint8_t *)output + pos, innerRecord + innerNullByteLen, innerCopyLen);
        pos += innerCopyLen;
        return 0;
    }

    bool QEHelper::isSameKey(uint8_t* key1, uint8_t* key2, AttrType& type) {
        switch (type) {
            case TypeInt:
                return memcmp(key1, key2, sizeof(int32_t)) == 0;
                break;
            case TypeReal:
                return memcmp(key1, key2, sizeof(float)) == 0;
                break;
            case TypeVarChar:
                int32_t strLen1, strLen2;
                strLen1 = *(int32_t *)key1;
                strLen2 = *(int32_t *)key2;
                return strLen1 == strLen2 && memcmp(key1, key2, sizeof(int32_t) + strLen1) == 0;
                break;
        }
        return false;
    }
}