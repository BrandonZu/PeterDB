#include "src/include/ix.h"

namespace PeterDB {
    IXPageHandle::IXPageHandle(IXFileHandle& fileHandle, uint32_t page): ixFileHandle(fileHandle), pageNum(page) {
        ixFileHandle.readPage(pageNum, data);
        memcpy(origin, data, PAGE_SIZE);

        freeBytePtr = getFreeBytePointerFromData();
        pageType = getPageTypeFromData();
        counter = getCounterFromData();
    }

    // New Page
    IXPageHandle::IXPageHandle(IXFileHandle& fileHandle, uint32_t page, int16_t type, int16_t freeByte, int16_t counter):
        ixFileHandle(fileHandle), pageNum(page), pageType(type), freeBytePtr(freeByte), counter(counter) {
        ixFileHandle.readPage(pageNum, data);
        memcpy(origin, data, PAGE_SIZE);
    }

    // New Page with data
    IXPageHandle::IXPageHandle(IXFileHandle& fileHandle, uint8_t* newData, int16_t dataLen, uint32_t page, int16_t type, int16_t freeByte, int16_t counter):
            ixFileHandle(fileHandle), pageNum(page), pageType(type), freeBytePtr(freeByte), counter(counter) {
        memcpy(data, newData, dataLen);
    }

    IXPageHandle::~IXPageHandle() {
        flushHeader();
        if(memcmp(origin, data, PAGE_SIZE) != 0) {
            ixFileHandle.writePage(pageNum, data);
        }
    }

    bool IXPageHandle::isOpen() {
        return ixFileHandle.isOpen();
    }

    bool IXPageHandle::isTypeIndex() {
        return pageType == IX::PAGE_TYPE_INDEX;
    }
    bool IXPageHandle::isTypeLeaf() {
        return pageType == IX::PAGE_TYPE_LEAF;
    }

    bool IXPageHandle::isCompositeKeyMeetCompCondition(const uint8_t* key1, const RID& rid1, const uint8_t* key2, const RID& rid2, const Attribute& attr, const CompOp op) {
        switch (op) {
            case GT_OP:
                return isKeyMeetCompCondition(key1, key2, attr, GT_OP) ||
                       (isKeyMeetCompCondition(key1, key2, attr, EQ_OP) && isRidMeetCompCondition(rid1, rid2, GT_OP));
            case GE_OP:
                return isKeyMeetCompCondition(key1, key2, attr, GE_OP) ||
                       (isKeyMeetCompCondition(key1, key2, attr, EQ_OP) && isRidMeetCompCondition(rid1, rid2, GE_OP));
            case LT_OP:
                return isKeyMeetCompCondition(key1, key2, attr, LT_OP) ||
                       (isKeyMeetCompCondition(key1, key2, attr, EQ_OP) && isRidMeetCompCondition(rid1, rid2, LT_OP));
            case LE_OP:
                return isKeyMeetCompCondition(key1, key2, attr, LE_OP) ||
                       (isKeyMeetCompCondition(key1, key2, attr, EQ_OP) && isRidMeetCompCondition(rid1, rid2, LE_OP));
            case NE_OP:
                return !(isKeyMeetCompCondition(key1, key2, attr, EQ_OP) && isRidMeetCompCondition(rid1, rid2, EQ_OP));
            case EQ_OP:
                return isKeyMeetCompCondition(key1, key2, attr, EQ_OP) && isRidMeetCompCondition(rid1, rid2, EQ_OP);
            default:
                return false;
        }
        return false;
    }

    bool IXPageHandle::isKeyMeetCompCondition(const uint8_t* key1, const uint8_t* key2, const Attribute& attr, const CompOp op) {
        switch (attr.type) {
            case TypeInt:
                int32_t int1;
                int1 = getKeyInt(key1);
                int32_t int2;
                int2 = getKeyInt(key2);
                switch (op) {
                    case GT_OP:
                        return int1 > int2;
                    case GE_OP:
                        return int1 >= int2;
                    case LT_OP:
                        return int1 < int2;
                    case LE_OP:
                        return int1 <= int2;
                    case NE_OP:
                        return int1 != int2;
                    case EQ_OP:
                        return int1 == int2;
                    default:
                        return false;
                }
                break;
            case TypeReal:
                float float1;
                float1 = getKeyReal(key1);
                float float2;
                float2 = getKeyReal(key2);
                switch (op) {
                    case GT_OP:
                        return float1 > float2;
                    case GE_OP:
                        return float1 >= float2;
                    case LT_OP:
                        return float1 < float2;
                    case LE_OP:
                        return float1 <= float2;
                    case NE_OP:
                        return float1 != float2;
                    case EQ_OP:
                        return float1 == float2;
                    default:
                        return false;
                }
            case TypeVarChar:
                std::string str1;
                str1 = getKeyString(key1);
                std::string str2;
                str2 = getKeyString(key2);
                switch (op) {
                    case GT_OP:
                        return str1 > str2;
                    case GE_OP:
                        return str1 >= str2;
                    case LT_OP:
                        return str1 < str2;
                    case LE_OP:
                        return str1 <= str2;
                    case NE_OP:
                        return str1 != str2;
                    case EQ_OP:
                        return str1 == str2;
                    default:
                        return false;
                }
                break;
        }
        return false;
    }

    bool IXPageHandle::isRidMeetCompCondition(const RID& rid1, const RID& rid2, const CompOp op) {
        switch (op) {
            case GT_OP:
                return rid1.pageNum > rid2.pageNum || (rid1.pageNum == rid2.pageNum && rid1.slotNum > rid2.slotNum);
            case GE_OP:
                return rid1.pageNum >= rid2.pageNum || (rid1.pageNum == rid2.pageNum && rid1.slotNum >= rid2.slotNum);
            case LT_OP:
                return rid1.pageNum < rid2.pageNum || (rid1.pageNum == rid2.pageNum && rid1.slotNum < rid2.slotNum);
            case LE_OP:
                return rid1.pageNum <= rid2.pageNum || (rid1.pageNum == rid2.pageNum && rid1.slotNum <= rid2.slotNum);
            case NE_OP:
                return !(rid1.pageNum == rid2.pageNum && rid1.slotNum == rid2.slotNum);
            case EQ_OP:
                return rid1.pageNum == rid2.pageNum && rid1.slotNum == rid2.slotNum;
            default:
                return false;
        }
        return false;
    }

    RC IXPageHandle::shiftRecordLeft(int16_t dataNeedShiftStartPos, int16_t dist) {
        int16_t dataNeedMoveLen = freeBytePtr - dataNeedShiftStartPos;
        if(dataNeedMoveLen < 0) {
            return ERR_IMPOSSIBLE;
        }
        if(dataNeedMoveLen == 0) {
            return 0;
        }

        // Must Use Memmove! Source and Destination May Overlap
        memmove(data + dataNeedShiftStartPos - dist, data + dataNeedShiftStartPos, dataNeedMoveLen);

        return 0;
    }

    RC IXPageHandle::shiftRecordRight(int16_t dataNeedMoveStartPos, int16_t dist) {
        int16_t dataNeedMoveLen = freeBytePtr - dataNeedMoveStartPos;
        if(dataNeedMoveLen < 0) {
            return ERR_IMPOSSIBLE;
        }
        if(dataNeedMoveLen == 0) {
            return 0;
        }
        // Must Use Memmove! Source and Destination May Overlap
        memmove(data + dataNeedMoveStartPos + dist, data + dataNeedMoveStartPos, dataNeedMoveLen);

        return 0;
    }

    int16_t IXPageHandle::getKeyLen(const uint8_t* key, const Attribute &attr) {
        switch (attr.type) {
            case TypeInt:
                return sizeof(int32_t);
                break;
            case TypeReal:
                return sizeof(float);
                break;
            case TypeVarChar:
                int32_t strLen;
                memcpy(&strLen, key, sizeof(int32_t));
                return strLen + sizeof(int32_t);
                break;
            default:
                return -1;
        }
    }

    int32_t IXPageHandle::getKeyInt(const uint8_t* key) {
        return *(int32_t *)key;
    }
    float IXPageHandle::getKeyReal(const uint8_t* key) {
        return *(float *)key;
    }
    std::string IXPageHandle::getKeyString(const uint8_t* key) {
        int32_t strLen;
        memcpy(&strLen, key, sizeof(int32_t));
        std::string str((char *)(key + sizeof(int32_t)), strLen);
        return str;
    }

    int16_t IXPageHandle::getHeaderLen() {
        return IX::PAGE_TYPE_LEN + IX::PAGE_FREEBYTE_PTR_LEN + IX::PAGE_COUNTER_LEN;
    }
    void IXPageHandle::flushHeader() {
        setPageType(pageType);
        setFreeBytePointer(freeBytePtr);
        setCounter(counter);
    }

    int16_t IXPageHandle::getPageType() {
        return pageType;
    }
    int16_t IXPageHandle::getPageTypeFromData() {
        int16_t type;
        memcpy(&type, data + PAGE_SIZE - IX::PAGE_TYPE_LEN, IX::PAGE_TYPE_LEN);
        return type;
    }
    void IXPageHandle::setPageType(int16_t type) {
        memcpy(data + PAGE_SIZE - IX::PAGE_TYPE_LEN, &type, IX::PAGE_TYPE_LEN);
        this->pageType = type;
    }

    int16_t IXPageHandle::getFreeBytePointerOffset() {
        return PAGE_SIZE - IX::PAGE_TYPE_LEN - IX::PAGE_FREEBYTE_PTR_LEN;
    }
    int16_t IXPageHandle::getFreeBytePointer() {
        return freeBytePtr;
    }
    int16_t IXPageHandle::getFreeBytePointerFromData() {
        int16_t ptr;
        memcpy(&ptr, data + getFreeBytePointerOffset(), IX::PAGE_FREEBYTE_PTR_LEN);
        return ptr;
    }
    void IXPageHandle::setFreeBytePointer(int16_t ptr) {
        memcpy(data + getFreeBytePointerOffset(), &ptr, IX::PAGE_FREEBYTE_PTR_LEN);
        this->freeBytePtr = ptr;
    }

    int16_t IXPageHandle::getCounterOffset() {
        return PAGE_SIZE - IX::PAGE_TYPE_LEN - IX::PAGE_FREEBYTE_PTR_LEN - IX::PAGE_COUNTER_LEN;
    }
    int16_t IXPageHandle::getCounter() {
        return counter;
    }
    int16_t IXPageHandle::getCounterFromData() {
        int16_t counter;
        memcpy(&counter, data + getCounterOffset(), IX::PAGE_COUNTER_LEN);
        return counter;
    }
    void IXPageHandle::setCounter(int16_t counter) {
        memcpy(data + getCounterOffset(), &counter, IX::PAGE_COUNTER_LEN);
        this->counter = counter;
    }

    void IXPageHandle::getRid(const uint8_t* keyData, const Attribute& attr, RID& rid) {
        int16_t offset = getKeyLen(keyData, attr);
        memcpy(&rid.pageNum, keyData + offset, IX::PAGE_RID_PAGE_LEN);
        offset += IX::PAGE_RID_PAGE_LEN;
        memcpy(&rid.slotNum, keyData + offset, IX::PAGE_RID_SLOT_LEN);
    }

}
