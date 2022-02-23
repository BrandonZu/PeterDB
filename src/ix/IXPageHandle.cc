#include "src/include/ix.h"

namespace PeterDB {
    IXPageHandle::IXPageHandle(IXFileHandle& fileHandle, uint32_t page): fh(fileHandle), pageNum(page) {
        fh.readPage(pageNum, data);
        freeBytePtr = getFreeBytePointer();
        pageType = getPageType();
        counter = getCounter();
        parentPtr = getParentPtr();
    }

    IXPageHandle::IXPageHandle(IXPageHandle& pageHandle): fh(pageHandle.fh) {
        pageNum = pageHandle.pageNum;
        pageType = pageHandle.pageType;
        freeBytePtr = pageHandle.freeBytePtr;
        counter = pageHandle.counter;
        parentPtr = pageHandle.parentPtr;

        memcpy(data, pageHandle.data, PAGE_SIZE);
    }

    IXPageHandle::~IXPageHandle() {
        fh.writePage(pageNum, data);
    }

    bool IXPageHandle::isTypeIndex() {
        return pageType == IX::PAGE_TYPE_INDEX;
    }
    bool IXPageHandle::isTypeLeaf() {
        return pageType == IX::PAGE_TYPE_LEAF;
    }

    bool IXPageHandle::isKeySatisfyComparison(const uint8_t* key1, const uint8_t* key2, const Attribute& attr, const CompOp op) {
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

    RC IXPageHandle::shiftRecordRight(int16_t dataNeedShiftStartPos, int16_t dist) {
        int16_t dataNeedMoveLen = freeBytePtr - dataNeedShiftStartPos;
        if(dataNeedMoveLen < 0) {
            return ERR_IMPOSSIBLE;
        }
        if(dataNeedMoveLen == 0) {
            return 0;
        }
        // Must Use Memmove! Source and Destination May Overlap
        memmove(data + dataNeedShiftStartPos + dist, data + dataNeedShiftStartPos, dataNeedMoveLen);

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
        return IX::PAGE_TYPE_LEN + IX::PAGE_FREEBYTE_PTR_LEN + IX::PAGE_COUNTER_LEN + IX::PAGE_PARENT_PTR_LEN;
    }
    void IXPageHandle::flushHeader() {
        setPageType(pageType);
        setFreeBytePointer(freeBytePtr);
        setCounter(counter);
        setParentPtr(parentPtr);
    }

    int16_t IXPageHandle::getPageType() {
        int16_t type;
        memcpy(&type, data + PAGE_SIZE - IX::PAGE_TYPE_LEN, IX::PAGE_TYPE_LEN);
        return type;
    }
    void IXPageHandle::setPageType(int16_t type) {
        memcpy(data + PAGE_SIZE - IX::PAGE_TYPE_LEN, &type, IX::PAGE_TYPE_LEN);
    }

    int16_t IXPageHandle::getFreeBytePointerOffset() {
        return PAGE_SIZE - IX::PAGE_TYPE_LEN - IX::PAGE_FREEBYTE_PTR_LEN;
    }
    int16_t IXPageHandle::getFreeBytePointer() {
        int16_t ptr;
        memcpy(&ptr, data + getFreeBytePointerOffset(), IX::PAGE_FREEBYTE_PTR_LEN);
        return ptr;
    }
    void IXPageHandle::setFreeBytePointer(int16_t ptr) {
        memcpy(data + getFreeBytePointerOffset(), &ptr, IX::PAGE_FREEBYTE_PTR_LEN);
    }

    int16_t IXPageHandle::getCounterOffset() {
        return PAGE_SIZE - IX::PAGE_TYPE_LEN - IX::PAGE_FREEBYTE_PTR_LEN - IX::PAGE_COUNTER_LEN;
    }
    int16_t IXPageHandle::getCounter() {
        int16_t counter;
        memcpy(&counter, data + getCounterOffset(), IX::PAGE_COUNTER_LEN);
        return counter;
    }
    void IXPageHandle::setCounter(int16_t counter) {
        memcpy(data + getCounterOffset(), &counter, IX::PAGE_COUNTER_LEN);
    }
    void IXPageHandle::addCounterByOne() {
        int16_t counter = getCounter();
        setCounter(counter + 1);
    }

    int16_t IXPageHandle::getParentPtrOffset() {
        return PAGE_SIZE - IX::PAGE_TYPE_LEN - IX::PAGE_FREEBYTE_PTR_LEN - IX::PAGE_COUNTER_LEN - IX::PAGE_PARENT_PTR_LEN;
    }
    uint32_t IXPageHandle::getParentPtr() {
        uint32_t parent;
        memcpy(&parent, data + getParentPtrOffset(), IX::PAGE_PARENT_PTR_LEN);
        return parent;
    }
    void IXPageHandle::setParentPtr(uint32_t parent) {
        memcpy(data + getParentPtrOffset(), &parent, IX::PAGE_PARENT_PTR_LEN);
    }
    bool IXPageHandle::isParentPtrNull() {
        return parentPtr == IX::PAGE_PTR_NULL;
    }

}
