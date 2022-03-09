#include "src/include/qe.h"

namespace PeterDB {
    Filter::Filter(Iterator *input, const Condition &condition) {
        this->iter = input;
        this->condtion = condition;
        input->getAttributes(attrs);
    }

    Filter::~Filter() = default;

    RC Filter::getNextTuple(void *data) {
        RC ret = 0;
        ret = iter->getNextTuple(data);
        if(ret) return QE_EOF;
        while(!isRecordMeetCondition((uint8_t *)data)) {
            ret = iter->getNextTuple(data);
            if(ret) return QE_EOF;
        }
        return 0;
    }

    RC Filter::getAttributes(std::vector<Attribute> &attrs) const {
        attrs = this->attrs;
        return 0;
    }

    template<typename T>
    bool Filter::performOper(const T& oper1, const T& oper2) {
        switch(condtion.op) {
            case EQ_OP: return oper1 == oper2;
            case LT_OP: return oper1 < oper2;
            case LE_OP: return oper1 <= oper2;
            case GT_OP: return oper1 > oper2;
            case GE_OP: return oper1 >= oper2;
            case NE_OP: return oper1 != oper2;
            default:
                LOG(ERROR) << "Comparison Operator Not Supported!" << std::endl;
                return false;
        }
    }

    bool Filter::isRecordMeetCondition(uint8_t * data) {
        int16_t attrNum = attrs.size();
        if(attrNum == 0) {
            return true;
        }

        int16_t nullByteLen = ceil(attrNum / 8.0);
        int16_t apiDataPos = nullByteLen;
        for(int16_t i = 0; i < attrNum; i++) {
            if(attrs[i].name == condtion.lhsAttr) {
                if(RecordHelper::isAttrNull(data, i)) {
                    return false;
                }
                switch (attrs[i].type) {
                    case TypeInt:
                        int32_t attrInt;
                        memcpy(&attrInt, data + apiDataPos, sizeof(int32_t));
                        return performOper(attrInt, *(int32_t *)condtion.rhsValue.data);
                        break;
                    case TypeReal:
                        float attrFloat;
                        memcpy(&attrFloat, data + apiDataPos, sizeof(float));
                        return performOper(attrFloat, *(float *)condtion.rhsValue.data);
                        break;
                    case TypeVarChar:
                        int32_t attrLen;
                        memcpy(&attrLen, data + apiDataPos, sizeof(int32_t));
                        apiDataPos += sizeof(int32_t);
                        std::string attrStr((char *)data + apiDataPos, attrLen);
                        return performOper(attrStr, std::string((char *)condtion.rhsValue.data + sizeof(int32_t), *(int32_t *)condtion.rhsValue.data));
                        break;
                }
            }
            else {
                switch (attrs[i].type) {
                    case TypeInt:
                        apiDataPos += sizeof(int32_t);
                        break;
                    case TypeReal:
                        apiDataPos += sizeof(float);
                        break;
                    case TypeVarChar:
                        int32_t strLen;
                        memcpy(&strLen, data + apiDataPos, sizeof(int32_t));
                        apiDataPos += sizeof(int32_t);
                        apiDataPos += strLen;
                        break;
                }
            }
        }
        return false;
    }

    Project::Project(Iterator *input, const std::vector<std::string> &attrNames) {
        iter = input;
        std::vector<Attribute> allAttrs;
        input->getAttributes(allAttrs);
        for(const std::string& attrName: attrNames) {
            for(Attribute& attr: allAttrs) {
                if(attrName == attr.name) {
                    selectedAttrs.push_back(attr);
                }
            }
        }
    }

    Project::~Project() = default;

    RC Project::getNextTuple(void * output) {
        RC ret = 0;
        ret = iter->getNextTuple(inputBuffer);
        if(ret) return QE_EOF;
        std::vector<Attribute> allAttrs;
        iter->getAttributes(allAttrs);
        // Build dict for input
        std::vector<int16_t> inputDict(allAttrs.size());
        ApiDataHelper::buildDict(inputBuffer, allAttrs, inputDict);

        // Project
        int16_t outputPos = ceil(selectedAttrs.size() / 8.0);
        for(int16_t outputIndex = 0; outputIndex < selectedAttrs.size(); outputIndex++) {
            int16_t inputIndex;
            for(inputIndex = 0; inputIndex < allAttrs.size(); inputIndex++)
                if(allAttrs[inputIndex].name == selectedAttrs[outputIndex].name)
                    break;

            if(inputIndex >= allAttrs.size())  {
                return ERR_ATTR_NOT_EXIST;
            }

            if(RecordHelper::isAttrNull(inputBuffer, inputIndex)) {
                RecordHelper::setAttrNull((uint8_t *)output, outputIndex);
                continue;
            }

            switch (allAttrs[inputIndex].type) {
                case TypeInt:
                    memcpy((uint8_t *)output + outputPos, inputBuffer + inputDict[inputIndex], sizeof(int32_t));
                    outputPos += sizeof(int32_t);
                    break;
                case TypeReal:
                    memcpy((uint8_t *)output + outputPos, inputBuffer + inputDict[inputIndex], sizeof(float));
                    outputPos += sizeof(float);
                    break;
                case TypeVarChar:
                    int32_t strLen;
                    memcpy(&strLen, inputBuffer + inputDict[inputIndex], sizeof(int32_t));
                    memcpy((uint8_t *)output + outputPos, inputBuffer + inputDict[inputIndex], sizeof(int32_t));
                    outputPos += sizeof(int32_t);
                    memcpy((uint8_t *)output + outputPos, inputBuffer + inputDict[inputIndex] + sizeof(int32_t), strLen);
                    outputPos += strLen;
                    break;
            }
        }

        return 0;
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        attrs = selectedAttrs;
        return 0;
    }

    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages) {
        outer = leftIn;
        inner = rightIn;
        cond = condition;
        hashTableMaxSize = numPages * PAGE_SIZE;
        remainSize = hashTableMaxSize;

        leftIn->getAttributes(outerAttr);
        rightIn->getAttributes(innerAttr);

        for(auto& attr: outerAttr) {
            if(attr.name == cond.lhsAttr) {
                joinAttr = attr;
                break;
            }
        }

        loadBlocks();
    }

    BNLJoin::~BNLJoin() = default;

    RC BNLJoin::loadBlocks() {
        RC ret = 0;
        remainSize = hashTableMaxSize;
        intHash.clear();
        floatHash.clear();
        strHash.clear();

        int32_t intKey;
        float floatKey;
        std::string strKey;

        while(remainSize > 0) {
            ret = outer->getNextTuple(outerLoadBuffer);
            if(ret) break;
            int16_t dataLen = ApiDataHelper::getDataLen(outerLoadBuffer, outerAttr);
            switch (joinAttr.type) {
                case TypeInt:
                    ApiDataHelper::getIntAttribute(outerLoadBuffer, outerAttr, cond.lhsAttr, intKey);
                    intHash[intKey].push_back(std::vector<uint8_t>(outerLoadBuffer, outerLoadBuffer + dataLen));
                    break;
                case TypeReal:
                    ApiDataHelper::getFloatAttribute(outerLoadBuffer, outerAttr, cond.lhsAttr, floatKey);
                    floatHash[floatKey].push_back(std::vector<uint8_t>(outerLoadBuffer, outerLoadBuffer + dataLen));
                    break;
                case TypeVarChar:
                    ApiDataHelper::getStrAttribute(outerLoadBuffer, outerAttr, cond.lhsAttr, strKey);
                    strHash[strKey].push_back(std::vector<uint8_t>(outerLoadBuffer, outerLoadBuffer + dataLen));
                    break;
            }
            remainSize -= dataLen;
        }
        if(remainSize == hashTableMaxSize) {
            return QE_EOF;
        }
        return 0;
    }

    RC BNLJoin::concatRecords(void * output, std::vector<uint8_t>& outerRecord, uint8_t* innerRecord) {
        int16_t nullByteLen = ceil((outerAttr.size() + innerAttr.size()) / 8.0);
        int16_t pos = nullByteLen;
        for(int16_t i = 0; i < outerAttr.size(); i++) {
            if(RecordHelper::isAttrNull(outerRecord.data(), i)) {
                RecordHelper::setAttrNull((uint8_t *)output, i);
            }
        }
        for(int16_t i = 0 ; i < innerAttr.size(); i++) {
            if(RecordHelper::isAttrNull(innerRecord, i)) {
                RecordHelper::setAttrNull((uint8_t *)output, i + outerAttr.size());
            }
        }
        int16_t outerNullByteLen = ceil(outerAttr.size() / 8.0);
        int16_t outerCopyLen = outerRecord.size() - outerNullByteLen;
        int16_t innerNullByteLen = ceil(innerAttr.size() / 8.0);
        int16_t innerCopyLen = ApiDataHelper::getDataLen(innerRecord, innerAttr) - innerNullByteLen;
        memcpy((uint8_t *)output + pos, outerRecord.data() + outerNullByteLen, outerCopyLen);
        pos += outerCopyLen;
        memcpy((uint8_t *)output + pos, innerRecord + innerNullByteLen, innerCopyLen);
        pos += innerCopyLen;
        return 0;
    }

    RC BNLJoin::getNextTuple(void * output) {
        RC ret = 0;
        // Remaining records with the same key
        if(hasProbe) {
            switch (joinAttr.type) {
                case TypeInt:
                    if(hashValLisPos < intHash[lastIntKey].size()) {
                        concatRecords(output, intHash[lastIntKey][hashValLisPos], innerReadBuffer);
                        hashValLisPos++;
                        return 0;
                    }
                    break;
                case TypeReal:
                    if(hashValLisPos < floatHash[lastFloatKey].size()) {
                        concatRecords(output, floatHash[lastFloatKey][hashValLisPos], innerReadBuffer);
                        hashValLisPos++;
                        return 0;
                    }
                    break;
                case TypeVarChar:
                    if(hashValLisPos < strHash[lastStrKey].size()) {
                        concatRecords(output, strHash[lastStrKey][hashValLisPos], innerReadBuffer);
                        hashValLisPos++;
                        return 0;
                    }
                    break;
            }
        }

        int32_t intKey;
        float floatKey;
        std::string strKey;
        bool isMatchFound = false;
        while(!isMatchFound) {
            ret = inner->getNextTuple(innerReadBuffer);
            if(ret) {
                // Reach inner table's end, reload blocks
                ret = loadBlocks();
                hasProbe = false;
                if(ret) {
                    return QE_EOF;  // Reload blocks fail, reach outer table's end, return QE_EOF
                }
                inner->setIterator();   // Reset inner table's iterator
                inner->getNextTuple(innerReadBuffer);
            }
            // Probe hash table in memory
            switch (joinAttr.type) {
                case TypeInt:
                    ApiDataHelper::getIntAttribute(innerReadBuffer, innerAttr, cond.rhsAttr, intKey);
                    if(intHash.find(intKey) != intHash.end()) {
                        concatRecords(output, intHash[intKey][0], innerReadBuffer);
                        lastIntKey = intKey;
                        hashValLisPos = 1;
                        hasProbe = true;
                        isMatchFound = true;
                    }
                    break;
                case TypeReal:
                    ApiDataHelper::getFloatAttribute(innerReadBuffer, innerAttr, cond.rhsAttr, floatKey);
                    if(floatHash.find(floatKey) != floatHash.end()) {
                        concatRecords(output, floatHash[floatKey][0], innerReadBuffer);
                        lastFloatKey = floatKey;
                        hashValLisPos = 1;
                        hasProbe = true;
                        isMatchFound = true;
                    }
                    break;
                case TypeVarChar:
                    ApiDataHelper::getStrAttribute(innerReadBuffer, innerAttr, cond.rhsAttr, strKey);
                    if(strHash.find(strKey) != strHash.end()) {
                        concatRecords(output, strHash[strKey][0], innerReadBuffer);
                        lastStrKey = strKey;
                        hashValLisPos = 1;
                        hasProbe = true;
                        isMatchFound = true;
                    }
                    break;
            }
        }

        return 0;
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs.insert(attrs.end(), outerAttr.begin(), outerAttr.end());
        attrs.insert(attrs.end(), innerAttr.begin(), innerAttr.end());
        return 0;
    }

    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {

    }

    INLJoin::~INLJoin() {

    }

    RC INLJoin::getNextTuple(void *data) {
        return -1;
    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned int numPartitions) {

    }

    GHJoin::~GHJoin() {

    }

    RC GHJoin::getNextTuple(void *data) {
        return -1;
    }

    RC GHJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) {

    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {

    }

    Aggregate::~Aggregate() {

    }

    RC Aggregate::getNextTuple(void *data) {
        return -1;
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }
} // namespace PeterDB
