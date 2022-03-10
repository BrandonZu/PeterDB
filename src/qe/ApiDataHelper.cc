#include "src/include/qe.h"

namespace PeterDB {
    int16_t ApiDataHelper::getAttrLen(uint8_t* data, int16_t pos, const Attribute& attr) {
        switch (attr.type) {
            case TypeInt: return sizeof(int32_t);
            case TypeReal: return sizeof(float);
            case TypeVarChar: return *(int32_t *)(data + pos) + sizeof(int32_t);
        }
        return 0;
    }

    int16_t ApiDataHelper::getDataLen(uint8_t* data, const std::vector<Attribute>& attrs) {
        int16_t pos = ceil(attrs.size() / 8.0);
        for(int16_t i = 0; i < attrs.size(); i++) {
            if(RecordHelper::isAttrNull(data, i)) {
                continue;
            }
            pos += getAttrLen(data, pos, attrs[i]);
        }
        return pos;
    }

    RC ApiDataHelper::buildDict(uint8_t* data, const std::vector<Attribute>& attrs, std::vector<int16_t>& dict) {
        int16_t pos = ceil(attrs.size() / 8.0);
        for(int16_t i = 0; i < attrs.size(); i++) {
            dict[i] = pos;
            if(RecordHelper::isAttrNull(data, i)) {
                continue;
            }
            pos += getAttrLen(data, pos, attrs[i]);
        }
        return 0;
    }

    RC ApiDataHelper::getRawAttr(uint8_t* data, const std::vector<Attribute>& attrs, const std::string& attrName, uint8_t* attrVal) {
        int16_t pos = ceil(attrs.size() / 8.0);
        for(int16_t i = 0; i < attrs.size(); i++) {
            if(RecordHelper::isAttrNull(data, i)) {
                if(attrs[i].name == attrName) {
                    return ERR_JOIN_ATTR_NULL;
                }
                continue;
            }
            if(attrs[i].name == attrName) {
                switch (attrs[i].type) {
                    case TypeInt:
                        memcpy(attrVal, data + pos, sizeof(int32_t));
                        break;
                    case TypeReal:
                        memcpy(attrVal, data + pos, sizeof(float));
                        break;
                    case TypeVarChar:
                        int32_t strLen;
                        memcpy(&strLen, data + pos, sizeof(int32_t));
                        memcpy(attrVal, data + pos, sizeof(int32_t) + strLen);
                        break;
                }
                break;
            }
            pos += getAttrLen(data, pos, attrs[i]);
        }
        return 0;
    }
    RC ApiDataHelper::getIntAttr(uint8_t* data, const std::vector<Attribute>& attrs, const std::string& attrName, int32_t& attrVal) {
        return getRawAttr(data, attrs, attrName, (uint8_t *) &attrVal);
    }
    RC ApiDataHelper::getFloatAttr(uint8_t* data, const std::vector<Attribute>& attrs, const std::string& attrName, float& attrVal) {
        return getRawAttr(data, attrs, attrName, (uint8_t *) &attrVal);
    }
    RC ApiDataHelper::getStrAttr(uint8_t* data, const std::vector<Attribute>& attrs, const std::string& attrName, std::string& attrVal) {
        uint8_t tmp[PAGE_SIZE];
        RC ret = getRawAttr(data, attrs, attrName, tmp);
        if(ret) return ret;
        int32_t strLen;
        strLen = *(int32_t *)tmp;
        attrVal = std::string((char *)(tmp + sizeof(int32_t)), strLen);
        return 0;
    }
}
