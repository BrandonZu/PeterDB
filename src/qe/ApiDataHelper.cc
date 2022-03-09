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

    RC ApiDataHelper::getIntAttribute(uint8_t* data, const std::vector<Attribute>& attrs, const std::string& attrName, int32_t& attrVal) {
        int16_t pos = ceil(attrs.size() / 8.0);
        for(int16_t i = 0; i < attrs.size(); i++) {
            if(RecordHelper::isAttrNull(data, i)) {
                if(attrs[i].name == attrName) {
                    return ERR_JOIN_ATTR_NULL;
                }
                continue;
            }
            if(attrs[i].name == attrName) {
                attrVal = *(int32_t *)(data + pos);
            }
            pos += getAttrLen(data, pos, attrs[i]);
        }
        return 0;
    }
    RC ApiDataHelper::getFloatAttribute(uint8_t* data, const std::vector<Attribute>& attrs, const std::string& attrName, float& attrVal) {
        int16_t pos = ceil(attrs.size() / 8.0);
        for(int16_t i = 0; i < attrs.size(); i++) {
            if(RecordHelper::isAttrNull(data, i)) {
                if(attrs[i].name == attrName) {
                    return ERR_JOIN_ATTR_NULL;
                }
                continue;
            }
            if(attrs[i].name == attrName) {
                attrVal = *(float *)(data + pos);
                return 0;
            }
            pos += getAttrLen(data, pos, attrs[i]);
        }
        return 0;
    }
    RC ApiDataHelper::getStrAttribute(uint8_t* data, const std::vector<Attribute>& attrs, const std::string& attrName, std::string& attrVal) {
        int16_t pos = ceil(attrs.size() / 8.0);
        for(int16_t i = 0; i < attrs.size(); i++) {
            if(RecordHelper::isAttrNull(data, i)) {
                if(attrs[i].name == attrName) {
                    return ERR_JOIN_ATTR_NULL;
                }
                continue;
            }
            if(attrs[i].name == attrName) {
                int32_t strLen;
                strLen = *(int32_t *)(data + pos);
                attrVal = std::string((char *)(data + pos + sizeof(int32_t)), strLen);
                return 0;
            }
            pos += getAttrLen(data, pos, attrs[i]);
        }
        return 0;
    }
}
