#include "src/include/rm.h"

namespace PeterDB {
    CatalogColumnsRecord::CatalogColumnsRecord() {
        tableID = columnType = columnLen = columnPos = CATALOG_COLUMNS_ATTR_NULL;
    }

    CatalogColumnsRecord::~CatalogColumnsRecord() = default;

    CatalogColumnsRecord::CatalogColumnsRecord(int32_t id, const std::string& name, int32_t type, int32_t length, int32_t pos) {
        tableID = id;
        columnName = name;
        columnType = type;
        columnLen = length;
        columnPos = pos;
    }

    CatalogColumnsRecord::CatalogColumnsRecord(uint8_t* apiData, const std::vector<std::string>& attrNames) {
        constructFromAPIFormat(apiData, attrNames);
    }

    RC CatalogColumnsRecord::constructFromAPIFormat(uint8_t *apiData, const std::vector<std::string> &attrNames) {
        std::unordered_set<std::string> attrSet(attrNames.begin(), attrNames.end());
        uint32_t nullByteNum = ceil(CATALOG_COLUMNS_ATTR_NUM / 8.0);
        int16_t apiDataPos = nullByteNum;
        // Table ID
        if(attrSet.find(CATALOG_COLUMNS_TABLEID) != attrSet.end()) {
            memcpy(&tableID, apiData + apiDataPos, sizeof(tableID));
            apiDataPos += sizeof(tableID);
        }
        else {
            tableID = CATALOG_COLUMNS_ATTR_NULL;
        }
        // Column Name
        if(attrSet.find(CATALOG_COLUMNS_COLUMNNAME) != attrSet.end()) {
            int32_t colNameLen;
            memcpy(&colNameLen, apiData + apiDataPos, sizeof(colNameLen));
            apiDataPos += sizeof(colNameLen);
            uint8_t tmpStr[colNameLen];
            memcpy(tmpStr, apiData + apiDataPos, colNameLen);
            apiDataPos += colNameLen;
            columnName.assign((char *)tmpStr, colNameLen);
        }
        else {
            columnName.clear();
        }
        // Column Type
        if(attrSet.find(CATALOG_COLUMNS_COLUMNTYPE) != attrSet.end()) {
            memcpy(&columnType, apiData + apiDataPos, sizeof(columnType));
            apiDataPos += sizeof(columnType);
        }
        else {
            columnType = CATALOG_COLUMNS_ATTR_NULL;
        }
        // Column Length
        if(attrSet.find(CATALOG_COLUMNS_COLUMNLENGTH) != attrSet.end()) {
            memcpy(&columnLen, apiData + apiDataPos, sizeof(columnLen));
            apiDataPos += sizeof(columnLen);
        }
        else {
            columnLen = CATALOG_COLUMNS_ATTR_NULL;
        }
        // Column Position
        if(attrSet.find(CATALOG_COLUMNS_COLUMNPOS) != attrSet.end()) {
            memcpy(&columnPos, apiData + apiDataPos, sizeof(columnPos));
            apiDataPos += sizeof(columnPos);
        }
        else {
            columnPos = CATALOG_COLUMNS_ATTR_NULL;
        }
        return 0;
    }

    RC CatalogColumnsRecord::getRecordAPIFormat(uint8_t* apiData) {
        // NUll Byte
        uint32_t nullByteNum = ceil(CATALOG_COLUMNS_ATTR_NUM / 8.0);
        uint8_t nullByte[nullByteNum];
        bzero(nullByte, nullByteNum);
        memcpy(apiData, nullByte, nullByteNum);

        int16_t apiDataPos = nullByteNum;
        // Table ID
        memcpy(apiData + apiDataPos, &tableID, sizeof(tableID));
        apiDataPos += sizeof(tableID);
        // Attr Name
        int32_t colNameLen = columnName.size();
        memcpy(apiData + apiDataPos, &colNameLen, sizeof(colNameLen));
        apiDataPos += sizeof(colNameLen);
        memcpy(apiData + apiDataPos, columnName.c_str(), colNameLen);
        apiDataPos += colNameLen;
        // Attr Type
        memcpy(apiData + apiDataPos, &columnType, sizeof(columnType));
        apiDataPos += sizeof(columnType);
        // Attr Length
        memcpy(apiData + apiDataPos, &columnLen, sizeof(columnLen));
        apiDataPos += sizeof(columnLen);
        // Attr Pos
        memcpy(apiData + apiDataPos, &columnPos, sizeof(columnPos));
        apiDataPos += sizeof(columnPos);
        return 0;
    }
}
