#include "src/include/rm.h"

using namespace PeterDB::RM;

namespace PeterDB {
    CatalogIndexesRecord::CatalogIndexesRecord() {
        tableID = CATALOG_INDEXES_ATTR_NULL;
    }

    CatalogIndexesRecord::CatalogIndexesRecord(int32_t id, const std::string& attr, const std::string& file) {
        tableID = id;
        attrName = attr;
        fileName = file;
    }

    CatalogIndexesRecord::CatalogIndexesRecord(uint8_t* apiData, const std::vector<std::string>& attrNames) {
        constructFromAPIFormat(apiData, attrNames);
    }

    CatalogIndexesRecord::~CatalogIndexesRecord() = default;

    RC CatalogIndexesRecord::constructFromAPIFormat(uint8_t* apiData, const std::vector<std::string>& attrNames) {
        std::unordered_set<std::string> attrSet(attrNames.begin(), attrNames.end());
        uint32_t nullByteNum = ceil(CATALOG_INDEXES_ATTR_NUM / 8.0);
        int16_t apiDataPos = nullByteNum;
        // Table ID
        if(attrSet.find(CATALOG_INDEXES_TABLEID) != attrSet.end()) {
            memcpy(&tableID, apiData + apiDataPos, sizeof(tableID));
            apiDataPos += sizeof(tableID);
        }
        else {
            tableID = CATALOG_TABLES_ATTR_NULL;
        }
        // Attribute Name
        if(attrSet.find(CATALOG_INDEXES_ATTRNAME) != attrSet.end()) {
            int32_t attrNameLen;
            memcpy(&attrNameLen, apiData + apiDataPos, sizeof(attrNameLen));
            apiDataPos += sizeof(attrNameLen);
            uint8_t tmpStr[attrNameLen];
            memcpy(tmpStr, apiData + apiDataPos, attrNameLen);
            apiDataPos += attrNameLen;
            attrName.assign((char *)tmpStr, attrNameLen);
        }
        else {
            attrName.clear();
        }
        // File Name
        if(attrSet.find(CATALOG_INDEXES_FILENAME) != attrSet.end()) {
            int32_t fileNameLen;
            memcpy(&fileNameLen, apiData + apiDataPos, sizeof(fileNameLen));
            apiDataPos += sizeof(fileNameLen);
            uint8_t tmpStr[fileNameLen];
            memcpy(tmpStr, apiData + apiDataPos, fileNameLen);
            apiDataPos += fileNameLen;
            fileName.assign((char *)tmpStr, fileNameLen);
        }
        else {
            fileName.clear();
        }
        return 0;
    }

    RC CatalogIndexesRecord::getRecordAPIFormat(uint8_t* apiData) {
        uint32_t nullByteNum = ceil(CATALOG_INDEXES_ATTR_NUM / 8.0);
        uint8_t nullByte[nullByteNum];
        bzero(nullByte, nullByteNum);
        memcpy(apiData, nullByte, nullByteNum);

        int16_t apiDataPos = nullByteNum;
        // Table ID
        memcpy(apiData + apiDataPos, &tableID, sizeof(tableID));
        apiDataPos += sizeof(tableID);
        // Attribute Name
        int32_t attrNameLen = attrName.length();
        memcpy(apiData + apiDataPos, &attrNameLen, sizeof(attrNameLen));
        apiDataPos += sizeof(attrNameLen);
        memcpy(apiData + apiDataPos, attrName.c_str(), attrNameLen);   // Ignore '\0' at the end
        apiDataPos += attrNameLen;
        // File name
        int32_t fileNameLen = fileName.size();
        memcpy(apiData + apiDataPos, &fileNameLen, sizeof(fileNameLen));
        apiDataPos += sizeof(fileNameLen);
        memcpy(apiData + apiDataPos, fileName.c_str(), fileNameLen);   // Ignore '\0' at the end
        apiDataPos += fileNameLen;

        return 0;
    }
}