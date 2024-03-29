#include "src/include/rm.h"

using namespace PeterDB::RM;

namespace PeterDB {
    CatalogTablesRecord::CatalogTablesRecord() {
        tableID = CATALOG_TABLES_ATTR_NULL;
        tableVersion = CATALOG_TABLES_ATTR_NULL;
    }

    CatalogTablesRecord::~CatalogTablesRecord() = default;

    CatalogTablesRecord::CatalogTablesRecord(int32_t id, const std::string& name, const std::string& file, int32_t version) {
        tableID = id;
        tableName = name;
        fileName = file;
        tableVersion = version;
    }

    CatalogTablesRecord::CatalogTablesRecord(uint8_t* apiData, const std::vector<std::string>& attrNames) {
        constructFromAPIFormat(apiData, attrNames);
    }

    RC CatalogTablesRecord::constructFromAPIFormat(uint8_t* apiData, const std::vector<std::string>& attrNames) {
        std::unordered_set<std::string> attrSet(attrNames.begin(), attrNames.end());
        uint32_t nullByteNum = ceil(CATALOG_TABLES_ATTR_NUM / 8.0);
        int16_t apiDataPos = nullByteNum;
        // Table ID
        if(attrSet.find(CATALOG_TABLES_TABLEID) != attrSet.end()) {
            memcpy(&tableID, apiData + apiDataPos, sizeof(tableID));
            apiDataPos += sizeof(tableID);
        }
        else {
            tableID = CATALOG_TABLES_ATTR_NULL;
        }
        // Table Name
        if(attrSet.find(CATALOG_TABLES_TABLENAME) != attrSet.end()) {
            int32_t tableNameLen;
            memcpy(&tableNameLen, apiData + apiDataPos, sizeof(tableNameLen));
            apiDataPos += sizeof(tableNameLen);
            uint8_t tmpStr[tableNameLen];
            memcpy(tmpStr, apiData + apiDataPos, tableNameLen);
            apiDataPos += tableNameLen;
            tableName.assign((char *)tmpStr, tableNameLen);
        }
        else {
            tableName.clear();
        }
        // File Name
        if(attrSet.find(CATALOG_TABLES_FILENAME) != attrSet.end()) {
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
        // Table Version
        if(attrSet.find(CATALOG_TABLES_TABLEVERSION) != attrSet.end()) {
            memcpy(&tableVersion, apiData + apiDataPos, sizeof(tableVersion));
            apiDataPos += sizeof(tableVersion);
        }
        else {
            tableVersion = CATALOG_TABLES_ATTR_NULL;
        }
        return 0;
    }

    RC CatalogTablesRecord::getRecordAPIFormat(uint8_t* apiData) {
        uint32_t nullByteNum = ceil(CATALOG_TABLES_ATTR_NUM / 8.0);
        uint8_t nullByte[nullByteNum];
        bzero(nullByte, nullByteNum);
        memcpy(apiData, nullByte, nullByteNum);

        int16_t apiDataPos = nullByteNum;
        // Table ID
        memcpy(apiData + apiDataPos, &tableID, sizeof(tableID));
        apiDataPos += sizeof(tableID);
        // Table Name
        int32_t tableNameLen = tableName.length();
        memcpy(apiData + apiDataPos, &tableNameLen, sizeof(tableNameLen));
        apiDataPos += sizeof(tableNameLen);
        memcpy(apiData + apiDataPos, tableName.c_str(), tableNameLen);   // Ignore '\0' at the end
        apiDataPos += tableNameLen;
        // File name
        int32_t fileNameLen = fileName.size();
        memcpy(apiData + apiDataPos, &fileNameLen, sizeof(fileNameLen));
        apiDataPos += sizeof(fileNameLen);
        memcpy(apiData + apiDataPos, fileName.c_str(), fileNameLen);   // Ignore '\0' at the end
        apiDataPos += fileNameLen;
        // Table version
        memcpy(apiData + apiDataPos, &tableVersion, sizeof(tableVersion));
        apiDataPos += sizeof(tableVersion);
        return 0;
    }

}

