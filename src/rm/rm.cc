#include "src/include/rm.h"

using namespace PeterDB::RM;

namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager() = default;

    RelationManager::~RelationManager() {
        for(auto& fh: ixScanFHList) {
            delete fh;
        }
        for(auto& p: ixFHMap) {
            delete p.second;
        }
        ixScanFHList.clear();
        ixFHMap.clear();
    }

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    RC RelationManager::createCatalog() {
        RC ret;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        ret = rbfm.createFile(catalogTablesName);
        if(ret) {
            LOG(ERROR) << "Fail to create TABLES catalog! @ RelationManager::createCatalog" << std::endl;
            return ret;
        }
        ret = rbfm.createFile(catalogColumnsName);
        if(ret) {
            LOG(ERROR) << "Fail to create COLUMNS catalog! @ RelationManager::createCatalog" << std::endl;
            return ret;
        }
        ret = rbfm.createFile(catalogIndexesName);
        if(ret) {
            LOG(ERROR) << "Fail to create INDEXES catalog! @ RelationManager::createCatalog" << std::endl;
            return ret;
        }

        ret = insertTableColIntoCatalog(catalogTablesName, catalogTablesSchema);
        if(ret) {
            LOG(ERROR) << "Fail to insert TABLES metadata into catalog @ RelationManager::createCatalog" << std::endl;
            return ret;
        }
        ret = insertTableColIntoCatalog(catalogColumnsName, catalogColumnsSchema);
        if(ret) {
            LOG(ERROR) << "Fail to insert COLUMNS metadata into catalog @ RelationManager::createCatalog" << std::endl;
            return ret;
        }
        ret = insertTableColIntoCatalog(catalogIndexesName, catalogIndexesSchema);
        if(ret) {
            LOG(ERROR) << "Fail to insert INDEXES metadata into catalog @ RelationManager::createCatalog" << std::endl;
            return ret;
        }
        return 0;
    }

    RC RelationManager::deleteCatalog() {
        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        catalogTablesFH.close();
        catalogColumnsFH.close();
        catalogIndexesFH.close();
        ret = rbfm.destroyFile(catalogTablesName);
        if(ret) {
            if(ret == ERR_FILE_NOT_EXIST)
                LOG(ERROR) << "TABLES catalog file not exist! @ RelationManager::deleteCatalog";
            else
                LOG(ERROR) << "Fail to delete TABLES catalog @ RelationManager::deleteCatalog";
            return ret;
        }
        ret = rbfm.destroyFile(catalogColumnsName);
        if(ret) {
            if(ret == ERR_FILE_NOT_EXIST)
                LOG(ERROR) << "COLUMNS catalog file not exist! @ RelationManager::deleteCatalog";
            else
                LOG(ERROR) << "Fail to delete COLUMNS catalog @ RelationManager::deleteCatalog";
            return ret;
        }
        ret = rbfm.destroyFile(catalogIndexesName);
        if(ret) {
            if(ret == ERR_FILE_NOT_EXIST)
                LOG(ERROR) << "INDEXES catalog file not exist! @ RelationManager::deleteCatalog";
            else
                LOG(ERROR) << "Fail to delete INDEXES catalog @ RelationManager::deleteCatalog";
            return ret;
        }
        return 0;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        if(!isTableAccessible(tableName)) {
            return ERR_ACCESS_DENIED_SYS_TABLE;
        }
        if(!isTableNameValid(tableName)) {
            return ERR_TABLE_NAME_INVALID;
        }

        RC ret = 0;
        ret = openCatalog();
        if(ret) {
            LOG(ERROR) << "Catalog not open! @ RelationManager::createTable" << std::endl;
            return ERR_CATALOG_NOT_OPEN;
        }

        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        ret = rbfm.createFile(tableName);
        if(ret) {
            LOG(ERROR) << "Fail to create table's file! @ RelationManager::createTable" << std::endl;
            return ret;
        }

        ret = insertTableColIntoCatalog(tableName, attrs);
        if(ret) {
            LOG(ERROR) << "Fail to insert metadata into catalog @ RelationManager::createTable" << std::endl;
            return ret;
        }
        return 0;
    }

    RC RelationManager::createIndex(const std::string &tableName, const std::string &attrName) {
        if(!isTableAccessible(tableName)) {
            return ERR_ACCESS_DENIED_SYS_TABLE;
        }
        if(!isTableNameValid(tableName)) {
            return ERR_TABLE_NAME_INVALID;
        }
        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        IndexManager& ix = IndexManager::instance();

        // 1. Create Index File
        std::string ixFileName = getIndexFileName(tableName, attrName);
        ret = ix.createFile(ixFileName);
        if(ret) {
            LOG(ERROR) << "Fail to create table's file! @ RelationManager::createIndex" << std::endl;
            return ret;
        }

        // 2. Insert index metadata into INDEXES catalog
        ret = openCatalog();
        if(ret) {
            return ERR_CATALOG_NOT_OPEN;
        }
        // Get Table ID
        CatalogTablesRecord tableRecord;
        ret = getTableMetaData(tableName, tableRecord);
        if(ret) return ret;
        ret = insertIndexIntoCatalog(tableRecord.tableID, attrName, ixFileName);
        if(ret) return ret;

        // 3. Scan table and insert every record into corresponding index
        // Get all attributes and search for the target attribute
        std::vector<Attribute> attrs;
        ret = getAttributes(tableName, attrs);
        if(ret) {
            return ERR_GET_METADATA;
        }
        int32_t attr_pos = 0;
        for(attr_pos = 0; attr_pos < attrs.size(); attr_pos++) {
            if(attrs[attr_pos].name == attrName) {
                break;
            }
        }
        if(attr_pos >= attrs.size()) {
            return ERR_ATTR_NOT_EXIST;
        }

        RBFM_ScanIterator tableScanIter;
        FileHandle fh;
        ret = rbfm.openFile(tableRecord.fileName, fh);
        if(ret) return ret;
        std::vector<std::string> indexedAttr = {attrs[attr_pos].name};
        ret = rbfm.scan(fh, attrs, "", NO_OP, nullptr, {}, tableScanIter);
        if(ret) return ret;

        RID rid;
        uint8_t recordData[PAGE_SIZE] = {};
        uint8_t attrData[PAGE_SIZE] = {};
        uint8_t keyData[PAGE_SIZE] = {};
        IXFileHandle ixFileHandle;
        ret = ix.openFile(ixFileName, ixFileHandle);
        if(ret) return ret;
        while(tableScanIter.getNextRecord(rid, recordData) == 0) {
            ret = rbfm.readAttribute(fh, attrs, rid, attrs[attr_pos].name, attrData);
            if(ret) return ret;
            // Convert api format to raw key
            ret = ApiDataHelper::getRawAttr(attrData, {attrs[attr_pos]}, attrs[attr_pos].name, keyData);
            if(ret) return ret;
            ret = ix.insertEntry(ixFileHandle, attrs[attr_pos], keyData, rid);
            if(ret) return ret;
        }
        return 0;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        if(!isTableAccessible(tableName)) {
            return ERR_ACCESS_DENIED_SYS_TABLE;
        }
        if(!isTableNameValid(tableName)) {
            return ERR_TABLE_NAME_INVALID;
        }
        RC ret = 0;
        ret = openCatalog();
        if(ret) {
            LOG(ERROR) << "Catalog not open! @ RelationManager::deleteTable" << std::endl;
            return ERR_CATALOG_NOT_OPEN;
        }
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        IndexManager& ix = IndexManager::instance();
        CatalogTablesRecord tableRecord;
        ret = getTableMetaData(tableName, tableRecord);
        if(ret) {
            LOG(ERROR) << "Fail to get table meta data @ RelationManager::deleteTable" << std::endl;
            return ret;
        }

        // Get Indexes and delete all index files
        std::unordered_map<std::string, std::string> indexedAttrAndFileName;
        ret = getIndexes(tableName, indexedAttrAndFileName);
        if(ret) return ret;
        for(auto& p: indexedAttrAndFileName) {
            ret = deleteIndexFromCatalog(tableRecord.tableID, p.first);
            if(ret) return ret;
            ret = ix.destroyFile(p.second);
            if(ret) return ret;
        }

        // Delete index metadata
        ret = deleteIndexFromCatalog(tableRecord.tableID);
        if(ret) return ret;

        // Delete table metadata
        ret = deleteTableColFromCatalog(tableRecord.tableID);
        if(ret) return ret;

        // Delete table file
        ret = rbfm.destroyFile(tableName);
        if(ret) {
            LOG(ERROR) << "Fail to destroy table file! @ RelationManager::deleteTable" << std::endl;
            return ret;
        }

        // Close FileHandle if it is bound to the deleted table
        if(tableFileHandle.fileName == tableName) {
            tableFileHandle.close();
        }
        return 0;
    }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attrName) {
        if(!isTableAccessible(tableName)) {
            return ERR_ACCESS_DENIED_SYS_TABLE;
        }
        if(!isTableNameValid(tableName)) {
            return ERR_TABLE_NAME_INVALID;
        }
        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        IndexManager& ix = IndexManager::instance();
        ret = openCatalog();
        if(ret) {
            return ERR_CATALOG_NOT_OPEN;
        }

        std::string ixFileName;

        // 1. Get table and index metadata
        // Get Table ID
        CatalogTablesRecord tableRecord;
        ret = getTableMetaData(tableName, tableRecord);
        if(ret) return ret;

        // Find all indexes associated with this table and corresponding file names
        std::unordered_map<std::string, std::string> indexedAttrAndFileName;
        ret = getIndexes(tableName, indexedAttrAndFileName);
        if(ret) return ret;
        if(indexedAttrAndFileName.find(attrName) == indexedAttrAndFileName.end()) {
            return ERR_INDEX_NOT_EXIST;
        }
        ixFileName = indexedAttrAndFileName[attrName];

        // 3. Delete index from catalog
        ret = deleteIndexFromCatalog(tableRecord.tableID, attrName);
        if(ret) return ret;

        // 4. Delete index file
        ret = ix.destroyFile(ixFileName);
        if(ret) return ret;

        return 0;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        RC ret = 0;
        ret = openCatalog();
        if(ret) {
            return ERR_CATALOG_NOT_OPEN;
        }
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        CatalogTablesRecord tablesRecord;
        ret = getTableMetaData(tableName, tablesRecord);
        if(ret) {
            LOG(ERROR) << "Fail to get table meta data @ RelationManager::getAttributes" << std::endl;
            return ret;
        }

        RBFM_ScanIterator colIter;
        std::vector<std::string> colAttrName = {
                CATALOG_COLUMNS_COLUMNNAME, CATALOG_COLUMNS_COLUMNTYPE, CATALOG_COLUMNS_COLUMNLENGTH, CATALOG_COLUMNS_COLUMNVERSION
        };
        ret = rbfm.scan(catalogColumnsFH, catalogColumnsSchema, CATALOG_COLUMNS_TABLEID,
                        EQ_OP, &tablesRecord.tableID, colAttrName, colIter);
        if(ret) {
            return ret;
        }
        RID curRID;
        uint8_t apiData[PAGE_SIZE];
        while(colIter.getNextRecord(curRID, apiData) == 0) {
            CatalogColumnsRecord curCol(apiData, colAttrName);
            if(curCol.columnVersion == tablesRecord.tableVersion) {
                attrs.push_back(curCol.getAttribute());
            }
        }

        return 0;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        if(!isTableAccessible(tableName)) {
            return ERR_ACCESS_DENIED_SYS_TABLE;
        }
        if(!isTableNameValid(tableName)) {
            return ERR_TABLE_NAME_INVALID;
        }

        RC ret = 0;
        std::vector<Attribute> attrs;
        CatalogTablesRecord tableRecord;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        if(!tableFileHandle.isOpen() || tableFileHandle.fileName != tableName) {
            tableFileHandle.close();
            ret = rbfm.openFile(tableName, tableFileHandle);
            if(ret) {
                return ret;
            }
        }

        // 1. Insert Record into Table
        ret = getAttributes(tableName, attrs);
        if(ret || attrs.empty()) {
            LOG(ERROR) << "Fail to get meta data @ RelationManager::insertTuple" << std::endl;
            return ERR_GET_METADATA;
        }
        ret = getTableMetaData(tableName, tableRecord);
        if(ret) return ret;

        ret = rbfm.insertRecord(tableFileHandle, attrs, data, (int8_t)tableRecord.tableVersion, rid);
        if(ret) {
            return ret;
        }

        // 2. Insert Record into Index
        IndexManager& ix = IndexManager::instance();
        // Find all indexes associated with this table and corresponding file names
        std::unordered_map<std::string, std::string> indexedAttrAndFileName;
        ret = getIndexes(tableName, indexedAttrAndFileName);
        if(ret) {
            return ret;
        }

        int32_t dataPos = ceil(attrs.size() / 8.0);
        for(int i = 0; i < attrs.size(); i++) {
            if(RecordHelper::isAttrNull((uint8_t *)data, i)) {
                continue;
            }
            if(indexedAttrAndFileName.find(attrs[i].name) != indexedAttrAndFileName.end()) {
                // insert entry into each index
                if(ixFHMap.find(indexedAttrAndFileName[attrs[i].name]) == ixFHMap.end()) {
                    IXFileHandle* fh = new IXFileHandle;
                    ret = ix.openFile(indexedAttrAndFileName[attrs[i].name], *fh);
                    if(ret) return ret;
                    ixFHMap[indexedAttrAndFileName[attrs[i].name]] = fh;
                }
                ret = ix.insertEntry(*ixFHMap[indexedAttrAndFileName[attrs[i].name]], attrs[i], (uint8_t *)data + dataPos, rid);
                if(ret) return ret;
            }
            switch (attrs[i].type) {
                case TypeInt:
                    dataPos += sizeof(int32_t);
                    break;
                case TypeReal:
                    dataPos += sizeof(float);
                    break;
                case TypeVarChar:
                    int32_t tmpStrLen;
                    memcpy(&tmpStrLen, (uint8_t *)data + dataPos, sizeof(int32_t));
                    dataPos += sizeof(int32_t);
                    dataPos += tmpStrLen;
                    break;
            }
        }

        tableFileHandle.flushMetadata();

        return 0;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        if(!isTableAccessible(tableName)) {
            return ERR_ACCESS_DENIED_SYS_TABLE;
        }
        if(!isTableNameValid(tableName)) {
            return ERR_TABLE_NAME_INVALID;
        }

        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        std::vector<Attribute> attrs;
        ret = getAttributes(tableName, attrs);
        if(ret) {
            LOG(ERROR) << "Fail to get meta data @ RelationManager::deleteTuple" << std::endl;
            return ERR_GET_METADATA;
        }

        if(!tableFileHandle.isOpen() || tableFileHandle.fileName != tableName) {
            tableFileHandle.close();
            ret = rbfm.openFile(tableName, tableFileHandle);
            if(ret) {
                return ret;
            }
        }

        // 1. Get tuple data and delete entries in each index
        uint8_t data[PAGE_SIZE] = {};
        ret = rbfm.readRecord(tableFileHandle, attrs, rid, data);
        if(ret) return ret;
        IndexManager& ix = IndexManager::instance();
        // Find all indexes associated with this table and corresponding file names
        std::unordered_map<std::string, std::string> indexedAttrAndFileName;
        ret = getIndexes(tableName, indexedAttrAndFileName);
        if(ret) return ret;

        int32_t dataPos = ceil(attrs.size() / 8.0);
        for(int i = 0; i < attrs.size(); i++) {
            if(RecordHelper::isAttrNull(data, i)) {
                continue;
            }
            if(indexedAttrAndFileName.find(attrs[i].name) != indexedAttrAndFileName.end()) {
                // delete entry from each index
                if(ixFHMap.find(indexedAttrAndFileName[attrs[i].name]) == ixFHMap.end()) {
                    IXFileHandle* fh = new IXFileHandle;
                    ret = ix.openFile(indexedAttrAndFileName[attrs[i].name], *fh);
                    if(ret) return ret;
                    ixFHMap[indexedAttrAndFileName[attrs[i].name]] = fh;
                }
                ret = ix.deleteEntry(*ixFHMap[indexedAttrAndFileName[attrs[i].name]], attrs[i], (uint8_t *)data + dataPos, rid);
                if(ret) return ret;
            }
            switch (attrs[i].type) {
                case TypeInt:
                    dataPos += sizeof(int32_t);
                    break;
                case TypeReal:
                    dataPos += sizeof(float);
                    break;
                case TypeVarChar:
                    int32_t tmpStrLen;
                    memcpy(&tmpStrLen, (uint8_t *)data + dataPos, sizeof(int32_t));
                    dataPos += sizeof(int32_t);
                    dataPos += tmpStrLen;
                    break;
            }
        }

        // 2. Delete tuple in the table
        ret = rbfm.deleteRecord(tableFileHandle, attrs, rid);
        if(ret) {
            return ret;
        }

        tableFileHandle.flushMetadata();

        return 0;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *newData, const RID &rid) {
        if(!isTableAccessible(tableName)) {
            return ERR_ACCESS_DENIED_SYS_TABLE;
        }
        if(!isTableNameValid(tableName)) {
            return ERR_TABLE_NAME_INVALID;
        }

        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        std::vector<Attribute> attrs;
        ret = getAttributes(tableName, attrs);
        if(ret) {
            LOG(ERROR) << "Fail to get meta data @ RelationManager::updateTuple" << std::endl;
            return ERR_GET_METADATA;
        }

        if(!tableFileHandle.isOpen() || tableFileHandle.fileName != tableName) {
            tableFileHandle.close();
            ret = rbfm.openFile(tableName, tableFileHandle);
            if(ret) {
                return ret;
            }
        }

        // 1. Update tuple in index
        IndexManager& ix = IndexManager::instance();
        std::unordered_map<std::string, std::string> indexedAttrAndFileName;
        ret = getIndexes(tableName, indexedAttrAndFileName);
        if(ret) return ret;

        uint8_t oldData[PAGE_SIZE] = {};
        ret = rbfm.readRecord(tableFileHandle, attrs, rid, oldData);
        if(ret) return ret;

        // Delete and re-insert
        int32_t dataPos = ceil(attrs.size() / 8.0);
        for(int i = 0; i < attrs.size(); i++) {
            if(RecordHelper::isAttrNull(oldData, i)) {
                continue;
            }
            if(indexedAttrAndFileName.find(attrs[i].name) != indexedAttrAndFileName.end()) {
                // delete old entry and insert new entry
                if(ixFHMap.find(indexedAttrAndFileName[attrs[i].name]) == ixFHMap.end()) {
                    IXFileHandle* fh = new IXFileHandle;
                    ret = ix.openFile(indexedAttrAndFileName[attrs[i].name], *fh);
                    if(ret) return ret;
                    ixFHMap[indexedAttrAndFileName[attrs[i].name]] = fh;
                }
                ret = ix.deleteEntry(*ixFHMap[indexedAttrAndFileName[attrs[i].name]], attrs[i], (uint8_t *)oldData + dataPos, rid);
                if(ret) return ret;
            }
            switch (attrs[i].type) {
                case TypeInt:
                    dataPos += sizeof(int32_t);
                    break;
                case TypeReal:
                    dataPos += sizeof(float);
                    break;
                case TypeVarChar:
                    int32_t tmpStrLen;
                    memcpy(&tmpStrLen, (uint8_t *)oldData + dataPos, sizeof(int32_t));
                    dataPos += sizeof(int32_t);
                    dataPos += tmpStrLen;
                    break;
            }
        }

        dataPos = ceil(attrs.size() / 8.0);
        for(int i = 0; i < attrs.size(); i++) {
            if(RecordHelper::isAttrNull((uint8_t *)newData, i)) {
                continue;
            }
            if(indexedAttrAndFileName.find(attrs[i].name) != indexedAttrAndFileName.end()) {
                // insert entry into each index
                if(ixFHMap.find(indexedAttrAndFileName[attrs[i].name]) == ixFHMap.end()) {
                    IXFileHandle* fh = new IXFileHandle;
                    ret = ix.openFile(indexedAttrAndFileName[attrs[i].name], *fh);
                    if(ret) return ret;
                    ixFHMap[indexedAttrAndFileName[attrs[i].name]] = fh;
                }
                ret = ix.insertEntry(*ixFHMap[indexedAttrAndFileName[attrs[i].name]], attrs[i], (uint8_t *)newData + dataPos, rid);
                if(ret) return ret;
            }
            switch (attrs[i].type) {
                case TypeInt:
                    dataPos += sizeof(int32_t);
                    break;
                case TypeReal:
                    dataPos += sizeof(float);
                    break;
                case TypeVarChar:
                    int32_t tmpStrLen;
                    memcpy(&tmpStrLen, (uint8_t *)newData + dataPos, sizeof(int32_t));
                    dataPos += sizeof(int32_t);
                    dataPos += tmpStrLen;
                    break;
            }
        }

        // 2. Update tuple in table
        ret = rbfm.updateRecord(tableFileHandle, attrs, newData, rid);
        if(ret) {
            return ret;
        }
        tableFileHandle.flushMetadata();

        return 0;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        if(!isTableNameValid(tableName)) {
            return ERR_TABLE_NAME_INVALID;
        }

        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        ret = openCatalog();
        if(ret) {
            return ERR_CATALOG_NOT_OPEN;
        }
        if(!tableFileHandle.isOpen() || tableFileHandle.fileName != tableName) {
            tableFileHandle.close();
            ret = rbfm.openFile(tableName, tableFileHandle);
            if(ret) {
                return ret;
            }
        }

        // 0. Get table version and record version
        CatalogTablesRecord tableRecord;
        ret = getTableMetaData(tableName, tableRecord);
        if(ret) return ret;

        int8_t recordVersion;
        ret = rbfm.readRecordVersion(tableFileHandle, rid, recordVersion);
        if(ret) return ret;

        if(tableRecord.tableVersion == recordVersion) {
            std::vector<Attribute> attrs;
            ret = getAttributes(tableName, attrs);
            if(ret) return ret;
            return rbfm.readRecord(tableFileHandle, attrs, rid, data);
        }

        // 1. Get all versions of schema
        std::unordered_map<int32_t, std::vector<Attribute>> originAttrVersionMap;
        std::unordered_map<int32_t, std::vector<Attribute>> projAttrVersionMap;
        RBFM_ScanIterator colIter;
        std::vector<std::string> colAttrName = {
                CATALOG_COLUMNS_COLUMNNAME, CATALOG_COLUMNS_COLUMNTYPE, CATALOG_COLUMNS_COLUMNLENGTH, CATALOG_COLUMNS_COLUMNVERSION
        };
        ret = rbfm.scan(catalogColumnsFH, catalogColumnsSchema, CATALOG_COLUMNS_TABLEID,
                        EQ_OP, &tableRecord.tableID, colAttrName, colIter);
        if(ret) {
            return ret;
        }
        RID curRID;
        uint8_t apiData[PAGE_SIZE];
        while(colIter.getNextRecord(curRID, apiData) == 0) {
            CatalogColumnsRecord curCol(apiData, colAttrName);
            originAttrVersionMap[curCol.columnVersion].push_back(curCol.getAttribute());
        }

        projAttrVersionMap[tableRecord.tableVersion] = originAttrVersionMap[tableRecord.tableVersion];
        for(int32_t v = tableRecord.tableVersion - 1; v >= 0; v--) {
            for(auto& attr: originAttrVersionMap[v]) {
                uint32_t index;
                for(index = 0; index < projAttrVersionMap[v + 1].size(); index++) {
                    if (projAttrVersionMap[v + 1][index].name == attr.name) {
                        break;
                    }
                }
                if(index < projAttrVersionMap[v + 1].size()) {
                    projAttrVersionMap[v].push_back(attr);
                }
            }
        }

        // 2. Read record and select certain attributes
        if(originAttrVersionMap.find(recordVersion) == originAttrVersionMap.end()) {
            return ERR_VERSION_NOT_EXIST;
        }
        ret = rbfm.readRecord(tableFileHandle, originAttrVersionMap[recordVersion],
                              projAttrVersionMap[recordVersion], rid, apiData);
        if(ret) {
            return ret;
        }

        // 3. Construct Api format based on current format
        ret = rbfm.transformSchema(projAttrVersionMap[recordVersion], apiData,
                                   projAttrVersionMap[tableRecord.tableVersion], (uint8_t *)data);
        if(ret) return ret;

        tableFileHandle.flushMetadata();

        return 0;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        RC ret = rbfm.printRecord(attrs, data, out);
        if(ret) {
            return ret;
        }
        return 0;
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        std::vector<Attribute> attrs;
        ret = getAttributes(tableName, attrs);
        if(ret) {
            LOG(ERROR) << "Fail to get meta data @ RelationManager::readTuple" << std::endl;
            return ERR_GET_METADATA;
        }

        uint8_t recordApiData[PAGE_SIZE] = {};
        ret = readTuple(tableName, rid, recordApiData);
        if(ret) return ret;

        uint32_t attrIndex;
        for(attrIndex = 0; attrIndex < attrs.size(); attrIndex++) {
            if(attrs[attrIndex].name == attributeName) {
                break;
            }
        }
        if(attrIndex >= attrs.size()) {
            return ERR_ATTR_NOT_EXIST;
        }

        std::vector<uint32_t> selectedAttrIndex = {attrIndex};
        uint8_t byteSeq[PAGE_SIZE] = {};
        int16_t recordLen;
        RecordHelper::APIFormatToRecordByteSeq(recordApiData, attrs, byteSeq, recordLen);
        RecordHelper::recordByteSeqToAPIFormat(byteSeq, attrs, selectedAttrIndex, (uint8_t*)data);
        return 0;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        if(!isTableNameValid(tableName)) {
            return ERR_TABLE_NAME_INVALID;
        }

        RC ret = 0;
        std::vector<Attribute> attrs;
        ret = getAttributes(tableName, attrs);
        if(ret) {
            LOG(ERROR) << "Fail to get meta data @ RelationManager::readTuple" << std::endl;
            return ERR_GET_METADATA;
        }
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        FileHandle fh;
        ret = rbfm.openFile(tableName, fh);
        if(ret) {
            return ret;
        }

        ret = rm_ScanIterator.open(fh, attrs, conditionAttribute, compOp, value, attributeNames);
        if(ret) {
            return ret;
        }

        return 0;
    }

    RC RelationManager::indexScan(const std::string &tableName,
                 const std::string &attrName,
                 const void *lowKey,
                 const void *highKey,
                 bool lowKeyInclusive,
                 bool highKeyInclusive,
                 RM_IndexScanIterator &rm_IndexScanIterator) {
        if(!isTableAccessible(tableName)) {
            return ERR_ACCESS_DENIED_SYS_TABLE;
        }
        if(!isTableNameValid(tableName)) {
            return ERR_TABLE_NAME_INVALID;
        }
        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        IndexManager& ix = IndexManager::instance();

        std::vector<Attribute> attrs;
        ret = getAttributes(tableName, attrs);
        if(ret) {
            return ERR_GET_METADATA;
        }
        int32_t attr_pos = 0;
        for(attr_pos = 0; attr_pos < attrs.size(); attr_pos++) {
            if(attrs[attr_pos].name == attrName) {
                break;
            }
        }
        if(attr_pos >= attrs.size()) {
            return ERR_ATTR_NOT_EXIST;
        }

        std::unordered_map<std::string, std::string> indexedAttrAndFileName;
        ret = getIndexes(tableName, indexedAttrAndFileName);
        if(ret) return ret;
        if(indexedAttrAndFileName.find(attrName) == indexedAttrAndFileName.end()) {
            return ERR_INDEX_NOT_EXIST;
        }
        std::string ixFileName = indexedAttrAndFileName[attrName];

        IXFileHandle* ixScanFH = new IXFileHandle;
        ret = ix.openFile(ixFileName, *ixScanFH);
        if(ret) return ret;

        ixScanFHList.push_back(ixScanFH);
        ret = rm_IndexScanIterator.open(ixScanFH, attrs[attr_pos], (uint8_t *)lowKey, (uint8_t *)highKey, lowKeyInclusive, highKeyInclusive);
        if(ret) return ret;

        return 0;
    }

    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        if(!isTableAccessible(tableName)) {
            return ERR_ACCESS_DENIED_SYS_TABLE;
        }
        if(!isTableNameValid(tableName)) {
            return ERR_TABLE_NAME_INVALID;
        }
        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        ret = openCatalog();
        if(ret) {
            return ERR_CATALOG_NOT_OPEN;
        }

        uint8_t apiData[PAGE_SIZE] = {};
        // 1. Get table record in TABLES and update the version number
        RID rid;
        CatalogTablesRecord tableRecord;
        ret = getTableMetaDataAndRID(tableName, tableRecord, rid);
        if(ret) {
            return ret;
        }
        tableRecord.tableVersion++;    // Add table version by one
        tableRecord.getRecordAPIFormat(apiData);
        ret = rbfm.updateRecord(catalogTablesFH, catalogTablesSchema, apiData, rid);
        if(ret) {
            return ret;
        }

        // 2. Insert updated records and the new column record into COLUMNS
        RBFM_ScanIterator colIter;
        std::vector<std::string> colAttrName = {
                CATALOG_COLUMNS_TABLEID, CATALOG_COLUMNS_COLUMNNAME, CATALOG_COLUMNS_COLUMNTYPE,
                CATALOG_COLUMNS_COLUMNLENGTH, CATALOG_COLUMNS_COLUMNPOS, CATALOG_COLUMNS_COLUMNVERSION
        };
        ret = rbfm.scan(catalogColumnsFH, catalogColumnsSchema, CATALOG_COLUMNS_TABLEID,
                        EQ_OP, &tableRecord.tableID, colAttrName, colIter);
        if(ret) {
            return ret;
        }

        RID curRID;
        CatalogColumnsRecord prevCol;
        // Insert updated column records
        while(colIter.getNextRecord(curRID, apiData) == 0) {
            CatalogColumnsRecord curCol(apiData, colAttrName);
            if(curCol.columnVersion == tableRecord.tableVersion - 1) {
                curCol.columnVersion++;
                curCol.getRecordAPIFormat(apiData);
                ret = rbfm.insertRecord(catalogColumnsFH, catalogColumnsSchema, apiData, curRID);
                if(ret) {
                    return ret;
                }
                prevCol = curCol;
            }
        }
        // Insert new column record
        CatalogColumnsRecord newCol(prevCol.tableID, attr.name, attr.type, attr.length,
                                    prevCol.columnPos + 1, prevCol.columnVersion);
        newCol.getRecordAPIFormat(apiData);
        ret = rbfm.insertRecord(catalogColumnsFH, catalogColumnsSchema, apiData, curRID);
        if(ret) {
            return ret;
        }

        return 0;
    }

    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attrToDeleteName) {
        if(!isTableAccessible(tableName)) {
            return ERR_ACCESS_DENIED_SYS_TABLE;
        }
        if(!isTableNameValid(tableName)) {
            return ERR_TABLE_NAME_INVALID;
        }
        RC ret = 0;

        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        ret = openCatalog();
        if(ret) {
            return ERR_CATALOG_NOT_OPEN;
        }

        uint8_t apiData[PAGE_SIZE] = {};
        // 0. Check if this attribute exists
        RID rid;
        CatalogTablesRecord tableRecord;
        ret = getTableMetaDataAndRID(tableName, tableRecord, rid);
        if(ret) {
            return ret;
        }

        bool attrExist = false;
        RBFM_ScanIterator colIter;
        std::vector<std::string> colAttrName = {
                CATALOG_COLUMNS_TABLEID, CATALOG_COLUMNS_COLUMNNAME, CATALOG_COLUMNS_COLUMNTYPE,
                CATALOG_COLUMNS_COLUMNLENGTH, CATALOG_COLUMNS_COLUMNPOS, CATALOG_COLUMNS_COLUMNVERSION
        };
        ret = rbfm.scan(catalogColumnsFH, catalogColumnsSchema, CATALOG_COLUMNS_TABLEID,
                        EQ_OP, &tableRecord.tableID, colAttrName, colIter);
        if(ret) {
            return ret;
        }

        RID curRID;
        while(colIter.getNextRecord(curRID, apiData) == 0) {
            CatalogColumnsRecord curCol(apiData, colAttrName);
            if(curCol.columnName == attrToDeleteName && curCol.columnVersion == tableRecord.tableVersion) {
                attrExist = true;
                break;
            }
        }
        if(!attrExist) {
            return ERR_COL_NOT_EXIST;
        }

        // 1. Update table record in TABLES
        tableRecord.tableVersion++;    // Add table version by one
        tableRecord.getRecordAPIFormat(apiData);
        ret = rbfm.updateRecord(catalogTablesFH, catalogTablesSchema, apiData, rid);
        if(ret) {
            return ret;
        }

        // 2. Insert updated records and the new column record into COLUMNS
        colIter.close();
        ret = rbfm.scan(catalogColumnsFH, catalogColumnsSchema, CATALOG_COLUMNS_TABLEID,
                        EQ_OP, &tableRecord.tableID, colAttrName, colIter);
        if(ret) {
            return ret;
        }

        // Insert updated column records
        while(colIter.getNextRecord(curRID, apiData) == 0) {
            CatalogColumnsRecord curCol(apiData, colAttrName);
            if(curCol.columnVersion == tableRecord.tableVersion - 1 && curCol.columnName != attrToDeleteName) {
                curCol.columnVersion++;
                curCol.getRecordAPIFormat(apiData);
                ret = rbfm.insertRecord(catalogColumnsFH, catalogColumnsSchema, apiData, curRID);
                if(ret) {
                    return ret;
                }
            }
        }

        return 0;
    }

    RC RelationManager::insertTableColIntoCatalog(const std::string& tableName, std::vector<Attribute> schema) {
        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();

        int32_t tableID;
        ret = getNewTableID(tableName, tableID);
        if(ret) {
            LOG(ERROR) << "Fail to get a new table ID @ RelationManager::insertTableColIntoCatalog" << std::endl;
            return ret;
        }
        ret = openCatalog();
        if(ret) {
            LOG(ERROR) << "Catalog not open @ RelationManager::insertTableColIntoCatalog" << std::endl;
            return ERR_CATALOG_NOT_OPEN;
        }
        RID rid;

        uint8_t data[PAGE_SIZE];
        bzero(data, PAGE_SIZE);
        // Insert one table record into TABLES catalog
        CatalogTablesRecord tablesRecord(tableID, tableName, tableName, CATALOG_TABLES_VERSION_INIT);
        tablesRecord.getRecordAPIFormat(data);
        ret = rbfm.insertRecord(catalogTablesFH, catalogTablesSchema, data, rid);
        if(ret) {
            LOG(ERROR) << "Fail to insert metadata into TABLES @ RelationManager::insertTableColIntoCatalog" << std::endl;
            return ret;
        }
        // Insert all column records into COLUMNS catalog
        for(int32_t i = 0; i < schema.size(); i++) {       // Column position starts from 1
            CatalogColumnsRecord columnsRecord(tableID, schema[i].name, schema[i].type, schema[i].length, i + 1, CATALOG_COLUMNS_VERSION_INIT);
            columnsRecord.getRecordAPIFormat(data);
            ret = rbfm.insertRecord(catalogColumnsFH, catalogColumnsSchema, data, rid);
            if(ret) {
                LOG(ERROR) << "Fail to insert metadata into COLUMNS @ RelationManager::insertTableColIntoCatalog" << std::endl;
                return ret;
            }
        }
        return 0;
    }

    RC RelationManager::insertIndexIntoCatalog(const int32_t tableID, const std::string& attrName, const std::string& fileName) {
        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        ret = openCatalog();
        if(ret) {
            LOG(ERROR) << "Catalog not open @ RelationManager::insertIndexIntoCatalog" << std::endl;
            return ERR_CATALOG_NOT_OPEN;
        }
        RID rid;
        uint8_t data[PAGE_SIZE];
        bzero(data, PAGE_SIZE);
        // Insert one index record into TABLES catalog
        CatalogIndexesRecord indexRecord(tableID, attrName, fileName);
        indexRecord.getRecordAPIFormat(data);
        ret = rbfm.insertRecord(catalogIndexesFH, catalogIndexesSchema, data, rid);
        if(ret) {
            LOG(ERROR) << "Fail to insert metadata into INDEXES @ RelationManager::insertIndexIntoCatalog" << std::endl;
            return ret;
        }
        return 0;
    }

    RC RelationManager::deleteTableColFromCatalog(int32_t tableID) {
        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();

        std::vector<std::string> tableAttrNames = {CATALOG_TABLES_TABLEID};
        std::vector<std::string> colAttrNames = {CATALOG_COLUMNS_TABLEID};

        RID curRID;
        uint8_t apiData[PAGE_SIZE];
        // Scan Catalog TABLES and delete target table record in TABLES
        RBFM_ScanIterator tablesIter;
        ret = rbfm.scan(catalogTablesFH, catalogTablesSchema, CATALOG_TABLES_TABLEID,
                        EQ_OP, &tableID, tableAttrNames, tablesIter);
        if(ret) {
            return ret;
        }
        int32_t tableRecordDeletedCount = 0;
        while(tablesIter.getNextRecord(curRID, apiData) == 0) {
            CatalogTablesRecord tmpRecord(apiData, tableAttrNames);
            ret = rbfm.deleteRecord(catalogTablesFH, catalogTablesSchema, curRID);
            if (ret) {
                return ret;
            }
            ++tableRecordDeletedCount;
        }
        if(tableRecordDeletedCount != 1) {
            LOG(ERROR) << "Should delete one record in TABLES @ RelationManager::deleteTableColFromCatalog" << std::endl;
            return ERR_DELETE_METADATA;
        }

        // Scan Catalog Columns and delete target column record in COLUMNS
        RBFM_ScanIterator colIter;
        ret = rbfm.scan(catalogColumnsFH, catalogColumnsSchema, CATALOG_COLUMNS_TABLEID,
                        EQ_OP, &tableID, colAttrNames, colIter);
        if(ret) {
            return ret;
        }
        int32_t colRecordDeletedCount = 0;
        while(colIter.getNextRecord(curRID, apiData) == 0) {
            ret = rbfm.deleteRecord(catalogColumnsFH, catalogColumnsSchema, curRID);
            if(ret) {
                return ret;
            }
            ++colRecordDeletedCount;
        }
        if(colRecordDeletedCount == 0) {
            LOG(ERROR) << "Should delete more than one record in COLUMNS @ RelationManager::deleteTableColFromCatalog" << std::endl;
            return ERR_DELETE_METADATA;
        }

        return 0;
    }

    RC RelationManager::deleteIndexFromCatalog(int32_t tableID) {
        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        RID curRID;
        uint8_t apiData[PAGE_SIZE];
        // Scan Catalog Columns and delete target column record in INDEXES
        RBFM_ScanIterator indexIter;
        ret = rbfm.scan(catalogIndexesFH, catalogIndexesSchema, CATALOG_INDEXES_TABLEID,
                        EQ_OP, &tableID, {}, indexIter);
        if(ret) {
            return ret;
        }
        while(indexIter.getNextRecord(curRID, apiData) == 0) {
            ret = rbfm.deleteRecord(catalogIndexesFH, catalogIndexesSchema, curRID);
            if(ret) {
                return ret;
            }
        }
        return 0;
    }

    RC RelationManager::deleteIndexFromCatalog(int32_t tableID, std::string attrName) {
        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        RID curRID;
        uint8_t apiData[PAGE_SIZE];
        std::vector<std::string> indexAttrNames = {CATALOG_INDEXES_ATTRNAME};
        // Scan Catalog Columns and delete target column record in INDEXES
        RBFM_ScanIterator indexIter;
        ret = rbfm.scan(catalogIndexesFH, catalogIndexesSchema, CATALOG_INDEXES_TABLEID,
                        EQ_OP, &tableID, indexAttrNames, indexIter);
        if(ret) {
            return ret;
        }

        int32_t indexCnt = 0;
        while(indexIter.getNextRecord(curRID, apiData) == 0) {
            CatalogIndexesRecord indexRecord(apiData, indexAttrNames);
            if(indexRecord.attrName == attrName) {
                ret = rbfm.deleteRecord(catalogIndexesFH, catalogIndexesSchema, curRID);
                if (ret) {
                    return ret;
                }
                indexCnt++;
                break;
            }
        }
        if(indexCnt == 0) {
            return ERR_INDEX_NOT_EXIST;
        }
        return 0;
    }

    RC RelationManager::openCatalog() {
        RC ret = 0;
        if(catalogTablesFH.isOpen() && catalogColumnsFH.isOpen() && catalogIndexesFH.isOpen()) {
            return 0;
        }
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        if(!catalogTablesFH.isOpen()) {
            ret = rbfm.openFile(catalogTablesName, catalogTablesFH);
            if(ret) {
                return ret;
            }
        }
        if(!catalogColumnsFH.isOpen()) {
            ret = rbfm.openFile(catalogColumnsName, catalogColumnsFH);
            if(ret) {
                return ret;
            }
        }
        if(!catalogIndexesFH.isOpen()) {
            ret = rbfm.openFile(catalogIndexesName, catalogIndexesFH);
            if(ret) {
                return ret;
            }
        }
        return 0;
    }

    RC RelationManager::getNewTableID(std::string tableName, int32_t& tableID) {
        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();

        RBFM_ScanIterator tableScanIter;
        int32_t emptyData = 0;
        std::vector<std::string> attrNames = {
                CATALOG_TABLES_TABLEID, CATALOG_TABLES_TABLENAME
        };

        ret = rbfm.scan(catalogTablesFH, catalogTablesSchema, CATALOG_TABLES_TABLEID,
                        NO_OP, &emptyData, attrNames, tableScanIter);
        if(ret) {
            LOG(ERROR) << "Fail to initiate scan iterator @ RelationManager::getTableInfo" << std::endl;
            return ret;
        }

        // Iterate through all tables' metadata records in TABLES catalog
        RID recordRID;
        uint8_t apiData[PAGE_SIZE];
        tableID = -1;
        int32_t maxTableID = -1;    // Table ID starts from 0
        while(tableScanIter.getNextRecord(recordRID, apiData) == 0) {
            CatalogTablesRecord tablesRecord(apiData, attrNames);
            if(tablesRecord.tableName == tableName) {
                tableID = tablesRecord.tableID;     // Table already exists, return current table ID
                break;
            }
            maxTableID = std::max(maxTableID, tablesRecord.tableID);
        }
        if(tableID == -1) {     // Table now exist, assign a new table
            tableID = maxTableID + 1;
        }
        return 0;
    }

    RC RelationManager::getTableMetaData(const std::string& tableName, CatalogTablesRecord& tableRecord) {
        RID recordRID;
        return getTableMetaDataAndRID(tableName, tableRecord, recordRID);
    }

    RC RelationManager::getTableMetaDataAndRID(const std::string& tableName, CatalogTablesRecord& tableRecord, RID& rid) {
        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();

        RBFM_ScanIterator tableScanIter;
        std::string tableNameCondition = CATALOG_TABLES_TABLENAME;
        int32_t tableNameLen = tableName.size();
        uint8_t tableNameStr[sizeof(tableNameLen) + tableName.size()];
        memcpy(tableNameStr, &tableNameLen, sizeof(tableNameLen));
        memcpy(tableNameStr + sizeof(tableNameLen), tableName.c_str(), tableName.size());
        std::vector<std::string> attrNames = {
                CATALOG_TABLES_TABLEID, CATALOG_TABLES_TABLENAME, CATALOG_TABLES_FILENAME, CATALOG_TABLES_TABLEVERSION
        };

        ret = rbfm.scan(catalogTablesFH, catalogTablesSchema, tableNameCondition,
                        EQ_OP, tableNameStr, attrNames, tableScanIter);
        if(ret) {
            LOG(ERROR) << "Fail to initiate scan iterator @ RelationManager::getTableMetaData" << std::endl;
            return ret;
        }

        uint8_t apiData[PAGE_SIZE];
        bool isRecordExist = false;
        while(tableScanIter.getNextRecord(rid, apiData) == 0) {
            CatalogTablesRecord curRecord(apiData, attrNames);
            if(curRecord.tableName == tableName) {
                tableRecord = curRecord;
                isRecordExist = true;
                break;
            }
        }
        if(!isRecordExist) {
            return ERR_TABLE_NOT_EXIST;
        }
        return 0;
    }

    RC RelationManager::getIndexes(const std::string& tableName, std::unordered_map<std::string, std::string>& indexedAttrAndFileName) {
        RC ret = 0;
        ret = openCatalog();
        if(ret) {
            return ERR_CATALOG_NOT_OPEN;
        }
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        CatalogTablesRecord tablesRecord;
        ret = getTableMetaData(tableName, tablesRecord);
        if(ret) {
            LOG(ERROR) << "Fail to get table meta data @ RelationManager::getIndexes" << std::endl;
            return ret;
        }

        RBFM_ScanIterator indexIter;
        std::vector<std::string> indexAttrName = {CATALOG_INDEXES_ATTRNAME, CATALOG_INDEXES_FILENAME};
        ret = rbfm.scan(catalogIndexesFH, catalogIndexesSchema, CATALOG_INDEXES_TABLEID,
                        EQ_OP, &tablesRecord.tableID, indexAttrName, indexIter);
        if(ret) {
            return ret;
        }
        RID curRID;
        uint8_t apiData[PAGE_SIZE];
        while(indexIter.getNextRecord(curRID, apiData) == 0) {
            CatalogIndexesRecord curIndex(apiData, indexAttrName);
            indexedAttrAndFileName[curIndex.attrName] = curIndex.fileName;
        }

        return 0;
    }

    bool RelationManager::isTableAccessible(const std::string& tableName) {
        return tableName != catalogTablesName && tableName != catalogColumnsName && tableName != catalogIndexesName;
    }

    bool RelationManager::isTableNameValid(const std::string& tableName) {
        return !tableName.empty();
    }

    std::string RelationManager::getTableFileName(const std::string& tableName) {
        return tableName;
    }
    std::string RelationManager::getIndexFileName(const std::string& tableName, const std::string& attrName) {
        return tableName + '_' + attrName + ".idx";
    }

} // namespace PeterDB