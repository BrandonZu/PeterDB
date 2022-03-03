#include "src/include/rm.h"

namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager() =default;

    RelationManager::~RelationManager() = default;

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

        ret = insertMetaDataIntoCatalog(catalogTablesName, catalogTablesSchema);
        if(ret) {
            LOG(ERROR) << "Fail to insert TABLES metadata into catalog @ RelationManager::createCatalog" << std::endl;
            return ret;
        }
        ret = insertMetaDataIntoCatalog(catalogColumnsName, catalogColumnsSchema);
        if(ret) {
            LOG(ERROR) << "Fail to insert COLUMNS metadata into catalog @ RelationManager::createCatalog" << std::endl;
            return ret;
        }
        return 0;
    }

    RC RelationManager::deleteCatalog() {
        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        catalogTablesFH.close();
        catalogColumnsFH.close();
        ret = rbfm.destroyFile(catalogTablesName);
        if(ret) {
            if(ret == ERR_FILE_NOT_EXIST)
                LOG(ERROR) << "File not exist! @ RelationManager::deleteCatalog";
            else
                LOG(ERROR) << "Fail to delete TABLES catalog @ RelationManager::deleteCatalog";
            return ret;
        }
        ret = rbfm.destroyFile(catalogColumnsName);
        if(ret) {
            LOG(ERROR) << "Fail to delete COLUMNS catalog @ RelationManager::deleteCatalog" << std::endl;
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

        ret = insertMetaDataIntoCatalog(tableName, attrs);
        if(ret) {
            LOG(ERROR) << "Fail to insert metadata into catalog @ RelationManager::createTable" << std::endl;
            return ret;
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

        if(!isTableAccessible(tableName)) {
            LOG(ERROR) << "Table name is invalid! @ RelationManager::createTable" << std::endl;
            return ERR_TABLE_NAME_INVALID;
        }
        RC ret = 0;
        ret = openCatalog();
        if(ret) {
            LOG(ERROR) << "Catalog not open! @ RelationManager::deleteTable" << std::endl;
            return ERR_CATALOG_NOT_OPEN;
        }
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        CatalogTablesRecord tablesRecord;
        ret = getMetaDataFromCatalogTables(tableName, tablesRecord);
        if(ret) {
            LOG(ERROR) << "Fail to get table meta data @ RelationManager::deleteTable" << std::endl;
            return ret;
        }

        ret = rbfm.destroyFile(tablesRecord.tableName);
        if(ret) {
            LOG(ERROR) << "Fail to destroy table file! @ RelationManager::deleteTable" << std::endl;
            return ret;
        }

        ret = deleteAllMetaDataFromCatalog(tablesRecord.tableID);
        if(ret) {
            return ret;
        }
        return 0;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        if(!isTableNameValid(tableName)) {
            return ERR_TABLE_NAME_INVALID;
        }

        RC ret = 0;
        ret = openCatalog();
        if(ret) {
            return ERR_CATALOG_NOT_OPEN;
        }
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        CatalogTablesRecord tablesRecord;
        ret = getMetaDataFromCatalogTables(tableName, tablesRecord);
        if(ret) {
            LOG(ERROR) << "Fail to get table meta data @ RelationManager::getAttributes" << std::endl;
            return ret;
        }

        RBFM_ScanIterator colIter;
        std::vector<std::string> colAttrName = {
                CATALOG_COLUMNS_COLUMNNAME, CATALOG_COLUMNS_COLUMNTYPE, CATALOG_COLUMNS_COLUMNLENGTH
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
            attrs.push_back(curCol.getAttribute());
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
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        ret = getAttributes(tableName, attrs);
        if(ret) {
            LOG(ERROR) << "Fail to get meta data @ RelationManager::insertTuple" << std::endl;
            return ERR_GET_METADATA;
        }

        if(!prevFH.isOpen() || prevFH.fileName != tableName) {
            prevFH.close();
            ret = rbfm.openFile(tableName, prevFH);
            if(ret) {
                return ret;
            }
        }
        ret = rbfm.insertRecord(prevFH, attrs, data, rid);
        if(ret) {
            return ret;
        }
        prevFH.flushMetadata();

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

        if(!prevFH.isOpen() || prevFH.fileName != tableName) {
            prevFH.close();
            ret = rbfm.openFile(tableName, prevFH);
            if(ret) {
                return ret;
            }
        }
        ret = rbfm.deleteRecord(prevFH, attrs, rid);
        if(ret) {
            return ret;
        }
        prevFH.flushMetadata();

        return 0;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
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

        if(!prevFH.isOpen() || prevFH.fileName != tableName) {
            prevFH.close();
            ret = rbfm.openFile(tableName, prevFH);
            if(ret) {
                return ret;
            }
        }
        ret = rbfm.updateRecord(prevFH, attrs, data, rid);
        if(ret) {
            return ret;
        }
        prevFH.flushMetadata();

        return 0;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        if(!isTableNameValid(tableName)) {
            return ERR_TABLE_NAME_INVALID;
        }

        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        std::vector<Attribute> attrs;
        ret = getAttributes(tableName, attrs);
        if(ret) {
            LOG(ERROR) << "Fail to get meta data @ RelationManager::readTuple" << std::endl;
            return ERR_GET_METADATA;
        }

        if(!prevFH.isOpen() || prevFH.fileName != tableName) {
            prevFH.close();
            ret = rbfm.openFile(tableName, prevFH);
            if(ret) {
                return ret;
            }
        }
        ret = rbfm.readRecord(prevFH, attrs, rid, data);
        if(ret) {
            return ret;
        }
        prevFH.flushMetadata();

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
        if(!isTableNameValid(tableName)) {
            return ERR_TABLE_NAME_INVALID;
        }

        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        std::vector<Attribute> attrs;
        ret = getAttributes(tableName, attrs);
        if(ret) {
            LOG(ERROR) << "Fail to get meta data @ RelationManager::readTuple" << std::endl;
            return ERR_GET_METADATA;
        }

        if(!prevFH.isOpen() || prevFH.fileName != tableName) {
            prevFH.close();
            ret = rbfm.openFile(tableName, prevFH);
            if(ret) {
                return ret;
            }
        }
        ret = rbfm.readAttribute(prevFH, attrs, rid, attributeName, data);
        if(ret) {
            return ret;
        }
        prevFH.flushMetadata();

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

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        if(!isTableAccessible(tableName)) {
            return ERR_ACCESS_DENIED_SYS_TABLE;
        }
        if(!isTableNameValid(tableName)) {
            return ERR_TABLE_NAME_INVALID;
        }
        RC ret = 0;

        return 0;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        if(!isTableAccessible(tableName)) {
            return ERR_ACCESS_DENIED_SYS_TABLE;
        }
        if(!isTableNameValid(tableName)) {
            return ERR_TABLE_NAME_INVALID;
        }
        return -1;
    }

    RC RelationManager::insertMetaDataIntoCatalog(const std::string& tableName, std::vector<Attribute> schema) {
        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();

        int32_t tableID;
        ret = getNewTableID(tableName, tableID);
        if(ret) {
            LOG(ERROR) << "Fail to get a new table ID @ RelationManager::insertMetaDataIntoCatalog" << std::endl;
            return ret;
        }
        ret = openCatalog();
        if(ret) {
            LOG(ERROR) << "Catalog not open @ RelationManager::insertMetaDataIntoCatalog" << std::endl;
            return ERR_CATALOG_NOT_OPEN;
        }
        RID rid;

        uint8_t data[PAGE_SIZE];
        bzero(data, PAGE_SIZE);
        // Insert one table record into TABLES catalog
        CatalogTablesRecord tablesRecord(tableID, tableName, tableName);
        tablesRecord.getRecordAPIFormat(data);
        ret = rbfm.insertRecord(catalogTablesFH, catalogTablesSchema, data, rid);
        if(ret) {
            LOG(ERROR) << "Fail to insert metadata into TABLES @ RelationManager::insertMetaDataIntoCatalog" << std::endl;
            return ret;
        }
        // Insert all column records into COLUMNS catalog
        for(int32_t i = 0; i < schema.size(); i++) {       // Column position starts from 1
            CatalogColumnsRecord columnsRecord(tableID, schema[i].name, schema[i].type, schema[i].length, i + 1);
            columnsRecord.getRecordAPIFormat(data);
            ret = rbfm.insertRecord(catalogColumnsFH, catalogColumnsSchema, data, rid);
            if(ret) {
                LOG(ERROR) << "Fail to insert metadata into COLUMNS @ RelationManager::insertMetaDataIntoCatalog" << std::endl;
                return ret;
            }
        }
        return 0;
    }

    RC RelationManager::deleteAllMetaDataFromCatalog(int32_t tableID) {
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
            LOG(ERROR) << "Should delete one record in TABLES @ RelationManager::deleteAllMetaDataFromCatalog" << std::endl;
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
            LOG(ERROR) << "Should delete more than one record in COLUMNS @ RelationManager::deleteAllMetaDataFromCatalog" << std::endl;
            return ERR_DELETE_METADATA;
        }
        return 0;
    }

    RC RelationManager::openCatalog() {
        RC ret = 0;
        if(catalogTablesFH.isOpen() && catalogColumnsFH.isOpen()) {
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
        return 0;
    }

    RC RelationManager::getMetaDataFromCatalogTables(const std::string& tableName, CatalogTablesRecord& tablesRecord) {
        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();

        RBFM_ScanIterator tableScanIter;
        std::string tableNameCondition = CATALOG_TABLES_TABLENAME;
        int32_t tableNameLen = tableName.size();
        uint8_t tableNameStr[sizeof(tableNameLen) + tableName.size()];
        memcpy(tableNameStr, &tableNameLen, sizeof(tableNameLen));
        memcpy(tableNameStr + sizeof(tableNameLen), tableName.c_str(), tableName.size());
        std::vector<std::string> attrNames = {
                CATALOG_TABLES_TABLEID, CATALOG_TABLES_TABLENAME, CATALOG_TABLES_FILENAME
        };

        ret = rbfm.scan(catalogTablesFH, catalogTablesSchema, tableNameCondition,
                        EQ_OP, tableNameStr, attrNames, tableScanIter);
        if(ret) {
            LOG(ERROR) << "Fail to initiate scan iterator @ RelationManager::getMetaDataFromCatalogTables" << std::endl;
            return ret;
        }

        RID recordRID;
        uint8_t apiData[PAGE_SIZE];
        bool isRecordExist = false;
        while(tableScanIter.getNextRecord(recordRID, apiData) == 0) {
            CatalogTablesRecord curRecord(apiData, attrNames);
            if(curRecord.tableName == tableName) {
                tablesRecord = curRecord;
                isRecordExist = true;
                break;
            }
        }
        if(!isRecordExist) {
            return ERR_TABLE_NOT_EXIST;
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
            LOG(ERROR) << "Fail to initiate scan iterator @ RelationManager::getTableID" << std::endl;
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

    bool RelationManager::isTableAccessible(const std::string& tableName) {
        return tableName != catalogTablesName && tableName != catalogColumnsName;
    }

    bool RelationManager::isTableNameValid(const std::string& tableName) {
        return !tableName.empty();
    }

} // namespace PeterDB