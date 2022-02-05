#include "src/include/rm.h"

namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    const std::string RelationManager::tableCatalogName = "Tables";
    const std::string RelationManager::colCatalogName = "Columns";
    const std::vector<Attribute> RelationManager::tableCatalogSchema = std::vector<Attribute> {
            Attribute{CATALOG_TABLES_TABLEID, TypeInt, sizeof(TypeInt)},
            Attribute{CATALOG_TABLES_TABLENAME, TypeVarChar, CATALOG_TABLES_TABLENAME_LEN},
            Attribute{CATALOG_TABLES_FILENAME, TypeVarChar, CATALOG_TABLES_FILENAME_LEN},
            Attribute{CATALOG_TABLES_TABLETYPE, TypeInt, sizeof(TypeInt)}
    };
    const std::vector<Attribute> RelationManager::colCatalogSchema = std::vector<Attribute> {
            Attribute{CATALOG_COLUMNS_TABLEID, TypeInt, sizeof(TypeInt)},
            Attribute{CATALOG_COLUMNS_COLUMNNAME, TypeVarChar, CATALOG_COLUMNS_COLUMNNAME_LEN},
            Attribute{CATALOG_COLUMNS_COLUMNTYPE, TypeInt, sizeof(TypeInt)},
            Attribute{CATALOG_COLUMNS_COLUMNLENGTH, TypeInt, sizeof(TypeInt)},
            Attribute{CATALOG_COLUMNS_COLUMNPOS, TypeInt, sizeof(TypeInt)},
    };

    RelationManager::RelationManager() =default;

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    RC RelationManager::createCatalog() {
        RC ret;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        ret = rbfm.createFile(tableCatalogName);
        if(ret) {
            LOG(ERROR) << "Fail to create TABLES catalog! @ RelationManager::createCatalog" << std::endl;
            return ret;
        }
        ret = rbfm.createFile(colCatalogName);
        if(ret) {
            LOG(ERROR) << "Fail to create COLUMNS catalog! @ RelationManager::createCatalog" << std::endl;
            return ret;
        }

        ret = rbfm.openFile(RelationManager::tableCatalogName, tableCatalogFH);
        if(ret) {
            return ERR_OPEN_TABLES_CATALOG;
        }
        ret = rbfm.openFile(RelationManager::colCatalogName, columnCatalogFH);
        if(ret) {
            return ERR_OPEN_COLUMNS_CATALOG;
        }

        ret = insertMetaDataIntoCatalog(RelationManager::tableCatalogName, RelationManager::tableCatalogSchema,
                                        TABLE_TYPE_SYSTEM);
        if(ret) {
            LOG(ERROR) << "Fail to insert TABLES metadata into catalog @ RelationManager::createCatalog" << std::endl;
            return ret;
        }
        ret = insertMetaDataIntoCatalog(RelationManager::colCatalogName, RelationManager::colCatalogSchema,
                                        TABLE_TYPE_SYSTEM);
        if(ret) {
            LOG(ERROR) << "Fail to insert COLUMNS metadata into catalog @ RelationManager::createCatalog" << std::endl;
            return ret;
        }
        return 0;
    }

    RC RelationManager::deleteCatalog() {
        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        tableCatalogFH.close();
        columnCatalogFH.close();
        ret = rbfm.destroyFile(RelationManager::tableCatalogName);
        if(ret) {
            LOG(ERROR) << "Fail to delete TABLES catalog @ RelationManager::deleteCatalog" << std::endl;
            return ret;
        }
        ret = rbfm.destroyFile(RelationManager::colCatalogName);
        if(ret) {
            LOG(ERROR) << "Fail to delete COLUMNS catalog @ RelationManager::deleteCatalog" << std::endl;
            return ret;
        }
        return 0;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        RC ret = 0;
        if(tableName.empty()) {
            LOG(ERROR) << "Table name can not be empty! @ RelationManager::createTable" << std::endl;
            return ERR_TABLE_NAME_INVALID;
        }
        ret = openCatalog();
        if(ret) {
            LOG(ERROR) << "Catalog not open! @ RelationManager::createTable" << std::endl;
            return ERR_CATALOG_NOT_OPEN;
        }

        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        std::string fileName = getFileNameOfTable(tableName, TABLE_TYPE_USER);
        ret = rbfm.createFile(fileName);
        if(ret) {
            LOG(ERROR) << "Fail to create table's file! @ RelationManager::createTable" << std::endl;
            return ret;
        }

        ret = insertMetaDataIntoCatalog(tableName, attrs, TABLE_TYPE_USER);
        if(ret) {
            LOG(ERROR) << "Fail to insert metadata into catalog @ RelationManager::createTable" << std::endl;
            return ret;
        }
        return 0;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        RC ret = 0;;
        ret = openCatalog();
        if(ret) {
            LOG(ERROR) << "Catalog not open! @ RelationManager::deleteTable" << std::endl;
            return ERR_CATALOG_NOT_OPEN;
        }
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        CatalogTablesRecord tablesRecord;
        ret = getMetaDataFromCatalogTables(tableName, TABLE_TYPE_USER, tablesRecord);
        if(ret) {
            LOG(ERROR) << "Fail to get table meta data @ RelationManager::deleteTable" << std::endl;
            return ret;
        }

        ret = rbfm.destroyFile(getFileNameOfTable(tablesRecord.tableName, tablesRecord.tableType));
        if(ret) {
            LOG(ERROR) << "Fail to destroy table file! @ RelationManager::deleteTable" << std::endl;
            return ret;
        }

        ret = deleteAllMetaDataFromCatalog(tablesRecord.tableID, tablesRecord.tableType);
        if(ret) {
            return ret;
        }
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
        ret = getMetaDataFromCatalogTables(tableName, TABLE_TYPE_USER, tablesRecord);
        if(ret) {
            LOG(ERROR) << "Fail to get table meta data @ RelationManager::getAttributes" << std::endl;
            return ret;
        }

        RBFM_ScanIterator colIter;
        std::vector<std::string> colAttrName = {
                CATALOG_COLUMNS_COLUMNNAME, CATALOG_COLUMNS_COLUMNTYPE, CATALOG_COLUMNS_COLUMNLENGTH
        };
        ret = rbfm.scan(columnCatalogFH, colCatalogSchema, CATALOG_COLUMNS_TABLEID,
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

        return -1;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        return -1;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        return -1;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        return -1;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        return -1;
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        return -1;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }

    RC RelationManager::insertMetaDataIntoCatalog(const std::string& tableName, std::vector<Attribute> schema, int32_t tableType) {
        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();

        int32_t tableID;
        ret = getNewTableID(tableName, tableType, tableID);
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
        std::string fileName = getFileNameOfTable(tableName, tableType);
        CatalogTablesRecord tablesRecord(tableID, tableName, fileName, tableType);
        tablesRecord.getRecordAPIFormat(data);
        ret = rbfm.insertRecord(tableCatalogFH, tableCatalogSchema, data, rid);
        if(ret) {
            LOG(ERROR) << "Fail to insert metadata into TABLES @ RelationManager::insertMetaDataIntoCatalog" << std::endl;
            return ret;
        }
        // Insert all column records into COLUMNS catalog
        for(int32_t i = 0; i < schema.size(); i++) {
            CatalogColumnsRecord columnsRecord(tableID, schema[i].name, schema[i].type, schema[i].length, i);
            columnsRecord.getRecordAPIFormat(data);
            ret = rbfm.insertRecord(columnCatalogFH, colCatalogSchema, data, rid);
            if(ret) {
                LOG(ERROR) << "Fail to insert metadata into COLUMNS @ RelationManager::insertMetaDataIntoCatalog" << std::endl;
                return ret;
            }
        }
        return 0;
    }

    RC RelationManager::deleteAllMetaDataFromCatalog(int32_t tableID, int32_t tableType) {
        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();

        std::vector<std::string> tableAttrNames = {CATALOG_TABLES_TABLETYPE};
        std::vector<std::string> colAttrNames = {CATALOG_COLUMNS_COLUMNPOS};

        RID curRID;
        uint8_t apiData[PAGE_SIZE];
        // Scan Catalog TABLES and delete target table record in TABLES
        RBFM_ScanIterator tablesIter;
        ret = rbfm.scan(tableCatalogFH, tableCatalogSchema, CATALOG_TABLES_TABLEID,
                        EQ_OP, &tableID, tableAttrNames, tablesIter);
        if(ret) {
            return ret;
        }
        int32_t tableRecordDeletedCount = 0;
        while(tablesIter.getNextRecord(curRID, apiData) == 0) {
            CatalogTablesRecord tmpRecord(apiData, tableAttrNames);
            if(tmpRecord.tableType == TABLE_TYPE_USER) {
                ret = rbfm.deleteRecord(tableCatalogFH, tableCatalogSchema, curRID);
                if (ret) {
                    return ret;
                }
                ++tableRecordDeletedCount;
            }
        }
        if(tableRecordDeletedCount != 1) {
            LOG(ERROR) << "Should delete one record in TABLES @ RelationManager::deleteAllMetaDataFromCatalog" << std::endl;
            return ERR_DELETE_METADATA;
        }

        // Scan Catalog Columns and delete target column record in COLUMNS
        RBFM_ScanIterator colIter;
        ret = rbfm.scan(columnCatalogFH, colCatalogSchema, CATALOG_COLUMNS_TABLEID,
                        EQ_OP, &tableID, colAttrNames, colIter);
        if(ret) {
            return ret;
        }
        int32_t colRecordDeletedCount = 0;
        while(colIter.getNextRecord(curRID, apiData) == 0) {
            ret = rbfm.deleteRecord(columnCatalogFH, colCatalogSchema, curRID);
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
        if(tableCatalogFH.isOpen() && columnCatalogFH.isOpen()) {
            return 0;
        }
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        if(!tableCatalogFH.isOpen()) {
            ret = rbfm.openFile(tableCatalogName, tableCatalogFH);
            if(ret) {
                return ret;
            }
        }
        if(!columnCatalogFH.isOpen()) {
            ret = rbfm.openFile(colCatalogName, columnCatalogFH);
            if(ret) {
                return ret;
            }
        }
        return 0;
    }

    RC RelationManager::getMetaDataFromCatalogTables(const std::string& tableName, const int32_t tableType, CatalogTablesRecord& tablesRecord) {
        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();

        RBFM_ScanIterator tableScanIter;
        std::string tableNameCondition = CATALOG_TABLES_TABLENAME;
        int32_t tableNameLen = tableName.size();
        uint8_t tableNameStr[sizeof(tableNameLen) + tableName.size()];
        memcpy(tableNameStr, &tableNameLen, sizeof(tableNameLen));
        memcpy(tableNameStr + sizeof(tableNameLen), tableName.c_str(), tableName.size());
        std::vector<std::string> attrNames = {
                CATALOG_TABLES_TABLEID, CATALOG_TABLES_TABLENAME, CATALOG_TABLES_FILENAME, CATALOG_TABLES_TABLETYPE
        };

        ret = rbfm.scan(tableCatalogFH, tableCatalogSchema, tableNameCondition,
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
            if(curRecord.tableName == tableName && curRecord.tableType == tableType) {
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

    RC RelationManager::getNewTableID(std::string tableName, const int32_t tableType, int32_t& tableID) {
        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();

        RBFM_ScanIterator tableScanIter;
        int32_t emptyData = 0;
        std::vector<std::string> attrNames = {
                CATALOG_TABLES_TABLEID, CATALOG_TABLES_TABLENAME, CATALOG_TABLES_TABLETYPE
        };

        ret = rbfm.scan(tableCatalogFH, tableCatalogSchema, CATALOG_TABLES_TABLEID,
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
            if(tablesRecord.tableName == tableName && tablesRecord.tableType == tableType) {
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

    std::string RelationManager::getFileNameOfTable(std::string tableName, std::int32_t type) {
        if(type == TABLE_TYPE_SYSTEM) {
            return tableName + ".bin";
        }
        else if(type == TABLE_TYPE_USER) {
            return tableName;
        }
        else {
            LOG(ERROR) << "Table type not supported!" << std::endl;
            return "";
        }
    }

} // namespace PeterDB