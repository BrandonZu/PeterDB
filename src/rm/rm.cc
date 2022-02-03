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

        ret = insertTableMetaDataIntoCatalog(RelationManager::tableCatalogName, RelationManager::tableCatalogSchema, TABLE_TYPE_SYSTEM);
        if(ret) {
            LOG(ERROR) << "Fail to insert TABLES metadata into catalog @ RelationManager::createCatalog" << std::endl;
            return ret;
        }
        ret = insertTableMetaDataIntoCatalog(RelationManager::colCatalogName, RelationManager::colCatalogSchema, TABLE_TYPE_SYSTEM);
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
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        std::string fileName = getFileNameOfTable(tableName, TABLE_TYPE_USER);
        // 1. Create file
        ret = rbfm.createFile(fileName);
        if(ret) {
            LOG(ERROR) << "Fail to create table's file! @ RelationManager::createTable" << std::endl;
            return ret;
        }
        // 2. Insert Metadata Into Catalog
        ret = insertTableMetaDataIntoCatalog(tableName, attrs, TABLE_TYPE_USER);
        if(ret) {
            LOG(ERROR) << "Fail to insert metadata into catalog @ RelationManager::createTable" << std::endl;
            return ret;
        }
        return 0;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();

        return 0;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        return -1;
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

    RC RelationManager::insertTableMetaDataIntoCatalog(std::string tableName, std::vector<Attribute> schema, int32_t type) {
        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        if(!isCatalogOpen()) {
            LOG(ERROR) << "Catalog not open @ RelationManager::insertTableMetaDataIntoCatalog" << std::endl;
            return ERR_CATALOG_NOT_OPEN;
        }


        return 0;
    }

    bool RelationManager::isCatalogOpen() {
        return tableCatalogFH.isOpen() && columnCatalogFH.isOpen();
    }

    RC RelationManager::getTableID(std::string tableName, uint32_t& tableID, const int32_t type) {
        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();

        // Initialize the scan iterator
        RBFM_ScanIterator scanIter;
        std::string emptyCondition = "";
        CompOp noOp = NO_OP;
        int32_t emptyData = 0;
        std::vector<std::string> attrNames = {
                CATALOG_TABLES_TABLEID, CATALOG_TABLES_TABLENAME, CATALOG_TABLES_TABLETYPE
        };
        ret = rbfm.scan(tableCatalogFH, tableCatalogSchema, emptyCondition, noOp, &emptyData, attrNames, scanIter);
        if(ret) {
            LOG(ERROR) << "Fail to initiate scan iterator @ RelationManager::getTableID" << std::endl;
            return ret;
        }

        // Iterate through all
        RID recordRID;
        uint8_t recordData[PAGE_SIZE];
        while(scanIter.getNextRecord(recordRID, recordData) == 0) {

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