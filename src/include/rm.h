#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "src/include/rbfm.h"

namespace PeterDB {
#define RM_EOF (-1)  // end of a scan operator

    const std::string CATALOG_TABLES_TABLEID = "table-id";
    const std::string CATALOG_TABLES_TABLENAME = "table-name";
    const std::string CATALOG_TABLES_FILENAME = "file-name";
    const std::string CATALOG_TABLES_TABLETYPE = "table-type";
    const int32_t CATALOG_TABLES_TABLENAME_LEN = 50;
    const int32_t CATALOG_TABLES_FILENAME_LEN = 50;
    const int32_t CATALOG_TABLES_ATTR_NUM = 4;
    const int32_t CATALOG_TABLES_ATTR_NULL = -1;

    const std::string CATALOG_COLUMNS_TABLEID = "table-id";
    const std::string CATALOG_COLUMNS_COLUMNNAME = "column-name";
    const std::string CATALOG_COLUMNS_COLUMNTYPE = "column-type";
    const std::string CATALOG_COLUMNS_COLUMNLENGTH = "column-length";
    const std::string CATALOG_COLUMNS_COLUMNPOS = "column-position";
    const int32_t CATALOG_COLUMNS_COLUMNNAME_LEN = 50;
    const int32_t CATALOG_COLUMNS_ATTR_NUM = 5;
    const int32_t CATALOG_COLUMNS_ATTR_NULL = -1;

    const int32_t TABLE_TYPE_SYSTEM = 0;
    const int32_t TABLE_TYPE_USER = 1;

    const std::string catalogTablesName = "Tables";
    const std::string catalogColumnsName = "Columns";
    const std::vector<Attribute> catalogTablesSchema = std::vector<Attribute> {
            Attribute{CATALOG_TABLES_TABLEID, TypeInt, sizeof(TypeInt)},
            Attribute{CATALOG_TABLES_TABLENAME, TypeVarChar, CATALOG_TABLES_TABLENAME_LEN},
            Attribute{CATALOG_TABLES_FILENAME, TypeVarChar, CATALOG_TABLES_FILENAME_LEN},
            Attribute{CATALOG_TABLES_TABLETYPE, TypeInt, sizeof(TypeInt)}
    };
    const std::vector<Attribute> catalogColumnsSchema = std::vector<Attribute> {
            Attribute{CATALOG_COLUMNS_TABLEID, TypeInt, sizeof(TypeInt)},
            Attribute{CATALOG_COLUMNS_COLUMNNAME, TypeVarChar, CATALOG_COLUMNS_COLUMNNAME_LEN},
            Attribute{CATALOG_COLUMNS_COLUMNTYPE, TypeInt, sizeof(TypeInt)},
            Attribute{CATALOG_COLUMNS_COLUMNLENGTH, TypeInt, sizeof(TypeInt)},
            Attribute{CATALOG_COLUMNS_COLUMNPOS, TypeInt, sizeof(TypeInt)},
    };


    // RM_ScanIterator is an iterator to go through tuples
    class RM_ScanIterator {
        RBFM_ScanIterator rbfmIter;
    public:
        RM_ScanIterator();
        ~RM_ScanIterator();

        RC open(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                const std::string &conditionAttribute, const CompOp compOp, const void *value,
                const std::vector<std::string> &attributeNames);
        // "data" follows the same format as RelationManager::insertTuple()
        RC getNextTuple(RID &rid, void *data);

        RC close();
    };

    class CatalogTablesRecord {
    public:
        int32_t tableID;
        std::string tableName;
        std::string fileName;
        int32_t tableType;

        CatalogTablesRecord();
        CatalogTablesRecord(int32_t id, const std::string& name, const std::string& file, int32_t type);
        CatalogTablesRecord(uint8_t* apiData, const std::vector<std::string>& attrNames);

        ~CatalogTablesRecord();

        RC constructFromAPIFormat(uint8_t* apiData, const std::vector<std::string>& attrNames);
        RC getRecordAPIFormat(uint8_t* apiData);
    };

    class CatalogColumnsRecord {
    public:
        int32_t tableID;
        std::string columnName;
        int32_t columnType;
        int32_t columnLen;
        int32_t columnPos;

        CatalogColumnsRecord();
        CatalogColumnsRecord(int32_t id, const std::string& name, int32_t type, int32_t length, int32_t pos);
        CatalogColumnsRecord(uint8_t* apiData, const std::vector<std::string>& attrNames);

        ~CatalogColumnsRecord();

        RC constructFromAPIFormat(uint8_t* apiData, const std::vector<std::string>& attrNames);
        RC getRecordAPIFormat(uint8_t* apiData);

        Attribute getAttribute();
    };

    // Relation Manager
    class RelationManager {
    private:
        FileHandle catalogTablesFH;
        FileHandle catalogColumnsFH;

    public:
        static RelationManager &instance();

        RC createCatalog();

        RC deleteCatalog();

        RC createTable(const std::string &tableName, const std::vector<Attribute> &attrs);

        RC deleteTable(const std::string &tableName);

        RC getAttributes(const std::string &tableName, std::vector<Attribute> &attrs);

        RC insertTuple(const std::string &tableName, const void *data, RID &rid);

        RC deleteTuple(const std::string &tableName, const RID &rid);

        RC updateTuple(const std::string &tableName, const void *data, const RID &rid);

        RC readTuple(const std::string &tableName, const RID &rid, void *data);

        // Print a tuple that is passed to this utility method.
        // The format is the same as printRecord().
        RC printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out);

        RC readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName, void *data);

        // Scan returns an iterator to allow the caller to go through the results one by one.
        // Do not store entire results in the scan iterator.
        RC scan(const std::string &tableName,
                const std::string &conditionAttribute,
                const CompOp compOp,                  // comparison type such as "<" and "="
                const void *value,                    // used in the comparison
                const std::vector<std::string> &attributeNames, // a list of projected attributes
                RM_ScanIterator &rm_ScanIterator);

        // Extra credit work (10 points)
        RC addAttribute(const std::string &tableName, const Attribute &attr);

        RC dropAttribute(const std::string &tableName, const std::string &attributeName);

    private:
        RC insertMetaDataIntoCatalog(const std::string& tableName, std::vector<Attribute> schema, int32_t tableType);
        RC deleteAllMetaDataFromCatalog(int32_t tableID, int32_t tableType);

        RC getMetaDataFromCatalogTables(const std::string& tableName, int32_t tableType, CatalogTablesRecord& tablesRecord);
        // Assume table not exist, assign an ID to it
        RC getNewTableID(std::string tableName, const int32_t tableType, int32_t& tableID);

        RC openCatalog();

        std::string getFileNameOfTable(std::string tableName, std::int32_t type);

    protected:
        RelationManager();                                                  // Prevent construction
        ~RelationManager();                                                 // Prevent unwanted destruction
        RelationManager(const RelationManager &);                           // Prevent construction by copying
        RelationManager &operator=(const RelationManager &);                // Prevent assignment

    };

} // namespace PeterDB

#endif // _rm_h_