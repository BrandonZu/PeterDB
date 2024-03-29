#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <unordered_set>

#include "src/include/rbfm.h"
#include "src/include/ix.h"

namespace PeterDB {
#define RM_EOF (-1)  // end of a scan operator
    namespace RM {
        const std::string CATALOG_TABLES_TABLEID = "table-id";
        const std::string CATALOG_TABLES_TABLENAME = "table-name";
        const std::string CATALOG_TABLES_FILENAME = "file-name";
        const std::string CATALOG_TABLES_TABLEVERSION = "table-version";
        const int32_t CATALOG_TABLES_TABLENAME_LEN = 50;
        const int32_t CATALOG_TABLES_FILENAME_LEN = 50;
        const int32_t CATALOG_TABLES_ATTR_NUM = 4;
        const int32_t CATALOG_TABLES_ATTR_NULL = -1;
        const int32_t CATALOG_TABLES_VERSION_INIT = 0;

        const std::string CATALOG_COLUMNS_TABLEID = "table-id";
        const std::string CATALOG_COLUMNS_COLUMNNAME = "column-name";
        const std::string CATALOG_COLUMNS_COLUMNTYPE = "column-type";
        const std::string CATALOG_COLUMNS_COLUMNLENGTH = "column-length";
        const std::string CATALOG_COLUMNS_COLUMNPOS = "column-position";
        const std::string CATALOG_COLUMNS_COLUMNVERSION = "column-version";
        const int32_t CATALOG_COLUMNS_COLUMNNAME_LEN = 50;
        const int32_t CATALOG_COLUMNS_ATTR_NUM = 6;
        const int32_t CATALOG_COLUMNS_ATTR_NULL = -1;
        const int32_t CATALOG_COLUMNS_VERSION_INIT = 0;

        const std::string CATALOG_INDEXES_TABLEID = "table-id";
        const std::string CATALOG_INDEXES_ATTRNAME = "attribute-name";
        const int32_t CATALOG_INDEXES_ATTRNAME_LEN = 50;
        const std::string CATALOG_INDEXES_FILENAME = "file-name";
        const int32_t CATALOG_INDEXES_FILENAME_LEN = 50;
        const int32_t CATALOG_INDEXES_ATTR_NUM = 3;
        const int32_t CATALOG_INDEXES_ATTR_NULL = -1;

        const std::string catalogTablesName = "Tables";
        const std::string catalogColumnsName = "Columns";
        const std::string catalogIndexesName = "Indexes";

        const std::vector<Attribute> catalogTablesSchema = std::vector<Attribute>{
                Attribute{CATALOG_TABLES_TABLEID, TypeInt, sizeof(int32_t)},
                Attribute{CATALOG_TABLES_TABLENAME, TypeVarChar, CATALOG_TABLES_TABLENAME_LEN},
                Attribute{CATALOG_TABLES_FILENAME, TypeVarChar, CATALOG_TABLES_FILENAME_LEN},
                Attribute{CATALOG_TABLES_TABLEVERSION, TypeInt, sizeof(int32_t)}
        };
        const std::vector<Attribute> catalogColumnsSchema = std::vector<Attribute>{
                Attribute{CATALOG_COLUMNS_TABLEID, TypeInt, sizeof(int32_t)},
                Attribute{CATALOG_COLUMNS_COLUMNNAME, TypeVarChar, CATALOG_COLUMNS_COLUMNNAME_LEN},
                Attribute{CATALOG_COLUMNS_COLUMNTYPE, TypeInt, sizeof(int32_t)},
                Attribute{CATALOG_COLUMNS_COLUMNLENGTH, TypeInt, sizeof(int32_t)},
                Attribute{CATALOG_COLUMNS_COLUMNPOS, TypeInt, sizeof(int32_t)},
                Attribute{CATALOG_COLUMNS_COLUMNVERSION, TypeInt, sizeof(int32_t)}
        };
        const std::vector<Attribute> catalogIndexesSchema = std::vector<Attribute>{
                Attribute{CATALOG_INDEXES_TABLEID, TypeInt, sizeof(int32_t)},
                Attribute{CATALOG_INDEXES_ATTRNAME, TypeVarChar, CATALOG_INDEXES_ATTRNAME_LEN},
                Attribute{CATALOG_INDEXES_FILENAME, TypeVarChar, CATALOG_INDEXES_FILENAME_LEN}
        };
    }

    class CatalogTablesRecord {
    public:
        int32_t tableID;
        std::string tableName;
        std::string fileName;
        int32_t tableVersion;

        CatalogTablesRecord();
        CatalogTablesRecord(int32_t id, const std::string& name, const std::string& file, int32_t version);
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
        int32_t columnVersion;

        CatalogColumnsRecord();
        CatalogColumnsRecord(int32_t id, const std::string& name, int32_t type, int32_t length, int32_t pos, int32_t version);
        CatalogColumnsRecord(uint8_t* apiData, const std::vector<std::string>& attrNames);

        ~CatalogColumnsRecord();

        RC constructFromAPIFormat(uint8_t* apiData, const std::vector<std::string>& attrNames);
        RC getRecordAPIFormat(uint8_t* apiData);

        Attribute getAttribute();
    };

    class CatalogIndexesRecord {
    public:
        int32_t tableID;
        std::string attrName;
        std::string fileName;

        CatalogIndexesRecord();
        CatalogIndexesRecord(int32_t id, const std::string& attr, const std::string& file);
        CatalogIndexesRecord(uint8_t* apiData, const std::vector<std::string>& attrNames);

        ~CatalogIndexesRecord();

        RC constructFromAPIFormat(uint8_t* apiData, const std::vector<std::string>& attrNames);
        RC getRecordAPIFormat(uint8_t* apiData);
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

    // RM_IndexScanIterator is an iterator to go through index entries
    class RM_IndexScanIterator {
        IX_ScanIterator ixIter;
    public:
        RM_IndexScanIterator();    // Constructor
        ~RM_IndexScanIterator();    // Destructor

        RC open(IXFileHandle* ixFileHandle, const Attribute& attr,
                const uint8_t* lowKey, const uint8_t* highKey,
                bool lowKeyInclusive, bool highKeyInclusive);
        // "key" follows the same format as in IndexManager::insertEntry()
        RC getNextEntry(RID &rid, void *key);    // Get next matching entry
        RC close();                              // Terminate index scan
    };

    // Relation Manager
    class RelationManager {
    private:
        FileHandle catalogTablesFH;
        FileHandle catalogColumnsFH;
        FileHandle catalogIndexesFH;

        FileHandle tableFileHandle;
        IXFileHandle ixFileHandle;

        std::vector<IXFileHandle*> ixScanFHList;
        std::unordered_map<std::string, IXFileHandle*> ixFHMap;
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

        RC dropAttribute(const std::string &tableName, const std::string &attrToDeleteName);

        // QE IX related
        RC createIndex(const std::string &tableName, const std::string &attrName);

        RC destroyIndex(const std::string &tableName, const std::string &attrName);

        // indexScan returns an iterator to allow the caller to go through qualified entries in index
        RC indexScan(const std::string &tableName,
                     const std::string &attrName,
                     const void *lowKey,
                     const void *highKey,
                     bool lowKeyInclusive,
                     bool highKeyInclusive,
                     RM_IndexScanIterator &rm_IndexScanIterator);

    public:
        RC insertTableColIntoCatalog(const std::string& tableName, std::vector<Attribute> schema);
        RC insertIndexIntoCatalog(const int32_t tableID, const std::string& attrName, const std::string& fileName);
        RC deleteTableColFromCatalog(int32_t tableID);
        RC deleteIndexFromCatalog(int32_t tableID);
        RC deleteIndexFromCatalog(int32_t tableID, std::string attrName);

        RC getNewTableID(std::string tableName, int32_t& tableID);  // If table exists, return tableID; otherwise, assign a new ID to it
        RC openCatalog();

        RC getTableMetaData(const std::string& tableName, CatalogTablesRecord& tableRecord);
        RC getTableMetaDataAndRID(const std::string& tableName, CatalogTablesRecord& tableRecord, RID& rid);
        RC getIndexes(const std::string& tableName, std::unordered_map<std::string, std::string>& indexedAttrAndFileName);

        bool isTableAccessible(const std::string& tableName);
        bool isTableNameValid(const std::string& tableName);

        std::string getTableFileName(const std::string& tableName);
        std::string getIndexFileName(const std::string& tableName, const std::string& attrName);

    protected:
        RelationManager();                                                  // Prevent construction
        ~RelationManager();                                                 // Prevent unwanted destruction
        RelationManager(const RelationManager &);                           // Prevent construction by copying
        RelationManager &operator=(const RelationManager &);                // Prevent assignment

    };

} // namespace PeterDB

#endif // _rm_h_