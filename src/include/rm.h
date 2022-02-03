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

    const std::string CATALOG_COLUMNS_TABLEID = "table-id";
    const std::string CATALOG_COLUMNS_COLUMNNAME = "column-name";
    const std::string CATALOG_COLUMNS_COLUMNTYPE = "column-type";
    const std::string CATALOG_COLUMNS_COLUMNLENGTH = "column-length";
    const std::string CATALOG_COLUMNS_COLUMNPOS = "column-position";
    const int32_t CATALOG_COLUMNS_COLUMNNAME_LEN = 50;

    const int32_t TABLE_TYPE_SYSTEM = 0;
    const int32_t TABLE_TYPE_USER = 1;


    // RM_ScanIterator is an iterator to go through tuples
    class RM_ScanIterator {
    public:
        RM_ScanIterator();

        ~RM_ScanIterator();

        // "data" follows the same format as RelationManager::insertTuple()
        RC getNextTuple(RID &rid, void *data);

        RC close();
    };

    // Relation Manager
    class RelationManager {
        // Catalog tables - Tables and Columns
        static const std::string tableCatalogName;
        static const std::vector<Attribute> tableCatalogSchema;
        static const std::string colCatalogName;
        static const std::vector<Attribute> colCatalogSchema;

        FileHandle tableCatalogFH;
        FileHandle columnCatalogFH;

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

        RC insertTableMetaDataIntoCatalog(std::string tableName, std::vector<Attribute> schema, int32_t type);

        // If table exists, return table-id; If not, assign an ID to it
        RC getTableID(std::string tableName, uint32_t& tableID, const int32_t type);

        bool isCatalogOpen();
        std::string getFileNameOfTable(std::string tableName, std::int32_t type);

    protected:
        RelationManager();                                                  // Prevent construction
        ~RelationManager();                                                 // Prevent unwanted destruction
        RelationManager(const RelationManager &);                           // Prevent construction by copying
        RelationManager &operator=(const RelationManager &);                // Prevent assignment

    };

} // namespace PeterDB

#endif // _rm_h_