#ifndef PETERDB_ERRORCODE_H
#define PETERDB_ERRORCODE_H

namespace PeterDB {
    /*
     * General
     * Start from 000
     */


    /*
     * Paged File System
     * Start from 100
     */

    const int32_t ERR_FILE_NOT_OPEN = 100;
    const int32_t ERR_PAGE_NOT_EXIST = 101;
    const int32_t ERR_OPEN_FILE = 102;
    const int32_t ERR_READ_PAGE_FAIL = 103;
    const int32_t ERR_WRITE_PAGE_FAIL = 104;
    const int32_t ERR_CREATE_FILE_ALREADY_EXIST = 105;
    const int32_t ERR_OPEN_FILE_ALREADY_OPEN = 106;
    const int32_t ERR_DELETE_FILE = 107;


    /*
     * Record Based File System
     * Start from 200
     */

    const int32_t ERR_SLOT_NOT_EXIST_OR_DELETED = 200;
    const int32_t ERR_SLOT_EMPTY = 201;
    const int32_t ERR_NEXT_RECORD_NOT_EXIST = 202;
    const int32_t ERR_COMPARISON_NOT_SUPPORT = 203;
    const int32_t ERR_TRANSFORM_BYTESEQ_TO_APIFORMAT = 204;
    const int32_t ERR_TRANSFORM_APIFORMAT_TO_BYTESEQ = 204;
    const int32_t ERR_ATTRIBUTE_NOT_SUPPORT = 299;

    /*
     * Relation Model
     * Start from 300
     */

    const int32_t ERR_TABLE_NAME_INVALID = 300;
    const int32_t ERR_TABLE_NOT_EXIST = 301;
    const int32_t ERR_OPEN_TABLES_CATALOG = 302;
    const int32_t ERR_OPEN_COLUMNS_CATALOG = 303;
    const int32_t ERR_CATALOG_NOT_OPEN = 304;
    const int32_t ERR_DELETE_METADATA = 305;
    const int32_t ERR_GET_METADATA = 306;

}

#endif //PETERDB_ERRORCODE_H
