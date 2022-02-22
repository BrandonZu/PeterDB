#ifndef PETERDB_ERRORCODE_H
#define PETERDB_ERRORCODE_H

namespace PeterDB {
    /*
     * General
     * Start from 010
     */
    const int32_t ERR_TODO = 9;
    const int32_t ERR_IMPOSSIBLE = 10;
    const int32_t ERR_FILE_NOT_EXIST = 11;


    /*
     * General File Error
     * Start from 100
     */

    const int32_t ERR_FILE_NOT_OPEN = 100;
    const int32_t ERR_PAGE_NOT_EXIST = 101;
    const int32_t ERR_OPEN_FILE = 102;
    const int32_t ERR_READ_PAGE = 103;
    const int32_t ERR_WRITE_PAGE = 104;
    const int32_t ERR_CREATE_FILE_ALREADY_EXIST = 105;
    const int32_t ERR_OPEN_FILE_ALREADY_OPEN = 106;
    const int32_t ERR_DELETE_FILE = 107;
    const int32_t ERR_APPEND_PAGE = 109;
    const int32_t ERR_CREATE_FILE = 110;

    /*
     * Record Based File System
     * Start from 200
     */

    const int32_t ERR_SLOT_NOT_EXIST_OR_DELETED = 200;
    const int32_t ERR_SLOT_EMPTY = 201;
    const int32_t ERR_NEXT_RECORD_NOT_EXIST = 202;
    const int32_t ERR_COMPARISON_NOT_SUPPORT = 203;
    const int32_t ERR_TRANSFORM_BYTESEQ_TO_APIFORMAT = 204;
    const int32_t ERR_TRANSFORM_APIFORMAT_TO_BYTESEQ = 205;
    const int32_t ERR_SCAN_INVALID_CONDITION_ATTR = 206;
    const int32_t ERR_ATTRIBUTE_NOT_SUPPORT = 207;
    const int32_t ERR_RECORD_NOT_FOUND = 208;
    const int32_t ERR_RECORD_NULL = 209;

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
    const int32_t ERR_ACCESS_DENIED_SYS_TABLE = 307;

    /*
     * Index Manager
     * Start from 400
     */

    const int32_t ERR_KEY_TYPE_NOT_SUPPORT = 400;
    const int32_t ERR_PAGE_TYPE_UNKNOWN = 401;
    const int32_t ERR_PAGE_NOT_ENOUGH_SPACE = 402;
    const int32_t ERR_PAGE_INTERNAL = 403;
    const int32_t ERR_CREATE_ROOT = 404;
    const int32_t ERR_ROOT_NOT_EXIST_OR_NULL = 405;
}

#endif //PETERDB_ERRORCODE_H
