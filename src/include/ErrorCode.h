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


}

#endif //PETERDB_ERRORCODE_H
