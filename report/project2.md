## Project 2 Report


### 1. Basic information
 - Team #: Dongjue Zu
 - Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222p-winter22-BrandonZu
 - Student 1 UCI NetID: 3256 9266
 - Student 1 Name: Dongjue Zu

### 2. Meta-data
- Show your meta-data design (Tables and Columns table) and information about each column.

**'Tables' Table**

| Name | Type | Length(Max) | Comment                                           |
| --- | ---- |-------------|---------------------------------------------------|
| table-id | Int | 4           | ID of the table, starting from zero               |
| table-name | Varchar | 50 | name of the table                                 | 
| file-name | Varchar | 50 | name of the file where the table is stored        |

**'Columns' Table**

| Name        | Type    | Length(Max) | Comment                                    |
|-------------|---------|-------------|--------------------------------------------|
| table-id    | Int     | 4           | ID of the table, starting from zero        |
| column-name | Varchar | 50 | name of the attribute                      | 
| column-type | Int     | 4 | type of the attribute |
| column-length   | Int     | 4 | (max)length of the attribute      | 
| column-pos | Int     | 4 | position of the attribute, 0 based |

### 3. Internal Record Format
- Show your record format design.

|             | Mask | AttrNum | Attr Directory | Attr Values |
|-------------|------| --- | --- | --- |
| Byte Length |  2   | 2 | 2 * Attr Num | Variable | 



![Record Format](Record%20Format.jpeg)

1. Mask: Indicate whether this record is pointer \
   0 -> Real Record; 1 -> Pointer
2. AttrNum: Number of attributes
3. Attr Directory: Store each attribute's ending position of its byte sequence
4. Attr Value: Concatenation of attributes' byte sequences

- Describe how you store a null field.

The ending position of a null field stored in the directory is -1

- Describe how you store a VarChar field.

Store all characters in the Attr Value, without '\0' \
The length of the VarChar could be computed using the end position of current attribute and last attribute which is not null

- Describe how your record design satisfies O(1) field access.

Given the index of a field, it takes O(1) time to get the ending position of this field and last field. Then, the data of
this field is within the range [lastEndPoint, curEndPoint], which takes O(1) time to fetch the data. \
If this field is the first field, then the ending position of last field will be initialized to the ending position of the directory.

### 4. Page Format
- Show your page format design.

The page format design is the same page format as the one shown on page 329 Database Management Systems 3rd

![Page Format](Page%20Format.jpeg)

- Explain your slot directory design if applicable.

Each slot consists of the pointer pointing to the start position of the record and the length of the record. \
If a record is deleted, the pointer will be set to -1 and the length will be set to 0 to show this slot is empty. \
The emtpy slot could be reused in the future.

### 5. Page Management
- How many hidden pages are utilized in your design?

One. The first page of a file is reserved.

- Show your hidden page(s) format design if applicable

| readPageCounter | writePageCounter | appendPageCounter | pageCounter |
|-----------------|------------------| --- | --- |
| 4 bytes         | 4 bytes          | 4 bytes | 4 bytes |

### 6. Describe the following operation logic.
- Delete a record

1. Get all the attributes of the table from catalog, i.e. Columns table
2. Open File
3. Call the delete record function provided by RBFM
   1. Follow the record pointer to find the real position of the record, during which delete all record pointers
   2. Delete the read record and shift all following records right to make sure there is no hole
4. Close File

- Update a record

1. Get all the attributes of the table from catalog, i.e. Columns table
2. Open File
3. Call the update record function provided by RBFM
   1. Follow the record pointer to find the real position of the record
   2. Tranform record byte sequence to the API data format, which contains the Null Attribute Indicator
   3. Find available space to store new record and update \
      Case 1: new byte seq is shorter -> shift records left && update slots and free byte pointer \
      Case 2: new byte seq is longer, cur page has enough space -> shift records right && update slots and free byte pointer \
      Case 3: new byte seq is longer, no enough space in cur page -> insert record in a new page && update old record to a pointer
4. Close File

- Scan on normal records

1. Create a RecordPageHandle, try to find next record in current page
   1. If next record does not exist, add the page index by 1 and set the slot index to 0
   2. If next record exist
      1. If NO_OP, found!
      2. If comparison attribute is NULL, continue to find next record
      3. Check if this record meets condition. If it does, found! If not, continue to find next record
2. Transform selected attributes into API Data format and return

- Scan on deleted records

While trying to find next record in current page, deleted records are ignored.

- Scan on updated records

While trying to find next record in current page, if the record is a pointer, it is ignored. \
If the record contains the content, it is considered as a normal record.

### 7. Implementation Detail
- Other implementation details goes here.

Create a RecordPageHandle class to handle all operations within the same page. Each RecordPageHandle is bound to a page. 
The main functions are shown as below.

    class RecordPageHandle {
    public:
        FileHandle& ixFileHandle;
        PageNum pageNum;

        int16_t freeBytePointer;
        int16_t slotCounter;
        uint8_t data[PAGE_SIZE] = {};

    public:
        RecordPageHandle(FileHandle& fileHandle, PageNum pageNum);
        ~RecordPageHandle();

        // Read Record
        // Record Format Described in report
        RC getRecordByteSeq(int16_t slotNum, uint8_t recordByteSeq[], int16_t& recordLen);
        RC getRecordPointerTarget(int16_t curSlotNum, int& ptrPageNum, int16_t& ptrSlotNum);

        RC getRecordAttr(int16_t slotNum, int16_t attrIndex, uint8_t* attrData);
        RC getNextRecord(uint16_t& slotIndex, uint8_t* byteSeq, int16_t& recordLen);

        // Insert Record
        RC insertRecord(uint8_t byteSeq[], int16_t byteSeqLen, RID& rid);

        // Delete Record
        RC deleteRecord(int16_t slotIndex);

        // Update Record
        RC updateRecord(int16_t slotIndex, uint8_t byteSeq[], int16_t recordLen);
        RC setRecordPointToNewRecord(int16_t curSlotIndex, const RID& newRecordPos);

        // Helper Functions
        // Shift records left to avoid empty holes between records
        RC shiftRecord(int16_t dataNeedShiftStartPos, int16_t dist, bool shiftLeft);

        // Find empty slot or append a new slot
        int16_t findAvailSlot();
    }

### 8. Member contribution (for team of two)
- Explain how you distribute the workload in team.

Did all the work by myself since it is an individual project.

### 9. Other (optional)
- Freely use this section to tell us about things that are related to the project 2, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)