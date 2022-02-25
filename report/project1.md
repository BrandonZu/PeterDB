## Project 1 Report


### 1. Basic information
 - Team #: Dongjue Zu
 - Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222p-winter22-BrandonZu
 - Student 1 UCI NetID: 3256 9266
 - Student 1 Name: Dongjue Zu

### 2. Internal Record Format
- Show your record format design.

My record format design is shown as below.

| 2 | 2 | 2 * Attr Num | Variable | 
| --- | --- | --- | --- |
| Mask | AttrNum | Attr Directory | Attr Values |

![Record Format](Record%20Format.jpeg)

1. Mask(2 bytes): Reserved for future use
2. AttrNum(2 bytes): Number of attributes
3. Attr Directory(2 * AttrNum bytes): Each attr has a pointer which points to the ending position of its byte sequence
4. Attr Value(it depends): Concatenation of attr's conditionAttrValue


- Describe how you store a null field.

The ending position pointer of this null field in the directory will be a negative number, in this case, -1.

- Describe how you store a VarChar field.

Store all chars in the VarChar field.
Use a ending position pointer in the directory to point to the ending position.

- Describe how your record design satisfies O(1) field access.

Given the index of a field, it takes O(1) time to get the ending position of this field and last field. Then, the data of
this field is within the range [lastEndPoint, curEndPoint], which takes O(1) time to fetch the data. If this field is the first
field, then the ending position of last field will be initialized to the ending position of the directory.

### 3. Page Format
- Show your page format design.

I chose to use the exact same page format as the one shown on page 329 Database Management Systems 3rd

![Page Format](Page%20Format.jpeg)

- Explain your slot directory design if applicable.

Each slot consists of two short integers: the first one is the begining position of this record, the second one is the length.

### 4. Page Management
- Show your algorithm of finding next available-space page when inserting a record.

1. Check if the last page satisfies the need
2. If not, traverse all the page from beginning until a page satisfies the need is found
3. if no page satisfies the need, append the new page and insert the record to the new page

- How many hidden pages are utilized in your design?

The first page is the hidden page, which stores the meta data, for example, all the counters, number of pages.

- Show your hidden page(s) format design if applicable

| readPageCounter | writePageCounter | appendPageCounter | pageCounter |
|-----------------|------------------| --- | --- |
| 4 bytes         | 4 bytes          | 4 bytes | 4 bytes |

### 5. Implementation Detail
- Other implementation details goes here.

I designed a RecordPageHandle class to deal with the page format.
Each instance of this class is bound to a specific page, providing some useful functions with regard to insert and read
record from the page.


    class RecordPageHandle {
    public:
        FileHandle& ixFileHandle;
        PageNum pageNum;
    
        short freeBytePointer;
        short slotCounter;
        char data[PAGE_SIZE] = {};
        
        RecordPageHandle(FileHandle& fileHandle, PageNum pageNum);
        RecordPageHandleHandle();
        
        short getFreeSpace();
        bool hasEnoughSpaceForRecord(int recordLen);
        
        RC insertRecord(char recordByteSeq[], RecordLen recordLen, RID& rid);
        RC getRecordByteSeq(short slotNum, char recordByteSeq[], short& recordLen);
    
    private:
        short getHeaderLen();
        short getSlotListLen();
        
        short getSlotCounterOffset();
        short getFreeBytePointerOffset();
        // Slot Num start from 1 !!!
        short getSlotOffset(short slotNum);
    };



### 6. Member contribution (for team of two)
- Explain how you distribute the workload in team.

I did this project all by myself.

### 7. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)