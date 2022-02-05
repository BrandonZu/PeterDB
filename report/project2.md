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
| file-type | Int | 4 | type of table: 0 -> System Table; 1 -> User Table | 

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



- Update a record



- Scan on normal records



- Scan on deleted records



- Scan on updated records



### 7. Implementation Detail
- Other implementation details goes here.



### 8. Member contribution (for team of two)
- Explain how you distribute the workload in team.

Did all the work by myself since it is an individual project.

### 9. Other (optional)
- Freely use this section to tell us about things that are related to the project 2, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)