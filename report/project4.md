## Project 4 Report


### 1. Basic information
- Team #: Dongjue Zu
- Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222p-winter22-BrandonZu
- Student 1 UCI NetID: dzu
- Student 1 Name: Dongjue Zu


### 2. Catalog information about Index
- Show your catalog information about an index (tables, columns).

There is a separate catalog table called INDEXES. The schema of this table is as below

        const std::vector<Attribute> catalogIndexesSchema = std::vector<Attribute>{
                Attribute{CATALOG_INDEXES_TABLEID, TypeInt, sizeof(int32_t)},
                Attribute{CATALOG_INDEXES_ATTRNAME, TypeVarChar, CATALOG_INDEXES_ATTRNAME_LEN},
                Attribute{CATALOG_INDEXES_FILENAME, TypeVarChar, CATALOG_INDEXES_FILENAME_LEN}
        };

### 3. Filter
- Describe how your filter works (especially, how you check the condition.)

Continuously retrieve tuples from input until reach the end. \
For each record, check if the record satisfies the condition.
* Iterate over all attributes in the record until find the condition attribute
* Check if the attribute satisfies the condition based on the type of attribute

### 4. Project
- Describe how your project works.

Continuously retrieve tuples from input until reach the end. \
For each record, retrieve selected attributes and generate a new record
* Calculate the null byte len of the new record
* Iterate over all attributes in the original record. If the attributed is selected and is null, set the null bit in the new record; if not, copy its content into the new record

### 5. Block Nested Loop Join
- Describe how your block nested loop join works (especially, how you manage the given buffers.)

Use given buffers to build a hash table using unordered_map
* Load tuples from the outer table into memory to build in-memory hash table
* Continuously retrieve tuples from inner table and use it to probe the in-memory hash table
* If reaches the end of the inner table, reload tuples from the outer table
* Repeat until the end of the outer table is reached

### 6. Index Nested Loop Join
- Describe how your index nested loop join works.

* Continuously retrieve tuples from outer table
* Get the join attribute and use it to set the index scanner on inner table
* Retrieve tuples from the inner table and concat records

### 7. Grace Hash Join (If you have implemented this feature)
- Describe how your grace hash join works (especially, in-memory structure).



### 8. Aggregation
- Describe how your basic aggregation works.

Maintain a float variable and a counter
* For every tuple in outer table, update the float variable based on the aggregate operation and add the counter by one
* Calculate the final result using the float variable and counter

- Describe how your group-based aggregation works. (If you have implemented this feature)



### 9. Implementation Detail
- Have you added your own module or source file (.cc or .h)?
  Clearly list the changes on files and CMakeLists.txt, if any.

New source file: QEHelper.cc
Add this source file to the library of QE CMakeLists.txt

- Other implementation details:



### 10. Member contribution (for team of two)
- Explain how you distribute the workload in team.

Finish all the work by myself

### 11. Other (optional)
- Freely use this section to tell us about things that are related to the project 4, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)