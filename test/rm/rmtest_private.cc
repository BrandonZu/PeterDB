#include "test/utils/rm_test_util.h"

namespace PeterDBTesting {

    TEST_F(RM_Private_Test, read_attributes) {
        // Functions tested
        // 1. Insert 100,000 tuples
        // 2. Read Attribute

        bufSize = 1000;
        size_t tupleSize = 0;
        int numTuples = 100000;

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::default_random_engine generator(std::random_device{}());
        std::uniform_int_distribution<unsigned> dist8(0, 7);
        std::uniform_int_distribution<unsigned> dist256(0, 255);


        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);
        nullsIndicators.clear();
        for (int i = 0; i < numTuples; i++) {
            memset(inBuffer, 0, bufSize);

            // Insert Tuple
            nullsIndicator[0] = dist256(generator);
            Tweet tweet;
            generateTuple(nullsIndicator, inBuffer, i, i + 100, tupleSize, tweet);
            ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                        << "RelationManager::insertTuple() should succeed.";
            rids.emplace_back(rid);
            nullsIndicators.emplace_back(nullsIndicator[0]);

            if (i % 10000 == 0) {
                GTEST_LOG_(INFO) << (i + 1) << "/" << numTuples << " records have been inserted so far." << std::endl;
            }
        }
        GTEST_LOG_(INFO) << "All records have been inserted." << std::endl;

        // validate a attribute of each tuple randomly
        for (int i = 0; i < numTuples; i = i + 10) {
            unsigned attrID = dist8(generator);
            validateAttribute(attrID, i, i, i + 100);

        }
    }

    TEST_F(RM_Private_Test, scan) {
        // Functions tested
        // 1. insert 100,000 tuples
        // 2. scan - NO_OP
        // 3. scan - GT_OP

        size_t tupleSize;
        bufSize = 1000;
        int numTuples = 100000;
        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);
        std::vector<float> lats;
        std::vector<float> lngs;
        std::vector<unsigned> user_ids;

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        for (int i = 0; i < numTuples; i++) {
            memset(inBuffer, 0, bufSize);

            // Insert Tuple
            Tweet tweet;
            generateTuple(nullsIndicator, inBuffer, i, i + 100, tupleSize, tweet);
            ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                        << "RelationManager::insertTuple() should succeed.";
            lats.emplace_back(tweet.lat);
            lngs.emplace_back(tweet.lng);
            if (tweet.hash_tags > "A") {
                user_ids.emplace_back(tweet.user_id);
            }
            rids.emplace_back(rid);

            if (i % 10000 == 0) {
                GTEST_LOG_(INFO) << (i + 1) << "/" << numTuples << " records have been inserted so far.";
            }
        }
        GTEST_LOG_(INFO) << "All records have been inserted.";
        // Set up the iterator
        std::vector<std::string> attributes{"lng", "lat"};

        // Scan
        ASSERT_EQ(rm.scan(tableName, "", PeterDB::NO_OP, NULL, attributes, rmsi), success)
                                    << "relationManager::scan() should succeed.";

        float latReturned, lngReturned;
        while (rmsi.getNextTuple(rid, outBuffer) != RM_EOF) {
            latReturned = *(float *) ((char *) outBuffer + 5);
            lngReturned = *(float *) ((char *) outBuffer + 1);

            auto targetLat = std::find(lats.begin(), lats.end(), latReturned);

            EXPECT_NE(targetLat, lats.end()) << "returned lat value is not from inserted.";
            lats.erase(targetLat);
            auto targetLng = std::find(lngs.begin(), lngs.end(), lngReturned);

            EXPECT_NE(targetLng, lngs.end()) << "returned lnt value is not from inserted.";
            lngs.erase(targetLng);

        }
        EXPECT_TRUE(lats.empty()) << "returned lat does not match inserted";
        EXPECT_TRUE(lngs.empty()) << "returned lng does not match inserted";

        ASSERT_EQ(rmsi.close(), success) << "close iterator should succeed.";

        char value[5] = {0, 0, 0, 0, 'A'};
        unsigned msgLength = 1;
        memcpy((char *) value, &msgLength, sizeof(unsigned));
        // Scan
        attributes = {"user_id"};
        ASSERT_EQ(rm.scan(tableName, "hash_tags", PeterDB::GT_OP, value, attributes, rmsi), success)
                                    << "relationManager::scan() should succeed.";

        while (rmsi.getNextTuple(rid, outBuffer) != RM_EOF) {

            unsigned userIdReturned = *(unsigned *) ((char *) outBuffer + 1);
            auto targetUserId = std::find(user_ids.begin(), user_ids.end(), userIdReturned);
            EXPECT_NE(targetUserId, user_ids.end()) << "returned user_id value is not from inserted.";
            user_ids.erase(targetUserId);

        }

        EXPECT_TRUE(user_ids.empty()) << "returned user_id does not match inserted";

    }

    TEST_F(RM_Private_Test, scan_with_null) {
        // Functions tested
        // 1. insert 100,000 tuples - will nulls
        // 2. scan - NO_OP
        // 3. scan - LE_OP

        size_t tupleSize;
        bufSize = 1000;
        int numTuples = 100000;
        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);
        std::vector<float> lats;
        std::vector<float> lngs;
        std::vector<unsigned> tweet_ids;
        float targetSentiment = 71234.5;

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        for (int i = 0; i < numTuples; i++) {
            memset(inBuffer, 0, bufSize);

            // Insert Tuple

            // make some tuple to have null fields
            if (i % 37 == 0) {
                nullsIndicator[0] = 53; // 00110101
            } else {
                nullsIndicator[0] = 0; // 00000000
            }

            Tweet tweet;
            generateTuple(nullsIndicator, inBuffer, i, i + 100, tupleSize, tweet);
            ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                        << "RelationManager::insertTuple() should succeed.";
            lats.emplace_back(tweet.lat);
            if (i % 37 != 0) {
                lngs.emplace_back(tweet.lng);
            }
            if (tweet.sentiment != -1 && tweet.sentiment <= targetSentiment) {
                tweet_ids.emplace_back(tweet.tweet_id);
            }
            rids.emplace_back(rid);

            if (i % 10000 == 0) {
                GTEST_LOG_(INFO) << (i + 1) << "/" << numTuples << " records have been inserted so far.";
            }
        }
        GTEST_LOG_(INFO) << "All records have been inserted.";
        // Set up the iterator
        std::vector<std::string> attributes{"lng", "lat", "user_id"};
        ASSERT_EQ(rm.scan(tableName, "", PeterDB::NO_OP, NULL, attributes, rmsi), success)
                                    << "relationManager::scan() should succeed.";

        // Scan
        float latReturned, lngReturned;
        while (rmsi.getNextTuple(rid, outBuffer) != RM_EOF) {
            if ((*(char *) outBuffer) >> 7 & 1u) {
                latReturned = *(float *) ((char *) outBuffer + 1);
                lngReturned = -1;
            } else {
                latReturned = *(float *) ((char *) outBuffer + 5);
                lngReturned = *(float *) ((char *) outBuffer + 1);
            }

            auto targetLat = std::find(lats.begin(), lats.end(), latReturned);

            EXPECT_NE(targetLat, lats.end()) << "returned lat value is not from inserted.";
            lats.erase(targetLat);

            if (lngReturned != -1) {
                auto targetLng = std::find(lngs.begin(), lngs.end(), lngReturned);

                EXPECT_NE(targetLng, lngs.end()) << "returned lnt value is not from inserted.";
                lngs.erase(targetLng);
            }

        }
        EXPECT_TRUE(lats.empty()) << "returned lat does not match inserted";
        EXPECT_TRUE(lngs.empty()) << "returned lng does not match inserted";

        ASSERT_EQ(rmsi.close(), success) << "close iterator should succeed.";

        // Scan
        attributes = {"tweet_id"};
        ASSERT_EQ(rm.scan(tableName, "sentiment", PeterDB::LE_OP, &targetSentiment, attributes, rmsi), success)
                                    << "relationManager::scan() should succeed.";

        while (rmsi.getNextTuple(rid, outBuffer) != RM_EOF) {

            unsigned tweetIdReturned = *(unsigned *) ((char *) outBuffer + 1);
            auto targetTweetId = std::find(tweet_ids.begin(), tweet_ids.end(), tweetIdReturned);
            EXPECT_NE(targetTweetId, tweet_ids.end()) << "returned tweet_id value is not from inserted.";
            tweet_ids.erase(targetTweetId);

        }

        EXPECT_TRUE(tweet_ids.empty()) << "returned tweet_id does not match inserted";

    }

    TEST_F(RM_Private_Test, scan_after_update) {
        // Functions tested
        // 1. insert 100,000 tuples
        // 2. update some tuples
        // 3. scan - NO_OP
        size_t tupleSize;
        bufSize = 1000;
        int numTuples = 100000;
        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);
        std::vector<float> lats;
        std::vector<float> lngs;
        std::vector<unsigned> user_ids;

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        for (int i = 0; i < numTuples; i++) {
            memset(inBuffer, 0, bufSize);

            // Insert Tuple
            Tweet tweet;
            generateTuple(nullsIndicator, inBuffer, i, i + 100, tupleSize, tweet);
            ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                        << "RelationManager::insertTuple() should succeed.";
            lats.emplace_back(tweet.lat);
            lngs.emplace_back(tweet.lng);
            rids.emplace_back(rid);

            if (i % 10000 == 0) {
                GTEST_LOG_(INFO) << (i + 1) << "/" << numTuples << " records have been inserted so far.";
            }
        }
        GTEST_LOG_(INFO) << "All records have been inserted.";

        // update tuples
        unsigned updateCount = 0;
        for (int i = 0; i < numTuples; i = i + 100) {
            memset(inBuffer, 0, bufSize);

            // Update Tuple
            Tweet tweet;
            generateTuple(nullsIndicator, inBuffer, i, i + 100, tupleSize, tweet);
            ASSERT_EQ(rm.updateTuple(tableName, inBuffer, rids[i]), success)
                                        << "RelationManager::updateTuple() should succeed.";
            lats[i] = tweet.lat;
            lngs[i] = tweet.lng;
            updateCount++;
            if (i % 10000 == 0) {
                GTEST_LOG_(INFO) << updateCount << "/" << numTuples << " records have been updated so far." << std::endl;
            }
        }
        GTEST_LOG_(INFO) << "All records have been processed - update count: " << updateCount << std::endl;

        // Set up the iterator
        std::vector<std::string> attributes{"lng", "user_id", "lat"};

        // Scan
        ASSERT_EQ(rm.scan(tableName, "", PeterDB::NO_OP, NULL, attributes, rmsi), success)
                                    << "relationManager::scan() should succeed.";

        float latReturned, lngReturned;
        while (rmsi.getNextTuple(rid, outBuffer) != RM_EOF) {
            latReturned = *(float *) ((char *) outBuffer + 9);
            lngReturned = *(float *) ((char *) outBuffer + 1);

            auto targetLat = std::find(lats.begin(), lats.end(), latReturned);

            EXPECT_NE(targetLat, lats.end()) << "returned lat value is not from inserted.";
            lats.erase(targetLat);
            auto targetLng = std::find(lngs.begin(), lngs.end(), lngReturned);

            EXPECT_NE(targetLng, lngs.end()) << "returned lnt value is not from inserted.";
            lngs.erase(targetLng);

        }
        EXPECT_TRUE(lats.empty()) << "returned lat does not match inserted";
        EXPECT_TRUE(lngs.empty()) << "returned lng does not match inserted";

        ASSERT_EQ(rmsi.close(), success) << "close iterator should succeed.";

    }

    TEST_F(RM_Private_Test, scan_after_delete) {
        // Functions tested
        // 1. insert 100,000 tuples
        // 2. delete tuples
        // 3. scan - NO_OP


        bufSize = 1000;
        size_t tupleSize = 0;
        int numTuples = 100000;

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::default_random_engine generator(std::random_device{}());
        std::uniform_int_distribution<unsigned> dist8(0, 7);
        std::uniform_int_distribution<unsigned> dist256(0, 255);


        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);
        nullsIndicators.clear();
        for (int i = 0; i < numTuples; i++) {
            memset(inBuffer, 0, bufSize);

            // Insert Tuple
            nullsIndicator[0] = dist256(generator);
            Tweet tweet;
            generateTuple(nullsIndicator, inBuffer, i, i + 78, tupleSize, tweet);
            ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                        << "RelationManager::insertTuple() should succeed.";
            rids.emplace_back(rid);
            nullsIndicators.emplace_back(nullsIndicator[0]);

            if (i % 10000 == 0) {
                GTEST_LOG_(INFO) << (i + 1) << "/" << numTuples << " records have been inserted so far.";
            }
        }
        GTEST_LOG_(INFO) << "All tuples have been inserted.";

        for (int i = 0; i < numTuples; i++) {

            ASSERT_EQ(rm.deleteTuple(tableName, rids[i]), success) << "RelationManager::deleteTuple() should succeed.";

            ASSERT_NE(rm.readTuple(tableName, rids[i], outBuffer), success)
                                        << "RelationManager::readTuple() should not succeed on deleted Tuple.";

            if (i % 10000 == 0) {
                GTEST_LOG_(INFO) << (i + 1) << " / " << numTuples << " have been processed.";
            }
        }
        GTEST_LOG_(INFO) << "All tuples have been deleted.";

        // Set up the iterator
        std::vector<std::string> attributes{"tweet_id", "sentiment"};
        ASSERT_EQ(rm.scan(tableName, "", PeterDB::NO_OP, NULL, attributes, rmsi), success)
                                    << "relationManager::scan() should succeed.";

        ASSERT_EQ(rmsi.getNextTuple(rid, outBuffer), RM_EOF)
                                    << "RM_ScanIterator::getNextTuple() should not succeed at this point, since there should be no tuples.";

        // Close the iterator
        ASSERT_EQ(rmsi.close(), success) << "RM_ScanIterator should be able to close.";

    }

    TEST_F(RM_Private_Test, try_to_modify_catalog) {
        // Functions tested
        // An attempt to modify System Catalogs tables - should no succeed

        bufSize = 1000;
        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        // GetAttributes
        ASSERT_EQ(rm.getAttributes("Tables", attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Try to insert a row - should not succeed
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        int offset = 1;
        int intValue = 0;
        int varcharLength = 7;
        std::string varcharStr = "Testing";
        float floatValue = 0.0;

        for (auto &attr : attrs) {
            // Generating INT value
            if (attr.type == PeterDB::TypeInt) {
                intValue = 9999;
                memcpy((char *) inBuffer + offset, &intValue, sizeof(int));
                offset += sizeof(int);
            } else if (attr.type == PeterDB::TypeReal) {
                // Generating FLOAT value
                floatValue = 9999.9;
                memcpy((char *) inBuffer + offset, &floatValue, sizeof(float));
                offset += sizeof(float);
            } else if (attr.type == PeterDB::TypeVarChar) {
                // Generating VarChar value
                memcpy((char *) inBuffer + offset, &varcharLength, sizeof(int));
                offset += sizeof(int);
                memcpy((char *) inBuffer + offset, varcharStr.c_str(), varcharLength);
                offset += varcharLength;
            }
        }

        ASSERT_NE(rm.insertTuple("Tables", inBuffer, rid), success)
                                    << "The system catalog should not be altered by a user's insertion call.";

        // Try to delete the system catalog
        ASSERT_NE (rm.deleteTable("Tables"), success) << "The system catalog should not be deleted by a user call.";


        // GetAttributes
        attrs.clear();
        ASSERT_EQ(rm.getAttributes("Columns", attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Try to insert a row - should not succeed
        free(nullsIndicator);
        nullsIndicator = initializeNullFieldsIndicator(attrs);
        memset(inBuffer, 0, bufSize);
        for (auto &attr : attrs) {
            // Generating INT value
            if (attr.type == PeterDB::TypeInt) {
                intValue = 9999;
                memcpy((char *) inBuffer + offset, &intValue, sizeof(int));
                offset += sizeof(int);
            } else if (attr.type == PeterDB::TypeReal) {
                // Generating FLOAT value
                floatValue = 9999.9;
                memcpy((char *) inBuffer + offset, &floatValue, sizeof(float));
                offset += sizeof(float);
            } else if (attr.type == PeterDB::TypeVarChar) {
                // Generating VarChar value
                memcpy((char *) inBuffer + offset, &varcharLength, sizeof(int));
                offset += sizeof(int);
                memcpy((char *) inBuffer + offset, varcharStr.c_str(), varcharLength);
                offset += varcharLength;
            }
        }

        ASSERT_NE(rm.insertTuple("Columns", inBuffer, rid), success)
                                    << "The system catalog should not be altered by a user's insertion call.";

        // Try to delete the system catalog
        ASSERT_NE (rm.deleteTable("Columns"), success) << "The system catalog should not be deleted by a user call.";


        attrs.clear();
        // GetAttributes
        ASSERT_EQ(rm.getAttributes("Tables", attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Set up the iterator
        std::vector<std::string> projected_attrs;
        projected_attrs.reserve(attrs.size());
        for (PeterDB::Attribute &attr : attrs) {
            projected_attrs.push_back(attr.name);
        }
        ASSERT_EQ(rm.scan("Tables", "", PeterDB::NO_OP, NULL, projected_attrs, rmsi), success)
                                    << "RelationManager::scan() should succeed.";


        // Check Tables table
        checkCatalog("table-id: x, table-name: Tables, file-name: Tables");

        // Check Columns table
        checkCatalog("table-id: x, table-name: Columns, file-name: Columns");

        // Keep scanning the remaining records
        memset(outBuffer, 0, bufSize);
        int count = 0;
        while (rmsi.getNextTuple(rid, outBuffer) != RM_EOF) {
            count++;
            memset(outBuffer, 0, bufSize);
        }

        // There should be at least one more table
        ASSERT_GE(count, 1) << "There should be at least one more table.";

    }

    TEST_F(RM_Version_Test, extra_multiple_add_drop_mix) {
        // Extra Credit Test Case - Functions Tested:
        // 1. Insert tuple
        // 2. Read Attributes
        // 3. Drop Attributes

        size_t tupleSize = 0;
        inBuffer = malloc(200);
        outBuffer = malloc(200);

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize two NULL field indicators
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        // Insert Tuple
        std::string name = "Peter Anteater";
        size_t nameLength = name.length();
        unsigned age = 24;
        float height = 185.7;
        float salary = 23333.3;
        prepareTuple(attrs.size(), nullsIndicator, nameLength, name, age, height, salary, inBuffer, tupleSize);
        ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                    << "RelationManager::insertTuple() should succeed.";

        // Read Attribute
        ASSERT_EQ(rm.readAttribute(tableName, rid, "salary", outBuffer), success)
                                    << "RelationManager::readAttribute() should succeed.";

        ASSERT_FLOAT_EQ(*(float *) ((uint8_t *) outBuffer + 1), salary)
                                    << "Returned height does not match the inserted.";

        // Drop the attribute
        ASSERT_EQ(rm.dropAttribute(tableName, "salary"), success) << "RelationManager::dropAttribute() should succeed.";


        // Get the attribute from the table again
        std::vector<PeterDB::Attribute> attrs2;
        ASSERT_EQ(rm.getAttributes(tableName, attrs2), success) << "RelationManager::getAttributes() should succeed.";

        // The size of the original attribute vector size should be greater than the current one.
        ASSERT_GT(attrs.size(), attrs2.size()) << "attributes should be less than the previous version.";

        // Read Tuple and print the tuple
        ASSERT_EQ(rm.readTuple(tableName, rid, outBuffer), success) << "RelationManager::readTuple() should succeed.";

        std::stringstream stream;
        ASSERT_EQ(rm.printTuple(attrs2, outBuffer, stream), success)
                                    << "RelationManager::printTuple() should succeed.";
        checkPrintRecord("emp_name: Peter Anteater, age: 24, height: 185.7", stream.str());

        inBuffer = malloc(200);
        outBuffer = malloc(200);

        // Add the Attribute back
        PeterDB::Attribute attr = attrs[3];
        ASSERT_EQ(rm.addAttribute(tableName, attr), success) << "RelationManager::addAttribute() should succeed.";

        // Drop another attribute
        ASSERT_EQ(rm.dropAttribute(tableName, "age"), success) << "RelationManager::dropAttribute() should succeed.";

        // GetAttributes again
        attrs.clear();
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        ASSERT_EQ(attrs.size(), attrs2.size())
                                    << "attributes count should remain the same after dropping and adding one.";

        // Read Tuple and print the tuple
        ASSERT_EQ(rm.readTuple(tableName, rid, outBuffer), success) << "RelationManager::readTuple() should succeed.";

        stream.str(std::string());
        stream.clear();
        ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                    << "RelationManager::printTuple() should succeed.";

        checkPrintRecord("emp_name: Peter Anteater, height: 185.7, salary: NULL",
                         stream.str());

    }

    TEST_F(RM_Version_Test, extra_insert_and_read_attribute) {
        // Extra Credit Test Case - Functions Tested:
        // 1. Insert tuple
        // 2. Read Attributes
        // 3. Drop Attributes

        size_t tupleSize = 0;
        inBuffer = malloc(200);
        outBuffer = malloc(200);

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        // Insert Tuple
        std::string name = "Peter Anteater";
        size_t nameLength = name.length();
        unsigned age = 24;
        float height = 185.7;
        float salary = 23333.3;
        prepareTuple(attrs.size(), nullsIndicator, nameLength, name, age, height, salary, inBuffer, tupleSize);
        ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                    << "RelationManager::insertTuple() should succeed.";

        // Drop the Attribute
        ASSERT_EQ(rm.dropAttribute(tableName, "salary"), success) << "RelationManager::dropAttribute() should succeed.";

        // Add the Attribute back
        PeterDB::Attribute attr = attrs[3];
        ASSERT_EQ(rm.addAttribute(tableName, attr), success) << "RelationManager::addAttribute() should succeed.";

        // Get the attribute from the table again
        std::vector<PeterDB::Attribute> attrs2;
        ASSERT_EQ(rm.getAttributes(tableName, attrs2), success) << "RelationManager::getAttributes() should succeed.";

        ASSERT_EQ(attrs.size(), attrs2.size())
                                    << "attributes count should remain the same after dropping and adding one.";

        std::string name2 = "John Doe";
        size_t nameLength2 = name2.length();
        unsigned age2 = 22;
        float height2 = 178.3;
        float salary2 = 800.23;
        PeterDB::RID rid2;

        prepareTuple(attrs2.size(), nullsIndicator, nameLength2, name2, age2, height2, salary2, inBuffer, tupleSize);
        std::stringstream stream;
        ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid2), success)
                                    << "RelationManager::insertTuple() should succeed.";

        // read the second tuple
        ASSERT_EQ(rm.readTuple(tableName, rid2, outBuffer), success) << "RelationManager::readTuple() should succeed.";

        ASSERT_EQ(rm.printTuple(attrs2, outBuffer, stream), success)
                                    << "RelationManager::printTuple() should succeed.";

        checkPrintRecord("emp_name: John Doe, age: 22, height: 178.3, salary: 800.23", stream.str());

        // read the first tuple
        memset(outBuffer, 0, bufSize);
        stream.str(std::string());
        stream.clear();
        ASSERT_EQ(rm.readTuple(tableName, rid, outBuffer), success) << "RelationManager::readTuple() should succeed.";

        ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                    << "RelationManager::printTuple() should succeed.";

        checkPrintRecord("emp_name: Peter Anteater, age: 24, height: 185.7, salary: NULL", stream.str());

        // read the second tuple's attribute
        memset(outBuffer, 0, bufSize);
        ASSERT_EQ(rm.readAttribute(tableName, rid2, "salary", outBuffer), success)
                                    << "RelationManager::readAttribute() should succeed.";

        ASSERT_EQ(*(char *) outBuffer, 0u) << "returned salary should not be NULL";

        ASSERT_FLOAT_EQ(*(float *) ((char *) outBuffer + 1), 800.23) << "returned salary should match inserted.";

        // read the first tuple's attribute
        memset(outBuffer, 0, bufSize);
        ASSERT_EQ(rm.readAttribute(tableName, rid, "salary", outBuffer), success)
                                    << "RelationManager::readAttribute() should succeed.";

        ASSERT_EQ(*(char *) outBuffer, (char)128u) << "returned salary should be NULL";

    }

}