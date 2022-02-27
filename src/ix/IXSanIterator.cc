#include "src/include/ix.h"

namespace PeterDB {
    IX_ScanIterator::IX_ScanIterator() {
        lowKey = nullptr;
        highKey = nullptr;
        curLeafPage = 0;
        remainDataLen = 0;
        entryExceedUpperBound = false;
    }

    IX_ScanIterator::~IX_ScanIterator() = default;

    RC IX_ScanIterator::open(IXFileHandle* fileHandle, const Attribute& attr, const uint8_t* lowKey,
                             const uint8_t* highKey, bool lowKeyInclusive, bool highKeyInclusive) {
        RC ret = 0;
        this->ixFileHandlePtr = fileHandle;
        this->attr = attr;
        this->lowKey = lowKey;
        this->highKey = highKey;
        this->lowKeyInclusive = lowKeyInclusive;
        this->highKeyInclusive = highKeyInclusive;

        curLeafPage = 0;
        remainDataLen = 0;
        entryExceedUpperBound = false;

        if(!ixFileHandlePtr->isRootPageExist()) {
            return ERR_ROOTPAGE_NOT_EXIST;
        }
        if(ixFileHandlePtr->isRootNull()) {
            return ERR_ROOT_NULL;
        }
        // Find the right leaf node to start scanning
        RID smallestRID = {0, 0};
        ret = IndexManager::instance().findTargetLeafNode(*ixFileHandlePtr, curLeafPage, lowKey, smallestRID, attr);
        if(ret) return ret;

        // Skip empty pages
        getNextNonEmptyPage();

        if(!lowKey) {
            return 0;
        }

        while(curLeafPage != IX::PAGE_PTR_NULL && curLeafPage < ixFileHandlePtr->getPageCounter()) {
            LeafPageHandle leafPH(*ixFileHandlePtr, curLeafPage);
            int16_t firstEntryPos;
            if(this->lowKeyInclusive) {
                leafPH.findFirstKeyMeetCompCondition(firstEntryPos, lowKey, attr, GE_OP);
            }
            else {
                leafPH.findFirstKeyMeetCompCondition(firstEntryPos, lowKey, attr, GT_OP);
            }

            if(firstEntryPos != leafPH.getFreeBytePointer()) {
                remainDataLen = leafPH.getFreeBytePointer() - firstEntryPos;
                break;
            }
            else {
                // Reach the end, go to next non-empty page
                curLeafPage = leafPH.getNextPtr();
                getNextNonEmptyPage();
            }
        }

        return 0;
    }

    RC IX_ScanIterator::close() {
        return 0;
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        RC ret = 0;
        if(curLeafPage >= ixFileHandlePtr->getPageCounter()) {
            return IX_EOF;
        }
        if(highKey && entryExceedUpperBound) {
            return IX_EOF;
        }

        // Read current entry and check if it meets condition
        LeafPageHandle leafPH(*ixFileHandlePtr, curLeafPage);
        ret = leafPH.getEntry(leafPH.getFreeBytePointer() - remainDataLen, (uint8_t *) key, rid, attr);
        if(ret) return IX_EOF;

        // Check if current entry exceed upper bound
        if(highKey && highKeyInclusive &&
           leafPH.isKeyMeetCompCondition((uint8_t *)key, highKey, attr, GT_OP)) {
            entryExceedUpperBound = true;
            return IX_EOF;
        }
        if(highKey && !highKeyInclusive &&
           leafPH.isKeyMeetCompCondition((uint8_t *)key, highKey, attr, GE_OP)) {
            entryExceedUpperBound = true;
            return IX_EOF;
        }

        int16_t nextEntryPos = leafPH.getNextEntryPos(leafPH.getFreeBytePointer() - remainDataLen, attr);
        remainDataLen = leafPH.getFreeBytePointer() - nextEntryPos;
        if(remainDataLen == 0) {
            // Reach the end of current page
            curLeafPage = leafPH.getNextPtr();
            getNextNonEmptyPage();
        }
        return 0;
    }

    RC IX_ScanIterator::getNextNonEmptyPage() {
        while(curLeafPage != IX::PAGE_PTR_NULL && curLeafPage < ixFileHandlePtr->getPageCounter()) {
            LeafPageHandle leafPH(*ixFileHandlePtr, curLeafPage);
            if(!leafPH.isEmpty()) {
                remainDataLen = leafPH.getFreeBytePointer();
                break;
            }
            curLeafPage = leafPH.getNextPtr();
        }
        return 0;
    }
}

