#include "src/include/ix.h"

namespace PeterDB {
    IX_ScanIterator::IX_ScanIterator() {
        lowKey = nullptr;
        highKey = nullptr;
        curLeafPage = 0;
        curSlotPos = 0;
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
        curSlotPos = 0;
        entryExceedUpperBound = false;

        if(!ixFileHandlePtr->isRootPageExist()) {
            return ERR_ROOTPAGE_NOT_EXIST;
        }
        if(ixFileHandlePtr->isRootNull()) {
            return ERR_ROOT_NULL;
        }
        // Find the right leaf node to start scanning
        ret = IndexManager::instance().findTargetLeafNode(*ixFileHandlePtr, curLeafPage, lowKey, attr);
        if(ret) return ret;

        // Skip empty pages
        curSlotPos = 0;
        while(curLeafPage != IX::PAGE_PTR_NULL && curLeafPage < ixFileHandlePtr->getPageCounter()) {
            LeafPageHandle leafPH(*ixFileHandlePtr, curLeafPage);
            if(!leafPH.isEmpty()) {
                break;
            }
            curLeafPage = leafPH.getNextPtr();
            curSlotPos = 0;
        }

        if(!lowKey) {
            return 0;
        }

        while(curLeafPage != IX::PAGE_PTR_NULL && curLeafPage < ixFileHandlePtr->getPageCounter()) {
            LeafPageHandle leafPH(*ixFileHandlePtr, curLeafPage);
            if(lowKeyInclusive) {
                leafPH.findFirstKeyMeetCompCondition(curSlotPos, lowKey, attr, GE_OP);
            }
            else {
                leafPH.findFirstKeyMeetCompCondition(curSlotPos, lowKey, attr, GT_OP);
            }

            if(curSlotPos != leafPH.getFreeBytePointer()) {
                break;
            }
            else {
                // Reach the end, go to next non-empty page
                curLeafPage = leafPH.getNextPtr();
                curSlotPos = 0;
                while(curLeafPage != IX::PAGE_PTR_NULL && curLeafPage < ixFileHandlePtr->getPageCounter()) {
                    LeafPageHandle leafPH(*ixFileHandlePtr, curLeafPage);
                    if(!leafPH.isEmpty()) {
                        break;
                    }
                    curLeafPage = leafPH.getNextPtr();
                    curSlotPos = 0;
                }
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
        LeafPageHandle leafPageHandle(*ixFileHandlePtr, curLeafPage);
        ret = leafPageHandle.getEntry(curSlotPos, (uint8_t *) key, rid, attr);
        if(ret) return ret;
        if(highKey && highKeyInclusive &&
           leafPageHandle.isKeyMeetCompCondition((uint8_t *)key, highKey, attr, GT_OP)) {
            entryExceedUpperBound = true;
            return IX_EOF;
        }
        if(highKey && !highKeyInclusive &&
           leafPageHandle.isKeyMeetCompCondition((uint8_t *)key, highKey, attr, GE_OP)) {
            entryExceedUpperBound = true;
            return IX_EOF;
        }

        curSlotPos = leafPageHandle.getNextEntryPos(curSlotPos, attr);
        if(curSlotPos == leafPageHandle.getFreeBytePointer()) {
            // Reach the end of current page
            curLeafPage = leafPageHandle.getNextPtr();
            curSlotPos = 0;
            while (curLeafPage != IX::PAGE_PTR_NULL && curLeafPage < ixFileHandlePtr->getPageCounter()) {
                LeafPageHandle leafPH(*ixFileHandlePtr, curLeafPage);
                if (!leafPH.isEmpty()) {
                    break;
                }
                curLeafPage = leafPH.getNextPtr();
                curSlotPos = 0;
            }
        }
        return 0;
    }
}

