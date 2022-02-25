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

    RC IX_ScanIterator::open(IXFileHandle &fileHandle, const Attribute& attr, const uint8_t* lowKey,
                             const uint8_t* highKey, bool lowKeyInclusive, bool highKeyInclusive) {
        RC ret = 0;
        this->ixFileHandle = fileHandle;
        this->attr = attr;
        this->lowKey = lowKey;
        this->highKey = highKey;
        this->lowKeyInclusive = lowKeyInclusive;
        this->highKeyInclusive = highKeyInclusive;
        entryExceedUpperBound = false;

        if(!ixFileHandle.isRootPageExist()) {
            return ERR_ROOTPAGE_NOT_EXIST;
        }
        if(ixFileHandle.isRootNull()) {
            return ERR_ROOT_NULL;
        }
        // Find the right leaf node to start scanning
        ret = IndexManager::instance().findTargetLeafNode(ixFileHandle, curLeafPage, lowKey, attr);
        if(ret) return ret;

        // Skip empty pages
        curSlotPos = 0;
        while(curLeafPage != IX::PAGE_PTR_NULL && curLeafPage < ixFileHandle.getPageCounter()) {
            LeafPageHandle leafPH(ixFileHandle, curLeafPage);
            if(!leafPH.isEmpty()) {
                break;
            }
            curLeafPage = leafPH.getNextPtr();
            curSlotPos = 0;
        }

        if(!lowKey) {
            return 0;
        }

        while(curLeafPage < ixFileHandle.getPageCounter()) {
            LeafPageHandle leafPH(ixFileHandle, curLeafPage);
            if(lowKeyInclusive) {
                leafPH.findFirstKeyMeetCompCondition(curSlotPos, lowKey, attr, LE_OP);
            }
            else {
                leafPH.findFirstKeyMeetCompCondition(curSlotPos, lowKey, attr, LT_OP);
            }

            if(curSlotPos != leafPH.getFreeBytePointer()) {
                curSlotPos = leafPH.getNextEntryPos(curSlotPos, attr);
            }
            else {
                // Reach the end, go to next non-empty page
                curLeafPage++;
                curSlotPos = 0;
                while(curLeafPage != IX::PAGE_PTR_NULL && curLeafPage < ixFileHandle.getPageCounter()) {
                    LeafPageHandle leafPH(ixFileHandle, curLeafPage);
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
        if(curLeafPage >= ixFileHandle.getPageCounter()) {
            return IX_EOF;
        }
        if(highKey && entryExceedUpperBound) {
            return IX_EOF;
        }

        LeafPageHandle leafPageHandle(ixFileHandle, curLeafPage);
        ret = leafPageHandle.getEntry(curSlotPos, (uint8_t *) key, rid, attr);
        if(ret) return ret;

        curLeafPage++;
        curSlotPos = 0;
        while(curLeafPage != IX::PAGE_PTR_NULL && curLeafPage < ixFileHandle.getPageCounter()) {
            LeafPageHandle leafPH(ixFileHandle, curLeafPage);
            if(!leafPH.isEmpty()) {
                break;
            }
            curLeafPage = leafPH.getNextPtr();
            curSlotPos = 0;
        }
        return 0;
    }
}

