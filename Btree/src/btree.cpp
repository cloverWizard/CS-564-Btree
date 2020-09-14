/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "filescan.h"
#include <utility>
#include <vector>
#include <climits>
#define getint(X) *((int*)X)
//#define DEBUG

namespace badgerdb
{
/**
 * BTreeIndex Constructor. 
 * Check to see if the corresponding index file exists. If so, open the file.
 * If not, create it and insert entries for every tuple in the base relation using FileScan class.
 *
 * @param relationName        Name of file.
 * @param outIndexName        Return the name of index file.
 * @param bufMgrIn			  Buffer Manager Instance
 * @param attrByteOffset	  Offset of attribute, over which index is to be built, in the record
 * @param attrType			  Datatype of attribute over which index is built
 * @throws  BadIndexInfoException     If the index file already exists for the corresponding attribute, 
 * but values in metapage(relationName, attribute byte offset, attribute type etc.) do not match with 
 * values received through constructor parameters.
 */
BTreeIndex::BTreeIndex(const std::string &relationName,
                       std::string &outIndexName,
                       BufMgr *bufMgrIn,
                       const int attrByteOffset,
                       const Datatype attrType)
{
    // update variables
    this->attributeType = attrType;
    this->attrByteOffset = attrByteOffset; 
    this->bufMgr = bufMgrIn;
    this->scanExecuting = false; // updated when start and end scanning

    Page *hdrPage, *rootPage, *leafPage; 
    std::ostringstream idxStr;
    idxStr << relationName << '.' << attrByteOffset;
    std::string indexName = idxStr.str(); // indexName is the name of the index file 
    outIndexName = indexName;
    
    // If the index file already exists, open the file 
    try{
        this->file = new BlobFile(indexName, false);
        this->headerPageNum = file->getFirstPageNo();
        // read the first metadata page 
        bufMgr->readPage(this->file, this->headerPageNum, hdrPage);

        // Metadata: throw exception if not match 
        if(((IndexMetaInfo*) hdrPage)->attrType != attrType 
        || ((IndexMetaInfo*) hdrPage)->attrByteOffset != attrByteOffset
        || strcmp(((IndexMetaInfo*) hdrPage)->relationName, relationName.c_str())){
            throw BadIndexInfoException("Values in metapage not match with values received!");
        }
        this->rootPageNum = ((IndexMetaInfo*) hdrPage)->rootPageNo;
        bufMgr->unPinPage(this->file, this->headerPageNum, false);;
    }catch (FileNotFoundException){ // otherwise, create a file 
        // create an index file with BlobFile
        this->file = new BlobFile(indexName, true);
        bufMgr->allocPage(this->file, this->headerPageNum, hdrPage);       
        // metadata 
        ((IndexMetaInfo*)(hdrPage))->attrType = attrType;
        ((IndexMetaInfo*)(hdrPage))->attrByteOffset = attrByteOffset;  
        strcpy(((IndexMetaInfo*)(hdrPage))->relationName, relationName.c_str());
        
        this->headerPageNum = file->getFirstPageNo();
        
        // Allocate the first leaf node page and root node(page)
        PageId leafPageNum;
        bufMgr->allocPage(this->file, leafPageNum, leafPage);
        LeafNodeInt* leafNode = (LeafNodeInt*) leafPage;
        for(int i=0;i<INTARRAYNONLEAFSIZE+1;i++){
            leafNode->ridArray[i] = INVALID_RECORD; 
        }    
        leafNode->rightSibPageNo = (PageId) Page::INVALID_NUMBER;
        bufMgr->allocPage(this->file, this->rootPageNum, rootPage);
        ((IndexMetaInfo*)(hdrPage))->rootPageNo = this->rootPageNum;
        NonLeafNodeInt* rootNode = (NonLeafNodeInt*) rootPage;
        for(int i=0;i<INTARRAYNONLEAFSIZE+1;i++){
            rootNode->pageNoArray[i] = Page::INVALID_NUMBER;    
        }            
        rootNode->level = 1; 
        rootNode->pageNoArray[0] = leafPageNum;

        bufMgr->unPinPage(this->file, leafPageNum, true);
        bufMgr->unPinPage(this->file, this->rootPageNum, true);
        bufMgr->unPinPage(this->file, this->headerPageNum, true);
        
        // Scan the file 
        try{
            FileScan fScan(relationName, bufMgr);
            RecordId rid; 
            while(true){
                fScan.scanNext(rid);
                std::string recordStr = fScan.getRecord();
                const char *record = recordStr.c_str();
                int key = *((int *)(record + attrByteOffset));
                this->insertEntry(&key, rid);
            }
        }catch(EndOfFileException){
            
        }
    }
}

/**
 * BTreeIndex Destructor. 
 * End any initialized scan, flush index file, after unpinning any pinned pages, from the buffer manager
 * and delete file instance thereby closing the index file.
 * Destructor should not throw any exceptions. All exceptions should be caught in here itself.
 * 
 * Perform any cleanup that may be necessary, including clearing up any state variables, 
 * unpinning any B+ Tree pages that are pinned, and flushing the index ﬁle (by calling bufMgr->flushFile()).
 * Note that this method does not delete the index file! But, deletion of the file object is required, 
 * which will call the destructor of File class causing the index file to be closed.  
 * */
BTreeIndex::~BTreeIndex()
{     
    if(this->scanExecuting){
        endScan(); // cleanup if there is any initialized scan
    }
    this->bufMgr->flushFile(file); // flush the index file 
    delete this->file;
    this->file = NULL;
}

/**
 *  Insert to leaf node
 * 
 * @param key  A pointer to the value (integer) we want to insert. 
 * @param rid  The corresponding record id of the tuple in the base relation.
 * @param newLeafNode  The leaf node to be inserted
 */
const std::pair<int, PageId> BTreeIndex::insertToLeafNode(const void* key, const RecordId rid, PageId pageNum){
    Page *page;
    LeafNodeInt *node;

    bufMgr->readPage(this->file, pageNum, page);
    node = (LeafNodeInt*)page;
    // if the current node is full
    int insertPos = INTARRAYLEAFSIZE;
    for(int i = 0; i < INTARRAYLEAFSIZE; ++i) {
        if (node->ridArray[i].page_number == Page::INVALID_NUMBER || node->keyArray[i] > getint(key)) {
            insertPos = i; 
            break;
        }
    }
    // copy into cache
    std::vector<RecordId> cacheRid;
    std::vector<int> cacheKey;
    for(int j=0;j<INTARRAYLEAFSIZE;j++){
        cacheRid.push_back(node->ridArray[j]);
    }
    for(int j=0;j<INTARRAYLEAFSIZE;j++){
        cacheKey.push_back(node->keyArray[j]);
    }
    // insert into corresponding pos
    cacheRid.insert(cacheRid.begin() + insertPos, rid);
    cacheKey.insert(cacheKey.begin() + insertPos, getint(key));
    
    if(cacheRid.rbegin()->page_number == Page::INVALID_NUMBER) {
        // directly copy back
        for(int j=0;j<INTARRAYLEAFSIZE;j++){
            node->ridArray[j] = cacheRid[j];
        }
        for(int j=0;j<INTARRAYLEAFSIZE;j++){
            node->keyArray[j] = cacheKey[j];
        }
        bufMgr->unPinPage(this->file, pageNum, true);
        return std::make_pair(-1, (PageId)Page::INVALID_NUMBER);                 
    } else {
        Page* newPage;
        PageId newPageNum;
        bufMgr->allocPage(this->file, newPageNum, newPage);
        LeafNodeInt* newNode = (LeafNodeInt*) newPage;
        // Fill in with invalid record
        std::fill(node->ridArray, node->ridArray+INTARRAYLEAFSIZE, INVALID_RECORD);
        std::fill(newNode->ridArray, newNode->ridArray+INTARRAYLEAFSIZE, INVALID_RECORD);
        for(int j=0;j<(int)cacheRid.size()/2;j++){
            node->ridArray[j] = cacheRid[j];
            node->keyArray[j] = cacheKey[j];
        }
        for(int j=(int)cacheRid.size()/2;j<(int)cacheRid.size();j++){
            newNode->ridArray[j-(int)cacheRid.size()/2] = cacheRid[j];
            newNode->keyArray[j-(int)cacheRid.size()/2] = cacheKey[j];
        }
        newNode->rightSibPageNo = node->rightSibPageNo;
        node->rightSibPageNo = newPageNum;        
        int midKey = cacheKey[cacheRid.size()/2];
        bufMgr->unPinPage(this->file, pageNum, true);
        bufMgr->unPinPage(this->file, newPageNum, true);
        
        return std::make_pair(midKey, (PageId)newPageNum);
    }
}
/**
 * Insert to non-leaf node
 * 
 * @param key  A pointer to the value (integer) we want to insert.
 * @param newNode The non-leaf node to be inserted
 * @return 
 */
const std::pair<int, PageId> BTreeIndex::insertToNonLeafNode(const void *key, const RecordId rid, 
                                                PageId pageNum){
    Page* page; 
    NonLeafNodeInt* node; 
    std::pair<int, PageId> pair; 
    
    bufMgr->readPage(this->file, pageNum, page);
    node = (NonLeafNodeInt*)page;
    // Page0 | key0 | Page
    // Page0 | key0 | Page1 | key1 | Page (last)
    // Size of the page array is (order+1)
    for(int i = 0; i < INTARRAYNONLEAFSIZE+1 ; ++i){
        if (i == INTARRAYNONLEAFSIZE || node->pageNoArray[i+1] == Page::INVALID_NUMBER 
        || node->keyArray[i] > getint(key)){
            if(node->level == 1){ 
                pair = insertToLeafNode((const void*)key, rid, node->pageNoArray[i]); 
            } else{
                pair = insertToNonLeafNode((const void*)key, rid, node->pageNoArray[i]);
            }
            // Check if receiving a new rhs page and key, if not, return null directly
            if (pair.second != Page::INVALID_NUMBER){
                
                std::vector<PageId> cachePage;
                std::vector<int> cacheKey;
                for(int j=0;j<INTARRAYNONLEAFSIZE+1;j++){
                    cachePage.push_back(node->pageNoArray[j]);
                }
                for(int j=0;j<INTARRAYNONLEAFSIZE;j++){
                    cacheKey.push_back(node->keyArray[j]);
                }
                cachePage.insert(cachePage.begin()+i+1, pair.second);
                cacheKey.insert(cacheKey.begin()+i, pair.first);
                if (*cachePage.rbegin() == Page::INVALID_NUMBER){ // not full, directly copy back
                    for(int j=0;j<INTARRAYNONLEAFSIZE+1;j++){
                        node->pageNoArray[j] = cachePage[j];
                    }
                    for(int j=0;j<INTARRAYNONLEAFSIZE;j++){
                        node->keyArray[j] = cacheKey[j];
                    }
                    bufMgr->unPinPage(this->file, pageNum, true);
                    return std::make_pair(-1, (PageId)Page::INVALID_NUMBER);
                }else{ // if full, split again
                    
                    Page* newPage;
                    PageId newPageNum;
                    bufMgr->allocPage(this->file, newPageNum, newPage);
                    NonLeafNodeInt* newNode = (NonLeafNodeInt*) newPage;
                    PageId invalid = Page::INVALID_NUMBER;
                    std::fill(node->pageNoArray, node->pageNoArray + INTARRAYNONLEAFSIZE + 1, invalid);
                    std::fill(newNode->pageNoArray, newNode->pageNoArray + INTARRAYNONLEAFSIZE + 1, invalid);
                    for(int j=0;j<(int)cachePage.size()/2;j++){
                        node->pageNoArray[j] = cachePage[j];
                        if (j!=(int)cachePage.size()/2-1){ // not the last page in the node, copy key also
                            node->keyArray[j] = cacheKey[j];
                        }
                    }
                    for(int j=(int)cachePage.size()/2;j<(int)cachePage.size();j++){
                        newNode->pageNoArray[j-(int)cachePage.size()/2] = cachePage[j];
                        if (j!=(int)cachePage.size()-1){ // not the last page in the node, copy key also
                            newNode->keyArray[j-(int)cachePage.size()/2] = cacheKey[j];
                        }                        
                    }
                    newNode->level = node->level;
                    int midKey = cacheKey[cachePage.size()/2-1];
                    bufMgr->unPinPage(this->file, pageNum, true);
                    bufMgr->unPinPage(this->file, newPageNum, true);                    
                    return std::make_pair(midKey, newPageNum);
                }
            } else{
                bufMgr->unPinPage(this->file, pageNum, false);
                return std::make_pair(-1, (PageId)Page::INVALID_NUMBER);
            }
        }
    }
    // Shouldn't reach here
    assert(false);    
    return std::make_pair(-1, (PageId)Page::INVALID_NUMBER);
}

/**
 * Insert a new entry using the pair <key, rid>. 
 * 
 * Start from root to recursively find out the leaf to insert the entry in. The insertion may cause splitting 
 * of leaf node. This splitting will require addition of new leaf page number entry into the parent non-leaf, 
 * which may in-turn get split. This may continue all the way upto the root causing the root to get split. 
 * If root gets split, metapage needs to be changed accordingly.
 * 
 * Make sure to unpin pages as soon as you can.
 * @param key	A pointer to the value (integer) we want to insert. 
 * @param rid	The corresponding record id of the tuple in the base relation.
 **/
const void BTreeIndex::insertEntry(const void *key, const RecordId rid)
{
    std::pair<int, PageId> ret = insertToNonLeafNode(key,rid,this->rootPageNum);
    if (ret.second != Page::INVALID_NUMBER){ // split root page
        PageId newRootPageNum;
        Page* newRootPage;
        bufMgr->allocPage(this->file, newRootPageNum, newRootPage);
        NonLeafNodeInt* rootNode = (NonLeafNodeInt*) newRootPage;
        rootNode->keyArray[0] =  ret.first;
        PageId invalid = Page::INVALID_NUMBER;
        std::fill(rootNode->pageNoArray, rootNode->pageNoArray+INTARRAYNONLEAFSIZE+1, invalid);
        rootNode->pageNoArray[0] = this->rootPageNum;
        rootNode->pageNoArray[1] = ret.second;
        rootNode->level = 0;
        this->rootPageNum = newRootPageNum;
        bufMgr->unPinPage(this->file, newRootPageNum, true);
    }
}

/**
 * Helper function for starting the scan 
 * 
 * Unpin the page not needed, only keep the page we want to be pinned in the buffer pool
 * 
 * @param pageNum: Start from pageNum to find out the leaf page that contains the first RecordID 
 * that satisfies the scan parameters 
 * @return Index of the next entry to be scanned in current leaf being scanned
 * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that satisfies the scan criteria.
 */
const int BTreeIndex::startScanHelper(PageId pageNum){
    NonLeafNodeInt* node; 
    LeafNodeInt* childNode; // used if we reach the last level above leaf node 
    
    // start from the page with pageId as pageNum
    this->currentPageNum = pageNum;
    this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
    node = (NonLeafNodeInt*)this->currentPageData; 

    for(int i = 0; i < INTARRAYNONLEAFSIZE+1; ++i){
        if(i == INTARRAYNONLEAFSIZE || node->pageNoArray[i+1] == Page::INVALID_NUMBER 
        || node->keyArray[i] > lowValInt){
            // if the node is right above the leaf node 
            if(node->level == 1){ 
                // read the child page which is leaf, update variables accordingly
                this->currentPageNum = node->pageNoArray[i];
                this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
                childNode = (LeafNodeInt*)this->currentPageData;
                // scan the page  
                // Page0 | key0 | Page
                // Page0 | key0 | Page1 | key1 | Page (last)
                for(int j = 0; j < INTARRAYLEAFSIZE; ++j){
                    if (childNode->ridArray[j] == INVALID_RECORD){
                        break;
                    }
                    if((this->lowOp == GT && childNode->keyArray[j] > this->lowValInt)
                    || (this->lowOp == GTE && childNode->keyArray[j] >= this->lowValInt)){
                        // unpin the page read at the beginning of this function 
                        this->bufMgr->unPinPage(this->file, pageNum, false);
                        return j;
                    }
                }   
                // if reach the end and not found
                // unPinPage throw PageNotPinnedException if the page is not already pinned 
                this->bufMgr->unPinPage(this->file, pageNum, false); 
                this->endScan();  
                throw NoSuchKeyFoundException();      
            } else{ // otherwise, recurse on non-leaf nodes
                try{
                    int nxt = startScanHelper(node->pageNoArray[i]);
                    this->bufMgr->unPinPage(this->file, pageNum, false);
                    return nxt;
                }catch (NoSuchKeyFoundException){
                    this->bufMgr->unPinPage(this->file, pageNum, false);
                    throw NoSuchKeyFoundException(); 
                }
            }
        }
    }
    assert(false);
    return -1;
}

/**
 * This method is used to begin a “filtered scan” of the index. For example, if the method is called 
 * using arguments (1,GT,100,LTE), then the scan should seek all entries greater than 1 and less than 
 * or equal to 100. 
 * 
 * If another scan is already executing, that needs to be ended here.
 * Set up all the variables for scan. Start from root to find out the leaf page that contains the 
 * first RecordID that satisfies the scan parameters. Keep that page pinned in the buffer pool.
 * 
 * @param lowVal	Low value of range, pointer to integer
 * @param lowOp		The operation to be used in testing the low range. You should only support GT 
 * and GTE here; anything else should throw BadOpcodesException. Note that the Operator enumeration 
 * is deﬁned in btree.h. 
 * @param highVal	High value of range, pointer to integer
 * @param highOp	The operation to be used in testing the high range. You should only support LT
 * and LTE here; anything else should throw BadOpcodesException
 * 
 * Both the high and low values are in a binary form, i.e., for integer keys, these point to the 
 * address of an integer.
 * 
 * @throws  BadOpcodesException If lowOp and highOp do not contain one of their their expected values 
 * @throws  BadScanrangeException If lowVal > highval
 * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that satisfies the scan criteria.
 **/
const void BTreeIndex::startScan(const void *lowValParm,
                                 const Operator lowOpParm,
                                 const void *highValParm,
                                 const Operator highOpParm)
{
    // If another scan is already executing, that needs to be ended here.
    if(this->scanExecuting)
        endScan();

    // Initialize the variables in BTreeIndex
    this->lowValInt = getint(lowValParm);
    this->highValInt = getint(highValParm);
    this->lowOp = lowOpParm;
    this->highOp = highOpParm;
    
    // BadOpcodesException 
    if((this->lowOp != GT && this->lowOp != GTE) 
       || (this->highOp != LT && this->highOp != LTE))
        throw BadOpcodesException();

    // BadScanrangeException 
    if(this->lowValInt > this->highValInt)
        throw BadScanrangeException();

    // set the scan state variable to true 
    this->scanExecuting = true; 

    // nextEntry(int): Index of next entry to be scanned in current leaf being scanned.
    // currentPage & currentPageData updated in this helper function 
    this->nextEntry = startScanHelper(this->rootPageNum);
}

/**
 * Fetch the record id of the next index entry (tuple) that matches the scan criteria.
 * Return the next record from current page being scanned. If current page has been scanned to its 
 * entirety, move on to the right sibling of current page, if any exists, to start scanning that 
 * page. Make sure to unpin any pages that are no longer required.
 * 
 * @param outRid RecordId of next record found that satisfies the scan criteria. Return in this.
 * @throws ScanNotInitializedException If no scan has been initialized.
 * @throws IndexScanCompletedException If the scan has reached the end. 
 **/
const void BTreeIndex::scanNext(RecordId &outRid)
{
    // if no scan has been initialized 
    if(this->scanExecuting == false)
        throw ScanNotInitializedException(); 
    // if next entry invalid 
    LeafNodeInt* currLeafNode = (LeafNodeInt*) this->currentPageData;
    if(this->nextEntry == INT_MAX || currLeafNode->ridArray[this->nextEntry] == INVALID_RECORD){
        throw IndexScanCompletedException();
    }
        
    int i = this->nextEntry;
    if((this->highOp == LT && (currLeafNode->keyArray[i] < this->highValInt))
    || (this->highOp == LTE && (currLeafNode->keyArray[i] <= this->highValInt))){
        outRid = currLeafNode->ridArray[i];
    }else{ 
        throw IndexScanCompletedException();
    }
    
    // advance nextEntry
    if (this->nextEntry < INTARRAYLEAFSIZE-1 && currLeafNode->ridArray[this->nextEntry+1]!=INVALID_RECORD){
        this->nextEntry++;
    }else{
        // Still has next page
        if (currLeafNode->rightSibPageNo!=Page::INVALID_NUMBER){
            // move to the right sibling 
            PageId lastPageId = this->currentPageNum;
            this->nextEntry = 0;
            this->currentPageNum = currLeafNode->rightSibPageNo; 
            this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
            this->bufMgr->unPinPage(this->file, lastPageId, false);
        }else{
            this->nextEntry = INT_MAX;
        }
    }
}

/**      
 * Terminate the current scan. Reset scan specific variables.
 * 
 * @throws ScanNotInitializedException If no scan has been initialized. (When called before a 
 * successful startScan call.)
 **/
const void BTreeIndex::endScan()
{
    // Throw exception when called before a successful startScan call 
    if(this->scanExecuting == false)
        throw ScanNotInitializedException();

    // Unpin any pinned pages that have been pinned for the purpuse
    // Only one page is pinned for scanning purpose, and it's not marked dirty  
    this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
    
    // Reset scan specific varaible 
    this->highOp = EMPTY; 
    this->lowOp = EMPTY;
    this->scanExecuting = false; 
    this->currentPageData = (Page*)NULL;
    this->currentPageNum = (PageId)NULL;
    this->highValInt = INT_MAX;
    this->lowValInt = INT_MIN;
    this->nextEntry = -1; 
}
}