/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <string.h>
#include <cstring>
#include <cstdio>
#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/page_not_pinned_exception.h"

//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------


BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
	this->bufMgr = bufMgrIn;
	this->scanExecuting = false; // we are not scanning yet

	// Save attributes
	if (attrType == INTEGER) {
		this->leafOccupancy = INTARRAYLEAFSIZE;
		this->nodeOccupancy = INTARRAYNONLEAFSIZE;
	}
	else if (attrType == DOUBLE) {
		this->leafOccupancy = DOUBLEARRAYLEAFSIZE;
		this->nodeOccupancy = DOUBLEARRAYNONLEAFSIZE;
	}
	else if (attrType == STRING) {
		this->leafOccupancy = STRINGARRAYLEAFSIZE;
		this->nodeOccupancy = STRINGARRAYNONLEAFSIZE; 
	}

	// Construct index file name
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	std::string indexName = idxStr.str();

	if (File::exists(indexName)) {
		// Open existing index file
		this->file = new BlobFile(indexName, false);
		this->openIndexFile(relationName, attrByteOffset, attrType);
	}
	else {
		// Create new index file
		this->file = new BlobFile(indexName, true);
		this->createIndexFile(relationName, attrByteOffset, attrType);

	}
	// Output index file name
	outIndexName = indexName;
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	this->bufMgr->flushFile(this->file);
	this->scanExecuting = false;
	delete this->file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	
	Page* rootPage;

	bool isLeaf = false; // if lower level is leaf
	if (this->attributeType == INTEGER) {
		RIDKeyPair<int> leafEntry;
		leafEntry.set(rid, *((int*)(key)));
		// Special case: root is the leaf
		if (rootIsLeaf){
			insertRootLeaf<int, struct LeafNodeInt,struct NonLeafNodeInt,PageKeyPair<int>,RIDKeyPair<int>>(leafEntry);
		}
		// Filter paramters base on conditions; traverse and insert the entry later down the recursion
		else {
			PageKeyPair<int>	newPagePair;
			newPagePair.set(0,leafEntry.key);

			traverse<int, struct LeafNodeInt,struct NonLeafNodeInt,PageKeyPair<int>,RIDKeyPair<int>>(this->rootPageNum, newPagePair, leafEntry);
			PageId oldPageNum = this->rootPageNum;
			this->bufMgr->readPage(this->file, oldPageNum, rootPage);
			NonLeafNodeInt* rootNode = (NonLeafNodeInt*)rootPage;
			
			// if new child node is created (split happened in immediate child level)
			if (newPagePair.pageNo != 0) {
				createNewRoot<int, struct LeafNodeInt,struct NonLeafNodeInt,PageKeyPair<int>,RIDKeyPair<int>>(oldPageNum, newPagePair, false);
			}
			this->bufMgr->unPinPage(this->file, oldPageNum, true);
		}
	}
	// same case for other attribute types
	else if (this->attributeType == DOUBLE) {
		RIDKeyPair<double> leafEntry;
		leafEntry.set(rid, *((double*)(key)));
		if (rootIsLeaf) {
			insertRootLeaf<double, struct LeafNodeDouble,struct NonLeafNodeDouble,PageKeyPair<double>,RIDKeyPair<double>>(leafEntry);
		}
		else {
			PageKeyPair<double>	newPagePair;
			newPagePair.set(0,leafEntry.key);
			traverse<double, struct LeafNodeDouble,struct NonLeafNodeDouble,PageKeyPair<double>,RIDKeyPair<double>>(this->rootPageNum, newPagePair, leafEntry);

			PageId oldPageNum = this->rootPageNum;
			this->bufMgr->readPage(this->file, oldPageNum, rootPage);
			NonLeafNodeDouble* rootNode = (NonLeafNodeDouble*)rootPage;
			if (newPagePair.pageNo!= 0) {
				if (rootNode->pageNoArray[nodeOccupancy] == 0) {
					putEntryNonLeaf<double, struct NonLeafNodeDouble,PageKeyPair<double>> (rootNode,newPagePair);
				}
				else{
					PageKeyPair<double> rightFirstEntry;
					splitNonLeaf<double, struct NonLeafNodeDouble,PageKeyPair<double>>(rootNode,newPagePair,rightFirstEntry);
					createNewRoot<double, struct LeafNodeDouble,struct NonLeafNodeDouble,PageKeyPair<double>,RIDKeyPair<double>>(this->rootPageNum, rightFirstEntry, false);
				}
			}
			this->bufMgr->unPinPage(this->file, oldPageNum, true);
		}
	}
	else if (this->attributeType == STRING) {
		RIDKeyPair<char*> leafEntry;
		char* trimmedString = (char*)malloc(STRINGSIZE);
		snprintf(trimmedString, STRINGSIZE,"%s",(char*)key);
		leafEntry.set(rid, trimmedString);
		if(rootIsLeaf){
			insertRootLeaf<char*, struct LeafNodeString,struct NonLeafNodeString,PageKeyPair<char*>,RIDKeyPair<char*>>(leafEntry);
		}
		else{
			PageKeyPair<char*>	newPagePair;
			newPagePair.set(0,leafEntry.key);
			PageId oldPageNum = this->rootPageNum;
			traverse<char*, struct LeafNodeString,struct NonLeafNodeString,PageKeyPair<char*>,RIDKeyPair<char*>>(oldPageNum, newPagePair, leafEntry);
			
			this->bufMgr->readPage(this->file, oldPageNum, rootPage);
			NonLeafNodeString* rootNode = (NonLeafNodeString*)rootPage;

			if (newPagePair.pageNo!= 0) {
				if (rootNode->pageNoArray[nodeOccupancy] == 0) {
					putEntryNonLeaf<char*, struct NonLeafNodeString,PageKeyPair<char*>> (rootNode,newPagePair);
				}
				else{
					PageKeyPair<char*> rightFirstEntry;
					splitNonLeaf<char*, struct NonLeafNodeString,PageKeyPair<char*>>(rootNode,newPagePair,rightFirstEntry);
					createNewRoot<char*, struct LeafNodeString,struct NonLeafNodeString,PageKeyPair<char*>,RIDKeyPair<char*>>(oldPageNum, rightFirstEntry, false);
				}
			}
			this->bufMgr->unPinPage(this->file, oldPageNum, true);
		}
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
		if(lowOpParm != GT && lowOpParm != GTE){
			throw BadOpcodesException ();
		}
		if(highOpParm != LT && highOpParm != LTE){
			throw BadOpcodesException ();
		}

		// if another scan is executing, end it here
		if (scanExecuting) {
			endScan();
		}

		if (attributeType == INTEGER) {
			if (*(int*)lowValParm > *(int*)highValParm){
				throw BadScanrangeException();
			}
			lowValInt = *(int*)lowValParm;
			highValInt = *(int*)highValParm;
		}
		if (attributeType == DOUBLE) {
			if (*(double*)lowValParm > *(double*)highValParm){
				throw BadScanrangeException();
			}
			lowValDouble = *(double*)lowValParm;
			highValDouble = *(double*)highValParm;
		}
		if (attributeType==STRING) {
			if (strncmp((char*)lowValParm,(char*)highValParm,STRINGSIZE)>0){
				throw BadScanrangeException();
			}
			this->lowValString = std::string((char*)lowValParm,STRINGSIZE);
			this->highValString = std::string((char*)highValParm,STRINGSIZE);
		}

		scanExecuting = true;
		lowOp = lowOpParm;
		highOp = highOpParm;

		if(attributeType == INTEGER){
			scan<int, struct LeafNodeInt,struct NonLeafNodeInt,class PageKeyPair<int>,class RIDKeyPair<int>>(lowValInt);
		}
		if(attributeType == DOUBLE){
			scan<double, struct LeafNodeDouble,struct NonLeafNodeDouble,class PageKeyPair<double>,class RIDKeyPair<double>>(lowValDouble);
		}
		if(attributeType == STRING){
			char* key = (char*)malloc(STRINGSIZE);
			snprintf(key, STRINGSIZE, "%s",lowValString.c_str());
			scan<char*, struct LeafNodeString,struct NonLeafNodeString,class PageKeyPair<char*>,class RIDKeyPair<char*>>(key);
		}

}

// -----------------------------------------------------------------------------
// BTreeIndex::scan
// -----------------------------------------------------------------------------
template<class T,class L_T,class NL_T,class P_T,class RID_T> void BTreeIndex::scan(T lowVal) 
{
	Page* tmpPage;
	PageId tmpPageNo;
	NL_T* tmpNonLeafNode;
	
	// if root is leaf, that means the curent page for scanning is the only page in the tree
	if (rootIsLeaf){
		this->currentPageNum = this->rootPageNum;		
		this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);	
		nextEntry = findPos<T, L_T,NL_T,P_T,RID_T>(true, false, this->rootPageNum, lowVal);

		if (nextEntry == -1) {
			throw IndexScanCompletedException();
		}
		return;
	}

	// else we need to traverse down to the right leaf node
	tmpPageNo = this->rootPageNum;
	this->bufMgr->readPage(this->file,tmpPageNo,tmpPage);
	this->bufMgr->unPinPage(this->file,tmpPageNo,false);
	tmpNonLeafNode = (NL_T*) tmpPage;

	// if current node is not the level above leaf node, keep traversing
	while (tmpNonLeafNode->level != 1) {
		int nextPos = findPos<T, L_T,NL_T,P_T,RID_T>(false, true, tmpPageNo, lowVal);
		tmpPageNo = tmpNonLeafNode->pageNoArray[nextPos];
		this->bufMgr->readPage(this->file,tmpPageNo,tmpPage);
		tmpNonLeafNode = (NL_T*)tmpPage;
		this->bufMgr->unPinPage(this->file,tmpPageNo,false);
	}
	
	// traversed to the right nonleafnode. Get the correct current page and then 
	// unpin this nonleafnode page.
	
	int leafPagePos = findPos<T,L_T,NL_T,P_T,RID_T>(false, true,tmpPageNo, lowVal);

	if (leafPagePos == -1) {
		// exit scan if error happens
		throw IndexScanCompletedException();
	}
	this->currentPageNum = tmpNonLeafNode->pageNoArray[leafPagePos];

	nextEntry = findPos<T, L_T,NL_T,P_T,RID_T>(true, false,this->currentPageNum, lowVal);

	if (nextEntry == -1) {
		// exit scan if error happens
		throw IndexScanCompletedException();
	}

	this->bufMgr->readPage(this->file,this->currentPageNum,this->currentPageData);
	this->bufMgr->unPinPage(this->file, this->currentPageNum, false);

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{
	if (!scanExecuting){
		throw ScanNotInitializedException();
	}
	if (this->currentPageNum == 0){
		throw IndexScanCompletedException();
	} 
	if (attributeType == INTEGER){
		LeafNodeInt* currLeaf = (LeafNodeInt*) (this->currentPageData);
		if(highOp == LT && compareKey((void*)&(currLeaf->keyArray[nextEntry]),(void*)(&highValInt)) >=0)  {
			throw IndexScanCompletedException();
		}
		else if(highOp == LTE && compareKey((void*)&(currLeaf->keyArray[nextEntry]),(void*)(&highValInt)) >0)  {
			throw IndexScanCompletedException();
		}		
		outRid = currLeaf->ridArray[nextEntry];	
		nextEntry++;
			
		if( nextEntry == leafOccupancy ||  currLeaf->ridArray[nextEntry].page_number == 0 ){
			
			this->currentPageNum =  currLeaf->rightSibPageNo;
			if(currLeaf->rightSibPageNo == 0) return;
			this->bufMgr->readPage(this->file,this->currentPageNum,this->currentPageData);
			this->bufMgr->unPinPage(this->file,this->currentPageNum,false);
			nextEntry = 0;
		}
	}
	else if(attributeType == DOUBLE){
		LeafNodeDouble* currLeaf = (LeafNodeDouble*) (this->currentPageData);	
		if(highOp == LT && compareKey((void*)&(currLeaf->keyArray[nextEntry]),(void*)(&highValDouble)) >=0)  {
			throw IndexScanCompletedException();
		}
		else if(highOp == LTE && compareKey((void*)&(currLeaf->keyArray[nextEntry]),(void*)(&highValDouble)) >0)  {
			throw IndexScanCompletedException();
		}	
		outRid = currLeaf->ridArray[nextEntry];	
		nextEntry++;
		if( nextEntry == leafOccupancy || currLeaf->ridArray[nextEntry].page_number == 0 ){
			this->currentPageNum =  currLeaf->rightSibPageNo;
			if(currLeaf->rightSibPageNo == 0) return;
			this->bufMgr->readPage(this->file,this->currentPageNum,this->currentPageData);
			this->bufMgr->unPinPage(this->file,this->currentPageNum,false);
			nextEntry = 0;
		}
	}
	else if(attributeType == STRING){
		LeafNodeString* currLeaf = (LeafNodeString*) (this->currentPageData);
		char* key = (char*) (malloc)(STRINGSIZE);
		snprintf(key,STRINGSIZE, "%s",highValString.c_str());

		if(highOp == LT && compareKey((void*)&(currLeaf->keyArray[nextEntry]),key) >=0)  {
			throw IndexScanCompletedException();
		}
		else if(highOp == LTE && compareKey((void*)&(currLeaf->keyArray[nextEntry]),key) >0)  {
			throw IndexScanCompletedException();
		}	
		outRid = currLeaf->ridArray[nextEntry];	
		nextEntry++;
		if(nextEntry == leafOccupancy || currLeaf->ridArray[nextEntry].page_number == 0 ) {
			this->currentPageNum =  currLeaf->rightSibPageNo;
			if(currLeaf->rightSibPageNo == 0) return;
			this->bufMgr->readPage(this->file,this->currentPageNum,this->currentPageData);
			this->bufMgr->unPinPage(this->file,this->currentPageNum,false);
			nextEntry = 0;
		}	
	}
	
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{
	// if no scan is live, throw exception
	if (!scanExecuting) {
		throw ScanNotInitializedException();
	}
	// unpin any pinned pages
	try{
		if(this->currentPageNum !=0) this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
	}
	catch(PageNotPinnedException e){

	}
	
	scanExecuting = false;
}

///////////////////////
// Custom Functions //
/////////////////////

// -----------------------------------------------------------------------------
// BTreeIndex::compareKey
// -----------------------------------------------------------------------------
template<class T> int BTreeIndex::compare(T k1, T k2) {
	if (k1 > k2) {
		return 1;
	}
	else if (k1 < k2) {
		return -1;
	}
	return 0;
}

template<> int BTreeIndex::compare<char*>(char* k1, char* k2) {
	return strcmp((char*)k1, (char*)k2);
}


int BTreeIndex::compareKey(void* k1, void* k2) {
	if(attributeType == INTEGER){
		if(*(int*)k1 > *(int*)k2){
			return 1;
		}
		else if(*(int*)k1 < *(int*)k2){
			return -1;
		}
		else{
			return 0;
		}
	}
	else if(attributeType == DOUBLE){
		if(*(double*)k1 > *(double*)k2){
				return 1;
		}
		else if(*(double*)k1 < *(double*)k2){
			return -1;
		}
		else{
			return 0;
		}
	}
	else if(attributeType == STRING){
		return strcmp((char*)k1, (char*)k2);
	}
	return -2;
}


// -----------------------------------------------------------------------------
// BTreeIndex::findPos
// -----------------------------------------------------------------------------
//
template<class T, class L_T,class NL_T,class P_T,class RID_T> int BTreeIndex::findPos(
bool leaf, bool nonleaf, PageId tmpPageNo, T lowVal){
	int result = -1;
	int count = 0;
	if (nonleaf) {
		int pos = 0;
		Page* tmpPage;

		this->bufMgr->readPage(this->file,tmpPageNo,tmpPage);
		NL_T* currNode = (NL_T*) tmpPage;
		T itr;
		
		while (pos < nodeOccupancy && currNode->pageNoArray[pos] != 0) {
			itr = currNode->keyArray[pos];
			if (attributeType == STRING) {
				if(compare(itr, lowVal) > 0) {
					this->bufMgr->unPinPage(this->file,tmpPageNo,false);
					return pos;
				}
			}
			else {
				if(compare<T>(itr, lowVal) > 0) {
					this->bufMgr->unPinPage(this->file,tmpPageNo,false);
					return pos;
				}
			}
			pos++;
		}

		this->bufMgr->unPinPage(this->file,tmpPageNo,false);
		result = (currNode->pageNoArray[pos] == 0)? (pos-1) : pos;
	}
	if(leaf){
		int pos = 0;
		Page* tmpPage;
		T itr;

		this->bufMgr->readPage(this->file,tmpPageNo,tmpPage);
		L_T* currNode = (L_T*) tmpPage;

		while (pos < leafOccupancy && currNode->ridArray[pos].page_number != 0) {
			itr = currNode->keyArray[pos];
			if(lowOp == GT){
				if (attributeType == STRING) {
					if (compare(itr, lowVal) > 0) {
						this->bufMgr->unPinPage(this->file,tmpPageNo,false);
						return pos;
					}
				}
				else {
					if (compare<T>(itr, lowVal) > 0) {
						this->bufMgr->unPinPage(this->file,tmpPageNo,false);
						return pos;
					}
				}
			}
			else if(lowOp == GTE){
				if (attributeType == STRING) {
					if (compare(itr, lowVal) >= 0) {
						this->bufMgr->unPinPage(this->file,tmpPageNo,false);
						return pos;
					}
				}
				else {
					if (compare<T>(itr, lowVal) >= 0) {
						this->bufMgr->unPinPage(this->file,tmpPageNo,false);
						return pos;
					}
				}
			}
			pos++;	
		}
		this->bufMgr->unPinPage(this->file,tmpPageNo,false);
		result = (pos == leafOccupancy || currNode->ridArray[pos].page_number == 0)? (pos - 1):pos ;
	}

	return result;

}
	


// -----------------------------------------------------------------------------
// BTreeIndex::openIndexFile
// -----------------------------------------------------------------------------
//
const void BTreeIndex::openIndexFile(const std::string & relationName, const int attrByteOffset, const Datatype attrType) 
{
	Page* metaPage; // header page that stores struct IndexMetaInfo
	Page* rootPage; // root of Btree
	IndexMetaInfo * meta;

	// Read meta info page (header page)
	this->headerPageNum = file->getFirstPageNo(); 	
	this->bufMgr->readPage(this->file, this->headerPageNum, metaPage);

	// Unpin file
	meta = (IndexMetaInfo *) metaPage;	
	
	// Save attributes
	if (meta->attrByteOffset == attrByteOffset && meta->attrType == attrType) {
		this->attrByteOffset = meta->attrByteOffset;
		this->attributeType = meta->attrType;
		this->rootPageNum = meta->rootPageNo;
	}
	else {
		throw BadIndexInfoException("Index info not matched");
	}
	
	this->rootIsLeaf = (meta->rootPageNo == 2);
	this->scanExecuting = false;	

	if (attrType == INTEGER) {
		this->leafOccupancy = INTARRAYLEAFSIZE;
		this->nodeOccupancy = INTARRAYNONLEAFSIZE;
	}
	else if (attrType == DOUBLE) {
		this->leafOccupancy = DOUBLEARRAYLEAFSIZE;
		this->nodeOccupancy = DOUBLEARRAYNONLEAFSIZE;
	}
	else if (attrType == STRING) {
		this->leafOccupancy = STRINGARRAYLEAFSIZE;
		this->nodeOccupancy = STRINGARRAYNONLEAFSIZE; 
	}

	this->bufMgr->unPinPage(this->file, this->headerPageNum, false);

}



// -----------------------------------------------------------------------------
// BTreeIndex::createIndexFile
// -----------------------------------------------------------------------------
//
const void BTreeIndex::createIndexFile(const std::string & relationName, const int attrByteOffset, const Datatype attrType)
{
	Page* metaPage; // header page that stores struct IndexMetaInfo
	Page* rootPage; // root of Btree
	IndexMetaInfo * meta;
	// Save attributes
	this->attrByteOffset = attrByteOffset;
	this->attributeType = attrType;
	this->rootIsLeaf = true; // root node is initially a LeafNode

	// allocate metaInfo page, allocate root page
	this->bufMgr->allocPage(this->file, this->headerPageNum, metaPage);
	this->bufMgr->allocPage(this->file, this->rootPageNum, rootPage);

	meta = (IndexMetaInfo *) metaPage;

	meta->attrByteOffset = this->attrByteOffset;
	meta->attrType = this->attributeType;
	meta->rootPageNo = this->rootPageNum;
	strcpy(meta->relationName, relationName.c_str());

	// Cast rootPage to LeafNode (root is a leaf for a new Btree)
	if (attrType == INTEGER) {
		LeafNodeInt* root = (LeafNodeInt*)rootPage;
		root->rightSibPageNo = 0;
	}
	else if (attrType == DOUBLE) {
		LeafNodeDouble* root = (LeafNodeDouble*)rootPage;
		root->rightSibPageNo = 0;
	}
	else if (attrType == STRING) {
		LeafNodeString* root = (LeafNodeString*)rootPage;
		root->rightSibPageNo = 0;
	}


	this->bufMgr->unPinPage(this->file, this->rootPageNum, true);
	this->bufMgr->unPinPage(this->file, this->headerPageNum, true);

	// Scan the relation file

	FileScan* scan = new FileScan(relationName, this->bufMgr);
	try {
		while (1) {
			std::string recordString;
			const char* record;

			//const char* key;
			RecordId rid;
			int attr_int;
			double attr_double;

			// Iterate the records in relation file
			scan->scanNext(rid);
			recordString = scan->getRecord();
			record = recordString.c_str();
			void* key = (void*)(record+attrByteOffset);
			insertEntry(key,rid);
		}
		
	}
	catch (EndOfFileException e) 
	{
		// do nothing
		std::cout << "Finished creating new index file." << std::endl;		
	}

	this->bufMgr->flushFile(this->file);
	delete scan;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertRootLeaf
// insert entry into root when root is a leaf node 
// split old root into two leafs and create new root if old root is full after insert
// ----------------------------------------------------------------------------
template<class T, class L_T,class NL_T,class P_T, class RID_T> void BTreeIndex::insertRootLeaf( RID_T RIDPair){

	Page* leafPage;
	L_T* leafNode;

	this->bufMgr->readPage(this->file, this->rootPageNum, leafPage);
	PageId oldPageNum = this->rootPageNum;
	leafNode = (L_T*) leafPage;

	// If rootLeaf is not full just put the entry in
	if ( leafNode->ridArray[this->leafOccupancy-1].page_number == 0 ) {
		putEntryLeaf<T, L_T,RID_T> (leafNode,RIDPair);
	}
	else {
		// splite leaf node into 2
		P_T newChildPage;
		splitLeaf<T, L_T,P_T,RID_T>(leafNode, RIDPair, newChildPage);

		// create a new root (non-leaf node)
		createNewRoot<T, L_T,NL_T,P_T,RID_T>(rootPageNum, newChildPage, true);

	}
	this->bufMgr->unPinPage(this->file, oldPageNum, true);
}

// -----------------------------------------------------------------------------
// BTreeIndex::assign
// assign value
// ----------------------------------------------------------------------------

void BTreeIndex::assignPrime (void* des, void* src) {
	if (attributeType == INTEGER) {
		*((int*)des) = *((int*)src);
	}
	else if (attributeType == DOUBLE) {
		*(double*) des = *(double*)src;
	}
	else {
		// string is not handled here
	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::assign
// assign value for int and double
// ----------------------------------------------------------------------------
template<class T> void BTreeIndex::assign(T a, T b) {
	
}
 
// -----------------------------------------------------------------------------
// BTreeIndex::assign
// assign value for char*
// ----------------------------------------------------------------------------
template<> void BTreeIndex::assign <char*> (char* a, char* b) {
	strncpy(a, b, STRINGSIZE);
}


// -----------------------------------------------------------------------------
// BTreeIndex::putEntryLeaf
// insert entry into leaf when leaf is not null
// ----------------------------------------------------------------------------
template<class T, class L_T,class RID_T> void BTreeIndex::putEntryLeaf(L_T* leafNode, RID_T RIDPair){
	
	int pos = 0;
	int i = 0;
	RID_T itr;

	while( (pos < leafOccupancy) && (leafNode->ridArray[pos].page_number != 0)) {
		itr.set(leafNode->ridArray[pos], leafNode->keyArray[pos]);
		if ( compare(itr.key, RIDPair.key) >= 0) {
			break;
		}
		pos++;
	}
	// else shift everything to the right, insert at pos
	for (i = leafOccupancy-1; i > pos; i--){

		leafNode->ridArray[i] = leafNode->ridArray[i-1];
		if (attributeType == STRING) {
			assign( leafNode->keyArray[i], leafNode->keyArray[i-1]);
		}
		else {
			assignPrime( &(leafNode->keyArray[i]), &(leafNode->keyArray[i-1]) );
		}
	}

	leafNode->ridArray[pos] = RIDPair.rid;
	
	if (attributeType == STRING) {
		assign(leafNode->keyArray[pos], RIDPair.key);
	}
	else {
		assignPrime( &(leafNode->keyArray[pos]), &(RIDPair.key) );
	}

}

// -----------------------------------------------------------------------------
// BTreeIndex::putEntryNonLeaf
// insert entry into non-leaf node
// ----------------------------------------------------------------------------
template<class T, class NL_T,class P_T> void BTreeIndex::putEntryNonLeaf(NL_T* nonLeafNode, P_T pagePair){
	
	int pos = 0;
	int pgno_pos = 0;
	int key_pos = 0;
	P_T itr;

	while( pos < nodeOccupancy && nonLeafNode->pageNoArray[pos] != 0 ) {
		itr.set(nonLeafNode->pageNoArray[pos], nonLeafNode->keyArray[pos]);
		if( compare(itr.key, pagePair.key) >= 0) {
			break;
		}
		pos++;
	}

	// else shift everything to the right, insert at pos
	for(int i = nodeOccupancy-1; i > pos; i--){
		if (attributeType == STRING) {
			assign(nonLeafNode->keyArray[i], nonLeafNode->keyArray[i-1] );
		}
		else {
			assignPrime( &(nonLeafNode->keyArray[i]), &(nonLeafNode->keyArray[i-1]) );
		}
		nonLeafNode->pageNoArray[i+1] = nonLeafNode->pageNoArray[i];	
	}

	if (nonLeafNode->pageNoArray[pos] == 0) {
		key_pos = pos-1;
		pgno_pos = pos;
	}
	else {
		key_pos = pos;
		pgno_pos = pos+1;
	}
	
	nonLeafNode->pageNoArray[pgno_pos] = pagePair.pageNo;

	if (attributeType == STRING) {
		assign(nonLeafNode->keyArray[key_pos], pagePair.key);
	}
	else {
		assignPrime( &(nonLeafNode->keyArray[key_pos]), &(pagePair.key) );
	}

}


// -----------------------------------------------------------------------------
// BTreeIndex::splitLeaf
// split a leaf node into 2, return the new page number
// ----------------------------------------------------------------------------
template<class T, class L_T,class P_T,class RID_T> void BTreeIndex::splitLeaf(L_T* leafNode, RID_T RIDPair, P_T& rightFirst) {
	PageId newPageNo;
	Page* newPage;
	L_T* newLeafNode;
	int mid = leafOccupancy/2+1;

	this->bufMgr->allocPage(this->file, newPageNo, newPage); // allocate a new page
	newLeafNode = (L_T*)newPage; // create new leaf node

	for (int i = mid; i < leafOccupancy; i++) {
		newLeafNode->ridArray[i-mid] = leafNode->ridArray[i];
		leafNode->ridArray[i].page_number = 0;

		if (attributeType == STRING) {
			assign(newLeafNode->keyArray[i-mid], leafNode->keyArray[i] );
		}
		else {
			assignPrime( &(newLeafNode->keyArray[i-mid]), &(leafNode->keyArray[i]) );
		}
	}

	newLeafNode->rightSibPageNo = leafNode->rightSibPageNo;
	leafNode->rightSibPageNo = newPageNo;

	rightFirst.set(newPageNo, newLeafNode->keyArray[0]);

	if ( compare(RIDPair.key, rightFirst.key) < 0 ){
		putEntryLeaf<T, L_T,RID_T>(leafNode,RIDPair);
	}
	else {
		putEntryLeaf<T, L_T,RID_T>(newLeafNode,RIDPair);
	}

	bufMgr->unPinPage(file, newPageNo, true);

}


// -----------------------------------------------------------------------------
// BTreeIndex::splitNonLeaf
// split a non-leaf node into 2, return the new page number
// ----------------------------------------------------------------------------
template<class T, class NL_T,class P_T> void BTreeIndex::splitNonLeaf(NL_T* nonLeafNode, P_T pagePair2insert, P_T& rightFirstEntry) {
	PageId newPageNo;
	Page* newPage;
	NL_T* newNonLeafNode;
	int mid = nodeOccupancy/2+1;

	this->bufMgr->allocPage(file, newPageNo, newPage);
	newNonLeafNode = (NL_T*)newPage;

	// new node has same level with spliteed node
	newNonLeafNode->level = nonLeafNode->level; 

	for (int i = mid; i < nodeOccupancy; i++) {
		newNonLeafNode->pageNoArray[i-mid] = nonLeafNode->pageNoArray[i];
		if (i != mid) 
			nonLeafNode->pageNoArray[i] = 0;
		if (attributeType == STRING) {
			assign(newNonLeafNode->keyArray[i-mid], nonLeafNode->keyArray[i] );
		}
		else {
			assignPrime( &(newNonLeafNode->keyArray[i-mid]), &(nonLeafNode->keyArray[i]) );
		}
	}
	newNonLeafNode->pageNoArray[nodeOccupancy-mid] = nonLeafNode->pageNoArray[nodeOccupancy];
	nonLeafNode->pageNoArray[nodeOccupancy] = 0;

	rightFirstEntry.set(newPageNo,newNonLeafNode->keyArray[0]);
	
	if (pagePair2insert.key<rightFirstEntry.key){
		putEntryNonLeaf <T, NL_T,P_T> (nonLeafNode, pagePair2insert);
	}
	else{
		putEntryNonLeaf <T, NL_T,P_T> (newNonLeafNode, pagePair2insert);
	}

	bufMgr->unPinPage(file, newPageNo, true);

} 


// -----------------------------------------------------------------------------
// BTreeIndex::createNewRoot
// create a new root node (non-leaf)
// ----------------------------------------------------------------------------
template<class T, class L_T,class NL_T,class P_T,class RID_T> void BTreeIndex::createNewRoot(PageId left, P_T rightFirst, bool isLeaf){
	Page* newRootPage;
	PageId newRootPageNo;
	Page* headerPage;
	NL_T* newRootNode;
	IndexMetaInfo * meta;

	this->bufMgr->allocPage(file, newRootPageNo, newRootPage); // allocate a new page
	// insert new values
	newRootNode = (NL_T*)newRootPage;
	newRootNode->pageNoArray[0] = left;
	newRootNode->pageNoArray[1] = rightFirst.pageNo;

	if (attributeType == STRING) {
		assign(newRootNode->keyArray[0], rightFirst.key);
	}
	else {
		assignPrime( &(newRootNode->keyArray[0]), &(rightFirst.key) );
	}

	if (isLeaf) {
		newRootNode->level = 1;
	}
	else {
		newRootNode->level = 0;
	}
	this->rootPageNum = newRootPageNo;
	this->rootIsLeaf = false;
	this->bufMgr->unPinPage(file, newRootPageNo, true);

	this->bufMgr->readPage(file, headerPageNum, headerPage);
	meta = (IndexMetaInfo*) headerPage;
	meta->rootPageNo = this->rootPageNum;
	// write back
	this->bufMgr->unPinPage(file, headerPageNum, true);

}

// -----------------------------------------------------------------------------
// BTreeIndex::traverse
// -----------------------------------------------------------------------------
template<class T, class L_T,class NL_T,class P_T,class RID_T> void BTreeIndex::traverse(PageId currPageNo, P_T& newPagePair, RID_T RIDPair2insert) 
{
	Page* rightPage;
	int pos = 0;
	PageId childPageNo;
	Page* childPage;
	L_T* childLeafNode;
	NL_T* childNonLeafNode;

	Page* currPage;
	NL_T* currNode;

	P_T itr;
	P_T rightFirstEntry;
	P_T pagePair2insert;


	this->bufMgr->readPage(file, currPageNo, currPage);
	currNode = (NL_T*) currPage;

	while (pos < nodeOccupancy && currNode->pageNoArray[pos] != 0) {
		itr.set( currNode->pageNoArray[pos], currNode->keyArray[pos]);
		if( compare(itr.key, RIDPair2insert.key) >= 0 ){
			break;
		} 
		pos++;
	}
	   
	if (currNode->pageNoArray[pos] == 0 && pos > 0) pos--;

	childPageNo = currNode->pageNoArray[pos]; 

	// check level, if currNode is at level 1 just insert entry into leaf node
	if (currNode->level == 1) {
		// check if leaf node is full, if it is need to split leaf node
		this->bufMgr->readPage(file, childPageNo, childPage);
		L_T* childLeafNode = (L_T*) childPage;

		if ( (childLeafNode->ridArray[leafOccupancy-1]).page_number == 0) {
			putEntryLeaf<T, L_T,RID_T>(childLeafNode, RIDPair2insert);
		}
		else {
			splitLeaf<T, L_T,P_T,RID_T>(childLeafNode, RIDPair2insert, pagePair2insert);

		  if (currNode->pageNoArray[nodeOccupancy] == 0) {
		  	putEntryNonLeaf<T, NL_T,P_T>(currNode,pagePair2insert);
		  }
		  // if current non-leaf node is full, split the leaf and insert into right node
		  else {	
		  	splitNonLeaf<T, NL_T,P_T>(currNode, pagePair2insert, rightFirstEntry);
		  	newPagePair = rightFirstEntry;
		  }
		}
		this->bufMgr->unPinPage(file,childPageNo,true); 
		this->bufMgr->unPinPage(file,currPageNo,true);
		return;
	}

	P_T newChildPagePair;
	Page* newChildPage;
	NL_T* newChildNode;
	T dummykey;

	newChildPagePair.set(0,dummykey);

	// if currNode is at level 0
	this->bufMgr->unPinPage(file,currPageNo,false); 
  traverse<T, L_T, NL_T, P_T, RID_T> (childPageNo, newChildPagePair, RIDPair2insert);

  Page* newReadCurr;
  this->bufMgr->readPage(file,currPageNo,newReadCurr);

  if (newChildPagePair.pageNo != 0) {
  	pagePair2insert.set(newChildPagePair.pageNo, newChildPagePair.key);

  	if (currNode->pageNoArray[nodeOccupancy]==0) {
  		// if currNode is not full
  		putEntryNonLeaf<T, NL_T,P_T>(currNode,pagePair2insert);
  	}
  	else {
  		// if currNode is full
  		splitNonLeaf<T, NL_T,P_T>(currNode, pagePair2insert, rightFirstEntry);
			newPagePair = rightFirstEntry;
  	}
  }
  this->bufMgr->unPinPage(file, currPageNo, (newChildPagePair.pageNo !=0) ); 

}

} // end namespace badgerdb


