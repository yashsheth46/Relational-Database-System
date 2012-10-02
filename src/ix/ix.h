
#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../pf/pf.h"
#include "../rm/rm.h"

# define IX_EOF (-1)  // end of the index scan

using namespace std;

typedef struct{
	void* data;
	int length;
}VarCharSplitValue;

typedef union{
	int IntSplitValue;
	float FloatSplitValue;
	VarCharSplitValue v;
}splitValue;

//PF_Manager *pf_IX;

class IX_IndexHandle;

class IX_Manager {
 public:
  static IX_Manager* Instance();
  PF_Manager *pf_IX;

  //Variables for scan
	vector<string> attributes;
	vector<cat> attrs;						//Catalog info for all projected attributes
  string conditionAttribute;
  cat condAttr;							//Catalog info for only the comparison attribute
  CompOp compOp;
  const void *value;
  //End variables for scan

  RC CreateIndex(const string tableName,       // create new index
		 const string attributeName);
  RC DestroyIndex(const string tableName,      // destroy an index
		  const string attributeName);
  RC OpenIndex(const string tableName,         // open an index
	       const string attributeName,
	       IX_IndexHandle &indexHandle);
  RC CloseIndex(IX_IndexHandle &indexHandle);  // close index
  
  RC ScanCatalog(const string tableName,
	      const string conditionAttribute,
	      const CompOp compOp,                  // comparision type such as "<" and "="
	      const void *value,                    // used in the comparison
	      const vector<string> &attributeNames, // a list of projected attributes
	      RM_ScanIterator &rm_ScanIterator);

 protected:
  IX_Manager   ();                             // Constructor
  ~IX_Manager  ();                             // Destructor
 

 private:
  static IX_Manager *_ix_manager;
};
int max(int a,int b);


class IX_IndexHandle {
 public:
	PF_FileHandle ixHandle;
	int currentPageNo;
	AttrType indexKeyType;

	//Split Parameters
	bool rootSplit;
	bool splitCheck;
	bool deleteCheck;
	int newPageNo;
	splitValue value;
	bool NonLeaflastElement;

  IX_IndexHandle  (){
	  currentPageNo=-1;
	  splitCheck=false;
  	  rootSplit=true;
  	  newPageNo=-1;
  	NonLeaflastElement=true;
  	deleteCheck=false;
  }                           // Constructor
  ~IX_IndexHandle (){}                           // Destructor

  // The following two functions are using the following format for the passed key value.
  //  1) data is a concatenation of values of the attributes
  //  2) For int and real: use 4 bytes to store the value;
  //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
  RC InsertEntry(void *key, const RID &rid);  // Insert new index entry
  RC DeleteEntry(void *key, const RID &rid);  // Delete index entry
  RC Split(void *bufferPage,short int traversePointer,int currentPageNo,void * key, RID rid);
  void BubbleSortLeaf(void *buffer1, int pg, AttrType type);

//  IX_IndexHandle& operator=(const IX_IndexHandle &rhs);
};


class IX_IndexScan {
public:
IX_IndexHandle iHandle;

vector<float> dataVec;
vector<string> strVec;
vector<int> strLenVec;
vector<RID> ridVec;

IX_IndexScan(){} // Constructor
~IX_IndexScan(){} // Destructor

// for the format of "value", please see IX_IndexHandle::InsertEntry()
RC OpenScan(IX_IndexHandle &indexHandle, // Initialize index scan
CompOp compOp,
void *value);

RC GetNextEntry(RID &rid); // Get next matching entry
RC CloseScan(); // Terminate index scan
};

// print out the error message for a given return code
void IX_PrintError (RC rc);


#endif
