
#include "ix.h"

#include<string.h>
#include<string>
#include<stdlib.h>

  IX_Manager *IX_Manager::_ix_manager=0;
//  PF_Manager* pf_IX;												//Page file manager instance

  IX_Manager* IX_Manager::Instance(){

	  if(!_ix_manager)
		  _ix_manager=new IX_Manager();

	  return _ix_manager;
  }


  IX_Manager::IX_Manager(){																					// Constructor
	  pf_IX=PF_Manager::Instance();

  }

//  IX_IndexHandle& IX_IndexHandle::operator=(const IX_IndexHandle &rhs)
//  {
//	  if(this == &rhs)
//		  return *this;
//
//	  this->NonLeaflastElement = rhs.NonLeaflastElement;
//	  this->currentPageNo = rhs.currentPageNo;
//	  this->deleteCheck = rhs.deleteCheck;
//	  this->indexKeyType = rhs.indexKeyType;
////	  this->ixHandle = rhs.ixHandle;
//	  this->newPageNo = rhs.newPageNo;
//	  this->rootSplit = rhs.rootSplit;
//	  this->splitCheck = rhs.splitCheck;
//	  this->value = rhs.value;
//	  return *this;
//  }


  RC IX_Manager::CreateIndex(const string tableName, const string attributeName){  								// create new index

	  char* makeName=(char*)malloc(tableName.length() + attributeName.length() + 5);
	  memcpy(makeName,"IX_",3);
	  memcpy(makeName + 3,tableName.c_str(),tableName.length());
	  memcpy(makeName + 3 + tableName.length(),"_",1);
	  memcpy(makeName + 4 + tableName.length(),attributeName.c_str(),attributeName.length());
	  *(makeName + 4 + tableName.length() + attributeName.length()) = '\0';
	  	  	  	  	  	  	  	  	  	  	  	  	  	  	  	  	  	  	  	  	  	  	  	  	  	  	  	  // Making up the filename for the index file
	  RM_ScanIterator rmsi;
	  vector<string> Attributes;
	  PF_FileHandle handle;

	  if(!pf_IX->CreateFile(makeName))
	  {
		  if(!ScanCatalog("CAT_COLUMNS","TBLNAME",EQ_OP,(const void*)tableName.c_str(),Attributes,rmsi))
		  {
			  	 if(!pf_IX->OpenFile(makeName,handle))
			  	 {
			  		 	void* buffer=calloc(PF_PAGE_SIZE,sizeof(char));
			  		 	unsigned i=0;
			  		 	for(unsigned i=0;i<attrs.size();i++)
			  		 	{
			  		 		if(!strcmp(attributes[i].c_str(),attributeName.c_str()))
			  		 		{
			  		 			*(AttrType*)buffer=(AttrType)attrs[i].type;
			  		 			*(int*)((char*)buffer + sizeof(AttrType)) = 0;
			  		 			*((char*)buffer+ sizeof(AttrType) + sizeof(int))='L';
			  		 			*(short int*)((char*)buffer + sizeof(AttrType) + sizeof(int) + sizeof(char)) = sizeof(AttrType) + sizeof(int) + sizeof(char) + sizeof(short int) + 2*sizeof(int);
			  		 			*(int*)((char*)buffer + sizeof(AttrType) + sizeof(int) + sizeof(char) + sizeof(short int)) = -1;
			  		 			*(int*)((char*)buffer + sizeof(AttrType) + sizeof(int) + sizeof(char) + sizeof(short int) + sizeof(int)) = -1;
			  		 		}
			  		 	}
			  		 	if(i==attrs.size())
			  		 		return 3;
			  		 	handle.WritePage(0,buffer);
			  		 	pf_IX->CloseFile(handle);
			  		 	free(buffer);
			  	 }
			  	 else
			  	 {
			  		free(makeName);
			  		return 2;
			  	 }
		  }
		  else
		  {
			 remove(makeName);
			 free(makeName);
			 return 8;
		  }
	  }
	  else
		  return 1;
	  free(makeName);
	  return 0;
  }


  RC IX_Manager::DestroyIndex(const string tableName, const string attributeName){      						// destroy an index
	  char* fileName=(char*)malloc(tableName.length() + attributeName.length() + 5);
	  memcpy(fileName,"IX_",3);
	  memcpy(fileName + 3,tableName.c_str(),tableName.length());
	  memcpy(fileName + 3 + tableName.length(),"_",1);
	  memcpy(fileName + 4 + tableName.length(),attributeName.c_str(),attributeName.length());
	  *(fileName + 4 + tableName.length() + attributeName.length()) = '\0';

	  if(!pf_IX->DestroyFile(fileName))
	  {
		  free(fileName);
		  return 0;
	  }
	  else
	  {
		  free(fileName);
		  return 3;
	  }
  }



  RC IX_Manager::OpenIndex(const string tableName, const string attributeName, IX_IndexHandle &indexHandle){	// open an index
	  char* fileName=(char*)malloc(tableName.length() + attributeName.length() + 5);
	  memcpy(fileName,"IX_",3);
	  memcpy(fileName + 3,tableName.c_str(),tableName.length());
	  memcpy(fileName + 3 + tableName.length(),"_",1);
	  memcpy(fileName + 4 + tableName.length(),attributeName.c_str(),attributeName.length());
	  *(fileName + 4 + tableName.length() + attributeName.length())='\0';

	  if(!pf_IX->OpenFile(fileName,indexHandle.ixHandle))
	  {
		  free(fileName);
		  return 0;
	  }
	  else
	  {
		  free(fileName);
		  return 2;
	  }
  }


  RC IX_Manager::CloseIndex(IX_IndexHandle &indexHandle){  														// close index
	  if(!pf_IX->CloseFile(indexHandle.ixHandle))
		  return 0;
	  else
		  return 4;
  }

  // IX_IndexHandle Functions

  RC IX_Manager::ScanCatalog(const string tableName,const string conditionAttribute,const CompOp compOp,const void *value,const vector<string> &attributeNames, RM_ScanIterator &rm_ScanIterator)
  {
		RID firstRID;
		firstRID.pageNum=0;						//Populating the Scan_Iterator
		firstRID.slotNum=0;
		PF_FileHandle catHandle;


		if(!strcmp("CAT_COLUMNS",tableName.c_str())){
			pf_IX->OpenFile("CAT_COLUMNS",catHandle);
			void * catalog=malloc(PF_PAGE_SIZE);
			short int offset1, offset2, offset3, offset5;
			unsigned p,s,nSlots,pgNum;
			pgNum=catHandle.GetNumberOfPages();

			for(p=0;p<pgNum;p++){																		//Traverse through all pages
				catHandle.ReadPage(p,catalog);
				nSlots=*(int *)((char *)catalog+PF_PAGE_SIZE-10);
					for(s=0;s<nSlots;s++){																	//Traverse through slots
						void *recPtr= (char *)catalog+ *(int *)((char *)catalog+PF_PAGE_SIZE-10-8*(s+1));
						offset1=*(short int *)recPtr;
						offset2=*((short int *)recPtr+1);

						void * tblNm=malloc(offset2-offset1+1);
						memmove(tblNm, (char *)recPtr+offset1, offset2-offset1);
						*((char *)tblNm+(offset2-offset1))='\0';

						if(!strcmp((char *)tblNm, (char *)value)){										//Compare if we are accessing the right table record
							offset1=offset2;
							offset2=*((short int *)recPtr+2);
							offset3=*((short int *)recPtr+3);
							offset5=*((short int *)recPtr+5);
							void *colNm=calloc(offset2-offset1+1,sizeof(char));
							memmove(colNm, (char *)recPtr+offset1, offset2-offset1);
							*((char *)colNm+(offset2-offset1))='\0';

							attributes.push_back((char*)colNm);
							cat catvctr;
							catvctr.position=*(int *)((char *)recPtr+offset2);
							catvctr.type=(AttrType)*(int *)((char *)recPtr+offset3);
							catvctr.length=(AttrLength)*(int *)((char *)recPtr+offset5);
							catvctr.crid.pageNum=p;
							catvctr.crid.slotNum=s;
							attrs.push_back(catvctr);

				}
				free(tblNm);

				}
			}
			free(catalog);
			pf_IX->CloseFile(catHandle);
		}
		return 0;
  }

  RC IX_IndexHandle::InsertEntry(void *key, const RID &rid){													// Insert new index entry

  if(ixHandle.file.is_open())
  {
		  //Determine the type of attribute
	  	  //Finding the root page
	  	  RC rc;
		  if(currentPageNo == -1 && ixHandle.GetNumberOfPages()>0)																			//Find the root page numher initially
		  {
			  void *bufferPage = calloc(PF_PAGE_SIZE,sizeof(char));
			  ixHandle.ReadPage(0,bufferPage);
			  indexKeyType = *((AttrType*)bufferPage);
			  currentPageNo = *((int*)bufferPage+1);
			  free(bufferPage);
			  rc = InsertEntry(key,rid);
			  if(rc == 0)
			  {
				  currentPageNo = -1; 															//Terminating adjustment so that next insert starts from Root
				  return 0;
			  }
			  else
				  return rc;
		  }
		  else if(currentPageNo > -1)
		  {
			  void *bufferPage=calloc(PF_PAGE_SIZE,sizeof(char));
			  ixHandle.ReadPage(currentPageNo,bufferPage);
			  int PageZeroOffset = 0;
			  if(currentPageNo == 0)
			  	  PageZeroOffset = 2*sizeof(int);
			  char PageType=*((char*)bufferPage + PageZeroOffset);
			  short int freeSpacePointer;

			  if(PageType == 'R' || PageType == 'N')
			  {


				  freeSpacePointer = *(short int*)((char*)bufferPage + PageZeroOffset + sizeof(char));
				  short int traversePointer = PageZeroOffset + sizeof(char) + sizeof(short int) + sizeof(int);
				  bool leftPg=false; //only for tests


				  int prevPageNo=currentPageNo;

				  //freeSpace=(short int)(PF_PAGE_SIZE - freeSpacePointer);
				  if(indexKeyType == TypeInt)
				  {
					  int currentKeyValue;
					  //Searching the entry
					  while((unsigned)traversePointer + 2*sizeof(int) <= (unsigned)freeSpacePointer)
					  {
						  currentKeyValue = *(int*)((char*)bufferPage+traversePointer);

						  if(currentKeyValue <= *(int*)key)
						  {

							  if(*(int*)key < *(int*)((char*)bufferPage + traversePointer + 2*sizeof(int)) || (traversePointer + 2*sizeof(int)) >= freeSpacePointer)
							  //Check whether the record is greater than the current one and less than next one OR there is no record on the right
							  {
								  currentPageNo = *(int*)((char*)bufferPage + traversePointer + sizeof(int)); //Update new page no.


								  rc = InsertEntry(key,rid);
								  if(rc != 0)
								  {
									  free(bufferPage); //new
									  return rc;
								  }
								  break;
							  }
						  }
						  else if(currentKeyValue > *(int*)key) 											//To check the initial key value if left page pointer is traversed..
						  {
							  leftPg = true;
							  currentPageNo = *(int*)((char*)bufferPage + traversePointer - sizeof(int));
							  rc = InsertEntry(key,rid);
							  if(rc != 0)
							  {
								  free(bufferPage); //new
								  return rc;
							  }

							  break;
						  }
						  traversePointer+=2*sizeof(int);
					  }
					  //Inserting the entry in the non leaf
					  if(splitCheck)
					  {
						  if(!leftPg)
						  {
							  traversePointer += 2*sizeof(int);
						  }
						  leftPg=false;
						  currentPageNo = prevPageNo;

						  splitCheck=false;

						  if((freeSpacePointer + 2*sizeof(int)) < PF_PAGE_SIZE)
						  {
							  //needs modification for left data!!

							  memmove((char*)bufferPage + traversePointer + 2*sizeof(int), (char*)bufferPage + traversePointer, freeSpacePointer - traversePointer);

							  *(int *)((char*)bufferPage + traversePointer)=value.IntSplitValue;
							  *(int *)((char*)bufferPage + traversePointer + sizeof(int))=newPageNo;
							  *(short int*)((char*)bufferPage + sizeof(char) + PageZeroOffset)= freeSpacePointer + 2*sizeof(int);
							  ixHandle.WritePage(prevPageNo, bufferPage);
						  }
						  else
						  {
							  if(!Split(bufferPage, traversePointer, currentPageNo, (void *)value.IntSplitValue, rid))
							  {
								  if(rootSplit)
									  splitCheck=true;
								  else
									  rootSplit=true;
								  free(bufferPage); //new
								  return 0;
							  }
							  else
							  {
								  free(bufferPage); //new
								  return -1;
							  }
						  }

					  }
					  free(bufferPage); // new
					  return 0;
				  }
				  else if (indexKeyType == TypeReal)
				  {
					  float currentKeyValue;
					  while((unsigned)traversePointer + 2*sizeof(int) <= (unsigned)freeSpacePointer)
					  {
						  currentKeyValue = *(float*)((char*)bufferPage+traversePointer);
						  if(currentKeyValue <= *(float*)key)
						  {
							  if(*(float*)key < *(float*)((char*)bufferPage + traversePointer + 2*sizeof(float)) || (traversePointer + 2*sizeof(float)) >= freeSpacePointer)
							  //Check whether the record is greater than the current one and less than next one OR there is no record on the right
							  {
								  currentPageNo = *(int*)((char*)bufferPage + traversePointer + sizeof(float));
								  rc = InsertEntry(key,rid);
								  if(rc != 0)
								  {
									  free(bufferPage); //new
									  return rc;
								  }
								  break;
							  }
						  }
						  else if(currentKeyValue > *(float*)key)
						  {
							  leftPg = true;
							  currentPageNo = *(int*)((char*)bufferPage + traversePointer - sizeof(int));
							  rc = InsertEntry(key,rid);
							  if(rc != 0)
							  {
								  free(bufferPage); //new
								  return rc;
							  }
							  break;
						  }
						  traversePointer+=2*sizeof(int);
					  }
					  if(splitCheck)
					  {
						  if(!leftPg)
						  {
							  traversePointer += sizeof(int) + sizeof(float);
						  }
						  currentPageNo = prevPageNo;
						  splitCheck=false;
						  leftPg=false;
						  if((freeSpacePointer + 2*sizeof(int)) < PF_PAGE_SIZE)
						  {
							  memmove((char*)bufferPage + traversePointer + 2*sizeof(int), (char*)bufferPage + traversePointer, freeSpacePointer - traversePointer);
							  *(float *)((char*)bufferPage + traversePointer)=value.FloatSplitValue;
							  *(int *)((char*)bufferPage + traversePointer + sizeof(int))=newPageNo;
							  *(short int*)((char*)bufferPage + sizeof(char) + PageZeroOffset)= freeSpacePointer + 2*sizeof(int);
							  ixHandle.WritePage(prevPageNo, bufferPage);
						  }
						  else
						  {
							  void* keyReal=malloc(sizeof(float));
							  *(float*)keyReal = value.FloatSplitValue;
							  if(!Split(bufferPage, traversePointer, currentPageNo, keyReal, rid))
							  {
								  if(rootSplit)
									  splitCheck=true;
								  else
									  rootSplit=true;
								  free(bufferPage);	  //new
								  return 0;
							  }
							  else
							  {
								  free(bufferPage); //new
								  return -1;
							  }
						  }
					  }
					  free(bufferPage);//new
					  return 0;

				  }
				  else if(indexKeyType == TypeVarChar)
				  {
					  int currentKeyLength=0;
					  char* strKey = (char*)malloc(*(int *)key + 1);
					  memcpy(strKey, (const void*)((int*)key+1), *(int *)key);
					  *((char *)strKey + *(int *)key) = '\0';

					  if(traversePointer < freeSpacePointer)
						  currentKeyLength = *(int*)((char*)bufferPage+traversePointer);

					  while(traversePointer + (int)sizeof(int) + currentKeyLength + (int)sizeof(int) <= freeSpacePointer)
					  {
						  char* str2 = (char*)malloc(currentKeyLength + 1);
						  memcpy(str2, (const void*)((char*)bufferPage + traversePointer + sizeof(int)), currentKeyLength);
						  *((char *)str2 + currentKeyLength) = '\0';
//						  if(memcmp((const void*)((int*)key+1),(const void*)((char*)bufferPage + traversePointer + sizeof(int)),max(currentKeyLength,*(int*)key)) >= 0)

						  if(strcasecmp(strKey, str2) >= 0)
						  {
							  int NextKeyValueOffset= traversePointer + sizeof(int) + currentKeyLength + sizeof(int) + sizeof(int);
							  str2 = (char*)realloc(str2, *(int*)((char*)bufferPage + NextKeyValueOffset - sizeof(int)) + 1);
							  *((char *)str2 + *(int*)((char*)bufferPage + NextKeyValueOffset - sizeof(int))) = '\0';

//							  if(memcmp((const void*)((int*)key+1),(const void*)((char*)bufferPage + NextKeyValueOffset),max(*(int*)((char*)bufferPage + NextKeyValueOffset - sizeof(int)),*(int*)key)) < 0 || (traversePointer + sizeof(int) + currentKeyLength + sizeof(int)) >= freeSpacePointer)
							  //Check whether the record is greater than the current one and less than next one OR there is no record on the right

							  if(strcasecmp(strKey, str2) < 0 || (NextKeyValueOffset - sizeof(int)) >= freeSpacePointer)
							  {
								  currentPageNo = *(int*)((char*)bufferPage + traversePointer + sizeof(int) + currentKeyLength);
								  rc = InsertEntry(key,rid);
								  if(rc != 0)
								  {
									  free(bufferPage); //new
									  return rc;
								  }
								  break;
							  }
						  }
//						  else if(memcmp((const void*)((int*)key+1),(const void*)((char*)bufferPage + traversePointer + sizeof(int)),max(currentKeyLength,*(int*)key)) < 0)

						  else if(strcasecmp(strKey, str2) < 0)
						  {
							  leftPg = true;
							  currentPageNo = *(int*)((char*)bufferPage + traversePointer - sizeof(int));
							  rc = InsertEntry(key,rid);
							  if(rc != 0)
							  {
								  free(bufferPage); //new
								  return rc;
							  }
							  break;
						  }
						  traversePointer += (2*sizeof(int) + currentKeyLength);
						  if((unsigned)traversePointer + sizeof(int) < (unsigned)freeSpacePointer)
							  currentKeyLength = *(int*)((char*)bufferPage+traversePointer);
					  }
					  if(splitCheck)
					  {
						  if(!leftPg)
						  {
							  traversePointer += 2*sizeof(int) + currentKeyLength;
						  }
						  currentPageNo = prevPageNo;
						  splitCheck=false;
						  leftPg=false;
						  if((freeSpacePointer + sizeof(int) + value.v.length + sizeof(int)) < PF_PAGE_SIZE)
						  {
							  memmove((char*)bufferPage + traversePointer + 2*sizeof(int) + value.v.length, (char*)bufferPage + traversePointer, freeSpacePointer - traversePointer);
							  *(int *)((char*)bufferPage + traversePointer) = value.v.length;
							  memcpy((char*)bufferPage + traversePointer + sizeof(int), value.v.data, value.v.length);
							  *(int *)((char*)bufferPage + traversePointer + sizeof(int) + value.v.length)=newPageNo;
							  *(short int*)((char*)bufferPage + sizeof(char) + PageZeroOffset)= freeSpacePointer + 2*sizeof(int) + value.v.length;
							  ixHandle.WritePage(prevPageNo, bufferPage);
						  }
						  else
						  {
							  if(!Split(bufferPage, traversePointer, currentPageNo, value.v.data, rid))
							  {
								  if(rootSplit)
									  splitCheck=true;
								  else
									  rootSplit=true;
								  free(bufferPage); //new
								  return 0;
							  }
							  else
							  {
								  free(bufferPage); //new
								  return -1;
							  }
						  }
					  }
					  free(bufferPage); //new
					  return 0;
				  }
			  }
			  else if(PageType=='L')
			  {
				  freeSpacePointer=*(short int*)((char*)bufferPage + PageZeroOffset + sizeof(char));
				  short int traversePointer = PageZeroOffset + sizeof(char) + sizeof(short int) + 2*sizeof(int);
				  unsigned checkPtr = traversePointer; //To test for duplicate key values on the leaf page!

				  if(indexKeyType == TypeInt)
				  {
					  while (checkPtr < (unsigned)freeSpacePointer)  //This loop runs through the existing data to check for duplicate key
					  {
						if(*(int*)((char*)bufferPage + checkPtr) == *(int*)key)
						{
							cout<<"Duplicate Key Not Accepted!\n";
							return 10;
						}
						checkPtr += 3*sizeof(int);
					  }

					  if((freeSpacePointer + 3*sizeof(int)) < PF_PAGE_SIZE)   //(PageZeroOffset + 131))
					  {

						  *(int *)((char *)bufferPage + freeSpacePointer) = *(int*)key;
						  *(int *)((char *)bufferPage + freeSpacePointer + sizeof(int)) = rid.pageNum;
						  *(int *)((char *)bufferPage + freeSpacePointer + 2*sizeof(int)) = rid.slotNum;
						  *(short int *)((char *)bufferPage + PageZeroOffset + sizeof(char)) = freeSpacePointer + 3*sizeof(int);
						  ixHandle.WritePage(currentPageNo,bufferPage);
						  free(bufferPage); //new
						  return 0;
					  }
					  else
					  {//Trigger a split
						  if(!Split(bufferPage,traversePointer, currentPageNo, key, rid))
						  {
							  if(rootSplit)
								  splitCheck=true;
							  else
								  rootSplit=true;
							  free(bufferPage);  //new
							  return 0;
						  }
						  else
						  {
							  free(bufferPage); //new
							  return -1;
						  }
					  }
				  }
				  else if(indexKeyType == TypeReal)
				  {
					  while (checkPtr < (unsigned)freeSpacePointer)  //This loop runs through the existing data to check for duplicate key
					  {
						if(*(float*)((char*)bufferPage + checkPtr) == *(float*)key)
						{
							cout<<"Duplicate Key Not Accepted!\n";
							return 10;
						}
						checkPtr += sizeof(float) + 2*sizeof(int);
					  }

					  if((freeSpacePointer + 3*sizeof(int)) < PF_PAGE_SIZE)
					  {
						  *(float *)((char *)bufferPage + freeSpacePointer) = *(float*)key;
						  *(int *)((char *)bufferPage + freeSpacePointer + sizeof(int)) = (int)rid.pageNum;
						  *(int *)((char *)bufferPage + freeSpacePointer + 2*sizeof(int)) = (int)rid.slotNum;
						  *(short int *)((char *)bufferPage + PageZeroOffset + sizeof(char)) = freeSpacePointer + 3*sizeof(int);
						  ixHandle.WritePage(currentPageNo,bufferPage);
						  free(bufferPage); //new
						  return 0;
					  }
					  else
					  {
						  if(!Split(bufferPage,traversePointer, currentPageNo,key, rid))
						  {
							  if(rootSplit)
								  splitCheck=true;
							  else
								  rootSplit=true;
							  free(bufferPage); //new
							  return 0;
							  }
						  else
						  {
							  free(bufferPage); //new
							  return -1;
						  }
					  }
				  }
				  else if(indexKeyType == TypeVarChar)
				  {

					  while (checkPtr < (unsigned)freeSpacePointer)  //This loop runs through the existing data to check for duplicate key
					  {
						  unsigned valLength=*(int*)((char*)bufferPage + checkPtr);					//Storing the value as a string to compare
						  char *strVal=(char*)malloc(valLength + 1);
						  memcpy(strVal, ((char*)bufferPage + checkPtr + sizeof(int)), valLength);
						  *((char*)strVal + valLength) = '\0';

						  unsigned keyLength=*(int*)key;

						  char *strKey=(char*)malloc(keyLength + 1);		//Storing the current key as a string to compare
						  memcpy(strKey, (char*)key + sizeof(int), keyLength);
						  *((char*)strKey + keyLength) = '\0';

						if(strcasecmp(strVal, strKey) == 0)
						{
							cout<<"Duplicate Key Not Accepted!\n";
							return 10;
						}
						checkPtr += sizeof(int) + valLength + 2*sizeof(int);
					  }

					  if((freeSpacePointer + 3*sizeof(int) + *(int*)key) < PF_PAGE_SIZE)
					  {
						  memcpy((void*)((char *)bufferPage+freeSpacePointer),key,*(int*)key + sizeof(int));
						  *(int *)((char *)bufferPage + freeSpacePointer + sizeof(int) + *(int*)key) = (int)rid.pageNum;
						  *(int *)((char *)bufferPage + freeSpacePointer + 2*sizeof(int) + *(int*)key) = (int)rid.slotNum;
						  *(short int *)((char *)bufferPage + PageZeroOffset + sizeof(char)) = freeSpacePointer + 3*sizeof(int) + *(int*)key;
						  ixHandle.WritePage(currentPageNo,bufferPage);
						  free(bufferPage); //new
						  return 0;
					  }
					  else
					  {
						  if(!Split(bufferPage,traversePointer, currentPageNo, key, rid))
						  {
							  if(rootSplit)
								  splitCheck=true;
							  else
								  rootSplit=true;
							  free(bufferPage); //new
							  return 0;
						  }
						  else
						  {
							  free(bufferPage);
							  return -1;
						  }

					  }
				  }
			}
	  }
  }

  return -1;

}

//Split

  RC IX_IndexHandle :: Split(void *bufferPage, short int traversePointer,int currentPageNo, void * key, RID rid)
  {
  	  void* newPage=calloc(PF_PAGE_SIZE,sizeof(char));
  	  short int newLength = 0;
  	  int PageZeroOffset = 0;

  	  if(currentPageNo==0)
  		  PageZeroOffset = 2*sizeof(int);

  	  char pgType=*((char *)bufferPage + PageZeroOffset);
  	  short int freeSpacePointer = *(short int*)((char*) bufferPage + sizeof(char) + PageZeroOffset);

  	  if(indexKeyType == TypeVarChar)
  		  newLength = *(int*)key;

  	  if(pgType == 'L')
  	  {
		  void *sortBuffer = calloc(PF_PAGE_SIZE + 3*sizeof(int) + newLength,sizeof(char));
		  memcpy(sortBuffer,bufferPage,freeSpacePointer);													// Copying the old data
		  memcpy((char*)sortBuffer + freeSpacePointer,key, newLength + sizeof(int));						// Adding the key value
		  *(int*)((char*)sortBuffer + freeSpacePointer + newLength + sizeof(int)) = rid.pageNum;			//Adding the RID
		  *(int*)((char*)sortBuffer + freeSpacePointer + newLength + 2*sizeof(int)) = rid.slotNum;			//Adding the RID

		  freeSpacePointer += newLength + 3*sizeof(int);
		  *(short int*)((char*)sortBuffer+ PageZeroOffset + sizeof(char)) = freeSpacePointer;

		  BubbleSortLeaf(sortBuffer,currentPageNo,indexKeyType);
		  bufferPage=sortBuffer;

		  unsigned middle = (freeSpacePointer-traversePointer)/2;// + newLength;
		  if(((freeSpacePointer-traversePointer)/(3*sizeof(int))) % 2 != 0 && (indexKeyType != TypeVarChar))
		  {
			  middle = (freeSpacePointer-traversePointer - (3*sizeof(int)))/2;
		  }
		  else if(indexKeyType == TypeVarChar)
		  {

			  int Offset = traversePointer;
			  int length=*(int* )((char*)bufferPage + Offset);
			  do
			  {

				  Offset += length + 3*sizeof(int);
				  length = *(int* )((char*)bufferPage + Offset);


			  }while((Offset + length + 3*sizeof(int)) < middle);
			  middle=Offset-traversePointer;

		  }

		  //Writing the parameters and the data for the new page
		  *(char*)newPage='L';
		  *(short int*)((char*)newPage + sizeof(char)) = freeSpacePointer - (middle + traversePointer) + traversePointer - PageZeroOffset;			// As the page splitting might be page 0 // -1 as the data is 1 byte before freeSpaceOffset ...
		  *(int*)((char*)newPage + sizeof(char) + sizeof(short int)) = currentPageNo;																//Setting the left pointer of the new page
		  *(int*)((char*)newPage + sizeof(char) + sizeof(short int) + sizeof(int)) = *(int*)((char*)bufferPage + PageZeroOffset + sizeof(char) + sizeof(short int) + sizeof(int)); //Setting the right pointer of the new page
		  //Modified!!
		  memmove((char*)newPage + traversePointer - PageZeroOffset, (char*)bufferPage + traversePointer + middle, freeSpacePointer - (middle + traversePointer) );	//Copying the second half of sorted data into new page
		  ixHandle.AppendPage(newPage);

		  newPageNo=ixHandle.GetNumberOfPages()-1;

		  //Updating the chaining in the right leaf..   ***NEW***
		  unsigned rightLeafNo=*(int*)((char*)bufferPage + PageZeroOffset + sizeof(char) + sizeof(short int) + sizeof(int));

		  if(rightLeafNo != (unsigned)-1)
		  {
			  void *rightPage = calloc(PF_PAGE_SIZE, sizeof(char));
			  ixHandle.ReadPage(rightLeafNo, rightPage);
			  *(int*)((char*)rightPage + sizeof(char) + sizeof(short int)) = newPageNo;	    //Left Pointer of the right leaf page
			  ixHandle.WritePage(rightLeafNo,rightPage);
			  free(rightPage);
		  }

		  //Modifying parameters of the old page
		  *(int*)((char*)bufferPage + PageZeroOffset + sizeof(char) + sizeof(short int) + sizeof(int)) = newPageNo;	    //Right Pointer of the old page
		  *(short int*)((char*)bufferPage + PageZeroOffset + sizeof(char)) =  traversePointer + middle;					//Setting the free space pointer
		  ixHandle.WritePage(currentPageNo,bufferPage);



		  //Populating the split parameters
		  if(indexKeyType == TypeInt)
			  value.IntSplitValue=*(int*)((char*)newPage + traversePointer - PageZeroOffset);
		  else if(indexKeyType == TypeReal)
			  value.FloatSplitValue=*(float*)((char*)newPage + traversePointer - PageZeroOffset);
		  else if(indexKeyType == TypeVarChar)
		  {
			value.v.length=*(int*)((char*)newPage + traversePointer - PageZeroOffset);
			value.v.data=calloc(value.v.length, sizeof(char));
			memcpy(value.v.data,(char*)newPage + traversePointer - PageZeroOffset + sizeof(int),value.v.length);
		  }

		  splitCheck=true;  //To indicate insert that the leaf was split!

		  free(newPage); //new
		  if(currentPageNo!=0 || (*(int*)((char*)bufferPage + sizeof(AttrType))!=0))
		  {
			  free(sortBuffer); //new.. freed later as it is used in if().. bufferPage=sortBuffer!
			  return 0;
		  }
  	  }

  	  else if(pgType == 'N' || pgType == 'R')
  	  {
  		  int KeyLength=sizeof(int);
  	  	  if(indexKeyType == TypeVarChar)
  	  		KeyLength += value.v.length;

  	  	  void *sortBuffer = calloc(PF_PAGE_SIZE + KeyLength + sizeof(int),sizeof(char));
		  memcpy(sortBuffer,bufferPage,freeSpacePointer);													// Copying the old data
		  bufferPage=sortBuffer;


		  if(indexKeyType == TypeInt)
		  {
			  memmove((char*)sortBuffer + traversePointer + 2*sizeof(int), (char*)bufferPage + traversePointer, freeSpacePointer - traversePointer);
			  *(int*)((char*)sortBuffer + traversePointer) = value.IntSplitValue;
			  *(int*)((char*)sortBuffer + traversePointer + sizeof(int)) = newPageNo;
			  *(short int *)((char*)sortBuffer + PageZeroOffset + sizeof(char)) = freeSpacePointer + 2*sizeof(int);
		  }
		  else if(indexKeyType == TypeReal)
		  {
			  memmove((char*)sortBuffer + traversePointer + 2*sizeof(int), (char*)bufferPage + traversePointer, freeSpacePointer - traversePointer);
			  *(float*)((char*)sortBuffer + traversePointer) = value.FloatSplitValue;
			  *(int*)((char*)sortBuffer + traversePointer + sizeof(int)) = newPageNo;
			  *(short int *)((char*)sortBuffer + PageZeroOffset + sizeof(char)) = freeSpacePointer + sizeof(float) + sizeof(int);

		  }
		  else if(indexKeyType == TypeVarChar)
		  {
			  memmove((char*)sortBuffer + traversePointer + 2*sizeof(int) + value.v.length, (char*)bufferPage + traversePointer, freeSpacePointer - traversePointer);
			  *(int*)((char*)sortBuffer + traversePointer) = value.v.length;
			  memcpy((char*)sortBuffer + traversePointer + sizeof(int), (void*)value.v.data, value.v.length);
			  *(int*)((char*)sortBuffer + traversePointer + sizeof(int) + value.v.length) = newPageNo;
			  *(short int *)((char*)sortBuffer + PageZeroOffset + sizeof(char)) = freeSpacePointer + 2*sizeof(int) + value.v.length;
		  }



		  traversePointer = PageZeroOffset + sizeof(char) + sizeof(short int) + sizeof(int);  //The previous traversePointer was the position of adding new key, sent by the splitting non-leaf
		  freeSpacePointer=*(short int*)((char*)sortBuffer + sizeof(char) + PageZeroOffset);

		  int splitOffset=(freeSpacePointer-traversePointer)/2;
		  if(((freeSpacePointer-traversePointer)/(2*sizeof(int))) % 2 != 0 && (indexKeyType != TypeVarChar))
			  splitOffset = (freeSpacePointer-traversePointer - (2*sizeof(int)))/2;
		  else if(indexKeyType == TypeVarChar)
		  {
			  int Offset = traversePointer;
			  int length=*(int* )((char*)bufferPage + Offset);
			  do
			  {
				  Offset += length + 2*sizeof(int);
				  length = *(int* )((char*)bufferPage + Offset);

			  }while((Offset + length + 2*sizeof(int)) < splitOffset);
			  splitOffset=Offset-traversePointer;
		  }


		  int splitDataLength = KeyLength;
		  if(indexKeyType == TypeVarChar)
			  splitDataLength = *(int*)((char*)sortBuffer + + splitOffset  + traversePointer) + sizeof(int);	//Size of the key pushed up to the non leaf

		  *(char *)newPage ='N';
		  *(short int*)((char*)newPage + sizeof(char)) = freeSpacePointer - (splitOffset+traversePointer) + traversePointer - PageZeroOffset - splitDataLength - sizeof(int); // freespace ptr

		  //the right ptr of splitting key becoe left ptr of new node..
		  memcpy((char*)newPage + traversePointer - PageZeroOffset - sizeof(int), (char*)sortBuffer + splitOffset  + traversePointer + splitDataLength, freeSpacePointer - (splitOffset+traversePointer) - KeyLength);
		  ixHandle.AppendPage(newPage);

		  *(char *)sortBuffer = 'N'; //Set R to N incase Root is splitting
		  *(short int*)((char*)sortBuffer + sizeof(char)) = splitOffset + traversePointer; //freespace ptr
		  ixHandle.WritePage(currentPageNo, sortBuffer);

		  newPageNo=ixHandle.GetNumberOfPages()-1;
		  if(indexKeyType == TypeInt)
			  value.IntSplitValue=*(int *)((char *)sortBuffer + splitOffset + traversePointer);
		  else if(indexKeyType == TypeReal)
			  value.FloatSplitValue=*(float *)((char *)sortBuffer + splitOffset + traversePointer);
		  else if(indexKeyType == TypeVarChar)
		  {
			  value.v.length = *(int *)((char *)sortBuffer + splitOffset + traversePointer);
			  memcpy(value.v.data, (char *)sortBuffer + splitOffset + traversePointer + sizeof(int), value.v.length);
		  }

		  //splitCheck=true;  //To indicate insert that the non-leaf was split!

		  free(sortBuffer);//new
		  free(newPage);//new
		  if(pgType!='R')
			  return 0;

  	  }

	  if(currentPageNo == 0 || pgType == 'R')
	  {
		  //Create a new root for the tree
		  void *newRoot=calloc(PF_PAGE_SIZE,sizeof(char));
		  int length=0;
		  *(char*)newRoot='R';																							//Setting the root literal
		  *(int *)((char*)newRoot + sizeof(char) + sizeof(short int)) = currentPageNo;									//Setting the left pointer

		  //Copy the data
		  if(indexKeyType == TypeInt)
			  *(int *)((char*)newRoot + sizeof(char) + sizeof(short int) + sizeof(int)) = value.IntSplitValue;
		  else if(indexKeyType == TypeReal)
			  *(float *)((char*)newRoot + sizeof(char) + sizeof(short int) + sizeof(int)) = value.FloatSplitValue;
		  else if(indexKeyType == TypeVarChar)
		  {
			  *(int *)((char*)newRoot + sizeof(char) + sizeof(short int) + sizeof(int)) = value.v.length;
			  length=value.v.length;
			  memcpy((char*)newRoot + sizeof(char) + sizeof(short int) + 2*sizeof(int), value.v.data, value.v.length);
		  }

		  *(int *)((char*)newRoot + sizeof(char) + sizeof(short int) + 2*sizeof(int) + length) = newPageNo;				//Setting the right pointer
		  *(short int*)((char*)newRoot + sizeof(char)) = sizeof(char) + sizeof(short int) + 3*sizeof(int) + length;		//Setting the free Space pointer


		  ixHandle.AppendPage(newRoot);
		  int rootPageNo=ixHandle.GetNumberOfPages()-1;
		  void *pageZero = malloc(PF_PAGE_SIZE);
		  ixHandle.ReadPage(0,pageZero);
		  *((int*)pageZero+1) = rootPageNo;
		  ixHandle.WritePage(0,pageZero);
		  rootSplit=false;
		  splitCheck=false;

		  free(pageZero);
		  free(newRoot);
		  return 0;
	  }
  	  else
  		  return -1;

  }

  //Sort
  void IX_IndexHandle :: BubbleSortLeaf(void *buffer1, int pg, AttrType type)
  {
	  // offset has to change accordingly

	void * next;
  	int incr=0, length1=0, length2=0, tuplesize=0, offset=11;
  	if(pg==0)
  		offset=19;
  	int freespace=*(short int *)((char *)buffer1+offset - (2*sizeof(int) + sizeof(short int)));
  	int recmem=freespace-offset;

  	while(recmem >= 0)
  	{

  		if(pg==0)
  			offset=19;
  		else
  			offset=11;

//  		cout<<"Free space ptr = "<<freespace<<"\tRecmem = "<<recmem<<"\tOffset"<<offset<<endl;
  		int rem = 0;

  		while(offset + tuplesize + rem < (freespace - 1))		//modified **NEW** -1
  		{
  			tuplesize=0;
  			length1=0; length2=0; incr=0;
  			next=(char*)buffer1+offset;
  			if(type==TypeInt)
  			{
 				if((*(int*)next)>*((int*)next+3))
  				{
 					void *temp=calloc(3*sizeof(int),sizeof(char));
  					memmove(temp,(void*)next,3*sizeof(int));											//Copy next into temp1
  					memmove(next,(void*)((char*)next + 3*sizeof(int)),3*sizeof(int));					//Copy 2nd record into next start
  					memmove((void*)((char*)next + 3*sizeof(int)),temp,3*sizeof(int));					//Copy temp into 2nd record
  	 				free(temp); //new
  				}
  				tuplesize=(3*sizeof(int));
  			}
  			else if(type==TypeReal)
  			{
  				if((*(float*)next)>*((float*)next+3))
  				{
  					void *temp=calloc(3*sizeof(float),sizeof(char));
  					memmove(temp,(void*)next,3*sizeof(float));											//Copy next into temp1
  					memmove(next,(void*)((char*)next + 3*sizeof(float)),3*sizeof(float));				//Copy 2nd record into next start
  					memmove((void*)((char*)next + 3*sizeof(float)),temp,3*sizeof(float));				//Copy temp into 2nd record
  	  				free(temp); //new
  				}
  				tuplesize=(3*sizeof(float));
  			}
  			else if(type==TypeVarChar)
  			{
  				length1=*(int*)((char*)next);
  				length2 = *(int*)((char*)next + 3*sizeof(int) + length1);
  				char *str1,*str2;
  				str1=(char*)calloc(length1+1,sizeof(char));
  				str2=(char*)calloc(length2+1,sizeof(char));
  				memcpy((char*)str1,(char*)next+ sizeof(int),length1);
  				*((char*)str1+length1)='\0';
  				memcpy((char*)str2,(char*)next + 4*sizeof(int) + length1,length2);
  				*((char*)str2+length2)='\0';
//  				cout<<"Offset= "<<offset<<"\tStr1= "<<str1<<"  Length1= "<<length1<<"\tStr2= "<<str2<<"  Length2= "<<length2<<endl;

  				if(strcasecmp(str1,str2) > 0)
  				{
  					void *temp = calloc(length1 + 3*sizeof(int),sizeof(char));
  					void *temp1 = calloc(length2 + 3*sizeof(int),sizeof(char));

  					memmove(temp,next,length1 + 3*sizeof(int));										//Copy next into temp1
  					memmove(temp1,((char*)next + 3*sizeof(int) + length1),length2+3*sizeof(int));	//Copy 2nd record into next start
  					memmove(next,temp1,length2+3*sizeof(int));
  					memmove(((char*)next + 3*sizeof(int) + length2),temp,length1 + 3*sizeof(int));	//Copy temp into 2nd record

  					tuplesize=(3*sizeof(int)+length2);
  					incr=length1;
  					rem = 3*sizeof(int) + length1;
  					free(temp);
  					free(temp1);
  				}
  				else
  				{
  					incr=length2;
  					tuplesize=(3*sizeof(int)+length1);
  					rem = 3*sizeof(int) + length2;
  				}
//  				rem= 3*sizeof(int) + *(int*)((char*)next + 6*sizeof(int) + length1 + length2);
  				free(str1);
  				free(str2);
  			}
  			offset+=tuplesize;
  		}
  		freespace -= (3*sizeof(int) + incr);
  		recmem-=(3*sizeof(int) + incr);
  	}

  }


  RC IX_IndexHandle::DeleteEntry(void *key, const RID &rid){													// Delete index entry

	  if(ixHandle.file.is_open())
	  {
			  //Determine the type of attribute
		  	  //Finding the root page
			  if(currentPageNo == -1 && ixHandle.GetNumberOfPages()>0)																			//Find the root page numher initially
			  {
				  void *bufferPage = calloc(PF_PAGE_SIZE,sizeof(char));
				  ixHandle.ReadPage(0,bufferPage);
				  indexKeyType = *((AttrType*)bufferPage);
				  currentPageNo = *((int*)bufferPage+1);
				  free(bufferPage);
				  if(!DeleteEntry(key,rid))
				  {
					  currentPageNo = -1; 															//Terminating adjustment so that next insert starts from Root
					  return 0;
				  }
			  }
			  else if(currentPageNo > -1)
			  {
				  void *bufferPage=calloc(PF_PAGE_SIZE,sizeof(char));
				  ixHandle.ReadPage(currentPageNo,bufferPage);
				  int PageZeroOffset = 0;
				  if(currentPageNo == 0)
				  	  PageZeroOffset = 2*sizeof(int);
				  char PageType=*((char*)bufferPage + PageZeroOffset);
				  short int freeSpacePointer;

				  if(PageType == 'R' || PageType == 'N')
				  {

					  freeSpacePointer = *(short int*)((char*)bufferPage + PageZeroOffset + sizeof(char));
					  short int traversePointer = PageZeroOffset + sizeof(char) + sizeof(short int) + sizeof(int);
					  bool leftPg=false; //only for tests


					  int prevPageNo=currentPageNo;

					  if(indexKeyType == TypeInt)
					  {
						  //counting the number of elements in the non leaf
						  int dummy = traversePointer;
						  int count = 0;
						  while(dummy < freeSpacePointer)
						  {
							  count++;
							  dummy += 2*sizeof(int);
						  }
						  if(count > 1)
							  NonLeaflastElement = false;
						  int currentKeyValue;
						  //Searching the entry
						  while((unsigned)traversePointer + 2*sizeof(int) <= (unsigned)freeSpacePointer)
						  {
							  currentKeyValue = *(int*)((char*)bufferPage+traversePointer);

							  if(currentKeyValue <= *(int*)key)
							  {

								  if(*(int*)key < *(int*)((char*)bufferPage + traversePointer + 2*sizeof(int)) || (traversePointer + 2*sizeof(int)) >= freeSpacePointer)
								  //Check whether the record is greater than the current one and less than next one OR there is no record on the right
								  {
									  currentPageNo = *(int*)((char*)bufferPage + traversePointer + sizeof(int)); //Update new page no.

									  DeleteEntry(key,rid);
									  break;
								  }
							  }
							  else if(currentKeyValue > *(int*)key) 											//To check the initial key value if left page pointer is traversed..
							  {
								  leftPg = true;
								  currentPageNo = *(int*)((char*)bufferPage + traversePointer - sizeof(int));
								  DeleteEntry(key,rid);
								  break;
							  }
							  traversePointer+=2*sizeof(int);
						  }
						  //Inserting the entry in the non leaf
						  if(deleteCheck)
						  {
							  if(leftPg)
								  traversePointer -= sizeof(int);

							  leftPg=false;

							  currentPageNo = prevPageNo;

							  deleteCheck=false;


							  //Since the element has been found and deleted in the leaf node, its time to delete the element from the non leaf node and
							  // move other key values and set the pointers

							  memmove((char*)bufferPage + traversePointer,(char*)bufferPage + traversePointer + 2*sizeof(int),freeSpacePointer - (traversePointer + 2*sizeof(int))); 	//move the data to delete the entry
							  *(short int*)((char*)bufferPage + sizeof(char) + PageZeroOffset)= freeSpacePointer - 2*sizeof(int);														//Set the free space pointer
							  ixHandle.WritePage(prevPageNo,bufferPage);
						  }
						  free(bufferPage);//new
						  return 0;
					  }
					  else if (indexKeyType == TypeReal)
					  {
						  //counting the number of elements in the non leaf
						  int dummy = traversePointer;
						  int count = 0;
						  while(dummy < freeSpacePointer)
						  {
							  count++;
							  dummy += 2*sizeof(float);
						  }
						  if(count > 1)
							  NonLeaflastElement = false;

						  float currentKeyValue;
						  while((unsigned)traversePointer + 2*sizeof(int) <= (unsigned)freeSpacePointer)
						  {
							  currentKeyValue = *(float*)((char*)bufferPage+traversePointer);
							  if(currentKeyValue <= *(float*)key)
							  {
								  if(*(float*)key < *(float*)((char*)bufferPage + traversePointer + 2*sizeof(float)) || (traversePointer + 2*sizeof(float)) >= freeSpacePointer)
								  //Check whether the record is greater than the current one and less than next one OR there is no record on the right
								  {
									  currentPageNo = *(int*)((char*)bufferPage + traversePointer + sizeof(float));
									  DeleteEntry(key,rid);
									  break;
								  }
							  }
							  else if(currentKeyValue > *(float*)key)
							  {
								  leftPg = true;
								  currentPageNo = *(int*)((char*)bufferPage + traversePointer - sizeof(float));
								  DeleteEntry(key,rid);
								  break;
							  }
							  traversePointer+=2*sizeof(int);
						  }
						  if(deleteCheck)
						  {
							  if(leftPg)
							  {
							   	  traversePointer -= sizeof(float);
							  }

							  leftPg=false;
							  deleteCheck=false;
							  currentPageNo=prevPageNo;
							  //Since the element has been found and deleted in the leaf node, its time to delete the element from the non leaf node and
							  // move other key values and set the pointers

							  memmove((char*)bufferPage + traversePointer,(char*)bufferPage + traversePointer + 2*sizeof(int),freeSpacePointer - (traversePointer + 2*sizeof(int))); 	//move the data to delete the entry
							  *(short int*)((char*)bufferPage + sizeof(char) + PageZeroOffset)= freeSpacePointer - 2*sizeof(float);														//Set the free space pointer
							  ixHandle.WritePage(prevPageNo,bufferPage);

						  }
						  free(bufferPage);//new
						  return 0;

					  }
				  else if(indexKeyType == TypeVarChar)
				  {
					  int dummy = traversePointer;
					  int dummyKeyLength = 0;
					  int count = 0;
					  while(dummy < freeSpacePointer)
					  {
						  count++;
						  dummyKeyLength = *(int*)((char*)bufferPage + dummy);
						  dummy += sizeof(int) + dummyKeyLength + sizeof(int);
					  }
					  if(count > 1)
						  NonLeaflastElement = false;

					  char* strKey = (char*)malloc(*(int *)key + 1);
					  memcpy(strKey, (const void*)((int*)key+1), *(int *)key);
					  *((char *)strKey + *(int *)key) = '\0';

					  int currentKeyLength = *(int*)((char*)bufferPage+traversePointer);

					  while((unsigned)traversePointer + sizeof(int) + (unsigned)currentKeyLength + sizeof(int) <= (unsigned)freeSpacePointer)
					  {
						  char* str2 = (char*)malloc(currentKeyLength + 1);
						  memcpy(str2, (const void*)((char*)bufferPage + traversePointer + sizeof(int)), currentKeyLength);
						  *((char *)str2 + currentKeyLength) = '\0';

						  if(strcasecmp(strKey, str2) >= 0)
						  {
							  int NextKeyValueOffset= traversePointer + sizeof(int) + currentKeyLength + sizeof(int) + sizeof(int);
							  str2 = (char*)realloc(str2, *(int*)((char*)bufferPage + NextKeyValueOffset - sizeof(int)) + 1);
							  *((char *)str2 + *(int*)((char*)bufferPage + NextKeyValueOffset - sizeof(int))) = '\0';

							  //Check whether the record is greater than the current one and less than next one OR there is no record on the right

							  if(strcasecmp(strKey, str2) < 0 || (NextKeyValueOffset - sizeof(int)) >= freeSpacePointer)
							  {
								  currentPageNo = *(int*)((char*)bufferPage + traversePointer + sizeof(int) + currentKeyLength);
								  DeleteEntry(key,rid);
								  break;
							  }
						  }

						  else if(strcasecmp(strKey, str2) < 0)
						  {
							  leftPg = true;
							  currentPageNo = *(int*)((char*)bufferPage + traversePointer - sizeof(int));
							  DeleteEntry(key,rid);
							  break;
						  }
						  traversePointer += (2*sizeof(int) + currentKeyLength);
						  if((unsigned)traversePointer + sizeof(int) < (unsigned)freeSpacePointer)
							  currentKeyLength = *(int*)((char*)bufferPage+traversePointer);
					  }
					  if(deleteCheck)
					  {
						  if(leftPg)
						  {
							  cout<<"DELETE : Left PAGE TAKEN!!";
							  traversePointer -= sizeof(int);
						  }
						  currentPageNo = prevPageNo;
						  deleteCheck=false;
						  //Since the element has been found and deleted in the leaf node, its time to delete the element from the non leaf node and
						  // move other key values and set the pointers

						  memmove((char*)bufferPage + traversePointer,(char*)bufferPage + traversePointer + sizeof(int) + currentKeyLength + sizeof(int),freeSpacePointer - (traversePointer + sizeof(int) + currentKeyLength + sizeof(int))); 	//move the data to delete the entry
						  *(short int*)((char*)bufferPage + sizeof(char) + PageZeroOffset)= freeSpacePointer - (sizeof(int) + currentKeyLength + sizeof(int));														//Set the free space pointer
						  ixHandle.WritePage(prevPageNo,bufferPage);
						  free(bufferPage);
						  return 0;
					  }
				  }

				  }
				  else if(PageType=='L')
				  {
					  freeSpacePointer=*(short int*)((char*)bufferPage + PageZeroOffset + sizeof(char));
					  short int traversePointer = PageZeroOffset + sizeof(char) + sizeof(short int) + 2*sizeof(int);

					  if(indexKeyType == TypeInt)
					  {
						  //Find the data and delete the data if found

						  if((traversePointer - freeSpacePointer) == 0)
						  {
							  free(bufferPage); //new
							  return -1;
						  }

						  int dummy=traversePointer;
						  while(traversePointer < freeSpacePointer)
						  {
							  int currentKeyValue = *(int*)((char*)bufferPage+traversePointer);

							  if(currentKeyValue == *(int*)key)
							  {
								  //Check if this is the only element in the node
								  if(((unsigned)dummy + 3*sizeof(int)) ==  (unsigned)freeSpacePointer)
								  {
									  if(!NonLeaflastElement)
									  {//Since this is the last element in the page this page would be deleted

										  deleteCheck=true;
										  //Read the adjacent pages and update their left and right page numbers
										  int LeftPageNo = *(int*)((char*)bufferPage+ traversePointer - 2*sizeof(int));
										  int RightPageNo = *(int*)((char*)bufferPage+ traversePointer - sizeof(int));
										  void *temp=calloc(PF_PAGE_SIZE,sizeof(char));

										  if(LeftPageNo != -1 && !ixHandle.ReadPage(LeftPageNo,temp))
										  {
											  int PgZeroOffset = 0;
											  if(LeftPageNo == 0)
												  PgZeroOffset = 2*sizeof(int);
											  //Set the right pointer of the left page to the right pointer of the page being deleted
											  *(int *)((char*)temp + PgZeroOffset + sizeof(char) + sizeof(short int) + sizeof(int)) = RightPageNo;
											  ixHandle.WritePage(LeftPageNo,temp);
										  }

										  if(RightPageNo != -1 && !ixHandle.ReadPage(RightPageNo,temp))
										  {
											  int PgZeroOffset = 0;
											  if(RightPageNo == 0)
												  PgZeroOffset = 2*sizeof(int);
											  //Set the left pointer of the right page to the left pointer of the page being deleted
											  *(int *)((char*)temp + PgZeroOffset + sizeof(char) + sizeof(short int)) = LeftPageNo;
											  ixHandle.WritePage(RightPageNo,temp);
										  }
										  free(temp);
										  //Important consideration :  just populate the first bit as 'F' to indicate a free page
										  //All other information is kept intact, since if this is page 0, the program directly reads data and proceeds for routing in the tree further
										  *((char*)bufferPage + PageZeroOffset)= 'F';
										  *(int *)((char*)bufferPage + PageZeroOffset + sizeof(char)) = dummy;
										  ixHandle.WritePage(currentPageNo,bufferPage);
										  free(bufferPage); //new
										  return 0;
									  }
									  else if(NonLeaflastElement)
									  {
										  *(short int*)((char*)bufferPage + sizeof(char) + PageZeroOffset) = dummy;
										  ixHandle.WritePage(currentPageNo,bufferPage);
										  free(bufferPage); //new
										  return 0;
									  }
								  }

								  //Now this part of the code deals with the deleting the element from the leaf node
								  memmove((char*)bufferPage + traversePointer,(char*)bufferPage + traversePointer + 3*sizeof(int),freeSpacePointer - (traversePointer + 3*sizeof(int)));
								  *(short int*)((char*)bufferPage + sizeof(char) + PageZeroOffset)= freeSpacePointer - 3*sizeof(int);														//Set the free space pointer
								  ixHandle.WritePage(currentPageNo,bufferPage);
								  free(bufferPage); //new
								  return 0;
							  }
							  traversePointer += 3*sizeof(int);
						  }

						  //Testing purposes only
						  if(traversePointer == freeSpacePointer)
						  {
							  free(bufferPage); //new
							  return 9;
						  }

					  }
					  else if(indexKeyType == TypeReal)
					  {
						  if((traversePointer - freeSpacePointer) == 0)
						  {
							  free(bufferPage); //new
							  return 9;
						  }

						  int dummy=traversePointer;
						  while(traversePointer < freeSpacePointer)
						  {
							  float currentKeyValue = *(float*)((char*)bufferPage+traversePointer);

							  if(currentKeyValue == *(float*)key)
							  {
								  //Check if this is the only element in the node
								  if(((unsigned)dummy + 3*sizeof(float)) ==  (unsigned)freeSpacePointer)
								  {
									  if(!NonLeaflastElement)
									  {//Since this is the last element in the page this page would be deleted

										  deleteCheck=true;
										  //Read the adjacent pages and update their left and right page numbers
										  int LeftPageNo = *(int*)((char*)bufferPage+ traversePointer - 2*sizeof(int));
										  int RightPageNo = *(int*)((char*)bufferPage+ traversePointer - sizeof(int));
										  void *temp=calloc(PF_PAGE_SIZE,sizeof(char));

										  if(LeftPageNo != -1 && !ixHandle.ReadPage(LeftPageNo,temp))
										  {
											  int PgZeroOffset = 0;
											  if(LeftPageNo == 0)
												  PgZeroOffset = 2*sizeof(int);
											  //Set the right pointer of the left page to the right pointer of the page being deleted
											  *(int *)((char*)temp + PgZeroOffset + sizeof(char) + sizeof(short int) + sizeof(int)) = RightPageNo;
										  }

										  if(RightPageNo != -1 && !ixHandle.ReadPage(RightPageNo,temp))
										  {
											  int PgZeroOffset = 0;
											  if(RightPageNo == 0)
												  PgZeroOffset = 2*sizeof(int);
											  //Set the left pointer of the right page to the left pointer of the page being deleted
											  *(int *)((char*)temp + PgZeroOffset + sizeof(char) + sizeof(short int)) = LeftPageNo;
										  }
										  free(temp);
										  //Important consideration :  just populate the first bit as 'F' to indicate a free page
										  //All other information is kept intact, since if this is page 0, the program directly reads data and proceeds for routing in the tree further
										  *((char*)bufferPage + PageZeroOffset)= 'F';
										  ixHandle.WritePage(currentPageNo,bufferPage);
										  free(bufferPage); //new
										  return 0;
									  }
									  else if(NonLeaflastElement)
									  {
										  *(short int*)((char*)bufferPage + PageZeroOffset + sizeof(char) ) = dummy;
										  ixHandle.WritePage(currentPageNo,bufferPage);
										  free(bufferPage); //new
										  return 0;
									  }
								  }

								  //Now this part of the code deals with the deleting the element from the leaf node
								  memmove((char*)bufferPage + traversePointer,(char*)bufferPage + traversePointer + 3*sizeof(float),freeSpacePointer - (traversePointer + 3*sizeof(float)));
								  *(short int*)((char*)bufferPage + sizeof(char) + PageZeroOffset)= freeSpacePointer - 3*sizeof(float);														//Set the free space pointer
								  ixHandle.WritePage(currentPageNo,bufferPage);
								  free(bufferPage); //new
								  return 0;
							  }
							  traversePointer += 3*sizeof(float);
						  }

						  //Testing purposes only
						  if(traversePointer == freeSpacePointer)
						  {
							  free(bufferPage); //new
							  return 9;
						  }

					  }
					  else if(indexKeyType == TypeVarChar)
					  {
						  if((traversePointer - freeSpacePointer) == 0)
						  {
							  free(bufferPage); //new
							  return 9;
						  }
						  int dummy=traversePointer;

						  char* strKey = (char*)malloc(*(int *)key + 1);
						  memcpy(strKey, (const void*)((int*)key+1), *(int *)key);
						  *((char *)strKey + *(int *)key) = '\0';

						  int currentKeyLength = *(int*)((char*)bufferPage+traversePointer);
						  while(traversePointer < freeSpacePointer)
						  {
							  char* str2 = (char*)malloc(currentKeyLength + 1);
							  memcpy(str2, (const void*)((char*)bufferPage + traversePointer + sizeof(int)), currentKeyLength);
							  *((char *)str2 + currentKeyLength) = '\0';
							  if(strcasecmp(strKey, str2) == 0)
							  {
								  //Check if this is the only element in the node
								  if(((unsigned)dummy + 3*sizeof(int) + currentKeyLength) ==  (unsigned)freeSpacePointer)
								  {
									  if(!NonLeaflastElement)
									  {//Since this is the last element in the page this page would be deleted

										  deleteCheck=true;
										  //Read the adjacent pages and update their left and right page numbers
										  int LeftPageNo = *(int*)((char*)bufferPage+ traversePointer - 2*sizeof(int));
										  int RightPageNo = *(int*)((char*)bufferPage+ traversePointer - sizeof(int));
										  void *temp=calloc(PF_PAGE_SIZE,sizeof(char));

										  if(LeftPageNo != -1 && !ixHandle.ReadPage(LeftPageNo,temp))
										  {
											  int PgZeroOffset = 0;
											  if(LeftPageNo == 0)
												  PgZeroOffset = 2*sizeof(int);
											  //Set the right pointer of the left page to the right pointer of the page being deleted
											  *(int *)((char*)temp + PgZeroOffset + sizeof(char) + sizeof(short int) + sizeof(int)) = RightPageNo;
										  }

										  if(RightPageNo != -1 && !ixHandle.ReadPage(RightPageNo,temp))
										  {
											  int PgZeroOffset = 0;
											  if(RightPageNo == 0)
												  PgZeroOffset = 2*sizeof(int);
											  //Set the left pointer of the right page to the left pointer of the page being deleted
											  *(int *)((char*)temp + PgZeroOffset + sizeof(char) + sizeof(short int)) = LeftPageNo;
										  }
										  free(temp);
										  //Important consideration :  just populate the first bit as 'F' to indicate a free page
										  //All other information is kept intact, since if this is page 0, the program directly reads data and proceeds for routing in the tree further
										  *((char*)bufferPage + PageZeroOffset)= 'F';
										  ixHandle.WritePage(currentPageNo,bufferPage);
										  free(bufferPage); //new
										  return 0;
									  }
									  else if(NonLeaflastElement)
									  {
										  *(short int*)((char*)bufferPage + PageZeroOffset + sizeof(char)) = dummy;
										  ixHandle.WritePage(currentPageNo,bufferPage);
										  free(bufferPage); //new
										  return 0;
									  }
								  }

								  //Now this part of the code deals with the deleting the element from the leaf node
								  memmove((char*)bufferPage + traversePointer,(char*)bufferPage + traversePointer + 3*sizeof(int) + currentKeyLength,freeSpacePointer - (traversePointer + 3*sizeof(int) + currentKeyLength));
								  *(short int*)((char*)bufferPage + sizeof(char) + PageZeroOffset)= freeSpacePointer - (3*sizeof(int) + currentKeyLength);														//Set the free space pointer
								  ixHandle.WritePage(currentPageNo,bufferPage);
								  free(bufferPage); //new
								  return 0;
							  }
							  traversePointer += 3*sizeof(int) + currentKeyLength;
							  currentKeyLength = *(int*)((char*)bufferPage+traversePointer);
						  }
						  //Testing purposes only
						  if(traversePointer == freeSpacePointer)
						  {
							  free(bufferPage); //new
							  return 9;
						  }
					  }//End if for index types
				}//End if for Page Types // wrong bracket
		  }//End if for current page checking
	  }//End if for checking whether file is open

	  return -1;

  }


  // IX_IndexScan Functions
  RC IX_IndexScan::OpenScan(IX_IndexHandle &indexHandle, CompOp compOp, void *value){						// Initialize index scan

//	  compOperator = compOp;
//	  keyValue = value;

	  if(!indexHandle.ixHandle.file.is_open())
		  return 6;//index file not open
	  int pageNo=0;
	  char pageType;
	  void *bufferPage = calloc(PF_PAGE_SIZE, sizeof(char));
	  indexHandle.ixHandle.ReadPage(pageNo, bufferPage);
	  indexHandle.indexKeyType=*(AttrType *)bufferPage;
	  pageNo=*(int *)((AttrType *)bufferPage + 1);   				//Now we can have the root page in buffer

	  unsigned traversePointer;
	  unsigned PageZeroOffset = 2*sizeof(int);

	  indexHandle.ixHandle.ReadPage(pageNo, bufferPage);
	  pageType = *((char*)bufferPage + PageZeroOffset);

	  if(pageNo == 0)
		  PageZeroOffset = 2*sizeof(int);
	  else
		  PageZeroOffset = 0;


	  while(pageType!='L') 											//Traverse all non leaves till first eligible leaf node is reached
	  {

		  traversePointer = sizeof(char) + sizeof(short int) + sizeof(int);
		  short int freeSpacePointer=*(short int*)((char*)bufferPage + PageZeroOffset + sizeof(char));

		  if(compOp!=NO_OP && value == NULL)
		  {
			  free(bufferPage); //new
			  return 5;
		  }

		  if (compOp==NO_OP || compOp==NE_OP)
			  pageNo=*(int*)((char*)bufferPage + traversePointer + PageZeroOffset - sizeof(int));

		  else if(indexHandle.indexKeyType == TypeInt)
		  {
			  while(traversePointer + PageZeroOffset < (unsigned)freeSpacePointer)
			  {
				  unsigned keyOffset=PageZeroOffset + traversePointer;
				  if(*(int *)value < *(int *)((char*)bufferPage + keyOffset))
				  {
					  pageNo=*(int *)((char*)bufferPage + keyOffset - sizeof(int));			//Take left child if value is least for that page..
					  break;
				  }
				  else if((keyOffset + 2*sizeof(int)) >= (unsigned)freeSpacePointer || (*(int *)value >= *(int *)((char*)bufferPage + keyOffset) && *(float *)value < *(int *)((char*)bufferPage + keyOffset + sizeof(int) + sizeof(int))))
				  {
					  pageNo=*(int *)((char*)bufferPage + keyOffset + sizeof(int));		//Take right pointer if value is greater than current key and lesser than next key OR this is the last key on the page
					  break;
				  }
				  else
					  traversePointer += (2*sizeof(int));
			  }
		  }
		  else if (indexHandle.indexKeyType == TypeReal)
		  {
			  while(traversePointer + PageZeroOffset < (unsigned)freeSpacePointer)
			  {
				  unsigned keyOffset=PageZeroOffset + traversePointer;
				  if(*(float *)value < *(float *)((char*)bufferPage + keyOffset))
				  {
					  pageNo=*(int *)((char*)bufferPage + keyOffset - sizeof(int));			//Take left child if value is least for that page..
					  break;
				  }
				  else if((keyOffset + sizeof(float) + sizeof(int)) >= (unsigned)freeSpacePointer || (*(float *)value >= *(float *)((char*)bufferPage + keyOffset) && *(float *)value < *(float *)((char*)bufferPage + keyOffset + sizeof(float) + sizeof(int))))
				  {
					  pageNo=*(int *)((char*)bufferPage + keyOffset + sizeof(float));		//Take right pointer if value is greater than current key and lesser than next key OR this is the last key on the page
					  break;
				  }
				  else traversePointer += (sizeof(float) + sizeof(int));
			  }

		  }
		  else if(indexHandle.indexKeyType == TypeVarChar)
		  {
			  while(traversePointer + PageZeroOffset < (unsigned)freeSpacePointer)
			  {
				  unsigned valLength=*(int*)value;					//Storing the value as a string to compare
				  char *strVal=(char*)malloc(valLength + 1);
				  memcpy(strVal, (char*)value + sizeof(int), valLength);
				  *((char*)strVal + valLength) = '\0';

				  unsigned keyOffset=PageZeroOffset + traversePointer + sizeof(int);

				  unsigned keyLength=*(int*)((char*)bufferPage + keyOffset - sizeof(int));
				  char *strKey=(char*)malloc(keyLength + 1);		//Storing the current key as a string to compare
				  memcpy(strKey, (char*)bufferPage + keyOffset, keyLength);
				  *((char*)strKey + keyLength) = '\0';

				  char *strKeyNext=(char*)malloc(keyLength + 1);	//Storing the next key as a string to compare
				  unsigned keyNextLength = *(int *)((char*)bufferPage + keyOffset + keyLength);
				  memcpy(strKeyNext, (char*)bufferPage + keyOffset + keyLength + 2*sizeof(int), keyNextLength); //traverse the current key, its pg no. and the length field of next key..
				  *((char*)strKeyNext + keyNextLength) = '\0';

				  if(strcasecmp(strVal, strKey)<0)
				  {
					  pageNo=*(int *)((char*)bufferPage + keyOffset - 2*sizeof(int));	//Key offset - size field and - left page field
					  break;
				  }
				  else if(keyOffset + keyLength + 2*sizeof(int) >= (unsigned)freeSpacePointer || (strcasecmp(strVal, strKey) >= 0 && strcasecmp(strVal, strKeyNext)<0))
				  {
					  pageNo=*(int *)((char*)bufferPage + keyOffset + keyLength);
					  break;
				  }
				  else traversePointer += (2*sizeof(int) + keyLength);
				  free(strVal); //new
				  free(strKey); //new
				  free(strKeyNext); //new
			  }
		  }

		  indexHandle.ixHandle.ReadPage(pageNo, bufferPage);
		  if(pageNo == 0)
			  PageZeroOffset = 2*sizeof(int);
		  else
			  PageZeroOffset = 0;
		  pageType = *((char*)bufferPage + PageZeroOffset);
	  }

	  //Now that we are at the leaf page, we check the operator and traverse accordingly

	  bool isNextPage = true; //to proceed to the next page incase of a range query
	  while(isNextPage && pageNo > -1)
	  {
		  bool check = false;	//Parameter to determine correct operator
		  isNextPage = true;
		  RID rid;
		  indexHandle.ixHandle.ReadPage(pageNo, bufferPage);

		  if(pageNo == 0)
			  PageZeroOffset = 2*sizeof(int);
		  else
			  PageZeroOffset = 0;
		  traversePointer = PageZeroOffset + sizeof(char) + sizeof(short int) + 2*sizeof(int);	//Initialize traversal pointer to beginning of page very new page
		  unsigned keyOffset=traversePointer;
		  short int freeSpacePointer=*(short int*)((char*)bufferPage + PageZeroOffset + sizeof(char));

		  if(indexHandle.indexKeyType == TypeInt)
		  {

			  while(keyOffset < (unsigned)freeSpacePointer)	//traverse this particular page
			  {
				  check = false;

				  switch(compOp)
				  {
				  case EQ_OP:
					  if(*(int*)((char*)bufferPage + keyOffset) == *(int*)value)
						  check=true;
					  break;
				  case LT_OP:
					  if(*(int*)((char*)bufferPage + keyOffset) < *(int*)value)
						  check=true;
					  break;
				  case GT_OP:
					  if(*(int*)((char*)bufferPage + keyOffset) > *(int*)value)
						  check=true;
					  break;
				  case LE_OP:
					  if(*(int*)((char*)bufferPage + keyOffset) <= *(int*)value)
						  check=true;
					  break;
				  case GE_OP:
					  if(*(int*)((char*)bufferPage + keyOffset) >= *(int*)value)
						  check=true;
					  break;
				  case NE_OP:
					  if(*(int*)((char*)bufferPage + keyOffset) != *(int*)value)
						  check=true;
					  break;
				  case NO_OP:
					  check=true;
					  break;
				  default:
					  break;
				  }

				  if(check)
				  {
					  dataVec.push_back(*(int*)((char*)bufferPage + keyOffset));
					  rid.pageNum = *(int*)((char*)bufferPage + keyOffset + sizeof(int));
					  rid.slotNum = *(int*)((char*)bufferPage + keyOffset + sizeof(int) + sizeof(int));
					  ridVec.push_back(rid);
				  }
				  keyOffset += (sizeof(int) + 2*sizeof(int)); //current value + RID
			  }
			  if(compOp == LT_OP || compOp == LE_OP){
				  pageNo=*(int *)((char*)bufferPage + traversePointer - 2*sizeof(int));
			  }

			  else if(compOp == GT_OP || compOp == GE_OP || compOp == NE_OP || compOp == NO_OP)
				  pageNo=*(int *)((char*)bufferPage + traversePointer - sizeof(int));
			  else
				  isNextPage = false;
			  if(pageNo == 0xFFFF)		//If we reach the extreme leaf pages
				  isNextPage = false;
		  }

		  else if(indexHandle.indexKeyType == TypeReal)
		  {
			  while(keyOffset < (unsigned)freeSpacePointer)	//traverse this particular page
			  {
				  check = false;

				  switch(compOp)
				  {
				  case EQ_OP:
					  if(*(float*)((char*)bufferPage + keyOffset) == *(float*)value)
						  check=true;
					  break;
				  case LT_OP:
					  if(*(float*)((char*)bufferPage + keyOffset) < *(float*)value)
						  check=true;
					  break;
				  case GT_OP:
					  if(*(float*)((char*)bufferPage + keyOffset) > *(float*)value)
						  check=true;
					  break;
				  case LE_OP:
					  if(*(float*)((char*)bufferPage + keyOffset) <= *(float*)value)
						  check=true;
					  break;
				  case GE_OP:
					  if(*(float*)((char*)bufferPage + keyOffset) >= *(float*)value)
						  check=true;
					  break;
				  case NE_OP:
					  if(*(float*)((char*)bufferPage + keyOffset) != *(float*)value)
						  check=true;
					  break;
				  case NO_OP:
					  check=true;
					  break;
				  default:
					  break;
				  }

				  if(check)
				  {
					  dataVec.push_back(*(float*)((char*)bufferPage + keyOffset));
					  rid.pageNum = *(int*)((char*)bufferPage + keyOffset + sizeof(int));
					  rid.slotNum = *(int*)((char*)bufferPage + keyOffset + sizeof(float) + sizeof(int));
					  ridVec.push_back(rid);
				  }
				  keyOffset += (sizeof(float) + 2*sizeof(int)); //current value + RID
			  }

			  if(compOp == LT_OP || compOp == LE_OP)
				  pageNo=*(int *)((char*)bufferPage + traversePointer - 2*sizeof(int));
			  else if(compOp == GT_OP || compOp == GE_OP || compOp == NE_OP || compOp == NO_OP)
				  pageNo=*(int *)((char*)bufferPage + traversePointer - sizeof(int));
			  else
				  isNextPage = false;

			  if(pageNo == 0xFFFF)		//If we reach the extreme leaf pages
				  isNextPage = false;
		  }

		  else if(indexHandle.indexKeyType == TypeVarChar)
		  {
			  while(keyOffset < (unsigned)freeSpacePointer)
			  {
				  unsigned valLength=*(int*)value;					//Storing the value as a string to compare
				  char *strVal=(char*)malloc(valLength + 1);
				  memcpy(strVal, (char*)value + sizeof(int), valLength);
				  *((char*)strVal + valLength) = '\0';

				  unsigned keyLength=*(int*)((char*)bufferPage + keyOffset);
				  keyOffset += sizeof(int); 						//to go to the varchar value

				  char *strKey=(char*)malloc(keyLength + 1);		//Storing the current key as a string to compare
				  memcpy(strKey, (char*)bufferPage + keyOffset, keyLength);
				  *((char*)strKey + keyLength) = '\0';

				  check = false;
				  switch(compOp)
				  {
				  case EQ_OP:
					  if(strcasecmp(strKey, strVal) == 0)
						  check=true;
					  break;
				  case LT_OP:
					  if(strcasecmp(strKey, strVal) < 0)
						  check=true;
					  break;
				  case GT_OP:
					  if(strcasecmp(strKey, strVal) > 0)
						  check=true;
					  break;
				  case LE_OP:
					  if(strcasecmp(strKey, strVal) <= 0)
						  check=true;
					  break;
				  case GE_OP:
					  if(strcasecmp(strKey, strVal) >= 0)
						  check=true;
					  break;
				  case NE_OP:
					  if(strcasecmp(strKey, strVal) != 0)
						  check=true;
					  break;
				  case NO_OP:
					  check=true;
					  break;
				  default:
					  break;
				  }

				  if(check)
				  {
					  strVec.push_back(strVal);
					  strLenVec.push_back(keyLength);
					  rid.pageNum = (*(int*)((char*)bufferPage + keyOffset + keyLength));
					  rid.slotNum = (*(int*)((char*)bufferPage + keyOffset + keyLength + sizeof(int)));
					  ridVec.push_back(rid);
				  }
				  keyOffset += (keyLength + 2*sizeof(int));
				  free(strVal);
				  free(strKey);
			  }
			  if(compOp == LT_OP || compOp == LE_OP)
				  pageNo=*(int *)((char*)bufferPage + traversePointer - 2*sizeof(int));
			  else if(compOp == GT_OP || compOp == GE_OP || compOp == NE_OP || compOp == NO_OP)
				  pageNo=*(int *)((char*)bufferPage + traversePointer - sizeof(int));
			  else
				  isNextPage = false;
			  if(pageNo == 0xFFFF)
				  isNextPage = false;
		  }

	  }
	  free(bufferPage); //new
	  return 0;
  }

  RC IX_IndexScan::GetNextEntry(RID &rid){																	// Get next matching entry
	  if(!ridVec.empty())
	  {
		  unsigned size=ridVec.size();
		  dataVec.pop_back();
		  rid.pageNum = ridVec[size-1].pageNum;
		  rid.slotNum = ridVec[size-1].slotNum;
		  ridVec.pop_back();
	  }
	  else
		  return IX_EOF;
	  return 0;
  }

  RC IX_IndexScan::CloseScan(){																				// Terminate index scan
	  ridVec.clear();									//Empty the vectors
	  return 0;
  }



  //Printing Error codes

  void IX_PrintError (RC rc){
	  switch(rc)
	  {
	  case 0: cout<<"Transaction Successfull"<<endl;
	  break;
	  case 1: cout<<"Index file cannot be created"<<endl;
	  break;
	  case 2: cout<<"Index file cannot be opened"<<endl;
	  break;
	  case 3: cout<<"The attribute does not exist in the table"<<endl;
	  break;
	  case 4: cout<<"Index file cannot be destroyed"<<endl;
	  break;
	  case 5: cout<<"Index file cannot be closed"<<endl;
	  break;
	  case 6: cout<<"Index file is not open"<<endl;
	  break;
	  case 7: cout<<"Invalid parameters for Scan"<<endl;
	  break;
	  case 8: cout<<"Table does not exist"<<endl;
	  break;
	  case 9: cout<<"Data does not exist"<<endl;
	  break;
//	  case 10: cout<<"Overflow of duplicate data in a single page"<<endl;
//	  break;
	  case -1: cout<<" ERROR! "<<endl;
	  break;
	  }

  }
