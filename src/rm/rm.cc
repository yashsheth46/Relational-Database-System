
#include "rm.h"
#include<cstdlib>
#include<cstring>

RM* RM::_rm = 0;
PF_Manager* pf;												//Page file manager instance
string tombStone="~~";										//Tomb stone string to recognize

RM* RM::Instance()
{
    if(!_rm)
        _rm = new RM();
    
    return _rm;
}

PF_FileHandle catHandle;									//File Handle for the Catalog file

RM::RM()
{
	pf=PF_Manager::Instance();								//Invoking the PF_Manager Instance
if(!pf->FileExists("CAT_COLUMNS")){
	struct Attribute CatAttr[6];
															//Hard coding the information to be stored in the CAT_COLUMNS.CAT file
	CatAttr[0].name="TBLNAME";
	CatAttr[0].length=20;
	CatAttr[0].type=TypeVarChar;
	CatAttr[1].name="COLNAME";
	CatAttr[1].length=20;
	CatAttr[1].type=TypeVarChar;
	CatAttr[2].name="POSITION";
	CatAttr[2].length=4;
	CatAttr[2].type=TypeInt;
	CatAttr[3].name="TYPE";
	CatAttr[3].length=15;
	CatAttr[3].type=TypeInt;
	CatAttr[4].name="EXISTS";
	CatAttr[4].length=1;
	CatAttr[4].type=TypeVarChar;
	CatAttr[5].name="MAXLENGTH";
	CatAttr[5].length=4;
	CatAttr[5].type=TypeInt;

	const vector<Attribute> CatVector(CatAttr,CatAttr + sizeof(CatAttr)/sizeof(struct Attribute));
	createTable("CAT_COLUMNS",CatVector);

}
	pf->OpenFile("CAT_COLUMNS",catHandle);
}

RM::~RM()
{

	if(delRids.empty())
			cout<<"RID list is empty!!";
	else
		for(unsigned i=0;i<delRids.size();i++){
			reorganizePage(delRids[i].fileName, delRids[i].drid.pageNum);
			delRids.pop_back();
		}
	pf->CloseFile(catHandle);
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data){

	if(!strcmp("CAT_COLUMNS",fileName.c_str())){													//Do not allow if the file is the Catalog
		return -1;
	}

	PF_FileHandle handle;
	void *page=calloc(PF_PAGE_SIZE, sizeof(char));

	unsigned dataOffset=0;
	if(!(pf->OpenFile(fileName.c_str(), handle))){													//Enter only if file opens successfully
		unsigned nPgs=handle.GetNumberOfPages();
		for(unsigned p=lrid.pageNum; p<nPgs; p++){													//Traverse pages starting from last result
			pf->OpenFile(fileName.c_str(), handle);//********NEW*************
			handle.ReadPage(p, page);
			unsigned nSlots=*(int *)((char *)page+PF_PAGE_SIZE-10);

			unsigned s;
			if(p==lrid.pageNum)
				s=lrid.slotNum;
			else s=0;

			if(lrid.pageNum==(nPgs-1) && lrid.slotNum == (nSlots))
								return RM_EOF;

			short int pos, pos2;
			for(; s<nSlots; s++){																	//Traverse pages starting from last result(dff for current page)
				rid.pageNum=p; rid.slotNum=s;
				bool check=false;
				void *recPtr= (char *)page+ *(int *)((char *)page+PF_PAGE_SIZE-10-8*(s+1));

				if(conditionAttribute.empty()){
					check=true;
				}
				else{
					pos=*(short int *)((char*)recPtr+(condAttr.position-1)*sizeof(short int));
					pos2=*(short int *)((char*)recPtr+(condAttr.position)*sizeof(short int));

					void *cAttr=calloc(pos2-pos,sizeof(char));
					memcpy(cAttr,(char*)recPtr+pos, pos2-pos);

					int diff=0;
					float fdiff=0;

					if(value==NULL)
						check=true;
					else if(condAttr.type== TypeInt)
						diff=(*(int *)cAttr-*(int *)value);
					else if(condAttr.type== TypeReal)
						fdiff=(*(float *)cAttr-*(float *)value);
					else
						diff=(strcmp((char *)cAttr, (char *)value));

					switch(compOp){																	//Compare the attribute with the value accordingly
					case EQ_OP:
						check=(bool)(diff==0 && fdiff==0);
						break;
					case LT_OP:
						check=(bool)(diff<0||fdiff<0);
						break;
					case GT_OP:
						check=(bool)(diff>0||fdiff>0);
						break;
					case LE_OP:
						check=(bool)(diff<=0 && fdiff<=0);
						break;
					case GE_OP:
						check=(bool)(diff>=0 && fdiff>=0);
						break;
					case NE_OP:
						check=(bool)(diff<0||fdiff<0||diff>0||fdiff>0);
						break;
					case NO_OP:
						check=true;
						break;
					default:
						break;
					}
				}
				lrid.pageNum=p; lrid.slotNum=++s;

				for(unsigned k=0; k<attrs.size() && check; k++){									//Get attribute values from file for the Comp attribute required

					pos=*(short int *)((char*)recPtr+(attrs[k].position-1)*sizeof(short int));
					pos2=*(short int *)((char*)recPtr+(attrs[k].position)*sizeof(short int));
					if(attrs[k].type==TypeInt)
						*(int *)((char *)data+dataOffset)=*(int *)((char *)recPtr+pos);
					else if(attrs[k].type==TypeReal)
						*(float *)((char *)data+dataOffset)=*(float *)((char *)recPtr+pos);
					else
					{
						*(int*)((char *)data+dataOffset) = (int)(pos2-pos);								//**********************Added for QE ..
						memcpy((char *)data+dataOffset + sizeof(int), (char *)recPtr+pos, pos2-pos);
					}
					dataOffset+=pos2-pos;
				}

				pf->CloseFile(handle);
				free(page);
				if(!check)
					continue;//***********NEW************
//					*(int *)data=-1;
				return 0;


			}

		}

	}
return RM_EOF;
}

RC RM_ScanIterator::close(){

	return -1;
}

//Robust
RC RM::createTable(const string tableName, const vector<Attribute> &attrs){  //Yash
	void *data=calloc(PF_PAGE_SIZE,sizeof(char));
	void *next=data;
	RID rid;
	if(!pf->FileExists(tableName.c_str()))								//Check if the table already exists, if yes return -1
	{
		if(!pf->CreateFile(tableName.c_str()))							//If the file can be created
		{
			if(attrs.size()>0)											//Check if the attribute vector is populated
			{
				for(unsigned i=0; i<attrs.size(); i++)
				{
					next=data;
					*(short int *)next=(7)*sizeof(short int);						//Enter Offset of Table Name in cat tuple
					next=(short int *)next+1;

					*(short int *)next=*((short int *)next-1)+tableName.length();				//Enter Offset of Column Name
					next=(short int *)next+1;

					*(short int *)next=*((short int *)next-1)+attrs.at(i).name.length();		//Enter offset of position value
					next=(short int *)next+1;

					*(short int *)next=*((short int *)next-1)+sizeof(int);						//Enter offset of Type value
					next=(short int *)next+1;

					*(short int *)next=*((short int *)next-1)+sizeof(int);						//Enter offset of Exists value
					next=(short int *)next+1;

					*(short int *)next=*((short int *)next-1)+sizeof(char);						//Enter offset of Max Length
					next=(short int *)next+1;

					*(short int *)next=*((short int *)next-1)+sizeof(int);						//Enter offset of END
					next=(short int *)next+1;


					memmove(next, (const void *)(tableName.c_str()), tableName.length());		//Enter Table Name string
					next=(char *)next+tableName.length();

					memmove(next, (const void *)(attrs[i].name.c_str()), attrs.at(i).name.length());	//Enter Column Name string
					next=(char *)next+attrs[i].name.length();

					*(int *)next=i+1;															//Enter position
					next=(int *)next+1;

					*(int *)next=attrs.at(i).type;												//Enter Type
					next=(int *)next+sizeof(char);

					memmove(next, (const void *)"Y", 1);										//Store the Exists Character
					next=(char *)next+sizeof(char);

					*(int *)next=(int)attrs.at(i).length;										//Enter Max Length
					next=(int *)next+sizeof(char);

					newTupleSize=((int)next-(int)data);
					insertTuple("CAT_COLUMNS",data,rid);
				}
				free(data);
				return 0;
			}
			else
			{
				cout<<"Cannot create table with 0 attributes"<<endl;
				return -1;
			}
		}
		else
		{
			cout<<"Unable to create file"<<endl;
			return -1;
		}
	}
	else
	{
		cout<<"Table already exists "<<endl;
		return -1;
	}

}

RC RM :: createTuple(const string tableName,const void *data, void *buffer,int &size){			//CATATLOG_SCAN SPECIAL

	RM_ScanIterator rmsi;
	vector<string> Attributes;
	vector<cat> load;

	if(!scan("CAT_COLUMNS","TBLNAME",EQ_OP,(const void*)(tableName.c_str()),Attributes,rmsi))			//SCANNING THE catalog to find the position and type of all the columns of this table
	{
		int length=0;
		int previous=0;
		const void *next=data;
		short int offset=((int)rmsi.attrs.size()+1)*(sizeof(short int)); //offset of the first data = number of records + end offset
		int i;
		for(i=0;i<(int)rmsi.attrs.size();i++)
		{
			if(rmsi.attrs[i].type == TypeInt)
			{
				*((short int*)buffer + i) = offset + previous; //storing the offset
				*(int*)((char*)buffer + offset + previous) = *(int*)next; //Storing the record
				previous += sizeof(int);
				next=(char*)next + sizeof(int);
			}
			else if(rmsi.attrs[i].type == TypeReal)
			{
				*((short int*)buffer + i) = offset + previous; //storing the offset
				*(float*)((char*)buffer + offset + previous) = *(float*)next; //Storing the record
				previous += sizeof(float);
				next=(char*)next + sizeof(float);

			}
			else if(rmsi.attrs[i].type == TypeVarChar)
			{
				*((short int*)buffer + i)=offset + previous;
				length=*(int*)next;
				memmove((char*)buffer + offset + previous,(char*)next + sizeof(int),length);
				previous += length;
				next=(char*)next + length + sizeof(int);
			}
		}
		*((short int*)buffer + i)=offset + previous; //Storing the end offset
		size=offset+previous; //Storing the size
		return 0;
	}
	else
		return -1;
}



RC RM::deleteTable(const string tableName){
	RM_ScanIterator rmsi;
	vector<string> Attributes;
	Attributes.push_back("COLNAME");

	if(!scan("CAT_COLUMNS","TBLNAME",EQ_OP,(const void*)(tableName.c_str()),Attributes,rmsi))
	{
		for(unsigned i=0; i<rmsi.attrs.size(); i++)
		{
			deleteTuple("CAT_COLUMNS",rmsi.attrs[i].crid);
		}
		if(!remove(tableName.c_str()))
			return 0;
	}
	return -1;
}

RC RM :: getAttributes(const string tableName, vector<Attribute> &attrs){
	RM_ScanIterator rmsi;
	vector<string> Attributes;
	Attribute dummy;

	if(!scan("CAT_COLUMNS","TBLNAME",EQ_OP,(const void*)(tableName.c_str()),Attributes,rmsi))
	{
		for(unsigned i=0;i<rmsi.attrs.size();i++)
		{
			dummy.name=rmsi.attributes[i];
			dummy.length=rmsi.attrs[i].length;
			dummy.type=rmsi.attrs[i].type;
			attrs.push_back(dummy);
		}
		return 0;
	}
	return -1;
}


RC RM::insertTuple(const string tableName, const void *data, RID &rid){ //Yash
	PF_FileHandle handle;
	void *buffer, *buffer1;
	int size=newTupleSize;				//**Have an issue with passing the size**
	newTupleSize=0;
	buffer=calloc(PF_PAGE_SIZE,sizeof(char));
	buffer1=calloc(PF_PAGE_SIZE,sizeof(char));

	if(strcmp("CAT_COLUMNS",tableName.c_str()))
		createTuple(tableName,data,buffer1,size);
	else
		memcpy(buffer1,data,size);
	if(!size)
		return -1;
	if(!pf->OpenFile(tableName.c_str(),handle))
	{
		findFreeSlot(tableName,rid,size);													//Find the free slot in the page with the free space
		if(!delRids.empty())
			reorganizePage(tableName, rid.pageNum);											//Make sure this page does not have any records to be deleted

		handle.ReadPage(rid.pageNum,buffer);
		int OffsetFreeSpace=*(int*)((char*)buffer + PF_PAGE_SIZE - 4);
		int NoOfSlots=*(int*)((char*)buffer + PF_PAGE_SIZE - 10);
		if(rid.slotNum>=(unsigned)NoOfSlots)
			NoOfSlots++;
		memcpy((char*)buffer+OffsetFreeSpace,buffer1,size);									//Copying the data to the new slot
		*(int*)((char*)buffer + PF_PAGE_SIZE - 10 - (rid.slotNum+1)*8)=OffsetFreeSpace;		//Record offset
		*(int*)((char*)buffer + PF_PAGE_SIZE - 10 - (rid.slotNum+1)*8 + 4)=size;			//Record length
		*(int*)((char*)buffer + PF_PAGE_SIZE - 10) = NoOfSlots;								//Updating the number of slots
		OffsetFreeSpace=OffsetFreeSpace+size;
		*(int*)((char*)buffer + PF_PAGE_SIZE - 4)=OffsetFreeSpace;							//Updating the offset for free memory
		handle.WritePage(rid.pageNum,(const void*)buffer);
		pf->CloseFile(handle);
		free(buffer);
		free(buffer1);
		return 0;
	}
	return -1;
}


RC RM::deleteTuples(const string tableName){ //Yash											//We close the file using the handle, delete it, and recreate the same but empty file
	if(!strcmp("CAT_COLUMNS",tableName.c_str()))											//Do not allow if the file is the Catalog
	{
		cout<<"Enable to delete Catalog tuples"<<endl;
		return -1;
	}
	PF_FileHandle handle;
	pf->OpenFile(tableName.c_str(),handle);
	pf->CloseFile(handle);
	if(remove(tableName.c_str()))
		return -1;
	if(!pf->CreateFile(tableName.c_str()))
	{
		for(unsigned i=0; i<delRids.size();i++) //*************NEW********** Emptying the deleted RIDs List of that table since all tuples are deleted!
		{
			if(!strcmp(delRids[i].fileName.c_str(), tableName.c_str()))
			{
				delRids.erase(delRids.begin() + i);
				i--;
			}
		}
		return 0;
	}
	return -1;
}

RC RM::deleteTuple(const string tableName, const RID &rid){ //Yash

	int open = 0;
	delRID dr;																				//actual deletion occurs through reorganize page where 'stale' records of a file are removed
	dr.drid=rid;
	PF_FileHandle handle;
	void *buffer = malloc(PF_PAGE_SIZE);

	if(!strcmp("CAT_COLUMNS",tableName.c_str()))											//Check if the file is the Catalog
		catHandle.ReadPage(rid.pageNum, buffer);
	else
	{
		open = 1;
		pf->OpenFile(tableName.c_str(), handle);
		handle.ReadPage(rid.pageNum, buffer);
	}


	void * offset=(char *)buffer+PF_PAGE_SIZE-10;
	unsigned noOfSlots=*(int *)offset;
	if(rid.slotNum+1>noOfSlots){															//To check for invalid slot number
		cout<<"Invalid RID!\n";
		return -1;
	}
	dr.fileName=tableName;
	delRids.push_back(dr);
	if(open)
		pf->CloseFile(handle);
	reorganizePage(tableName,rid.pageNum);
	return 0;
}

RC RM::updateTuple(const string tableName, const void *data, const RID &rid){	//Yash
	RID nrid;
	PF_FileHandle handle;
	void *buffer=malloc(PF_PAGE_SIZE);
	void *recOfst, *slotOfst1, *slotOfst2;
	int temp;
	pf->OpenFile(tableName.c_str(), handle);

	if(insertTuple(tableName, data, nrid))
		return -1;
	handle.ReadPage(rid.pageNum, buffer);   //************************NEW******** Read Page before Pointer assignment
	slotOfst1=(char *)buffer+PF_PAGE_SIZE-10-8*(rid.slotNum+1);
	recOfst=(char *)buffer+*(int *)slotOfst1;


	if(nrid.pageNum==rid.pageNum){
		slotOfst2=(char *)buffer+PF_PAGE_SIZE-10-8*(nrid.slotNum+1);
																				//We swap the new slot with the old one, and delete the new one, hence reclaiming the old space
		temp=*(int *)slotOfst1;
		*(int *)slotOfst1=*(int *)slotOfst2;
		*(int *)slotOfst2=temp;

		temp=*((int *)slotOfst1+1);
		*((int *)slotOfst1+1)=*((int *)slotOfst2+1);
		*((int *)slotOfst2+1)=temp;

		handle.WritePage(rid.pageNum,buffer);
		deleteTuple(tableName, nrid);
		pf->CloseFile(handle);
		free(buffer);
	}
	else{
//		void *buffer2=malloc(PF_PAGE_SIZE);										//We store the new RID after string "~~" as the TOMBSTONE
//		handle.ReadPage(nrid.pageNum, buffer2);
//		slotOfst2=(char *)buffer2+PF_PAGE_SIZE-10-8*nrid.slotNum;

		memmove(recOfst,tombStone.c_str(),tombStone.length());
		recOfst=(char *)recOfst+tombStone.length();
		*(int *)recOfst=nrid.pageNum;
		recOfst=(int *)recOfst+1;
		*(int *)recOfst=nrid.slotNum;

		handle.WritePage(rid.pageNum,buffer);   //*******************NEW******************
		pf->CloseFile(handle);
		free(buffer);
	}
	return 0;

}


//RC RM :: readTuple(const string tableName, const RID &rid, void *data){
//
//
//	for(int i=0;i<(int)delRids.size();i++)
//	{
//		if(!(strcmp(delRids[i].fileName.c_str(),tableName.c_str())))
//		{
//			if(delRids[i].drid.pageNum == rid.pageNum && delRids[i].drid.slotNum== rid.slotNum )
//			{
////				cout<<"The record is deleted "<<endl;   //**********NEW********* commented this line
//				return -1;
//			}
//		}
//	}
//	PF_FileHandle handle;
//	void *buffer,*data1;
//	data1=calloc(PF_PAGE_SIZE,1);
//	buffer=calloc(PF_PAGE_SIZE,1);
//	if(!pf->OpenFile(tableName.c_str(),handle))
//	{
//		int NoOfPages = handle.GetNumberOfPages();
//		if(NoOfPages == 0)
//			return -1;
//		handle.ReadPage(rid.pageNum,buffer);
//		int recordOffset=*(int*)((char*)buffer + PF_PAGE_SIZE - 10 - (rid.slotNum+1)*8);
//		int recordLength=*(int*)((char*)buffer + PF_PAGE_SIZE - 10 - (rid.slotNum+1)*8+4);
//		int NoOfSlots=*(int*)((char*)buffer + PF_PAGE_SIZE - 10);
//		if(rid.slotNum >= (unsigned)NoOfSlots)
//		{
//			cout<<"Invalid RID!"<<endl;
//			return -1;
//		}
//		memcpy(data1,((char*)buffer+recordOffset),recordLength);
//		char d[3];
//		memcpy(d,data1,2);
//		d[2]='\0';
//		if(!strcmp(d,"~~"))
//		{
//			RID recordID;
//			recordID.pageNum=*(int*)((char*)data1 + 2);					//Changed data to data1
//			recordID.slotNum=*(int*)((char*)data1 + sizeof(int) + 2);	//Changed data to data1
//			handle.ReadPage(recordID.pageNum,buffer);
//			recordOffset=*(int*)((char*)buffer + PF_PAGE_SIZE - 10 - (recordID.slotNum+1)*8);
//			recordLength=*(int*)((char*)buffer + PF_PAGE_SIZE - 10 - (recordID.slotNum+1)*8+4);
//			memcpy(data1,(char*)buffer+recordOffset,recordLength);
//		}
//		//decode the data to the l1f1 format
//		RM_ScanIterator rmsi;
//		vector<string> Attributes;
//		Attribute dummy;
//		short int offset1,offset2;
//		void *next=data;
//		if(!scan("CAT_COLUMNS","TBLNAME",EQ_OP,(const void*)(tableName.c_str()),Attributes,rmsi))
//		{
//			offset1=(rmsi.attrs.size()+1)*sizeof(short int);
//			for(unsigned i=0;i<rmsi.attrs.size();i++)
//			{
//				if(rmsi.attrs[i].type==TypeInt)
//				{
//				offset1=*((short int*)data1+i);
//				*(int*)next=*(int*)((char*)data1+offset1);
//				next=(char*)next+sizeof(int);
//
//				}
//				else if(rmsi.attrs[i].type==TypeReal)
//				{
//				offset1=*((short int*)data1+i);
//				*(float*)next=*(float*)((char*)data1+offset1);
//				next=(char*)next+sizeof(float);
//				}
//				else if(rmsi.attrs[i].type==TypeVarChar)
//				{
//				offset1=*((short int*)data1+i);
//				offset2=*((short int*)data1+i+1);
//				*(int*)next=(int)(offset2-offset1);
//				next=(char*)next+sizeof(int);
//				memmove(next,(char*)data1+offset1,offset2-offset1);
//				next=(char *)next+offset2-offset1;
//				}
//			}
//
//	}
//	pf->CloseFile(handle);
//	free(buffer);
//	free(data1);
//	//modified
//
//	return 0;
//	}
//
//	return -1;
//
//}


RC RM :: readTuple(const string tableName, const RID &rid, void *data){


	for(int i=0;i<(int)delRids.size();i++)
	{
		if(!(strcmp(delRids[i].fileName.c_str(),tableName.c_str())))
		{
			if(delRids[i].drid.pageNum == rid.pageNum && delRids[i].drid.slotNum== rid.slotNum )
			{
//				cout<<"The record is deleted "<<endl;  //*****************NEW***************
				return -1;
			}
		}
	}
	PF_FileHandle handle;
	void *buffer,*data1;
	data1=calloc(PF_PAGE_SIZE,1);
	buffer=calloc(PF_PAGE_SIZE,1);
	if(!pf->OpenFile(tableName.c_str(),handle))
	{
		int NoOfPages = handle.GetNumberOfPages();
		if(NoOfPages == 0)
			return -1;
		handle.ReadPage(rid.pageNum,buffer);
		int recordOffset=*(int*)((char*)buffer + PF_PAGE_SIZE - 10 - (rid.slotNum+1)*8);
		int recordLength=*(int*)((char*)buffer + PF_PAGE_SIZE - 10 - (rid.slotNum+1)*8+4);
		int NoOfSlots=*(int*)((char*)buffer + PF_PAGE_SIZE - 10);
		if(rid.slotNum >= (unsigned)NoOfSlots)
		{
			cout<<"Invalid RID!"<<endl;
			return -1;
		}
		memcpy(data1,((char*)buffer+recordOffset),recordLength);
		char d[3];
		memcpy(d,data1,2);
		d[2]='\0';    //Appended for strcmp to work

		if(!strcmp(d,"~~"))
		{
		RID recordID;
		recordID.pageNum=*(int*)((char*)data1 + 2);		//**************NEW************ data changed to data1
		recordID.slotNum=*(int*)((char*)data1 + sizeof(int) + 2);
		handle.ReadPage(recordID.pageNum,buffer);
		recordOffset=*(int*)((char*)buffer + PF_PAGE_SIZE - 10 - (recordID.slotNum+1)*8);
		recordLength=*(int*)((char*)buffer + PF_PAGE_SIZE - 10 - (recordID.slotNum+1)*8+4);
		memcpy(data1,((char*)buffer+recordOffset),recordLength);
		}
		//decode the data to the l1f1 format
		RM_ScanIterator rmsi;
		vector<string> Attributes;
		Attribute dummy;
		short int offset1,offset2;
		void *next=data;
		if(!scan("CAT_COLUMNS","TBLNAME",EQ_OP,(const void*)(tableName.c_str()),Attributes,rmsi))
		{
			offset1=(rmsi.attrs.size()+1)*sizeof(short int);
			for(unsigned i=0;i<rmsi.attrs.size();i++)
			{
				if(rmsi.attrs[i].type==TypeInt)
				{
				offset1=*((short int*)data1+i);
				*(int*)next=*(int*)((char*)data1+offset1);
				next=(char*)next+sizeof(int);

				}
				else if(rmsi.attrs[i].type==TypeReal)
				{
				offset1=*((short int*)data1+i);
				*(float*)next=*(float*)((char*)data1+offset1);
				next=(char*)next+sizeof(float);
				}
				else if(rmsi.attrs[i].type==TypeVarChar)
				{
				offset1=*((short int*)data1+i);
				offset2=*((short int*)data1+i+1);
				*(int*)next=(int)(offset2-offset1);
				next=(char*)next+sizeof(int);
				memmove(next,(char*)data1+offset1,offset2-offset1);
				next=(char *)next+offset2-offset1;
				}
			}
	}
	pf->CloseFile(handle);
	free(buffer);
	free(data1);
	//modified

	return 0;
	}

	return -1;

}


RC RM::readAttribute(const string tableName, const RID &rid, const string attributeName, void *data){
	RM_ScanIterator rmsi;
	vector<string> Attributes;
	PF_FileHandle handle;
	int position=0,type=0;
	unsigned i;
	void *buffer;
	if(!scan("CAT_COLUMNS","TBLNAME",EQ_OP,(const void*)(tableName.c_str()),Attributes,rmsi))
	{
		for(i=0;i<rmsi.attrs.size();i++)
		{
			if(!strcmp(rmsi.attributes[i].c_str(),attributeName.c_str()))
			{
				position=rmsi.attrs[i].position;
				type=rmsi.attrs[i].type;
				break;
			}
		}
		if(i==rmsi.attrs.size())
		{
			cout<<"The given attribute doesnt exist in the table"<<endl;
			return -1;
		}
		buffer=malloc(PF_PAGE_SIZE);
		if(!pf->OpenFile(tableName.c_str(),handle))
		{
			handle.ReadPage(rid.pageNum,buffer);
			int recordOffset=*(int*)((char*)buffer + PF_PAGE_SIZE - 10 - (rid.slotNum+1)*8);
			short int AttrOffset=*(short int*)((char*)buffer + recordOffset + (position-1)*(sizeof(short int)));
			if(type == TypeInt)
			{
				*(int*)data=*(int*)((char*)buffer + recordOffset + AttrOffset);
			}
			else if(type == TypeReal)
			{
				*(float*)data=*(float*)((char*)buffer + recordOffset + AttrOffset);
			}
			else if(type == TypeVarChar)
			{
				short int NextAttrOffset=*(int*)((char*)buffer + recordOffset + (position)*(sizeof(short int))); //Getting the attribute of the next offset .. if this is at the end then getting the end offset
				memmove(data,(char*)buffer + recordOffset + AttrOffset, NextAttrOffset - AttrOffset);
			}
		}
		return 0;
	}
	else
		return -1;


}


RC RM::scan(const string tableName,
     const string conditionAttribute,
     const CompOp compOp,					// comparison type such as "<" and "="
     const void *value,						// used in the comparison
     const vector<string> &attributeNames,	// a list of projected attributes
     RM_ScanIterator &rm_ScanIterator){

	RID firstRID;
	firstRID.pageNum=0;						//Populating the Scan_Iterator
	firstRID.slotNum=0;
	rm_ScanIterator.lrid=firstRID;
	rm_ScanIterator.fileName=tableName;
	rm_ScanIterator.attributes=attributeNames;
	rm_ScanIterator.conditionAttribute=conditionAttribute;
	rm_ScanIterator.compOp=compOp;
	rm_ScanIterator.value=value;

	if(!strcmp("CAT_COLUMNS",tableName.c_str())){
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

						rm_ScanIterator.attributes.push_back((char*)colNm);
						cat catvctr;
						catvctr.position=*(int *)((char *)recPtr+offset2);
						catvctr.type=(AttrType)*(int *)((char *)recPtr+offset3);
						catvctr.length=(AttrLength)*(int *)((char *)recPtr+offset5);
						catvctr.crid.pageNum=p;
						catvctr.crid.slotNum=s;
						rm_ScanIterator.attrs.push_back(catvctr);

			}
			free(tblNm);

			}
		}
		free(catalog);
		return 0;
	}
	else{
		RM_ScanIterator rmsi;
		vector<string> Attributes;

		scan("CAT_COLUMNS", "TBLNAME", EQ_OP, tableName.c_str(), Attributes, rmsi);

		unsigned i=0,j=0;

		while(i<attributeNames.size()){
			if(!strcmp(attributeNames[i].c_str(), rmsi.attributes[j].c_str())){
				rm_ScanIterator.attrs.push_back(rmsi.attrs[j]);
				if(!strcmp(rm_ScanIterator.conditionAttribute.c_str(), rmsi.attributes[j].c_str())){
					rm_ScanIterator.condAttr.crid.pageNum=rmsi.attrs[j].crid.pageNum;
					rm_ScanIterator.condAttr.crid.slotNum=rmsi.attrs[j].crid.slotNum;
					rm_ScanIterator.condAttr.length=rmsi.attrs[j].length;
					rm_ScanIterator.condAttr.position=rmsi.attrs[j].position;
					rm_ScanIterator.condAttr.type=rmsi.attrs[j].type;
				}

				i++; j++;
			}
			else if(!strcmp(rm_ScanIterator.conditionAttribute.c_str(), rmsi.attributes[j].c_str())){
				rm_ScanIterator.condAttr.crid.pageNum=rmsi.attrs[j].crid.pageNum;
				rm_ScanIterator.condAttr.crid.slotNum=rmsi.attrs[j].crid.slotNum;
				rm_ScanIterator.condAttr.length=rmsi.attrs[j].length;
				rm_ScanIterator.condAttr.position=rmsi.attrs[j].position;
				rm_ScanIterator.condAttr.type=rmsi.attrs[j].type;
			}
			else
				j++;

		}

	return 0;
	}
	return -1;
}


RC RM::reorganizePage(const string tableName, const unsigned pageNumber){ //Swapnil
	//Algorithm
	//Take the page number and search for all the vacant slots in the rid vector
	//Arrange the offsets of slots in ascending order
	//Move the data accordingly so that the space moves gradually down ...
	//Update the free space offset and all the offsets of slots having offsets greater than the current deleted slot
	//Store the slot numbers in ascending order in the slotNum array
	int open=0;
	vector<unsigned> slotNum; 										// to store the slotnumbers
	vector< int > RecordOffset; 									// Storing offsets for the deleted records
	int j=0; 														// counter for the "slotNum" vector
	PF_FileHandle handle;
	int offset;

	void *buffer=calloc(PF_PAGE_SIZE,1);
	if(!strcmp("CAT_COLUMNS",tableName.c_str()))											//Check if the file is the Catalog
		catHandle.ReadPage(pageNumber, buffer);
	else
	{
		open=1;
		pf->OpenFile(tableName.c_str(), handle);
		handle.ReadPage(pageNumber, buffer);
	}



	for(unsigned i=0;i<delRids.size();i++)
	{
		if(!strcmp(delRids[i].fileName.c_str(), tableName.c_str()) && delRids[i].drid.pageNum==pageNumber)
		{
			offset=*(int*)((char*)buffer + PF_PAGE_SIZE - 10 - (delRids[i].drid.slotNum+1)*8);
			if(offset!=-1)
			{
				if(j==0)											//push the first slot onto the vector
				{
					RecordOffset.push_back(offset);
					slotNum.push_back(delRids[i].drid.slotNum);
					j++;
				}
				else if(offset>RecordOffset[j-1])
				{
					RecordOffset.push_back(offset);
					slotNum.push_back(delRids[i].drid.slotNum);
					j++;
				}
				else
				{
					RecordOffset.push_back(offset);
					slotNum.push_back(delRids[i].drid.slotNum);

					for(unsigned k=j; k>=0; k--)
					{
						if(RecordOffset[k]<RecordOffset[k-1])
						{
							unsigned temp=RecordOffset[k-1];
							RecordOffset[k-1]=RecordOffset[k];
							RecordOffset[k]=temp;

							temp=slotNum[k-1];
							slotNum[k-1]=slotNum[k];
							slotNum[k]=temp;
						}
					}
					slotNum.push_back(slotNum[j-1]);				//duplicate to new place
					slotNum[j-1]=delRids[i].drid.slotNum;			//swap new vector in place of old one
					j++;
				}
			}
		}
	}
	//Move the data to its right place
	//Compare the slots 1 by 1 and shift the data gradually downwards
	for(unsigned i=0;i<slotNum.size();i++)
	{
		int delrecordOffset,recordOffset,recordLength,NoOfSlots,OffsetFreeSpace;
		NoOfSlots=*(int*)((char*)buffer + PF_PAGE_SIZE - 10);
		OffsetFreeSpace=*(int*)((char*)buffer + PF_PAGE_SIZE - 4);
		delrecordOffset=*(int*)((char*)buffer + PF_PAGE_SIZE - 10 - (slotNum[i]+1)*8);
		recordLength=*(int*)((char*)buffer + PF_PAGE_SIZE - 10 - (slotNum[i]+1)*8 + 4);

		memmove((char*)buffer+delrecordOffset,(char*)buffer + delrecordOffset + recordLength,OffsetFreeSpace - delrecordOffset - recordLength);
		OffsetFreeSpace -= recordLength;
		//UPDATING THE RECORD OFFSETS
		for(int k=0; k < NoOfSlots; k++)
		{
				recordOffset=*(int*)((char*)buffer + PF_PAGE_SIZE - 10 - (k+1)*8);
				if(recordOffset != -1 && recordOffset>delrecordOffset)
				{
					recordOffset -= recordLength;
					*(int*)((char*)buffer + PF_PAGE_SIZE - 10 - (k+1)*8)=recordOffset;
				}

		}
		*(int*)((char*)buffer + PF_PAGE_SIZE - 10 - (slotNum[i]+1)*8) = -1;	//setting the record offset to -1 for the deleted record
		*(int*)((char*)buffer + PF_PAGE_SIZE - 4)=OffsetFreeSpace;
	}

	if(open)
	{
		handle.WritePage(pageNumber,buffer);
		pf->CloseFile(handle);
	}
	else
		catHandle.WritePage(pageNumber,buffer);



	return 0;

}

RC RM::findFreeSlot(const string fileName,RID &rid,int size) //Swapnil
{
	PageNum NoOfPages;
	PF_FileHandle handle;
	int FreeSpaceOffset,NoOfSlots;
	void *buffer;
	buffer=malloc(PF_PAGE_SIZE);

	if(!pf->OpenFile(fileName.c_str(),handle))
	{
		NoOfPages=handle.GetNumberOfPages();
		//If there are no pages, add a new page and allocate a slot from the new page
		if(NoOfPages==0)
		{
			FreeSpaceOffset=0;
			NoOfSlots=1;

			*(int*)((char*)buffer+PF_PAGE_SIZE-4)=FreeSpaceOffset;
			*((char*)buffer+PF_PAGE_SIZE-5)='Y';
			*((char*)buffer+PF_PAGE_SIZE-6)='N';
			//Slot no is written to the file in this case since this is the first slot and it would return garbage values if the correct value is not written
			*(int*)((char*)buffer+PF_PAGE_SIZE-10) = NoOfSlots;
			if(!handle.AppendPage(buffer))
			{
				rid.pageNum=0;
				rid.slotNum=0;
				pf->CloseFile(handle);
				return 0;
			}
		}
		else
		{
			//Check for the pages to find free space
			int i;
			for(i=0;i<(int)NoOfPages;i++)
			{
				handle.ReadPage(i,buffer);
				char checkFreeSpace = *((char*)buffer + PF_PAGE_SIZE - 5);
				//IF the current free page has free space
				if(checkFreeSpace=='Y')
				{
					int NewSlot=0;
					char checkFreeSlot = *((char*)buffer + PF_PAGE_SIZE - 6);
					//Would there be a new slot in the directory
					if(checkFreeSlot=='N')
						NewSlot=8;
					FreeSpaceOffset=*(int*)((char*)buffer + PF_PAGE_SIZE - 4);
					NoOfSlots=*(int*)((char*)buffer + PF_PAGE_SIZE - 10);
					int DirectoryOffset=PF_PAGE_SIZE - (10 + NoOfSlots*8 + NewSlot);
					int AvailableSpace=DirectoryOffset-FreeSpaceOffset;
					if(AvailableSpace >= size)
					{
						rid.pageNum=i;				//The new record can be stored in this page
						//Now Calculate the slot number
						if(checkFreeSlot=='N')
						{//There are no free slots available .. so create a new slot and allocate that slot
							rid.slotNum=NoOfSlots;
							handle.WritePage(i,buffer);
							pf->CloseFile(handle);
							return 0;
						}
						else
						{//There is a free slot available .. so find that free slot and allocate the slot
							for(int slotNum=0; slotNum < NoOfSlots ; i++)
							{
								int RecordOffset = *(int*)((char*)buffer + PF_PAGE_SIZE - 10 - slotNum*8);
								//if this is the free slot we are looking for
								if(RecordOffset == -1)
								{
									rid.slotNum=slotNum;
									handle.WritePage(i,buffer);
									pf->CloseFile(handle);
									return 0;
								}
							}
						}
					}
				}
			}
			if(i == (int)NoOfPages)
			{//There are no pages with free space
				FreeSpaceOffset=0;
				NoOfSlots=1;
				*(int*)((char*)buffer + PF_PAGE_SIZE - 4)=FreeSpaceOffset;
				*((char*)buffer+ PF_PAGE_SIZE - 5)='Y';
				*((char*)buffer+ PF_PAGE_SIZE - 6)='N';
				//Slot no is written to the file in this case since this is the first slot and it would return garbage values if the correct value is not written
				*(int*)((char*)buffer+PF_PAGE_SIZE-10)=NoOfSlots;
				if(!handle.AppendPage(buffer))
				{
					rid.pageNum=i;
					rid.slotNum=0;
					pf->CloseFile(handle);
					return 0;
				}
			}
		}
	}
	return -1;
}

