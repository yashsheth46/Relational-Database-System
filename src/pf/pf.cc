#include "pf.h"

#include <iostream>
#include <fstream>
#include <stdio.h>

PF_Manager* PF_Manager::_pf_manager = 0;
int fileMode=0;

bool PF_Manager::FileExists(const char *fileName)
{
	ifstream chkFile (fileName);
	if (chkFile){
//		cout<<"File "<<fileName<<" exists!\n";
		chkFile.close();
		return true;
	}
	else
		cout<<"File "<<fileName<<" does not exist!\n";
	return false;
}


PF_Manager* PF_Manager::Instance()
{
    if(!_pf_manager)
        _pf_manager = new PF_Manager();
    
    return _pf_manager;    
}


PF_Manager::PF_Manager()
{
}


PF_Manager::~PF_Manager()
{
}

    
//PF_FileHandle& PF_FileHandle::operator=(const PF_FileHandle &rhs)
//{
//	if(this == &rhs)
//	  return *this;
//
//	this->file = rhs.file;
//	return *this;
//}

RC PF_Manager::CreateFile(const char *fileName)
{
	if(FileExists(fileName)){
		cout<<"File already exists.\nNew file not created!\n";
		return -1;
	}

	fstream myFile (fileName, ios::out | ios::binary);
	myFile.close();
	cout<<"File "<<fileName<<" is created!\n";
	return 0;
}


RC PF_Manager::DestroyFile(const char *fileName)
{
	int rc;
	if(FileExists(fileName)){
		rc= remove(fileName);
		if(!rc){
			cout<<"FIle "<<fileName<<" is destroyed!\n";
			return 0;
		}
	}

	cout<<"Cannot destroy file "<<fileName<<endl;
	return -1;
}


RC PF_Manager::OpenFile(const char *fileName, PF_FileHandle &fileHandle)
{
	if (FileExists(fileName)){
		if (fileHandle.file.is_open())
		{
//			cout<<"A file is already open!\n";
			return 0;  //*********************NEW**************
		}
		else{
			fileHandle.file.open(fileName, ios::in|ios::out|ios::binary);
//			cout<<"File "<<fileName<<" is open!\n";
			return 0;
		}
	}
	cout<< "File "<<fileName<<" could not be opened!\n";
	return -1;
}


RC PF_Manager::CloseFile(PF_FileHandle &fileHandle)
{
	if(!fileHandle.file.is_open()){
//		cout<<"File is already closed!\n";
		return -1;
	}
	else
		fileHandle.file.close();

	if(!fileHandle.file.is_open()){
//    	cout<<"File is now closed!\n";
    	return 0;
    }
    else{
    	cout<<"File could not be closed!\n";
    	return -1;
    }
}


PF_FileHandle::PF_FileHandle()
{
}
 

PF_FileHandle::~PF_FileHandle()
{
	if(file.is_open())
		file.close();
}


RC PF_FileHandle::ReadPage(PageNum pageNum, void *data)
{
	long size;
	unsigned pgs= GetNumberOfPages();				//Subtract 1 as this returns actual no. of pages, but our pageNum starts from 0!
	if(file.is_open()){
		file.seekg(0, ios::end);
		size=file.tellg();
		if(pgs==0){
			cout<<"File is empty\n";
			return -1;
		}
		else if(pageNum<pgs){
			file.seekg(pageNum*PF_PAGE_SIZE, ios::beg);
			file.read((char *)data, PF_PAGE_SIZE);
//			cout<<"Page read successfully: \n";
			return 0;
		}
		else{
			cout<<"Reading Failed: Page "<<pageNum<<" does not exist!\n";
			return -1;
		}

	}
	else
		cout<<"Reading Failed: File is not open!\n";
	return -1;
}


RC PF_FileHandle::WritePage(PageNum pageNum, const void *data)
{
	long size;
	unsigned pgs= GetNumberOfPages();
	if(file.is_open()){
		file.seekp(0, ios::end);
		size=file.tellp();							//check if page is greater than existing pages and less than 1st page
		if(pageNum==pgs){
			AppendPage(data);
			return 0;
		}
		else if(pageNum<pgs){
			file.seekp(pageNum*PF_PAGE_SIZE, ios::beg);
			file.write((char *)data, PF_PAGE_SIZE);
//			cout<<"Page "<<pageNum<<" is overwritten!\n";
			return 0;
		}
		else{
			cout<<"Writing Failed: Page "<<pageNum<<" does not exist!\n";
			return -1;
		}
	}
	else
		cout<<"Writing failed: File is not open!\n";
	return -1;
}


RC PF_FileHandle::AppendPage(const void *data)
{
	if(file.is_open()){
//		cout<<"Appending New Page...\n";
		file.seekp(0, ios::end);
		file.write((char *)data, PF_PAGE_SIZE);
		return 0;
	}
	else
		cout<<"Appending failed: File is not open!\n";
		return -1;
}


unsigned PF_FileHandle::GetNumberOfPages()
{
	long length;
	if(file.is_open()){
		file.seekg(0, ios::end);
		length= file.tellg();
//		cout<<"Total file size= "<<length<<" bytes\n";
		if(length%PF_PAGE_SIZE)
			return length/PF_PAGE_SIZE+1;
		else
			return length/PF_PAGE_SIZE;
	}
	else{
		cout<<"File is not open!\n";
		return 0;
	}
}
