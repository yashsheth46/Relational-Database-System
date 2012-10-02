#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include<string.h>
#include<cstdlib>
#include <string.h>

#include "../pf/pf.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

# define QE_EOF (-1)  // end of the index scan

using namespace std;

typedef enum{ MIN = 0, MAX, SUM, AVG, COUNT } AggregateOp;


// The following functions use  the following 
// format for the passed data.
//    For int and real: use 4 bytes
//    For varchar: use 4 bytes for the length followed by
//                          the characters

struct Value {
    AttrType type;          // type of value               
    void     *data;         // value                       
};


struct Condition {
    string lhsAttr;         // left-hand side attribute                     
    CompOp  op;             // comparison operator                          
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string rhsAttr;         // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};


class Iterator {
    // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;
        virtual void getAttributes(vector<Attribute> &attrs) const = 0;
        virtual ~Iterator() {};
};


class TableScan : public Iterator
{
    // A wrapper inheriting Iterator over RM_ScanIterator
    public:
        RM &rm;
        RM_ScanIterator *iter;
        string tablename;
        vector<Attribute> attrs;
        vector<string> attrNames;
        
        TableScan(RM &rm, const string tablename, const char *alias = NULL):rm(rm)
        {
            // Get Attributes from RM
            rm.getAttributes(tablename, attrs);

            // Get Attribute Names from RM
            unsigned i;
            for(i = 0; i < attrs.size(); ++i)
            {
                // convert to char *
                attrNames.push_back(attrs[i].name);
            }
            // Call rm scan to get iterator
            iter = new RM_ScanIterator();
            rm.scan(tablename, "", NO_OP, NULL, attrNames, *iter);

            // Store tablename
            this->tablename = tablename;
            if(alias) this->tablename = alias;
        };
       
        // Start a new iterator given the new compOp and value
        void setIterator()
        {
            iter->close();
            delete iter;
            iter = new RM_ScanIterator();
            rm.scan(tablename, "", NO_OP, NULL, attrNames, *iter);
        };
       
        RC getNextTuple(void *data)
        {
            RID rid;
            return iter->getNextTuple(rid, data);
        };
        
        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;
            
            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tablename;
                tmp += ".";
                tmp += attrs[i].name;
                attrs[i].name = tmp;
            }
        };
        
        ~TableScan() 
        {
            iter->close();
        };
};


class IndexScan : public Iterator
{
    // A wrapper inheriting Iterator over IX_IndexScan
    public:
        RM &rm;
        IX_IndexScan *iter;
        IX_IndexHandle *handle;
        string tablename;
        vector<Attribute> attrs;
        
        IndexScan(RM &rm, IX_IndexHandle &indexHandle, const string tablename, const char *alias = NULL):rm(rm)
        {
            // Get Attributes from RM
            rm.getAttributes(tablename, attrs);
                     
            // Store tablename
            this->tablename = tablename;
            if(alias) this->tablename = string(alias);
            
            // Store Index Handle
            iter = NULL;
            this->handle = &indexHandle;     //********Will have to do something about it!!
        };
       
        // Start a new iterator given the new compOp and value
        void setIterator(CompOp compOp, void *value)
        {
            if(iter != NULL)
            {
                iter->CloseScan();
                delete iter;
            }
            iter = new IX_IndexScan();
            iter->OpenScan(*handle, compOp, value);
        };
       
        RC getNextTuple(void *data)
        {
            RID rid;
            int rc = iter->GetNextEntry(rid);
            if(rc == 0)
            {
                rc = rm.readTuple(tablename.c_str(), rid, data);
            }
            return rc;
        };
        
        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tablename;
                tmp += ".";
                tmp += attrs[i].name;
                attrs[i].name = tmp;
            }
        };
        
        ~IndexScan() 
        {
            iter->CloseScan();
        };
};


class Filter : public Iterator {
    // Filter operator
    public:
	Condition fCondition;
	Iterator *fInput;
	vector<Attribute> fAttrs;

	Filter(Iterator *input,const Condition &condition)              // Iterator of input R // Selection condition
	{
        	fInput=input;
        	if(condition.bRhsIsAttr)
        	{
        		cout<<"Condition is not for Select"<<endl;
        		return;
        	}
        	strcpy((char*)fCondition.lhsAttr.c_str(),condition.lhsAttr.c_str());
        	fCondition.op=condition.op;
        	fCondition.rhsValue.type=condition.rhsValue.type;
        	fCondition.rhsValue.data=condition.rhsValue.data;

        	fInput->getAttributes(fAttrs);
	}

        ~Filter()
        {

        }
        
        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const
        {
        	attrs=fAttrs;
        }
};


class Project : public Iterator {
    // Projection operator
    public:
	vector<Attribute> pAttrNames,projectAttrs;


	Iterator *pInput;

        Project(Iterator *input,const vector<string> &attrNames)           // Iterator of input R // vector containing attribute names
		{
        	Attribute dummy;
        	pInput=input;
        	for(unsigned i=0;i<attrNames.size();i++)
        	{
        		strcpy((char*)dummy.name.c_str(),attrNames[i].c_str());
        		projectAttrs.push_back(dummy);
        	}
        	pInput->getAttributes(pAttrNames);
        	for(unsigned i=0,j=0;i<pAttrNames.size();i++)
        	{
        		if(!strcmp((char*)projectAttrs[j].name.c_str(),pAttrNames[i].name.c_str()))
        		{
        			projectAttrs[j].type=pAttrNames[i].type;
        			projectAttrs[j].length=pAttrNames[i].length;
        			j++;
        		}
        	}
		}
        ~Project()
        {

        }
        
        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const
        {
        	attrs=projectAttrs;
        }
};


class NLJoin : public Iterator {
    // Nested-Loop join operator
    public:
	RC LeftRC, RightRC;
	void *dataLeft,*dataRight;
	Iterator *leftTable;
	TableScan *rightTable;
	Condition joinCondition;
	vector<Attribute> leftAttrNames;
	vector<Attribute> rightAttrNames;
	vector<Attribute> NLAttrNames;
        NLJoin(Iterator *leftIn,                             // Iterator of input R
               TableScan *rightIn,                           // TableScan Iterator of input S
               const Condition &condition,                   // Join condition
               const unsigned numPages)                       // Number of pages can be used to do join (decided by the optimizer)
        {
        	leftTable = leftIn;
        	rightTable = rightIn;
        	if(!condition.bRhsIsAttr)
        	{
        		cout<<"Condition is not for Join"<<endl;
        		return;
        	}
        	joinCondition.bRhsIsAttr = condition.bRhsIsAttr;
        	joinCondition.lhsAttr = condition.lhsAttr;
        	joinCondition.op = condition.op;
        	joinCondition.rhsAttr = condition.rhsAttr;
        	leftTable->getAttributes(leftAttrNames);
        	rightTable->getAttributes(rightAttrNames);
        	for(unsigned i=0; i< leftAttrNames.size(); i++)
        	{
        		NLAttrNames.push_back(leftAttrNames[i]);
        	}

        	for(unsigned i=0; i< rightAttrNames.size(); i++)
        	{
        		NLAttrNames.push_back(rightAttrNames[i]);
        	}
        	dataLeft = calloc(PF_PAGE_SIZE,sizeof(char));
        	dataRight = calloc(PF_PAGE_SIZE,sizeof(char));
        	RightRC = QE_EOF;
        }
        ~NLJoin(){
			free(dataLeft);
			free(dataRight);
        }
        
        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const
        {
        	attrs=NLAttrNames;
        }

};


class INLJoin : public Iterator {
    // Index Nested-Loop join operator
    public:
	RC LeftRC, RightRC;
	void *dataLeft,*dataRight;
	Iterator *leftTable;
	IndexScan *rightTable;
	Condition joinCondition;
	vector<Attribute> leftAttrNames;
	vector<Attribute> rightAttrNames;
	vector<Attribute> INLAttrNames;


        INLJoin(Iterator *leftIn,                               // Iterator of input R
                IndexScan *rightIn,                             // IndexScan Iterator of input S
                const Condition &condition,                     // Join condition
                const unsigned numPages)                         // Number of pages can be used to do join (decided by the optimizer)
        {
        	leftTable = leftIn;
        	rightTable = rightIn;
        	if(!condition.bRhsIsAttr)
        	{
        		cout<<"Condition is not for Join"<<endl;
        		return;
        	}
        	joinCondition.bRhsIsAttr = condition.bRhsIsAttr;
        	joinCondition.lhsAttr = condition.lhsAttr;
        	joinCondition.op = condition.op;
        	joinCondition.rhsAttr = condition.rhsAttr;
        	leftTable->getAttributes(leftAttrNames);
        	rightTable->getAttributes(rightAttrNames);
        	for(unsigned i=0; i< leftAttrNames.size(); i++)
        	{
        		INLAttrNames.push_back(leftAttrNames[i]);
        	}

        	for(unsigned i=0; i< rightAttrNames.size(); i++)
        	{
        		INLAttrNames.push_back(rightAttrNames[i]);
        	}
        	dataLeft = calloc(PF_PAGE_SIZE,sizeof(char));
        	dataRight = calloc(PF_PAGE_SIZE,sizeof(char));
        	RightRC = QE_EOF;
        }
        ~INLJoin()
        {
			free(dataLeft);
			free(dataRight);
        }

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const
        {
            	attrs=INLAttrNames;
        }
};


class HashJoin : public Iterator {
    // Hash join operator
    public:
	Iterator *leftTable, *rightTable;
	unsigned numPgs;
	Condition hCondition;
	vector<Attribute> leftAttrs;
	vector<Attribute> rightAttrs;
	vector<Attribute> hashAttrs;
	void *leftTblPgs[10000], *rightTblPgs[10000];
	unsigned leftPgsPtrs[10000],  rightPgsPtrs[10000];
	RC leftRC;
	RC rightRC;
	PF_Manager *pf_hash;
	PF_FileHandle handle;
	void *dataLeft, *dataRight;

        HashJoin(Iterator *leftIn,                                // Iterator of input R
                 Iterator *rightIn,                               // Iterator of input S
                 const Condition &condition,                      // Join condition
                 const unsigned numPages      )                   // Number of pages can be used to do join (decided by the optimizer)
	{
        	leftTable = leftIn;
        	rightTable = rightIn;
        	numPgs = numPages;
        	if(!condition.bRhsIsAttr)
        		return;

        	pf_hash = PF_Manager::Instance();

        	hCondition.bRhsIsAttr = condition.bRhsIsAttr;
        	hCondition.lhsAttr = condition.lhsAttr;
			hCondition.op = condition.op;
			hCondition.rhsAttr = condition.rhsAttr;

			leftTable->getAttributes(leftAttrs);
        	rightTable->getAttributes(rightAttrs);

        	for(unsigned i=0; i< leftAttrs.size(); i++)
        	{
        		hashAttrs.push_back(leftAttrs[i]);
        	}

        	for(unsigned i=0; i< rightAttrs.size(); i++)
        	{
        		hashAttrs.push_back(rightAttrs[i]);
        	}


        	//Creating Buckets and Hashing the 2 tables once

        	unsigned bucket = 0;
        	for(unsigned i = 0 ; i<numPgs ; i++)
        	{
        		leftTblPgs[i] = calloc(PF_PAGE_SIZE, sizeof(char));
        		leftPgsPtrs[i] = 0;
        	}

        	dataLeft = calloc(PF_PAGE_SIZE, sizeof(char));

        	while((leftRC = leftTable->getNextTuple(dataLeft)) != QE_EOF)  //Partitioning Left Table
        	{
        		unsigned offsetLeft = 0, endofleft = 0;
        		unsigned positionLeft;
        		bool check = false;
        		unsigned temp = -1;

        		for(positionLeft = 0; positionLeft < (int)leftAttrs.size() ; positionLeft++)
        		{
        			if(!strcmp(leftAttrs[positionLeft].name.c_str(),hCondition.lhsAttr.c_str()))
        				check=true;

        			if(leftAttrs[positionLeft].type == TypeInt)
        				endofleft += sizeof(int);
        			else if(leftAttrs[positionLeft].type == TypeReal)
        				endofleft += sizeof(float);
        			else if(leftAttrs[positionLeft].type == TypeVarChar)
        			{
        				int length = *(int*)((char*)dataLeft + endofleft);
        				endofleft += sizeof(int) + length;
        			}
        			if(!check)
        			{
        				offsetLeft=endofleft;	//Assign offsetLeft to the matching attribute
        				temp = positionLeft;
        			}
        		}

        		positionLeft = temp + 1;  //***********Ambiguous

        		if(leftAttrs[positionLeft].type == TypeInt)
					bucket = *(int*)((char*)dataLeft + offsetLeft)%numPgs;

				else if(leftAttrs[positionLeft].type == TypeReal)
					bucket = (int)(*(float*)((char*)dataLeft + offsetLeft))%numPgs;

				else if(leftAttrs[positionLeft].type == TypeVarChar)
					bucket = (toupper((*((char*)dataLeft + offsetLeft + sizeof(int)))) - (int)'A') % numPgs;


				if(leftPgsPtrs[bucket] + endofleft + sizeof(short int) >= PF_PAGE_SIZE)
				{
					char *name = (char*)malloc(20);
					memcpy(name, "leftBucket_", 11);
					name[11] = '\0';
					char *num = (char*)malloc(5);
					itoa(bucket, num, 10);
					strcat(name, num);

					if(!pf_hash->FileExists(name))
						pf_hash->CreateFile(name);

					pf_hash->OpenFile(name, handle);
					handle.AppendPage(leftTblPgs[bucket]);
					leftPgsPtrs[bucket] = 0;
					pf_hash->CloseFile(handle);

					free(num);
					free(name);
				}
				memcpy((char*)leftTblPgs[bucket] + leftPgsPtrs[bucket], dataLeft, endofleft);
				leftPgsPtrs[bucket] += endofleft;
				//Storing the free space pointer for the page in the last 2 bytes of the page
				*(short int*)((char*)leftTblPgs[bucket] + PF_PAGE_SIZE - sizeof(short int)) = (short int)leftPgsPtrs[bucket];
			}
        	//write all the in memory buffers to files
        	for(unsigned i=0; i<numPgs; i++)
        	{
        		if(leftPgsPtrs[i]>0)
        		{
					char *name = (char*)malloc(20);
					memcpy(name, "leftBucket_", 11);
					name[11] = '\0';
					char *num = (char*)malloc(5);
					itoa(i, num, 10);
					strcat(name, num);

					if(!pf_hash->FileExists(name))
						pf_hash->CreateFile(name);

					pf_hash->OpenFile(name, handle);
					handle.AppendPage(leftTblPgs[bucket]);
					leftPgsPtrs[bucket] = 0;
					pf_hash->CloseFile(handle);

					free(num);
					free(name);
        		}
        	}

//        	free(dataLeft);
        	for(unsigned i = 0 ; i<numPgs ; i++)
        		free(leftTblPgs[i]);

        	//Do the same thing for the right table
        	for(unsigned i = 0 ; i<numPgs ; i++)
        	{
        		rightTblPgs[i] = calloc(PF_PAGE_SIZE, sizeof(char));
        		rightPgsPtrs[i] = 0;
        	}

        	dataRight = calloc(PF_PAGE_SIZE, sizeof(char));

        	while((rightRC = rightTable->getNextTuple(dataRight)) != QE_EOF)  //Partitioning right Table
        	{
        		unsigned offsetRight = 0, endofright = 0;
        		unsigned positionRight;
        		bool check = false;
        		unsigned temp = -1;

        		for(positionRight = 0; positionRight < (int)rightAttrs.size() ; positionRight++)
        		{
        			if(!strcmp(rightAttrs[positionRight].name.c_str(),hCondition.rhsAttr.c_str()))
        				check=true;

        			if(rightAttrs[positionRight].type == TypeInt)
        				endofright += sizeof(int);
        			else if(rightAttrs[positionRight].type == TypeReal)
        				endofright += sizeof(float);
        			else if(rightAttrs[positionRight].type == TypeVarChar)
        			{
        				int length = *(int*)((char*)dataRight + endofright);
        				endofright += sizeof(int) + length;
        			}
        			if(!check)
        			{
        				offsetRight=endofright;	//Assign offsetLeft to the matching attribute
        				temp = positionRight;
        			}
        		}

        		positionRight = temp + 1;  //**********Ambiguous

        		if(rightAttrs[positionRight].type == TypeInt)
					bucket = *(int*)((char*)dataRight + offsetRight)%numPgs;

				else if(rightAttrs[positionRight].type == TypeReal)
					bucket = (int)(*(float*)((char*)dataRight + offsetRight))%numPgs;

				else if(rightAttrs[positionRight].type == TypeVarChar)
					bucket = (toupper((*((char*)dataRight + offsetRight + sizeof(int)))) - (int)'A') % numPgs;


				if(rightPgsPtrs[bucket] + endofright + sizeof(short int) >= PF_PAGE_SIZE)
				{
					char *name = (char*)malloc(20);
					memcpy(name, "rightBucket_", 11);
					name[11] = '\0';
					char *num = (char*)malloc(5);
					itoa(bucket, num, 10);
					strcat(name, num);
					if(!pf_hash->FileExists(name))
						pf_hash->CreateFile(name);

					pf_hash->OpenFile(name, handle);
					handle.AppendPage(rightTblPgs[bucket]);
					rightPgsPtrs[bucket] = 0;
					pf_hash->CloseFile(handle);

					free(num);
					free(name);
				}
				memcpy((char*)rightTblPgs[bucket] + rightPgsPtrs[bucket], dataRight, endofright);
				rightPgsPtrs[bucket] += endofright;
				//Storing the free space pointer for the page in the last 2 bytes of the page
				*(short int*)((char*)rightTblPgs[bucket] + PF_PAGE_SIZE - sizeof(short int)) = (short int)rightPgsPtrs[bucket];


			}
        	for(unsigned i=0; i<numPgs; i++)
        	{
        		if(rightPgsPtrs[i]>0)
        		{
					char *name = (char*)malloc(20);
					memcpy(name, "rightBucket_", 11);
					name[11] = '\0';
					char *num = (char*)malloc(5);
					itoa(i, num, 10);
					strcat(name, num);

					if(!pf_hash->FileExists(name))
						pf_hash->CreateFile(name);

					pf_hash->OpenFile(name, handle);
					handle.AppendPage(rightTblPgs[bucket]);
					rightPgsPtrs[bucket] = 0;
					pf_hash->CloseFile(handle);

					free(num);
					free(name);
        		}
        	}

        	free(dataRight);
        	for(unsigned i = 0 ; i<numPgs ; i++)
        		free(rightTblPgs[i]);

	}
        
        ~HashJoin()
        {
        	free(dataLeft);
        	free(dataRight);
        }

        RC getNextTuple(void *data);

        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const
        {
        	attrs = hashAttrs;
        }
};


class Aggregate : public Iterator {
    // Aggregation operator
    public:
        Aggregate(Iterator *input,                              // Iterator of input R
                  Attribute aggAttr,                            // The attribute over which we are computing an aggregate
                  AggregateOp op                                // Aggregate operation
        );

        // Extra Credit
        Aggregate(Iterator *input,                              // Iterator of input R
                  Attribute aggAttr,                            // The attribute over which we are computing an aggregate
                  Attribute gAttr,                              // The attribute over which we are grouping the tuples
                  AggregateOp op                                // Aggregate operation
        )
        {

        }

        ~Aggregate()
        {

        }
        
        RC getNextTuple(void *data) {return QE_EOF;};
        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrname = "MAX(rel.attr)"
        void getAttributes(vector<Attribute> &attrs) const;
};

#endif
