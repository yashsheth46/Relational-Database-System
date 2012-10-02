
# include "qe.h"

RC Filter :: getNextTuple(void *data)
{
	bool check;
	while(fInput->getNextTuple(data) != QE_EOF)
	{
		check = false;
		int position=0;
		int offset=0;
		for(position = 0; position < (int)fAttrs.size() ; position++)
		{
			if(!strcmp(fAttrs[position].name.c_str(),fCondition.lhsAttr.c_str()))
				break;
			if(fAttrs[position].type == TypeInt)
				offset += sizeof(int);
			else if(fAttrs[position].type == TypeReal)
				offset += sizeof(float);
			else if(fAttrs[position].type == TypeVarChar)
			{
				int length = *(int*)((char*)data+offset);
				offset += sizeof(int) + length;
			}
		}
		if(fCondition.rhsValue.type == fAttrs[position].type)
		{
			if(fCondition.rhsValue.type == TypeInt)
			{
				check=false;
				switch(fCondition.op)
				{
				case EQ_OP:
					if(*(int*)((char*)data+offset) == *(int*)(fCondition.rhsValue.data))
						  check=true;
					break;
				case LT_OP:
					if(*(int*)((char*)data+offset) < *(int*)(fCondition.rhsValue.data))
						check=true;
					break;
				case GT_OP:
					if(*(int*)((char*)data+offset) > *(int*)(fCondition.rhsValue.data))
						check=true;
					break;
				case LE_OP:
					if(*(int*)((char*)data+offset) <= *(int*)(fCondition.rhsValue.data))
						check=true;
					break;
				case GE_OP:
					if(*(int*)((char*)data+offset) >= *(int*)(fCondition.rhsValue.data))
						check=true;
					break;
				case NE_OP:
					if(*(int*)((char*)data+offset) != *(int*)(fCondition.rhsValue.data))
						check=true;
					break;
				case NO_OP:
					check=true;
					break;
				default:
					break;
				}

				if(check)
					return 0;

				//				if(*(int*)((char*)data+offset) == *(int*)(fCondition.rhsValue.data))
//					return 0;
			}
			else if(fCondition.rhsValue.type == TypeReal)
			{
				check=false;
				switch(fCondition.op)
				{
				  case EQ_OP:
					  if(*(float*)((char*)data+offset) == *(float*)(fCondition.rhsValue.data))
						  check=true;
					  break;
				  case LT_OP:
					  if(*(float*)((char*)data+offset) < *(float*)(fCondition.rhsValue.data))
						  check=true;
					  break;
				  case GT_OP:
					  if(*(float*)((char*)data+offset) > *(float*)(fCondition.rhsValue.data))
						  check=true;
					  break;
				  case LE_OP:
					  if(*(float*)((char*)data+offset) <= *(float*)(fCondition.rhsValue.data))
						  check=true;
					  break;
				  case GE_OP:
					  if(*(float*)((char*)data+offset) >= *(float*)(fCondition.rhsValue.data))
						  check=true;
					  break;
				  case NE_OP:
					  if(*(float*)((char*)data+offset) != *(float*)(fCondition.rhsValue.data))
						  check=true;
					  break;
				  case NO_OP:
					  check=true;
					  break;
				  default:
					  break;
				  }
				if(check)
					return 0;
			}
			else if(fCondition.rhsValue.type == TypeVarChar)
			{
				char *str1,*str2;
				int length = *(int*)((char*)data + offset);
				str1=(char*)calloc(length + 1,sizeof(char));
				memcpy(str1,(char*)data + offset + sizeof(int),length);
				str1[length] = '\0';

				length = *(int*)fCondition.rhsValue.data;
				str2=(char*)calloc(length + 1,sizeof(char));
				memcpy(str2,(char*)fCondition.rhsValue.data + sizeof(int),length);
				str2[length] = '\0';


				check=false;
				  switch(fCondition.op)
				  {
				  case EQ_OP:
					  if(!strcmp(str1,str2))
						  check=true;
					  break;
				  case LT_OP:
					  if(!strcmp(str1,str2) < 0)
						  check=true;
					  break;
				  case GT_OP:
					  if(!strcmp(str1,str2) > 0)
						  check=true;
					  break;
				  case LE_OP:
					  if(!strcmp(str1,str2) <= 0)
						  check=true;
					  break;
				  case GE_OP:
					  if(!strcmp(str1,str2) >= 0)
						  check=true;
					  break;
				  case NE_OP:
					  if(!strcmp(str1,str2) != 0)
						  check=true;
					  break;
				  case NO_OP:
					  check=true;
					  break;
				  default:
					  break;
				  }
				free(str1);
				free(str2);
				if(check)
					return 0;
			}
		}
	}
	return QE_EOF;
}


RC Project :: getNextTuple(void *data)
{
	if(pInput->getNextTuple(data) != QE_EOF)
	{
		int position = 0;
		int offset = 0;
		int j = 0;
		int oldOffset;
		for(;position < (int)pAttrNames.size();position++)
		{
			oldOffset=offset;
			if(pAttrNames[position].type == TypeInt)
				offset += sizeof(int);
			else if(pAttrNames[position].type == TypeReal)
				offset += sizeof(float);
			else if(pAttrNames[position].type == TypeVarChar)
			{
				int length = *(int*)((char*)data+offset);
				offset += sizeof(int) + length;
			}
			if(strcmp(pAttrNames[position].name.c_str(),projectAttrs[j].name.c_str()))
			{
				memmove((char*)data + oldOffset,(char*)data + offset,offset - oldOffset);
				j--;
			}
			j++;
		}
		return 0;
	}
	else
		return QE_EOF;
}

RC NLJoin :: getNextTuple(void *data)
{
	int endofleft = 0;
	int endofright = 0;
	bool check;

	while(true)
	{
		if(LeftRC == QE_EOF)
		{
			RightRC = QE_EOF;
			return LeftRC;
		}
		if(RightRC == QE_EOF)
		{
			leftTable->getNextTuple(dataLeft);
			rightTable->setIterator();
		}
		int positionLeft = 0, offsetLeft = 0;
		int positionRight = 0, offsetRight = 0;
		int temp = -1;

		check = false;
		//Finding the position and offset of the join attribute in the left table
		for(positionLeft = 0; positionLeft < (int)leftAttrNames.size() ; positionLeft++)
		{
			if(!strcmp(leftAttrNames[positionLeft].name.c_str(),joinCondition.lhsAttr.c_str()))
				check=true;

			if(leftAttrNames[positionLeft].type == TypeInt)
				endofleft += sizeof(int);
			else if(leftAttrNames[positionLeft].type == TypeReal)
				endofleft += sizeof(float);
			else if(leftAttrNames[positionLeft].type == TypeVarChar)
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
		positionLeft = temp + 1;  //************Ambiguous

		while((RightRC = rightTable->getNextTuple(dataRight)) != QE_EOF)
		{
			temp=-1;	//change
			check = false;
			//Finding the position and offset of the join attribute in the right table
			for(positionRight = 0; positionRight < (int)rightAttrNames.size() ; positionRight++)
			{
				if(!strcmp(rightAttrNames[positionRight].name.c_str(),joinCondition.rhsAttr.c_str()))
					check = true;
				if(rightAttrNames[positionRight].type == TypeInt)
					endofright += sizeof(int);
				else if(rightAttrNames[positionRight].type == TypeReal)
					endofright += sizeof(float);
				else if(rightAttrNames[positionRight].type == TypeVarChar)
				{
					int length = *(int*)((char*)dataRight + endofright);
					endofright += sizeof(int) + length;
				}
				if(!check)
				{
					temp = positionRight;		//change
					offsetRight = endofright;
				}
			}

			positionRight = temp + 1;	//change

			//Comparing the values of the join attributes in the left and the right table
			if(leftAttrNames[positionLeft].type == rightAttrNames[positionRight].type)
			{

				if(leftAttrNames[positionLeft].type == TypeInt)
				{
					check=false;
					  switch(joinCondition.op)
					  {
					  case EQ_OP:
						  if(*(int*)((char*)dataLeft + offsetLeft) == *(int*)((char*)dataRight + offsetRight))
							  check=true;
						  break;
					  case LT_OP:
						  if(*(int*)((char*)dataLeft + offsetLeft) < *(int*)((char*)dataRight + offsetRight))
							  check=true;
						  break;
					  case GT_OP:
						  if(*(int*)((char*)dataLeft + offsetLeft) > *(int*)((char*)dataRight + offsetRight))
							  check=true;
						  break;
					  case LE_OP:
						  if(*(int*)((char*)dataLeft + offsetLeft) <= *(int*)((char*)dataRight + offsetRight))
							  check=true;
						  break;
					  case GE_OP:
						  if(*(int*)((char*)dataLeft + offsetLeft) >= *(int*)((char*)dataRight + offsetRight))
							  check=true;
						  break;
					  case NE_OP:
						  if(*(int*)((char*)dataLeft + offsetLeft) != *(int*)((char*)dataRight + offsetRight))
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
//						data = realloc(data,endofleft + endofright);//changed dataleft to data
						memmove(data, dataLeft, endofleft);
						memmove((char*)data + endofleft, dataRight, endofright);
						return 0;
					}
				}
				else if(leftAttrNames[positionLeft].type == TypeReal)
				{
					check=false;
					  switch(joinCondition.op)
					  {
					  case EQ_OP:
						  if(*(float*)((char*)dataLeft + offsetLeft) == *(float*)((char*)dataRight + offsetRight))
							  check=true;
						  break;
					  case LT_OP:
						  if(*(float*)((char*)dataLeft + offsetLeft) < *(float*)((char*)dataRight + offsetRight))
							  check=true;
						  break;
					  case GT_OP:
						  if(*(float*)((char*)dataLeft + offsetLeft) > *(float*)((char*)dataRight + offsetRight))
							  check=true;
						  break;
					  case LE_OP:
						  if(*(float*)((char*)dataLeft + offsetLeft) <= *(float*)((char*)dataRight + offsetRight))
							  check=true;
						  break;
					  case GE_OP:
						  if(*(float*)((char*)dataLeft + offsetLeft) >= *(float*)((char*)dataRight + offsetRight))
							  check=true;
						  break;
					  case NE_OP:
						  if(*(float*)((char*)dataLeft + offsetLeft) != *(float*)((char*)dataRight + offsetRight))
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
//						data = realloc(data,endofleft + endofright);//changed dataleft to data
						memmove(data, dataLeft, endofleft);
						memmove((char*)data + endofleft, dataRight, endofright);

						return 0;
					}
				}
				else if(leftAttrNames[positionLeft].type == TypeVarChar)
				{
					char *str1,*str2;
					int length = *(int*)((char*)dataLeft + offsetLeft);
					str1=(char*)calloc(length + 1,sizeof(char));
					memcpy(str1,(char*)data + offsetLeft + sizeof(int),length);
					str1[length] = '\0';

					length = *(int*)((char*)dataRight + offsetRight);
					str2=(char*)calloc(length + 1,sizeof(char));
					memcpy(str2,(char*)dataRight + offsetRight + sizeof(int),length);
					str2[length] = '\0';

					check=false;
					  switch(joinCondition.op)
					  {
					  case EQ_OP:
						  if(!strcmp(str1,str2))
							  check=true;
						  break;
					  case LT_OP:
						  if(!strcmp(str1,str2) < 0)
							  check=true;
						  break;
					  case GT_OP:
						  if(!strcmp(str1,str2) > 0)
							  check=true;
						  break;
					  case LE_OP:
						  if(!strcmp(str1,str2) <= 0)
							  check=true;
						  break;
					  case GE_OP:
						  if(!strcmp(str1,str2) >= 0)
							  check=true;
						  break;
					  case NE_OP:
						  if(!strcmp(str1,str2) != 0)
							  check=true;
						  break;
					  case NO_OP:
						  check=true;
						  break;
					  default:
						  break;
					  }

					free(str1);
					free(str2);
					if(check)
					{
//						data = realloc(data,endofleft + endofright);//changed dataleft to data
						memmove(data, dataLeft, endofleft);
						memmove((char*)data + endofleft, dataRight, endofright);

						return 0;
					}
				}
			}
		}
	}
//Commented below unreachable code
//	free(dataLeft);
//	free(dataRight);
//	return QE_EOF;
}

RC INLJoin :: getNextTuple(void *data)
{
	int endofleft = 0;
	int endofright = 0;
	bool check;

	while(true)
	{
		if(LeftRC == QE_EOF)
		{
			RightRC = QE_EOF;
			return LeftRC;
		}
		if(RightRC == QE_EOF)
			leftTable->getNextTuple(dataLeft);

		int positionLeft = 0, offsetLeft = 0;
		int positionRight = 0, offsetRight = 0;
		int temp = -1;

		check = false;
		//Finding the position and offset of the join attribute in the left table
		for(positionLeft = 0; positionLeft < (int)leftAttrNames.size() ; positionLeft++)
		{
			if(!strcmp(leftAttrNames[positionLeft].name.c_str(),joinCondition.lhsAttr.c_str()))
				check=true;

			if(leftAttrNames[positionLeft].type == TypeInt)
				endofleft += sizeof(int);
			else if(leftAttrNames[positionLeft].type == TypeReal)
				endofleft += sizeof(float);
			else if(leftAttrNames[positionLeft].type == TypeVarChar)
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
		positionLeft = temp + 1;   //************Ambiguous

		if(RightRC == QE_EOF)
			rightTable->setIterator(joinCondition.op, ((char*)dataLeft + offsetLeft));  //cannot think of what to put here ... it should be the value of the left table join attribute

		while((RightRC = rightTable->getNextTuple(dataRight)) != QE_EOF)
		{
			temp = -1;
			check = false;
			//Finding the position and offset of the join attribute in the right table
			for(positionRight = 0; positionRight < (int)rightAttrNames.size() ; positionRight++)
			{
				if(!strcmp(rightAttrNames[positionRight].name.c_str(),joinCondition.rhsAttr.c_str()))
					check = true;
				if(rightAttrNames[positionRight].type == TypeInt)
					endofright += sizeof(int);
				else if(rightAttrNames[positionRight].type == TypeReal)
					endofright += sizeof(float);
				else if(rightAttrNames[positionRight].type == TypeVarChar)
				{
					int length = *(int*)((char*)dataRight + endofright);
					endofright += sizeof(int) + length;
				}
				if(!check)
				{
					temp = positionRight;
					offsetRight = endofright;
				}
			}

			//Comparing the values of the join attributes in the left and the right table
			if(leftAttrNames[positionLeft].type == rightAttrNames[positionRight].type)
			{

				if(leftAttrNames[positionLeft].type == TypeInt)
				{
					check=false;
					  switch(joinCondition.op)
					  {
					  case EQ_OP:
						  if(*(int*)((char*)dataLeft + offsetLeft) == *(int*)((char*)dataRight + offsetRight))
							  check=true;
						  break;
					  case LT_OP:
						  if(*(int*)((char*)dataLeft + offsetLeft) < *(int*)((char*)dataRight + offsetRight))
							  check=true;
						  break;
					  case GT_OP:
						  if(*(int*)((char*)dataLeft + offsetLeft) > *(int*)((char*)dataRight + offsetRight))
							  check=true;
						  break;
					  case LE_OP:
						  if(*(int*)((char*)dataLeft + offsetLeft) <= *(int*)((char*)dataRight + offsetRight))
							  check=true;
						  break;
					  case GE_OP:
						  if(*(int*)((char*)dataLeft + offsetLeft) >= *(int*)((char*)dataRight + offsetRight))
							  check=true;
						  break;
					  case NE_OP:
						  if(*(int*)((char*)dataLeft + offsetLeft) != *(int*)((char*)dataRight + offsetRight))
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
//						data = realloc(data,endofleft + endofright); //changed dataleft to data
						memmove(data, dataLeft, endofleft);
						memmove((char*)data + endofleft, dataRight, endofright);

						return 0;
					}
				}
				else if(leftAttrNames[positionLeft].type == TypeReal)
				{
					check=false;
					  switch(joinCondition.op)
					  {
					  case EQ_OP:
						  if(*(float*)((char*)dataLeft + offsetLeft) == *(float*)((char*)dataRight + offsetRight))
							  check=true;
						  break;
					  case LT_OP:
						  if(*(float*)((char*)dataLeft + offsetLeft) < *(float*)((char*)dataRight + offsetRight))
							  check=true;
						  break;
					  case GT_OP:
						  if(*(float*)((char*)dataLeft + offsetLeft) > *(float*)((char*)dataRight + offsetRight))
							  check=true;
						  break;
					  case LE_OP:
						  if(*(float*)((char*)dataLeft + offsetLeft) <= *(float*)((char*)dataRight + offsetRight))
							  check=true;
						  break;
					  case GE_OP:
						  if(*(float*)((char*)dataLeft + offsetLeft) >= *(float*)((char*)dataRight + offsetRight))
							  check=true;
						  break;
					  case NE_OP:
						  if(*(float*)((char*)dataLeft + offsetLeft) != *(float*)((char*)dataRight + offsetRight))
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
//						data = realloc(data,endofleft + endofright);//changed dataleft to data
						memmove(data, dataLeft, endofleft);
						memmove((char*)data + endofleft, dataRight, endofright);

						return 0;
					}
				}
				else if(leftAttrNames[positionLeft].type == TypeVarChar)
				{
					char *str1,*str2;
					int length = *(int*)((char*)dataLeft + offsetLeft);
					str1=(char*)calloc(length + 1,sizeof(char));
					memcpy(str1,(char*)data + offsetLeft + sizeof(int),length);
					str1[length] = '\0';

					length = *(int*)((char*)dataRight + offsetRight);
					str2=(char*)calloc(length + 1,sizeof(char));
					memcpy(str2,(char*)dataRight + offsetRight + sizeof(int),length);
					str2[length] = '\0';

					check=false;
					  switch(joinCondition.op)
					  {
					  case EQ_OP:
						  if(!strcmp(str1,str2))
							  check=true;
						  break;
					  case LT_OP:
						  if(!strcmp(str1,str2) < 0)
							  check=true;
						  break;
					  case GT_OP:
						  if(!strcmp(str1,str2) > 0)
							  check=true;
						  break;
					  case LE_OP:
						  if(!strcmp(str1,str2) <= 0)
							  check=true;
						  break;
					  case GE_OP:
						  if(!strcmp(str1,str2) >= 0)
							  check=true;
						  break;
					  case NE_OP:
						  if(!strcmp(str1,str2) != 0)
							  check=true;
						  break;
					  case NO_OP:
						  check=true;
						  break;
					  default:
						  break;
					  }

					free(str1);
					free(str2);
					if(check)
					{
//						data = realloc(data,endofleft + endofright);//changed dataleft to data
						memmove(data, dataLeft, endofleft);
						memmove((char*)data + endofleft, dataRight, endofright);

						return 0;
					}
				}
			}
		}
	}

//Commented below unreachable code
//	free(dataLeft);
//	free(dataRight);
//	return QE_EOF;
}


RC HashJoin::getNextTuple(void *data)
{
	for(unsigned i=0; i<numPgs; i++)
	{
		void *leftPage = calloc(PF_PAGE_SIZE,sizeof(char));
		char *name = (char*)malloc(20);
		unsigned npgs = 0;
		memcpy(name, "leftBucket_", 11);
		name[11] = '\0';
		char *num = (char*)malloc(5);
		itoa(i, num, 10);
		strcat(name, num);
		if(pf_hash->FileExists(name))
		{
			pf_hash->OpenFile(name,handle);
			npgs=handle.GetNumberOfPages();
		}
		else
			continue;

//		void* leftBuckets[10];
//		int leftBucketOffsets[10];

		//Here we take num of pages of the hashed files from memory, and make it the base for second hash function!
		for(int i=0;i<(int)npgs;i++)
		{
			leftTblPgs[i] = calloc(PF_PAGE_SIZE, sizeof(char));
			leftPgsPtrs[i]=0;
		}

		for(unsigned j=0; j < npgs; j++)
		{
			PF_FileHandle handleRight;
			handle.ReadPage(j,leftPage);
			int offsetPageLeft=0;
			short int FreeSpaceOffset = *(short int*)((char*)leftPage + PF_PAGE_SIZE - sizeof(short int));

			while(offsetPageLeft <= FreeSpaceOffset)
			{
	       		unsigned offsetLeft = 0, endofleft = 0;
				unsigned positionLeft;
				bool check = false;
				unsigned temp = 0;
				int bucket=0;

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
				positionLeft = temp + 1;

				if(leftAttrs[positionLeft].type == TypeInt)
					bucket = *(int*)((char*)leftPage + offsetPageLeft + offsetLeft)%(numPgs*npgs);
				else if(leftAttrs[positionLeft].type == TypeReal)
					bucket = (int)(*(float*)((char*)leftPage + offsetPageLeft + offsetLeft))%(numPgs*npgs);
				else if(leftAttrs[positionLeft].type == TypeVarChar)
					bucket = (toupper(*((char*)leftPage + offsetPageLeft + offsetLeft + sizeof(int))) - (int)'A') % (numPgs*npgs);

				if(leftPgsPtrs[bucket] + endofleft + sizeof(short int) >= PF_PAGE_SIZE)
				{
					cout<<"We d0o not handle this .. this is due to the disgusting hash function we are using "<<endl;
					return -1;
				}
				memcpy((char*)leftTblPgs[bucket] + leftPgsPtrs[bucket],(char*)leftPage + offsetPageLeft,endofleft);
				leftPgsPtrs[bucket] += endofleft;
				//Storing the free space pointer for the page in the last 2 bytes of the page
				*(short int*)((char*)leftTblPgs[bucket] + PF_PAGE_SIZE - sizeof(short int)) = (short int)leftPgsPtrs[bucket];
				offsetPageLeft += endofleft;
			}

		}

	}

	return QE_EOF;
}
