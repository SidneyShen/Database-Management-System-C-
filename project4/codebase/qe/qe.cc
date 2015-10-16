
#include "qe.h"

#include <math.h>
#include <string.h>
#include <iostream>
#include <string>
#include <vector>


void printAttribute(Attribute attr, void* data){
	vector<Attribute> attrDescriptor;
	attrDescriptor.push_back(attr);
	RecordBasedFileManager::instance()->printRecord(attrDescriptor, data);
}


/*
This function return one attribute data (null-descriptore + data).
 */
/*void readOneAttributeByID(const void* recordData, int attrID, void* tempAttrData, vector<Attribute> attrs){
	int nullDescriptorLenInByte = (int)(ceil)(attrs.size()/8.0);
	// int pos_byte = (attrID-1)/8;
	// int pos_bit = (attrID-1)%8;

	char *curData = (char*) recordData;
	char *nullDescriptor = (char*) recordData;

	curData += nullDescriptorLenInByte;

	for(int i=0; i<(int)attrs.size(); i++){
		int pos = i/8;
        int j = i%8;
        Attribute attribute = attrs[i];

		if(i == attrID){
			if(*(nullDescriptor+pos) & (1<<(7-j))){ // The corresponding bit is not "0", means NULL
				*(char*)tempAttrData = 0x80;
			}
			else{
				*(char*)tempAttrData = 0x00;
				int len = 0;
				if(attribute.type == TypeVarChar){
					len += *(int*)curData;
				}
				len += sizeof(int);
				memcpy((char*)tempAttrData + sizeof(char), curData, len);
				// printAttribute(attribute, tempAttrData);
			}
			break;
		}
		else{	// have not reached attID
			if(*(nullDescriptor+pos) & (1<<(7-j))){
				continue;
			}
			else{
				if(attribute.type == TypeVarChar){
					int varCharLength = *(int*)curData;
					curData += varCharLength;
				}
				curData += sizeof(int);
			}
		}

	}

}*/

void readOneAttributeByName(const void* recordData, string attrName, void* tempAttrData, vector<Attribute> attrs){
	int nullDescriptorLenInByte = (int)(ceil)(attrs.size()/8.0);

	char *curData = (char*) recordData;
	char *nullDescriptor = (char*) recordData;

	curData += nullDescriptorLenInByte;

	for(int i=0; i<(int)attrs.size(); i++){
		int pos = i/8;
        int j = i%8;
        Attribute attribute = attrs[i];

		if(attrs[i].name == attrName){
			if(*(nullDescriptor+pos) & (1<<(7-j))){ // The corresponding bit is not "0", means NULL
				*(char*)tempAttrData = 0x80;
			}
			else{
				*(char*)tempAttrData = 0x00;
				int len = 0;
				if(attribute.type == TypeVarChar){
					len += *(int*)curData;
				}
				len += sizeof(int);
				memcpy((char*)tempAttrData + sizeof(char), curData, len);
				// printAttribute(attribute, tempAttrData);
			}
			break;
		}
		else{	// have not reached attID
			if(*(nullDescriptor+pos) & (1<<(7-j))){
				continue;
			}
			else{
				if(attribute.type == TypeVarChar){
					int varCharLength = *(int*)curData;
					curData += varCharLength;
				}
				curData += sizeof(int);
			}
		}

	}

	// Print this attribute:
	

}


bool checkAttribute(void* left,void* right, CompOp compOp, AttrType type){
    if(compOp == NO_OP)
        return true;

	if((*(char*)left ^ 0x80) == 0)    // The left attribute is null.
        return false;
    if((*(char*)right ^ 0x80) == 0)    // The right attribute is null.
        return false;

    int result;
    switch (type) {
        case TypeVarChar:{   
                // cout<<"checking varchar now!!!!!!!!!!!!!!!"<<endl;
                int leftLength = *(int*)((char*)left + sizeof(char));  // Length without head.
                string leftStr = string((char*)((char*)left + sizeof(int)+ sizeof(char)),leftLength);
                int rightLength = *(int*)((char*)right + sizeof(char));
                string rightStr = string((char*)((char*)right + sizeof(int)+ sizeof(char)), rightLength);  // Skip null descriptor (one byte) and varchar head (one int)
                result = leftStr.compare(rightStr);
                // cout<<"stored compared string: "<<valueStr<<endl;
                // cout<<"attribute string: "<<attrstr<<endl;
                break;
            }
        case TypeInt:
            result = *((int*)((char*)left + sizeof(char))) - *((int*)((char*)right + sizeof(char))); 
            break;
        case TypeReal:
            float temp = *((float*) ((char*)left + sizeof(char))) - *((float *) ((char*)right + sizeof(char)));
			result = 0;
			if(temp < -0.000001)
				result = -1;
			if(temp > 0.000001)
				result = 1;
			break;
    }

    switch (compOp){
        case NO_OP:
            return true;
            break;
        case EQ_OP:
            // cout<<"in EQ_OP result = "<<result<<endl;
            return(result == 0);
            break;
        case LT_OP:
            return(result < 0);
            break;
        case GT_OP:
            return(result > 0);
            break;
        case LE_OP:
            return (result <= 0);
            break;
        case GE_OP:
            return (result >= 0);
            break;
        case NE_OP:
            return (result != 0);
            break;
    }
    return false;
}





////////////////////////////////// Filter ////////////////////////////////


bool checkRecord(const vector<Attribute> &attrs, const Condition &cond, void* recordData){
	if(cond.op == NO_OP)
		return true;

	int leftAttrID;
	// int rightAttrID;

	for(leftAttrID=0; leftAttrID<(int)attrs.size(); leftAttrID++){
		if(attrs[leftAttrID].name == cond.lhsAttr)
			break;
	}
	// for(rightAttrID=0; rightAttrID<(int)attrs.size(); rightAttrID++){
	// 	if(attrs[rightAttrID].name == cond.lhsAttr)
	// 		break;
	// }

	void *lhs = malloc(200);	// With Null-Descriptor
	memset(lhs,0,200);
	void *rhs = malloc(200);	// With Null-Descriptor
	memset(rhs,0,200);

	readOneAttributeByName(recordData, cond.lhsAttr, lhs, attrs);
	if(cond.bRhsIsAttr)
		readOneAttributeByName(recordData, cond.rhsAttr, rhs, attrs);
	else{	// use the cond.value
		// Set rhs First Byte (Null Descriptor) to be 0x00
		*((char*)rhs) = 0x00;
		int len = sizeof(int);
		if(cond.rhsValue.type == TypeVarChar)
			len += *(int*) ((char*)cond.rhsValue.data);
		memcpy((char*)rhs + sizeof(char), cond.rhsValue.data, len);
	}

	bool ret = checkAttribute(lhs, rhs, cond.op, attrs[leftAttrID].type);
	free(lhs);
	free(rhs);
	return ret;
}

RC Filter::getNextTuple(void *data){
	vector<Attribute> attrs;
	getAttributes(attrs);
	while(iter_filter->getNextTuple(data)!=-1){
		if(checkRecord(attrs,cond,data))
			return 0;
	}
	return QE_EOF;
}

void Filter::getAttributes(vector<Attribute> &attrs) const{
	iter_filter->getAttributes(attrs);
}

////////////////////////////////////////////////////////////////////////////



//////////////////////////////////// Project ///////////////////////////////


int concatenateData(void* attrData, Attribute attr, int &offset, int attrCount, void* data){
    // cout<<"In concatenateData-----------------"<<endl;
    int pos_byte = (attrCount-1)/8; // [0, nullDescriptorLen-1]
    // cout<<"pos_byte = "<<pos_byte<<endl;
    int pos_bit = (attrCount-1)%8;  // [0,7]
    // cout<<"pos_bit = "<<pos_bit<<endl;
    // printf("first byte of attrData = %#x \n", *(char*)attrData);
    if((*(char*)attrData ^ 0x80) == 0){
        *((char*)data+pos_byte) |= (1<<(7-pos_bit));     // if null, set corresponding bit to 1
        // cout<<"CHECK NULL!!!!"<<*((char*)data+pos_byte)<<endl;
    }else{
        *((char*)data+pos_byte) &= ~(1<<(7-pos_bit));
    }
    // cout<<"first byte of data = "<<*(char*)data<<endl;
    // printf("first byte of data = %#x \n", *(char*)data);
    int len;
    if((*(char*)attrData ^ 0x80) == 0)
        len = 0;
    else{
        switch(attr.type){
            case TypeVarChar:
                len = sizeof(int) + *(int*) ((char*)attrData + sizeof(char));
                break;
            case TypeInt:
                len = sizeof(int);
                // cout<<"attribute = "<<*(int*)((char*)attrData + sizeof(char))<<endl;
                break;
            case TypeReal:
                len = sizeof(float);
                // cout<<"attribute = "<<*(float*)((char*)attrData + sizeof(char))<<endl;
                break;
        }
    }
    memcpy((char*)data+offset, (char*)attrData + sizeof(char), len);
/*    switch(attr.type){
        case TypeVarChar:
            len = sizeof(int) + *(int*) ((char*)attrData + sizeof(char));
            break;
        case TypeInt:
            cout<<"attribute' = "<<*(int*)((char*)data+offset)<<endl;
            break;
        case TypeReal:
            cout<<"attribute' = "<<*(float*)((char*)data+offset)<<endl;
            break;
    }*/
    offset += len;
    return 0;
}

int constructProjectData(void* recordData, void* data, vector<string> &projAttrNames, vector<Attribute> attributes){
	// cout<<"In constructProjectData()---------------------------"<<endl;

	unsigned i,j;
	int nullDescriptorLenInBit = 0;

	for(i=0; i<projAttrNames.size(); i++){
		for(j=0; j<attributes.size(); j++){
			if(projAttrNames[i]==attributes[j].name){
				nullDescriptorLenInBit++;
				break;
			}
		}
	}
	// cout<<"nullDescriptorLenInBit = "<<nullDescriptorLenInBit<<endl;
	int nullDescriptorLen = (int)(ceil)(nullDescriptorLenInBit/8.0);
    int curAttrCount = 0;   // used as a marker which bit should to be set
    void* tempAttrData = malloc(200);
    memset(tempAttrData,0,200);
    int offset = nullDescriptorLen;    // used as the position offset for next memcpy
    // cout<<"nullDescriptorLen = "<<nullDescriptorLen<<endl;

    for(unsigned i=0; i<projAttrNames.size(); i++){
        for(unsigned j=0; j<attributes.size(); j++){
            if(projAttrNames[i] == attributes[j].name){
                curAttrCount++;
                // cout<<"j = "<<j<<endl;
                readOneAttributeByID(recordData, j, tempAttrData, attributes);
                concatenateData(tempAttrData, attributes[j], offset, curAttrCount, data);
                break;
            }
        }
    }

    free(tempAttrData);
    return 0;
}



RC Project::getNextTuple(void *data){
	// cout<<"In Project::getNextTuple----------------------"<<endl;
	void* tempRecordData = malloc(PAGE_SIZE);
	memset(tempRecordData,0,PAGE_SIZE);

	if(iter_proj->getNextTuple(tempRecordData)!=0){
		free(tempRecordData);
		return QE_EOF;
	}

	constructProjectData(tempRecordData, data, projAttrNames, attributes);

	free(tempRecordData);
	return 0;
}

void Project::getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	unsigned i,j;

	for(i=0; i<projAttrNames.size(); i++){
		for(j=0; j<attributes.size(); j++){
			if(projAttrNames[i]==attributes[j].name){
				attrs.push_back(attributes[j]);
				break;
			}
		}
	}
}

////////////////////////////////////////////////////////////////////////////



//////////////////////////////////// BNLJoin ///////////////////////////////



bool checkEqual(void* data1, void* data2, vector<Attribute> &attr1, vector<Attribute> &attr2, string &keyName1, string &keyName2, AttrType joinKeyType){
	// cout<<"In checkEquall()---------------------------------------"<<endl;
	void * key1 = malloc(200);
	void * key2 = malloc(200);
	memset(key1,0,200);
	memset(key2,0,200);

	readOneAttributeByName(data1, keyName1, key1, attr1);
	readOneAttributeByName(data2, keyName2, key2, attr2);

	return checkAttribute(key1, key2, EQ_OP, joinKeyType);
}

void generateJoinedTuple(void * left, void * right, vector<Attribute> &leftAttr, vector<Attribute> &rightAttr, void* tuple){
	// cout<<"In generateJoinedTuple()-------------------------"<<endl;

	int nullDescriptorLenInByte = (int)(ceil)((leftAttr.size()+rightAttr.size())/8.0);
	int curAttrCount = 0;
    void* tempAttrData = malloc(200);
    memset(tempAttrData,0,200);
    int offset = nullDescriptorLenInByte;
    memset(tuple,0,nullDescriptorLenInByte);
    // Add left data
    for(int i=0; i<(int)leftAttr.size(); i++){
    	curAttrCount++;
    	readOneAttributeByID(left, i, tempAttrData, leftAttr);
        concatenateData(tempAttrData, leftAttr[i], offset, curAttrCount, tuple);
    }
    // Add right data
    for(int i=0; i<(int)rightAttr.size(); i++){
    	curAttrCount++;
    	readOneAttributeByID(right, i, tempAttrData, rightAttr);
        concatenateData(tempAttrData, rightAttr[i], offset, curAttrCount, tuple);
    }

    free(tempAttrData);
}


void BNLJoin::freeCurrentBlock(){

	while(block.size()!=0){
		free(block.front());
		block.erase(block.begin());
	}
}

RC BNLJoin::loadNextBlock(){

	freeCurrentBlock();

	void *lhsTupleData = malloc(PAGE_SIZE);
	memset(lhsTupleData, 0, PAGE_SIZE);
	
	if(leftIn->getNextTuple(lhsTupleData) == QE_EOF){
		free(lhsTupleData);
		return QE_EOF;
	}

	block.push_back(lhsTupleData);

	for(unsigned i=1; i<numRecords; i++){

		lhsTupleData = malloc(PAGE_SIZE);
		memset(lhsTupleData, 0, PAGE_SIZE);	

		if(leftIn->getNextTuple(lhsTupleData) == QE_EOF){
			free(lhsTupleData);
			break;
		}

		block.push_back(lhsTupleData);
	}

	blockIterator = 0;
	return 0;
}

RC BNLJoin::getNextTuple(void *data){

	// cout<<"In BNLJoin::getNextTuple()----------------------"<<endl;
	if(isFirstCall)	{
		memset(rhsTupleData, 0, PAGE_SIZE);
		if((loadNextBlock() == QE_EOF) || (rightIn->getNextTuple(rhsTupleData) == QE_EOF)){
			return QE_EOF;
		}
	}

	if(block.size()==0)
		return QE_EOF;

	do{
		if(blockIterator == block.size()){	// Current block is done!

			memset(rhsTupleData,0,PAGE_SIZE);
			if(rightIn->getNextTuple(rhsTupleData) == QE_EOF){	// Inner table has scanned over.

				if(loadNextBlock() == QE_EOF){	// All the blocks have done!
					return QE_EOF;
				}
				else{
					rightIn->setIterator();
					rightIn->getNextTuple(rhsTupleData);
				}
			}
			blockIterator = 0;	// Reset the iterator;
		}

		lhsTupleData = block[blockIterator]; // Read next block tuple
		blockIterator++;

	}while(!checkEqual(lhsTupleData,rhsTupleData,
		lhsAttributes,rhsAttributes, condition.lhsAttr, condition.rhsAttr, joinKeyType));

	generateJoinedTuple(lhsTupleData,rhsTupleData,lhsAttributes,rhsAttributes,data);

/*	vector<Attribute> joinTupleAttributes;
	getAttributes(joinTupleAttributes);
	RecordBasedFileManager::instance()->printRecord(joinTupleAttributes, data);*/


	isFirstCall = false;

	return 0;
}



// For attribute in vector<Attribute>, name it as rel.attr
void BNLJoin::getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	leftIn->getAttributes(attrs);
	vector<Attribute> v;
	rightIn->getAttributes(v);
	for(unsigned i=0; i<v.size(); i++)
		attrs.push_back(v[i]);
}

////////////////////////////////////////////////////////////////////////////




//////////////////////////////////// INLJoin ///////////////////////////////
///

/*bool isAttrNull(void* attrData){
	if((*(char*)attrData ^ 0x80) == 0)
		return true;
	else
		return false;
}*/

// Check wether the joined key is null, if null return false
bool isTupleSatisfied(void * tuple, string attrName, vector<Attribute> attrs){
	void* tempAttrData = malloc(200);
	memset(tempAttrData,0,200);
	readOneAttributeByName(tuple,attrName,tempAttrData,attrs);
	bool ret = !isAttrNull(tempAttrData);
	free(tempAttrData);
	return ret;
}

/*void removeNullDescriptor(void* before, void* after, AttrType type){
	int len = sizeof(int);
	if(type == TypeVarChar)
		len += *(int*)((char*)before + sizeof(char));
	memcpy((char*)after,(char*)before+sizeof(char), len);
    switch(type){
	    case TypeVarChar:{
            int varLen = *(int*) after;  // Length without head.
            string str = string((char*)((char*)after + sizeof(int)),varLen);
            cout<<"attribute string: "<<str<<endl;
            break;
	    }
        case TypeInt:
            cout<<"attribute' = "<<*(int*)((char*)after)<<endl;
            break;
        case TypeReal:
            cout<<"attribute' = "<<*(float*)((char*)after)<<endl;
            break;
    }
	return;
}*/

RC INLJoin::getNextTuple(void *data){
	// cout<<"In INLJoin::getNextTuple()--------------"<<endl;
	if(isFirstCall){
		// cout<<"In fist call--------------------"<<endl;
		// Initialize left table
		if(leftIn->getNextTuple(lhsTupleData) == QE_EOF){
			// cout<<"1"<<endl;
			return QE_EOF;
		}
		while(isTupleSatisfied(lhsTupleData,condition.lhsAttr,lhsAttributes) == false){
			// cout<<"2"<<endl;
			if(leftIn->getNextTuple(lhsTupleData) == QE_EOF){
				// cout<<"3"<<endl;
				return QE_EOF;
			}
		}
		// Initialize right table
		setIndexScanIterator(lhsTupleData);
	}

	// int result = rightIn->getNextTuple(rhsTupleData);
	// cout<<"result = "<<result<<endl;

	isFirstCall = false;

	memset(rhsTupleData,0,PAGE_SIZE);
	if(rightIn->getNextTuple(rhsTupleData) != QE_EOF){
		// cout<<"In IF------------------------"<<endl;
		generateJoinedTuple(lhsTupleData,rhsTupleData,lhsAttributes,rhsAttributes,data);
		vector<Attribute> joinTupleAttributes;
		getAttributes(joinTupleAttributes);
		// RecordBasedFileManager::instance()->printRecord(joinTupleAttributes, data);
		return 0;
	}

	do{
		// cout<<"In DO WHILE------------------------------"<<endl;
		if(leftIn->getNextTuple(lhsTupleData) == QE_EOF)
			return QE_EOF;
		while(isTupleSatisfied(lhsTupleData,condition.lhsAttr,lhsAttributes) == false){
			if(leftIn->getNextTuple(lhsTupleData) == QE_EOF)
				return QE_EOF;
		}
		// Initialize right table
		setIndexScanIterator(lhsTupleData);
	}while(rightIn->getNextTuple(rhsTupleData) == QE_EOF);

	generateJoinedTuple(lhsTupleData,rhsTupleData,lhsAttributes,rhsAttributes,data);
	vector<Attribute> joinTupleAttributes;
	getAttributes(joinTupleAttributes);
	// RecordBasedFileManager::instance()->printRecord(joinTupleAttributes, data);
	return 0;
}

RC INLJoin::setIndexScanIterator(void *lhsTupleData){
	void* tempAttrData = malloc(200);
	memset(tempAttrData,0,200);
	void* key = malloc(200);
	memset(key, 0, 200);

	readOneAttributeByName(lhsTupleData,condition.lhsAttr,tempAttrData,lhsAttributes);
	removeNullDescriptor(tempAttrData,key,joinKeyType);

	rightIn->setIterator(key, key, true, true);

	free(tempAttrData);
	free(key);

	return 0;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	leftIn->getAttributes(attrs);
	vector<Attribute> v;
	rightIn->getAttributes(v);
	for(unsigned i=0; i<v.size(); i++)
		attrs.push_back(v[i]);
}


////////////////////////////////////////////////////////////////////////////




//////////////////////////////////// Aggregate ///////////////////////////////
///


float Aggregate::getBasicInfo(){
	void* tuple = malloc(PAGE_SIZE);
	memset(tuple,0,PAGE_SIZE);
	void* key = malloc(200);
	memset(key,0,200);
	void* keyWithoutNullDescriptor = malloc(200);
	memset(keyWithoutNullDescriptor,0,200);

	vector<Attribute> attrs;
	iter_agg->getAttributes(attrs);

	float sum = 0;
	float max = -99999999;
	float min = 99999999;

	while(iter_agg->getNextTuple(tuple)!=QE_EOF){

		count++;

		readOneAttributeByName(tuple, aggAttr.name, key, attrs);
		if(!isAttrNull(key)){
			returnValNull = false;	// At least one tuple has non-null attribute that can be used to aggregate
			
			removeNullDescriptor(key,keyWithoutNullDescriptor,aggAttr.type);
			float keyValue;
			if(aggAttr.type == TypeInt)
				keyValue = (float)*(int*)((char*)keyWithoutNullDescriptor);
			else if(aggAttr.type == TypeReal)
				keyValue = *(int*)((char*)keyWithoutNullDescriptor);
			sum += keyValue;
			if(keyValue > max)
				max = keyValue;
			if(keyValue < min)
				min = keyValue;
		}
	}
	free(keyWithoutNullDescriptor);
	free(key);
	free(tuple);

	if(op == SUM)
		return sum;
	if(op == AVG)
		return sum / count;
	if(op == MIN)
		return min;
	if(op == MAX)
		return max;
	return 0;
}


void Aggregate::getGroupInfo(){
	// cout<<"In Aggregate::getGroupInfo()----------------"<<endl;
	void* tuple = malloc(PAGE_SIZE);
	memset(tuple,0,PAGE_SIZE);
	void* key = malloc(200);
	memset(key,0,200);
	void* keyWithoutNullDescriptor = malloc(200);
	memset(keyWithoutNullDescriptor,0,200);
	void* aggData = malloc(200);
	memset(aggData,0,200);
	void* aggDataWithoutDescriptor = malloc(200);
	memset(aggDataWithoutDescriptor,0,200);

	vector<Attribute> attrs;
	iter_agg->getAttributes(attrs);

	while(iter_agg->getNextTuple(tuple)!=QE_EOF){
		// cout<<"In While Loop--------------------"<<endl;
		readOneAttributeByName(tuple, groupAttr.name, key, attrs);
		if(!isAttrNull(key)){
			removeNullDescriptor(key,keyWithoutNullDescriptor,groupAttr.type);	// Get the group attribute

			readOneAttributeByName(tuple, aggAttr.name, aggData, attrs);
			removeNullDescriptor(aggData,aggDataWithoutDescriptor,aggAttr.type);
			float aggValue;
			if(aggAttr.type == TypeInt){
				aggValue = (float)*(int*)((char*)aggDataWithoutDescriptor);
				// cout<<"aggValue = "<<aggValue<<endl;
			}
			else if(aggAttr.type == TypeReal){
				aggValue = *(int*)((char*)aggDataWithoutDescriptor);
			}

			if(groupAttr.type == TypeInt){
				int groupKey = *(int*)keyWithoutNullDescriptor;
				// memcpy(groupKeyData, &groupKey, sizeof(int));
				// groupKeyDataLength = sizeof(int);
				// cout<<"groupKey = "<<groupKey<<endl;
				
				
				unordered_map<int,AggVal>::iterator got = map_int.find(groupKey);
				if(got == map_int.end()){
					AggVal * aggStruct = new AggVal();
					aggStruct->count = 1;
					aggStruct->sum += aggValue;
					if(aggStruct->max<aggValue)
						aggStruct->max = aggValue;
					if(aggStruct->min>aggValue)
						aggStruct->min = aggValue;
					map_int.insert(pair<int,AggVal>(groupKey,*aggStruct));
					delete aggStruct;
				}
				else{
					got->second.count = (got->second.count)+1;
					// aggStruct->count = aggStruct-count+1;
					got->second.sum += aggValue;
					if(got->second.max<aggValue)
						got->second.max = aggValue;
					if(got->second.min>aggValue)
						got->second.min = aggValue;
					// cout<<"got->second->count = "<<got->second.count<<endl;
					// cout<<"got->second->sum = "<<got->second.sum<<endl;
					// cout<<"got->second->max = "<<got->second.max<<endl;
					// cout<<"got->second->min = "<<got->second.min<<endl;
				}
			}
			if(groupAttr.type == TypeReal){
				float groupKey = *(float*)keyWithoutNullDescriptor;
				// cout<<"groupKey = "<<groupKey<<endl;
				unordered_map<float,AggVal>::iterator got = map_float.find(groupKey);
				if(got == map_float.end()){
					AggVal * aggStruct = new AggVal();
					aggStruct->count = 1;
					aggStruct->sum += aggValue;
					if(aggStruct->max<aggValue)
						aggStruct->max = aggValue;
					if(aggStruct->min>aggValue)
						aggStruct->min = aggValue;
					map_float.insert(pair<float,AggVal>(groupKey,*aggStruct));
					delete aggStruct;
				}
				else{
					got->second.count = (got->second.count)+1;
					got->second.sum += aggValue;
					if(got->second.max<aggValue)
						got->second.max = aggValue;
					if(got->second.min>aggValue)
						got->second.min = aggValue;
				}
			}
			if(groupAttr.type == TypeVarChar){
				int varLen = *(int*)keyWithoutNullDescriptor;
				string groupKey = string((char*)((char*)keyWithoutNullDescriptor+sizeof(int)),varLen);
				// cout<<"groupKey = "<<groupKey<<endl;
				unordered_map<string,AggVal>::iterator got = map_string.find(groupKey);
				if(got == map_string.end()){
					AggVal * aggStruct = new AggVal();
					aggStruct->count = 1;
					aggStruct->sum += aggValue;
					if(aggStruct->max<aggValue)
						aggStruct->max = aggValue;
					if(aggStruct->min>aggValue)
						aggStruct->min = aggValue;
					map_string.insert(pair<string,AggVal>(groupKey,*aggStruct));
					delete aggStruct;
				}
				else{
					got->second.count = (got->second.count)+1;
					got->second.sum += aggValue;
					if(got->second.max<aggValue)
						got->second.max = aggValue;
					if(got->second.min>aggValue)
						got->second.min = aggValue;
				}
			}
		}
	}
	free(aggDataWithoutDescriptor);
	free(aggData);
	free(keyWithoutNullDescriptor);
	free(key);
	free(tuple);
}




RC Aggregate::getNextTuple(void *data){
	// cout<<"In Aggregate::getNextTuple()------------------"<<endl;
	if(isGroup == false){
		return getNextTupleWithoutGroup(data);
	}
	else{
		return getNextTupleWithGroup(data);
	}

	return QE_EOF;
}


void Aggregate::gotoInt(){
	// cout<<"In Aggregate::gotoInt()--------------"<<endl;
	unordered_map<int,AggVal>::iterator got = map_int.begin();
	int groupKey = got->first;
	aggregateStruct = got->second;
	// cout<<"groupKey = "<<groupKey<<endl;
	memcpy(groupKeyData, &groupKey, sizeof(int));
	groupKeyDataLength = sizeof(int);
	map_int.erase(groupKey);
}

void Aggregate::gotoFloat(){
	// cout<<"In Aggregate::gotoFloat()--------------"<<endl;
	unordered_map<float,AggVal>::iterator got = map_float.begin();
	float groupKey = got->first;
	aggregateStruct = got->second;
	// cout<<"groupKey = "<<groupKey<<endl;
	memcpy(groupKeyData, &groupKey, sizeof(float));
	groupKeyDataLength = sizeof(float);
	map_float.erase(groupKey);
}

void Aggregate::gotoString(){
	// cout<<"In Aggregate::gotoInt()--------------"<<endl;
	unordered_map<string,AggVal>::iterator got = map_string.begin();
	string groupKey = got->first;
	aggregateStruct = got->second;
	// cout<<"groupKey = "<<groupKey<<endl;
	int keyLen = (int)groupKey.length();
	*(int*)(char*)groupKeyData = keyLen;
	for(int i=0; i<keyLen; i++){
		*(char*)((char*)groupKeyData + i + sizeof(int)) = groupKey[i];
	}
	groupKeyDataLength = sizeof(int) + keyLen;
	map_string.erase(groupKey);
}

bool Aggregate::getResult(void *data){
	// cout<<"In Aggregate::getResult()--------------------"<<endl;
	if(aggregateStruct.count <= 0)	// No satisfied tuple;
		return false;

	if(op == COUNT){
		*(char*)data = 0x00;
		memcpy((char*)data + sizeof(char), groupKeyData, groupKeyDataLength);
		*(float*)((char*)data + sizeof(char)+groupKeyDataLength) = (float)aggregateStruct.count;
		return true;
	}
	else{
		*(char*)data = 0x00;
		memcpy((char*)data + sizeof(char), groupKeyData, groupKeyDataLength);
		switch(op){
			case SUM:
				*(float*)((char*)data + sizeof(char)+groupKeyDataLength) = (float)aggregateStruct.sum;
				break;
			case AVG:
				*(float*)((char*)data + sizeof(char)+groupKeyDataLength) = (float)(aggregateStruct.sum/aggregateStruct.count);
				break;
			case MAX:
				*(float*)((char*)data + sizeof(char)+groupKeyDataLength) = (float)aggregateStruct.max;
				break;
			case MIN:
				*(float*)((char*)data + sizeof(char)+groupKeyDataLength) = (float)aggregateStruct.min;
				break;
		}
	}
	return true;

}

RC Aggregate::getNextTupleWithGroup(void *data){
	// cout<<"In Aggregate::getNextTupleWithGroup()-------------------"<<endl;
	if(groupAttr.type == TypeInt){
		if(map_int.empty())
			return QE_EOF;
		else{
			do{
				gotoInt();
				bool result = getResult(data);
				if(result){
					return 0;
				}
			}while(!map_int.empty());
			return QE_EOF;
		}
	}
	if(groupAttr.type == TypeReal){
		if(map_float.empty())
			return QE_EOF;
		else{
			do{
				gotoFloat();
				bool result = getResult(data);
				if(result){
					return 0;
				}
			}while(!map_float.empty());
			return QE_EOF;
		}
	}
	if(groupAttr.type == TypeVarChar){
		if(map_string.empty())
			return QE_EOF;
		else{
			do{
				gotoString();
				bool result = getResult(data);
				if(result){
					return 0;
				}
			}while(!map_string.empty());
			return QE_EOF;
		}
	}
	return QE_EOF;
}


RC Aggregate::getNextTupleWithoutGroup(void *data){
	if(hasReturned == true)	// Has returned the value;
		return QE_EOF;

	if(count <= 0)	// No satisfied tuple;
		return QE_EOF;

	hasReturned = true;

	if(op == COUNT){
		*(char*)data = 0x00;
		*(float*)((char*)data+1) = (float)count;
		return 0;
	}
	else{
		if(returnValNull == true){
			return QE_EOF;
		}
		*(char*)data = 0x00;
		*(float*)((char*)data+1) = (float)returnVal;
	}
	return 0;
}

void Aggregate::getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();

	if(isGroup)
		attrs.push_back(groupAttr);

	Attribute att = aggAttr;
	string op_name;
	switch(op){
		case SUM:
			op_name = "SUM"; break;
		case MIN: 
			op_name = "MIN"; break;
		case MAX:
			op_name = "MAX"; break;
		case AVG:
			op_name = "AVG"; break;
		case COUNT:
			op_name = "COUNT"; break;
	} 

	att.name = op_name + "(" + att.name + ")";

	attrs.push_back(att);
}





// ... the rest of your implementations go here









/*// For 1st TableScan Version
void Aggregate::loadGroupInfo(){
	// cout<<"In Aggregate::loadGroupInfo()----------------"<<endl;
	void* tuple = malloc(PAGE_SIZE);
	memset(tuple,0,PAGE_SIZE);
	void* key = malloc(200);
	memset(key,0,200);
	void* keyWithoutNullDescriptor = malloc(200);
	memset(keyWithoutNullDescriptor,0,200);

	vector<Attribute> attrs;
	iter_group->getAttributes(attrs);

	while(iter_group->getNextTuple(tuple)!=QE_EOF){
		readOneAttributeByName(tuple, groupAttr.name, key, attrs);
		if(!isAttrNull(key)){
			removeNullDescriptor(key,keyWithoutNullDescriptor,groupAttr.type);	

			if(groupAttr.type == TypeInt){
				int groupKey = *(int*)keyWithoutNullDescriptor;
				// cout<<"groupKey = "<<groupKey<<endl;
				set_int.insert(groupKey);
			}
			else if(groupAttr.type == TypeReal){
				float groupKey = *(float*)keyWithoutNullDescriptor;
				set_float.insert(groupKey);
			}
			else if(groupAttr.type == TypeVarChar){
				int varLen = *(int*)keyWithoutNullDescriptor;
				string groupKey = string((char*)((char*)keyWithoutNullDescriptor+sizeof(int)),varLen);
				set_string.insert(groupKey);
			}
		}
	}
	iter_group->setIterator();
	free(keyWithoutNullDescriptor);
	free(key);
	free(tuple);
}*/







/*// For 1st TableScan Version
float Aggregate::getBasicInfoGroup(){
	// cout<<"In Aggregate::getBasicInfoGroup()---------------------"<<endl;
	void* tuple = malloc(PAGE_SIZE);
	memset(tuple,0,PAGE_SIZE);
	void* key = malloc(200);
	memset(key,0,200);
	void* keyWithoutNullDescriptor = malloc(200);
	memset(keyWithoutNullDescriptor,0,200);

	vector<Attribute> attrs;
	iter_group->getAttributes(attrs);

	float sum = 0;
	float max = -99999999;
	float min = 99999999;

	count = 0;
	while(iter_group->getNextTuple(tuple)!=QE_EOF){

		// count++;
		readOneAttributeByName(tuple, groupAttr.name, key, attrs);
		if(!isAttrNull(key)){
			removeNullDescriptor(key,keyWithoutNullDescriptor,groupAttr.type);
			int memCmpLen = 4;
			if(groupAttr.type == TypeVarChar)
				memCmpLen += *(int*)keyWithoutNullDescriptor;
			if(memcmp(groupKeyData, keyWithoutNullDescriptor, memCmpLen) == 0){	// find the tuple that needs to be grouped
				count++;
				// cout<<"count = "<<count<<endl;
				// cout<<"groupKeyData = "<<*(int*)groupKeyData<<endl;
				// cout<<"keyWithoutNullDescriptor = "<<*(int*)keyWithoutNullDescriptor<<endl;

				readOneAttributeByName(tuple, aggAttr.name, key, attrs);
				if(!isAttrNull(key)){
					returnValNull = false;	// At least one tuple has non-null attribute that can be used to aggregate
					
					removeNullDescriptor(key,keyWithoutNullDescriptor,aggAttr.type);
					float keyValue;
					if(aggAttr.type == TypeInt){
						keyValue = (float)*(int*)((char*)keyWithoutNullDescriptor);
						// cout<<"keyValue = "<<keyValue<<endl;
					}
					else if(aggAttr.type == TypeReal)
						keyValue = *(int*)((char*)keyWithoutNullDescriptor);
					sum += keyValue;
					if(keyValue > max)
						max = keyValue;
					if(keyValue < min)
						min = keyValue;
				}
			}

		}

	}
	free(keyWithoutNullDescriptor);
	free(key);
	free(tuple);

	if(op == SUM)
		return sum;
	if(op == AVG)
		return sum / count;
	if(op == MIN)
		return min;
	if(op == MAX)
		return max;
	return 0;
}*/






/*// For 1st TableScan Version
RC Aggregate::getNextTupleWithGroup(void *data){
	// cout<<"In Aggregate::getNextTupleWithGroup()---------------------"<<endl;
	if(groupAttr.type == TypeInt){
		if(set_int.empty())
			return QE_EOF;
		else{
			do{
				gotoInt();
				// getBasicInfoGroup(groupKeyData);
				bool result = getResult(data);
				// cout<<"result = "<<result<<endl;
				if(result){
					gotoReset();
					return 0;
				}
			}while(!set_int.empty());
			return QE_EOF;
		}
	}
	else if(groupAttr.type == TypeReal){
		if(set_float.empty())
			return QE_EOF;
		else{
			do{
				gotoFloat();
				// getBasicInfoGroup(groupKeyData);
				bool result = getResult(data);
				if(result){
					gotoReset();
					return 0;
				}
			}while(!set_float.empty());
			return QE_EOF;
		}
	}
	else if(groupAttr.type == TypeVarChar){
		if(set_string.empty())
			return QE_EOF;
		else{
			do{
				gotoString();
				// getBasicInfoGroup(groupKeyData);
				bool result = getResult(data);
				if(result){
					gotoReset();
					return 0;
				}
			}while(!set_string.empty());
			return QE_EOF;
		}
	}
	return QE_EOF;
}*/














/*// For 1st TableScan Version
void Aggregate::gotoInt(){
	// cout<<"In Aggregate::gotoInt()--------------"<<endl;
	int groupKey = *set_int.begin();
	// cout<<"groupKey = "<<groupKey<<endl;
	memcpy(groupKeyData, &groupKey, sizeof(int));
	groupKeyDataLength = sizeof(int);
	set_int.erase(groupKey);
}
// For 1st TableScan Version
void Aggregate::gotoFloat(){
	float groupKey = *set_float.begin();
	memcpy(groupKeyData, &groupKey, sizeof(float));
	groupKeyDataLength = sizeof(float);
	set_float.erase(groupKey);
}
// For 1st TableScan Version
void Aggregate::gotoString(){
	string groupKey = *set_string.begin();
	int keyLen = (int)groupKey.length();
	*(int*)(char*)groupKeyData = keyLen;
	for(int i=0; i<keyLen; i++){
		*(char*)((char*)groupKeyData + i + sizeof(int)) = groupKey[i];
	}
	groupKeyDataLength = sizeof(int) + keyLen;
	set_string.erase(groupKey);
}*/

/*// For 1st TableScan Version
bool Aggregate::getResult(void *data){
	// cout<<"In Aggregate::getResult()--------------------"<<endl;
	returnVal = getBasicInfoGroup();


	if(count <= 0)	// No satisfied tuple;
		return false;

	if(op == COUNT){
		*(char*)data = 0x00;
		memcpy((char*)data + sizeof(char), groupKeyData, groupKeyDataLength);
		*(float*)((char*)data + sizeof(char)+groupKeyDataLength) = (float)count;
		return true;
	}
	else{
		if(returnValNull == true){
			return false;
		}
		*(char*)data = 0x00;
		memcpy((char*)data + sizeof(char), groupKeyData, groupKeyDataLength);
		*(float*)((char*)data + sizeof(char)+groupKeyDataLength) = (float)returnVal;
	}
	return true;
}*/




/*// For 1st TableScan Version
void Aggregate::gotoReset(){
	// cout<<"In Aggregate::gotoReset()----------------"<<endl;
	iter_group->setIterator();
	returnVal = 0;
	returnValNull = true;
	count = 0;
	memset(groupKeyData,0,200);
	groupKeyDataLength = 0;
}*/

























/*void BNLJoin::initialize(){
    tableName = to_string(tableNameCount);
    tableNameCount++;

    vector<Attribute> leftAttr;
    vector<Attribute> rightAttr;

    leftIn->getAttributes(leftAttr);
    rightIn->getAttributes(rightAttr);

    // Get join-key attribute type
    AttrType joinKeyType;
    for(unsigned i=0; i<leftAttr.size(); i++){
        if(leftAttr[i].name == condition.lhsAttr){
            joinKeyType = leftAttr[i].type;
            break;
        }
    }

    // Create Join Table File
    RelationManager * rm = RelationManager::instance();

    vector<Attribute> joinAttributes;
    getAttributes(joinAttributes);
    string joinTableName = "BNL_"+tableName;

    rm->createTable(joinTableName, joinAttributes);

    void* innerTableData = malloc(PAGE_SIZE);
    memset(innerTableData, 0, PAGE_SIZE);

    while(true){
    	int count = 0;
    	// Load memory
    	for(unsigned i=0; i<numRecords; i++){
    		if(leftIn->getNextTuple(memory[i])!=0)
    			break;
    		count++;
    	}
    	
    	while(rightIn->getNextTuple(innerTableData)==0){
    		for(unsigned j=0; j<count; j++){
    			if(checkEqual(memory[j], innerTableData, leftAttr, rightAttr, condition.lhsAttr, condition.rhsAttr, joinKeyType)){
    				insertTupleIntoJoinTableFile(memory[j],innerTableData, leftAttr, rightAttr, joinTableName);
    			}
    		}
    	}
    	rightIn->setIterator();
    }

    RM_ScanIterator rmsi;
    vector<string> joinAttrNames;
	for(int i=0; i<joinAttributes.size(); i++){
		joinAttrNames.push_back(joinAttributes[i].name);
	}
	rm->scan(joinTableName,"", NO_OP, NULL, joinAttrNames, rmsi);

	free(innerTableData);
	for(int i=0; i<numRecords; i++)	{
		free(memory[i]);
	}
}

RC BNLJoin::getNextTuple(void *data)
{
	RID rid;
	RC rc = rmsi.getNextTuple(rid, data);
	if(rc != 0)
	{
		RelationManager::instance()->deleteTable("BNL_" + tableName);
	}
	return rc;
}*/