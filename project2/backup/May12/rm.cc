
#include "rm.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <iostream>
#include <queue>

using namespace std;
RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
	rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	Attribute table_id = {"table-id", TypeInt, 4};
	Attribute table_name = {"table-name", TypeVarChar, 50};
	Attribute file_name = {"file-name", TypeVarChar, 50};
	tablesDescriptor.push_back(table_id);
	tablesDescriptor.push_back(table_name);
	tablesDescriptor.push_back(file_name);
	Attribute column_name = {"column-name", TypeVarChar, 50};
	Attribute column_type = {"column-type", TypeInt, 4};
	Attribute column_length = {"column-length", TypeInt, 4};
	Attribute column_position = {"column-position", TypeInt, 4};
	columnsDescriptor.push_back(table_id);
	columnsDescriptor.push_back(column_name);
	columnsDescriptor.push_back(column_type);
	columnsDescriptor.push_back(column_length);
	columnsDescriptor.push_back(column_position);
	if(rbfm ->openFile("Tables", fileHandle) == 0){
		MAX_TableID = loadTableID();
		rbfm ->closeFile(fileHandle);
	}
	else{
		MAX_TableID = 0;
		createCatalog();
	}
}



RelationManager::~RelationManager()
{
	_rm = NULL;
}

int RelationManager::loadTableID(){
	int temp, maxID = 0;
	vector<string> attrNames;
	attrNames.push_back("table-id");
	RBFM_ScanIterator scanIterator;
	FileHandle fileHandle;
	rbfm ->openFile("Tables", fileHandle);
	rbfm ->scan(fileHandle, tablesDescriptor, "", NO_OP, NULL, attrNames, scanIterator);
	RID rid;
	void *data = malloc(1 + sizeof(int));
	memset(data, 0, 1 + sizeof(int));
	while(scanIterator.getNextRecord(rid, data) != RBFM_EOF){
		memcpy(&temp, (char *)data + 1, sizeof(int));
		if(temp > maxID)
			maxID = temp;
		memset(data, 0, 1 + sizeof(int));
	}
	free(data);
	return maxID;
}

int RelationManager::calcLength(const vector<Attribute> &descriptor){
	int size = descriptor.size();
	int nullBytes = (int)ceil(size/8.0);
	int length = 0;
	length += nullBytes;
	for(int i = 0; i < size; i ++){
		Attribute attr = descriptor[i];
		length += attr.length;
		if(attr.type == TypeVarChar){
			length += 4;
		}
	}
	return length;
}

queue<unsigned char> RelationManager::initialNullDescriptor(const vector<Attribute> &descriptor){
	queue<unsigned char> res;
	int size = descriptor.size();
	int nullBytes = (int)ceil(size/8.0);
	for(int i = 0; i < nullBytes; i ++){
		res.push(0x00);
	}
	return res;
}



RC RelationManager::createCatalog()
{
	if(initialTablesTable() == -1)
		return -1;
	if(initialColumnsTable() == -1)
		return -1;
    return 0;
}



RC RelationManager::initialTablesTable()
{
	if(rbfm ->createFile("Tables") == -1)
		return -1;
	addTablesColumn("Tables");
	addTablesColumn("Columns");
	return 0;
}

RC RelationManager::addTablesColumn(const string &tableName){
	FileHandle fileHandle;
	RID rid;
	string fileName = tableName;
	MAX_TableID ++;
	rbfm ->openFile("Tables", fileHandle);
	int length = calcLength(tablesDescriptor);
	void *buffer = malloc(length);
	memset(buffer, 0, length);
	int offset = 0;
	queue<unsigned char> nullDescriptor = initialNullDescriptor(tablesDescriptor);

	while(!nullDescriptor.empty()){
		unsigned char temp = nullDescriptor.front();
		memcpy((char *)buffer + offset, &temp, 1);
		offset ++;
		nullDescriptor.pop();
	}

	memcpy((char *)buffer + offset, & MAX_TableID, sizeof(int));
	offset += sizeof(int);
	int tableNameSize = (int)tableName.size();
	memcpy((char *)buffer + offset, &tableNameSize, sizeof(int));
	offset +=sizeof(int);
	memcpy((char *)buffer + offset, tableName.c_str(), tableName.size());
	offset +=tableName.size();
	int fileNameSize = (int)fileName.size();
	memcpy((char *)buffer + offset, &fileNameSize, sizeof(int));
	offset +=sizeof(int);
	memcpy((char *)buffer + offset, fileName.c_str(), fileName.size());
	offset += fileName.size();
	int attrsSize = (int)tablesDescriptor.size();
	memcpy((char *)buffer + offset, &attrsSize, sizeof(int));
	offset +=sizeof(int);
	void *data = malloc(offset);
	memset(data, 0, offset);
	memcpy((char *)data, (char *)buffer, offset);
	free(buffer);
	rbfm ->insertRecord(fileHandle, tablesDescriptor, data, rid);
	rbfm ->closeFile(fileHandle);
	free(data);

	return 0;
}

RC RelationManager::initialColumnsTable(){
	if(rbfm ->createFile("Columns") == -1)
		return -1;
	addColumnsColumn("Tables", tablesDescriptor);
	addColumnsColumn("Columns", columnsDescriptor);

	return 0;

}

RC RelationManager::addColumnsColumn(const string &tableName, const vector<Attribute> &attrs){
	FileHandle fileHandle;
	RID rid;
	int tableID;
	if(tableName == "Tables")
		tableID = 1;
	else if(tableName == "Columns")
		tableID = 2;
	else
		tableID = MAX_TableID;
	string fileName = tableName;
	rbfm ->openFile("Columns", fileHandle);
	int length = calcLength(columnsDescriptor);
	void *buffer = malloc(length);
	memset(buffer, 0, length);
	int offset = 0;
	queue<unsigned char> nullDescriptor = initialNullDescriptor(columnsDescriptor);

	for (int i = 0; i < (int)attrs.size(); i++) {
		Attribute attr = attrs[i];
		offset = 0;
		int size = nullDescriptor.size();
		for(int j = 0; j < size; j ++){
			unsigned char temp = nullDescriptor.front();
			memcpy((char *)buffer + offset, &temp, 1);
			offset ++;
			nullDescriptor.pop();
			nullDescriptor.push(temp);
		}
		memcpy((char *)buffer + offset, &tableID, sizeof(int));
		offset += sizeof(int);
		int attrNameSize = (int)attr.name.size();
		memcpy((char *)buffer + offset, &attrNameSize, sizeof(int));
		offset += sizeof(int);
		memcpy((char *)buffer + offset, attr.name.c_str(), attr.name.size());
		offset += attr.name.size();
		int typ = (int)attr.type;
		memcpy((char *)buffer + offset, &typ, sizeof(int));
		offset += sizeof(int);
		int len = (int)attr.length;
		memcpy((char *)buffer + offset, &len, sizeof(int));
		offset += sizeof(int);
		int pos = i + 1;
		memcpy((char *)buffer + offset, &pos, sizeof(int));
		offset += sizeof(int);
		void *data = malloc(offset);
		memset(data, 0, offset);
		memcpy((char *)data, (char *)buffer, offset);
		rbfm ->insertRecord(fileHandle, columnsDescriptor, data, rid);

		free(data);
    }
    free(buffer);
    rbfm ->closeFile(fileHandle);

    return 0;
}

RC RelationManager::deleteCatalog()
{
	FileHandle fileHandle;
	if(rbfm ->openFile("Tables", fileHandle) == -1)
		return -1;
	rbfm ->closeFile(fileHandle);
	rbfm ->destroyFile("Tables");
	if(rbfm ->openFile("Columns", fileHandle) == -1)
		return -1;
	rbfm ->closeFile(fileHandle);
	rbfm ->destroyFile("Columns");
	MAX_TableID = 0;
    return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    if(tableName == "Tables" || tableName == "Columns") {
        return -1;
    }
    string fileName = tableName;
	if(rbfm ->createFile(fileName) == -1)
		return -1;
	addTablesColumn(tableName);
	addColumnsColumn(tableName, attrs);
	return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
	if(tableName == "Tables" || tableName == "Columns") {
        return -1;
    }
	string fileName = tableName;
	FileHandle fileHandle;
	if(rbfm ->openFile(fileName, fileHandle) == -1)
		return -1;
	rbfm -> closeFile(fileHandle);
	if(rbfm ->destroyFile(fileName) == -1)
		return -1;

	RBFM_ScanIterator scanIterator;
	vector<string> attrNames;
	RID rid;
	int tableID = getTableID(tableName);

	rbfm -> openFile("Columns", fileHandle);
	rbfm -> scan(fileHandle, columnsDescriptor, "table-id", EQ_OP, &tableID, attrNames, scanIterator);
	void *data = malloc(calcLength(columnsDescriptor));
	while (scanIterator.getNextRecord(rid, data) != RBFM_EOF) {
		rbfm ->deleteRecord(fileHandle, columnsDescriptor, rid);
	}
	free(data);
	scanIterator.close();
	rbfm ->closeFile(fileHandle);

	rbfm -> openFile("Tables", fileHandle);
	rbfm -> scan(fileHandle, tablesDescriptor, "table-id", EQ_OP, &tableID, attrNames, scanIterator);
	data = malloc(calcLength(tablesDescriptor));
	while (scanIterator.getNextRecord(rid, data) != RBFM_EOF) {
		rbfm ->deleteRecord(fileHandle, tablesDescriptor, rid);
	}
	free(data);
	scanIterator.close();
	rbfm ->closeFile(fileHandle);

    return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	vector <string> attrNames;
	FileHandle fileHandle;
	int tableID = getTableID(tableName);
	RID rid;
	RBFM_ScanIterator scanIterator;

	attrNames.push_back("column-name");
	attrNames.push_back("column-type");
	attrNames.push_back("column-length");
	Attribute attr;
	// cout<<"In RelationManager::getAttributes, Before rbfm ->openFile(Columns, fileHandle)"<<endl;
	rbfm ->openFile("Columns", fileHandle);
	rbfm ->scan(fileHandle, columnsDescriptor, "table-id", EQ_OP, &tableID, attrNames, scanIterator);
	// cout<<"In RelationManager::getAttributes, Scan Columns Done!"<<endl;
	void *data = malloc(1 + sizeof(int) + 50 + 2 * sizeof(int));	//"1" for nulldescriptor, "50" for column-name varchar, "sizeof(int)" for column-type int.
	memset(data, 0, 1 + sizeof(int) + 50 + 2 * sizeof(int));
	// cout<<"In Get Attribute::: Before getNextRecord()!!!!!"<<endl;
	// cout<<endl;
	while (scanIterator.getNextRecord(rid, data) != RBFM_EOF) {
		unsigned char flag = 0;
		memcpy(&flag, (char *)data, 1);
		// cout<<"flag = "<<*(char*)data<<endl;
		if((flag & 0x80) == 0x80 || (flag & 0x40) == 0x40  )
			return -1;
		int nameLength = 0;
		memcpy(&nameLength, (char *) data + 1, sizeof(int));
		// cout<<endl;
		// cout<<"---------------------------------------"<<endl;
		// cout<<"column-name length = "<<nameLength<<endl;
		char *attrName = new char[nameLength+1];
		attrName[nameLength] = '\0';
		memcpy(attrName, (char *) data + 1 + sizeof(int), nameLength);
		attr.name = string(attrName);
		// cout<<"column-name = "<<attr.name<<endl;

		memcpy(&attr.type, (char *) data + 1 + sizeof(int) + nameLength, sizeof(int));
		memcpy(&attr.length, (char *) data + 1 + sizeof(int) + nameLength + sizeof(int), sizeof(int));
		attrs.push_back(attr);
		delete[] attrName;
		// cout<<"------------------------------------------"<<endl;
		memset(data, 0, 1 + sizeof(int) + 50 + 2 * sizeof(int));
	}
	// cout<<"###################################################"<<endl;
	// cout<<"Vector attr content : "<<endl;
	// for(int i=0; i<(int)attrs.size(); i++){
		// cout<<"attribute name = "<<attrs[i].name<<endl;
	// }
	// cout<<"###################################################"<<endl;
	rbfm ->closeFile(fileHandle);
	scanIterator.close();
	free(data);
	return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{	
	// cout<<"In RelationManager::insertTuple: "<<endl;
	if(tableName == "Tables" || tableName == "Columns"){
		return -1;
	}
	string fileName = tableName;
	FileHandle fileHandle;
	vector<Attribute> attrs;
	// cout<<"Before getAttribute()"<<endl;
	getAttributes(tableName, attrs);
	// cout<<"After getAttritute()"<<endl;
	// cout<<"attribute size = "<<attrs.size()<<endl;
	if(rbfm ->openFile(fileName, fileHandle) == -1)
		return -1;
	RC res = rbfm ->insertRecord(fileHandle, attrs, data, rid);
	rbfm ->closeFile(fileHandle);
	if(res == -1)
		return -1;
    return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	if(tableName == "Tables" || tableName == "Columns"){
		return -1;
	}
    string fileName = tableName;
    FileHandle fileHandle;
    vector<Attribute> attrs;
    getAttributes(tableName, attrs);
    if(rbfm ->openFile(fileName, fileHandle) == -1)
    	return -1;
    RC res = rbfm ->deleteRecord(fileHandle, attrs, rid);
    rbfm ->closeFile(fileHandle);
    if (res == -1) {
        return -1;
    }
    return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	if(tableName == "Tables" || tableName == "Columns"){
		return -1;
	}
    string fileName = tableName;
    FileHandle fileHandle;
    vector<Attribute> attrs;
    getAttributes(tableName, attrs);
    if(rbfm ->openFile(fileName, fileHandle) == -1)
    	return -1;
    RC res = rbfm ->updateRecord(fileHandle, attrs, data, rid);
    rbfm ->closeFile(fileHandle);
    if (res == -1) {
        return -1;
    }
    return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	string fileName = tableName;
	FileHandle fileHandle;
	vector<Attribute> attrs;
	if(rbfm ->openFile(fileName, fileHandle) == -1)
		return -1;
	getAttributes(tableName, attrs);
	// cout<<endl;
	// cout<<"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"<<endl;
	// cout<<"Before RBFM readRecord:"<<endl;
	RC res = rbfm ->readRecord(fileHandle, attrs, rid, data);
	// cout<<"After RBFM readRecord:"<<endl;
	// cout<<"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"<<endl;
	rbfm ->closeFile(fileHandle);
	// cout<<"readRecord result = "<<res<<endl;
	// cout<<endl;
	if(res == -1)
		return -1;
	return 0;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	rbfm ->printRecord(attrs, data);
	return 0;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	string fileName = tableName;
	FileHandle fileHandle;
	vector<Attribute> attrs;
    if(rbfm ->openFile(fileName, fileHandle) == -1)
    	return -1;
    getAttributes(tableName, attrs);
    RC res = rbfm ->readAttribute(fileHandle, attrs, rid, attributeName, data);
    rbfm ->closeFile(fileHandle);
    if(res == -1)
    	return -1;
    return 0;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,
      const void *value,
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator){
    FileHandle fileHandle;
    vector<Attribute> attrs;
    // cout<<"tableName = "<<tableName<<endl;
    // cout<<"conditionAttribute = "<<conditionAttribute<<endl;
    if(rbfm ->openFile(tableName,fileHandle) == -1)
    	return -1;
    // cout<<"||||||||||||||||||||||||||||||||||||||||||||||||||||"<<endl;
    // cout<<fileHandle.getNumberOfPages()<<endl;
    // cout<<"||||||||||||||||||||||||||||||||||||||||||||||||||||"<<endl;
    // cout<<endl;
    // cout<<"In RelationManager::scan, After OpenFile, Before getAttributes()."<<endl;
    // cout<<endl;
    getAttributes(tableName,attrs);
 //    cout<<"||||||||||||||||||||||||||||||||||||||||||||||||||||"<<endl;
 //    cout<<fileHandle.getNumberOfPages()<<endl;
 //    cout<<"||||||||||||||||||||||||||||||||||||||||||||||||||||"<<endl;
	// cout<<"###################################################"<<endl;
	// cout<<"Vector attr content : "<<endl;
	// for(int i=0; i<(int)attrs.size(); i++){
	// 	cout<<"attribute name = "<<attrs[i].name<<endl;
	// }
	// cout<<"###################################################"<<endl;

    // cout<<"conditionAttribute = "<<conditionAttribute<<endl;
    // cout<<"Go to fetch all satisfied record with specific attribute : "<<endl;
    // cout<<endl;

    rm_ScanIterator.scan(fileHandle, attrs, conditionAttribute, compOp, value, attributeNames);
    return 0;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
    if(rbfm_ScanIterator.getNextRecord(rid,data)==RM_EOF){
    	// cout<<"In RM_ScanIterator::getNextTuple, Return -1."<<endl;
        return -1;
    }
    return 0;
};

RC RM_ScanIterator::close(){
	return rbfm_ScanIterator.close();
}

RC RM_ScanIterator::scan(FileHandle &fileHandle,
        const vector<Attribute> &recordDescriptor,
        const string &conditionAttribute,
        const CompOp compOp,
        const void *value,
        const vector<string> &attributeNames){
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    // cout<<"In RM Scan---------------------------"<<endl;
    rbfm->scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rbfm_ScanIterator);
    return 0;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
	int tableID = getTableID(tableName);
	addColumnsColumn(tableName, attr, tableID);
    return 0;
}

int RelationManager::getTableID(const string &tableName){
	int tableNameLength = tableName.size();
	void *value = malloc(tableNameLength + sizeof(int));
	memset(value, 0, tableNameLength + sizeof(int));
	memcpy((char *)value , &tableNameLength, sizeof(int));
	memcpy((char *)value + sizeof(int), tableName.c_str(), tableNameLength);

	vector <string> attrNames;
	attrNames.push_back("table-id");
	FileHandle fileHandle;
	RBFM_ScanIterator scanIterator;
	rbfm ->openFile("Tables", fileHandle);
	rbfm ->scan(fileHandle, tablesDescriptor, "table-name", EQ_OP, value, attrNames, scanIterator);
	RID rid;

	void *data = malloc(1 + sizeof(int));
	memset(data, 0, 1 + sizeof(int));
	int tableID;
	while (scanIterator.getNextRecord(rid, data) != RBFM_EOF) {
		unsigned char flag = 0;
		memcpy(&flag, (char *)data, 1);
		if(flag == 0x80)
			return -1;
		memcpy(&tableID, (char *)data + 1, sizeof(int));
		memset(data, 0, 1 + sizeof(int));
	}
	free(data);
	free(value);
	rbfm -> closeFile(fileHandle);
	scanIterator.close();
	return tableID;
}

RC RelationManager::addColumnsColumn(const string &tableName, const Attribute &attr, const int &tableID){
	FileHandle fileHandle;
	RID rid;
	string fileName = tableName;
	rbfm ->openFile("Columns", fileHandle);
	int length = calcLength(columnsDescriptor);
	void *buffer = malloc(length);
	memset(buffer, 0, length);
	int offset = 0;
	queue<unsigned char> nullDescriptor = initialNullDescriptor(columnsDescriptor);

	int size = nullDescriptor.size();
	for(int j = 0; j < size; j ++){
		unsigned char temp = nullDescriptor.front();
		memcpy((char *)buffer + offset, &temp, 1);
		offset ++;
		nullDescriptor.pop();
		nullDescriptor.push(temp);
	}
	memcpy((char *)buffer + offset, &tableID, sizeof(int));
	offset += sizeof(int);
	int attrNameSize = (int)attr.name.size();
	memcpy((char *)buffer + offset, &attrNameSize, sizeof(int));
	offset += sizeof(int);
	memcpy((char *)buffer + offset, attr.name.c_str(), attr.name.size());
	offset += attr.name.size();
	int typ = (int)attr.type;
	memcpy((char *)buffer + offset, &typ, sizeof(int));
	offset += sizeof(int);
	int len = (int)attr.length;
	memcpy((char *)buffer + offset, &len, sizeof(int));
	offset += sizeof(int);
	vector<Attribute> attrs;
	getAttributes(tableName, attrs);
	int pos = attrs.size() + 1;
	memcpy((char *)buffer + offset, &pos, sizeof(int));
	offset += sizeof(int);
	void *data = malloc(offset);
	memset(data, 0, offset);
	memcpy((char *)data, (char *)buffer, offset);
	rbfm ->insertRecord(fileHandle, columnsDescriptor, data, rid);
	free(data);
	free(buffer);
	rbfm ->closeFile(fileHandle);

	return 0;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
	int attrNameLength = attributeName.size();
	void *value = malloc(attrNameLength + sizeof(int));
	memset(value, 0, attrNameLength + sizeof(int));
	memcpy((char *)value , &attrNameLength, sizeof(int));
	memcpy((char *)value + sizeof(int), attributeName.c_str(), attrNameLength);

	int tableID = getTableID(tableName);
	RBFM_ScanIterator scanIterator;
	FileHandle fileHandle;
	vector<string> attrNames;
	attrNames.push_back("table-id");
	attrNames.push_back("column-name");
	attrNames.push_back("column-type");
	attrNames.push_back("column-length");
	attrNames.push_back("column-position");
	rbfm ->openFile("Columns", fileHandle);
	rbfm ->scan(fileHandle, columnsDescriptor, "table-id", EQ_OP, &tableID, attrNames, scanIterator);
	RID rid;

	int length = attributeName.length();
	int columnsDescriptorLength = calcLength(columnsDescriptor);
	void *data = malloc(columnsDescriptorLength);
	char *name = new char[length+1];
	name[length] = '\0';
	memset(data, 0, columnsDescriptorLength);
	while (scanIterator.getNextRecord(rid, data) != RBFM_EOF) {
		memcpy(name, (char *)data + 1 + 2 * sizeof(int), length);
		if(string(name) == attributeName){
			int zero = 0;
			memcpy((char *)data + 1 + 2 * sizeof(int) + length + sizeof(int), &zero, sizeof(int));
			updateColumnsTuple(data, rid);
			free(data);
			free(name);
			return 0;
		}
		memset(data, 0, columnsDescriptorLength);
	}
	free(data);
	free(name);
	return -1;
}

RC RelationManager::updateColumnsTuple(const void *data, const RID &rid)
{
    FileHandle fileHandle;
    if(rbfm ->openFile("Columns", fileHandle) == -1)
    	return -1;
    RC res = rbfm ->updateRecord(fileHandle, columnsDescriptor, data, rid);
    rbfm ->closeFile(fileHandle);
    if (res == -1) {
        return -1;
    }
    return 0;
}
