
#include "rbfm.h"
#include <math.h>
#include <string.h>
#include <iostream>

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    return PagedFileManager::instance()->createFile(fileName.c_str());
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return PagedFileManager::instance()->destroyFile(fileName.c_str()); 
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return PagedFileManager::instance()->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return PagedFileManager::instance()->closeFile(fileHandle);
}

int* RecordBasedFileManager::getPageFreeSpacePointer(void* pageStartPosPointer){
    return (int*)((char*)pageStartPosPointer+PAGE_SIZE-sizeof(int));
}

int RecordBasedFileManager::getPageFreeSpacePos(void* pageStartPosPointer){
    return *getPageFreeSpacePointer(pageStartPosPointer);
}

int* RecordBasedFileManager::getPageSLotCountPointer(void* pageStartPosPointer){
    return (int*)((char*)pageStartPosPointer+PAGE_SIZE-sizeof(int)*2);
}

int RecordBasedFileManager::getPageSlotCount(void* pageStartPosPointer){
    return *getPageSLotCountPointer(pageStartPosPointer);
}

SlotInfo* RecordBasedFileManager::getSlotInfoPointer(void* pageStartPosPointer, int slotID){
    int pos = PAGE_SIZE-sizeof(int)*2-sizeof(SlotInfo)*(slotID+1);
    return (SlotInfo*)((char*)pageStartPosPointer + pos);
}

void RecordBasedFileManager::initNewPageData(void* pageData){
    *getPageFreeSpacePointer(pageData) = 0; // Pointer to the start of free space
    *getPageSLotCountPointer(pageData) = 0; // Slot number
}

void RecordBasedFileManager::insertRecordIntoPage(void* curPageData, const void* recordData, int recordLength, RID &rid){
    // cout<<"In insertRecordIntoPage()"<<endl;
    int freeSpaceStart = getPageFreeSpacePos(curPageData);    //Obtain free space start position
    int slotCount = getPageSlotCount(curPageData);
    void* copyPos = (char*)curPageData + freeSpaceStart;    // Move to the insert position.

    for(int i=0; i<slotCount; i++){
        SlotInfo* slotInfo = getSlotInfoPointer(curPageData, i);
        if(slotInfo->status == 0){  // Find a deleted slot information
            rid.slotNum = i;
            memcpy(copyPos, recordData, recordLength);
            slotInfo->status = 1;   // Set Status to "1"
            slotInfo->slotStartPos = freeSpaceStart;    // Set Start Pos of the record to the slot info
            slotInfo->slotLength = recordLength;    // Set record length to the slot info
            *getPageFreeSpacePointer(curPageData) = freeSpaceStart+recordLength;   // Set Page Information: free space start position.  
            return;
        }
    }
    SlotInfo* slotInfo = getSlotInfoPointer(curPageData, slotCount);
    rid.slotNum = slotCount;
    memcpy(copyPos, recordData, recordLength);
    slotInfo->status = 1;   // Set Status to "1"
    slotInfo->slotStartPos = freeSpaceStart;    // Set Start Pos of the record to the slot info
    slotInfo->slotLength = recordLength;    // Set record length to the slot info
    *getPageFreeSpacePointer(curPageData) = freeSpaceStart+recordLength;   // Set Page Information: free space start position.  
    *getPageSLotCountPointer(curPageData) = slotCount+1;
    return;
}

bool RecordBasedFileManager::canInsert(void* pageData, int recordLength){
    
    int freeSpaceStart = getPageFreeSpacePos(pageData);    //Obtain free space start position
    int slotCount = getPageSlotCount(pageData);

    int freeSpace = PAGE_SIZE - freeSpaceStart - sizeof(int)*2 - sizeof(SlotInfo)*slotCount;

    for(int i=0; i<slotCount; i++){
        SlotInfo* slotInfo = getSlotInfoPointer(pageData, i);
        if(slotInfo->status == 0){
            if(freeSpace >= recordLength)
                return true;
        }
    }

    if(freeSpace>=(int)(recordLength+sizeof(SlotInfo))) //free space + slot information space
        return true;
    return false;
}



int RecordBasedFileManager::addHeaderToRecord(const vector<Attribute> &recordDescriptor, void * newData, const void *rawData){
    char *curData = (char *)newData;
    int fieldCount = (int)recordDescriptor.size();  // get field count
    int nullDescriptorLen = (int)(ceil)(fieldCount/8.0);  //get Null Descriptor Length in bytes;
    unsigned char * nullDescriptor = (unsigned char*) rawData;

    // First 4 bytes: Field Count
    *(int*) curData = fieldCount;
    curData += sizeof(int);

    // Second 4 bytes: Null Descriptor Length
    *(int*) curData = nullDescriptorLen;
    curData += sizeof(int);

    int headerLength = (2 +fieldCount)*sizeof(int);
    int rawDataLength = nullDescriptorLen;

    // Store each field end position
    for(int i=0; i<fieldCount; i++){

        int pos = i/8;
        int j = i%8;
        if(nullDescriptor[pos] & (1<<(8-1-j))){ // The current attribute is NULL
            *(int*) curData = headerLength + rawDataLength;    //Current field end pos = previous position.
            // cout<<recordDescriptor[i].type<<"'s end position is "<<*(int*) curData<<endl;
            curData += sizeof(int);
            // break;
        }
        else{
            if(recordDescriptor[i].type == TypeVarChar){// Current field is varchar and not null!
                rawDataLength += *(int*)((char*)rawData + rawDataLength);  //Plus the varChar first char.
            }
            rawDataLength += sizeof(int);
            *(int*) curData = headerLength + rawDataLength;
            // cout<<recordDescriptor[i].type<<"'s end position is "<<*(int*) curData<<endl;
            curData += sizeof(int);
        }
    }

    memcpy(curData, (char *)rawData, rawDataLength);
    return headerLength + rawDataLength;
}

int RecordBasedFileManager::insertRecordHelper(FileHandle &fileHandle, const void *newData, RID &rid, const int recordLength){
    // cout<<"In insertRecordHelper-------------------"<<endl;

    int lastPage = fileHandle.getNumberOfPages();

    if(lastPage == 0){
        // cout<<"Append first Page!"<<endl;
        void *newPageData = malloc(PAGE_SIZE);
        initNewPageData(newPageData);
        insertRecordIntoPage(newPageData, newData, recordLength, rid);
        fileHandle.appendPage(newPageData);
        rid.pageNum = lastPage;
        free(newPageData);
        return 0;
    }

    bool isInserted = false;
    void *curPageData = malloc(PAGE_SIZE);
    fileHandle.readPage(lastPage-1, curPageData);

    if(canInsert(curPageData, recordLength)){
        // cout<<"Add in last Page!"<<endl;
        insertRecordIntoPage(curPageData, newData, recordLength, rid);
        fileHandle.writePage(lastPage-1, curPageData);
        rid.pageNum = lastPage-1;
        free(curPageData);
        isInserted = true;
    }else{
        free(curPageData);
        for(int i=0; i<lastPage-1; i++){
            curPageData = malloc(PAGE_SIZE);
            fileHandle.readPage(i, curPageData);
            if(canInsert(curPageData, recordLength)){
                // cout<<"Add in Page "<<i<<endl;
                insertRecordIntoPage(curPageData, newData, recordLength, rid);
                fileHandle.writePage(i, curPageData);
                rid.pageNum = i;
                free(curPageData);
                isInserted = true;
                break;
            }else{
                free(curPageData);
            }
        }
    }

    if(!isInserted){
        // cout<<"Append a new page!"<<endl;
        void *newPageData = malloc(PAGE_SIZE);
        initNewPageData(newPageData);
        insertRecordIntoPage(newPageData, newData, recordLength, rid);
        fileHandle.appendPage(newPageData);
        rid.pageNum = lastPage;
        free(newPageData);
    }

    return 0;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    // cout<<"In insertRecord--------------"<<endl;
    void * newData = malloc(PAGE_SIZE);
    int recordLength = addHeaderToRecord(recordDescriptor, newData, data);
    // cout<<"record length: "<<recordLength<<endl;
    insertRecordHelper(fileHandle, newData, rid, recordLength);
    free(newData);
    return 0;
}


void RecordBasedFileManager::removeHeaderFromRecord(char* storedRecord, void* originalRecord){
    // cout<<"In removeHeaderFromRecord()"<<endl;
    int fieldCount = *(int*)storedRecord;
    // cout<<"fieldCount = "<<fieldCount<<endl;
    int headerLength = (2+fieldCount)*sizeof(int);
    // cout<<"headerLength = "<<headerLength<<endl;
    int recordEndPos = *(int*)(storedRecord + headerLength - sizeof(int));
    // cout<<"recordEndPos = "<<recordEndPos<<endl;
    int originalRecordLength = recordEndPos - headerLength;
    // cout<<"originalRecordLength = "<<originalRecordLength<<endl;
    storedRecord += headerLength;
    // unsigned int shouldBeNullDescriptor = (unsigned int)*storedRecord;
    // cout<<"Should be 40 : "<<shouldBeNullDescriptor<<endl;
    memcpy((char*)originalRecord, storedRecord, originalRecordLength);
    // cout<<"returned record size : "<<originalRecordLength<<endl;
    return;
}

int RecordBasedFileManager::readMigrateRecord(FileHandle &fileHandle, const int pageNumber, const int slotNumber, char* recordData){
    // cout<<"In readMifrateRecord()"<<endl;
    void* pageData = malloc(PAGE_SIZE);
    RC rc = fileHandle.readPage(pageNumber, pageData);
    if (rc != 0) {
        free(pageData);
        return -1;
    }
    int slotCount = getPageSlotCount(pageData);
    if(slotCount<=slotNumber){
        perror("The Slot ID does not exist");
        return -1;
    }

    SlotInfo* slotInfo = getSlotInfoPointer(pageData, slotNumber);
    // cout<<"status : "<<status<<endl;
    // cout<<"recordStartPos : "<<recordStartPos<<endl;
    switch(slotInfo->status){
        case 0:{// deleted record
            perror("The record has been deleted");
            free(pageData);
            return -1;
        }
        case 1:{// normal record
            memcpy(recordData, (char*)pageData + slotInfo->slotStartPos, slotInfo->slotLength);
            free(pageData);
            return 0;
        }
        case 2:{// migrated record
            // int newPageNum = slotInfo->slotStartPos;
            int newPageNum = *(int*)((char*)pageData+PAGE_SIZE-sizeof(int)*2-sizeof(int)*(slotNumber+1)*3 + sizeof(int));
            int newSlotNum = *(int*)((char*)pageData+PAGE_SIZE-sizeof(int)*2-sizeof(int)*(slotNumber+1)*3 + sizeof(int)*2);
            free(pageData);
            int ret = readMigrateRecord(fileHandle, newPageNum, newSlotNum, recordData);
            // cout<<"ret---"<<ret<<endl;
            return ret;
        }
    }
    // cout<<"At the end of Read Migrated Record!"<<endl;
    return 0;
}


RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    // cout<<"In readRecord()"<<endl;
    char* recordData = (char*)malloc(PAGE_SIZE);
    int result = readMigrateRecord(fileHandle, rid.pageNum, rid.slotNum, recordData);
    if(result != 0){
        perror("Read Record Error!");
        return -1;
    }
    removeHeaderFromRecord(recordData, data);
    free(recordData);
    return 0;
}


RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    char *curData = (char*) data;
    int attrSize = recordDescriptor.size();
    int nullDescriptorLen = (int)ceil(attrSize/8.0);
    char *nullDescriptor = (char*) data;
    // cout<<"null descriptor = "<<(int)*nullDescriptor<<endl;
    curData += nullDescriptorLen;

    for(int i=0; i<attrSize; i++){
        int pos = i/8;
        int j = i%8;
        Attribute attr = recordDescriptor[i];
        // int temp = nullDescriptor[pos]>>(8-1-j);
        // cout<<"j = "<<j<<endl;
        // cout<<"null descriptor = "<<(int)*nullDescriptor<<endl;
        // cout<<"Nullbit = "<<temp<<endl;

        cout<< attr.name<<" "<< attr.type<<" ";

        if(nullDescriptor[pos] & (1<<(8-1-j))){// The current attribute is NULL
            cout<< "NULL" <<endl;
        } 
        else{
            if(attr.type == TypeInt){
                cout<<*(int*)curData <<endl;
                curData += 4;
            }
            else if(attr.type == TypeReal){
                cout<<*(float*)curData <<endl;
                curData += 4;
            }
            else if(attr.type == TypeVarChar){
                int varCharLength = *(int*)curData;
                curData += 4;
                for(int k=0; k<varCharLength; k++){
                    cout<<*((char*)curData+k);
                }
                cout<<endl;
                curData += varCharLength;
            }
        }
    }

    return 0;
}

void RecordBasedFileManager::expandFreeSpace(void* pageData){
    void* newPageData = malloc(PAGE_SIZE);
    int slotCount = getPageSlotCount(pageData);
    int curPos = 0;
    for(int i=0; i<slotCount; i++){
        SlotInfo* slotInfo = getSlotInfoPointer(pageData, i);
        if(slotInfo->status == 1){
            memcpy((char*)newPageData+curPos,(char*)pageData+slotInfo->slotStartPos, slotInfo->slotLength);
            slotInfo->slotStartPos = curPos;    //update slot info
            curPos += slotInfo->slotLength;
        }
    }
    *getPageFreeSpacePointer(pageData) = curPos;    //update Page Info in the original page
    memcpy(pageData,newPageData, curPos);   //Copy organized data from new page to original page.
    free(newPageData);
    return;
}   

int RecordBasedFileManager::deleteMigrateRecord(FileHandle &fileHandle, const int pageNumber, const int slotNumber){
    void* pageData = malloc(PAGE_SIZE);
    RC rc = fileHandle.readPage(pageNumber, pageData);
    if (rc != 0) {
        free(pageData);
        return -1;
    }
    int slotCount = getPageSlotCount(pageData);
    if(slotCount<=slotNumber){
        perror("The Slot ID does not exist");
        return -1;
    }

    SlotInfo* slotInfo = getSlotInfoPointer(pageData, slotNumber);
    // cout<<"status : "<<status<<endl;
    // cout<<"recordStartPos : "<<recordStartPos<<endl;
    switch(slotInfo->status){
        case 0:{// deleted record
            perror("The record has been deleted");
            free(pageData);
            return -1;
        }
        case 1:{// normal record
            slotInfo->status = 0;
            slotInfo->slotStartPos = 0;
            slotInfo->slotLength = 0;
            expandFreeSpace(pageData);
            fileHandle.writePage(pageNumber, pageData);
            free(pageData);
            return 0;
        }
        case 2:{// migrated record
            int newPageNum = *(int*)((char*)pageData+PAGE_SIZE-sizeof(int)*2-sizeof(int)*(slotNumber+1)*3 + sizeof(int));
            int newSlotNum = *(int*)((char*)pageData+PAGE_SIZE-sizeof(int)*2-sizeof(int)*(slotNumber+1)*3 + sizeof(int)*2);
            int ret = deleteMigrateRecord(fileHandle, newPageNum, newSlotNum);
            if(ret==0){ // Delete succeed, chage migrated slot info.
                slotInfo->status = 0;
                slotInfo->slotStartPos = 0;
                slotInfo->slotLength = 0;
                fileHandle.writePage(pageNumber, pageData);
            }
            free(pageData);
            // cout<<"ret---"<<ret<<endl;
            return ret;
        }
    }
    // cout<<"At the end of Read Migrated Record!"<<endl;
    return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){
    int result = deleteMigrateRecord(fileHandle, rid.pageNum, rid.slotNum);
    return result;
}

void RecordBasedFileManager::shrinkFreeSpace(void* pageData, const int slotNumber, void* updateData, const int recordLength){
    void* newPageData = malloc(PAGE_SIZE);
    int slotCount = getPageSlotCount(pageData);
    int curPos = 0;
    for(int i=0; i<slotCount; i++){
        SlotInfo* slotInfo = getSlotInfoPointer(pageData, i);
        if(slotInfo->status == 1){
            if(slotNumber == i){
                memcpy((char*)newPageData+curPos, (char*)updateData, recordLength);
                slotInfo->slotStartPos = curPos;
                slotInfo->slotLength = recordLength;
                curPos += recordLength;
            }
            else{
                memcpy((char*)newPageData+curPos,(char*)pageData+slotInfo->slotStartPos, slotInfo->slotLength);
                slotInfo->slotStartPos = curPos;    //update slot info
                curPos += slotInfo->slotLength;
            }
        }
    }
    *getPageFreeSpacePointer(pageData) = curPos;    //update Page Info in the original page
    memcpy(pageData,newPageData, curPos);   //Copy organized data from new page to original page.
    free(newPageData);
    return;
}

int RecordBasedFileManager::updateRecordInThisPage(FileHandle &fileHandle, void* pageData, const int slotNumber, void* updateData, const int recordLength){
    SlotInfo* slotInfo = getSlotInfoPointer(pageData, slotNumber);
    if(slotInfo->slotLength == recordLength){
        memcpy((char*)pageData+slotInfo->slotStartPos, (char*)updateData, recordLength);
        return 0;
    }
    if(slotInfo->slotLength > recordLength){
        memcpy((char*)pageData+slotInfo->slotStartPos, (char*)updateData, recordLength);
        slotInfo->slotLength = recordLength;
        expandFreeSpace(pageData);
        return 0;
    }
    if(slotInfo->slotLength < recordLength){
        int freeSpaceStart = getPageFreeSpacePos(pageData);
        int slotCount = getPageSlotCount(pageData);
        int freeSpace = PAGE_SIZE - freeSpaceStart - sizeof(int)*2 - sizeof(SlotInfo)*slotCount;
        if(freeSpace >= (recordLength-slotInfo->slotLength)){
            shrinkFreeSpace(pageData, slotNumber, updateData, recordLength);
            return 0;
        }
        else{
            RID newRid;
            insertRecordHelper(fileHandle, updateData, newRid, recordLength);
            slotInfo->status = 2;
            slotInfo->slotStartPos = newRid.pageNum;
            slotInfo->slotLength = newRid.slotNum;
            expandFreeSpace(pageData);
            return 0;
        }
    }
    return -1;
}

int RecordBasedFileManager::updateMigrateRecord(FileHandle &fileHandle, const int pageNumber, const int slotNumber, void *updateData, const int recordLength){
    void* pageData = malloc(PAGE_SIZE);
    RC rc = fileHandle.readPage(pageNumber, pageData);
    if (rc != 0) {
        free(pageData);
        return -1;
    }
    int slotCount = getPageSlotCount(pageData);
    if(slotCount<=slotNumber){
        perror("The Slot ID does not exist");
        return -1;
    }

    SlotInfo* slotInfo = getSlotInfoPointer(pageData, slotNumber);
    switch(slotInfo->status){
        case 0:{// deleted record
            perror("The record has been deleted");
            free(pageData);
            return -1;
        }
        case 1:{// normal record
            updateRecordInThisPage(fileHandle, pageData, slotNumber, updateData, recordLength);
            fileHandle.writePage(pageNumber, pageData);
            free(pageData);
            return 0;
        }
        case 2:{// migrated record
            int newPageNum = *(int*)((char*)pageData+PAGE_SIZE-sizeof(int)*2-sizeof(int)*(slotNumber+1)*3 + sizeof(int));
            int newSlotNum = *(int*)((char*)pageData+PAGE_SIZE-sizeof(int)*2-sizeof(int)*(slotNumber+1)*3 + sizeof(int)*2);
            free(pageData);
            int ret = updateMigrateRecord(fileHandle, newPageNum, newSlotNum, updateData, recordLength);
            return ret;
        }
    }
    // cout<<"At the end of Read Migrated Record!"<<endl;
    return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){
    void * newData = malloc(PAGE_SIZE);
    int recordLength = addHeaderToRecord(recordDescriptor, newData, data);
    int result = updateMigrateRecord(fileHandle, rid.pageNum, rid.slotNum, newData, recordLength);
    return result;
}

int RecordBasedFileManager::readAttributeHelper(char* recordData, int attrID, void* data){
    int fieldCount = *(int*)recordData;
    int nullDescriptorLen = *(int*)(recordData + sizeof(int));
    int field_start_pos = (2+fieldCount)*sizeof(int)+nullDescriptorLen;
    int field_end_pos = *(int*)(recordData + sizeof(int)*(2+attrID));
    if(attrID > 0)  // The start position is the end positon of previous attribute
        field_start_pos = *(int*)(recordData + sizeof(int)*(2+attrID-1));
    int field_length = field_end_pos - field_start_pos;
    if(field_length<=0)
        *(char*)data = 0x80;    // The field does not has data, which means the field is null.
    else{
        *(char*)data = 0x00;
        memcpy((char*)data+sizeof(char), recordData+field_start_pos, field_length);
    }
    return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data){
    char* recordData = (char*)malloc(PAGE_SIZE);
    int result = readMigrateRecord(fileHandle, rid.pageNum, rid.slotNum, recordData);
    if(result != 0){
        perror("Read Record Error!");
        return -1;
    }
    for(unsigned i=0; i<recordDescriptor.size(); i++){
        if(recordDescriptor[i].name == attributeName){
            readAttributeHelper(recordData, i, data);
            free(recordData);
            return 0;
        }
    }

    free(recordData);
    return -1;
}


RC RecordBasedFileManager::scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator){

    rbfm_ScanIterator.fileHandle = fileHandle;
    rbfm_ScanIterator.recordDescriptor = recordDescriptor;
    rbfm_ScanIterator.conditionAttribute = conditionAttribute;
    rbfm_ScanIterator.compOp = compOp;
    rbfm_ScanIterator.attributeNames = attributeNames;

    getConditionAttr(recordDescriptor, conditionAttribute, rbfm_ScanIterator);
    if(compOp != NO_OP){
        getValue(value, rbfm_ScanIterator);
    }

    rbfm_ScanIterator.nextRID.pageNum = 0;
    rbfm_ScanIterator.nextRID.slotNum = 0;

    return 0;
}

int RecordBasedFileManager::getConditionAttr(const vector<Attribute> &recordDescriptor, const string &conditionAttribute, RBFM_ScanIterator &rbfm_ScanIterator){
    rbfm_ScanIterator.conditionAttributeID = -1;
    
    if(conditionAttribute.empty() || conditionAttribute=="")
        return 0;

    for(unsigned i=0; i<recordDescriptor.size(); i++){
        if(recordDescriptor[i].name == conditionAttribute){
            rbfm_ScanIterator.conditionAttributeID = i;
            rbfm_ScanIterator.conditionAttributeType = recordDescriptor[i].type;
            break;
        }
    }
    return 0;
}

int RecordBasedFileManager::getValue(const void* value, RBFM_ScanIterator &rbfm_ScanIterator){
    switch(rbfm_ScanIterator.conditionAttributeType){
        case TypeVarChar:{
            int dataLength = *(int*) value + sizeof(int);
            rbfm_ScanIterator.value = malloc(dataLength);
            memcpy((char*)rbfm_ScanIterator.value, (char*)value, dataLength);
            break;
        }
        case TypeInt:{
            rbfm_ScanIterator.value = malloc(sizeof(int));
            *(int*)rbfm_ScanIterator.value = *(int*)value;
            break;
        }
        case TypeReal:{
            rbfm_ScanIterator.value = malloc(sizeof(int));
            *(int*)rbfm_ScanIterator.value = *(int*)value;
            break;   
        }
    }

    return 0;
}


RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data){

    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    RC returnVal = 0;

    void* pageData = malloc(PAGE_SIZE);
    void* recordData = malloc(PAGE_SIZE);

    if(fileHandle.readPage(nextRID.pageNum, pageData)<0){
        free(pageData);
        free(recordData);
        return RBFM_EOF;
    }
    unsigned slotCountOfPage = rbfm->getPageSlotCount(pageData);

    while(validateRecord(pageData, recordData)==false){
        gotoNextSlot(rid);
        if((nextRID.slotNum+1) >= slotCountOfPage){
            nextRID.pageNum++;
            if((nextRID.pageNum+1) >= fileHandle.getNumberOfPages()){
                returnVal = RBFM_EOF;
                break;
            }
            nextRID.slotNum = 0;
            fileHandle.readPage(nextRID.pageNum, pageData);
            slotCountOfPage = rbfm->getPageSlotCount(pageData);
        }
    }

    if(returnVal != RBFM_EOF){
        getResultData(recordData, data);
        gotoNextSlot(rid);
    }

    free(pageData);
    free(recordData);

    return returnVal;
}

int RBFM_ScanIterator::getResultData(void* recordData, void* data){
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    int nullDescriptorLen = (int)(ceil)(attributeNames.size()/8.0);
    // int numOfAttr = attributeNames.size();
    int curAttrCount = 0;   // used as a marker which bit should to be set
    void* tempAttrData = malloc(500);
    // int len;
    int offset = nullDescriptorLen;    // used as the position offset for next memcpy

    for(unsigned i=0; i<attributeNames.size(); i++){
        for(unsigned j=0; j<recordDescriptor.size(); j++){
            if(attributeNames[i] == recordDescriptor[j].name){

                curAttrCount++;
                rbfm->readAttributeHelper((char*)recordData, j, tempAttrData);
                concatenateResult(tempAttrData, recordDescriptor[j].type, offset, curAttrCount, data);
            }
        }
    }

    free(tempAttrData);
    return 0;
}

int RBFM_ScanIterator::concatenateResult(void* attrData, AttrType type, int &offset, int attrCount, void* data){
    int pos_byte = (attrCount-1)/8; // [0, nullDescriptorLen-1]
    int pos_bit = (attrCount-1)%8;  // [0,7]
    if(*(char*)attrData == 0x80)
        *((char*)data+pos_byte) |= (1<<(7-pos_bit));     // if null, set corresponding bit to 1
    
    int len;
    if(*(char*)attrData == 0x80)
        len = 0;
    else{
        switch(type){
            case TypeVarChar:
                len = sizeof(int) + *(int*) ((char*)attrData + sizeof(char));
                break;
            case TypeInt:
                len = sizeof(int);
                break;
            case TypeReal:
                len = sizeof(int);
                break;
        }
    }

    memcpy((char*)data+offset, (char*)attrData+1, len);
    offset += len;
    return 0;
}

void RBFM_ScanIterator::gotoNextSlot(RID &rid){
    rid.slotNum = nextRID.slotNum;
    rid.pageNum = nextRID.pageNum;
    nextRID.slotNum++;
}

bool RBFM_ScanIterator::validateRecord(void* pageData, void* recordData){
    if(checkSlotStatus(pageData)==false)    // The slot is deleted or migrated
        return false;
    if(checkRecordRequirement(pageData, recordData)==false)
        return false;
    return true;
}

bool RBFM_ScanIterator::checkSlotStatus(void* pageData){
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    unsigned slotCountOfPage = rbfm->getPageSlotCount(pageData);
    if((nextRID.slotNum+1)>=slotCountOfPage)
        return false;
    SlotInfo* slotInfo = rbfm->getSlotInfoPointer(pageData, nextRID.slotNum);
    if(slotInfo->status != 1)   // The slot is deleted or migrated
        return false;
    return true;
}

bool RBFM_ScanIterator::checkRecordRequirement(void* pageData, void* recordData){
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    rbfm->readMigrateRecord(fileHandle, nextRID.pageNum, nextRID.slotNum, (char*)recordData);

    void* attrData = malloc(500);
    rbfm->readAttributeHelper((char*)recordData, conditionAttributeID, attrData);

    if(checkAttribute(value, attrData)==false){
        free(attrData);
        return false;
    }
    free(attrData);
    return true;
}

bool RBFM_ScanIterator::checkAttribute(void* value, void* attrData){
    // The returned attribute data has an one-byte null descriptor header.
    if(*(char*)attrData == 0x80)    // The attribute data is null.
        return false;
    if(compOp == NO_OP)
        return true;

    int result;
    switch (conditionAttributeType) {
        case TypeVarChar:{   
                int valueLength = *(int*) value;  // Length without head.
                string valueStr = string((char*)((char*)value + sizeof(int)),valueLength);
                int attrLength = *(int*) ((char*)attrData + sizeof(char));
                string  attrstr = string((char*)((char*)attrData + sizeof(int)), attrLength);  // Skip null descriptor (one byte) and varchar head (one int)
                result = attrstr.compare(valueStr);
                break;
            }
        case TypeInt:
            result = *((int*) ((char*)attrData + sizeof(char))) - *((int*) value); 
            break;
        case TypeReal:
            result = *((float*) ((char*)attrData + sizeof(char))) - *((float*) value);
            break;
    }

    switch (compOp){
        case NO_OP:
            return true;
            break;
        case EQ_OP:
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


RC RBFM_ScanIterator::close()
{
    PagedFileManager::instance()->closeFile(fileHandle);
    if(compOp != NO_OP)
    {
        free(value);
    }

    return 0;
}