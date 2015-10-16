
#include "ix.h"
#include <math.h>
#include <string.h>
#include <iostream>
#include <string>
#include <vector>


IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    return PagedFileManager::instance()->createFile(fileName.c_str());
}

RC IndexManager::destroyFile(const string &fileName)
{
    return PagedFileManager::instance()->destroyFile(fileName.c_str()); 
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    return ixfileHandle.openFile(fileName);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    return ixfileHandle.closeFile();
}

int* IndexManager::getFreeSpacePointer(void* pageStartPosPointer){
    return (int*)((char*)pageStartPosPointer + PAGE_SIZE - 2 * sizeof(int));
}

int IndexManager::getFreeSpaceOffset(void* pageStartPosPointer){
    return *getFreeSpacePointer(pageStartPosPointer);
}

RC IndexManager::setFreeSpaceOffset(void* pageStartPosPointer, int offset){
    memcpy((char*)pageStartPosPointer + PAGE_SIZE - 2 * sizeof(int), &offset, sizeof(int));
    return 0;
}

int* IndexManager::getKeysCountPointer(void* pageStartPosPointer) const{
    return (int*)((char*)pageStartPosPointer + PAGE_SIZE - 3 * sizeof(int));
}

int IndexManager::getKeysCount(void* pageStartPosPointer) const{
    return *getKeysCountPointer(pageStartPosPointer);
}

RC IndexManager::setKeysCount(void* pageStartPosPointer, int count){
    memcpy((char*)pageStartPosPointer + PAGE_SIZE - 3 * sizeof(int), &count, sizeof(int));
    return 0;
}

int* IndexManager::getIsLeafPointer(void* pageStartPosPointer) const{
    return (int*)((char*)pageStartPosPointer + PAGE_SIZE - sizeof(int));
}

RC IndexManager::checkLeaf(void* pageStartPosPointer) const{
    return *getIsLeafPointer(pageStartPosPointer);
}

RC IndexManager::setLeaf(void* pageStartPosPointer){
    int leaf = 1;
    memcpy((char*)pageStartPosPointer + PAGE_SIZE - sizeof(int), &leaf, sizeof(int));
    return 0;
}

RC IndexManager::setIndex(void* pageStartPosPointer){
    int leaf = 0;
    memcpy((char*)pageStartPosPointer + PAGE_SIZE - sizeof(int), &leaf, sizeof(int));
    return 0;
}

int* IndexManager::getLeftPointer(void* pageStartPosPointer){
    return (int*)((char*)pageStartPosPointer + PAGE_SIZE - 5 * sizeof(int));
}

int IndexManager::getLeftNode(void* pageStartPosPointer){
    return *getLeftPointer(pageStartPosPointer);
}

RC IndexManager::setLeftNode(void* pageStartPosPointer, int pageNum){
    memcpy((char*)pageStartPosPointer + PAGE_SIZE - 5 * sizeof(int), &pageNum, sizeof(int));
    return 0;
}

int* IndexManager::getRightPointer(void* pageStartPosPointer){
    return (int*)((char*)pageStartPosPointer + PAGE_SIZE - 4 * sizeof(int));
}

int IndexManager::getRightNode(void* pageStartPosPointer){
    return *getRightPointer(pageStartPosPointer);
}

RC IndexManager::setRightNode(void* pageStartPosPointer, int pageNum){
    memcpy((char*)pageStartPosPointer + PAGE_SIZE - 4 * sizeof(int), &pageNum, sizeof(int));
    return 0;
}

int IndexManager::getFreeSpace(void* pageStartPosPointer, bool isLeaf){
    if(isLeaf){
        return (PAGE_SIZE - getFreeSpaceOffset(pageStartPosPointer) - 6 * sizeof(int));
    }
    return (PAGE_SIZE - getFreeSpaceOffset(pageStartPosPointer) - 3 * sizeof(int));
}

int IndexManager::getFreeSpaceOffset(IXFileHandle &ixfileHandle, int pageNum){
    void *data = malloc(PAGE_SIZE);
    ixfileHandle.readPage(pageNum, data);
    int pos = PAGE_SIZE - 2 * sizeof(int);
    int res = -1;
    memcpy(&res, (char *)data + pos, sizeof(int));
    free(data);
    return res;
}

RC IndexManager::setFreeSpaceOffset(IXFileHandle &ixfileHandle, int pageNum, int offset){
    void *data = malloc(PAGE_SIZE);
    memset(data, 0, PAGE_SIZE);
    ixfileHandle.readPage(pageNum, data);
    int pos = PAGE_SIZE - 2 * sizeof(int);
    memcpy((char *) data + pos, &offset, sizeof(int));
    ixfileHandle.writePage(pageNum, data);
    free(data);
    return 0;
}

int IndexManager::getKeysCount(IXFileHandle &ixfileHandle, int pageNum){
    void *data = malloc(PAGE_SIZE);
    memset(data, 0, PAGE_SIZE);
    ixfileHandle.readPage(pageNum, data);
    int pos = PAGE_SIZE - 3 * sizeof(int);
    int res = -1;
    memcpy(&res, (char *)data + pos, sizeof(int));
    free(data);
    return res;
}

int IndexManager::setKeysCount(IXFileHandle &ixfileHandle, int pageNum, int count){
    void *data = malloc(PAGE_SIZE);
    memset(data, 0, PAGE_SIZE);
    ixfileHandle.readPage(pageNum, data);
    int pos = PAGE_SIZE - 3 * sizeof(int);
    memcpy((char *) data + pos, &count, sizeof(int));
    ixfileHandle.writePage(pageNum, data);
    free(data);
    return 0;
}

RC IndexManager::setLeaf(IXFileHandle &ixfileHandle, int pageNum){
    void *data = malloc(PAGE_SIZE);
    memset(data, 0, PAGE_SIZE);
    ixfileHandle.readPage(pageNum, data);
    int pos = PAGE_SIZE - sizeof(int);
    int temp = 1;
    memcpy((char *) data + pos, &temp, sizeof(int));
    ixfileHandle.writePage(pageNum, data);
    free(data);
    return 0;
}

RC IndexManager::setIndex(IXFileHandle &ixfileHandle, int pageNum){
    void *data = malloc(PAGE_SIZE);
    memset(data, 0, PAGE_SIZE);
    ixfileHandle.readPage(pageNum, data);
    int pos = PAGE_SIZE - sizeof(int);
    int temp = 0;
    memcpy((char *) data + pos, &temp, sizeof(int));
    ixfileHandle.writePage(pageNum, data);
    free(data);
    return 0;
}


int getAttributeLength(Attribute attribute,const void *key)
{
    switch(attribute.type)
    {
            case 2:
                return *((int *)key)+sizeof(int);
                break;
            case 0:
                return sizeof(int);
                break;
            case 1:
                return sizeof(float);
                break;
    }
    return -1;
}

bool isSmaller(const Attribute &attribute,const void *a,const void *b)
{
    switch(attribute.type)
    {
    case 0:return ((*(int *)a)<(*(int *)b));
    case 1:return ((*(float *)a)<(*(float *)b));
    case 2:
        string x="",y="";
        int l=*(int *)a;
        for(int i=0;i<l;i++)
            x+=*((char *)a+sizeof(int)+i);
        l=*(int *)b;
        for(int i=0;i<l;i++)
            y+=*((char *)b+sizeof(int)+i);
        return x<y;
    }
    return false;
}

int getOffsetOfSpecificKey(const Attribute &attribute, int keyID, void * pageData){
    IndexManager *ixm = IndexManager::instance();
    int numOfKeys = ixm->getKeysCount(pageData);
    int offset = 0;
    int currentKeyID = 0;
    if(keyID < numOfKeys){
        while(currentKeyID < keyID){
            int keyLen = getAttributeLength(attribute, (char*)pageData+offset);
            int RidLen = *(int*)((char*)pageData+offset+keyLen);
            offset += (keyLen + sizeof(int)*(1 + 2*RidLen));
            currentKeyID++;
        }
        return offset;
    }
    return -1;
}




RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{   
    // cout<<"In IndexManager::insertEntry()--------------------------"<<endl;
    if(ixfileHandle.getNumberOfPages()==0 ){ // No page now, initialize the first leaf page
        newLeafPage(ixfileHandle, attribute, key, rid, 1);
    }
    else if(getKeysCount(ixfileHandle, 0) == 0){
        newLeafPage(ixfileHandle, attribute, key, rid, 0);
    }
    else{
        int pageNum = searchForInsert(ixfileHandle, attribute, key, 0, -1);
        // cout<<"page number = "<<pageNum<<endl;
        insertIntoLeafNode(ixfileHandle, attribute, key, rid, pageNum);
    }
    return 0;
}

RC IndexManager::insertIntoLeafNode(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, const int pageNum){
    // cout<<"in IndexManager::insertIntoLeafNode()-------------------"<<endl;
    void *leaf = malloc(PAGE_SIZE);
    memset(leaf, 0, PAGE_SIZE);
    ixfileHandle.readPage(pageNum, leaf);
    writeToLeaf(leaf, attribute, key, rid);
    ixfileHandle.writePage(pageNum, leaf);
    free(leaf);
    return 0;
}

int IndexManager::searchForInsert(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const int pageNum, const int fatherPageNum){
    void *data = malloc(PAGE_SIZE);
    memset(data, 0 ,PAGE_SIZE);
    ixfileHandle.readPage(pageNum, data);
    int keyLength = getAttributeLength(attribute, key);
    void *temp;
    if(attribute.type == TypeVarChar){
        temp = malloc(sizeof(int) + attribute.length);
        memset(temp, 0, sizeof(int) + attribute.length);
    }
    else{
        temp = malloc(4);
    }


    if(checkLeaf(data) == 0){ //is Index
        if((int)(keyLength + sizeof(int)) > getFreeSpace(data, false)){
            split(ixfileHandle, attribute, pageNum, fatherPageNum, false);
            int res = searchForInsert(ixfileHandle, attribute, key, 0, -1);
            free(temp);
            free(data);
            return res;
        }

        int keysCount = getKeysCount(data);
        int curOff = sizeof(int);
        int tempKeyLen;
        for(int i = 0; i < keysCount; i++){
            tempKeyLen = getAttributeLength(attribute, (char *)data + curOff);
            if(attribute.type == TypeVarChar){
                memset(temp, 0, sizeof(int) + attribute.length);    //???
            }
            memcpy((char *)temp, (char *)data + curOff, tempKeyLen);

            if(isSmaller(attribute, key, temp)){ //key is smaller than temp key we get
                int leftPointer;
                memcpy(&leftPointer, (char *)data + curOff - sizeof(int), sizeof(int));
                // curOff  += (tempKeyLen + sizeof(int));  //???
                int res = searchForInsert(ixfileHandle, attribute, key, leftPointer, pageNum);
                free(temp);
                free(data);
                return res;
            }

            else{ //key is bigger than temp key we get, continue for loop
                curOff  += (tempKeyLen + sizeof(int));
            }
        }
        //cannot find a key smaller than the given key in the current node
        //use the right pointer of the last key in the node to search
        int rightPointer;
        memcpy(&rightPointer, (char *)data + curOff - sizeof(int), sizeof(int));
        int res = searchForInsert(ixfileHandle, attribute, key, rightPointer, pageNum);
        free(temp);
        free(data);
        return res;
    }
    else{ // Is leaf node
        if((int)(keyLength + 3 * sizeof(int)) > getFreeSpace(data, true)){ //check if need split
            split(ixfileHandle, attribute, pageNum, fatherPageNum, true);
            int res = searchForInsert(ixfileHandle, attribute, key, 0, -1);
            free(temp);
            free(data);
            return res;
        }
        free(temp);
        free(data);
        return pageNum; //return leaf node page
    }
    free(temp);
    free(data);
    return -1; //error
}

RC IndexManager::split(IXFileHandle &ixfileHandle, const Attribute &attribute, const int pageNum, const int fatherPageNum, const bool isLeaf){
    //cout<<"In IndexManager::split()---------------------------------------"<<endl;
    void *data = malloc(PAGE_SIZE);
    memset(data, 0, PAGE_SIZE);
    ixfileHandle.readPage(pageNum, data);
    int keysCount = getKeysCount(data);
    int firstHalf = keysCount/2, secondHalf = keysCount/2;
    if((keysCount % 2) != 0){
        firstHalf += 1;
    }

    void *temp1 = malloc(PAGE_SIZE);
    void *temp2 = malloc(PAGE_SIZE);
    memset(temp1, 0, PAGE_SIZE);
    memset(temp2, 0, PAGE_SIZE);

    int curOff = 0;
    int pageCount = ixfileHandle.getNumberOfPages();
    int keyLength; //The length of the first key in temp2

    if(isLeaf){
        int ridCount;
        for(int i = 0; i < firstHalf; i++){
            curOff += getAttributeLength(attribute, (char *)data + curOff);
            memcpy(&ridCount, (char *)data + curOff, sizeof(int));
            curOff += (ridCount*2 + 1) * sizeof(int); //??? Shold be (ridCount*2+1)*sizeof(int)?
        }

        memcpy((char *)temp1, (char *)data, curOff);
        setFreeSpaceOffset(temp1, curOff);
        setLeaf(temp1);
        setKeysCount(temp1, firstHalf);
        setLeftNode(temp1, getLeftNode(data));
        setRightNode(temp1, pageCount); //pageCount is equal to the new page number
        memcpy((char *)temp2, (char *)data + curOff, getFreeSpaceOffset(data) - curOff);
        setFreeSpaceOffset(temp2, getFreeSpaceOffset(data) - curOff);
        setLeaf(temp2);
        setKeysCount(temp2, secondHalf);
        setLeftNode(temp2, pageNum);
        setRightNode(temp2, getRightNode(data));
        ixfileHandle.appendPage(temp2);
        keyLength = getAttributeLength(attribute, temp2); //The length of the first key in temp2

        if(getRightNode(data) != -1){
            void *oldRightNode = malloc(PAGE_SIZE); //change the pointer of the old right node
            memset(oldRightNode, 0, PAGE_SIZE);
            ixfileHandle.readPage(getRightNode(data), oldRightNode);
            setLeftNode(oldRightNode, pageCount);
            ixfileHandle.writePage(getRightNode(data), oldRightNode);
            free(oldRightNode);
        }
    }
    else{ //the split node is an index node
        curOff = sizeof(int);
        for(int i = 0; i < firstHalf - 1; i++){
            curOff += (getAttributeLength(attribute, (char *)data + curOff) + sizeof(int));
        }

        memcpy((char *)temp1, (char *)data, curOff);
        setFreeSpaceOffset(temp1, curOff);
        setIndex(temp1);
        setKeysCount(temp1, firstHalf - 1);

        memcpy((char *)temp2, (char *)data + curOff, getFreeSpaceOffset(data) - curOff);
        // temp2 includes the key which need to be pushed up in the second node
        keyLength = getAttributeLength(attribute, (char *)temp2); //??? Why +sizeof(int)

        void *temp3 = malloc(PAGE_SIZE);
        // temp3 includes the information of the second node after push up
        memset(temp3, 0, PAGE_SIZE);
        memcpy((char *)temp3, (char *)temp2 + keyLength, getFreeSpaceOffset(data) - curOff - keyLength);
        setFreeSpaceOffset(temp3, getFreeSpaceOffset(data) - curOff - keyLength);
        setIndex(temp3);
        setKeysCount(temp3, secondHalf);
        ixfileHandle.appendPage(temp3);
        free(temp3);
        //??? Where is push up part??? The pushed-up key should point to temp3 page
    }

    if(pageNum == 0){ // split root node of split the first leaf node
        void *root = malloc(PAGE_SIZE);
        memset(root, 0, PAGE_SIZE);
        int leftNode = pageCount + 1; // another new page which is going to append later
        int rightNode = pageCount;
        memcpy((char *) root, &leftNode, sizeof(int));
        memcpy((char *) root + sizeof(int), (char *)temp2, keyLength);
        memcpy((char *) root + sizeof(int) + keyLength, &rightNode, sizeof(int));
        setIndex(root);
        setFreeSpaceOffset(root, keyLength + 2 * sizeof(int));
        setKeysCount(root, 1);
        ixfileHandle.writePage(0, root);
        ixfileHandle.appendPage(temp1);
        free(root);
    }
    else{ // split a normal index node or leaf node
        void *index = malloc(PAGE_SIZE); //father index node
        memset(index, 0, PAGE_SIZE);
        ixfileHandle.readPage(fatherPageNum, index);
        void *newKey = malloc(keyLength);
        memcpy((char *)newKey, (char *)temp2, keyLength);
        //cout<<"new key in root = "<<*(int*)newKey<<endl;
        int newNode = pageCount;
        writeToIndex(index, attribute, newKey, newNode);
        ixfileHandle.writePage(fatherPageNum, index);
        ixfileHandle.writePage(pageNum, temp1);
        free(index);
        free(newKey);
    }
    free(temp1);
    free(temp2);
    free(data);
    return 0;
}

//following write is writing into memory (pointed by index), not into disk
RC IndexManager::writeToIndex(void *index, const Attribute &attribute, const void *newKey, const int newPageNum){
    // cout<<"In IndexManager::writeToIndex()-------------------------------"<<endl;
    void *data = malloc(PAGE_SIZE);
    memset(data, 0, PAGE_SIZE);
    void *temp;
    if(attribute.type == TypeVarChar){
        temp = malloc(sizeof(int) + attribute.length);
        memset(temp, 0, sizeof(int) + attribute.length);
    }
    else{
        temp = malloc(4);
    }
    int keysCount = getKeysCount(index);
    int curOff = sizeof(int);
    int keyLength;
    int newKeyLength = getAttributeLength(attribute, newKey);
    for(int i = 0; i < keysCount; i++){
        keyLength = getAttributeLength(attribute, (char *)index + curOff);
        memcpy((char *)temp, (char *)index + curOff, keyLength);
        if(isSmaller(attribute, newKey, temp)){
            break;
        }
        curOff += (keyLength + sizeof(int));
    }
    memcpy((char *)data, (char *)index + curOff, getFreeSpaceOffset(index) - curOff);
    memcpy((char *)index + curOff, (char *)newKey, newKeyLength);
    memcpy((char *)index + curOff + newKeyLength, &newPageNum, sizeof(int));
    memcpy((char *)index + curOff + newKeyLength + sizeof(int), (char *)data, getFreeSpaceOffset(index) - curOff);
    setFreeSpaceOffset(index, getFreeSpaceOffset(index) + newKeyLength + sizeof(int));
    setKeysCount(index, getKeysCount(index) + 1);

    free(temp);
    free(data);
    return 0;
}

//following write is writing into memory (pointed by leaf), not into disk
RC IndexManager::writeToLeaf(void *leaf, const Attribute &attribute, const void *newKey, const RID &rid){
    // cout<<"In IndexManager::writeToLeaf()------------------------"<<endl;

    void *data = malloc(PAGE_SIZE);
    memset(data, 0, PAGE_SIZE);
    void *temp;
    if(attribute.type == TypeVarChar){
        temp = malloc(sizeof(int) + attribute.length);
        memset(temp, 0, sizeof(int) + attribute.length);
    }
    else{
        temp = malloc(4);
    }
    int keysCount = getKeysCount(leaf);
    int curOff = 0;
    int keyLength;
    int ridCount;
    bool equalKeys = false;
    int newKeyLength = getAttributeLength(attribute, newKey);

    for(int i = 0; i < keysCount; i++){
        keyLength = getAttributeLength(attribute, (char *)leaf + curOff);
        memcpy((char *)temp, (char *)leaf + curOff, keyLength);
        if(isSmaller(attribute, newKey, temp)){
            break;
        }
        if(isEqual(attribute, newKey, temp)){
            equalKeys = true;
            break;
        }
        memcpy(&ridCount, (char *)leaf + curOff + keyLength, sizeof(int));
        curOff += (keyLength + sizeof(int)*(1+2*ridCount));
    }
    // cout<<"curOff = "<<curOff<<endl;
    // cout<<"newKeyLength = "<<newKeyLength<<endl;
    // cout<<"free space offset = "<<getFreeSpaceOffset(leaf)<<endl;
    if(equalKeys){
        memcpy((char *)data, (char *)leaf + curOff + newKeyLength + sizeof(int), getFreeSpaceOffset(leaf) - curOff - newKeyLength -sizeof(int));
        int ridCount;
        memcpy(&ridCount, (char *)leaf + curOff + newKeyLength, sizeof(int));
        ridCount ++;
        memcpy((char *)leaf + curOff + newKeyLength, &ridCount, sizeof(int));
        memcpy((char *)leaf + curOff + newKeyLength + sizeof(int), &rid.pageNum, sizeof(int));
        memcpy((char *)leaf + curOff + newKeyLength + 2 * sizeof(int), &rid.slotNum, sizeof(int));
        memcpy((char *)leaf + curOff + newKeyLength + 3 * sizeof(int), (char *)data, getFreeSpaceOffset(leaf) - curOff - newKeyLength -sizeof(int));
        setFreeSpaceOffset(leaf, getFreeSpaceOffset(leaf) + 2 * sizeof(int));
    }

    else{
        memcpy((char *)data, (char *)leaf + curOff, getFreeSpaceOffset(leaf) - curOff);
        memcpy((char *)leaf + curOff, (char *)newKey, newKeyLength);
        int ridCount = 1;
        memcpy((char *)leaf + curOff + newKeyLength, &ridCount, sizeof(int));
        memcpy((char *)leaf + curOff + newKeyLength + sizeof(int), &rid.pageNum, sizeof(int));
        memcpy((char *)leaf + curOff + newKeyLength + 2 * sizeof(int), &rid.slotNum, sizeof(int));
        memcpy((char *)leaf + curOff + newKeyLength + 3 * sizeof(int), (char *)data, getFreeSpaceOffset(leaf) - curOff);
        setFreeSpaceOffset(leaf, getFreeSpaceOffset(leaf) + newKeyLength + 3 * sizeof(int));
        setKeysCount(leaf, getKeysCount(leaf) + 1);
    }

    free(data);
    free(temp);
    // cout<<"writeToLeaf END------------------------------"<<endl;
    return 0;
}

bool IndexManager::isEqual(const Attribute &attribute, const void *a, const void *b){
    switch(attribute.type)
        {
        case 0:return ((*(int *)a)==(*(int *)b));
        case 1:return ((*(float *)a)==(*(float *)b));
        case 2:
            string x="",y="";
            int l=*(int *)a;
            for(int i=0;i<l;i++)
                x+=*((char *)a+sizeof(int)+i);
            l=*(int *)b;
            for(int i=0;i<l;i++)
                y+=*((char *)b+sizeof(int)+i);
            return x==y;
        }
        return false;
}

RC IndexManager::newLeafPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, const bool append){

    void *data = malloc(PAGE_SIZE);
    memset(data, 0, PAGE_SIZE);
    int keyLength = getAttributeLength(attribute, key);
    memcpy((char*) data, key, keyLength);
    int ridCount = 1;
    memcpy((char*) data + keyLength, &ridCount, sizeof(int));
    memcpy((char*) data + keyLength + sizeof(int), &rid.pageNum, sizeof(int));
    memcpy((char*) data + keyLength + 2 * sizeof(int), &rid.slotNum, sizeof(int));
    setFreeSpaceOffset(data, keyLength + 3 * sizeof(int));
    setKeysCount(data, 1);
    setLeaf(data);
    setLeftNode(data, -1);
    setRightNode(data, -1);
    if(append == true){
        ixfileHandle.appendPage(data); // new Leaf Page
    }
    else{
        ixfileHandle.writePage(0, data); // Special case when delete all entries of index file then insert.
    }
    free(data);
    return 0;
}







RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    //cout<<"In IndexManager::deleteEntry()--------------------------------"<<endl;
    int pageNum = search(ixfileHandle, attribute, key, 0); //searchTillLeafPage??
    //cout<<"pageNum = "<<pageNum<<endl;
    RC res = deleteRID(ixfileHandle, attribute, pageNum, key, rid);
    if(res != 1)
        return res; //-1 or 0 for unsuccessful or successful delete
    //If res == 1, we need to do some work on the index layers, search and change (or delete) some data in the index
    searchAndChange(ixfileHandle, attribute, key, 0, -1);
    return 0;
}

RC IndexManager::search(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const int pageNum){
    //cout<<"In IndexManager::search()-------------------------------------------"<<endl;
    void *data = malloc(PAGE_SIZE);
    memset(data, 0 ,PAGE_SIZE);
    ixfileHandle.readPage(pageNum, data);
    void *temp;
    if(attribute.type == TypeVarChar){
        temp = malloc(sizeof(int) + attribute.length);
        memset(temp, 0, sizeof(int) + attribute.length);
    }
    else{
        temp = malloc(4);
    }
    if(checkLeaf(data) == 0){ //is Index
        int keysCount = getKeysCount(data);
        int curOff = sizeof(int);
        int tempKeyLen;
        for(int i = 0; i < keysCount; i++){
            tempKeyLen = getAttributeLength(attribute, (char *)data + curOff);
            if(attribute.type == TypeVarChar){
                memset(temp, 0, sizeof(int) + attribute.length);    //???
            }
            memcpy((char *)temp, (char *)data + curOff, tempKeyLen);

            if(isSmaller(attribute, key, temp)){ //key is smaller than temp key we get
                int leftPointer;
                memcpy(&leftPointer, (char *)data + curOff - sizeof(int), sizeof(int));
                // curOff  += (tempKeyLen + sizeof(int));  //???
                int res = search(ixfileHandle, attribute, key, leftPointer);
                free(temp);
                free(data);
                return res;
            }

            else{ //key is bigger than temp key we get, continue for loop
                curOff  += (tempKeyLen + sizeof(int));
            }
        }
        //cannot find a key bigger than the given key in the current node
        //use the right pointer of the last key in the node to search
        int rightPointer;
        memcpy(&rightPointer, (char *)data + curOff - sizeof(int), sizeof(int));
        int res = search(ixfileHandle, attribute, key, rightPointer);
        free(temp);
        free(data);
        return res;
    }
    else{ // Is leaf node
        free(temp);
        free(data);
        return pageNum; //return leaf node page
    }
    free(temp);
    free(data);
    return -1; //error
}

RC IndexManager::deleteRID(IXFileHandle &ixfileHandle, const Attribute &attribute, const int pageNum, const void *key, const RID rid){
    void *data = malloc(PAGE_SIZE);
    memset(data, 0, PAGE_SIZE);
    ixfileHandle.readPage(pageNum, data);
    int keysCount = getKeysCount(data);
    void *temp;
    if(attribute.type == TypeVarChar){
            temp = malloc(sizeof(int) + attribute.length);
            memset(temp, 0, sizeof(int) + attribute.length);
    }
    else{
        temp = malloc(4);
    }
    int curOff = 0;
    int keyLength;
    int ridCount;
    bool equalKeys = false;

    for(int i = 0; i < keysCount; i++){
        keyLength = getAttributeLength(attribute, (char *)data + curOff);
        memcpy((char *)temp, (char *)data + curOff, keyLength);
        if(isSmaller(attribute, key, temp)){
            break;
        }
        if(isEqual(attribute, key, temp)){
            equalKeys = true;
            break;
        }
        memcpy(&ridCount, (char *)data + curOff + keyLength, sizeof(int));
        curOff += (keyLength + sizeof(int) * (1 + 2 * ridCount));
    }

    if(equalKeys){ //If we find the key which equals to the input key
        memcpy(&ridCount, (char *)data + curOff + keyLength, sizeof(int));
        RID tempRID;
        for(int i = 0; i < ridCount; i++){
            memcpy(&tempRID, (char *)data + curOff + keyLength + (1 + 2 * i) * sizeof(int), 2 * sizeof(int));
            if(tempRID.pageNum == rid.pageNum && tempRID.slotNum == rid.slotNum){// If we find the rid
                void *temp2 = malloc(PAGE_SIZE);
                memset(temp2, 0, PAGE_SIZE);
                memcpy((char *)temp2, (char *)data, curOff + keyLength + (1 + 2 * i) * sizeof(int)); // copy the first half
                memcpy((char *)temp2 + curOff + keyLength + (1 + 2 * i) * sizeof(int),
                        (char *)data + curOff + keyLength + (1 + 2 * (i + 1)) * sizeof(int),
                        getFreeSpaceOffset(data) - curOff - keyLength - (1 + 2 * (i + 1)) * sizeof(int)); // copy the second half
                int newRIDCount = ridCount - 1;
                memcpy((char *)temp2 + curOff + keyLength, &newRIDCount, sizeof(int)); //change the rid count
                setLeaf(temp2);
                setKeysCount(temp2, getKeysCount(data));
                setFreeSpaceOffset(temp2, getFreeSpaceOffset(data) - 2 * sizeof(int));
                setLeftNode(temp2, getLeftNode(data));
                setRightNode(temp2, getRightNode(data));
                if(newRIDCount == 0){ //If ridCount decreases to 0, delete the key
                    memset(temp2, 0, PAGE_SIZE);
                    memcpy((char *)temp2, (char *)data, curOff);
                    memcpy((char *)temp2 + curOff, (char *)data + curOff + keyLength + 3 * sizeof(int),
                            getFreeSpaceOffset(data) - (curOff + keyLength + 3 * sizeof(int)));
                    setLeaf(temp2);
                    setKeysCount(temp2, getKeysCount(data) - 1);
                    setFreeSpaceOffset(temp2, getFreeSpaceOffset(data) - keyLength - 3 * sizeof(int));
                    setLeftNode(temp2, getLeftNode(data));
                    setRightNode(temp2, getRightNode(data));
                }
                if(getKeysCount(temp2) == 0){ // If keysCount decreases to 0, rewrite the left pointer of the
                                            //right page and right pointer of the left page
                    void *temp3 = malloc(PAGE_SIZE);
                    void *temp4 = malloc(PAGE_SIZE);
                    memset(temp3, 0, PAGE_SIZE);
                    memset(temp4, 0, PAGE_SIZE);
                    if(getLeftNode(data) != -1){
                        ixfileHandle.readPage(getLeftNode(data), temp3);
                        setRightNode(temp3, getRightNode(data));
                        ixfileHandle.writePage(getLeftNode(data), temp3);
                    }
                    if(getRightNode(data) != -1){
                        ixfileHandle.readPage(getRightNode(data), temp4);
                        setLeftNode(temp4, getLeftNode(data));
                        ixfileHandle.writePage(getRightNode(data), temp4);
                    }
                    free(temp3);
                    free(temp4);
                }

                ixfileHandle.writePage(pageNum, temp2);
                //
                free(data);
                free(temp);

                if(getKeysCount(temp2) == 0){
                    free(temp2);
                    return 1; //We need to do more work on the index nodes
                }
                free(temp2);
                return 0;
            }
        }
    }
    free(data);
    free(temp);
    return -1; //else return error
}


RC IndexManager::searchAndChange(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const int pageNum, const int fatherPageNum){
    void *data = malloc(PAGE_SIZE);
    memset(data, 0, PAGE_SIZE);
    ixfileHandle.readPage(pageNum, data);
    int keysCount = getKeysCount(data);

    if(checkLeaf(data) && keysCount > 0){ //used for terminating the recursion
        free(data);
        return 0;
    }
    else if(!checkLeaf(data) && keysCount > 0){
        int pos = searchPtrPosInIndex(data, attribute, key);
        int res;
        memcpy(&res, (char *)data + pos, sizeof(int));
        free(data);
        return searchAndChange(ixfileHandle, attribute, key, res, pageNum);
    }
    else if(!checkLeaf(data) && keysCount == 0){
        int temp; //this temp variable is the only one page number left in this page
        memcpy(&temp, (char *)data, sizeof(int));
        void *fatherPage = malloc(PAGE_SIZE);
        memset(fatherPage, 0, PAGE_SIZE);
        int pos = searchPtrPosInIndex(fatherPage, attribute, key);
        memcpy((char *)fatherPage + pos, &temp, sizeof(int));
        ixfileHandle.writePage(fatherPageNum, fatherPage);
        free(fatherPage);
        free(data);
        return searchAndChange(ixfileHandle, attribute, key, 0, -1);
    }
    else if(checkLeaf(data) && keysCount == 0){
        void *fatherPage = malloc(PAGE_SIZE);
        memset(fatherPage, 0, PAGE_SIZE);
        if(pageNum == 0){
            free(fatherPage);
            free(data);
            return 0;
        }
        else if(fatherPageNum == 0){
            if(keysCount == 1){
                int temp;
                memcpy(&temp, (char *)fatherPage, sizeof(int));
                if(temp == pageNum){
                    int attrLen = getAttributeLength(attribute, (char *)fatherPage + sizeof(int));
                    memcpy(&temp, (char *)fatherPage + attrLen, sizeof(int));
                }
                void *tempPage = malloc(PAGE_SIZE);
                memset(tempPage, 0, PAGE_SIZE);
                ixfileHandle.readPage(temp, tempPage);
                ixfileHandle.writePage(0, tempPage);
                free(fatherPage);
                free(tempPage);
                free(data);
                return 0;
            }
            else if(keysCount > 1){
                searchAndDeleteKey(fatherPage, attribute, key);
                ixfileHandle.writePage(fatherPageNum, fatherPage);
                free(fatherPage);
                free(data);
                return 0;
            }
        }
        else if(fatherPageNum > 0){
            searchAndDeleteKey(fatherPage, attribute, key);
            ixfileHandle.writePage(fatherPageNum, fatherPage);
            free(fatherPage);
            free(data);
            return searchAndChange(ixfileHandle, attribute, key, 0, -1);
        }
        free(fatherPage);
    }
    free(data);
    return -1; //else return error
}


//Search the position of the pointer for the specified key in the specified index
RC IndexManager::searchPtrPosInIndex(void *data, const Attribute &attribute, const void *key){
    int keysCount = getKeysCount(data);
    void *temp;
    int res; //result
    if(attribute.type == TypeVarChar){
        temp = malloc(sizeof(int) + attribute.length);
        memset(temp, 0, sizeof(int) + attribute.length);
    }
    else{
        temp = malloc(4);
    }
    int curOff = sizeof(int);
    int tempKeyLen;
    for(int i = 0; i < keysCount; i++){
        tempKeyLen = getAttributeLength(attribute, (char *)data + curOff);
        if(attribute.type == TypeVarChar){
            memset(temp, 0, sizeof(int) + attribute.length);
        }
        memcpy((char *)temp, (char *)data + curOff, tempKeyLen);

        if(isSmaller(attribute, key, temp)){ //key is smaller than temp key we get
            break;
        }

        else{ //key is bigger than or equal to the temp key we get, continue for loop
            curOff  += (tempKeyLen + sizeof(int));
        }
    }
    //whether we find or cannot find a key bigger than the given key in the current node,
    //we can use the position of the left pointer of the key we found, or use the position
    //of the right pointer of the last key in the index as the result, which should be curOff - sizeof(int)
    res = curOff - sizeof(int);
    free(temp);
    return res;
}

//search and delete a key and a pointer in the index (corresponding to the given key)
RC IndexManager::searchAndDeleteKey(void *data, const Attribute &attribute, const void *key){
    int keysCount = getKeysCount(data);
    void *temp;
    if(attribute.type == TypeVarChar){
        temp = malloc(sizeof(int) + attribute.length);
        memset(temp, 0, sizeof(int) + attribute.length);
    }
    else{
        temp = malloc(4);
    }
    int curOff = sizeof(int);
    int oldCurOff = curOff;
    int begin = -1;
    int end;
    int tempKeyLen;
    for(int i = 0; i < keysCount; i++){
        tempKeyLen = getAttributeLength(attribute, (char *)data + curOff);
        if(attribute.type == TypeVarChar){
            memset(temp, 0, sizeof(int) + attribute.length);
        }
        memcpy((char *)temp, (char *)data + curOff, tempKeyLen);

        if(isSmaller(attribute, key, temp)){ //key is smaller than temp key we get
            begin = curOff - sizeof(int);
            end = curOff + tempKeyLen;
            break;
        }

        else{ //key is bigger than or equal to the temp key we get, continue for loop
            oldCurOff = curOff - sizeof(int);
            curOff  += (tempKeyLen + sizeof(int));
        }
    }
    //whether we find or cannot find a key bigger than the given key in the current node,
    //we can use the position of the left pointer of the key we found, or use the position
    //of the right pointer of the last key in the index as the result, which should be curOff - sizeof(int)
    if(begin == -1){//(Didn't find the key)
        begin = oldCurOff - sizeof(int);
        end = curOff - sizeof(int);
    }
    void *tempPage = malloc(PAGE_SIZE);
    memset(tempPage, 0, PAGE_SIZE);
    memcpy((char *)tempPage, (char *)data, begin);
    memcpy((char *)tempPage + begin, (char *)data + end, getFreeSpaceOffset(data) - end);
    setIndex(tempPage);
    setFreeSpaceOffset(tempPage, getFreeSpaceOffset(data) - (end - begin));
    setKeysCount(tempPage, getKeysCount(data) - 1);
    memcpy((char *)data, (char *)tempPage, PAGE_SIZE);
    free(tempPage);
    free(temp);
    return 0;
}





void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {

    if(ixfileHandle.getNumberOfPages()==0){
        printf("{\n}\n");
        return;
    }
    void * root = malloc(PAGE_SIZE);
    memset(root,0,PAGE_SIZE);
    ixfileHandle.readPage(0,root);
    int level = 0;
    printHelper(ixfileHandle, attribute, root, level, true);
    free(root);
    cout<<endl;
    return;
}

void IndexManager::printHelper(IXFileHandle &ixfileHandle, const Attribute &attribute, void * node, int level, bool lastChild) const{
    if(checkLeaf(node)){
        printLeafNode(attribute, node, level,lastChild);
        return;
    }
    printIndexNodeHeader(attribute, node, level);
    int numOfKeys = getKeysCount(node);
    int offset = 0;
    for(int i=0; i<=numOfKeys; i++){
        void * newNode = malloc(PAGE_SIZE);
        memset(newNode,0,PAGE_SIZE);
        int keyLen = 0;
        if(i>0){
            keyLen = getAttributeLength(attribute,(char*)node+offset);
        }
        offset += keyLen;
        int newPageNum = *(int*)((char*)node+offset);
        offset += sizeof(int);
        ixfileHandle.readPage(newPageNum,newNode);
        if(i == numOfKeys){
            printHelper(ixfileHandle, attribute, newNode, ++level, true);
        }else{
            printHelper(ixfileHandle, attribute, newNode, ++level, false);
        }
        --level;
        free(newNode);
    }
    printIndexNodeTailer(attribute, node, level, lastChild);
    return;
}

void IndexManager::printLeafNode(const Attribute &attribute, void * data, int level, bool lastChild) const{
    int tabCount = level;
    printf("\n");
    while(tabCount>0){
        printf("\t");
        tabCount--;
    }
    printf("{\"keys\": [");
    int numOfKeys = getKeysCount(data);
    for(int i=0; i<numOfKeys; i++){
        printLeafNodeKeyWithRID(attribute, data, i);
        if(i<numOfKeys-1)
            printf(",");
    }

    printf("]}");
    if(!lastChild)
        printf(",");
}

void IndexManager::printLeafNodeKeyWithRID(const Attribute &attribute, void * data, int keyID) const{
    int offset = getOffsetOfSpecificKey(attribute, keyID, data);
    switch(attribute.type){
        case TypeVarChar:{
            int len = *(int*)((char*)data+offset);
            string s = string((char*)((char*)data+offset+sizeof(int)),len);
            cout<<"\""<<s<<":[";
            break;
        }
        case TypeInt:
            cout<<"\""<<*(int*)((char*)data+offset)<<":[";
            break;
        case TypeReal:
            cout<<"\""<<*(float*)((char*)data+offset)<<":[";
            break;
    }

    int keyLen = getAttributeLength(attribute,(char*)data + offset);
    int numOfRID = *(int*)((char*)data + offset + keyLen);
    for(int j=0; j<numOfRID; j++){
        cout<<"(";
        cout<<*(int*)((char*)data + offset + keyLen + sizeof(int)*(1+j*2));
        cout<<",";
        cout<<*(int*)((char*)data + offset + keyLen + sizeof(int)*(1+j*2+1));
        cout<<")";
        if(j<numOfRID-1)
            cout<<",";
    }

    cout<<"]\"";
    return;
}

void IndexManager::printIndexNodeHeader(const Attribute &attribute, void * data, int level) const{
    int tabCount = level;
    printf("\n");
    while(tabCount>0){
        printf("\t");
        tabCount--;
    }
    printf("{\"keys\": [");
    int numOfKeys = getKeysCount(data);
    int offset = sizeof(int);
    for(int i=0; i<numOfKeys; i++){
        switch(attribute.type){
            case TypeVarChar:{
                int len = *(int*)((char*)data+offset);
                string s = string((char*)((char*)data+offset+sizeof(int)),len);
                cout<<"\""<<s<<"\"";
                offset += (len+2*sizeof(int));
                break;
            }
            case TypeInt:
                cout<<"\""<<*(int*)((char*)data+offset)<<"\"";
                offset += 2*sizeof(int);
                break;
            case TypeReal:
                cout<<"\""<<*(float*)((char*)data+offset)<<"\"";
                offset += 2*sizeof(int);
                break;
        }
        if(i < (numOfKeys-1)){
            cout<<",";
        }
    }
    printf("],\n");
    tabCount = level;
    while(tabCount>0){
        printf("\t");
        tabCount--;
    }
    printf("\"children\": [");
}

void IndexManager::printIndexNodeTailer(const Attribute &attribute, void * data, int level, bool lastChild) const{
    printf("\n");
    while(level>0){
        printf("\t");
        level--;
    }
    printf("]}");
    if(!lastChild)
        printf(",");
}






int IndexManager::getNextPageNumInIndexPage(void* pageStartPosPointer, int offset){
    return *(int*)((char*)pageStartPosPointer+offset-sizeof(int));
}

/*
 * Find the Leaf Page which contains the given key starting from root.
 */
int IndexManager::searchTillLeafPage(IXFileHandle &ixfileHandle,const Attribute attribute,const void * key,int pageNum)
{
    void * data=malloc(PAGE_SIZE);
    memset(data, 0 ,PAGE_SIZE);
    ixfileHandle.readPage(pageNum,data);

    if(checkLeaf(data)) {
        free(data);
        return pageNum;
    }

    int freeSpaceOffset = getFreeSpaceOffset(data);
    int currentOffset = sizeof(int);    // The first int is the left pointer of the first key!

    while(currentOffset<freeSpaceOffset && isSmaller(attribute,(char *)data+currentOffset,key)){

        currentOffset += getAttributeLength(attribute,(char *)data+currentOffset) + sizeof(int);
    }

    int newPageNum = getNextPageNumInIndexPage(data, currentOffset);
    free(data);
    return searchTillLeafPage(ixfileHandle,attribute,key,newPageNum);
}

int IndexManager::findLeftMostLeafPage(IXFileHandle &ixfileHandle, int pageNum){
    // cout<<"IndexManager::findLeftMostLeafPage()------------"<<endl;
    void * data=malloc(PAGE_SIZE);
    memset(data, 0 ,PAGE_SIZE);
    ixfileHandle.readPage(pageNum,data);

    if(checkLeaf(data)){
        // cout<<"In checkLeaf(data)"<<endl;
        return pageNum;
    }

    int newPageNum = *(int*)((char*)data);  // Get the left most pointer
    // cout<<"newPageNum = "<<newPageNum<<endl;
    free(data);
    return findLeftMostLeafPage(ixfileHandle,newPageNum);
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    // cout<<"In IndexManager::scan--------------------"<<endl;
    // cout<<"ixfileHandle.getNumberOfPages() = "<<ixfileHandle.getNumberOfPages()<<endl;
    
    if(ixfileHandle.getFile()==NULL || ixfileHandle.getNumberOfPages()==0)
        return -1;

    ix_ScanIterator.ixfileHandle = ixfileHandle;
    ix_ScanIterator.attribute = attribute;
    ix_ScanIterator.highKey = malloc(PAGE_SIZE);
    memset(ix_ScanIterator.highKey,0,PAGE_SIZE);
    ix_ScanIterator.lowKey = malloc(PAGE_SIZE);
    memset(ix_ScanIterator.lowKey,0,PAGE_SIZE);
    ix_ScanIterator.highKeyInclusive = highKeyInclusive;
    ix_ScanIterator.lowKeyInclusive = lowKeyInclusive;

    if(highKey == NULL){
        ix_ScanIterator.highKey = NULL;
    }else{
        int len = getAttributeLength(attribute, highKey);
        memcpy(ix_ScanIterator.highKey, highKey, len);
    }

    if(lowKey == NULL){
        ix_ScanIterator.lowKey = NULL;
    }else{
        int len = getAttributeLength(attribute, lowKey);
        memcpy(ix_ScanIterator.lowKey, lowKey, len);
    }


    if(lowKey != NULL){
        ix_ScanIterator.nextPageNum = searchTillLeafPage(ixfileHandle,attribute,lowKey,0);
    }else{
        // cout<<"Before findLeftMostLeafPage------------"<<endl;
        ix_ScanIterator.nextPageNum = findLeftMostLeafPage(ixfileHandle,0);
        // cout<<"nextPage = "<<ix_ScanIterator.nextPageNum<<endl;
    }

    ix_ScanIterator.nextKey = malloc(PAGE_SIZE);
    memset(ix_ScanIterator.nextKey, 0, PAGE_SIZE);
    ix_ScanIterator.nextPageData = malloc(PAGE_SIZE);
    memset(ix_ScanIterator.nextPageData, 0, PAGE_SIZE);

    ixfileHandle.readPage(ix_ScanIterator.nextPageNum, ix_ScanIterator.nextPageData);

    int numOfKeys = getKeysCount(ix_ScanIterator.nextPageData);
    // cout<<"Page "<<ix_ScanIterator.nextPageNum<<" has "<<numOfKeys<<" keys."<<endl;

    if(numOfKeys >= 0){
        int firstKeyLen = getAttributeLength(attribute, ix_ScanIterator.nextPageData);
        memcpy((char*)ix_ScanIterator.nextKey, (char*)ix_ScanIterator.nextPageData, firstKeyLen);
        // cout<<"first key = "<<*(float*)ix_ScanIterator.nextKey<<endl;
        ix_ScanIterator.nextKeyNum = 1;
        ix_ScanIterator.nextRIDNumOfNextKey = 1;
    }else{
        // cout<<"return -1"<<endl;
        return -1;
    }

/*    int firstKeyLen = getAttributeLength(attribute, ix_ScanIterator.nextPageData);
    memcpy((char*)ix_ScanIterator.nextKey, (char*)ix_ScanIterator.nextPageData, firstKeyLen);
    cout<<"first key = "<<*(float*)ix_ScanIterator.nextKey<<endl;
    ix_ScanIterator.nextKeyNum = 1;
    ix_ScanIterator.nextRIDNumOfNextKey = 1;*/

    return 0;

}



IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{

}

void IX_ScanIterator::printBasicInfo(){
    cout<<endl;
    cout<<"############Basic Info of This getNextEntry()###################"<<endl;
    cout<<"next page number is "<<nextPageNum<<endl;
    cout<<"next key number is "<<nextKeyNum<<endl;
    cout<<"next rid number is "<<nextRIDNumOfNextKey<<endl;
    cout<<"next key is ";
    switch(attribute.type){
        case TypeVarChar:{
            int len = *(int*)((char*)nextKey);
            string s = string((char*)((char*)nextKey+sizeof(int)),len);
            cout<<s<<endl;
            break;
        }
        case TypeInt:
            cout<<*(int*)((char*)nextKey)<<endl;
            break;
        case TypeReal:
            cout<<*(float*)((char*)nextKey)<<endl;
            break;
    }
    cout<<"#################################################################"<<endl;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    // cout<<"IX_ScanIterator::getNextEntry------------------"<<endl;
    // printBasicInfo();
    IndexManager *ixm = IndexManager::instance();
    int numOfKeys = ixm->getKeysCount(nextPageData);
    int nextLeafPageNum = ixm->getRightNode(nextPageData);
    // cout<<"Right Neighbor Page Number is "<<nextLeafPageNum<<endl;

    int nextKeyOffset = getOffsetOfSpecificKey(attribute,nextKeyNum-1,nextPageData);
    // cout<<"next key offset is "<<nextKeyOffset<<endl;
    int nextKeyLen = getAttributeLength(attribute,(char*)nextPageData + nextKeyOffset);
    // cout<<"next key length is "<<nextKeyLen<<endl;
    int numOfRidOfKey = *(int*)((char*)nextPageData + nextKeyOffset + nextKeyLen);
    // cout<<"total number of rid corresponding to the key is "<<numOfRidOfKey<<endl;

    // cout<<"validateKey() = "<<(bool)validateKey()<<endl;
    // cout<<"(numOfRidOfKey < nextRIDNumOfNextKey) = "<<(bool)(numOfRidOfKey < nextRIDNumOfNextKey)<<endl;
    while(!validateKey() || (numOfRidOfKey < nextRIDNumOfNextKey)){
        nextKeyNum++;
        if(nextKeyNum > numOfKeys){
            if(nextLeafPageNum == -1){
                return IX_EOF;
            }

            nextPageNum = nextLeafPageNum;
            ixfileHandle.readPage(nextPageNum, nextPageData);
            int numOfKeysInNextPage = ixm->getKeysCount(nextPageData);
            if(numOfKeysInNextPage > 0){
                int firstKeyLen = getAttributeLength(attribute, nextPageData);
                memcpy((char*)nextKey, nextPageData, firstKeyLen);
                nextKeyNum = 1;
                nextRIDNumOfNextKey = 1;
                numOfRidOfKey = *(int*)((char*) nextPageData + firstKeyLen);
                nextLeafPageNum = ixm->getRightNode(nextPageData);
            }else{
                return IX_EOF;
            }
        }else{
            int offset = getOffsetOfSpecificKey(attribute, nextKeyNum-1, nextPageData);
            // cout<<"key's offset = "<<offset<<endl;
            int keyLen = getAttributeLength(attribute,(char*)nextPageData+offset);
            memcpy((char*)nextKey, (char*)nextPageData + offset, keyLen);
            nextRIDNumOfNextKey = 1;
            numOfRidOfKey = *(int*)((char*) nextPageData + offset + keyLen);
        }
    }

    getRID(ixm, attribute, nextKeyNum-1, nextRIDNumOfNextKey-1, rid, nextPageData, key);
    nextRIDNumOfNextKey++;
    // cout<<"rid.pageNum = "<<rid.pageNum<<endl;
    // cout<<"rid.slotNum = "<<rid.slotNum<<endl;
    // cout<<"IX_ScanIterator::getNextEntry() END------------------------"<<endl;
    return 0;
}

void IX_ScanIterator::getRID(IndexManager * ixm, const Attribute &attribute, int keyID, int RidID, RID &rid, void * pageData, void * key){
    // cout<<"IX_ScanIterator::getRID()----------------------------------"<<endl;
    int numOfKeys = ixm->getKeysCount(pageData);
    int offset = 0;
    int currentKeyID = 0;
    // cout<<"keyID = "<<keyID<<endl;
    // cout<<"RidID = "<<RidID<<endl;
    if(keyID < numOfKeys){
        int keyLen = getAttributeLength(attribute, (char*)pageData+offset);
        while(currentKeyID < keyID){
            keyLen = getAttributeLength(attribute, (char*)pageData+offset);
            int RidLen = *(int*)((char*)pageData+offset+keyLen);
            offset += (keyLen + sizeof(int)*(1 + 2*RidLen));
            currentKeyID++;
        }
        int numOfRidOfKey = *(int*)((char*) nextPageData + offset + keyLen);
        if(RidID < numOfRidOfKey){
            // cout<<"should be here!!!!!!!!!!!"<<endl;
            rid.pageNum = *(int*)((char*) nextPageData + offset + keyLen + sizeof(int)*(1+RidID*2));
            rid.slotNum = *(int*)((char*) nextPageData + offset + keyLen + sizeof(int)*(1+RidID*2+1));
            memcpy((char*)key,(char*)nextPageData + offset, keyLen);
        }
    }
    // cout<<"IX_ScanIterator::getRID() END---------------------------"<<endl;
}

bool IX_ScanIterator::validateKey(){
    // cout<<"In IX_ScanIterator::validateKey()------------------------"<<endl;
    if(lowKey==NULL && highKey==NULL)
        return true;

    bool low_flag = true, high_flag = true;
    int keyLength = getAttributeLength(attribute, nextKey);

    if(lowKey!=NULL){
        if(isSmaller(attribute,nextKey, lowKey))
            low_flag = false;
        if(lowKeyInclusive && (memcmp(lowKey, nextKey, keyLength)==0))
            low_flag = true;
    }

    if(highKey!=NULL){
        if(isSmaller(attribute,highKey,nextKey))
            high_flag = false;
        if(highKeyInclusive && (memcmp(nextKey, highKey, keyLength)==0))
            high_flag = true;
    }

    return low_flag && high_flag;
}


RC IX_ScanIterator::close()
{
    free(lowKey);
    free(highKey);
    free(nextPageData);
    free(nextKey);
    return 0;
}


IXFileHandle::IXFileHandle()
{
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{

    _fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);

    return 0;
}

RC IXFileHandle::openFile(const string &fileName){
    return PagedFileManager::instance()->openFile(fileName.c_str(), _fileHandle);
}

RC IXFileHandle::closeFile(){
    return PagedFileManager::instance()->closeFile(_fileHandle);
}

unsigned IXFileHandle::getNumberOfPages(){
    return _fileHandle.getNumberOfPages();
}

RC IXFileHandle::readPage(PageNum pageNum, void *data)
{
    return _fileHandle.readPage(pageNum, data);
}

RC IXFileHandle::writePage(PageNum pageNum, const void *data)
{
    return _fileHandle.writePage(pageNum, data);
}

RC IXFileHandle::appendPage(const void *data)
{
    return _fileHandle.appendPage(data);
}

FILE* IXFileHandle::getFile(){
    return _fileHandle.getFile();
}

void IX_PrintError (RC rc)
{
}
