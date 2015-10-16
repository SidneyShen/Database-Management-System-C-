#include "pfm.h"
#include <iostream>
#include <stdlib.h>
#include <string>
PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
    FILE* pFile = fopen(fileName.c_str(), "r+b");
    if(pFile == NULL){
        pFile = fopen(fileName.c_str(), "w+b");
        fclose(pFile);
        return 0;
    }
    // perror("Create File Error: File already exists!");
    return -1;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    FILE* pFile = fopen(fileName.c_str(), "r+b");

    if(pFile==NULL){// File does not exist
        // perror("Destroy File Error: File does not exist!");
    }

    if(pFile != NULL){  // File exists
        fclose(pFile);
        int res = remove(fileName.c_str());
        if(0 == res)    // Delete succeed
            return 0;
    }

    // perror("Destroy Failed!");
    return -1;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    // cout<<"In PagedFileManager::openFile!"<<endl;
    FILE* file = fileHandle.getFile();
    if(file!=NULL){
        // perror("Open File Error: FileHandle is already a handle for some open file ");
        return -1;
    } 
    file = fopen(fileName.c_str(), "r+b");
    if(file==NULL){
        // perror("Open File Error: File does not exist!");
        return -1;
    }
    fileHandle.markFile(file);
    return 0;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    FILE* file = fileHandle.getFile();
    if(file==NULL){
        // perror("Close File Error: File is not being using");
        return -1;
    }
    fflush(file);
    fclose(file);
    fileHandle.markFile(NULL);
    return 0;
}


FileHandle::FileHandle()
{
	readPageCounter = 0;
	writePageCounter = 0;
	appendPageCounter = 0;
    pFile = NULL;
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{   
    if(pFile!=NULL){
        if(pageNum<0){
            perror("Could not read page with negative page number!");
            return -1;
        }
        if(pageNum+1 > getNumberOfPages()){
            perror("The current page number exceeds the total page number!");
            return -1;
        }
        int res = fseek(pFile, pageNum * PAGE_SIZE, SEEK_SET);
        // cout<<"In ReadPage Function: res = "<<res<<endl;
        if(res == 0){
            fread(data, 1, PAGE_SIZE, pFile);
            rewind(pFile);
            readPageCounter = readPageCounter + 1;
            return 0;
        }
    }        
    perror("Read Page Error!");
    return -1;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    if(pFile!=NULL){
        if(pageNum<0){
            perror("Could not write page with negative page number!");
            return -1;
        }
        if(pageNum+1 > getNumberOfPages()){
            perror("The current page number exceeds the total page number!");
            return -1;
        }
        int res = fseek(pFile, pageNum*PAGE_SIZE, SEEK_SET);
        if(res == 0){
            fwrite(data,1,PAGE_SIZE, pFile);
            rewind(pFile);
            writePageCounter = writePageCounter+1;
            return 0;
        }
    }
    perror("Write Page Error!");
    return -1;
}


RC FileHandle::appendPage(const void *data)
{
    if(pFile!=NULL){
        int res = fseek(pFile, 0, SEEK_END);
        if(res == 0){
            fwrite(data, 1, PAGE_SIZE,pFile);
            appendPageCounter = appendPageCounter+1;
            return 0;
        }
    }
    perror("Append Page Error!");
    return -1;
}


unsigned FileHandle::getNumberOfPages()
{
    if(pFile!=NULL){
        int res = fseek(pFile, 0, SEEK_END); // Set the current position to the end of file;
        if(res == 0)
            return (unsigned)ftell(pFile)/PAGE_SIZE;   // Get number of pages;
    }
    perror("Get Number of Page Error!");
    return -1;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
	return 0;
}

FILE* FileHandle::getFile(){
    return pFile;
}

void FileHandle::markFile(FILE* _file){
    pFile = _file;
}
