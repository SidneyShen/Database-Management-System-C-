#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <climits>

#include "../rbf/pfm.h"

using namespace std;


// Record ID
typedef struct
{
  unsigned pageNum;	// page number
  unsigned slotNum; // slot number in the page
} RID;


// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef unsigned AttrLength;

struct Attribute {
    string   name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum { NO_OP = 0,  // no condition
		   EQ_OP,      // =
           LT_OP,      // <
           GT_OP,      // >
           LE_OP,      // <=
           GE_OP,      // >=
           NE_OP,      // !=
} CompOp;

typedef struct{
    int status; //"0"--delete, "1"--normal, "2"--migrated
                // if "2", slot info format: "2", "NewPageNum", "NewSlotID"
    int slotStartPos;
    int slotLength; //"0"--deleted record
}SlotInfo;



/****************************************************************************
The scan iterator is NOT required to be implemented for part 1 of the project 
*****************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();


class RBFM_ScanIterator {
public:
  RBFM_ScanIterator() {};
  ~RBFM_ScanIterator() {};

  // "data" follows the same format as RecordBasedFileManager::insertRecord()
  RC getNextRecord(RID &rid, void *data);
  RC close();

  FileHandle fileHandle;
  vector<Attribute> recordDescriptor;
  string conditionAttribute;
  int conditionAttributeID;
  AttrType conditionAttributeType;
  CompOp compOp;
  vector<string> attributeNames;
  void* value;
  RID nextRID;

public:

  int getResultData(void* recordData, void* data);
  int concatenateResult(void* attrData, AttrType type, int &offset, int attrCount, void* data);
  void gotoNextSlot(RID &rid);
  bool validateRecord(void* pageData, void* recordData);
  bool checkSlotStatus(void* pageData);
  bool checkRecordRequirement(void* pageData, void* recordData);
  bool checkAttribute(void* value, void* attrData);

  // int getConditionAttr(vector<Attribute> &recordDescriptor, string &conditionAttribute, RBFM_ScanIterator &rbfm_ScanIterator);
  // int getValue(void* value, RBFM_ScanIterator &rbfm_ScanIterator);
};


class RecordBasedFileManager
{
public:
  static RecordBasedFileManager* instance();

  RC createFile(const string &fileName);
  
  RC destroyFile(const string &fileName);
  
  RC openFile(const string &fileName, FileHandle &fileHandle);
  
  RC closeFile(FileHandle &fileHandle);

  //  Format of the data passed into the function is the following:
  //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
  //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
  //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
  //     Each bit represents whether each field contains null value or not.
  //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data.
  //     If k-th bit from the left is set to 0, k-th field contains non-null values.
  //     If thre are more than 8 fields, then you need to find the corresponding byte, then a bit inside that byte.
  //  2) actual data is a concatenation of values of the attributes
  //  3) For int and real: use 4 bytes to store the value;
  //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
  //  !!!The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute()
  // For example, refer to the Q8 of Project 1 wiki page.
  RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);

  RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);
  
  // This method will be mainly used for debugging/testing
  RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

/**************************************************************************************************************************************************************
IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for part 1 of the project
***************************************************************************************************************************************************************/
  RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

  // Assume the rid does not change after update
  RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

  RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data);

  // scan returns an iterator to allow the caller to go through the results one by one. 
  RC scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator);

protected:
  RecordBasedFileManager();
  ~RecordBasedFileManager();

public:
  static RecordBasedFileManager *_rbf_manager;

  int* getPageFreeSpacePointer(void* pageStartPosPointer);
  int getPageFreeSpacePos(void* pageStartPosPointer);
  int* getPageSLotCountPointer(void* pageStartPosPointer);
  int getPageSlotCount(void* pageStartPosPointer);
  SlotInfo* getSlotInfoPointer(void* pageStartPosPointer, int slotID);

  void initNewPageData(void* pageData);
  void insertRecordIntoPage(void* curPageData, const void* recordData, int recordLength, RID &rid);
  bool canInsert(void* pageData, int recordLength);
  int addHeaderToRecord(const vector<Attribute> &recordDescriptor, void * newData, const void *rawData);
  int insertRecordHelper(FileHandle &fileHandle, const void *newData, RID &rid, const int recordLength);
  void removeHeaderFromRecord(char* storedRecord, void* originalRecord);
  int readMigrateRecord(FileHandle &fileHandle, const int pageNumber, const int slotNumber, char* recordData);
  int deleteMigrateRecord(FileHandle &fileHandle, const int pageNumber, const int slotNumber);
  void expandFreeSpace(void* pageData);
  void shrinkFreeSpace(void* pageData, const int slotNumber, void* updateData, const int recordLength);
  int updateRecordInThisPage(FileHandle &fileHandle, void* pageData, const int slotNumber, void* updateData, const int recordLength);
  int updateMigrateRecord(FileHandle &fileHandle, const int pageNumber, const int slotNumber, void *updateData, const int recordLength);
  int readAttributeHelper(char* recordData, int attrID, void* data);
  int getConditionAttr(const vector<Attribute> &recordDescriptor, const string &conditionAttribute, RBFM_ScanIterator &rbfm_ScanIterator);
  int getValue(const void* value, RBFM_ScanIterator &rbfm_ScanIterator);

};

#endif
