
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <iostream>
#include <queue>
#include "../rbf/rbfm.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data) ;
  RC close() ;
  RC scan(FileHandle &fileHandle,
          const vector<Attribute> &recordDescriptor,
          const string &conditionAttribute,
          const CompOp compOp,                  // comparision type such as "<" and "="
          const void *value,                    // used in the comparison
          const vector<string> &attributeNames);

private:
  RBFM_ScanIterator rbfm_ScanIterator;
};


// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createCatalog();

  int loadTableID();

  int calcLength(const vector<Attribute> &attrs);

  queue<unsigned char> initialNullDescriptor(const vector<Attribute> &descriptor);

  RC initialTablesTable();

  RC addTablesColumn(const string &tableName);

  RC initialColumnsTable();

  RC addColumnsColumn(const string &tableName, const vector<Attribute> &attrs);

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // mainly for debugging
  // Print a tuple that is passed to this utility method.
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  // mainly for debugging
  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // scan returns an iterator to allow the caller to go through the results one by one.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);

// Extra credit work (10 points)
public:
  RC dropAttribute(const string &tableName, const string &attributeName);

  RC addAttribute(const string &tableName, const Attribute &attr);

  int getTableID(const string &tableName);

  RC addColumnsColumn(const string &tableName, const Attribute &attr, const int &tableID);

  RC updateColumnsTuple(const void *data, const RID &rid);


protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;
  RecordBasedFileManager *rbfm;
  vector<Attribute> tablesDescriptor;
  vector<Attribute> columnsDescriptor;
  int MAX_TableID;
};

#endif
