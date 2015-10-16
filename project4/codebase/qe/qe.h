#ifndef _qe_h_
#define _qe_h_

#include <limits>
#include <vector>
#include <utility> 
#include <string.h>
#include <iostream>
#include <unordered_set>
#include <unordered_map>
#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

#define QE_EOF (-1)  // end of the index scan

using namespace std;

typedef enum{ COUNT=0, SUM, AVG, MIN, MAX } AggregateOp;

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by
//                 the characters

struct Value {
    AttrType type;          // type of value
    void     *data;         // value
};


struct Condition {
    string  lhsAttr;        // left-hand side attribute
    CompOp  op;             // comparison operator
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string  rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};

struct AggVal{
    int count;
    float sum;
    float max;
    int min;
    AggVal():count(0),sum(0),max(-numeric_limits<float>::infinity()),min(999999)
    {}
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
        RelationManager &rm;
        RM_ScanIterator *iter;
        string tableName;
        vector<Attribute> attrs;
        vector<string> attrNames;
        RID rid;

        TableScan(RelationManager &rm, const string &tableName, const char *alias = NULL):rm(rm)
        {
        	//Set members
        	this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            unsigned i;
            for(i = 0; i < attrs.size(); ++i)
            {
                // convert to char *
                attrNames.push_back(attrs.at(i).name);
            }

            // Call rm scan to get iterator
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator()
        {
            iter->close();
            delete iter;
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
        };

        RC getNextTuple(void *data)
        {
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
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
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
        RelationManager &rm;
        RM_IndexScanIterator *iter;
        string tableName;
        string attrName;
        vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;

        IndexScan(RelationManager &rm, const string &tableName, const string &attrName, const char *alias = NULL):rm(rm)
        {
        	// Set members
        	this->tableName = tableName;
        	this->attrName = attrName;


            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void* lowKey,
                         void* highKey,
                         bool lowKeyInclusive,
                         bool highKeyInclusive)
        {
            iter->close();
            delete iter;
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
                           highKeyInclusive, *iter);
        };

        RC getNextTuple(void *data)
        {
            int rc = iter->getNextEntry(rid, key);
            if(rc == 0)
            {
                rc = rm.readTuple(tableName.c_str(), rid, data);
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
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~IndexScan()
        {
            iter->close();
        };
};


class Filter : public Iterator {
    // Filter operator
    public:
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        )
        {
            iter_filter = input;
            cond = condition;
        };
        ~Filter()
        {
            iter_filter = NULL;
        };

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
    private:
        Iterator *iter_filter;
        Condition cond;
};


class Project : public Iterator {
    // Projection operator
    public:
        Project(Iterator *input,                    // Iterator of input R
              const vector<string> &attrNames)   // vector containing attribute names
        {
            iter_proj = input;
            iter_proj->getAttributes(attributes);
            for(unsigned i=0; i<attrNames.size(); i++){
                projAttrNames.push_back(attrNames[i]);   
            }
        };
        ~Project()
        {
            iter_proj = NULL;
        };

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
    private:
        Iterator *iter_proj;
        vector<string> projAttrNames;
        vector<Attribute> attributes;
};

// Optional for the undergraduate solo teams. 5 extra-credit points
class BNLJoin : public Iterator {
    // Block nested-loop join operator
    public:
        BNLJoin(Iterator *leftIn,            // Iterator of input R
               TableScan *rightIn,           // TableScan Iterator of input S
               const Condition &condition,   // Join condition
               const unsigned numRecords     // # of records can be loaded into memory, i.e., memory block size (decided by the optimizer)
        ){
            this->leftIn = leftIn;
            this->rightIn = rightIn;
            this->condition = condition;
            this->numRecords = numRecords;

            leftIn->getAttributes(lhsAttributes);
            rightIn->getAttributes(rhsAttributes);

            isFirstCall = true;
            // Get join-key attribute type
            for(unsigned i=0; i<lhsAttributes.size(); i++){
                if(lhsAttributes[i].name == condition.lhsAttr){
                    this->joinKeyType = lhsAttributes[i].type;
                    break;
                }
            }

            rhsTupleData = malloc(PAGE_SIZE);
            memset(rhsTupleData,0,PAGE_SIZE);
        };  
        ~BNLJoin()
        {
            leftIn = NULL;
            rightIn = NULL;
            freeCurrentBlock();
            free(rhsTupleData);
        };

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;

        RC loadNextBlock();
        void freeCurrentBlock();
    private:
        Iterator* leftIn;
        TableScan* rightIn;
        Condition condition;
        unsigned numRecords;
        vector<void*> block;
        AttrType joinKeyType;
        vector<Attribute> lhsAttributes;
        vector<Attribute> rhsAttributes;
        bool isFirstCall;
        void *lhsTupleData;
        void *rhsTupleData;
        unsigned blockIterator;
};


class INLJoin : public Iterator {
    // Index nested-loop join operator
    public:
        INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition   // Join condition
        ){
            this->leftIn = leftIn;
            this->rightIn = rightIn;
            this->condition = condition;

            leftIn->getAttributes(lhsAttributes);
            rightIn->getAttributes(rhsAttributes);

            isFirstCall = true;
            // Get join-key attribute type
            for(unsigned i=0; i<lhsAttributes.size(); i++){
                if(lhsAttributes[i].name == condition.lhsAttr){
                    this->joinKeyType = lhsAttributes[i].type;
                    break;
                }
            }

            lhsTupleData = malloc(PAGE_SIZE);
            memset(lhsTupleData,0,PAGE_SIZE);
            rhsTupleData = malloc(PAGE_SIZE);
            memset(rhsTupleData,0,PAGE_SIZE);
        };
        ~INLJoin(){
            free(lhsTupleData);
            free(rhsTupleData);
        };

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
        RC setIndexScanIterator(void *lhsTupleData);
    private:
        Iterator* leftIn;
        IndexScan* rightIn;
        Condition condition; 
        AttrType joinKeyType;
        vector<Attribute> lhsAttributes;
        vector<Attribute> rhsAttributes;
        bool isFirstCall;
        void *lhsTupleData;
        void *rhsTupleData;
};

// Optional for everyone. 10 extra-credit points
class GHJoin : public Iterator {
    // Grace hash join operator
    public:
      GHJoin(Iterator *leftIn,               // Iterator of input R
            Iterator *rightIn,               // Iterator of input S
            const Condition &condition,      // Join condition (CompOp is always EQ)
            const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
      ){};
      ~GHJoin(){};

      RC getNextTuple(void *data){return QE_EOF;};
      // For attribute in vector<Attribute>, name it as rel.attr
      void getAttributes(vector<Attribute> &attrs) const{};
};

class Aggregate : public Iterator {
    // Aggregation operator
    public:
        // Mandatory for graduate teams only
        // Basic aggregation
        Aggregate(Iterator *input,          // Iterator of input R
                  Attribute aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        ){
            this->iter_agg = input;
            this->aggAttr = aggAttr;
            this->op = op;
            this->count = 0;
            this->returnValNull = true;
            this->hasReturned = false;
            this->returnVal = getBasicInfo();
            this->isGroup = false;
        };

        // Optional for everyone. 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  Attribute aggAttr,           // The attribute over which we are computing an aggregate
                  Attribute groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op              // Aggregate operation
        ){
            this->iter_agg = input;
            this->aggAttr = aggAttr;
            this->op = op;
            this->groupAttr = groupAttr;
            this->isGroup = true;
            getGroupInfo();
            this->returnVal = 0;
            this->returnValNull = true;
            groupKeyData = malloc(200);
            memset(groupKeyData,0,200);
        };
        ~Aggregate(){
            iter_agg = NULL;
            // free(groupKeyData);
            map_int.clear();
            map_float.clear();
            map_string.clear();
        };

        RC getNextTuple(void *data);
        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrname = "MAX(rel.attr)"
        void getAttributes(vector<Attribute> &attrs) const;

        float getBasicInfo();
        RC getNextTupleWithGroup(void *data);
        RC getNextTupleWithoutGroup(void *data);
        // void loadGroupInfo();
        float getBasicInfoGroup();
        // void gotoReset();
        void gotoInt();
        void gotoFloat();
        void gotoString();
        bool getResult(void *data);
        void getGroupInfo();
    private:
        Iterator *iter_agg;
        // TableScan *iter_group;
        Attribute aggAttr;
        AggregateOp op;
        int count;
        bool returnValNull;
        float returnVal;
        bool hasReturned;
        bool isGroup;
        Attribute groupAttr;
        unordered_set<int> set_int;
        unordered_set<float> set_float;
        unordered_set<string> set_string;
        void * groupKeyData;
        int groupKeyDataLength;
        unordered_map<int,AggVal> map_int;
        unordered_map<float,AggVal> map_float;
        unordered_map<string,AggVal> map_string;
        AggVal aggregateStruct;

};

#endif
