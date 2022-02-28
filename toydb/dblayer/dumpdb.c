#include <stdio.h>
#include <stdlib.h>
#include "codec.h"
#include "tbl.h"
#include "util.h"
#include "../pflayer/pf.h"
#include "../amlayer/am.h"
#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(1);}}


void
printRow(void *callbackObj, RecId rid, byte *row, int len) {
    Schema *schema = (Schema *) callbackObj;
    byte *cursor = row;
    //UNIMPLEMENTED;
    int cur = 0,type;
    int numColumns = schema->numColumns;
    for(int i = 0; i < numColumns; i++){
        type = schema->columns[i]->type;
        if(type == INT){
            printf("%d",DecodeInt(cursor+cur));
            //four bytes shift
            cur += 4;
        }
        else if(type == LONG){
            printf("%lld",DecodeLong(cursor+cur));
            //eight bytes shift
            cur += 8;
        }
        else if(type == VARCHAR){
            char s[len];
            // 2 for ' and number of bytes according to the string shifted
            cur += (2 + DecodeCString(cursor+cur,s,len-cur));
            printf("%s",s); 
        }
        if(i != numColumns-1){
            printf(",")
        }
    }
    printf("\n");
}

#define DB_NAME "data.db"
#define INDEX_NAME "data.db.0"
	 
void
index_scan(Table *tbl, Schema *schema, int indexFD, int op, int value) {

    int fd = tbl->open_filedescriptor;
    int scanner = AM_OpenIndexScan(fd,'i',4,op,char*(&value));
    while(true){
        int recid = AM_FindNextEntry(scanner);
        if(recid == AME_EOF)break;
        char record[PF_PAGE_SIZE];
        int len = Table_Get(tbl,recid,record,PF_PAGE_SIZE);
        printRow(schema,recid,record,len);
    }
    AM_CloseIndexScan(scanner);
    //UNIMPLEMENTED;
    /*
    Open index ...
    while (true) {
	find next entry in index
	fetch rid from table
        printRow(...)
    }
    close index ...
    */

}

int
main(int argc, char **argv) {
    char *schemaTxt = "Country:varchar,Capital:varchar,Population:int";
    Schema *schema = parseSchema(schemaTxt);
    Table *tbl;

    //UNIMPLEMENTED;
    if (argc == 2 && *(argv[1]) == 's') {
	//UNIMPLEMENTED;
    Table_Scan(tbl,schema,printRow);
	// invoke Table_Scan with printRow, which will be invoked for each row in the table.
    } else {
	// index scan by default
	int indexFD = PF_OpenFile(INDEX_NAME);
	checkerr(indexFD);

	// Ask for populations less than 100000, then more than 100000. Together they should
	// yield the complete database.
	index_scan(tbl, schema, indexFD, LESS_THAN_EQUAL, 100000);
	index_scan(tbl, schema, indexFD, GREATER_THAN, 100000);
    }
    Table_Close(tbl);
}
