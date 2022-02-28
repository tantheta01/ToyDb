
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "tbl.h"
#include "codec.h"
#include "../pflayer/pf.h"

#define SLOT_COUNT_OFFSET 2
#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(EXIT_FAILURE);}}

int  getLen(int slot, byte *pageBuf); UNIMPLEMENTED;
int  getNumSlots(byte *pageBuf); UNIMPLEMENTED;
void setNumSlots(byte *pageBuf, int nslots); UNIMPLEMENTED;
int  getNthSlotOffset(int slot, char* pageBuf); UNIMPLEMENTED;


/**
   Opens a paged file, creating one if it doesn't exist, and optionally
   overwriting it.
   Returns 0 on success and a negative error code otherwise.
   If successful, it returns an initialized Table*.
 */
int
Table_Open(char *dbname, Schema *schema, bool overwrite, Table **ptable)
{
    PF_Init();
    Table *t;
    t = malloc(sizeof(Table));
    t -> dbname = malloc(sizeof(dbname));
    t -> numpages = 0;
    strcpy(dbname, t->dbname);
    t -> schema  = malloc(sizeof(Schema));
    t -> schema -> numColumns = schema -> numColumns;
    t -> schema -> columns = malloc(schema->numColumns*sizeof(char*));
    int i=0;
    for(i=0;i<schema->numColumns; i++){
        t -> schema -> columns[i] = malloc(sizeof(schema -> columns[i]));
        strcpy(t->schema->columns[i], schema->columns[i]);
    }


    int fd = PF_OpenFile(t -> dbname);
    if(fd >= 0){
        if(overwrite){
            PF_CloseFile(fd);
            PF_DestroyFile(fd);
            fd = PF_CreateFile(dbname);
        }
    }
    else{
        fd = PF_CreateFile(dbname);
        t -> open_filedescriptor;
    }
    return 0;

    // name of file is dbname

    // Initialize PF, create PF file,
    // allocate Table structure  and initialize and return via ptable
    // The Table structure only stores the schema. The current functionality
    // does not really need the schema, because we are only concentrating
    // on record storage. 
}

void
Table_Close(Table *tbl) {
    int fd = tbl -> open_filedescriptor;
    PF_CloseFile(fd);
    

    // Unfix any dirty pages, close file.
}


int
Table_Insert(Table *tbl, byte *record, int len, RecId *rid) {

    // Allocate a fresh page if len is not enough for remaining space
    // Get the next free slot on page, and copy record in the free
    // space
    // Update slot and free space index information on top of page.
    // a page is a character array - how to check how much page is free
}

#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(EXIT_FAILURE);}}

/*
  Given an rid, fill in the record (but at most maxlen bytes).
  Returns the number of bytes copied.
 */
int
Table_Get(Table *tbl, RecId rid, byte *record, int maxlen) {
    int slot = rid & 0xFFFF;
    int pageNum = rid >> 16;

    UNIMPLEMENTED;
    // PF_GetThisPage(pageNum)
    // In the page get the slot offset of the record, and
    // memcpy bytes into the record supplied.
    // Unfix the page
    return len; // return size of record
}

void
Table_Scan(Table *tbl, void *callbackObj, ReadFunc callbackfn) {

    UNIMPLEMENTED;

    // For each page obtained using PF_GetFirstPage and PF_GetNextPage
    //    for each record in that page,
    //          callbackfn(callbackObj, rid, record, recordLen)
}


