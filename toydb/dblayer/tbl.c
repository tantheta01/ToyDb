
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
    t -> lastpagenumber = -1;
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
    //table insert takes care of unfixing dirty pages. There can't be any more to unfix so directly closing file will work
}


int
Table_Insert(Table *tbl, byte *record, int len, RecId *rid) {

    // Allocate a fresh page if len is not enough for remaining space
    // Get the next free slot on page, and copy record in the free
    // space
    // Update slot and free space index information on top of page.
    // a page is a character array - how to check how much page is free
    int *pagenum = malloc(sizeof(int));
    char **pageBuffer;
    if(tbl -> numpages == 0){
        PF_AllocPage(tbl->open_filedescriptor, pagenum, pageBuffer);
        *(int *)(*pageBuffer+4) = PF_PAGE_SIZE - 8;
        *(int *)(*pageBuffer) = 0;

    }
    else{
        *pagenum = tbl->lastpagenumber;
        PF_GetThisPage(tbl->open_filedescriptor, *pagenum, (char **)pageBuffer);
        int space_left = *(int *)(*pageBuffer+4);
        if(space_left < len + 4){
            PF_UnfixPage(tbl->open_filedescriptor, *pagenum, 1);
            PF_AllocPage(tbl->open_filedescriptor, pagenum, pageBuffer);
            *(int *)(*pageBuffer+4) = PF_PAGE_SIZE - 8;
            *(int *)(*pageBuffer) = 0;
        }
    }
    int number_slots = *(int *)(*pageBuffer);
    int final_offset;
    if(number_slots == 0){
        final_offset = PF_PAGE_SIZE - len;
    }
    else{
        final_offset = *(int *)(*pageBuffer + 4*(number_slots + 1)) - len;
    }
    *(int *)(*pageBuffer) = number_slots+1;
    *(int *)(*pageBuffer + 4*(number_slots+2)) = final_offset;
    memcpy(*pageBuffer + final_offset, record, len);
    *rid = ((*pagenum) << 16) + number_slots+1;
    PF_UnfixPage(tbl->open_filedescriptor, *pagenum,1 );
    

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

    //UNIMPLEMENTED;
    // PF_GetThisPage(pageNum)
    // In the page get the slot offset of the record, and
    // memcpy bytes into the record supplied.
    // Unfix the page
    int fd = tbl -> open_filedescriptor;
    char **buffer = malloc(sizeof(char*));
    int *pageNo = malloc(sizeof(int)); //can use pageNum above too ig

    int getPage = PF_GetThisPage(fd,pageNo,buffer);
    int offset = getNthSlotOffset(slot,*buffer);

    //finding record size and checking if < maxlen
    int len;
    if(slot == 1)len = PF_PAGE_SIZE - offset;
    else len = (*(int *)(*buffer + 4*i)) - offset;
    if(len > maxlen)len = maxlen

    memcpy(record,*buffer+offset,len);
    PF_UnfixPage(fd,*pageNo,TRUE);
    return len; // return size of record
}

void
Table_Scan(Table *tbl, void *callbackObj, ReadFunc callbackfn) {

    //UNIMPLEMENTED;

    // For each page obtained using PF_GetFirstPage and PF_GetNextPage
    //    for each record in that page,
    //          callbackfn(callbackObj, rid, record, recordLen)
    int fd = tbl -> open_filedescriptor;
    char **buffer = malloc(sizeof(char*));
    int *pageNo = malloc(sizeof(int));

    //to store page obtained
    int getPage = PF_GetFirstPage(fd,pageNo,buffer);
    while(getPage >= 0){
        int slots = getNumSlots(*buffer);
        //for each record, do callback function
        for(int i = 1; i <= slots; i++){
            //get offset for that i
            int offset = getNthSlotOffset(i,*buffer);
            int recordLen;
            if(i == 1)recordLen = PF_PAGE_SIZE - offset;
            else recordLen = (*(int *)(*buffer + 4*i)) - offset; //cehck this
            int rid = ((*pageNo) << 16) + i;
            char record[PF_PAGE_SIZE];
            memcpy(record,*buffer + offset, recordLen); //store in record array
            callbackfn(callbackObj, rid, record, recordLen);
        }
        PF_UnfixPage(fd,*pageNo,TRUE);
        getPage = PF_GetNextPage(fd,pageNo,buffer);
    }
}


