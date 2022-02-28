
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "tbl.h"
#include "codec.h"
#include "../pflayer/pf.h"

#define SLOT_COUNT_OFFSET 2
#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(EXIT_FAILURE);}}

int  getLen(int slot, byte *pageBuf){
    int nslots = *(int *)(pageBuf);
    if(slot > nslots){
        return -1;
    }
    slot -= 1;
    int slotOffset = *(int *)(pageBuf + 4*(2+slot));
    if(slot == nslots-1){
        return PF_PAGE_SIZE - slotOffset;
    }
    int nextoff = *(int *)(pageBuf + 4*(3+slot));
    return nextoff - slotOffset;
}
int  getNumSlots(byte *pageBuf){
    return *(int *)(pageBuf);
}
void setNumSlots(byte *pageBuf, int nslots){
    *(int *)(pageBuf) = nslots;
}
int  getNthSlotOffset(int slot, char* pageBuf){
    int nslots = *(int *)(pageBuf);
    if(slot > nslots){
        return -1;
    }
    slot -= 1;
    int slotOffset = *(int *)(pageBuf + 4*(2+slot));
    return slotOffset;
}


/**
   Opens a paged file, creating one if it doesn't exist, and optionally
   overwriting it.
   Returns 0 on success and a negative error code otherwise.
   If successful, it returns an initialized Table*.
 */
int
Table_Open(char *dbname, Schema *schema, bool overwrite, Table **ptable)
{
    // printf("Inside table open\n");
    PF_Init();
    Table *t = *ptable;
    // printf("55\n");
    // t = malloc(sizeof(Table));
    t -> dbname = malloc(sizeof(dbname)+1);
    t -> numpages = 0;
    t -> lastpagenumber = -1;
    t -> pagenumbers = malloc(100 * sizeof(int));
    
    strcpy(t->dbname, dbname);
    // printf("60\n");
    t -> schema  = malloc(sizeof(Schema));
    t -> schema -> numColumns = schema -> numColumns;
    t -> schema -> columns = malloc(schema->numColumns*sizeof(char*));
    int i=0;
    // printf("66\n");
    for(i=0;i<schema->numColumns; i++){
        t -> schema -> columns[i] = malloc(sizeof(schema -> columns[i]));
        t->schema->columns[i]->name = malloc(20*sizeof(char));
        // printf("70\n");
        strcpy(t->schema->columns[i]->name, schema->columns[i]->name);
        t->schema->columns[i]->type = schema->columns[i]->type;
    }


    int fd = PF_OpenFile(t -> dbname);
    // printf("77\n");
    if(fd >= 0){
        if(overwrite){
            PF_CloseFile(fd);
            PF_DestroyFile(t->dbname);
            fd = PF_CreateFile(dbname);
        }
        t -> open_filedescriptor = fd;
    }
    
    else{
        fd = PF_CreateFile(dbname);
        t -> open_filedescriptor = fd;
    }
    // printf("86\n");
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
    // printf("121\n");
    int *pagenum = malloc(sizeof(int));
    char **pageBuffer = malloc(sizeof(char*));
    // printf("124\n");
    if(tbl -> numpages == 0){
        

        PF_AllocPage(tbl->open_filedescriptor, pagenum, (char **)pageBuffer);
        // printf("126\n");
        // printf("The size of this buffer is::::%d\n", sizeof(pageBuffer[0][0]));
        // printf("The size f a char is %d\n", sizeof(char));
        // printf("The value of top is as given%d\n", *(int *)(pageBuffer + 4));
        *(int *)(pageBuffer+4) = PF_PAGE_SIZE - 8;
        // printf("129\n");
        *(int *)(pageBuffer) = 0;
        // printf("129\n");

    }
    else{
        // printf("Just before segmentation fault\n");

        *pagenum = tbl->lastpagenumber;
        PF_GetThisPage(tbl->open_filedescriptor, *pagenum, (char **)pageBuffer);
        int space_left = *(int *)(pageBuffer+4);
        if(space_left < len + 4){
            PF_UnfixPage(tbl->open_filedescriptor, *pagenum, 1);
            PF_AllocPage(tbl->open_filedescriptor, pagenum, pageBuffer);
            *(int *)(pageBuffer+4) = PF_PAGE_SIZE - 8;
            *(int *)(pageBuffer) = 0;
        }
        // printf("After Segmentation fault\n");
    }
    int number_slots = *(int *)(pageBuffer);
    int final_offset;
    if(number_slots == 0){
        final_offset = PF_PAGE_SIZE - len;
    }
    else{
        final_offset = *(int *)(pageBuffer + 4*(number_slots + 1)) - len;
    }
    *(int *)(pageBuffer) = number_slots+1;
    *(int *)(pageBuffer + 4*(number_slots+2)) = final_offset;
    // printf("163 printf\n");
    memcpy(pageBuffer + final_offset, record, len);
    // printf("haan ye bhi\n");
    *rid = ((*pagenum) << 16) + number_slots+1;
    // printf("segmentationfaultttttttt\n");
    // printf("Value of pagenumber isss%d\n\n", *pagenum);
    // printf("open file descriptor%d \n", tbl->open_filedescriptor);
    // PF_UnfixPage(tbl->open_filedescriptor, *pagenum,(1>0) );
    // printf("printing this also\n");
    tbl -> lastpagenumber = *pagenum;
    

}

#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(EXIT_FAILURE);}}

/*
  Given an rid, fill in the record (but at most maxlen bytes).
  Returns the number of bytes copied.
 */
int
Table_Get(Table *tbl, RecId rid, byte *record, int maxlen) {
    int slot = rid % (1<<16);
    int pageNum = rid >> 16;

    //UNIMPLEMENTED;
    // PF_GetThisPage(pageNum)
    // In the page get the slot offset of the record, and
    // memcpy bytes into the record supplied.
    // Unfix the page
    int fd = tbl -> open_filedescriptor;
    char **buffer = malloc(sizeof(char*));


    int getPage = PF_GetThisPage(fd,pageNum,buffer);
    int offset = getNthSlotOffset(slot,*buffer);

    //finding record size and checking if < maxlen
    int len;
    if(slot == 1)len = PF_PAGE_SIZE - offset;
    else len = (*(int *)(*buffer + 4*slot)) - offset;
    if(len > maxlen)len = maxlen;

    memcpy(record,*buffer+offset,len);
    PF_UnfixPage(fd,pageNum,TRUE);
    return len; // return size of record
}

void
Table_Scan(Table *tbl, void *callbackObj, ReadFunc callbackfn) {

    //UNIMPLEMENTED;
    // PF_GetThisPage  
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


