#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>
#include "codec.h"
#include "../pflayer/pf.h"
#include "../amlayer/am.h"
#include "tbl.h"
#include "util.h"

#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(1);}}

#define MAX_PAGE_SIZE 4000


#define DB_NAME "data.db"
#define INDEX_NAME "data.db.0"
#define CSV_NAME "data.csv"


/*
Takes a schema, and an array of strings (fields), and uses the functionality
in codec.c to convert strings into compact binary representations
 */
int
encode(Schema *sch, char **fields, byte *record, int spaceLeft) {
    
    int tot_bytes = 0;
    for(int i=0;i < sch -> numColumns; i++){
        if(tot_bytes >= spaceLeft){
            break;
        }
        int type = sch -> columns[i];
        int totrecord = record+tot_bytes;
        int netspace = spaceLeft - tot_bytes;
        if(type == VARCHAR){
            tot_bytes = tot_bytes + EncodeCString(fields[i], totrecord, netspace);
        }
        if(type == INT)
        {
            tot_bytes = tot_bytes + EncodeInt(atoi(fields[i]), totrecord);
        }
        if(type == LONG){
            tot_bytes = tot_bytes + EncodeInt(atoll(fields[i]), totrecord);
        }
    }
    return tot_bytes;
    // for each field
    //    switch corresponding schema type is
    //        VARCHAR : EncodeCString
    //        INT : EncodeInt
    //        LONG: EncodeLong
    // return the total number of bytes encoded into record
}

Schema *
loadCSV() {
    // Open csv file, parse schema
    FILE *fp = fopen(CSV_NAME, "r");
    if (!fp) {
	perror("data.csv could not be opened");
        exit(EXIT_FAILURE);
    }

    char buf[MAX_LINE_LEN];
    char *line = fgets(buf, MAX_LINE_LEN, fp);
    if (line == NULL) {
	fprintf(stderr, "Unable to read data.csv\n");
	exit(EXIT_FAILURE);
    }

    // Open main db file
    Schema *sch = parseSchema(line);
    Table *tbl;
    tbl = malloc(sizeof(tbl));
    char *dbname = malloc(15*sizeof(char));
    dbname = "MyDatabase";
    tbl = Table_Open(dbname, sch, true, &tbl);

    char *tokens[MAX_TOKENS];
    char record[MAX_PAGE_SIZE];

    while ((line = fgets(buf, MAX_LINE_LEN, fp)) != NULL) {
	int n = split(line, ",", tokens);
	assert (n == sch->numColumns);
	int len = encode(sch, tokens, record, sizeof(record));
	RecId rid;

    Table_Insert(tbl, record, len, &rid);
    char isint = 'i';
    
	

	printf("%d %s\n", rid, tokens[0]);

	// Indexing on the population column 
	int population = atoi(tokens[2]);

	int err = AM_InsertEntry(tbl -> open_filedescriptor, isint, 4, (char *)&population, rid);
	// Use the population field as the field to index on
	    
	checkerr(err);
    }
    fclose(fp);
    Table_Close(tbl);
    // err = PF_CloseFile(indexFD);
    // checkerr(err);
    return sch;
}

int
main() {
    loadCSV();
}
