
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>

#include "i_f_eng.c"

long test_fptr;

time_t start_time,end_time;
FILE *stream_file,*sort_temp_file,*attributes_file;
char indexed_file_id[128],stream_file_id[128],attributes_file_id[128];
char *the_record;
char attributes_record[128];
char temp_string[16];
int errcode,file_handle;
unsigned short record_length,key_position,key_length;
unsigned char no_of_keys,key_sub;
time_t start_time, end_time;
char sort_temp_file_id[]="i_f_utl2.tmp",*temp_key,*temp_key_2;
long record_count,records_processed;
struct sort_temp_type  {     /* The fptr storage in the the sort-temp file. */
    long original_fptr;  /* This is where this key came from in the file. */
    long parent_fptr,left_fptr,right_fptr;  /* These will be the pointers for this key. */
    }  sort_temp;
struct merge_temp_type  {  /* Used for holding fptr in merge temp files. */
    long original_fptr;
    } merge_temp;
struct stream_record_descriptor_type  { /* This structure precedes the     */
    unsigned short record_length;  }    /* record data in the stream file. */
    stream_record_descriptor;
int merge_file_number,  /* The number of the extension to use.      */
    current_merge_file, /* Which file in the list to merge a record from. */
    merge_file_to_read_from; /* Same as the file extension (0 is 1st).  */
char merge_file_name[]="i_f_utl2.",merge_file_id[16],extension_string[4];
struct memory_list_type  {   /* Put the key info. after this in memory. */
    long original_fptr;
    struct memory_list_type *next;
    char *key_ptr;   /* Holds pointer to key allocated for this node. */
    }  *memory_list_head_ptr,*memory_list_new_ptr,
       *memory_list_current_ptr,*memory_list_previous_ptr;
struct merge_temp_file_list_type  {
    FILE *file_ptr;
    struct merge_temp_file_list_type *next;
    }  *merge_temp_file_head_ptr,*merge_temp_file_current_ptr,
       *merge_temp_file_new_ptr,*merge_temp_file_prev_ptr,
       *merge_temp_file_delete_ptr;


/* -------------------------  subroutines  ---------------------------------*/

fn_open_merge_temp_file(char init_flag)
                /* If non-zero, then start with temp. file extension of ".0".*/
{               /*  Assumes old merge temp file list has been deallocated.   */
if (init_flag)  {
    merge_temp_file_head_ptr = NULL;
    merge_file_number = 0;  }  /* Start at file ext. of ".0". */
strcpy(merge_file_id,merge_file_name);
sprintf(extension_string,"%d",merge_file_number++);
/* itoa(merge_file_number++,extension_string,10); */
strcat(merge_file_id,extension_string);
merge_temp_file_new_ptr = (struct merge_temp_file_list_type *)malloc(sizeof(struct merge_temp_file_list_type));
if (merge_temp_file_new_ptr == NULL)
	return(I_F_ERROR_OUTOFMEMORY);
printf("---Opening merge temp file %s.\n",merge_file_id);
merge_temp_file_new_ptr->file_ptr = fopen(merge_file_id,"wb");
if (merge_temp_file_new_ptr == NULL)  /* Problem creating temp file? */
	return(I_F_ERROR_BADFILENAME);
merge_temp_file_new_ptr->next =  NULL; /* So list will always be consistent. */
if (merge_temp_file_head_ptr == NULL)  /* Add to the list. */
    merge_temp_file_head_ptr = merge_temp_file_new_ptr;
else
    merge_temp_file_current_ptr->next = merge_temp_file_new_ptr; /* Stick on end of list. */
merge_temp_file_current_ptr = merge_temp_file_new_ptr;  /* Always leave set to new file's pointer. */
return(I_F_SUCCESS);
}

void fn_close_merge_temp_file()
{
fclose(merge_temp_file_current_ptr->file_ptr);
}


get_numeric_input()
{
char the_input[8];
int temp;
gets(the_input);
temp = atoi(the_input);
return(temp);
}


/*--------------------------- Subroutines -----------------------------------*/

void fn_traverse_and_write_to_merge_file()
{
memory_list_current_ptr = memory_list_head_ptr;
while (memory_list_current_ptr != NULL)  {
    merge_temp.original_fptr = memory_list_current_ptr->original_fptr;
    fwrite(&merge_temp,1,sizeof(struct merge_temp_type),merge_temp_file_current_ptr->file_ptr);
    fwrite(memory_list_current_ptr->key_ptr,1,key_definition.length,merge_temp_file_current_ptr->file_ptr);
    memory_list_previous_ptr = memory_list_current_ptr;
    memory_list_current_ptr = memory_list_current_ptr->next;
    free(memory_list_previous_ptr->key_ptr);
    free(memory_list_previous_ptr);
    }
memory_list_head_ptr = NULL;  /* Make the list empty! */
}


void sort_the_temp_file()
/* Read info. from sort temp file into memory until memory full or  */
/*  done.  Sort info. in memory and write out a sort merge file.    */
/*  If done, then quit; if not, then continue reading from sort     */
/*  temp file and creating sort merge files until whole sort temp   */
/*  file is read.  Then merge sort merge files, creating sort temp  */
/*  file again.                                                     */
{
long start_of_info_marker;
sort_temp_file = fopen(sort_temp_file_id,"rb");  /* The file we're sorting. */
fn_open_merge_temp_file(1);  /* Open first merge temp file. */
memory_list_head_ptr = NULL;  /* Make sure the list is empty. */
records_processed = 0;  /* Read all the records from the sort temp file. */
while (records_processed++ < record_count)
  {
  start_of_info_marker = ftell(sort_temp_file);   /* In case we run out of memory. */
  fread(&sort_temp,sizeof(struct sort_temp_type),1,sort_temp_file);
  memory_list_new_ptr = (struct memory_list_type *)malloc(sizeof(struct memory_list_type));
  if (memory_list_new_ptr != NULL)  /* Able to allocate structure? */
    {
    memory_list_new_ptr->key_ptr = (char *)malloc(key_definition.length);
    if (memory_list_new_ptr->key_ptr == NULL) { /* Unable to allocate key? */
	    free(memory_list_new_ptr);
	    memory_list_new_ptr = NULL;  }
    }
  if (memory_list_new_ptr == NULL)  {
    /* Sort info in memory, then write to temp file. */
    fn_traverse_and_write_to_merge_file();
    fn_close_merge_temp_file();  /* Close current merge temp file. */
    fn_open_merge_temp_file(0);
    /* Set things back to where we can try this record again. */
    fseek(sort_temp_file,start_of_info_marker,SEEK_SET);  /* Move back. */
    test_fptr = ftell(sort_temp_file);
    records_processed--;
    continue;  }
  memory_list_new_ptr->original_fptr = sort_temp.original_fptr;  /* ...which we first fread. */
  memory_list_new_ptr->next = NULL;
  fread(memory_list_new_ptr->key_ptr,key_definition.length,1,sort_temp_file);
  /* The new node is loaded; now insert in list. */
  if (memory_list_head_ptr == NULL)
    memory_list_head_ptr = memory_list_new_ptr;
  else
    {  /* Insert the new node at the appropriate (sorted) place. */
    memory_list_current_ptr = memory_list_head_ptr;
    memory_list_previous_ptr = NULL;  /* If we have a logic error we should get "null pointer assignment". */
    while (1)
	    {  /* Go until we reach a key which is bigger than the new one. */
	    if (memcmp(memory_list_new_ptr->key_ptr,memory_list_current_ptr->key_ptr,key_definition.length) < 0)
        {   /* Insert before this node. */
        memory_list_new_ptr->next = memory_list_current_ptr;
        if (memory_list_previous_ptr == NULL)  /* 1st item in list? */
          memory_list_head_ptr = memory_list_new_ptr;
        else
          memory_list_previous_ptr->next = memory_list_new_ptr;
        break;
        }
	    if (memory_list_current_ptr->next == NULL)  {  /* Append to end of list? */
        memory_list_current_ptr->next = memory_list_new_ptr;
        break;  }
	    memory_list_previous_ptr = memory_list_current_ptr;
	    memory_list_current_ptr = memory_list_current_ptr->next;
	    }
    }
  }
fn_traverse_and_write_to_merge_file();
fn_close_merge_temp_file();
fclose(sort_temp_file);

/* Merge the merge files together and create a new sort temp file. */
merge_temp_file_current_ptr = merge_temp_file_head_ptr;
merge_file_number = 0;    /* Start at file ext. of ".0". */
while (merge_temp_file_current_ptr != NULL)    /* Open merge files. */
  {
  strcpy(merge_file_id,merge_file_name);
  sprintf(extension_string,"%d",merge_file_number++);
  strcat(merge_file_id,extension_string);
  merge_temp_file_current_ptr->file_ptr = fopen(merge_file_id,"rb");
  if (merge_temp_file_current_ptr->file_ptr == NULL)
    printf("!!!Problem opening merge temp file %s\n",merge_file_id);
  merge_temp_file_current_ptr = merge_temp_file_current_ptr->next;
  }
sort_temp_file = fopen(sort_temp_file_id,"wb");  /* Open for write. */

while (1)  /* Do this until no more entries exist in list of merge files. */
  {
  memset(temp_key,255,key_definition.length);  /* Find a key <= this one. */
  current_merge_file = 0;      /* Start with the first file in the list. */
  merge_file_to_read_from = 0; /* Which file still in the list to read from; 0 means quit. */
  merge_temp_file_current_ptr = merge_temp_file_head_ptr;
  while (merge_temp_file_current_ptr != NULL)  /* Check each merge file. */
    {
    current_merge_file++;   /* Read next record from each merge file. */
    start_of_info_marker = ftell(merge_temp_file_current_ptr->file_ptr);
    if (!fread(&merge_temp,sizeof(struct merge_temp_type),1,merge_temp_file_current_ptr->file_ptr))  {  /* End of merge file? */
	    /* Point around the current item in the list of merge files. */
	    if (merge_temp_file_current_ptr == merge_temp_file_head_ptr)
        merge_temp_file_head_ptr = merge_temp_file_current_ptr->next;
	    else
        merge_temp_file_prev_ptr->next = merge_temp_file_current_ptr->next;
	    fclose(merge_temp_file_current_ptr->file_ptr);
	    merge_temp_file_delete_ptr = merge_temp_file_current_ptr;
	    merge_temp_file_current_ptr = merge_temp_file_current_ptr->next;
	    free(merge_temp_file_delete_ptr);
	    break;  }  /* Go start on this record again after deleting merge file from list. */
	/* Check this merge record to see if it's the one we should choose. */
	fread(temp_key_2,key_definition.length,1,merge_temp_file_current_ptr->file_ptr);
	if (memcmp(temp_key_2,temp_key,key_definition.length) <= 0)  {
    merge_file_to_read_from = current_merge_file; /* Smallest so far.*/
    memcpy(temp_key,temp_key_2,key_definition.length);  }
	fseek(merge_temp_file_current_ptr->file_ptr,start_of_info_marker,SEEK_SET);
	merge_temp_file_prev_ptr = merge_temp_file_current_ptr;
	merge_temp_file_current_ptr = merge_temp_file_current_ptr->next;
	}
  /* We've checked all the merge files in the list. */
  if (merge_temp_file_head_ptr == NULL)  /* No merge files left? */
    break;   /* Done! */
  if (merge_file_to_read_from == 0)  /* Did we just delete a merge file from the list? */
    continue;  /* That's the only way we'd get here w/o a merge file to read from.   */
  merge_temp_file_current_ptr = merge_temp_file_head_ptr;
  current_merge_file = 1;
  while (current_merge_file++ < merge_file_to_read_from)
    merge_temp_file_current_ptr = merge_temp_file_current_ptr->next;
  fread(&merge_temp,sizeof(struct merge_temp_type),1,merge_temp_file_current_ptr->file_ptr);
  fread(temp_key,key_definition.length,1,merge_temp_file_current_ptr->file_ptr);
  sort_temp.original_fptr = merge_temp.original_fptr;
  sort_temp.parent_fptr = 0;
  sort_temp.left_fptr = 0;
  sort_temp.right_fptr = 0;
  fwrite(&sort_temp,1,sizeof(struct sort_temp_type),sort_temp_file);
  fwrite(temp_key,1,key_definition.length,sort_temp_file);
  }
/* Done combining merge files back into sort temp file. */
fclose(sort_temp_file);
}


long fill_in_fptrs(long low_record_no,long high_record_no,long parent_fptr)  /* Returns record's fptr. */
{
struct sort_temp_type sort_temp2;  /* We need a copy of this for each call. */
long current_record_no;

current_record_no = (low_record_no + high_record_no) / 2; /* Figure middle record #. */

fseek(sort_temp_file,(current_record_no - 1) * (sizeof(struct sort_temp_type)+key_definition.length),SEEK_SET);
fread(&sort_temp2,1,sizeof(struct sort_temp_type),sort_temp_file);
sort_temp2.parent_fptr = parent_fptr;  /* Set this one first. */
if ((current_record_no - 1) < low_record_no)
	sort_temp2.left_fptr = 0;
else
    sort_temp2.left_fptr = fill_in_fptrs(low_record_no,current_record_no - 1,sort_temp2.original_fptr);
if ((current_record_no + 1) > high_record_no)
	sort_temp2.right_fptr = 0;
else
    sort_temp2.right_fptr = fill_in_fptrs(current_record_no + 1,high_record_no,sort_temp2.original_fptr);
fseek(sort_temp_file,(current_record_no - 1) * (sizeof(struct sort_temp_type)+key_definition.length),SEEK_SET);
fwrite(&sort_temp2,1,sizeof(struct sort_temp_type),sort_temp_file);
printf(".");
return(sort_temp2.original_fptr);
}


/*--------------------------- The Code --------------------------------------*/
int convert_indexed_to_stream(char indexed_file_name[],
                              unsigned char key_to_read_by,
                              char stream_file_name[],
                              char attributes_file_name[])
{
unsigned char temp;

errcode = indexed_file(&file_handle,I_F_OPEN_READ,indexed_file_name,0);
if (errcode)  {
	printf("?Couldn't open file %s; error code: %d\n",errcode);
	return(errcode);  }
errcode = indexed_file(&file_handle,I_F_REPORT_ATTRIBUTES,"",0);
if (errcode)  {
	printf("?Couldn't report file attributes; error code: %d\n",errcode);
	return(errcode);  }
record_length = i_f_next_file_attribute(1);
no_of_keys = i_f_next_file_attribute(0);
temp = key_to_read_by;
while (temp)  {
	i_f_next_file_attribute(0);  /* Skip key position. */
	i_f_next_file_attribute(0);  /* Skip length. */
	i_f_next_file_attribute(0);  /* Skip duplicates flag. */
	temp--;  }
key_position = i_f_next_file_attribute(0);
key_length = i_f_next_file_attribute(0);
attributes_file = fopen(attributes_file_name,"wt");  /* Write out attributes file for future reference. */
fputs(I_F_FILE_ATTRIBUTES,attributes_file);
fclose(attributes_file);
if (record_length)
	the_record = (char *)malloc(record_length);
else
	the_record = (char *)malloc(65536);
if (the_record == NULL)  {
	printf("?Not enough memory to allocate record.\n");
  indexed_file(&file_handle,I_F_CLOSE,"",0);
	return(I_F_ERROR_OUTOFMEMORY);  }
memset(the_record+key_position,0,key_length);  /* Set this key to nulls. */
if (indexed_file(&file_handle,I_F_START,the_record,key_to_read_by))  {
  printf("?No records found for key %d!\n",key_to_read_by);
  indexed_file(&file_handle,I_F_CLOSE,"",0);
	return(I_F_SUCCESS);  }
stream_file = fopen(stream_file_name,"wb");  /* File to write to. */
start_time = time(NULL);
while (!indexed_file(&file_handle,I_F_READNEXT,the_record,0))
	{
  printf(".");
  if (record_length)
    stream_record_descriptor.record_length = record_length;
  else
    stream_record_descriptor.record_length = I_F_RECORD_LENGTH;
	fwrite(&stream_record_descriptor,sizeof(struct stream_record_descriptor_type),1,stream_file);
	fwrite(the_record,stream_record_descriptor.record_length,1,stream_file);
	}
indexed_file(&file_handle,I_F_CLOSE,"",0);
free(the_record);
fclose(stream_file);
end_time = time(NULL);
printf("\nStream format file %s created; it took %d seconds.\n",stream_file_id,end_time - start_time);
return(I_F_SUCCESS);
}


int create_empty_indexed_file(char indexed_file_name[],char attributes_file_name[])
{
FILE *attributes_file;

attributes_file=fopen(attributes_file_name,"rt");
if (attributes_file == NULL)        {
	printf("?Error opening attributes file %s\n",attributes_file_name);
	return(I_F_ERROR_BADFILENAME);  }
fgets(I_F_FILE_ATTRIBUTES,sizeof(I_F_FILE_ATTRIBUTES),attributes_file);
fclose(attributes_file);
printf("Creating empty file %s with attributes: %s\n",indexed_file_name,I_F_FILE_ATTRIBUTES);
errcode = indexed_file(&file_handle,I_F_OPEN_WRITE,indexed_file_name,0);
if (errcode)  {
	printf("?create-empty-indexed-file error: %d\n",errcode);
	printf("--> Check documentation on file attributes carefully!\n");
	return(errcode);  }
indexed_file(&file_handle,I_F_CLOSE,"",0);
printf("Empty indexed file created!\n");
return(I_F_SUCCESS);
}


int convert_stream_to_indexed(char stream_file_name[],char indexed_file_name[])
{
long bytes_in_stream_file,bytes_in_indexed_file,current_indexed_file_fptr;
unsigned short bytes_read;

stream_file = fopen(stream_file_name,"rb");
if (stream_file == NULL)  {
	printf("?Unable to open stream file for input.\n");
	return(I_F_ERROR_BADFILENAME);  }
fseek(stream_file,0,SEEK_END);
bytes_in_stream_file = ftell(stream_file);
fseek(stream_file,0,SEEK_SET);

errcode = indexed_file(&file_handle,I_F_OPEN_IO,indexed_file_name,0);
if (errcode)  {
	fclose(stream_file);
	printf("?Error opening empty indexed file; error code: %d\n",errcode);
	return(errcode);  }
fn_read_file_header();
for (key_sub=0;key_sub<file_header.no_of_keys;key_sub++)
	{
	fn_read_key_definition(key_sub);
	if (key_definition.root_fptr)  {
		printf("?Indexed file to be loaded is not empty or has a bad format!\n");
		fclose(stream_file);
		indexed_file(&file_handle,I_F_CLOSE,"",0);
		return(I_F_ERROR_BADFILENAME);  }
	}
fseek(open_file_list_current_ptr->file_ptr,0,SEEK_END);  /* Check filesize. */
if (ftell(open_file_list_current_ptr->file_ptr) != sizeof(struct file_header_type) + (file_header.no_of_keys * sizeof(struct key_definition_type)))  {
	printf("?Indexed file to be loaded is not empty or has a bad format!\n");
	printf(" Its size has been calculated to be incorrect for an empty file.\n");
	fclose(stream_file);
	indexed_file(&file_handle,I_F_CLOSE,"",0);
	return(I_F_ERROR_BADFILENAME);  }

start_time = time(NULL);    /* Keep track of when we started this analysis. */
printf("===Loading indexed file %s from stream file %s.\n",indexed_file_name,stream_file_name);
/* Here, write the data out to the empty indexed file with no pointer info.. */
printf("Writing records with empty pointer data to indexed file.\n");
record_count = 0;  /* Count here the number of records we're loading. */
while (ftell(stream_file) < bytes_in_stream_file)
	{
	printf(".");
	fseek(open_file_list_current_ptr->file_ptr,0,SEEK_END);  /* Get ready to write data to indexed file (w/0 pointers). */
	current_indexed_file_fptr = ftell(open_file_list_current_ptr->file_ptr);
	bytes_read = fread(&stream_record_descriptor,1,sizeof(struct stream_record_descriptor_type),stream_file);
	if (bytes_read < sizeof(struct stream_record_descriptor_type))  {
		printf("?Premature end of stream input file while reading record length\n");
		printf(" Only %u bytes were read.\n",bytes_read);
		fclose(stream_file);
		indexed_file(&file_handle,I_F_CLOSE,"",0);
		return(I_F_ERROR_FILECORRUPTED);  }
		the_record = (char *)malloc(stream_record_descriptor.record_length);
    if (the_record == NULL)  {
        printf("?Not enough dynamic memory; aborting.\n");
        fclose(stream_file);
        indexed_file(&file_handle,I_F_CLOSE,"",0);
		return(I_F_ERROR_OUTOFMEMORY);  }
    bytes_read = fread(the_record,1,stream_record_descriptor.record_length,stream_file);
    if (bytes_read < stream_record_descriptor.record_length)  {
		printf("?Premature end of stream input file while reading record data\n");
        printf(" Only %u bytes were read.\n",bytes_read);
        fclose(stream_file);
        indexed_file(&file_handle,I_F_CLOSE,"",0);
		return(I_F_ERROR_INTERNAL);  }
	if (file_header.record_length)    /* Fixed length records? */
		{
		if (stream_record_descriptor.record_length != file_header.record_length) {
			printf("?Invalid record length of %u specified in stream file\n",stream_record_descriptor.record_length);
			fclose(stream_file);
			indexed_file(&file_handle,I_F_CLOSE,"",0);
			return(I_F_ERROR_INTERNAL);  }
		}
	else         /* Gotta set this for variable length records!. */
        I_F_RECORD_LENGTH = stream_record_descriptor.record_length;
    for (key_sub=0;key_sub<file_header.no_of_keys;key_sub++)  {
        keys_pointers.parent_fptr = 0;
        keys_pointers.left_fptr   = 0;
		keys_pointers.right_fptr  = 0;
        fn_write_keys_pointers(current_indexed_file_fptr,key_sub);  }
    fn_write_record_info(current_indexed_file_fptr,the_record);
    free(the_record);
    record_count++;   /* We need this record count for later. */
    }
printf("\n");
/* Done loading record data into indexed file. */
fclose(stream_file);  /* Done with this file. */

/* For each key, get fptrs and key from indexed file, sort, build */
/*  list (figuring fptrs), and write fptrs back to indexed file.  */
for (key_sub=0;key_sub<file_header.no_of_keys;key_sub++)
    {
    printf("---Processing key %d\n",key_sub);
    fn_read_key_definition(key_sub);

    temp_key = (char *)malloc(key_definition.length);  /* We'll use this   */
    if (temp_key == NULL)  {                           /*  here and there. */
        indexed_file(&file_handle,I_F_CLOSE,"",0);
		printf("?Not enough memory to allocate temp. storage for key\n");
		return(I_F_ERROR_OUTOFMEMORY);  }
    temp_key_2 = (char *)malloc(key_definition.length);
    if (temp_key_2 == NULL)  {
        indexed_file(&file_handle,I_F_CLOSE,"",0);
        printf("?Not enough memory to allocate temporary storage for key\n");
		return(I_F_ERROR_OUTOFMEMORY);  }

    /* Read in key info. (key value and fptr from indexed file). */
    /*  Create a sort temp file with this info..                 */
    fseek(open_file_list_current_ptr->file_ptr,sizeof(struct file_header_type) + (file_header.no_of_keys * sizeof(struct key_definition_type)),SEEK_SET);
	sort_temp_file = fopen(sort_temp_file_id,"wb");
    records_processed = 0;
    printf("Reading key information from indexed file....\n");
    while (records_processed++ < record_count)
        {
        printf(".");
        current_indexed_file_fptr = ftell(open_file_list_current_ptr->file_ptr);
		if (file_header.record_length)
            the_record = (char *)malloc(file_header.record_length);
        else  {
            fn_read_record_descriptor(current_indexed_file_fptr);
            the_record = (char *)malloc(record_descriptor.record_length);  }
        if (the_record == NULL)  {
			printf("?Not enough memory to allocate record\n");
            indexed_file(&file_handle,I_F_CLOSE,"",0);
			return(I_F_ERROR_OUTOFMEMORY);  }
		fn_read_record_info(current_indexed_file_fptr,the_record);
		sort_temp.original_fptr = current_indexed_file_fptr;
		/* Don't bother writing anything in the key pointers area. */
        fwrite(&sort_temp,sizeof(struct sort_temp_type),1,sort_temp_file);
        fwrite(the_record+key_definition.position,key_definition.length,1,sort_temp_file);
        free(the_record);
        }
    printf("\n");
    fclose(sort_temp_file);  /* Done creating this. */

    printf("Sorting temp file....\n");
	sort_the_temp_file();  /* Sort this by key. */

    /* Read middle item in temp file, then item in middle of each half, */
    /*  etc., recursively, while filling in parent, left, and right     */
    /*  fptrs to create the balanced key tree for this key.             */
	sort_temp_file = fopen(sort_temp_file_id,"rb+");
    printf("Balancing tree for this key.\n");
    key_definition.root_fptr = fill_in_fptrs((long)1,record_count,(long)0); /* Recursive.*/
    printf("\n");
    fn_write_key_definition(key_sub);

    printf("Root pointer for this key: %ld\n",key_definition.root_fptr);

    /* Write pointer data back out to indexed file. */
    fseek(sort_temp_file,0,SEEK_SET);
    records_processed = 0;
    printf("Writing pointer data back to indexed file....\n");
    while (records_processed++ < record_count)
		{
        printf(".");
        fread(&sort_temp,1,sizeof(struct sort_temp_type),sort_temp_file);
        fread(temp_key,1,key_definition.length,sort_temp_file);  /* This just moves pointer to next record. */
        keys_pointers.parent_fptr = sort_temp.parent_fptr;
        keys_pointers.left_fptr   = sort_temp.left_fptr;
		keys_pointers.right_fptr  = sort_temp.right_fptr;
		fn_write_keys_pointers(sort_temp.original_fptr,key_sub);
        }
	printf("\n");

	/* Great!  We're done processing this key. */
    fclose(sort_temp_file);
/*    unlink(sort_temp_file_id);  */
    free(temp_key);
	free(temp_key_2);
	printf("Done balancing key %d.\n",key_sub);
	}
indexed_file(&file_handle,I_F_CLOSE,"",0);
end_time = time(NULL);  /* How long did it take? */
printf("Done loading file; it took %d seconds.\n",end_time - start_time);
return(I_F_SUCCESS);
}


int convert_stream_to_indexed_2(char stream_file_name[],char indexed_file_name[])
{
long bytes_in_stream_file,bytes_in_indexed_file,current_indexed_file_fptr,
  temp_file_fptr;
unsigned short bytes_read;
FILE *temp_file;  /* We hold the fptrs to the records in the stream file here.*/
char temp_file_id[]="i_f_utl2.tmp";
struct temp_record_type {
  long the_fptr;
  } temp_record;

stream_file = fopen(stream_file_name,"rb");
if (stream_file == NULL)
  {
	printf("?Unable to open stream file for input.\n");
  return(I_F_ERROR_BADFILENAME);
  }
fseek(stream_file,0,SEEK_END);
bytes_in_stream_file = ftell(stream_file);
fseek(stream_file,0,SEEK_SET);

errcode = indexed_file(&file_handle,I_F_OPEN_IO,indexed_file_name,0);
if (errcode)
  {
	fclose(stream_file);
	printf("?Error opening empty indexed file; error code: %d\n",errcode);
  return(errcode);
  }
fn_read_file_header();
for (key_sub=0;key_sub<file_header.no_of_keys;key_sub++)
	{
	fn_read_key_definition(key_sub);
  if (key_definition.root_fptr)
    {
		printf("?Indexed file to be loaded is not empty or has a bad format!\n");
		fclose(stream_file);
    return(I_F_ERROR_BADFILENAME);
    }
	}
fseek(open_file_list_current_ptr->file_ptr,0,SEEK_END);  /* Check filesize. */
if (ftell(open_file_list_current_ptr->file_ptr) != sizeof(struct file_header_type) + (file_header.no_of_keys * sizeof(struct key_definition_type)))
  {
	printf("?Indexed file to be loaded is not empty or has a bad format!\n");
	printf(" Its size has been calculated to be incorrect for an empty file.\n");
	fclose(stream_file);
  return(I_F_ERROR_BADFILENAME);
  }

temp_file = fopen(temp_file_id,"wb");  /* Open for write. */

start_time = time(NULL);    /* Keep track of when we started this analysis. */
printf("===Write loading indexed file %s from stream file %s.\n",indexed_file_name,stream_file_name);
record_count = 0;  /* Count here the number of records we're loading. */
while (ftell(stream_file) < bytes_in_stream_file)
	{
  sprintf(temp_string,"%d",++record_count);
/*  itoa(++record_count,temp_string,10);  */
  printf("          %c%c%c%c%c%c%c%c%c%c%s%c",8,8,8,8,8,8,8,8,8,8,temp_string,13);
  temp_record.the_fptr = ftell(stream_file);  /* Save this fptr. */
  fwrite(&temp_record,1,sizeof(struct temp_record_type),temp_file);
  bytes_read = fread(&stream_record_descriptor,1,sizeof(struct stream_record_descriptor_type),stream_file);
  if (bytes_read < sizeof(struct stream_record_descriptor_type))
    {
		printf("?Premature end of stream input file while reading record length\n");
		printf(" Only %u bytes were read.\n",bytes_read);
		fclose(stream_file);
    fclose(temp_file);
		indexed_file(&file_handle,I_F_CLOSE,"",0);
    return(I_F_ERROR_FILECORRUPTED);
    }
  the_record = (char *)malloc(stream_record_descriptor.record_length);
  if (the_record == NULL)
    {
    printf("?Not enough dynamic memory; aborting.\n");
    fclose(stream_file);
    fclose(temp_file);
    indexed_file(&file_handle,I_F_CLOSE,"",0);
    return(I_F_ERROR_OUTOFMEMORY);
    }
  bytes_read = fread(the_record,1,stream_record_descriptor.record_length,stream_file);
  if (bytes_read < stream_record_descriptor.record_length)
    {
		printf("?Premature end of stream input file while reading record data\n");
    printf(" Only %u bytes were read.\n",bytes_read);
    fclose(stream_file);
    fclose(temp_file);
    indexed_file(&file_handle,I_F_CLOSE,"",0);
    return(I_F_ERROR_INTERNAL);
    }
  /* Check the record length. */
  if (file_header.record_length)    /* Fixed length records? */
		{
    if (stream_record_descriptor.record_length != file_header.record_length)
      {
			printf("?Invalid record length of %u specified in stream file\n",stream_record_descriptor.record_length);
			fclose(stream_file);
      fclose(temp_file);
			indexed_file(&file_handle,I_F_CLOSE,"",0);
      return(I_F_ERROR_INTERNAL);
      }
		}
  free(the_record);
  }
fclose(temp_file);    /* Done creating the temp file. */

temp_file = fopen(temp_file_id,"rb+");
srand(1);  /* Initialize random number generator. */

while (record_count)          /* Keep going until we're done. */
  {
  sprintf(temp_string,"%d",record_count);
/*  itoa(record_count,temp_string,10);  */
  printf("          %c%c%c%c%c%c%c%c%c%c%s%c",8,8,8,8,8,8,8,8,8,8,temp_string,13);
  temp_file_fptr = (rand() % record_count) * 4;
  fseek(temp_file,temp_file_fptr,SEEK_SET);
  fread(&temp_record,sizeof(struct temp_record_type),1,temp_file);
  fseek(stream_file,temp_record.the_fptr,SEEK_SET);
  bytes_read = fread(&stream_record_descriptor,1,sizeof(struct stream_record_descriptor_type),stream_file);
	if (bytes_read < sizeof(struct stream_record_descriptor_type))  {
		printf("?Premature end of stream input file while reading record length\n");
		printf(" Only %u bytes were read.\n",bytes_read);
		fclose(stream_file);
		indexed_file(&file_handle,I_F_CLOSE,"",0);
		return(I_F_ERROR_FILECORRUPTED);  }
  the_record = (char *)malloc(stream_record_descriptor.record_length);
  if (the_record == NULL)  {
    printf("?Not enough dynamic memory; aborting.\n");
    fclose(stream_file);
    indexed_file(&file_handle,I_F_CLOSE,"",0);
    return(I_F_ERROR_OUTOFMEMORY);  }
  bytes_read = fread(the_record,1,stream_record_descriptor.record_length,stream_file);
  if (bytes_read < stream_record_descriptor.record_length)
    {
		printf("?Premature end of stream input file while reading record data\n");
    printf(" Only %u bytes were read.\n",bytes_read);
    fclose(stream_file);
    indexed_file(&file_handle,I_F_CLOSE,"",0);
    return(I_F_ERROR_INTERNAL);
    }
  if (file_header.record_length)    /* Fixed length records? */
		{
    if (stream_record_descriptor.record_length != file_header.record_length)
      {
			printf("?Invalid record length of %u specified in stream file\n",stream_record_descriptor.record_length);
			fclose(stream_file);
			indexed_file(&file_handle,I_F_CLOSE,"",0);
      return(I_F_ERROR_INTERNAL);
      }
		}
	else         /* Gotta set this for variable length records!. */
    I_F_RECORD_LENGTH = stream_record_descriptor.record_length;
  errcode = indexed_file(&file_handle,I_F_WRITE,the_record,0);
  if (errcode)
    {
    printf("?Bad write to indexed file; errcode: %d\n",errcode);
    fclose(stream_file);
    fclose(temp_file);
    indexed_file(&file_handle,I_F_CLOSE,"",0);
    return(I_F_ERROR_INTERNAL);
    }
  free(the_record);
  /* Now, if we didn't just read the last 4 bytes of the file, we move them.  */
  if (temp_file_fptr != ((record_count - 1) * 4))   /* Not the last 4 bytes?  */
    {   /* We need to move last 4 bytes of file to wherever we just read from.*/
    fseek(temp_file,(record_count - 1) * 4,SEEK_SET);
    fread(&temp_record,sizeof(struct temp_record_type),1,temp_file);
    fseek(temp_file,temp_file_fptr,SEEK_SET);
    fwrite(&temp_record,sizeof(struct temp_record_type),1,temp_file);
    }
  record_count--;
  }
printf("\n");
fclose(stream_file);
fclose(temp_file);
unlink(temp_file_id);
indexed_file(&file_handle,I_F_CLOSE,"",0);
end_time = time(NULL);
printf("Done loading file; it took %d seconds.\n",end_time - start_time);
return(I_F_SUCCESS);
}



/*------------------------------ Main ----------------------------------------*/

int main(int argc,char *argv[])
{
unsigned char key_to_read;
char menu_command = ' ';
indexed_file(&file_handle,I_F_CLOSE_ALL,"",0);  /* Copyright notice. */

if (argc < 5)     /* This is all the input checking we have! */
  {
  printf("\n?Bad parameters; please enter:\n");
  printf("1st -- 1=indexed to stream, 2=create empty, 3=stream to indexed, 4=write load\n");
  printf("2nd -- indexed file id\n");
  printf("3rd -- stream file id\n");
  printf("4th -- attribute file id\n");
  printf("5th -- key to read indexed file by\n");
  return(I_F_ERROR_BADOPERATION);
  }

strcpy(indexed_file_id,argv[2]);
strcpy(stream_file_id,argv[3]);
strcpy(attributes_file_id,argv[4]);
key_to_read = (char)atoi(argv[5]);
printf("Number of parameters read from command line: %d\n",argc);
printf("#1 (operation):       %c\n",argv[1][0]);
printf(" 2 (indexed file id): %s\n",indexed_file_id);
printf(" 3 (stream file id):  %s\n",stream_file_id);
printf(" 4 (attrib file id):  %s\n",attributes_file_id);
printf(" 5 (key to read by):  %d\n",key_to_read);

menu_command = argv[1][0];  /* What operation to do? */
switch (menu_command)
    {
    case '1': errcode = convert_indexed_to_stream(indexed_file_id,key_to_read,stream_file_id,attributes_file_id); break;
    case '2': errcode = create_empty_indexed_file(indexed_file_id,attributes_file_id);                            break;
    case '3': errcode = convert_stream_to_indexed(stream_file_id,indexed_file_id);                                break;
    case '4': errcode = convert_stream_to_indexed_2(stream_file_id,indexed_file_id);                              break;
    default:  errcode = I_F_ERROR_BADOPERATION;
    }
return(errcode);

}

