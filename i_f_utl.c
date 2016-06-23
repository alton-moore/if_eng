
/* This is a utility program which uses only the functions made available  */
/*  by the i_f_eng object code.  It does not utilize any of the knowledge  */
/*  about the file which the routines have.  It does:                      */
/*    - File integrity checking: compares record counts for all keys       */
/*    - Thrashing (read/write testing for single/multiple users)           */

/* This source code is released as one example of how  */
/*  to use the indexed-file routine.                   */

/* This source code is copyright 1991 by Alton Moore,  */
/*  Rt. 5, Box 694-A, Edinburg, TX  78539-9805.        */
/*  This source code may be distributed freely but may */
/*  not be distributed if modified in any way.         */


#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>

#include "i_f_eng.c"

char file_name[128] = "i_f_utl.ife",*the_record,temp_string[16];
int errcode, file_handle;
long record_count;
unsigned short record_length,key_position,key_length,duplicates_flag,no_of_keys;
time_t start_time,end_time;
unsigned char key_sub;


/*---------------------------- Subroutines --------------------------------*/

get_numeric_input()
{
char the_input[8];
int temp;
gets(the_input);
temp = atoi(the_input);
return(temp);
}


void analyze_file_integrity()
{
char *hold_key;
start_time = time(NULL);    /* Keep track of when we started this analysis. */
errcode = indexed_file(&file_handle,I_F_OPEN_READ,file_name,0);
if (errcode)
  {
  printf("?Couldn't open file %s; open error: %d\n",file_name,errcode);
  printf(" See I_F_ENG.H for a listing of error codes returned.\n");
  return;
  }
errcode = indexed_file(&file_handle,I_F_REPORT_ATTRIBUTES,"",0);
if (errcode)
  {
  printf("?Error getting file attributes: %d\n",errcode);
  printf(" See I_F_ENG.H for a listing of error codes returned.\n");
  return;
  }
printf("File attributes: %s\n",I_F_FILE_ATTRIBUTES);
record_length = i_f_next_file_attribute(1);  /* Get 1st attribute.  */
if (record_length)
  {
  printf("File has a fixed record length of %u.\n",record_length);
  the_record = (char *)malloc(record_length);
  }
else
  {
  printf("File has variable length records.\n");
  the_record = (char *)malloc(65536);
  }
no_of_keys = i_f_next_file_attribute(0);     /* Get next attribute. */
printf("File has %u keys.\n",no_of_keys);
printf("Counting records found by reading each key:\n");
for (key_sub=0;key_sub<no_of_keys;key_sub++)  /* Once for each key in file. */
  {
  key_position = i_f_next_file_attribute(0);   /* Get key's attributes. */
  key_length = i_f_next_file_attribute(0);
  duplicates_flag = i_f_next_file_attribute(0);
  hold_key = (char *)malloc(key_length);
  if (hold_key == NULL)
    {
    printf("?Not enough memory to store this key.\n");
    continue;
    }
  printf("  Key %d:   Position: %d    Length: %d  ",key_sub,key_position,key_length);
  if (duplicates_flag)
    printf(" -- duplicates allowed\n");
  else
    printf(" -- no duplicates allowed\n");
  /* Read forwards through this key. */
  memset(the_record+key_position,0,key_length);  /* Set key to nulls. */
  record_count = 0;
  printf("Reading through key forwards; current record:\n");
  errcode = indexed_file(&file_handle,I_F_START,the_record,key_sub);
  if (errcode)
    {
    printf("?Error starting file forwards: %d\n",errcode);
    exit(10);
    }
  while (!indexed_file(&file_handle,I_F_READNEXT,the_record,0))
    {
    sprintf(temp_string,"%d",record_count+1);
/*    itoa(record_count+1,temp_string,10);  */
    printf("%s      %c",temp_string,13);
    if (++record_count > 1)    /* Don't compare the 1st record of course. */
        {
      if (duplicates_flag)
        {
        if (memcmp(hold_key,the_record+key_position,key_length) > 0)
          printf("?Keys out of sequence!  File is corrupted!\n");
        }
      else
        {
        if (memcmp(hold_key,the_record+key_position,key_length) >= 0)
          printf("?Keys out of sequence or a duplicate key was found!  File is corrupted!\n");
        }
			}
		memcpy(hold_key,the_record+key_position,key_length);
		}
	printf("\n %d records\n",record_count);
	/* Now read backwards through this key. */
	memset(the_record+key_position,255,key_length);  /* Set key to highs. */
	record_count = 0;
	printf("Reading through key backwards; current record:\n");
	I_F_DIRECTION = 0;  /* Set engine to go backwards. */
	errcode = indexed_file(&file_handle,I_F_START,the_record,key_sub);
  if (errcode)
    {
    printf("?Error starting file backwards.\n");
    exit(10);
    }
  while (!indexed_file(&file_handle,I_F_READNEXT,the_record,0))
    {
    sprintf(temp_string,"%d",record_count+1);
/*    itoa(record_count+1,temp_string,10);  */
    printf("%s      %c",temp_string,13);
    if (++record_count > 1)    /* Don't compare the 1st record of course. */
			{
      if (duplicates_flag)
        {
				if (memcmp(hold_key,the_record+key_position,key_length) < 0)
          printf("?Keys out of sequence!  File is corrupted!\n");
        }
      else
        {
				if (memcmp(hold_key,the_record+key_position,key_length) <= 0)
          printf("?Keys out of sequence or a duplicate key was found!  File is corrupted!\n");
        }
			}
		memcpy(hold_key,the_record+key_position,key_length);
		}
	printf("\n %d records\n",record_count);
	free(hold_key);
	I_F_DIRECTION = 1;  /* Reset this to normal. */
	}
printf("Record count should be the same for all keys, both ways.\n");
free(the_record);
indexed_file(&file_handle,I_F_CLOSE,"",0);
end_time = time(NULL);  /* How long did it take? */
printf("Analysis complete -- it took %d seconds.\n",end_time - start_time);
}


void thrashing_write_test()
{
int count;
unsigned short key_position,key_length;
char the_keys_string[64],  /* This has to be long enough to hold 2 rand() strings. */
     bytes_of_random_key_to_copy,
     file_attributes_input[128];

if (!strcmp(I_F_FILE_ATTRIBUTES,""))  /* Set file attributes default. */
    strcpy(I_F_FILE_ATTRIBUTES,"256 2 0 10 0 10 10 1");
printf("\nYou may run one or more read thrashing tests simultaneously with\n");
printf("this write thrashing test if you are using a multi-user or\n");
printf("multi-tasking system.\n\n");
printf("For the file attributes, enter numbers seperated by spaces.\n");
printf("The first number is the record length (1 to 65536).\n");
printf("The next number is the number of keys (1 to 255).\n");
printf("For each key, enter 3 numbers:  Starting position (0 to 65535)\n");
printf("                                Key length (1 to 65535)\n");
printf("                                Duplicates flag (0 or 1)\n\n");
printf("Hit return to accept the default value for file attributes.\n");
printf("Default file attributes are: %s\n",I_F_FILE_ATTRIBUTES);
printf("\nEnter the file attributes string to use:   ");
gets(file_attributes_input);
if (strcmp(file_attributes_input,""))   /* New attributes entered? */
    strcpy(I_F_FILE_ATTRIBUTES,file_attributes_input);
printf("Creating file %s with attributes: %s\n",file_name,I_F_FILE_ATTRIBUTES);
errcode = indexed_file(&file_handle,I_F_OPEN_WRITE,file_name,0);
if (errcode)  {
    printf("OPEN_WRITE error: %d\n",errcode);
    printf("--> Check documentation on file attributes carefully!\n");
    return;  }
indexed_file(&file_handle,I_F_CLOSE,"",0);
errcode = indexed_file(&file_handle,I_F_OPEN_IO,file_name,0);
if (errcode)  {
    printf("OPEN_IO error: %d\n",errcode);
    printf("?Error opening file for i/o after creation!\n");
    return;  }
printf("\nEnter the number of records to write to the file.\n");
record_count = 0;
while ((record_count < 1) || (record_count > 10000))
  {
  printf("Enter a record count from 1 to 10000: ");
  record_count = get_numeric_input();
  }

srand(1);   /* Initialize random number generator. */
record_length = i_f_next_file_attribute(1);
no_of_keys = i_f_next_file_attribute(0);
if (!record_length)
  {  /* We still need a fixed record length for this test.*/
  printf("\nPlease rerun and specify a fixed record length as the first\n");
  printf("number in the file attributes string (variable not yet supported here).\n");
  indexed_file(&file_handle,I_F_CLOSE,"",0);
  return;
  }

printf("I_F_FILE_ATTRIBUTES: %s\n",I_F_FILE_ATTRIBUTES);

printf("Each period represents one record successfully written.\n");
start_time = time(NULL);
the_record = (char *)malloc(record_length);
if (the_record == NULL)
  {
  printf("?Not enough memory to allocate to that record length.\n");
  indexed_file(&file_handle,I_F_CLOSE,"",0);
  return;
  }
for (count=0;count<record_count;count++)  /* Once for each record we write. */
  {
  i_f_next_file_attribute(1);  /* Init. to start of file attributes */
  i_f_next_file_attribute(0);  /*  string and skip 1st 2 numbers.   */
  memset(the_record,' ',record_length);   /* Set whole record to spaces. */
  for (key_sub=0;key_sub<no_of_keys;key_sub++)   /* Write a random # into each key. */
    {
    key_position = i_f_next_file_attribute(0);
    key_length   = i_f_next_file_attribute(0);  /* Get key length. */
    i_f_next_file_attribute(0);        /* Skip duplicates flag. */
    sprintf(the_keys_string,"%d",rand());
    sprintf(temp_string,"%d",rand());
    strcat(the_keys_string,temp_string);  /* Make a long random key. */
    if (key_length < strlen(the_keys_string))  /* Pick shorter key length.*/
      bytes_of_random_key_to_copy = key_length;
    else
      bytes_of_random_key_to_copy = strlen(the_keys_string);
    memcpy(the_record+key_position,the_keys_string,bytes_of_random_key_to_copy);
    }      /* Now keys are set to some random stuff. */
  sprintf(temp_string,"%d",count+1);
  printf("%s     %c",temp_string,13);      /* One period for each record written. */
  errcode = indexed_file(&file_handle,I_F_WRITE,the_record,0);
  if (errcode == I_F_ERROR_KEYEXISTS)
    {      /* Retry duplicates. */
    printf("\nDuplicate key; trying another random key.");
    count--;
    continue;
    }
  if (errcode)
    {           /* An unexpected error occurred? */
    printf("WRITE error: %d\n",errcode);
    printf("A write error occurred; check the documentation for error code explanation.\n");
    break;
    }  /* Break out of write loop, let close and return happen. */
  }
free(the_record);
end_time = time(NULL);  /* How long did it take? */
printf("\n\n%d records of length %u with %u random keys written to %s.\n",count,record_length,no_of_keys,file_name);
printf("Total time: %d seconds.\n",end_time - start_time);
indexed_file(&file_handle,I_F_CLOSE,"",0);
}


void thrashing_read_test()
{
int count;
unsigned short key_position,key_length;
char the_keys_string[8],bytes_of_random_key_to_copy,file_attributes_input[128];

errcode = indexed_file(&file_handle,I_F_OPEN_READ,file_name,0);
if (errcode)  {
    printf("?Couldn't open file %s; open error: %d\n",file_name,errcode);
    printf(" See I_F_ENG.H for a listing of error codes returned.\n");
    return;  }
printf("\nEnter the number of records to randomly read from the file.\n");
record_count = 0;
while ((record_count < 1) || (record_count > 10000))  {
    printf("Enter a record count from 1 to 10000: ");
    record_count = get_numeric_input();  }

srand(2);   /* Initialize random number generator. */
printf("Each period represents one record successfully read.\n");
start_time = time(NULL);
errcode = indexed_file(&file_handle,I_F_REPORT_ATTRIBUTES,"",0);
if (errcode)  {
    printf("?Error getting file attributes: %d\n",errcode);
    printf(" See I_F_ENG.H for a listing of error codes returned.\n");
    return;  }
printf("File attributes: %s\n",I_F_FILE_ATTRIBUTES);
record_length = i_f_next_file_attribute(1);  /* Skip the record length. */
if (record_length)
    the_record = (char *)malloc(record_length);
else
    the_record = (char *)malloc(65536);
i_f_next_file_attribute(0);  /* Skip key count. */
key_position = i_f_next_file_attribute(0);  /* ...of 1st key. */
key_length = i_f_next_file_attribute(0);    /* ...of 1st key. */
for (count=0;count<record_count;count++)
    {
    sprintf(the_keys_string,"%d",rand());
/*    itoa(rand(),the_keys_string,10); */
    if (key_length < strlen(the_keys_string))  /* Pick shorter key length.*/
        bytes_of_random_key_to_copy = key_length;
    else
        bytes_of_random_key_to_copy = strlen(the_keys_string);
    memcpy(the_record+key_position,the_keys_string,bytes_of_random_key_to_copy);
    indexed_file(&file_handle,I_F_START,the_record,0);     /* Do a start and a      */
    indexed_file(&file_handle,I_F_READNEXT,the_record,0);  /*  readnext (errors ok).*/
    sprintf(temp_string,"%d",count+1);
/*    itoa(count+1,temp_string,10); */
    printf("%s     %c",temp_string,13);
    }
free(the_record);
end_time = time(NULL);  /* How long did it take? */
printf("\n\n%d records read from file %s\n",count,file_name);
printf("Total time: %d seconds.\n",end_time - start_time);
indexed_file(&file_handle,I_F_CLOSE,"",0);
}


/*--------------------------- The Code --------------------------------------*/
int main()
{
char command_string[64], menu_command = ' ';

indexed_file(&file_handle,I_F_CLOSE_ALL,"",0);
while (1)
    {
    printf("\n\n--------------------------------------\nI_F_UTL:  Indexed File Utility Program \n\n");
    printf("1.  Analyze file for integrity\n");
    printf("2.  Thrashing write test      \n");
    printf("3.  Thrashing read test       \n");
    printf("9.  Filename to work with (%s)\n",file_name);
    printf("E.  Exit                      \n");
    printf("\nYour choice? ");
    gets(command_string);
    menu_command = command_string[0];
    printf("\n");
    switch (menu_command)
	{
	case '1': analyze_file_integrity();  break;
	case '2': thrashing_write_test();    break;
	case '3': thrashing_read_test();     break;
	case '9': printf("Enter filename to work with: ");
		  gets(file_name);           break;
	}
    if ((menu_command == 'E') || (menu_command == 'e'))
	break;
    }
return(0);
}


