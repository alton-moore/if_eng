

/* This software Copyright 1991 by Alton O. Moore III, Edinburg, TX..       */
/* All rights reserved.  This software or any portion thereof may not       */
/* be copied by anyone in any form whatsoever without the written consent   */
/* of the copyright owner.                                                  */

/* ARCHITECTURE:                                                            */
/*  The first group of data in the file is the file header information,     */
/*   defined below as file_header_type.  The no_of_keys field in that data  */
/*   indicates the number of keys in the file; one group of data of type    */
/*   key_definition_type follows the file header info. for each key.        */


/*-------------------------- DEFINITIONS -----------------------------------*/

#ifndef SEEK_SET
#define SEEK_SET 0
#endif
#ifndef SEEK_CUR
#define SEEK_CUR 1
#endif
#ifndef SEEK_END
#define SEEK_END 2
#endif

#include <stdio.h>
#include <string.h>

#define I_F_CURRENT_REVISION        1  /* This is the revision implemented   */
					/*  in this source code.              */
#define I_F_LOWEST_USABLE_REVISION  1  /* This is the lowest file rev. level */
                                       /*  this software can access.  Lower  */
                                       /*  level files need to be converted. */

#define I_F_CLOSE       0   /* These are file operations. */
#define I_F_OPEN_READ   1
#define I_F_OPEN_WRITE  2   /*   This requires I_F_FILE_ATTRIBUTES. */
#define I_F_OPEN_IO     3
#define I_F_CLOSE_ALL   4
#define I_F_READ     	16    /* These are record operations. */
#define I_F_START     17
#define I_F_READNEXT 	18
#define I_F_READPREV  19
#define I_F_WRITE    	20
#define I_F_REWRITE  	21
#define I_F_DELETE      22
#define I_F_REPORT_ATTRIBUTES     50  /* This sets I_F_FILE_ATTRIBUTES.   */
#define I_F_SUCCESS                 0  /* These are the error codes we return. */
#define I_F_ERROR_KEYNOTFOUND       1
#define I_F_ERROR_KEYEXISTS         2
#define I_F_ERROR_BADOPERATION     16
#define I_F_ERROR_BADHANDLE        17
#define I_F_ERROR_BADKEY           18
#define I_F_ERROR_BADFILENAME      19
#define I_F_ERROR_BADATTRIBUTES    20
#define I_F_ERROR_BADREVLEVEL      21
#define I_F_ERROR_OUTOFMEMORY      50
#define I_F_ERROR_INTERNAL         98
#define I_F_ERROR_FILECORRUPTED    99

unsigned short I_F_RECORD_LENGTH = 0;
  /* If the record length specified in I_F_FILE_ATTRIBUTES is 0, then this  */
  /*  variable should be set before any write/rewrite operation and checked */
  /*  after any read operation.                                             */

char I_F_DIRECTION = 1; /* 0 means backwards (previous); non-zero means forwards.*/

char I_F_FILE_ATTRIBUTES[128] = ""; /* The user gives key info., etc. here.*/
								/*  Format: numbers seperated by spaces.   */
								/*  First no. is fixed record length or 0. */
								/*  The next number is the # of keys.      */
								/*  Then, for each key, read 3 numbers     */
								/*  (position, length, and duplicates).    */

int I_F_RESULTCODE;  /* This always holds the last result code returned by */
                     /*  the routine.  It is probably most useful for      */
                     /*  inclusion in a "fatal error" routine of sorts.    */


/*------------------- FILE & MEMORY STRUCTURES ----------------------------*/
/* These are the structures found in the file and in dynamic memory.       */
/*  Pointers into the file end with -fptr instead of -ptr to distinguish   */
/*  them from memory ptrs.                                                 */

/* Start each operation by finding the correct file in this list.  Then read */
/*  relevant information from the file (it should be cached by the O/S).     */
/*  This list is kept in file handle order so we can identify unused handles.*/
struct open_file_list_type  {
	int file_handle;          /* The file handle we assigned (1..x).           */
	FILE *file_ptr;           /* This is the file pointer returned by fopen(). */
	char open_mode;           /* How the file was opened (e.g. I_F_OPEN_READ)  */
							/*  (I_F_CLOSE if closed).                       */
  unsigned char current_key;/* The key we're currently traversing, if any.   */
	long current_record_fptr;  /* Points to current node or 0.  */
  long lastread_record_fptr; /* Points to last node read.     */
	struct open_file_list_type *next;    /* Pointer to next entry in this list.*/
	}  *open_file_list_head_ptr = NULL,  /* NULL inits. open files list. */
	 *open_file_list_current_ptr, *open_file_list_prev_ptr,
	 *open_file_list_saved_ptr,   *open_file_list_traversing_ptr;

/* This is the next structure.  It is at the beginning (byte 0)   */
/*  of the file.  Read it into its variable (for every operation; */
/*  this is why we need caching!).  Cache this ourselves later?   */
struct file_header_type  {
	unsigned short revision_level; /* The rev. level of this file.            */
	unsigned short record_length; /* If 0, file has variable record length.   */
								  /*  If not, then fixed; routine sets        */
								  /*  I_F_RECORD_LENGTH before write/rewrites.*/
	unsigned char no_of_keys;    /* Number of keys in file (defs. follow).   */
	long empty_space_fptr;       /* Pointer to block of unused bytes in file.*/
								 /*  Each block contains structure           */
								 /*  empty_space_info; see below.  This field*/
								 /*  is only meaningful for files of fixed   */
								 /*  record length; otherwise it is 0.       */
	} file_header;               /* The variable we read this structure into.*/

/* This structure is found at the beginning of any blocks of unused bytes  */
/*  in the file.  It's just a pointer to the next block of unused bytes.   */
/*  This "unused-bytes-management" feature is found only in files with     */
/*  fixed-length records.                                                  */
struct empty_space_info_type  {
        long next_fptr;    /* Location of next empty space or 0 for last. */
	} empty_space_info;

/* Next level down are the key definitions in the file.  */
struct key_definition_type  {
	unsigned short position; /* Pos. of 1st char of key in record; [0] is 1st.*/
  unsigned short length;            /* Length of this key.                           */
	unsigned char duplicates_flag;  /* Are duplicates allowed?  0=FALSE, 1=TRUE.*/
	long root_fptr;   /* Pointer to root of tree for this index.   */
	} key_definition;          /* The variable we read this structure into. */

/* Now we're at the record level in the file.  Each node of the tree is a    */
/*  record.  In case of duplicates, the duplicate is written on the left.    */
/* First are [no_of_keys] instances of this structure. */
struct keys_pointers_type  {
	long parent_fptr,   /* Points to parent or NULL.      */
	left_fptr,     /* Pointer to left_child or NULL.      */
	right_fptr;    /* Points to right child or NULL.      */
	} keys_pointers;  /* For reading this structure.      */

struct record_descriptor_type  {
	unsigned short record_length;  /* The record length in bytes. */
	}  record_descriptor;
/* This is followed in the file by the record data. */

char i_f_position_in_attributes_string;  /* Used for parsing I_F_FILE_ATTRIBUTES. */
char i_f_copyright_notice_displayed_flag = 0;
char *temp_key;  /* This is for temporary storage of keys. */


/*---------------------------- SUBROUTINES ----------------------------------*/

/*---------- This group of routines reads & writes all the           */
/*---------- structures of any size in the file; also has            */
/*---------- all the routines which change the O/S's file pointer.   */

void fn_write_file_header(void)
{
fseek(open_file_list_current_ptr->file_ptr,0,SEEK_SET);
fwrite(&file_header,sizeof(struct file_header_type),1,open_file_list_current_ptr->file_ptr);
}

void fn_read_file_header(void)   /* Requires that open_file_list_current_ptr      */
                        /*  be set.  Return 0 for success.               */
{                       /* Leaves file pointer at start of 1st key def.. */
fseek(open_file_list_current_ptr->file_ptr,0,SEEK_SET);
fread(&file_header,sizeof(struct file_header_type),1,open_file_list_current_ptr->file_ptr);
}


void fn_write_key_definition(unsigned char key_sub)
{
fseek(open_file_list_current_ptr->file_ptr,sizeof(struct file_header_type) +
  (key_sub * sizeof(struct key_definition_type)),SEEK_SET);
fwrite(&key_definition,sizeof(struct key_definition_type),1,open_file_list_current_ptr->file_ptr);
}

void fn_read_key_definition(unsigned char key_sub)
{
fseek(open_file_list_current_ptr->file_ptr,sizeof(struct file_header_type) +
  (key_sub * sizeof(struct key_definition_type)),SEEK_SET);
fread(&key_definition,sizeof(struct key_definition_type),1,open_file_list_current_ptr->file_ptr);
}


void fn_write_keys_pointers(long fptr,unsigned char key_sub)
{
if (!fptr)  {
    printf("***** fn_write_keys_pointers got fptr 0 *****\n");
    exit(9);  }
fseek(open_file_list_current_ptr->file_ptr,fptr + (key_sub * sizeof(struct keys_pointers_type)),SEEK_SET);
fwrite(&keys_pointers,sizeof(struct keys_pointers_type),1,open_file_list_current_ptr->file_ptr);
}

void fn_read_keys_pointers(long fptr,unsigned char key_sub)
{
if (!fptr)
    printf("***** fn_read_keys_pointers got fptr 0 *****\n");
fseek(open_file_list_current_ptr->file_ptr,fptr + (key_sub * sizeof(struct keys_pointers_type)),SEEK_SET);
fread(&keys_pointers,sizeof(struct keys_pointers_type),1,open_file_list_current_ptr->file_ptr);
}


void fn_write_record_descriptor(long fptr)
{
fseek(open_file_list_current_ptr->file_ptr,fptr + (file_header.no_of_keys * sizeof(struct keys_pointers_type)),SEEK_SET);
fwrite(&record_descriptor,sizeof(struct record_descriptor_type),1,open_file_list_current_ptr->file_ptr);
}

void fn_read_record_descriptor(long fptr)
{
fseek(open_file_list_current_ptr->file_ptr,fptr + (file_header.no_of_keys * sizeof(struct keys_pointers_type)),SEEK_SET);
fread(&record_descriptor,sizeof(struct record_descriptor_type),1,open_file_list_current_ptr->file_ptr);
}


void fn_write_record_info(long fptr,char the_record[])
{
unsigned short record_length_to_write;
if (file_header.record_length == 0)  {       /* Variable record length? */
    record_descriptor.record_length = I_F_RECORD_LENGTH;
    fn_write_record_descriptor(fptr);   /* This does the seek for us. */
    record_length_to_write = I_F_RECORD_LENGTH;  }
  else  {
    fseek(open_file_list_current_ptr->file_ptr,fptr +
      (file_header.no_of_keys * sizeof(struct keys_pointers_type)),SEEK_SET);
    record_length_to_write = file_header.record_length;  }
fwrite(the_record,record_length_to_write,1,open_file_list_current_ptr->file_ptr);
}

void fn_read_record_info(long fptr,char the_record[])
{
unsigned short record_length_to_read;
if (file_header.record_length == 0)  {       /* Variable record length? */
    fn_read_record_descriptor(fptr);    /* This does the seek also. */
    I_F_RECORD_LENGTH = record_descriptor.record_length;
    record_length_to_read = record_descriptor.record_length;  }
  else  {
    fseek(open_file_list_current_ptr->file_ptr,fptr +
      (file_header.no_of_keys * sizeof(struct keys_pointers_type)),SEEK_SET);
    record_length_to_read = file_header.record_length;  }
fread(the_record,record_length_to_read,1,open_file_list_current_ptr->file_ptr);
}


void fn_read_empty_space_info(long fptr)
{
fseek(open_file_list_current_ptr->file_ptr,fptr,SEEK_SET);
fread(&empty_space_info,sizeof(struct empty_space_info_type),1,open_file_list_current_ptr->file_ptr);
}

void fn_write_empty_space_info(long fptr)
{
fseek(open_file_list_current_ptr->file_ptr,fptr,SEEK_SET);
fwrite(&empty_space_info,sizeof(struct empty_space_info_type),1,open_file_list_current_ptr->file_ptr);
}


fn_compare_keys(long fptr,unsigned char key_sub,char the_record[])
{
int result;   /* ...of memcmp()  */
fn_read_key_definition(key_sub);
if (file_header.record_length)   /* Which seek do we do?  Fixed rec. length? */
	fseek(open_file_list_current_ptr->file_ptr,fptr +
	  (file_header.no_of_keys * sizeof(struct keys_pointers_type)) +
	  key_definition.position,SEEK_SET);
else
	fseek(open_file_list_current_ptr->file_ptr,fptr +  /* Read key into temp key.*/
	  (file_header.no_of_keys * sizeof(struct keys_pointers_type)) +
	  sizeof(struct record_descriptor_type) + key_definition.position,SEEK_SET);
fread(temp_key,key_definition.length,1,open_file_list_current_ptr->file_ptr);
result = memcmp(the_record+key_definition.position,temp_key,key_definition.length);
return(result);
}


/* ------------------------ Other subroutines -----------------------------*/

void fn_reverse_list_entry(void)    /* ...after problem opening file. */
{
if (open_file_list_current_ptr == open_file_list_head_ptr)
	open_file_list_head_ptr = open_file_list_current_ptr->next;
else
	{
	open_file_list_prev_ptr = open_file_list_head_ptr;
	while (open_file_list_prev_ptr->next != open_file_list_current_ptr)
		open_file_list_prev_ptr = open_file_list_prev_ptr->next;
	open_file_list_prev_ptr->next = open_file_list_current_ptr->next;
	}
free(open_file_list_current_ptr);
}


void fn_file_handle_new(void)   /* Sets open_file_list_current_ptr to new entry. */
{
open_file_list_current_ptr =
  (struct open_file_list_type *)malloc(sizeof(struct open_file_list_type));
if (open_file_list_current_ptr == NULL)   /* Out of memory? */
	return;
if (open_file_list_head_ptr == NULL)                     /* Is list empty? */
	{
	open_file_list_current_ptr->file_handle = 1;       /* Handles start at 1. */
    open_file_list_head_ptr = open_file_list_current_ptr;
    open_file_list_current_ptr->next = NULL;
    }
else
if (open_file_list_head_ptr->file_handle != 1)
    {
    open_file_list_current_ptr->file_handle = 1;   /* Establish file handle 1. */
    open_file_list_current_ptr->next = open_file_list_head_ptr;
    open_file_list_head_ptr = open_file_list_current_ptr;
    }
  else
    {
    open_file_list_prev_ptr = open_file_list_head_ptr;
    open_file_list_traversing_ptr = open_file_list_head_ptr->next;
    while (open_file_list_traversing_ptr != NULL)  {
        if (open_file_list_traversing_ptr->file_handle > (open_file_list_prev_ptr->file_handle + 1))
            break;
        open_file_list_prev_ptr = open_file_list_traversing_ptr;
        open_file_list_traversing_ptr = open_file_list_traversing_ptr->next;  }
    open_file_list_prev_ptr->next = open_file_list_current_ptr;
    open_file_list_current_ptr->file_handle = open_file_list_prev_ptr->file_handle + 1;
    open_file_list_current_ptr->next = open_file_list_traversing_ptr;
    }
/* If unsuccessful, NULL gets returned in open_file_list_current_ptr. */
}


void fn_file_handle_find(int file_handle)   /* Sets open_file_list_current_ptr to entry for this handle. */
{
open_file_list_current_ptr = open_file_list_head_ptr;
while (open_file_list_current_ptr != NULL)
  {
  if (open_file_list_current_ptr->file_handle == file_handle)
      break;
  open_file_list_current_ptr = open_file_list_current_ptr->next;
  }
/* If handle not found, NULL gets returned in open_file_list_current_ptr. */
}


void fn_bad_attributes(char file_id[])  /* Close & erase file, then get rid */
                /* of entry.  Expects open_file_list_current_ptr to be set. */
{
fclose(open_file_list_current_ptr->file_ptr);
unlink(file_id);
fn_reverse_list_entry();
}


unsigned short i_f_next_file_attribute(int reset_flag) /* Return next number in string.*/
{
char parsed_no[8]="";
unsigned short temp_value;
if (reset_flag)
    i_f_position_in_attributes_string = 0; /* Get ready to get attributes. */
while (1)
    {
    switch(I_F_FILE_ATTRIBUTES[i_f_position_in_attributes_string])
	{
	case 0   : temp_value = (unsigned short)atoi(parsed_no);
	           return(temp_value);
	case ' ' : i_f_position_in_attributes_string++;  /* Skip the spaces. */
		   temp_value = (unsigned short)atoi(parsed_no);
		   return(temp_value);
	default  : strncat(parsed_no,&I_F_FILE_ATTRIBUTES[i_f_position_in_attributes_string++],1);
	}
    }
}


void fn_traverse_to_next(char the_direction)  /* Moves current_record_fptr to prev., next or 0. */
{
long child_just_traversed_from_fptr;   /* Non-zero if going up.     */
			/* This holds the fptr of the child just traversed */
			/*  up from, so we can see if it was a left child. */
child_just_traversed_from_fptr = 0;   /* Only nonzero when traversing up. */
fn_read_keys_pointers(open_file_list_current_ptr->current_record_fptr,
		      open_file_list_current_ptr->current_key);
if (!the_direction)   /* Move to previous? */
    {
    if (keys_pointers.left_fptr)
	open_file_list_current_ptr->current_record_fptr = keys_pointers.left_fptr;
    else  {
	child_just_traversed_from_fptr = open_file_list_current_ptr->current_record_fptr;
	open_file_list_current_ptr->current_record_fptr = keys_pointers.parent_fptr;  }
    }
else
    {
    if (keys_pointers.right_fptr)
	open_file_list_current_ptr->current_record_fptr = keys_pointers.right_fptr;
    else  {
	child_just_traversed_from_fptr = open_file_list_current_ptr->current_record_fptr;
	open_file_list_current_ptr->current_record_fptr = keys_pointers.parent_fptr;  }
    }
while (open_file_list_current_ptr->current_record_fptr)  /* Do this till 0 */
    {                                                    /*  or break.     */
    fn_read_keys_pointers(open_file_list_current_ptr->current_record_fptr,
			  open_file_list_current_ptr->current_key);

    /*  1. Did we just traverse up from a lower node?  If from left, current is
	     target.  If from right, traverse up another.
	2. Try left child.  If non-zero, traverse; else return current.   */

    if (child_just_traversed_from_fptr)  /* We're traversing up the nodes. */
	{
	if (!the_direction)
	    {
	    if (keys_pointers.right_fptr == child_just_traversed_from_fptr)
		break;     /* If coming up from right child, this is the next. */
	    }
	else
	    {
	    if (keys_pointers.left_fptr == child_just_traversed_from_fptr)
		break;     /* If coming up from left child, this is the next. */
	    }
	child_just_traversed_from_fptr = open_file_list_current_ptr->current_record_fptr;
	open_file_list_current_ptr->current_record_fptr =
	  keys_pointers.parent_fptr;    /* Go up to parent. */
	}
    else
	{                                /* We're traversing down the nodes. */
	if (!the_direction)
	    {
	    if (keys_pointers.right_fptr)
		{
		/* Clear because we're not traversing up. */
		child_just_traversed_from_fptr = 0;
		open_file_list_current_ptr->current_record_fptr = keys_pointers.right_fptr;
		}
	    else
		break;   /* Return current key as the record pointer. */
	    }
	else
	    {
	    if (keys_pointers.left_fptr)
		{
		/* Clear because we're not traversing up. */
		child_just_traversed_from_fptr = 0;
		open_file_list_current_ptr->current_record_fptr = keys_pointers.left_fptr;
		}
	    else
		break;   /* Return current key as the record pointer. */
	    }
	}
    /* Traverse to next key. */
    }
}

void fn_delete_node_from_keys_tree(long fptr_to_delete,unsigned char key_sub)
      /* Just changes the different records' pointers to go around this one.*/
{
long replacement_nodes_fptr,  /* For working with file pointers. */
  parents_fptr,hold_parent_fptr,hold_left_fptr,hold_right_fptr;
fn_read_keys_pointers(fptr_to_delete,key_sub);
parents_fptr = keys_pointers.parent_fptr;  /* Save parent's fptr. */

if ((!keys_pointers.left_fptr) || (!keys_pointers.right_fptr))
    {              /* At least one, maybe both children are 0. */
    if (!keys_pointers.left_fptr)
        replacement_nodes_fptr = keys_pointers.right_fptr;
    else
        replacement_nodes_fptr = keys_pointers.left_fptr;
    /* Start updating parent node (or root) to point to replacement. */
    if (!parents_fptr)  {  /* Are we deleting the root? */
        fn_read_key_definition(key_sub);
        key_definition.root_fptr = replacement_nodes_fptr;
        fn_write_key_definition(key_sub);   }
    else  {
        fn_read_keys_pointers(parents_fptr,key_sub);
        /* Which side were we on? */
        if (keys_pointers.left_fptr == fptr_to_delete)
            keys_pointers.left_fptr = replacement_nodes_fptr;
        else
            keys_pointers.right_fptr = replacement_nodes_fptr;
        fn_write_keys_pointers(parents_fptr,key_sub);  }
    /* Update replacement's parent field to point to parent. */
    if (replacement_nodes_fptr)  {
        fn_read_keys_pointers(replacement_nodes_fptr,key_sub);
        keys_pointers.parent_fptr = parents_fptr;
        fn_write_keys_pointers(replacement_nodes_fptr,key_sub);  }
    }
else
    {
    /* Both the children have data; replace this node with replacement. */
    replacement_nodes_fptr = keys_pointers.left_fptr;
    while (1)
        {
        fn_read_keys_pointers(replacement_nodes_fptr,key_sub);
        if (!keys_pointers.right_fptr)
            break;
        replacement_nodes_fptr = keys_pointers.right_fptr;
        }
    /* Now replacement node's fptr is figured out and its pointers read. */
    if (keys_pointers.parent_fptr == fptr_to_delete)
        {              /* Replacement node is left child of node to delete. */
        if (!parents_fptr)     /* Deleting the root node? */
            {
            fn_read_key_definition(key_sub);
            key_definition.root_fptr = replacement_nodes_fptr;
            fn_write_key_definition(key_sub);
	    fn_read_keys_pointers(fptr_to_delete,key_sub);
            hold_right_fptr = keys_pointers.right_fptr;  /* Hold deleted node's right child fptr. */
            fn_read_keys_pointers(replacement_nodes_fptr,key_sub);
            keys_pointers.right_fptr = hold_right_fptr;
            keys_pointers.parent_fptr = 0;
            fn_write_keys_pointers(replacement_nodes_fptr,key_sub);
            }
        else
            {
            fn_read_keys_pointers(fptr_to_delete,key_sub);
            hold_parent_fptr = keys_pointers.parent_fptr;
            hold_right_fptr = keys_pointers.right_fptr;
            fn_read_keys_pointers(replacement_nodes_fptr,key_sub);
            keys_pointers.parent_fptr = hold_parent_fptr;
            keys_pointers.right_fptr = hold_right_fptr;
            fn_write_keys_pointers(replacement_nodes_fptr,key_sub);
            fn_read_keys_pointers(hold_parent_fptr,key_sub);
            if (keys_pointers.left_fptr == fptr_to_delete)
                keys_pointers.left_fptr = replacement_nodes_fptr;
            else
                keys_pointers.right_fptr = replacement_nodes_fptr;
            fn_write_keys_pointers(hold_parent_fptr,key_sub);
            }
        fn_read_keys_pointers(hold_right_fptr,key_sub); /* Make right node's parent */
        keys_pointers.parent_fptr = replacement_nodes_fptr;   /* point to    */
        fn_write_keys_pointers(hold_right_fptr,key_sub);      /* replacement.*/
        }
    else
        {
        /* The replacement node is a leaf with no children. */
        fn_read_keys_pointers(replacement_nodes_fptr,key_sub);      /* Get the */
        hold_parent_fptr = keys_pointers.parent_fptr; /* replacement's parent.*/
        fn_read_keys_pointers(hold_parent_fptr,key_sub);  /* Make it stop   */
        keys_pointers.right_fptr = 0;           /* pointing to replacement. */
        fn_write_keys_pointers(hold_parent_fptr,key_sub);
        if (!parents_fptr)   /* Deleting the root node? */
            {
            fn_read_key_definition(key_sub);     /* Update root. */
	    key_definition.root_fptr = replacement_nodes_fptr;
            fn_write_key_definition(key_sub);
            fn_read_keys_pointers(fptr_to_delete,key_sub);
            hold_left_fptr  = keys_pointers.left_fptr;
            hold_right_fptr = keys_pointers.right_fptr;
            fn_read_keys_pointers(replacement_nodes_fptr,key_sub);
            keys_pointers.parent_fptr = 0;
            keys_pointers.left_fptr = hold_left_fptr;
            keys_pointers.right_fptr = hold_right_fptr;
            fn_write_keys_pointers(replacement_nodes_fptr,key_sub);
            }
        else
            {
            fn_read_keys_pointers(fptr_to_delete,key_sub);
            hold_parent_fptr = keys_pointers.parent_fptr;
            hold_left_fptr  = keys_pointers.left_fptr;
            hold_right_fptr = keys_pointers.right_fptr;
            fn_read_keys_pointers(replacement_nodes_fptr,key_sub);  /* Update */
            keys_pointers.parent_fptr = hold_parent_fptr;      /* replacement.*/
            keys_pointers.left_fptr = hold_left_fptr;
            keys_pointers.right_fptr = hold_right_fptr;
            fn_write_keys_pointers(replacement_nodes_fptr,key_sub);
            fn_read_keys_pointers(hold_parent_fptr,key_sub); /* Update parent.*/
            if (keys_pointers.left_fptr == fptr_to_delete)
                keys_pointers.left_fptr = replacement_nodes_fptr;
            else
                keys_pointers.right_fptr = replacement_nodes_fptr;
            fn_write_keys_pointers(hold_parent_fptr,key_sub);
            }
        fn_read_keys_pointers(hold_left_fptr,key_sub);   /* Update left. */
        keys_pointers.parent_fptr = replacement_nodes_fptr;
        fn_write_keys_pointers(hold_left_fptr,key_sub);
        fn_read_keys_pointers(hold_right_fptr,key_sub);  /* Update right. */
        keys_pointers.parent_fptr = replacement_nodes_fptr;
        fn_write_keys_pointers(hold_right_fptr,key_sub);
        }
    }
/* Make sure no info. is left in the pointers for this key. */
fn_read_keys_pointers(fptr_to_delete,key_sub);
keys_pointers.parent_fptr = 0;
keys_pointers.left_fptr = 0;
keys_pointers.right_fptr = 0;
fn_write_keys_pointers(fptr_to_delete,key_sub);
/* zzz Should I not do this?  So that people open for read don't get goofed up? */
}


void fn_add_node_to_keys_tree(long fptr_to_add,unsigned char key_sub,char the_record[])
{
long traversing_fptr,hold_fptr;  /* Used for working with file pointers. */
int result;  /* For holding memcmp() results. */

fn_read_key_definition(key_sub);     /* Find root of this key's tree. */
if (key_definition.root_fptr == 0)     {
    key_definition.root_fptr = fptr_to_add;
    fn_write_key_definition(key_sub);  }
else
    {
    traversing_fptr = key_definition.root_fptr;
    while (1)      /* Break outta here when we're finished updating. */
	{
	result = fn_compare_keys(traversing_fptr,key_sub,the_record);
	if (!result)
	    {        /* Duplicate key; tack onto left side. */
	    fn_read_keys_pointers(traversing_fptr,key_sub);
	    hold_fptr = keys_pointers.left_fptr;
	    fn_read_keys_pointers(fptr_to_add,key_sub);
	    keys_pointers.parent_fptr = traversing_fptr;
	    keys_pointers.left_fptr = hold_fptr;
	    fn_write_keys_pointers(fptr_to_add,key_sub);
	    fn_read_keys_pointers(traversing_fptr,key_sub);
	    keys_pointers.left_fptr = fptr_to_add;
	    fn_write_keys_pointers(traversing_fptr,key_sub);
	    if (hold_fptr)  {  /* Does this point to a node or NULL? */
		fn_read_keys_pointers(hold_fptr,key_sub);
		keys_pointers.parent_fptr = fptr_to_add;
		fn_write_keys_pointers(hold_fptr,key_sub);  }
	    break;    /* Done with this key since it was a duplicate. */
	    }
	fn_read_keys_pointers(traversing_fptr,key_sub);  /* Go to child.*/
	if (result < 0)
	    {
	    if (keys_pointers.left_fptr)
		traversing_fptr = keys_pointers.left_fptr;
	    else
		{    /* Make the new node this one's child. */
		fn_read_keys_pointers(fptr_to_add,key_sub);
		keys_pointers.parent_fptr = traversing_fptr;
		fn_write_keys_pointers(fptr_to_add,key_sub);
		fn_read_keys_pointers(traversing_fptr,key_sub);
		keys_pointers.left_fptr = fptr_to_add;
		fn_write_keys_pointers(traversing_fptr,key_sub);
		break;
		}
	    }
	else
	    {
	    if (keys_pointers.right_fptr != 0)
		traversing_fptr = keys_pointers.right_fptr;
	    else
		{
		fn_read_keys_pointers(fptr_to_add,key_sub);
		keys_pointers.parent_fptr = traversing_fptr;
		fn_write_keys_pointers(fptr_to_add,key_sub);
		fn_read_keys_pointers(traversing_fptr,key_sub);
		keys_pointers.right_fptr = fptr_to_add;
		fn_write_keys_pointers(traversing_fptr,key_sub);
		break;
		}
	    }
	}
    /* Done traversing down the tree. */
    }
}


fn_check_for_duplicates(unsigned char key_sub,char the_record[])
{
long traversing_fptr;
int result;
fn_read_key_definition(key_sub);
traversing_fptr = key_definition.root_fptr;
while (traversing_fptr) /* Go until no more nodes or duplicate found. */
    {
    result = fn_compare_keys(traversing_fptr,key_sub,the_record);
    if (result == 0)
        return(I_F_ERROR_KEYEXISTS);   /* This is a duplicate key! */
    /* We need to set traversing_fptr to point to next record. */
    /*  First read the pointers we need.                       */
    fn_read_keys_pointers(traversing_fptr,key_sub);
    if (result < 0)
        traversing_fptr = keys_pointers.left_fptr;
    else
        traversing_fptr = keys_pointers.right_fptr;
    }
return(I_F_SUCCESS);
}


fn_allocate_temp_key_memory(void)  /* Allocate enough for largest key possible  */
{                              /*  and leave allocated all the time.        */
                               /*  Return 0 if successful, non-zero if not. */
static unsigned short current_temp_key_size = 0;  /* This holds the currently allocated temp_key size. */
unsigned short largest_key_size_found;  /* ...in all open files. */
unsigned char key_sub;
if (open_file_list_head_ptr == NULL)     /* We have closed all files. */
    {
    if (current_temp_key_size)      {
        free(temp_key);   /* No memory at all needed. */
        current_temp_key_size = 0;  }
    return(I_F_SUCCESS);
    }
open_file_list_saved_ptr = open_file_list_current_ptr;
largest_key_size_found = 0; /* Find largest key's size. */
open_file_list_current_ptr = open_file_list_head_ptr;
while (open_file_list_current_ptr != NULL)   /* Check all open files. */
    {
    fn_read_file_header();
    for (key_sub=0;key_sub<file_header.no_of_keys;key_sub++)  /* Check all keys.*/
        {
        fn_read_key_definition(key_sub);
        if (key_definition.length > largest_key_size_found)
            largest_key_size_found = key_definition.length;
        }
    open_file_list_current_ptr = open_file_list_current_ptr->next;
    }
open_file_list_current_ptr = open_file_list_saved_ptr;   /* Restore this. */
/* We have determined the largest temp_key size we currently need.  */
if (largest_key_size_found == current_temp_key_size)
    return(I_F_SUCCESS);
if (current_temp_key_size)  /* Only deallocate if something's allocated. */
    free(temp_key);    /* Deallocate old size. */
temp_key = (char *)malloc(largest_key_size_found);  /* Allocate new size.*/
if (temp_key == NULL)
    {                    /* Failed; too big? */
	temp_key = (char *)malloc(current_temp_key_size);  /* Try original size. */
    if (temp_key == NULL)  {  /* Reallocation error?  Impossible! */
        return(I_F_ERROR_INTERNAL);  } /* Extremely unfortunate! */
    return(I_F_ERROR_OUTOFMEMORY);  /* Not enough for some file's large key. */
    }                               /* Leave temp key at original size. */
current_temp_key_size = largest_key_size_found;
return(I_F_SUCCESS);
}


/*=========================== OPERATIONS ====================================*/

/*-------------------- close --------------------*/

fn_i_f_close(int file_handle)
{
fn_file_handle_find(file_handle);
if (open_file_list_current_ptr == NULL)
    return(I_F_ERROR_BADHANDLE);
if (fclose(open_file_list_current_ptr->file_ptr) != 0)  /* Problem closing? */
    return(I_F_ERROR_INTERNAL);
if (open_file_list_current_ptr == open_file_list_head_ptr)  /* 1st entry? */
    open_file_list_head_ptr = open_file_list_current_ptr->next;
else
    {                          /* Find previous file's entry in list. */
    open_file_list_prev_ptr = open_file_list_head_ptr;
    while (open_file_list_prev_ptr->next != open_file_list_current_ptr)
        open_file_list_prev_ptr = open_file_list_prev_ptr->next;
    open_file_list_prev_ptr->next = open_file_list_current_ptr->next;
    }
free(open_file_list_current_ptr);
return(fn_allocate_temp_key_memory());  /* Everythings's ok if this works. */
}                                       /*  If not, file's still closed!   */

/*-------------------- open read --------------------*/

fn_i_f_open_read(int *file_handle,char file_id[])
{
int errcode;
fn_file_handle_new();
if (open_file_list_current_ptr == NULL)
    return(I_F_ERROR_OUTOFMEMORY);
open_file_list_current_ptr->file_ptr = fopen(file_id,"r");
if (open_file_list_current_ptr->file_ptr == NULL)   /* Problem opening file? */
    {                        /* Undo what we just did. */
    fn_reverse_list_entry();
    return(I_F_ERROR_BADFILENAME);
    }
open_file_list_current_ptr->open_mode = I_F_OPEN_READ;   /* Note the open mode. */
open_file_list_current_ptr->current_key = 0;
open_file_list_current_ptr->current_record_fptr = 0;
open_file_list_current_ptr->lastread_record_fptr = 0;
fn_read_file_header();        /* Check software revision level of file. */
if (file_header.revision_level < I_F_LOWEST_USABLE_REVISION)  {
    fclose(open_file_list_current_ptr->file_ptr); /* File's rev. level too low. */
    fn_reverse_list_entry();
    return(I_F_ERROR_BADREVLEVEL);  }
*file_handle = open_file_list_current_ptr->file_handle;
errcode = fn_allocate_temp_key_memory();
if (errcode)  {
    fclose(open_file_list_current_ptr->file_ptr);
    fn_reverse_list_entry();
    return(errcode);  } /* The right errcode is returned. */
return(I_F_SUCCESS);
}

/*-------------------- open write --------------------*/

fn_i_f_open_write(int *file_handle,char file_id[])
{
unsigned char key_sub,      /* This subscript is for writing out the */
                            /*  key defs. to the file.               */
              no_of_keys;   /* This holds the parsed number of keys. */
int errcode;

if (strlen(I_F_FILE_ATTRIBUTES) == 0)  /* No attributes specified? */
    return(I_F_ERROR_BADATTRIBUTES);
if (I_F_FILE_ATTRIBUTES[strlen(I_F_FILE_ATTRIBUTES)-1] != ' ')
    strcat(I_F_FILE_ATTRIBUTES," "); /* Add space to end to simplify parsing. */

fn_file_handle_new();                /* Create file handle and open file. */
if (open_file_list_current_ptr == NULL)
    return(I_F_ERROR_OUTOFMEMORY);
open_file_list_current_ptr->file_ptr = fopen(file_id,"w+");
if (open_file_list_current_ptr->file_ptr == NULL)   /* Problem opening file? */
    {
    fn_reverse_list_entry();
    return(I_F_ERROR_BADFILENAME);
    }
open_file_list_current_ptr->open_mode = I_F_OPEN_WRITE; /* Note the open mode. */
open_file_list_current_ptr->current_key = 0;
open_file_list_current_ptr->current_record_fptr = 0;
open_file_list_current_ptr->lastread_record_fptr = 0;

file_header.revision_level =       /* Write out file header. */
  I_F_CURRENT_REVISION;
file_header.record_length = i_f_next_file_attribute(1); /* Get 1st attribute.*/
no_of_keys = i_f_next_file_attribute(0);
if (no_of_keys < 1)  {
    fn_bad_attributes(file_id);
    return(I_F_ERROR_BADATTRIBUTES);  }
file_header.no_of_keys = no_of_keys;
file_header.empty_space_fptr = 0;
fn_write_file_header();

for (key_sub=0;key_sub<no_of_keys;key_sub++)
    {
    key_definition.position = i_f_next_file_attribute(0);
    key_definition.length = i_f_next_file_attribute(0);
    if (!key_definition.length)   {
        fn_bad_attributes(file_id);
        return(I_F_ERROR_BADATTRIBUTES);  }
	key_definition.duplicates_flag = i_f_next_file_attribute(0);

    key_definition.root_fptr = 0;   /* Empty file - empty tree. */
    fn_write_key_definition(key_sub);
    }
errcode = fn_allocate_temp_key_memory();
if (errcode)  {
    fn_bad_attributes(file_id);
    return(errcode);  } /* The right errcode is returned. */
*file_handle = open_file_list_current_ptr->file_handle; /* Return file handle.*/
return(I_F_SUCCESS);
}

/*-------------------- open io --------------------*/

fn_i_f_open_io(int *file_handle,char file_id[])
{
int errcode;
fn_file_handle_new();
if (open_file_list_current_ptr == NULL)
    return(I_F_ERROR_OUTOFMEMORY);
open_file_list_current_ptr->file_ptr = fopen(file_id,"r+");
if (open_file_list_current_ptr->file_ptr == NULL)
    {
    fn_reverse_list_entry();
    return(I_F_ERROR_BADFILENAME);
    }
open_file_list_current_ptr->open_mode = I_F_OPEN_IO;   /* Note the open mode. */
open_file_list_current_ptr->current_key = 0;
open_file_list_current_ptr->current_record_fptr = 0;
open_file_list_current_ptr->lastread_record_fptr = 0;
                                  /* Check software revision level of file. */
fn_read_file_header();
if (file_header.revision_level < I_F_LOWEST_USABLE_REVISION)  {
    fclose(open_file_list_current_ptr->file_ptr); /* File's rev. level too low. */
    fn_reverse_list_entry();
    return(I_F_ERROR_BADREVLEVEL);  }
errcode = fn_allocate_temp_key_memory();
if (errcode)  {
    fclose(open_file_list_current_ptr->file_ptr);
    fn_reverse_list_entry();
    return(errcode);  }
*file_handle = open_file_list_current_ptr->file_handle;
return(I_F_SUCCESS);
}

/*-------------------- close all --------------------*/

fn_i_f_close_all(void)    /* This can be called anytime, even if no files open. */
{
while (open_file_list_head_ptr != NULL)
	fn_i_f_close(open_file_list_head_ptr->file_handle);
return(fn_allocate_temp_key_memory());
}

/*-------------------- random read --------------------*/

fn_i_f_read(int file_handle,char the_record[],unsigned char key_no)
{
int result;  /* For holding key comparison result. */

fn_file_handle_find(file_handle);              /* Do these steps        */
if (open_file_list_current_ptr == NULL)        /*   for all record      */
    return(I_F_ERROR_BADHANDLE);               /*   operations          */
switch (open_file_list_current_ptr->open_mode) /*   v                   */
    {
    case I_F_OPEN_READ:   break;
    case I_F_OPEN_WRITE:  return(I_F_ERROR_BADOPERATION);
	case I_F_OPEN_IO:     break;
    default:              return(I_F_ERROR_INTERNAL);
    }                                          /*   .... down to here.   */

fn_read_file_header();
if (key_no >= file_header.no_of_keys)   /* Is the requested key valid? */
    return(I_F_ERROR_BADKEY);
open_file_list_current_ptr->current_key = key_no;

fn_read_key_definition(key_no);      /* Start traversing on correct key. */
if (key_definition.root_fptr == 0)
  {
  open_file_list_current_ptr->current_record_fptr = 0;
	open_file_list_current_ptr->lastread_record_fptr = 0;
  return(I_F_ERROR_KEYNOTFOUND);
  }
open_file_list_current_ptr->current_record_fptr = key_definition.root_fptr;
while (1)  /* Keep traversing until we break out. */
	{
	fn_read_keys_pointers(open_file_list_current_ptr->current_record_fptr,key_no);
	result = fn_compare_keys(open_file_list_current_ptr->current_record_fptr,key_no,the_record);
  if (result > 0)        /* To the right of this node? */
    {
    open_file_list_current_ptr->current_record_fptr = keys_pointers.right_fptr;
    if (keys_pointers.right_fptr == 0)
      return(I_F_ERROR_KEYNOTFOUND);
    continue;  /* Traverse to next. */
    }
  if (result < 0)        /* To the left? */
    {
    open_file_list_current_ptr->current_record_fptr = keys_pointers.left_fptr;
    if (keys_pointers.left_fptr == 0)
      return(I_F_ERROR_KEYNOTFOUND);
    continue;
    }
  /* This key is equal. */
  if (!key_definition.duplicates_flag)
    break;  /* No need to search farther if no duplicates. */
  if ( (!I_F_DIRECTION) && (!keys_pointers.left_fptr) )  /* No possibility of duplicates to the left.  */
    break;
  if ( (I_F_DIRECTION) && (!keys_pointers.right_fptr) )  /* No possibility of duplicates to the right. */
    break;
  /* Go check out the "next" key to see if it's a duplicate or not. */
  if (!I_F_DIRECTION)
    open_file_list_current_ptr->current_record_fptr = keys_pointers.left_fptr;
  else
    open_file_list_current_ptr->current_record_fptr = keys_pointers.right_fptr;
  fn_read_keys_pointers(open_file_list_current_ptr->current_record_fptr,key_no);
  result = fn_compare_keys(open_file_list_current_ptr->current_record_fptr,key_no,the_record);
  if (!result)  /* Was this "next" key a duplicate (which we want to go to)?  */
    continue;   /*  Fall on through and keep going.  This key will be reread. */
  /* Not a desired record.  Go back to parent and break out of loop. */
  open_file_list_current_ptr->current_record_fptr = keys_pointers.parent_fptr;
  break;
  }
/* At this point, the current_record_pointer is set correctly for reading.    */
/*  Read record info., then move the file pointer to the next record or NULL. */
fn_read_record_info(open_file_list_current_ptr->current_record_fptr,the_record);
open_file_list_current_ptr->lastread_record_fptr =   /* Save fptr now. */
  open_file_list_current_ptr->current_record_fptr;
fn_traverse_to_next(I_F_DIRECTION);
return(I_F_SUCCESS);
}

/*-------------------- start by specified key --------------------*/

fn_i_f_start(int file_handle,char the_record[],unsigned char key_no)
{
int result; /* For holding result of memcmp() done on keys. */

fn_file_handle_find(file_handle);             /* Do these steps        */
if (open_file_list_current_ptr == NULL)       /*   for all record      */
    return(I_F_ERROR_BADHANDLE);              /*   operations          */
switch (open_file_list_current_ptr->open_mode) /*   v                   */
    {
    case I_F_OPEN_READ:   break;
    case I_F_OPEN_WRITE:  return(I_F_ERROR_BADOPERATION);
    case I_F_OPEN_IO:     break;
    default:              return(I_F_ERROR_INTERNAL);
	}                                         /*   .... down to here.   */

fn_read_file_header();
if (key_no >= file_header.no_of_keys)   /* Is the requested key valid? */
	return(I_F_ERROR_BADKEY);
open_file_list_current_ptr->current_key = key_no;

fn_read_key_definition(key_no);      /* Start traversing on correct key. */
if (key_definition.root_fptr == 0)  {
	open_file_list_current_ptr->current_record_fptr = 0;
	open_file_list_current_ptr->lastread_record_fptr = 0;
	return(I_F_ERROR_KEYNOTFOUND);  }
open_file_list_current_ptr->current_record_fptr = key_definition.root_fptr;
while (1)  /* Keep traversing until we break out. */
	{
	fn_read_keys_pointers(open_file_list_current_ptr->current_record_fptr,key_no);
	result = fn_compare_keys(open_file_list_current_ptr->current_record_fptr,key_no,the_record);
	if (!key_definition.duplicates_flag)   /* No duplicates? */
	if (result == 0)
		break;  /* No need to search farther if no duplicates. */
	if (I_F_DIRECTION)
		{
		if (result > 0)            /* To the right of this node? */
			{
			if (keys_pointers.right_fptr == 0)
				{
				fn_traverse_to_next(I_F_DIRECTION);
				if (!open_file_list_current_ptr->current_record_fptr)
					return(I_F_ERROR_KEYNOTFOUND);
				break;   /* Return the value we found to be next. */
				}
			else
				open_file_list_current_ptr->current_record_fptr =
				  keys_pointers.right_fptr;
			}
		else
			{
			if (keys_pointers.left_fptr == 0)
				break;   /* Return the value of the current node. */
			else
				open_file_list_current_ptr->current_record_fptr =
				  keys_pointers.left_fptr;
			}
		}
	else
		{                     /* This is the code for a start-previous. */
		if (result < 0)       /* To the left of this node? */
			{
			if (keys_pointers.left_fptr == 0)
				{
				fn_traverse_to_next(I_F_DIRECTION);
				if (!open_file_list_current_ptr->current_record_fptr)
					return(I_F_ERROR_KEYNOTFOUND);
				break;   /* Return the value we found to be previous. */
				}
			else
				open_file_list_current_ptr->current_record_fptr =
				  keys_pointers.left_fptr;
			}
		else
			{
			if (keys_pointers.right_fptr == 0)
				break;   /* Return the value of the current node. */
			else
				open_file_list_current_ptr->current_record_fptr =
				  keys_pointers.right_fptr;
			}
		}
	/* Traverse to next key. */
	}

/* At this point, the current_record_pointer is set correctly. */
open_file_list_current_ptr->lastread_record_fptr = 0;
return(I_F_SUCCESS);
}

/*-------------------- read next along current key --------------------*/

fn_i_f_readnext(int file_handle,char the_record[])
{
fn_file_handle_find(file_handle);             /* Do these steps        */
if (open_file_list_current_ptr == NULL)       /*   for all record      */
    return(I_F_ERROR_BADHANDLE);              /*   operations          */
switch (open_file_list_current_ptr->open_mode) /*   v                   */
    {
    case I_F_OPEN_READ:   break;
    case I_F_OPEN_WRITE:  return(I_F_ERROR_BADOPERATION);
    case I_F_OPEN_IO:     break;
    default:              return(I_F_ERROR_INTERNAL);
    }                                         /*   .... down to here.   */
if (open_file_list_current_ptr->current_record_fptr == 0)
    return(I_F_ERROR_KEYNOTFOUND);
fn_read_file_header();
fn_read_record_info(open_file_list_current_ptr->current_record_fptr,the_record);
open_file_list_current_ptr->lastread_record_fptr =   /* Save this now. */
  open_file_list_current_ptr->current_record_fptr;
fn_traverse_to_next(I_F_DIRECTION);   /* Traverse current record fptr to next. */
return(I_F_SUCCESS);
}

/*-------------------- random write --------------------*/

fn_i_f_write(int file_handle,char the_record[])
{
unsigned char key_sub;    /* Used for updating keys. */
long new_records_fptr; /* Remember where we write new record in file.*/

fn_file_handle_find(file_handle);             /* Do these steps        */
if (open_file_list_current_ptr == NULL)       /*   for all record      */
    return(I_F_ERROR_BADHANDLE);              /*   operations          */
switch (open_file_list_current_ptr->open_mode) /*   v                   */
    {
    case I_F_OPEN_READ:   return(I_F_ERROR_BADOPERATION);
    case I_F_OPEN_WRITE:  break;
    case I_F_OPEN_IO:     break;
    default:              return(I_F_ERROR_INTERNAL);
    }                                         /*   .... down to here.   */

/* First, is it ok to write record?  Check for duplicate keys and flags. */
fn_read_file_header();
if (file_header.record_length)
    I_F_RECORD_LENGTH = file_header.record_length;
for (key_sub=0;key_sub<file_header.no_of_keys;key_sub++)
    {
    fn_read_key_definition(key_sub);
    /* Is record long enough to contain this key in its entirety? */
    if (I_F_RECORD_LENGTH < (key_definition.position+key_definition.length))
        return(I_F_ERROR_BADATTRIBUTES);
    if (key_definition.duplicates_flag)  /* Are duplicates allowed? */
        continue;    /* Yes; no need to check for them for this key.        */
    if (fn_check_for_duplicates(key_sub,the_record))
        return(I_F_ERROR_KEYEXISTS);
    }
/* Done checking keys for duplicates. */

/* Now write record with empty pointer information.  Where will we write  */
/*  this record?  If variable record length or no unused file space, then */
/*  write at EOF.  Otherwise, put record at 1st available empty space.    */
if ((!file_header.record_length) || (!file_header.empty_space_fptr))
    {
    fseek(open_file_list_current_ptr->file_ptr,0,SEEK_END);   /* Save fptr.*/
    new_records_fptr = ftell(open_file_list_current_ptr->file_ptr);
    }
else
    {     /* Records are fixed length and an empty space is available. */
    new_records_fptr = file_header.empty_space_fptr;  /* Where new record will go. */
    fn_read_empty_space_info(file_header.empty_space_fptr);
    file_header.empty_space_fptr = empty_space_info.next_fptr;  /* Point to next available. */
    fn_write_file_header();   /* Write updated information. */
    }
for (key_sub=0;key_sub<file_header.no_of_keys;key_sub++)
    {
    keys_pointers.parent_fptr = 0;  /* In case of problem, there will at  */
    keys_pointers.left_fptr   = 0;  /*  least be these nulls for pointers */
    keys_pointers.right_fptr  = 0;  /*  instead of random stuff in file.  */
    fn_write_keys_pointers(new_records_fptr,key_sub);
    }
/* This is followed in the file by the record information. */
fn_write_record_info(new_records_fptr,the_record);

/* Now update the new record's pointers. */
for (key_sub=0;key_sub<file_header.no_of_keys;key_sub++)
    fn_add_node_to_keys_tree(new_records_fptr,key_sub,the_record);

open_file_list_current_ptr->current_record_fptr = 0;
open_file_list_current_ptr->lastread_record_fptr = 0;
return(I_F_SUCCESS);
}

/*-------------------- rewrite --------------------*/

fn_i_f_rewrite(int file_handle,char the_record[])
               /* This routine doesn't touch current_record_fptr. */
{
unsigned char key_sub;    /* Used for updating keys. */
int result; /* For holding result of memcmp() done on keys. */

fn_file_handle_find(file_handle);             /* Do these steps        */
if (open_file_list_current_ptr == NULL)       /*   for all record      */
    return(I_F_ERROR_BADHANDLE);              /*   operations          */
switch (open_file_list_current_ptr->open_mode) /*   v                   */
    {
    case I_F_OPEN_READ:   return(I_F_ERROR_BADOPERATION);
    case I_F_OPEN_WRITE:  break;
    case I_F_OPEN_IO:     break;
    default:              return(I_F_ERROR_INTERNAL);
    }                                         /*   .... down to here.   */

/* Check record length to see if its ok. */
fn_read_file_header();
if (file_header.record_length)
    I_F_RECORD_LENGTH = file_header.record_length;

/* Check for duplicate keys if not allowed. */
for (key_sub=0;key_sub<file_header.no_of_keys;key_sub++)
    {
    fn_read_key_definition(key_sub);
    /* Is record long enough to contain this key in its entirety? */
    if (I_F_RECORD_LENGTH < (key_definition.position+key_definition.length))
        return(I_F_ERROR_BADATTRIBUTES);
    if (key_definition.duplicates_flag)  /* Are duplicates allowed? */
        continue;    /* Yes; no need to check for them for this key.  */
    /* Did key change?  If not, don't check for duplicates. */
    result = fn_compare_keys(open_file_list_current_ptr->lastread_record_fptr,key_sub,the_record);
    if (!result)
       continue;
    if (fn_check_for_duplicates(key_sub,the_record))
        return(I_F_ERROR_KEYEXISTS);
    }
/* Done checking keys for duplicates. */

if (!file_header.record_length)     /* Variable record lengths?  If so, check to see if record being rewritten is longer than one read. */
    fn_read_record_descriptor(open_file_list_current_ptr->lastread_record_fptr);
else
    record_descriptor.record_length = file_header.record_length;
if (I_F_RECORD_LENGTH > record_descriptor.record_length)    /* This can only happen for variable record lengths. */
    {    /* New record length is longer; stick record at EOF. */
    for (key_sub=0;key_sub<file_header.no_of_keys;key_sub++)  /* 1st remove references to record's old location. */
        fn_delete_node_from_keys_tree(open_file_list_current_ptr->lastread_record_fptr,key_sub);
    fseek(open_file_list_current_ptr->file_ptr,0,SEEK_END);   /* Point to EOF. */
    open_file_list_current_ptr->lastread_record_fptr = ftell(open_file_list_current_ptr->file_ptr);  /* Save fptr.*/
    for (key_sub=0;key_sub<file_header.no_of_keys;key_sub++)
        {
        keys_pointers.parent_fptr = 0;  /* In case of problem, there will at  */
        keys_pointers.left_fptr   = 0;  /*  least be these nulls for pointers */
        keys_pointers.right_fptr  = 0;  /*  instead of random stuff in file.  */
        fn_write_keys_pointers(open_file_list_current_ptr->lastread_record_fptr,key_sub);
        }
    fn_write_record_info(open_file_list_current_ptr->lastread_record_fptr,the_record);
    for (key_sub=0;key_sub<file_header.no_of_keys;key_sub++)
        fn_add_node_to_keys_tree(open_file_list_current_ptr->lastread_record_fptr,key_sub,the_record);
    }
else
    {    /* This code rewrites fixed-length records or variable-length ones that didn't get bigger since they were read. */
    /* First update the record's pointers. */
    for (key_sub=0;key_sub<file_header.no_of_keys;key_sub++)
        {
        /* First, did the key's value change?  No need to update if not. */
        result = fn_compare_keys(open_file_list_current_ptr->lastread_record_fptr,key_sub,the_record);
        if (result)
            {
            fn_delete_node_from_keys_tree(open_file_list_current_ptr->lastread_record_fptr,key_sub);
            fn_add_node_to_keys_tree(open_file_list_current_ptr->lastread_record_fptr,key_sub,the_record);
            }
        }
    /* Now rewrite record info.... */
    fn_write_record_info(open_file_list_current_ptr->lastread_record_fptr,the_record);
    }
return(I_F_SUCCESS);
}

/*-------------------- delete --------------------*/

fn_i_f_delete(int file_handle)   /* Deletes the last node read.  Does nothing */
                                 /*  to current_record_fptr.                  */
{
unsigned char key_sub;    /* Used for updating keys. */
long fptr_to_delete;
fn_file_handle_find(file_handle);              /* Do these steps        */
if (open_file_list_current_ptr == NULL)        /*   for all record      */
    return(I_F_ERROR_BADHANDLE);               /*   operations          */
switch (open_file_list_current_ptr->open_mode) /*   v                   */
    {
    case I_F_OPEN_READ:   return(I_F_ERROR_BADOPERATION);
    case I_F_OPEN_WRITE:  break;
    case I_F_OPEN_IO:     break;
    default:              return(I_F_ERROR_INTERNAL);
    }                                         /*   .... down to here.   */
if (!open_file_list_current_ptr->lastread_record_fptr)
    return(I_F_ERROR_KEYNOTFOUND);
fptr_to_delete = open_file_list_current_ptr->lastread_record_fptr;

fn_read_file_header();    /* Now update records' pointers. */
for (key_sub=0;key_sub<file_header.no_of_keys;key_sub++)
    fn_delete_node_from_keys_tree(fptr_to_delete,key_sub);

/* Mark space as unused in file if this is a fixed record length file. */
if (file_header.record_length)  {
    empty_space_info.next_fptr = file_header.empty_space_fptr;
    fn_write_empty_space_info(fptr_to_delete);   /* Insert at start of list. */
    file_header.empty_space_fptr = fptr_to_delete;
    fn_write_file_header();  }

/* Done updating all keys of file. */
open_file_list_current_ptr->lastread_record_fptr = 0;
return(I_F_SUCCESS);
}

/*--------------------- report file attributes --------------------*/

fn_i_f_report_attributes(int file_handle)
{
unsigned char key_sub;
char the_number[8];

fn_file_handle_find(file_handle);    /* Get pointer to info. in memory. */
if (open_file_list_current_ptr == NULL)
    return(I_F_ERROR_BADHANDLE);

strcpy(I_F_FILE_ATTRIBUTES,""); /* Init. this string to empty; then add to it. */

fn_read_file_header();
sprintf(the_number,"%d",file_header.record_length);
/* itoa(file_header.record_length,the_number,10); */ /* 0 or record length.*/
strcat(I_F_FILE_ATTRIBUTES,the_number);
strcat(I_F_FILE_ATTRIBUTES," ");
sprintf(the_number,"%d",file_header.no_of_keys);
/* itoa(file_header.no_of_keys,the_number,10); */ /* Return the # of keys. */
strcat(I_F_FILE_ATTRIBUTES,the_number);
strcat(I_F_FILE_ATTRIBUTES," ");

for (key_sub=0;key_sub<file_header.no_of_keys;key_sub++)
    {
    fn_read_key_definition(key_sub);
    sprintf(the_number,"%d",key_definition.position);
/*    itoa(key_definition.position,the_number,10); */
    strcat(I_F_FILE_ATTRIBUTES,the_number);
    strcat(I_F_FILE_ATTRIBUTES," ");
    sprintf(the_number,"%d",key_definition.length);
/*    itoa(key_definition.length,the_number,10); */
    strcat(I_F_FILE_ATTRIBUTES,the_number);
    strcat(I_F_FILE_ATTRIBUTES," ");
    sprintf(the_number,"%d",key_definition.duplicates_flag);
/*    itoa(key_definition.duplicates_flag,the_number,10); */
    strcat(I_F_FILE_ATTRIBUTES,the_number);
    strcat(I_F_FILE_ATTRIBUTES," ");
    }
I_F_FILE_ATTRIBUTES[strlen(I_F_FILE_ATTRIBUTES)-1] = 0; /* Remove last space. */
return(I_F_SUCCESS);
}


/*----------------------- THE MAIN ROUTINE ---------------------------------*/

int indexed_file(int *file_handle,int operation,void *passed_string,unsigned char key_no)
{

/*
if (!i_f_copyright_notice_displayed_flag)
  {
  i_f_copyright_notice_displayed_flag = 1;
  printf("+------------------------------+\n");
  printf("| The Indexed File Engine is   |\n");
  printf("| Copyright (C)1991,1998 by....|\n");
  printf("|    Alton Moore               |\n");
  printf("|    Rt. 6, Box 694-A          |\n");
  printf("|    Edinburg, TX  78539-9805  |\n");
  printf("|    amoore@hiline.net         |\n");
  printf("+------------------------------+\n");
  }
*/

switch (operation)
  {
  case I_F_CLOSE:              I_F_RESULTCODE = fn_i_f_close(*file_handle);                              break;
  case I_F_OPEN_READ:          I_F_RESULTCODE = fn_i_f_open_read(&*file_handle,(char *)passed_string);   break;
  case I_F_OPEN_WRITE:         I_F_RESULTCODE = fn_i_f_open_write(&*file_handle,(char *)passed_string);  break;
  case I_F_OPEN_IO:            I_F_RESULTCODE = fn_i_f_open_io(&*file_handle,(char *)passed_string);     break;
  case I_F_CLOSE_ALL:          I_F_RESULTCODE = fn_i_f_close_all();                                      break;
  case I_F_READ:               I_F_RESULTCODE = fn_i_f_read(*file_handle,(char *)passed_string,key_no);  break;
  case I_F_START:              I_F_RESULTCODE = fn_i_f_start(*file_handle,(char *)passed_string,key_no); break;
  case I_F_READNEXT:           I_F_RESULTCODE = fn_i_f_readnext(*file_handle,(char *)passed_string);     break;
  case I_F_WRITE:              I_F_RESULTCODE = fn_i_f_write(*file_handle,(char *)passed_string);        break;
  case I_F_REWRITE:            I_F_RESULTCODE = fn_i_f_rewrite(*file_handle,(char *)passed_string);      break;
  case I_F_DELETE:             I_F_RESULTCODE = fn_i_f_delete(*file_handle);                             break;
  case I_F_REPORT_ATTRIBUTES:  I_F_RESULTCODE = fn_i_f_report_attributes(*file_handle);                  break;
  default:                     I_F_RESULTCODE = I_F_ERROR_BADOPERATION;                                  break;
  }
return(I_F_RESULTCODE);
}


