#include<stdio.h>
#include<stdlib.h> 
#include<string.h> 
#include "mpi.h"

#define TOTAL_RECORDS_LENGTH 20 /* Defines the total number of records present in the database */ 
#define TOTAL_STRING_LENGTH 4000 /* Maximum lenth of the string processed */

void sort();
int match(char* _searchWord, char* _sentence);
int matchWord( char* s1, char *tempSearchWord);


FILE *file, *file_seek, *file_proc, *file_result, *file_result_sort, *file_relations;

int main(int argc, char *argv[]) 
{
 
 		int p, proc, start_row, end_row, number_of_rows, tag =0;
 		int i,j, k, seek_source;
 		char mystring[TOTAL_STRING_LENGTH];
 		char done_string[TOTAL_STRING_LENGTH]; 		
 		char string_source[TOTAL_STRING_LENGTH];
 		char search_string[100];
 		char done[4];
 		int num_of_lines = 0, num_of_lines_open=0;
 		int num_present;
 		double start_time, end_time;
 		int string_length;
 		int requested = 0;

 		MPI_Status status;  
  		MPI_Init ( &argc, &argv );
  		MPI_Comm_rank ( MPI_COMM_WORLD, &proc );
  		MPI_Comm_size ( MPI_COMM_WORLD, &p);

  		file_result=fopen("SearchEngineOutput.txt","w");
  		file_relations = fopen("RelatedOutputs.txt","w");
  		file=fopen("input_source.txt","r");

  		while (fgets (mystring , TOTAL_STRING_LENGTH , file) != NULL)		
  			num_of_lines++;
  		
  		int rows_per_process = num_of_lines/(p-1);
 		if(proc == 0)
		{
 			if(requested == 0 )
			{
 				requested =1;
				printf("Enter search string :: ");
  				fgets(search_string, sizeof(search_string), stdin); 
 			}
 		start_time = MPI_Wtime();
			printf("Dividing the work to %d Processes \n", p);
  			for(proc = 1; proc < p; proc ++)
			{
  				start_row = (proc-1)*rows_per_process;
  				end_row = (proc)*rows_per_process-1;
  				
				if(proc == (p-1))
  					end_row = num_of_lines-1;
  				
				number_of_rows = end_row - start_row;
  				file_seek=fopen("input_source.txt","r");
  				
				for(i=0;i<start_row;i++)				
  					fgets (mystring , TOTAL_STRING_LENGTH , file_seek);
  				
  				seek_source=ftell (file_seek);
  				MPI_Send(&seek_source, 1, MPI_INT, proc, tag, MPI_COMM_WORLD);
    			MPI_Send(&number_of_rows, 1, MPI_INT, proc, tag, MPI_COMM_WORLD);
    			MPI_Send(search_string, 100, MPI_CHAR, proc, tag, MPI_COMM_WORLD);
    			MPI_Send(&start_row, 1, MPI_INT, proc, tag, MPI_COMM_WORLD);
    			MPI_Send(&end_row, 1, MPI_INT, proc, tag, MPI_COMM_WORLD);
 				}
  			for(proc =1; proc <p;proc++)			
  				MPI_Recv(done, 4, MPI_CHAR, proc, tag, MPI_COMM_WORLD, &status);
  			end_time = MPI_Wtime();
  			printf("Best match out of all the results obtained \n");
  			sort();
			printf("Matching the relevant query words %s \n", search_string);
			printf("Results written to Output File !! \n");
			printf("Time elapsed is %1.5f\n",(end_time - start_time));
 		}
		else
		{
 			MPI_Recv(&seek_source, 1, MPI_INT, 0, tag, MPI_COMM_WORLD, &status);
			MPI_Recv(&number_of_rows, 1, MPI_INT, 0, tag, MPI_COMM_WORLD, &status);
			MPI_Recv(search_string, 100, MPI_CHAR, 0, tag, MPI_COMM_WORLD, &status);
			MPI_Recv(&start_row, 1, MPI_INT, 0, tag, MPI_COMM_WORLD, &status);
			MPI_Recv(&end_row, 1, MPI_INT, 0, tag, MPI_COMM_WORLD, &status);
						
			string_length = strlen(search_string);
			file_result=fopen("SearchEngineOutput.txt","a");
 			file_proc=fopen("input_source.txt","r");
 			fseek ( file_proc, seek_source, SEEK_SET );
 			
 			for(k =start_row; k<=end_row;k++)
			{
 				i=k-start_row;
				fgets (string_source , TOTAL_STRING_LENGTH , file_proc);
				
				num_present = match (string_source, search_string);
				if(num_present >0)
				{
					fprintf(file_result, "%d\n", num_present);
					fprintf(file_result, "%s", string_source);				
				}
			}
			MPI_Send("DONE", 4, MPI_CHAR, 0, tag, MPI_COMM_WORLD);
			fclose(file_result);
			
			
			
 		}
 		fclose(file_relations);	
		
 		MPI_Finalize();
 		
        return(0); 
 
}

int match(char* _sentence, char* _searchWord)
{	
	int i,j, k = 0;
	int flag =0, count = 0;
	int result=0, wordCount = 0;
	char *tempWord, *firstSent ;
	char tempSentence[TOTAL_STRING_LENGTH], tempSearchWord[25], firstSentence[TOTAL_STRING_LENGTH];
	char *listofRelatedQueries[] = {"pizza", "menu", "toppings", "sides", "review", "drinks",
							  "like","dislike","sauce","saucage","veggies","bacon", 
							  "price","worth","ambience","music","servings","quantity","hungry", "location"};


	char *RelatedQueries[10];
	strcpy(firstSentence, _sentence);
	strcpy(tempSentence, _sentence);
	strcpy(tempSearchWord, _searchWord);
	tempSentence[999] = '\0';
	firstSentence[999]='\0';
	file_relations = fopen("RelatedOutputs.txt","a");
	
	for(i =0;i<strlen(tempSearchWord);i++)
		tempSearchWord[i] = tolower(tempSearchWord[i]);
	
	firstSent = strtok(firstSentence, " .?;,");		
	while(firstSent != NULL)
	{
		wordCount++;
		if(wordCount == 10)
			break;
		for(i =0;i<strlen(firstSent);i++) 
			firstSent[i] = tolower(firstSent[i]);

		if(matchWord(firstSent, tempSearchWord) == 1)
			result+=3;

		for(i=0;i<TOTAL_RECORDS_LENGTH;i++) 
		{
			if(matchWord(firstSent, listofRelatedQueries[i]) == 1) 		
				result--;
		}
		firstSent = strtok(NULL, " .?;,");	
	}
	strcpy(firstSentence, "");	


	tempWord = strtok(tempSentence, " ,.?;");
	while(tempWord != NULL) 
	{
		for(i =0;i<strlen(tempSearchWord);i++)
			tempSearchWord[i] = tolower(tempSearchWord[i]);
		for(i =0;i<strlen(tempWord);i++)
			tempWord[i] = tolower(tempWord[i]);
				
		for(i=0;i<TOTAL_RECORDS_LENGTH;i++)
		{
			count = 0;
			if(matchWord(tempWord, listofRelatedQueries[i]) == 1)
			{
				if(matchWord(tempSearchWord, tempWord) == 1)
				{
					flag =1;
				}
				else
				{
					for(j=0;j<k;j++)
					{
						if(matchWord(tempWord, RelatedQueries[j]) == 1)
							count = 1;
					}
					if(count == 0)
						RelatedQueries[k++] = listofRelatedQueries[i];
				}
			}
		}
		
		if(matchWord(tempWord, tempSearchWord)==1)
			result++;

		tempWord = strtok(NULL, " ,.?;");
	}	
	
	if(flag == 1)
	{
		for(j=0;j<k;j++)
		{
			fprintf(file_relations,"  %s is a RELATED QUERY\n", RelatedQueries[j]);
			fprintf(file_relations,"%s", _sentence);
		}
	}
	return result;
}

int matchWord( char* s1, char *tempSearchWord)
{
	int A = strlen(s1);
	int B = strlen(tempSearchWord);
	int min_len = A>B?B:A;
	int i;
	
	if(abs(A-B) > 1)
		return 0;
	for(i=0;i<min_len;i++)
	{
		if(s1[i] == tempSearchWord[i]){}		
		else
			return 0;
	}
	return 1;
}

void sort()
{
	int num_of_lines_open=0;
	int i,j, temp_num;
	char temp_sentence[TOTAL_STRING_LENGTH];
	char done_string [TOTAL_STRING_LENGTH];
	
	file_result_sort=fopen("SearchEngineOutput.txt","r");
  	while (fgets (done_string , TOTAL_STRING_LENGTH , file_result_sort) != NULL)	
  		num_of_lines_open++;
  	
  	fclose(file_result_sort);
  	file_result_sort=fopen("SearchEngineOutput.txt","r");
  	int count_word[num_of_lines_open];
	char sentence_word[num_of_lines_open][TOTAL_STRING_LENGTH];
	i=0;
	while (fgets (done_string , TOTAL_STRING_LENGTH , file_result_sort) != NULL)
	{
  		count_word[i] = atoi(done_string);
  		fgets (done_string , TOTAL_STRING_LENGTH , file_result_sort);
  		strcpy(sentence_word[i], done_string);
  		i++;
  	}
  	for(i=0;i<num_of_lines_open/2;i++)
	{
  		for(j=i+1;j<num_of_lines_open/2;j++)
		{
  			if(count_word[i]<count_word[j])
			{
  				temp_num = count_word[i];
  				count_word[i] = count_word[j];
  				count_word[j] = temp_num;
  				strcpy(temp_sentence, sentence_word[i]);
  				strcpy(sentence_word[i], sentence_word[j]);
  				strcpy(sentence_word[j], temp_sentence);
  			}
  		}
  	}

  	fclose(file_result_sort);
  	file_result_sort=fopen("SearchEngineOutput.txt","w");
  	for(j=0;j<num_of_lines_open/2;j++)
	{
  		fprintf(file_result_sort, "%d\n",count_word[j]);
  		fprintf(file_result_sort, "%s",sentence_word[j]);
  		fflush(file_result_sort);
  	}
}

		
		
		
		
