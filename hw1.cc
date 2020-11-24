#include <cstdio>
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <boost/sort/spreadsort/float_sort.hpp>
#include <boost/sort/spreadsort/spreadsort.hpp>
#define DATA_TYPE boost::int64_t
// float_sort
// #include <boost/sort/sort.hpp>
// for block_indirect_sort
// boost::sort::spinsort
// #include <boost/sort/parallel/sort.hpp>
// namespace bsp = boost::sort::parallel;


#include <iostream>
#include <vector>
#include <algorithm>
using namespace std;

#define NUM_DATA 1  // number of data to be sent

// #define WTIME
#define CAL_TIME_RANK 0


// diff States
#define ONE_PROC 2
#define PROC_MORE_THAN_NUMBER 1
#define PROC_LESS_THAN_NUMBER 0


void mergeArrays(float* arr1, float arr2[], int n1, int n2, float arr3[]) ;

int main(int argc, char** argv) {
	
    // printf("n: %s\n",argv[2]);
    int rc;
	rc = MPI_Init(&argc, &argv);
	if (rc != MPI_SUCCESS) {
		printf ("Error starting MPI program. Terminating.\n");
		MPI_Abort (MPI_COMM_WORLD, rc);
	} 
	
    ///////// Get number of FLOAT numbers ////////
    int n;   // number of floating numbers
    n = atoi(argv[1]);

    //// Get rank ID & size (number of processes)
    int rank, size;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);

    //////// TIME MEASUREMENT //////////
    #if defined(WTIME)
    double starttime, endtime;
    double IOstarttime, IOendtime;     // read, write file time.
    // double CPUstarttime, CPUendtime;   // local_sort, merge time.
    double COMMstarttime, COMMendtime; // Send, Recv time.
    double COMM_total_time=0;
    double IO_total_time = 0;
    double CPU_total_time =0;
    double Total_time = 0;    
    if (rank == CAL_TIME_RANK) {
        starttime = MPI_Wtime();
    }
    #endif

    //////// LOAD BALANCING & STATE //////////
    int state;
    int data_per_proc = n/size;
    int remainder = n%size; // give to rank 0.
    if(size == 1){//only one process
        state = ONE_PROC;
    }else{
        state = PROC_LESS_THAN_NUMBER; //default
    }
    if(data_per_proc<1){
        data_per_proc = 1; // at least one FLOAT number per proc.
        remainder = 0;
        state = PROC_MORE_THAN_NUMBER;
    }
    
    //////// CRITICAL PARAMETERS //////////
	MPI_Offset offset;
    int LAST_INDEX, LOCAL_DATA_SIZE;
    if(rank==0){
        LAST_INDEX = data_per_proc+remainder-1;      
        LOCAL_DATA_SIZE = data_per_proc+remainder;  
        offset = data_per_proc*sizeof(MPI_FLOAT)*rank;  
    }
    else{
        LAST_INDEX = data_per_proc-1;   
        LOCAL_DATA_SIZE = data_per_proc;  
        offset = (data_per_proc*rank+remainder)*sizeof(MPI_FLOAT);
    }	
    int LAST_RANK = (state == PROC_LESS_THAN_NUMBER)? size-1 : n-1 ;

    //////// ALLOCATE BUFFER //////////
    float* data; // Local data
    float* buf;  // For Recving 
    float* MergeBuf;
    MergeBuf = (float*)malloc(sizeof(MPI_FLOAT)*(LOCAL_DATA_SIZE+data_per_proc));

    //////// NEVER DO THIS!!! //////////
    // float buf[data_per_proc];
    // float data[LOCAL_DATA_SIZE];

    ///////// Read FLOAT from file //////////
    #if defined(WTIME)
    if(rank==CAL_TIME_RANK) IOstarttime = MPI_Wtime();
    #endif
  	MPI_File f; // file handler
	rc =MPI_File_open(MPI_COMM_WORLD, argv[2], MPI_MODE_RDONLY, MPI_INFO_NULL, &f);
	if (rc != MPI_SUCCESS) {
		printf ("Error opening file. Terminating.\n");
		MPI_Abort (MPI_COMM_WORLD, rc);
	} 
	
    switch(state){
        case ONE_PROC:
            data = (float*)malloc(sizeof(MPI_FLOAT)*LOCAL_DATA_SIZE);
            rc = MPI_File_read_at(f, offset , data, LOCAL_DATA_SIZE, MPI_FLOAT, MPI_STATUS_IGNORE);
            if (rc != MPI_SUCCESS) {
                printf ("Error reading file. Terminating.\n");
                MPI_Abort (MPI_COMM_WORLD, rc);
            }         

        break;
        case PROC_LESS_THAN_NUMBER:
            data = (float*)malloc(sizeof(MPI_FLOAT)*LOCAL_DATA_SIZE);
            buf = (float*)malloc(sizeof(MPI_FLOAT)*data_per_proc);
            rc = MPI_File_read_at(f, offset , data, LOCAL_DATA_SIZE, MPI_FLOAT, MPI_STATUS_IGNORE);
            if (rc != MPI_SUCCESS) {
                printf ("Error reading file. Terminating.\n");
                MPI_Abort (MPI_COMM_WORLD, rc);
            }   
            // sort(data, data+LOCAL_DATA_SIZE);
            boost::sort::spreadsort::spreadsort(data, data+LOCAL_DATA_SIZE);
            // boost::sort::spreadsort::float_sort(data, data+LOCAL_DATA_SIZE);
            // boost::sort::spinsort(data, data+LOCAL_DATA_SIZE); 
            // boost::sort::block_indirect_sort(data, data+LOCAL_DATA_SIZE);    
            // bsp::parallel_sort(data, data+LOCAL_DATA_SIZE);
        break;
        case PROC_MORE_THAN_NUMBER:
            if(rank >= n){
                // don't read
                printf("rank %d don't read.\n",rank);
            }
            else{
                data = (float*)malloc(sizeof(MPI_FLOAT)*LOCAL_DATA_SIZE);
                buf = (float*)malloc(sizeof(MPI_FLOAT)*data_per_proc);
                rc = MPI_File_read_at(f, offset , data, LOCAL_DATA_SIZE, MPI_FLOAT, MPI_STATUS_IGNORE);
                if (rc != MPI_SUCCESS) {
                    printf ("Error reading file. Terminating.\n");
                    MPI_Abort (MPI_COMM_WORLD, rc);
                }   
                // sort(data, data+LOCAL_DATA_SIZE);
                boost::sort::spreadsort::spreadsort(data, data+LOCAL_DATA_SIZE);
                // boost::sort::spreadsort::float_sort(data, data+LOCAL_DATA_SIZE);
                //  boost::sort::spinsort(data, data+LOCAL_DATA_SIZE); 
                // boost::sort::block_indirect_sort(data, data+LOCAL_DATA_SIZE); 
                // bsp::parallel_sort(data, data+LOCAL_DATA_SIZE);
            }
        break;
        default:
        break;
    }
    MPI_File_close(&f);

    #if defined(WTIME)
    if(rank==CAL_TIME_RANK){
        IOendtime = MPI_Wtime();
        IO_total_time += (IOendtime - IOstarttime);
    }
    #endif
    
    
    ///////////// Hyperparameters to check whether the sorting is DONE ////////////////
    // int ratio = 1/10;
    // int TIME_TO_ALLREDUCE = n*ratio;
    int TIME_TO_ALLREDUCE = 100; //size
    
    /////////////  Odd - Even sort : Start from Even-phase. ///////////////////
	int count = 0;
    MPI_Status status;
    int local_not_sorted = 0;
    float original_min, original_max;
    

    /////////////  Odd - Even sort : Start from Even-phase. ///////////////////
    switch (state)
    {
    case ONE_PROC:
        boost::sort::spreadsort::spreadsort(data, data+LOCAL_DATA_SIZE);
        // boost::sort::spreadsort::float_sort(data, data+LOCAL_DATA_SIZE);
        // boost::sort::block_indirect_sort(data, data+LOCAL_DATA_SIZE); 
        // boost::sort::spinsort(data, data+LOCAL_DATA_SIZE); 
        // bsp::parallel_sort(data, data+LOCAL_DATA_SIZE);
        // sort(data, data+LOCAL_DATA_SIZE);
    break;
    case PROC_LESS_THAN_NUMBER:   // each proc has 1 or more numbers.
        while(count<=(size)){

                if(count%2==0){///// EVEN PHASE
                    if(size%2!=0 && rank== LAST_RANK){ // The last task AND must be EVEN rank. (e.g. 0,1,2)
                            //do nothing
                            local_not_sorted = 0; // assume sorted.
                    }
                    else{
                        if(rank%2==0){ // EVEN rank task: should recieve.  
                            // Receive data_per_proc FLOAT into buffer 'buf' from rank+1.
                            #if defined(WTIME)
                            if(rank==CAL_TIME_RANK) COMMstarttime = MPI_Wtime();
                            #endif
                            MPI_Recv(buf , data_per_proc , MPI_FLOAT , rank+1 , 0, MPI_COMM_WORLD , &status);
                            #if defined(WTIME)
                            if(rank==CAL_TIME_RANK){
                                COMMendtime= MPI_Wtime();
                                COMM_total_time += COMMendtime-COMMstarttime;
                            } 
                            #endif
                            original_max = data[LAST_INDEX];                   

                            
                            mergeArrays(data, buf, LOCAL_DATA_SIZE, data_per_proc, MergeBuf); 

                            #if defined(WTIME)
                            if(rank==CAL_TIME_RANK) COMMstarttime = MPI_Wtime();
                            #endif
                            MPI_Send( &MergeBuf[LOCAL_DATA_SIZE] , data_per_proc , MPI_FLOAT , rank+1 , 0, MPI_COMM_WORLD);
                            #if defined(WTIME)
                            if(rank==CAL_TIME_RANK){
                                COMMendtime= MPI_Wtime();
                                COMM_total_time += COMMendtime-COMMstarttime;
                            }     
                            #endif                   

                            if(original_max!=data[LAST_INDEX]){
                                // SWAP happened.
                                local_not_sorted = 1; // not sorted. 
                            }                            

                        }
                        else{          // ODD  rank task: should send to left(lower )
                            
                            original_min = data[0];
                            // Send all data!
                            #if defined(WTIME)
                            if(rank==CAL_TIME_RANK) COMMstarttime = MPI_Wtime();
                            #endif
                            MPI_Send( data, data_per_proc , MPI_FLOAT , rank-1 , 0, MPI_COMM_WORLD);        
                            MPI_Recv( data , data_per_proc , MPI_FLOAT , rank-1 , 0, MPI_COMM_WORLD , &status);
                            #if defined(WTIME)
                            if(rank==CAL_TIME_RANK){
                                COMMendtime= MPI_Wtime();
                                COMM_total_time += COMMendtime-COMMstarttime;
                            } 
                            #endif
                            if(original_min!=data[0]){
                                // SWAP happened.
                                local_not_sorted = 1; // not sorted. 
                            }                              
                        }
                    }
                }
                else{	///// ODD PHASE
                    if(rank != 0){
                        // boundary condition :
                        if(size%2==0 && rank== LAST_RANK){ // The last task AND must be ODD rank. (e.g. 0,[1,2],3)
                            // do nothing.
                            local_not_sorted = 0; //assume sorted.
                        }
                        else{ // Odd number of tasks, or IS NOT the LAST_RANK
                            if(rank%2!=0){// ODD rank:  should RECV then SEND
                                // Receive data_per_proc FLOAT into buffer 'buf' from rank+1.
                                #if defined(WTIME)
                                if(rank==CAL_TIME_RANK) COMMstarttime = MPI_Wtime();
                                #endif
                                MPI_Recv(buf , data_per_proc , MPI_FLOAT , rank+1 , 0, MPI_COMM_WORLD , &status);
                                #if defined(WTIME)
                                if(rank==CAL_TIME_RANK){
                                    COMMendtime= MPI_Wtime();
                                    COMM_total_time += COMMendtime-COMMstarttime;
                                }    
                                #endif                              
                                original_max = data[LAST_INDEX];                   
                                
                                mergeArrays(data, buf, LOCAL_DATA_SIZE, data_per_proc, MergeBuf); 
                                
                                #if defined(WTIME)
                                if(rank==CAL_TIME_RANK) COMMstarttime = MPI_Wtime();
                                #endif
                                MPI_Send( &MergeBuf[LOCAL_DATA_SIZE] , data_per_proc , MPI_FLOAT , rank+1 , 0, MPI_COMM_WORLD);
                                #if defined(WTIME)
                                if(rank==CAL_TIME_RANK){
                                    COMMendtime= MPI_Wtime();
                                    COMM_total_time += COMMendtime-COMMstarttime;
                                }                                  
                                #endif
                                if(original_max!=data[LAST_INDEX]){
                                    // SWAP happened.
                                    local_not_sorted = 1; // not sorted. 
                                } 
                            }
                            else{         // EVEN rank: should SEND then RECV
                                original_min = data[0];
                               
                                #if defined(WTIME)
                                if(rank==CAL_TIME_RANK) COMMstarttime = MPI_Wtime();
                                #endif                           
                                MPI_Send( data, data_per_proc , MPI_FLOAT , rank-1 , 0, MPI_COMM_WORLD);        
                                MPI_Recv( data , data_per_proc , MPI_FLOAT , rank-1 , 0, MPI_COMM_WORLD , &status);
                                #if defined(WTIME)
                                if(rank==CAL_TIME_RANK){
                                    COMMendtime= MPI_Wtime();
                                    COMM_total_time += COMMendtime-COMMstarttime;
                                } 
                                #endif

                                if(original_min!=data[0]){
                                    // SWAP happened.
                                    local_not_sorted = 1; // not sorted. 
                                }   
                            }

                        }
                    }
                    else{ // rank 0 : do nothing.
                        local_not_sorted = 0; //assume sorted.
                         // do nothing.
                        //////// TODO : how rank 0 knows others already swapped?
                    }
                }            


            // if( n>100000 && count%TIME_TO_ALLREDUCE==0){
            //     // float global_sum;
            //     int global_sum_not_sorted;                
            //     MPI_Allreduce(&local_not_sorted, &global_sum_not_sorted, 1, MPI_FLOAT, MPI_SUM, MPI_COMM_WORLD);
            //     if(global_sum_not_sorted == 0 && count>=1){
            //         if(rank==0){
            //             printf("allreduce: break at iteration %d\n",count);
            //         }
            //         break;
            //     }                
            // }            
        
            count++;
        }                  
    break;
    case PROC_MORE_THAN_NUMBER: // send only one number.
        while(count<=(size)){
            if(rank>=n){
                // For 03.in : rank 21~27
                // do nothing.
                break;
                local_not_sorted = 0; // assume sorted.
            }
            else{

                if(count%2==0){///// EVEN PHASE
                    if( n%2!=0 && rank== LAST_RANK){ // The last task AND must be ODD rank. (e.g. 0,[1,2],3)
                            //do nothing
                            local_not_sorted = 0; // assume sorted.
                    }
                    else{
                        if(rank%2==0){ // EVEN rank task: should recieve.  
                            // Receive data_per_proc FLOAT into buffer 'buf' from rank+1.
                            MPI_Recv(buf , data_per_proc , MPI_FLOAT , rank+1 , 0, MPI_COMM_WORLD , &status);
                           
                            original_max = data[LAST_INDEX];                   

                           
                            mergeArrays(data,buf,LOCAL_DATA_SIZE,data_per_proc,MergeBuf);
                            

                            MPI_Send(&MergeBuf[LOCAL_DATA_SIZE] , data_per_proc , MPI_FLOAT , rank+1 , 0, MPI_COMM_WORLD);
                            if(original_max!=data[LAST_INDEX]){
                                // SWAP happened.
                                local_not_sorted = 1; // not sorted. 
                            }                                
                           


                        }
                        else{          // ODD  rank task: should send to left(lower )
                           
                            original_min = data[0];                         
                            // Send all data!
                            MPI_Send( &data[0], data_per_proc , MPI_FLOAT , rank-1 , 0, MPI_COMM_WORLD);        
                        
                            MPI_Recv(&data[0] , data_per_proc , MPI_FLOAT , rank-1 , 0, MPI_COMM_WORLD , &status);
                          
                            if(original_min!=data[0]){
                                // SWAP happened.
                                local_not_sorted = 1; // not sorted. 
                            }    


                        }
                    }
                }
                else{	///// ODD PHASE
                    if(rank != 0){
                        // boundary condition :
                        if(rank== LAST_RANK && n%2==0 ){
                            // do nothing.
                            local_not_sorted = 0; // assume sorted. 
                        }
                        else{ // Odd number of tasks, or IS NOT the LAST_RANK
                            if(rank%2!=0){// ODD rank:  should RECV then SEND
                                // Receive data_per_proc FLOAT into buffer 'buf' from rank+1.
                                MPI_Recv(buf , data_per_proc , MPI_FLOAT , rank+1 , 0, MPI_COMM_WORLD , &status);
                               
                                original_max = data[LAST_INDEX]; 

                                mergeArrays(data,buf,LOCAL_DATA_SIZE,data_per_proc,MergeBuf);
                                
                                // send backward half size of data.
                                MPI_Send(&MergeBuf[LOCAL_DATA_SIZE] , data_per_proc , MPI_FLOAT , rank+1 , 0, MPI_COMM_WORLD);
                           
                                if(original_max!=data[LAST_INDEX]){
                                    // SWAP happened.
                                    local_not_sorted = 1; // not sorted. 
                                } 

                            }
                            else{         // EVEN rank: should SEND then RECV
                                // Send all data!
                                original_min = data[0];  
                                MPI_Send( data, data_per_proc , MPI_FLOAT , rank-1 , 0, MPI_COMM_WORLD);        
                                MPI_Recv( data , data_per_proc , MPI_FLOAT , rank-1 , 0, MPI_COMM_WORLD , &status);
                                if(original_min!=data[0]){
                                    // SWAP happened.
                                    local_not_sorted = 1; // not sorted. 
                                }

                          }

                        }
                    }
                    else{ // rank 0 : do nothing.
                        local_not_sorted = 0; // assume sorted.
                         // do nothing.
                        //////// TODO : how rank 0 knows others already swapped?
                    }
                }            
            }
         
            // if(n>100000 && count%TIME_TO_ALLREDUCE==0){
            //     // float global_sum;
            //     int global_sum_not_sorted;                
            //     MPI_Allreduce(&local_not_sorted, &global_sum_not_sorted, 1, MPI_FLOAT, MPI_SUM, MPI_COMM_WORLD);
            //     if(global_sum_not_sorted == 0 && count>=1){
            //         break;
            //     }                
            // }            
         
            count++;
        }  

    break;        
    default:
    break;
    }


    
    /////////////  WRITE OUTPUT FILE /////////////////// 
	#if defined(WTIME)
    if(rank==CAL_TIME_RANK) IOstarttime = MPI_Wtime();
    #endif

    MPI_File f2;
	rc = MPI_File_open(MPI_COMM_WORLD, argv[3], MPI_MODE_CREATE | MPI_MODE_WRONLY, MPI_INFO_NULL, &f2);
	if (rc != MPI_SUCCESS) {
		printf ("Error opening file. Terminating.\n");
		MPI_Abort (MPI_COMM_WORLD, rc);
	}     

    switch(state){
        case ONE_PROC:
            rc = MPI_File_write_at(f2, offset , data, LOCAL_DATA_SIZE, MPI_FLOAT, MPI_STATUS_IGNORE);         
        break;
        case PROC_LESS_THAN_NUMBER:
            rc = MPI_File_write_at(f2, offset , data, LOCAL_DATA_SIZE, MPI_FLOAT, MPI_STATUS_IGNORE);
        break;
        case PROC_MORE_THAN_NUMBER:
            if(rank>=n){
                printf("rank %d doesn't write file.\n",rank);
            }else{
                rc = MPI_File_write_at(f2, offset , data, LOCAL_DATA_SIZE, MPI_FLOAT, MPI_STATUS_IGNORE);
            }
        break;
        default:
        break;
    }
    if (rc != MPI_SUCCESS) {
		printf ("Error writing file. Terminating.\n");
		MPI_Abort (MPI_COMM_WORLD, rc);
	} 	
    MPI_File_close(&f2);

    #if defined(WTIME)
    if(rank==CAL_TIME_RANK){
        IOendtime = MPI_Wtime();
        IO_total_time += (IOendtime - IOstarttime);
    }
    #endif

    ///////////// TIME STOP ///////////////
    #if defined(WTIME)
    if(rank==CAL_TIME_RANK){
        endtime = MPI_Wtime();
        Total_time = endtime - starttime;
        // printf("Took %.5f seconds.\n", endtime - starttime);
        // // printf("break at iteration %d\n",count);

        // printf("CPU time: %.5f sec\n", Total_time - IO_total_time - COMM_total_time);
        // printf("IO time: %.5f sec\n", IO_total_time);
        // printf("Communication time: %.5f sec\n", COMM_total_time);
    }
    #endif
 
    MPI_Finalize();
}


void mergeArrays(float* arr1, float arr2[], int n1, int n2, float arr3[]) 
{ 
    int i = 0, j = 0, k = 0; 
  
    // Traverse both array 
    while (i<n1 && j <n2) 
    { 
        // Check if current element of first array is smaller than current element of second array. 
        // If yes, store first array element and increment first array index.
        // Otherwise do same with second array 
        if (arr1[i] < arr2[j]) 
            arr3[k++] = arr1[i++]; 
        else
            arr3[k++] = arr2[j++]; 
    } 
  
    // Store remaining elements of first array 
    while (i < n1) 
        arr3[k++] = arr1[i++]; 
  
    // Store remaining elements of second array 
    while (j < n2) 
        arr3[k++] = arr2[j++]; 

    for(int l=0; l<n1; l++){
        arr1[l] = arr3[l];
        // printf("%.1f ",arr1[l]);
    }
    // printf("\n");
}