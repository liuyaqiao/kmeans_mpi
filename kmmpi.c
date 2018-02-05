#include<stdio.h>
#include<stdlib.h>
#include<math.h>
#include<float.h>
#include "mpi.h"
#include<time.h>

#define MASTER 0

//Point *point;
typedef struct
{
    int x;
    int y;
    int z;
}Point;

Point *data;

double CLOCK() {
struct timespec t;
clock_gettime(CLOCK_MONOTONIC,  &t);
return (t.tv_sec * 1000)+(t.tv_nsec*1e-6);
}

void readHeader(FILE *input, int* num_clusters, int* num_points)
{
    printf("haha\n");
    fscanf(input,"%ld\n",num_clusters);
    //printf("%d\n",*num_clusters);
    fscanf(input,"%ld\n",num_points);
    //printf("%d\n",*num_points);
}

void readPoints(FILE *input, Point *point, int num_points)
{
    int i;
    for(i=0;i<num_points;i++)
    {
        fscanf(input,"%d,%d,%d\n",&point[i].x,&point[i].y,&point[i].z);
        //printf("%d %d %d\n",point[i].x,point[i].y,point[i].z);
    }
}

void initialize(int num_cluster, Point *mean, Point *point)
{
    int i;
    for(i=0;i<num_cluster;i++)
    {
    mean[i].x = point[i].x;
    mean[i].y = point[i].y;
    mean[i].z = point[i].z;
    }
}


double getDistance(Point point1, Point point2)
{
    double d;
    d = sqrt((point1.x - point2.x) * (point1.x - point2.x) + (point1.y - point2.y) * (point1.y - point2.y)+(point1.z - point2.z) * (point1.z - point2.z));
    return d;
}

int findIndex(Point point,Point * centroids, int num_centroids)
{
    int i,index = 0;
    double distance = 0;
    double minDistance = getDistance(point,centroids[0]);
    for(i=0;i<num_centroids;i++)
    {
    distance = getDistance(point,centroids[i]);
    if(minDistance >= distance)
        {
        index = i;
        minDistance = distance;
        }
    }
    return index;
}

void getCenter(int num_clusters, int num_points,int *center, Point *point, Point *centroids)
{
    Point tep;
    int i,j;
    int count = 0;
    for(i=0;i<num_clusters;++i)
    {
    count = 0;
    tep.x = 0.0;
    tep.y = 0.0;
    tep.z = 0.0;
        for(j = 0;j<num_points;++j)
        {
            if(i == center[j])
            {
            count++;
            tep.x += point[j].x;
            tep.y += point[j].y;
            tep.z += point[j].z;
            }
        }
	tep.x /= count;
	tep.y /= count;
	tep.z /= count;
	centroids[i] = tep;
    }
}

double getE(int num_clusters,int num_points,int *center,Point *point,Point *centroids)
{
    int i,j;
    double cnt = 0.0,sum = 0.0;
    for(i = 0;i<num_clusters;++i)
    {
        for(j=0;j<num_points;++j)
        {
            if(i == center[j])
            {
                cnt = (point[j].x - centroids[i].x) * (point[j].x - centroids[i].x) + (point[j].y - centroids[i].y) * (point[j].y - centroids[i].y)+(point[j].z - centroids[i].z)*(point[j].z - centroids[i].z);
                sum = sum +cnt;
            }
        }
    }
    return sum;
}

int checkConvergence(int *former_cluster, int * latter_cluster, int num_points)
{
    int i;
    for(i=0;i<num_points;i++)
    {
        if(former_cluster[i]!=latter_cluster[i])
		return -1;
    }
    return 0;
}

int main(int argc, char *argv[])
{
/////////////////////////////////
//declaration
    int num_clusters = 20;
    int num_points = 1000000;
    int i,n=0;
    int rank;
    int size;
    int job_size;
    int job_done = 0;
    int center[1000000];
    for(i=0;i<num_points;i++)
        center[i]=0;
    double start,end;
    Point *centroids;  //center point
    Point *received_points;
//job_size = 50;
    int *slave_clusters;
    int *former_clusters;
    int *latter_clusters;

    FILE *fp;
    fp = fopen("input1000.txt","r");
    data = (Point*)malloc(sizeof(Point)*num_points);
    readPoints(fp, data, num_points);
//printf("-----------------------------------\n");
//if(size == 1 || size > num_points || num_points % (size-1))
////MPI_Abort(MPI_COMM_WORLD,1);
    fclose(fp);
    start = CLOCK();
    MPI_Init(&argc,&argv);
    MPI_Status status;
    MPI_Comm_rank(MPI_COMM_WORLD,&rank);
    MPI_Comm_size(MPI_COMM_WORLD,&size);

//new MPI structure
    MPI_Datatype MPI_POINT;
    MPI_Datatype type=MPI_DOUBLE;
    int blocklen=3;
    MPI_Aint disp=0;
    MPI_Type_create_struct(1,&blocklen,&disp,&type,&MPI_POINT);
    MPI_Type_commit(&MPI_POINT);

///////////////////////
    if(rank == 0)
    {
//printf("process %d sending\n",rank);
//input data into process 0

    former_clusters = (int *)malloc(sizeof(int)*num_points);
    latter_clusters=(int*)malloc(sizeof(int)*num_points);
    job_size=num_points/(size-1);
    centroids =(Point *) malloc(sizeof(Point)*num_clusters);
    initialize(num_clusters,centroids,data);
/*
for(i=0;i<num_clusters;i++)
{
printf("x = %d  ",centroids[i].x);
printf("y = %d  ",centroids[i].y);
printf("z = %d  ",centroids[i].z);
printf("\n");
*/
//}

/////////////////////////传送接收了中心点和数据////////////////////////////

//printf("process %d sending\n",rank);
//printf("------------------------------------\n");

    for(i=1;i<size;i++)
    {
//	printf("Sending to [%d]\n",i);
	MPI_Send(&job_size              ,1           , MPI_INT        ,i,0,MPI_COMM_WORLD);
	MPI_Send(&num_clusters          ,1           , MPI_INT        ,i,0,MPI_COMM_WORLD);
	MPI_Send(centroids,num_clusters, MPI_POINT,i,0,MPI_COMM_WORLD);
//	MPI_Send(point+(i-1)*job_size,job_size    , MPI_POINT      ,i,0,MPI_COMM_WORLD);
    }
//printf("Sent\n");

    MPI_Barrier(MPI_COMM_WORLD);
/////////////////////////////////////////////////////////////////////////
//master process work
    while(1)
    {
    n++;
    MPI_Barrier(MPI_COMM_WORLD);
//printf("Master Receiving\n");

    for(i=1;i<size;i++)
        MPI_Recv(latter_clusters+(job_size*(i-1)),job_size,MPI_INT,i,0,MPI_COMM_WORLD,&status);
//printf("Master Received!\n");
    getCenter(num_clusters, num_points,latter_clusters, data, centroids);
//printf("New Centroids done\n");
    if(checkConvergence(former_clusters, latter_clusters, num_points) == 0)
    {
//printf("Converged\n");
    job_done = 1;
    }
    else{
//printf("Not convergerd!\n");
        for(i=0;i<num_points;i++)
        former_clusters[i] = latter_clusters[i];
    }
//inform job_done to slave process
    for(i=1;i<size;i++)
        MPI_Send(&job_done,1,MPI_INT,i,0,MPI_COMM_WORLD);

    MPI_Barrier(MPI_COMM_WORLD);
    if(job_done == 1) break;

    for(i=1;i<size;i++)
        MPI_Send(centroids,num_clusters,MPI_POINT,i,0,MPI_COMM_WORLD);

    MPI_Barrier(MPI_COMM_WORLD);

//for(i=0;i<num_clusters;i++)
//        printf("[%d] times center: x = %d,y = %d\n",n,centroids[i].x,centroids[i].y);

}
//输出文件
    FILE * output = fopen("output.txt","w");
    for(i=0;i<num_points;i++)
        fprintf(output,"%d,%d,%d,%d\n",data[i].x,data[i].y,data[i].z,latter_clusters[i]+1);
    fclose(output);
}

//slave process
    else{
//printf("process [%d]  Receiving\n",rank);

    MPI_Recv(&job_size    ,1           ,MPI_INT  ,MASTER,0,MPI_COMM_WORLD,&status);
    MPI_Recv(&num_clusters,1           ,MPI_INT  ,MASTER,0,MPI_COMM_WORLD,&status);
//printf("[%d] for %d\n",job_size,rank);

    centroids = (Point *)malloc(sizeof(Point)*num_clusters);
    MPI_Recv(centroids,num_clusters,MPI_POINT,MASTER,0,MPI_COMM_WORLD,&status);

//printf("process [%d] finishing sending\n",rank);


    received_points=(Point *)malloc(sizeof(Point)*job_size);

    slave_clusters=(int*)malloc(sizeof(int)*job_size);
//MPI_Recv(received_points,job_size,MPI_POINT,MASTER,0,MPI_COMM_WORLD,&status);
//不用recv来接受 使用全局变量
    for(i=0;i<job_size;i++)
        received_points[i] = data[(rank-1)*job_size+i];

//printf("Received [%d]\n",rank);

    MPI_Barrier(MPI_COMM_WORLD);
    while(1)
    {
//printf("Calculate the new cluster [%d]\n",rank);
    for(i=0;i<job_size;i++)
        slave_clusters[i] = findIndex(received_points[i],centroids,num_clusters);
//printf("Sending to master[%d]\n",rank);
    MPI_Send(slave_clusters,job_size,MPI_INT,MASTER,0,MPI_COMM_WORLD);
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Recv(&job_done,1,MPI_INT,MASTER,0,MPI_COMM_WORLD,&status);
    if(job_done == 1) break;  //end

    MPI_Recv(centroids,num_clusters,MPI_POINT,MASTER,0,MPI_COMM_WORLD,&status);
    MPI_Barrier(MPI_COMM_WORLD);
}
}
//end = CLOCK();
    MPI_Finalize();
    end = CLOCK();
    printf("parallel time = %lf\n",end-start);
    return 0;
}


