import os

mrRuns = 5
count = 0

for i in range(mrRuns):
	currentCentroids = 'health_centroids'+str(count)+'.txt'
	os.system('hdfs dfs -copyToLocal ./HW4/Medicare/health_centroids.txt ' + currentCentroids)

	# Run kmeans map-reduce job
	os.system('hadoop jar health_kmeans.jar ./HW4/Medicare/Norm/ ./HW4/Medicare/kmeansOUT')

	os.system('hdfs dfs -rm ./HW4/Medicare/health_centroids.txt')
	os.system('hdfs dfs -getmerge ./HW4/Medicare/kmeansOUT newHealthCentroids.txt')
	os.system('hdfs dfs -copyFromLocal newHealthCentroids.txt ./HW4/Medicare/health_centroids.txt')
	os.system('hdfs dfs -rm -r ./HW4/Medicare/kmeansOUT')

	count += 1


