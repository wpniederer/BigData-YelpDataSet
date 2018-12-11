# BigData-YelpDataSet

## Instructions
1. Change the data from JSON to CSV using python and pandas, setting the delimiter to | since commas are a character in the input
2. Upload to XSEDE bridges using Globus
3. Start Hadoop using following commands:
        interact -N 4 -t 01:00:00
        module load hadoop
        start-hadoop.sh
3. Compile the java file into a jar and execute:
        hadoop com.sun.tools.javac.Main NegativeReviewWordCount.java
        jar cf ReviewTest.jar *.class
        cd ..
        hadoop jar src/ReviewTest.jar NegativeReviewWordCount in/yelp_dataset.csv output

## Execution time
- Total time spent by all map tasks: 7770.375 seconds
- Total time spent by all reduce tasks: 636.246 seconds
- GC time elapsed: 7.680 seconds
- CPU time spent: 330.710 seconds

## Link to dataset
https://www.yelp.com/dataset/challenge
