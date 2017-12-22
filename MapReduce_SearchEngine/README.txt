Steps for execution.

1. Place the input files in the hdfs.

2. Download the jar files submitted in the assignment.

3. To execute DocWordCount use the following command

hadoop jar <path/DocWordCount.jar> org/myorg/DocWordCount <Input files path> <Output Path>

*******************************************************************************************
4. To execute Term frequency use the following command

hadoop jar <path/TermFrequency.jar> org/myorg/TermFrequency <Input files path> <Output Path>

*********************************************************************************************
5. To execute TFIDF use the following command

hadoop jar <path/TFIDF.jar> org/myorg/TFIDF <Input files path> <Output Path>

The temporary output is stored in TFIDF/TEMPOUTPUT

************************************************************************************************
6. To execute Search use the following commands

hadoop jar <path/SEARCH.jar> org/myorg/SEARCH <Input files path> <TFIDF OUTPUT FILE PATH> "computer science"

hadoop jar <path/SEARCH.jar> org/myorg/SEARCH <TFIDF output file path> <Output Path> "data analysis"

************************************************************************************************************
