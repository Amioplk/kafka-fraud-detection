## How to run our project ?

1. clone this repo locally
2. From your terminal, execute `docker-compose rm -f; docker-compose up` to start the streaming data of advertising events
3. Run kafka-fraud-detection/flink-project/fraudulent-click-detector/src/main/java/flinkiasd/StreamingJob.java
4. Change variable "date" to current date and hour in move_output_files.sh script
5. Run move_output_files.sh to get the output of the initial_stream and our 3 patterns in manageable files
6. Run streaming_data_report.ipynb to get the click through rate after dropping the clicks identified by the patterns

Note that the quickstart directory is just an archive of our tests on scala. The real work is in "flink-project" directory.
