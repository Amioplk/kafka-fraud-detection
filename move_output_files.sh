date="2021-07-08--16"
prefix_path="flink-project/fraudulent-click-detector/outputs"

file=`ls -a $prefix_path/all/$date/ | grep ".part"`
copy_file=$prefix_path/all/${date}/$file
cp $copy_file ./initial_stream.txt

file=`ls -a $prefix_path/snapshots_pattern_1/$date/ | grep ".part"`
copy_file=$prefix_path/snapshots_pattern_1/${date}/$file
cp $copy_file ./pattern_1.txt

file=`ls -a $prefix_path/snapshots_pattern_2/$date/ | grep ".part"`
copy_file=$prefix_path/snapshots_pattern_2/${date}/$file
cp $copy_file ./pattern_2.txt

file=`ls -a $prefix_path/snapshots_pattern_3/$date/ | grep ".part"`
copy_file=$prefix_path/snapshots_pattern_3/${date}/$file
cp $copy_file ./pattern_3.txt
