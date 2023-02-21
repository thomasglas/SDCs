#! /usr/bin/bash
qd_min_block_sizes=(1000 5000 10000 50000 100000 250000 500000)

cd build
results_file='results.txt'
rm $results_file

for block_size in "${qd_min_block_sizes[@]}"
do
    echo "Block size $block_size" | tee -a $results_file 
    execution_times=$( ./sdcs $block_size )
    echo $execution_times | tee -a $results_file 
done