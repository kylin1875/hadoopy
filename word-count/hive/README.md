$ hive
hive> CREATE TABLE word_count_input (line STRING);
hive> LOAD DATA LOCAL INPATH 'word-count-in/' INTO TABLE word_count_input;
hive> SELECT word, COUNT(*) FROM word_count_input LATERAL VIEW explode(split(line, ' ')) lTable as word GROUP BY word;
hive> DROP TABLE word_count_input;
