# twitterEventDetection


CREATE TABLE tweetcollection.events (
    round bigint,
    country text,
    word text,
    incrementpercent double,
    PRIMARY KEY (round, country, word)
);

CREATE TABLE tweetcollection.counts (
    round bigint,
    word text,
    country text,
    count bigint,
    totalnumofwords bigint,
    PRIMARY KEY (round, word, country)
);


CREATE TABLE tweetcollection.processtimes2 (
    row int,
    column int,
    id int,
    PRIMARY KEY (row, column)
);

TRUNCATE events ;TRUNCATE counts ;TRUNCATE processtimes2;


mvn clean compile assembly:single

./storm jar /Users/ozlemcerensahin/Desktop/twitterEventDetection/target/storm-twitter-1.0-SNAPSHOT-jar-with-dependencies.jar eventDetector.topologies.EventDetectionWithCassandraTopology



