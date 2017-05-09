# twitterEventDetection

CREATE TABLE tweetcollection.processed2 (
    round bigint,
    boltid int,
    finished boolean,
    PRIMARY KEY (round, boltid)
);

CREATE TABLE tweetcollection.events2 (
    round bigint,
    country text,
    word text,
    incrementpercent double,
    PRIMARY KEY (round, country, word)
);

CREATE TABLE tweetcollection.counts2 (
    round bigint,
    word text,
    country text,
    count bigint,
    totalnumofwords bigint,
    PRIMARY KEY (round, word, country)
);