-- Create the Posts table
CREATE TABLE Posts (
    post_id VARCHAR(255) NOT NULL,
    title text,
    author VARCHAR(255),
    subreddit VARCHAR(255),
    score INT,
    upvote_ratio FLOAT,
    num_comments INT,
    created_utc TIMESTAMP,
    url VARCHAR(255),
    subscribers INT,
    over_18 BOOLEAN,
    name VARCHAR(255),
    ups INT,
    author_premium BOOLEAN,
    flair VARCHAR(255),
    PRIMARY KEY (post_id)
);

-- Create the Comments table
CREATE TABLE Comments (
    comment_id VARCHAR(255) NOT NULL,
    post_id VARCHAR(255),
    body text,
    author VARCHAR(255),
    score INT,
    created_utc TIMESTAMP,
    FOREIGN KEY (post_id) REFERENCES Posts(post_id),
    PRIMARY KEY (comment_id)
);
