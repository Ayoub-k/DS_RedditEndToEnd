-- Create the Comments dimension table
CREATE TABLE Comments (
    comment_id VARCHAR(255) NOT NULL PRIMARY KEY,
    body VARCHAR(max),
    author VARCHAR(255),
    time_comment_id VARCHAR(50),
    created_utc_cmt TIMESTAMP
);

-- Create the Posts dimension table
CREATE TABLE Posts (
    post_id VARCHAR(255) NOT NULL PRIMARY KEY,
    title VARCHAR(max),
    author VARCHAR(255),
    subreddit VARCHAR(255),
    url VARCHAR(255),
    over_18 BOOLEAN,
    name VARCHAR(255),
    author_premium BOOLEAN,
    flair VARCHAR(255),
    time_post_id VARCHAR(50),
    created_utc TIMESTAMP
);

-- Create the Fact table
CREATE TABLE Fact (
    fact_id BIGSERIAL PRIMARY KEY,
    comment_id VARCHAR(255),
    post_id VARCHAR(255),
    score INT,
    score_cmt INT,
    upvote_ratio FLOAT,
    num_comments INT,
    subscribers INT,
    ups INT,
    FOREIGN KEY (comment_id) REFERENCES Comments(comment_id),
    FOREIGN KEY (post_id) REFERENCES Posts(post_id)
);

-- Create indexes on foreign key columns for improved performance
CREATE INDEX idx_Fact_comment_id ON Fact (comment_id);
CREATE INDEX idx_Fact_post_id ON Fact (post_id);
