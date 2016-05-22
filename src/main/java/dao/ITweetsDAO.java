package dao;

import org.bson.Document;

/**
 * Created by Felipe on 22/05/16.
 */
public interface ITweetsDAO
{
    void saveTweetInfo(Document tweetInfo);

    void dropCollection();

    void closeMongo();
}
