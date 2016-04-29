package model;

import twitter4j.UserMentionEntity;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Felipe on 4/16/16.
 */
public class TweetInfo implements Serializable
{
    private Long _id;
    private String _tweetText;
    private String _sentiment;
    private ConcurrentHashMap<String, String> _tagsMap;
    private UserMentionEntity[] _usersMention;

    public TweetInfo()
    {
    }

    public Long getId()
    {
        return _id;
    }

    public void setId(Long id)
    {
        _id = id;
    }

    public String getTweetText()
    {
        return _tweetText;
    }

    public void setTweetText(String tweet)
    {
        _tweetText = tweet;
    }

    public ConcurrentHashMap<String, String> getTagsMap()
    {
        return _tagsMap;
    }

    public void setTagsMap(ConcurrentHashMap<String, String> tagsMap)
    {
        _tagsMap = tagsMap;
    }

    public String getSentiment()
    {
        return _sentiment;
    }

    public void setSentiment(String sentiment)
    {
        _sentiment = sentiment;
    }

    public UserMentionEntity[] getUsersMention()
    {
        return _usersMention;
    }

    public void setUsersMention(UserMentionEntity[] usersMention)
    {
        _usersMention = usersMention;
    }
}
