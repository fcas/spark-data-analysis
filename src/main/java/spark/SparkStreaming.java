package spark;

import dao.TweetsDAO;
import helpers.KeyWords;
import helpers.TemporaryFilesMaker;
import helpers.TwitterKeysReader;
import model.Sentiments;
import model.TweetInfo;
import opennlp.tools.cmdline.postag.POSModelLoader;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSSample;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.bson.Document;
import twitter4j.Status;
import twitter4j.UserMentionEntity;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Created by Felipe on 25/04/16.
 */
public class SparkStreaming
{
    private static List<String> _symbols;
    private static List<String> _stopwords;
    private static List<String> _positiveEmoticons;
    private static List<String> _negativeEmoticons;
    private static List<String> _tokenList;
    private static ConcurrentHashMap<String, String> _abbreviationsMap;
    private static ConcurrentHashMap<String, Integer> _sentimentMap;
    private static Tokenizer _tokenizer;
    private static POSTaggerME _tagger;
    private static TweetsDAO _tweetsDAO;

    public static void main(String[] args) {

        authentication();

        SparkConf conf = new SparkConf().setMaster("spark://Felipes-MacBook-Air.local:7077").setAppName("SparkStreamingAnalysis");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, new Duration(30000));
        JavaReceiverInputDStream<Status> twitterDstream = TwitterUtils.createStream(javaStreamingContext, KeyWords.names());

        twitterDstream.map(status -> processTweetStatus(status)).foreachRDD((Function<JavaRDD<TweetInfo>, Void>) documentJavaRDD ->
        {
            if(!documentJavaRDD.isEmpty())
            {
                save(documentJavaRDD.collect());
            }
            return null;
        });

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }

    private static void authentication()
    {
        Properties properties = TwitterKeysReader.getTwitterKeys();
        if (properties != null)
        {
            System.setProperty("twitter4j.oauth.consumerKey", properties.get("consumerKey").toString());
            System.setProperty("twitter4j.oauth.consumerSecret", properties.get("consumerSecret").toString());
            System.setProperty("twitter4j.oauth.accessToken", properties.get("accessToken").toString());
            System.setProperty("twitter4j.oauth.accessTokenSecret", properties.get("accessTokenSecret").toString());
        }
    }

    private static void initialization()
    {
        String sentiLexFilename = "sentilex.txt";
        String positiveEmoticonsFilename = "positive_emoticons.txt";
        String negativeEmoticonsFilename = "negative_emoticons.txt";
        String symbolsFilename = "symbols.txt";
        String abbreviationsFileName = "abbreviations.txt";
        String stopwordsFilename = "stopwords.txt";
        String _tokenModelFileName = "pt-token.bin";

        File maxentModel = TemporaryFilesMaker.getFile("pt-pos-maxent.bin", ".bin");

        _sentimentMap = new ConcurrentHashMap<>();
        POSModel _taggerModel = new POSModelLoader().load(maxentModel);
        _tagger = new POSTaggerME(_taggerModel);
        InputStream inputStream = SparkStreaming.class.getClassLoader().getResourceAsStream(_tokenModelFileName);

        try
        {
            List<String> abbreviationsFileLines = readLines(abbreviationsFileName);
            _abbreviationsMap = linesToAbbreviationsMap(abbreviationsFileLines);
            _stopwords = readLines(stopwordsFilename);
            _positiveEmoticons = readLines(positiveEmoticonsFilename);
            _negativeEmoticons = readLines(negativeEmoticonsFilename);
            List<String> sentiLexFileLines = readLines(sentiLexFilename);
            linesToSentimentsMap(sentiLexFileLines);
            _symbols = readLines(symbolsFilename);

            if(inputStream != null)
            {
                TokenizerModel _tokenizerModel = new TokenizerModel(inputStream);
                _tokenizer = new TokenizerME(_tokenizerModel);
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private static synchronized TweetInfo processTweetStatus(Status status)
    {
        if(_symbols == null || _stopwords == null || _positiveEmoticons == null || _negativeEmoticons == null || _tokenList == null)
        {
            initialization();
        }

        String tweetText = status.getText().toLowerCase();
        String modifiedTweetText = replaceMentions(status.getUserMentionEntities(), tweetText);
        modifiedTweetText = replaceUrls(modifiedTweetText);

        if (status.isRetweet())
        {
            modifiedTweetText = modifiedTweetText.split(":")[1];
        }

        modifiedTweetText = replaceSymbols(modifiedTweetText);

        String tokens[] = _tokenizer.tokenize(modifiedTweetText);
        _tokenList = new ArrayList<>(Arrays.asList(tokens));

        replaceAbbreviations();
        removeStopWords();

        String[] tags = _tagger.tag(tokens);
        POSSample sample = new POSSample(tokens, tags);

        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
        for (int i = 0; i < sample.getSentence().length; i++)
        {
            map.put(sample.getSentence()[i], sample.getTags()[i]);
        }

        TweetInfo tweetStream = new TweetInfo();
        tweetStream.setId(status.getId());
        tweetStream.setTweetText(tweetText);
        tweetStream.setUsersMention(status.getUserMentionEntities());
        tweetStream.setTagsMap(map);
        tweetStream = analyzeSentiment(tweetStream);

        return tweetStream;
    }

    private static List<String> readLines(String filename){
        List<String> result = new ArrayList<>();
        try
        {
            result = IOUtils.readLines(SparkStreaming.class.getResourceAsStream(filename), "UTF-8");
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        return result;
    }

    private static void save(List<TweetInfo> tweetStreams)
    {
        for (TweetInfo tweetStream : tweetStreams)
        {
            Document document = new Document();
            document.put("_id", tweetStream.getId());
            document.put("_sentiment", tweetStream.getSentiment());

            List<String> _usersMention = new ArrayList<>();

            for (UserMentionEntity userMention : tweetStream.getUsersMention())
            {
                _usersMention.add(userMention.getScreenName());
            }

            document.put("_userMentions", _usersMention);

            if(_tweetsDAO == null)
            {
                String collectionName = "spark_twitter_stream";
                _tweetsDAO = TweetsDAO.getInstance(collectionName, false);
            }

            _tweetsDAO.saveTweetInfo(document);
        }
    }

    private static void removeStopWords()
    {
        _stopwords.stream().filter(string ->
                _tokenList.contains(string)).forEach(string ->
                _tokenList.remove(string));
    }

    private static String replaceSymbols(String tweet)
    {
        for (String symbol : _symbols)
        {
            tweet = tweet.replace(symbol, "");
        }
        return tweet;
    }

    private static String replaceUrls(String tweet)
    {
        return tweet.replaceAll("https?://\\S+\\s?", "");
    }

    private static String replaceMentions(UserMentionEntity[] userMentionEntities, String tweet)
    {
        for(UserMentionEntity userMentionEntity : userMentionEntities)
        {
            String userMention = userMentionEntity.getScreenName();
            tweet = tweet.replace("@" + userMention, "");
        }
        return tweet;
    }

    private static void replaceAbbreviations()
    {
        for (int i = 0; i < _tokenList.size(); i++)
        {
            String token = _tokenList.get(i);
            if (_abbreviationsMap.containsKey(token))
            {
                _tokenList.set(i, _abbreviationsMap.get(token));
            }
        }
    }

    private static ConcurrentHashMap<String, String> linesToAbbreviationsMap(List<String> lines)
    {
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
        for(String line : lines)
        {
            map.put(line.split(",")[0], line.split(",")[1]);
        }
        return map;
    }

    private static void linesToSentimentsMap(List<String> lines)
    {
        for(String line : lines)
        {
            String[] strings = line.split(",");
            String adjective = strings[0];
            int sentiment = Integer.parseInt(strings[1].split(";")[3].split("=")[1]);
            _sentimentMap.put(adjective, sentiment);
        }
    }

    public static TweetInfo analyzeSentiment(TweetInfo tweetStream)
    {
        ConcurrentHashMap<String, String> hashMap = tweetStream.getTagsMap();
        Iterator<String> words = hashMap.keySet().iterator();
        boolean hasNegativeAdverbs = hashMap.keySet().contains("não");
        int negativeSentimentsCounter = 0;
        int positiveSentimentsCounter = 0;
        while (words.hasNext())
        {
            String word = words.next();
            String tag = hashMap.get(word);
            if (tag.equals("adj"))
            {
                int sentiment = 0;
                if (_sentimentMap.containsKey(word))
                {
                    sentiment = _sentimentMap.get(word);
                }
                if (sentiment == 1)
                {
                    if (hasNegativeAdverbs)
                    {
                        negativeSentimentsCounter++;
                    }
                    else
                    {
                        positiveSentimentsCounter++;
                    }
                }
                else if (sentiment == -1)
                {
                    if (hasNegativeAdverbs)
                    {
                        positiveSentimentsCounter++;
                    }
                    else
                    {
                        negativeSentimentsCounter++;
                    }
                }
            }
        }

        String tweetText = tweetStream.getTweetText();
        try
        {
            positiveSentimentsCounter += emoticonCounter(Sentiments.positive, tweetText);
            negativeSentimentsCounter += emoticonCounter(Sentiments.negative, tweetText);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        double sentimentsScore = countSentimentsScore(positiveSentimentsCounter, negativeSentimentsCounter);

        if (sentimentsScore > 0.5)
        {
            tweetStream.setSentiment(Sentiments.positive.name());
        }
        else if (sentimentsScore != 0.0 && sentimentsScore < 0.5)
        {
            tweetStream.setSentiment(Sentiments.negative.name());
        }
        else
        {
            tweetStream.setSentiment(Sentiments.neutral.name());
        }

        return tweetStream;
    }

    private static int emoticonCounter(Sentiments sentiment, String text) throws IOException
    {
        Pattern pattern = Pattern.compile("([0-9A-Za-z'&\\-\\./\\(\\)\\]\\[<>\\}\\*\\^\"\\D=:;]+)|((?::|;|=)(?:-)?(?:\\)|D|P))");
        Matcher matcher = pattern.matcher(text);
        int emoticonCounter = 0;
        while(matcher.find())
        {
            String emoticon = matcher.group();
            Stream<String> filteredLines = null;
            if (sentiment == Sentiments.positive)
            {
                filteredLines = _positiveEmoticons.stream().filter(s -> s.contains(emoticon));
            }
            else if (sentiment == Sentiments.negative)
            {
                filteredLines = _negativeEmoticons.stream().filter(s -> s.contains(emoticon));
            }
            if (filteredLines != null)
            {
                Optional<String> hasString = filteredLines.findFirst();
                if(hasString.isPresent())
                {
                    emoticonCounter++;
                }
            }
        }

        return emoticonCounter;
    }

    private static double countSentimentsScore(int positiveSentimentsCounter, int negativeSentimentsCounter)
    {
        int sum = positiveSentimentsCounter + negativeSentimentsCounter;
        int dif = positiveSentimentsCounter - negativeSentimentsCounter;
        if (sum > 0)
        {
            return dif / sum;
        }
        else
        {
            return 0;
        }
    }

}