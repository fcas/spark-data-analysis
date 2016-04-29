package helpers;

/**
 * Created by Felipe on 4/23/16.
 */
public enum KeyWords
{
    pecesiqueira, naosalvo, rafinhabastos, jovemnerd, mkarolqueiroz, felipenetoreal, belpesce, dilma, cunha;

    public static String[] names()
    {
        KeyWords[] keyWords = KeyWords.values();
        String[] names = new String[keyWords.length];

        for (int i = 0; i < keyWords.length; i++)
        {
            names[i] = keyWords[i].name();
        }
        return names;
    }
}