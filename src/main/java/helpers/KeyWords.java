package helpers;

/**
 * Created by Felipe on 4/23/16.
 */
public enum KeyWords
{
    PrefRioBranco,
    PrefMaceio,
    PrefMacapa,
    PrefManaus,
    Curitiba_PMC,
    prefsp,
    prefeitura_CBA,
    Gov_DF,
    prefeiturapmf,
    VitoriaOnLine,
    PrefeituradeGyn,
    cgnoticias,
    Prefeitura_POA,
    scflorianopolis,
    agecomsalvador,
    PrefeituraSL,
    prefeiturabh,
    prefeiturabelem,
    prefeitura_pvh,
    PrefeituAracaju,
    cidadepalmas,
    prefeitura_the,
    Prefeitura_Rio,
    pmjponline,
    prefrecife,
    NatalPrefeitura,
    PrefeituraBV;

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