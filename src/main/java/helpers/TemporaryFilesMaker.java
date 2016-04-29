package helpers;

import java.io.*;

/**
 * Created by Felipe on 4/21/16.
 */
public class TemporaryFilesMaker
{
    public static File getFile(String filename, String extension)
    {
        File file = null;
        try
        {
            InputStream input = TemporaryFilesMaker.class.getClassLoader().getResourceAsStream(filename);
            file = File.createTempFile(filename, extension);
            OutputStream outputStream = new FileOutputStream(file);
            int read;
            byte[] bytes = new byte[1024];

            while ((read = input.read(bytes)) != -1)
            {
                outputStream.write(bytes, 0, read);
            }
            return file;
        }
        catch (IOException ex)
        {
            ex.printStackTrace();
            return file;
        }
    }
}
