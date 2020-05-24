import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Main
{
    private static String symbols = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";



    public static void main(String[] args) throws Exception
    {
         String nameContainer = "1d4bb94a9807";
         String rootPath = "hdfs://" + nameContainer + ":8020";
         String filePath = "hdfs://" + nameContainer + ":8020/test/file.txt";
         String filePath2 = "hdfs://" + nameContainer + ":8020/test/double/file2.txt";
         String filePathForScan = "hdfs://" + nameContainer + ":8020/test";
        FileSystem hdfsMain;

        FileAccess fileAccess = new  FileAccess(rootPath);
        hdfsMain = fileAccess.getHdfs();

        Path file = new Path(filePath);

        if (hdfsMain.exists(file)) fileAccess.delete(filePath, hdfsMain);

        fileAccess.create(filePath, hdfsMain);
        fileAccess.create(filePath2, hdfsMain);

        fileAccess.append(filePath, "Hello", hdfsMain);

        System.out.println("Содержимое файла: " + fileAccess.read(filePath, hdfsMain));

        System.out.print(filePath + " является: ");
        System.out.println(fileAccess.isDirectory(filePath, hdfsMain)?"Директорией":"Файлом");

        System.out.println("list of files and subdirectories: " + fileAccess.list(filePathForScan, hdfsMain));

        hdfsMain.close();
    }

    static String getRandomWord()
    {
        StringBuilder builder = new StringBuilder();
        int length = 2 + (int) Math.round(10 * Math.random());
        int symbolsCount = symbols.length();
        for(int i = 0; i < length; i++) {
            builder.append(symbols.charAt((int) (symbolsCount * Math.random())));
        }
        return builder.toString();
    }
}
