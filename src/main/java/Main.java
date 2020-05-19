import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Main
{
    private static String symbols = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static String rootPath;
    protected static FileSystem hdfs;


    public static void main(String[] args) throws Exception
    {
         String nameContainer = "54748e3fdfa5";
         String rootPath = "hdfs://" + nameContainer + ":8020";
         String filePath = "hdfs://" + nameContainer + ":8020/test/file.txt";
         String filePath2 = "hdfs://" + nameContainer + ":8020/test/double/file2.txt";
         String filePathForScan = "hdfs://" + nameContainer + ":8020/test";

        hdfs = FileAccess.fileAccess(rootPath);

        Path file = new Path(filePath);

        if (hdfs.exists(file)) {
            FileAccess.delete(filePath);
        }

        FileAccess.create(filePath);
        FileAccess.create(filePath2);

        FileAccess.append(filePath, "Hello");

        System.out.println("Содержимое файла: " + FileAccess.read(filePath));

        System.out.print(filePath + " является: ");
        System.out.println(FileAccess.isDirectory(filePath)?"Директорией":"Файлом");

        System.out.println("list of files and subdirectories: " + FileAccess.list(filePathForScan));

        hdfs.close();
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
