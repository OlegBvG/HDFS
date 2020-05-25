import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Main
{
    private static String symbols = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";



    public static void main(String[] args) throws Exception
    {
         String nameContainer = "7d2b8a536d55";
         String rootPath = "hdfs://" + nameContainer + ":8020";
         String filePath = "hdfs://" + nameContainer + ":8020/test/file.txt";
         String filePath2 = "hdfs://" + nameContainer + ":8020/test/double/file2.txt";
         String filePathForScan = "hdfs://" + nameContainer + ":8020/test";
        FileSystem hdfsMain;

        FileAccess fileAccess = new  FileAccess(rootPath);
        hdfsMain = fileAccess.getHdfs();

        Path file = new Path(filePath);

        if (hdfsMain.exists(file)) fileAccess.delete(filePath);

        fileAccess.create(filePath);
        fileAccess.create(filePath2);

        fileAccess.append(filePath, "Hello");

        System.out.println("Содержимое файла: " + fileAccess.read(filePath));

        System.out.print(filePath + " является: ");
        System.out.println(fileAccess.isDirectory(filePath)?"Директорией":"Файлом");

        System.out.println("list of files and subdirectories: " + fileAccess.list(filePathForScan));

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
