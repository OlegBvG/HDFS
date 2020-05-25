import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class FileAccess  {

    /** V
     * Initializes the class, using rootPath as "/" directory
     *
     * @param rootPath - the path to the root of HDFS,
     * for example, hdfs://localhost:32771
     */
    private FileSystem hdfs;
    private String rootPath;

    public FileAccess(String rootPath) throws URISyntaxException, IOException {
        this.rootPath = rootPath;
        Configuration configuration = new Configuration();
        configuration.set("dfs.client.use.datanode.hostname", "true");
        System.setProperty("HADOOP_USER_NAME", "root");

        this.hdfs = FileSystem.get(new URI(rootPath), configuration);

    }

    public FileSystem getHdfs() {
        return hdfs;
    }

    /** V
     * Creates empty file or directory
     *
     * @param path
     */
    public  void create(String path) throws IOException {
        Path file = new Path(path);
        hdfs.create(file).close();
    }

    /** V
     * Appends content to the file
     *
     * @param path
     * @param content
     */


    public  void append(String path, String content) {
        Path file = new Path(path);
        BufferedWriter br;
        try (OutputStream os = hdfs.append(file)) {
            br = new BufferedWriter(
                    new OutputStreamWriter(os, "UTF-8")
            );

        br.write(content);
//        for(int i = 0; i < 10_000_000; i++) {
//            br.write(Main.getRandomWord() + " ");
//        }
        br.flush();
        br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /** V
     * Returns content of the file
     *
     * @param path
     * @return
     */
    public  String read(String path) {
        Path file = new Path(path);

        String lineAll="";
        try (FSDataInputStream inputStream = hdfs.open(file)) {
            BufferedReader bufferedReader = new BufferedReader(
                    new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                System.out.println(line);
                lineAll = lineAll + line;
            }

//        String out= IOUtils.toString(inputStream, "UTF-8");

            inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lineAll;

    }

    /** V
     * Deletes file or directory
     *
     * @param path
     */
    public  void delete(String path) {
        Path file = new Path(path);
        try {
            if (hdfs.exists(file)) {
                hdfs.delete(file, true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /** V
     * Checks, is the "path" is directory or file
     *
     * @param path
     * @return
     */
    public  boolean isDirectory(String path) throws IOException {
        Path file = new Path(path);
        boolean isDir = false;
        if (hdfs.isDirectory(file)) isDir = true;
        if (hdfs.isFile(file)) isDir = false;

        return isDir;
    }

    /** V
     * Return the list of files and subdirectories on any directory
     *
     * @param path
     * @return
     */
    public  List<String> list(String path)
    {
        boolean recursive = true;
            List<String> list = new ArrayList<String>();
            try
            {
                Path pathScan = new Path(path);  //throws IllegalArgumentException, all others will only throw IOException
                pathScan = hdfs.resolvePath(pathScan);

                if (recursive)
                {
                    Queue<Path> fileQueue = new LinkedList<Path>();

                    fileQueue.add(pathScan);

                    while (!fileQueue.isEmpty())
                    {
                        Path filePath = fileQueue.remove();

                        if (hdfs.isFile(filePath))
                        {
                            list.add(filePath.toString());
                        }
                        else
                        {
                            FileStatus[] fileStatuses = hdfs.listStatus(filePath);
                            for (FileStatus fileStatus : fileStatuses)
                            {
                                fileQueue.add(fileStatus.getPath());
                            }
                        }

                    }

                }
                else
                {
                    if (hdfs.isDirectory(pathScan))
                    {
                        FileStatus[] fileStatuses = hdfs.listStatus(pathScan);

                        for (FileStatus fileStatus : fileStatuses)
                        {
                            if (fileStatus.isFile())
                                list.add(fileStatus.getPath().toString());
                        }
                    }
                    else
                    {
                        list.add(pathScan.toString());
                    }

                }

            }
            catch(Exception ex)
            {
                ex.printStackTrace();

                return new ArrayList<String>();
            }
            return list;
    }

}
