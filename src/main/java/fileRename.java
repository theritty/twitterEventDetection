import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;


public class fileRename {
    public static void main(String[] args) throws IOException, ParseException {
//        File folder = new File("/home/ceren/Desktop/CENG/tez/experiments/stable-docs");
//        File[] listOfFiles = folder.listFiles();
//
//        for (int i = 0; i < listOfFiles.length; i++) {
//            if (listOfFiles[i].isFile()) {
//                //Wed May 04 02:13:29 EEST 2016
//                String file = listOfFiles[i].getName();
//                String[] tokens = file.split(" ");
//                String newName = "2016 ";
//                if(tokens[1].equals("Apr")) newName+="04 ";
//                else newName+="05 ";
//                newName +=  tokens[2] + " " + tokens[3] + " EEST " + tokens[0];
//
//                Path source = Paths.get("/home/ceren/Desktop/CENG/tez/experiments/stable-docs/" + file);
//                Files.move(source, source.resolveSibling(newName));
//
//                System.out.println("File " + listOfFiles[i].getName());
//            } else if (listOfFiles[i].isDirectory()) {
//                System.out.println("Directory " + listOfFiles[i].getName());
//            }
//        }
        String string = "Thu Apr 28 19:39:31 EEST 2016";
        DateFormat format = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
        Date date = null;
        date = format.parse(string);
        System.out.println(date); // Sat Jan 02 00:00:00 GMT 2010

    }
}
