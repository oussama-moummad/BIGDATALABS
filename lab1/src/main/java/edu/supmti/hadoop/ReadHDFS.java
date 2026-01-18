package edu.supmti.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ReadHDFS {

    public static void main(String[] args) throws IOException {

        if (args.length < 1) {
            System.out.println("Usage: ReadHDFS <chemin_fichier_hdfs>");
            System.out.println("Exemples: ReadHDFS /user/root/input/achats.txt");
            System.out.println("          ReadHDFS ./purchases.txt");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        try (FileSystem fs = FileSystem.get(conf)) {

            Path filePath = new Path(args[0]);

            if (!fs.exists(filePath)) {
                System.out.println("File does not exist: " + filePath);
                System.exit(1);
            }

            try (FSDataInputStream inStream = fs.open(filePath);
                 BufferedReader br = new BufferedReader(new InputStreamReader(inStream))) {

                String line;
                while ((line = br.readLine()) != null) {
                    System.out.println(line);
                }
            }
        }
    }
}
