package edu.supmti.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSWrite {

    public static void main(String[] args) throws IOException {

        if (args.length < 1) {
            System.out.println("Usage: HDFSWrite <chemin_fichier_hdfs> [message]");
            System.out.println("Exemple: HDFSWrite ./input/bonjour.txt \"Bonjour tout le monde !\"");
            System.exit(1);
        }

        String filePathStr = args[0];
        String message = (args.length >= 2) ? args[1] : "Bonjour tout le monde !";

        Configuration conf = new Configuration();

        try (FileSystem fs = FileSystem.get(conf)) {

            Path filePath = new Path(filePathStr);

            if (fs.exists(filePath)) {
                System.out.println("File already exists: " + filePath);
                System.exit(0);
            }

            // create() crée le fichier dans HDFS
            try (FSDataOutputStream outStream = fs.create(filePath)) {
                outStream.writeBytes(message);
                outStream.writeBytes("\n");
                System.out.println("Write OK: " + filePath);
            }
        }
    }
}
