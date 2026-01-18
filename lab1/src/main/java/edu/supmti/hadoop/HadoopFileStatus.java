package edu.supmti.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopFileStatus {

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: HadoopFileStatus <chemin_hdfs> <nom_fichier> <nouveau_nom_fichier>");
            System.out.println("Exemple: HadoopFileStatus /user/root/input purchases.txt achats.txt");
            System.exit(1);
        }

        String dir = args[0];
        String fileName = args[1];
        String newFileName = args[2];

        Configuration conf = new Configuration();

        try (FileSystem fs = FileSystem.get(conf)) {

            Path filePath = new Path(dir, fileName);

            if (!fs.exists(filePath)) {
                System.out.println("File does not exist: " + filePath);
                System.exit(1);
            }

            FileStatus status = fs.getFileStatus(filePath);

            System.out.println("File Path: " + filePath);
            System.out.println("File Name: " + filePath.getName());
            System.out.println("File Size: " + status.getLen() + " bytes");
            System.out.println("File owner: " + status.getOwner());
            System.out.println("File permission: " + status.getPermission());
            System.out.println("File Replication: " + status.getReplication());
            System.out.println("File Block Size: " + status.getBlockSize());

            // Infos des blocs
            BlockLocation[] blockLocations = fs.getFileBlockLocations(status, 0, status.getLen());
            for (BlockLocation blockLocation : blockLocations) {
                System.out.println("---- Block ----");
                System.out.println("Block offset: " + blockLocation.getOffset());
                System.out.println("Block length: " + blockLocation.getLength());
                System.out.print("Block hosts: ");
                for (String host : blockLocation.getHosts()) {
                    System.out.print(host + " ");
                }
                System.out.println();
            }

            // Rename dans le même dossier
            Path newPath = new Path(dir, newFileName);
            boolean renamed = fs.rename(filePath, newPath);

            if (renamed) {
                System.out.println("Rename OK: " + filePath + " -> " + newPath);
            } else {
                System.out.println("Rename FAILED.");
                System.exit(1);
            }

        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
