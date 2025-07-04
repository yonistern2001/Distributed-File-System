package org.example;

import io.etcd.jetcd.Client;
import lombok.RequiredArgsConstructor;

import java.time.Duration;
import java.util.Scanner;

@RequiredArgsConstructor
public class FileSystemCLIRunner {

    private static final String ETCD_URI = "http://localhost:9000";

    public static void main(String[] args) {

        Client client = Client.builder()
                .endpoints(ETCD_URI)
                .keepaliveTime(Duration.ofSeconds(60))
                .keepaliveTimeout(Duration.ofSeconds(20))
                .build();

        NodeHttpClient nodeHttpClient = new NodeHttpClient();
        LeaderResolver leaderResolver = new LeaderResolver(client);
        CommandExecutor executor = new CommandExecutor(nodeHttpClient, leaderResolver);
        FileSystemCLIRunner runner = new FileSystemCLIRunner(executor);

        runner.run();
    }


    private final CommandExecutor executor;
    private boolean run = true;

    public void run() {
        try(Scanner scanner = new Scanner(System.in)) {
            while(run) {

                System.out.print("File System Command: ");
                String[] args = scanner.nextLine().split(" ");
                String command = args[0];

                String result = switch (command) {
                    case "upload" -> upload(args);
                    case "download" -> download(args);
                    case "delete" -> delete(args);
                    case "list" -> list(args);
                    case "quit" -> quit(args);
                    default -> "Unknown command: " + command;
                };
                System.out.println(result);
            }
        } finally {
            executor.close();
        }

        System.exit(0);
    }

    private String upload(String[] args) {
        if(args.length != 3) {
            return "Invalid arguments, should be: upload <local_path> <target_path>";
        }

        String localPath = args[1];
        String targetPath = args[2];

        executor.upload(localPath, targetPath);
        return "Successfully uploaded";
    }

    private String download(String[] args) {
        if(args.length != 3) {
            return "Invalid arguments, should be: download <target_path> <local_path>";
        }

        String targetPath = args[1];
        String localPath = args[2];

        executor.download(targetPath, localPath);
        return "Successfully downloaded";
    }

    private String delete(String[] args) {
        if(args.length != 2) {
            return "Invalid arguments, should be: delete <target_path>";
        }

        String targetPath = args[1];

        executor.delete(targetPath);
        return "Successfully deleted";
    }

    private String list(String[] args) {
        if(args.length != 2) {
            return "Invalid arguments, should be: list <prefix>";
        }

        String prefix = args[1];

        return "Files with prefix \"" + prefix + "\":\n" + String.join("\n", executor.list(prefix));
    }

    private String quit(String[] args) {
        if(args.length != 1) {
            return "Invalid arguments, should be: quit";
        }

        this.run = false;
        return "Exiting";
    }
}