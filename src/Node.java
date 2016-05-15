import org.apache.commons.cli.*;
import org.zeromq.ZMQ;

import java.nio.charset.Charset;

public class Node {

    private String pubEndpoint;
    private String routerEndpoint;
    private String nodeName;
    private String[] peers;

    private void start() {
        // Create a single-threaded ZMQ context.
        final ZMQ.Context context = ZMQ.context(1);

        // Create a SUB socket for receiving messages from the broker.
        final ZMQ.Socket subSocket = context.socket(ZMQ.SUB);
        subSocket.connect(pubEndpoint);
        System.out.println("Subscriber socket connected.");

        // Make sure we get messages meant for us!
        subSocket.setIdentity(nodeName.getBytes());

        // Create handler for SUB socket.

        // Create a REQ socket for sending messages to the broker.
        final ZMQ.Socket reqSocket = context.socket(ZMQ.REQ);
        reqSocket.connect(routerEndpoint);
        reqSocket.setIdentity(nodeName.getBytes());
        System.out.println("Request socket connected.");

        System.out.println("Blocking on recv...");
        String s = subSocket.recvStr(Charset.defaultCharset());
        System.out.println("Received: >>" + s);
    }

    /* ========================================
            Driver Methods
    ======================================== */

    private static Options commandLineOptions;

    private static Node parseArgs(String[] args) throws ParseException {
        commandLineOptions = new Options();

        // pub-endpoint
        Option o1 = new Option("pe", "pub-endpoint", true, "URI of the broker's ZeroMQ PUB socket.");
        o1.setRequired(true);
        commandLineOptions.addOption(o1);

        // router-endpoint
        Option o2 = new Option("re", "router-endpoint", true, "URI of the broker's ZeroMQ ROUTER socket.");
        o2.setRequired(true);
        commandLineOptions.addOption(o2);

        // node-name
        Option o3 = new Option("n", "node-name", true, "The node's name.");
        o3.setRequired(true);
        commandLineOptions.addOption(o3);

        // peer
        commandLineOptions.addOption("p", "peer", true, "This option can appear multiple times, and is used to specify all the other nodes in the data store.");

        // help
        commandLineOptions.addOption("h", "help", false, "Show this message.");

        // Command line arguments parsing
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(commandLineOptions, args);

        Node node = new Node();
        node.pubEndpoint = cmd.getOptionValue("pub-endpoint");
        node.routerEndpoint = cmd.getOptionValue("router-endpoint");
        node.nodeName = cmd.getOptionValue("node-name");
        node.peers = cmd.getOptionValues("peer");
        return node;
    }

    private static void printHelpMessage() {
        HelpFormatter hf = new HelpFormatter();
        hf.printHelp("node", commandLineOptions);
    }

    public static void main(String[] args) {
        System.out.println("Hello, world!");
        try {
            Node node = parseArgs(args);
            node.start();
        } catch (ParseException ex) {
            System.err.println("Error parsing arguments: " + ex);
            printHelpMessage();
            return;
        }
    }
}
