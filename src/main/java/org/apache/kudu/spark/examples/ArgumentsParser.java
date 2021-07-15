package org.apache.kudu.spark.examples;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class ArgumentsParser {
    @Option(name = "-k", aliases = "--mode", required = true,
            usage = "Specify the mode: [E]xport kudu to csv or [I]mport csv to kudu")
    public String mode;

    @Option(name = "-m", aliases = "--masters", required = true, usage = "Specify the master addresses")
    public String masters;

    @Option(name = "-t", aliases = "--table", required = true, usage = "Specify the table name")
    public String table;

    @Option(name = "-p", aliases = "--path", required = true, usage = "Specify the directory path of exported csv")
    public String csvPath;

    @Option(name = "-s", aliases = "--showsample", required = false, usage = "Show some sample data")
    public boolean showSample = false;

    public boolean parseArgs(final String[] args) {
        final CmdLineParser parser = new CmdLineParser(this);
        if (args.length < 1) {
            parser.printUsage(System.out);
            System.exit(-1);
        }
        boolean ret = true;
        try {
            parser.parseArgument(args);
        } catch (CmdLineException clEx) {
            System.out.println("Error: failed to parse command-line opts: " + clEx);
            ret = false;
        }
        return ret;
    }

}
