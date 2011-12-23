package org.pedesis;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A dead simple, hacky, logs cruncher.
 *
 * @author jochen@pedesis.org (Jochen Bekmann)
 */
public class greplog {

  interface LineHandler {
    void handle(String line, String filename);
  }

  private static LineHandler PRINT_TO_STDOUT_HANDLER = new LineHandler() {
    @Override
    public void handle(String line, String filename) {
      System.out.println(line);
    }
  };

  static class Metric implements Comparable {

    static final DecimalFormat simpleFormat = new DecimalFormat("#.##");
    long count = 0;

    Date start = null;
    Date last = null;

    public void inc(Date date) {
      if (start == null) {
        start = date;
      }
      last = date;
      count++;
    }

    public String toString() {
      double elapsed = last.getTime() - start.getTime();
      if (elapsed == 0)
        return Long.toString(count);

      DateFormat dateFormat = new SimpleDateFormat("y-M-d HH:MM:ss.SSS");
      double ratePerMs = (double) count / (double) elapsed;

      return Long.toString(count) + " (" +
          simpleFormat.format(ratePerMs) + "/ms " +
          simpleFormat.format(ratePerMs * 1000 <= count ? ratePerMs * 1000 : count) + "/s " +
          simpleFormat.format(ratePerMs * 60000 <= count ? ratePerMs * 60000 : count) + "/min " +
          simpleFormat.format(ratePerMs * 3600000 <= count ? ratePerMs * 3600000 : count) + "/hr " +
          simpleFormat.format(ratePerMs * 86400000 <= count ? ratePerMs * 86400000 : count) + "/day " +
          "from " + dateFormat.format(start) + " to " + dateFormat.format(last) +
          ")";
    }

    @Override
    public int compareTo(Object o) {
      return Longs.compare(count, ((Metric) o).count);
    }
  }

  public static List<String> LOG_LEVEL_LIST = ImmutableList.of("ERROR", "WARN", "INFO", "TRACE");

  static class LogHandler implements LineHandler {

    static final Pattern LOG_ITEM = Pattern.compile(
        "(\\d\\d:\\d\\d:\\d\\d\\.\\d\\d\\d) (ERROR|WARN|INFO|TRACE) (.*)");

    static final Pattern FILE_NAME_DATE = Pattern.compile(".*(\\d{4}-\\d\\d-\\d\\d).*");
    static final String TODAY = new SimpleDateFormat("y-M-d").format(new Date());

    ArrayList<Pattern> patternList;

    private int lineCount = 0;

    private final Map<String, Metric> metrics = Maps.newHashMap();
    private final boolean nonmatch;
    private final int logLevel;

    public LogHandler(List<String> patterns, int logLevel, boolean nonmatch) {
      this.logLevel = logLevel;
      this.nonmatch = nonmatch;
      patternList = new ArrayList<Pattern>();
      for (String p : patterns)
        patternList.add(Pattern.compile(p));
    }

    private void tick(String name, Date date) {
      if (!metrics.containsKey(name))
        metrics.put(name, new Metric());
      Metric m = metrics.get(name);
      m.inc(date);
    }

    public void handle(String line, String fileName) {
      ++lineCount;

      String day = TODAY;
      final Matcher fileNameMatcher = FILE_NAME_DATE.matcher(fileName);
      if (fileNameMatcher.matches()) {
        day = fileNameMatcher.group(1);
      }

      Matcher itemMatcher = LOG_ITEM.matcher(line);
      if (itemMatcher.matches()) {
        if (itemMatcher.groupCount() != 3) {
          throw new RuntimeException("Expected exactly 3 groups in " + line);
        }

        DateFormat dateFormat = new SimpleDateFormat("y-M-d@H:m:s.S");
        try {
          Date date = dateFormat.parse(day + "@" + itemMatcher.group(1));
          tick("items", date);

          String errorLevel = itemMatcher.group(2);
          tick(errorLevel, date);

          if (LOG_LEVEL_LIST.indexOf(errorLevel) > logLevel)
            return;

          for (Pattern pattern : patternList) {
            Matcher m = pattern.matcher(itemMatcher.group(3));
            if (m.find()) {
              String str = errorLevel + " " + pattern.pattern();
              tick(str, date);
              if (m.groupCount() > 0) {
                StringBuilder sb = new StringBuilder(str);
                sb.append(" ==> ");
                for (int i = 1; i <= m.groupCount(); i++) {
                  sb.append(" (( ").append(m.group(i)).append(" )) ");
                }
                tick(sb.toString(), date);
              }
              return;
            }
          }

          if (nonmatch) {
            tick("nomatch :: " + errorLevel + " " + itemMatcher.group(3), date);
          }

        } catch (ParseException e) {
          e.printStackTrace();
        }
      }
    }

    public void dumpStats(LineHandler handler) {
      handler.handle("lineCount: " + lineCount, null);

      handler.handle("=============================================", null);
      handler.handle("              BY FREQUENCY", null);
      handler.handle("=============================================", null);

      List<Map.Entry<String, Metric>> invSorted = Lists.newArrayList();
      invSorted.addAll(metrics.entrySet());
      Collections.sort(invSorted, new Comparator<Map.Entry<String, Metric>>() {
        @Override
        public int compare(Map.Entry<String, Metric> A, Map.Entry<String, Metric> B) {
          int c = B.getValue().compareTo(A.getValue()); // inverse sort on metric.
          if (c == 0) {
            return A.getKey().compareTo(B.getKey()); // alphabetical on string.
          }
          return c;
        }
      });
      for (Map.Entry<String, Metric> entry : invSorted) {
        handler.handle(entry.getValue() + " = " + entry.getKey(), null);
      }

      handler.handle("=============================================", null);
      handler.handle("              ALPHABETIC", null);
      handler.handle("=============================================", null);

      List<String> keys = new ArrayList<String>(metrics.keySet());
      Collections.sort(keys);
      for (String key : keys) {
        handler.handle(key + " = " + metrics.get(key).toString(), null);
      }
    }
  }

  static class ConfigFileHandler implements LineHandler {

    static final Pattern LOG_LEVEL = Pattern.compile("\\s*loglevel\\s*=\\s*(ERROR|WARN|INFO|TRACE)",
        Pattern.CASE_INSENSITIVE);
    static final Pattern NON_MATCH = Pattern.compile("\\s*nonmatch\\s*=\\s*(false|true)",
        Pattern.CASE_INSENSITIVE);
    static final Pattern GREPLOG_MARKER = Pattern.compile("\\s*greplogconfig",
        Pattern.CASE_INSENSITIVE);
    static final int DEFAULT_LOG_LEVEL = 1;
    static final boolean DEFAULT_NON_MATCH_VALUE = true;

    public List<String> patterns = Lists.newArrayList();
    public int logLevel = DEFAULT_LOG_LEVEL;
    public boolean nonmatch = DEFAULT_NON_MATCH_VALUE;
    public boolean greplogconfig = false;
    private LineHandler outputHandler;

    public ConfigFileHandler(LineHandler outputHandler) {
      this.outputHandler = outputHandler;
    }

    @Override
    public void handle(String line, String filename) {
      if (line.trim().length() > 0 && !line.trim().startsWith("#")) {
        Matcher llm = LOG_LEVEL.matcher(line);
        Matcher nmm = NON_MATCH.matcher(line);
        Matcher greplog = GREPLOG_MARKER.matcher(line);
        if (greplog.find()) {
          greplogconfig = true;
        } else if (llm.find()) {
          logLevel = LOG_LEVEL_LIST.indexOf(llm.group(1));
          outputHandler.handle("Config: loglevel set to " + llm.group(1), null);
        } else if (nmm.find()) {
          nonmatch = nmm.group(1).equals("true");
          outputHandler.handle("Config: nonmatch set to " + nmm.group(1), null);
        } else {
          patterns.add(line);
          // outputHandler.handle("Config: pattern = " + line, null);
        }
      }
    }
  }

  static class SourceFileHandler implements LineHandler {
    static final Pattern LOGGER_DECLARATION =
        Pattern.compile(".* Logger (\\w+).*LoggerFactory\\.getLogger\\((\\w+).class\\)");

    static final String LOG_STATEMENT_POSTFIX = "\\.(error|warn|info|trace)\\(\"(.*)\"";

    private String loggerClass = null;
    private Pattern logStatementPattern = null;
    private final LineHandler outputHandler;
    private boolean first = true;

    public SourceFileHandler(LineHandler outputHandler) {
      this.outputHandler = outputHandler;
    }

    @Override
    public void handle(String line, String filename) {
      if (loggerClass == null) {
        Matcher loggerm = LOGGER_DECLARATION.matcher(line);
        if (loggerm.find()) {
          String loggerVariable = loggerm.group(1);
          loggerClass = loggerm.group(2);
          logStatementPattern = Pattern.compile(".*" + loggerVariable + LOG_STATEMENT_POSTFIX);
        }
      } else {
        Matcher logstatm = logStatementPattern.matcher(line);
        if (logstatm.find()) {
          // ignore loglevel for now.
          String statement = Pattern.quote(logstatm.group(2));

          if (first) {
            outputHandler.handle("#", null);
            outputHandler.handle("# " + filename, null);
            outputHandler.handle("#", null);
            first = false;
          }

          // **ASSUMPTION** that slf4j '{}' notation is used!
          String regex = statement.replaceAll("\\{\\}", "\\\\E(.*)\\\\Q");
          outputHandler.handle(loggerClass + ": " + regex, filename);
        }
      }
    }
  }

  public static void main(final String[] args) {
    if (args.length < 2) {
      printHelp();
      return;
    }

    if ("crunch".equals(args[0])) {
      ConfigFileHandler configHandler = new ConfigFileHandler(PRINT_TO_STDOUT_HANDLER);
      readFile(args[1], configHandler);
      if (!configHandler.greplogconfig) {
        System.err.println("Config file not passed, or it doesn't contain \"greplogconfig\" string.");
        return;
      } else {
        System.out.println("Config: " + configHandler.patterns.size() + " patterns loaded.");
      }

      LogHandler logHandler = new LogHandler(configHandler.patterns, configHandler.logLevel,
          configHandler.nonmatch);
      for (int i = 2; i < args.length; i++) {
        readFile(args[i], logHandler);
      }
      logHandler.dumpStats(PRINT_TO_STDOUT_HANDLER);

    } else if ("suggest".equals(args[0])) {
      PRINT_TO_STDOUT_HANDLER.handle("# Automatically generated regex suggestions. Keep AFTER" +
          " manually maintained regexes, or delete. \n" +
          "# WARNING - this are hackily extracted!", null);
      for (int i = 1; i < args.length; i++) {
        readFile(args[i], new SourceFileHandler(PRINT_TO_STDOUT_HANDLER));
      }
    } else {
      printHelp();
    }
  }

  private static void printHelp() {
    System.out.println("greplog crunch <configfile> <logfile1> <logfile2> ..");
    System.out.println("greplog suggest <sourcefile1> <sourcefile2> ..");
  }

  static void readFile(String fname, LineHandler handler) {
    try {
      FileInputStream fstream = new FileInputStream(fname);
      DataInputStream in = new DataInputStream(fstream);
      BufferedReader br = new BufferedReader(new InputStreamReader(in));

      String strLine;
      while ((strLine = br.readLine()) != null) {
        handler.handle(strLine, fname);
      }
      in.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
