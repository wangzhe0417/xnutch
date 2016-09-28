package org.apache.nutch.crawl;

/**
 * Created by zhangmingke on 16/6/28.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.fetcher.Fetcher;
import org.apache.nutch.indexer.IndexingJob;
import org.apache.nutch.parse.ParseSegment;
import org.apache.nutch.segment.SegmentChecker;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CrawlAndIndex extends Configured implements Tool {
  public static final Logger LOG = LoggerFactory.getLogger(CrawlAndIndex.class);

  private static String getDate() {
    return new SimpleDateFormat("yyyyMMddHHmmss").format(new Date(System
        .currentTimeMillis()));
  }

  public static void main(String args[]) throws Exception {
    Configuration conf = NutchConfiguration.create();
    int res = ToolRunner.run(conf, new CrawlAndIndex(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err
          .println("Usage: [-dir dir] [-host] [-noindex] [-nolink] [-noinjecthost] [-norobots] [-depth depth] [-thread thread] [-topN n]");
      return -1;
    }

    int depth = 1;
    int threads = 10;
    boolean isHost = false;
    boolean isHostInject = true;
    boolean isRobots = true;
    boolean noindex = false;
    boolean nolink = false;
    long topN = Long.MAX_VALUE;
    Path dir = null;
    Path hostDb = null;
    Path hostDir = null;
    Path robotsDb = null;
    Path robotsDir = null;
    for (int i = 0; i < args.length; i++) {
      if ("-dir".equals(args[i])) {
        dir = new Path(args[i + 1]);
        i++;

      } else if ("-host".equals(args[i])) {
        isHost = true;
      } else if ("-depth".equals(args[i])) {
        depth = Integer.parseInt(args[i + 1]);
        i++;
      } else if ("-thread".equals(args[i])) {
        threads = Integer.parseInt(args[i + 1]);
        i++;
      } else if ("-noinjecthost".equals(args[i])) {
        isHostInject = false;
      } else if ("-norobots".equals(args[i])) {
        isRobots = false;
      } else if ("-noindex".equals(args[i])) {
        noindex = true;
      }else if ("-topN".equals(args[i])) {
        topN = Integer.parseInt(args[i + 1]);
        i++;
      } else if ("-nolink".equals(args[i])) {
        noindex = true;
      }
    }
    if (dir == null)
      dir = new Path("crawl-" + getDate());

    hostDb = new Path(dir + "/hostdb");
    robotsDb = new Path( dir + "/robotsdb");
    Path crawlDb = new Path(dir + "/crawldb");
    hostDir = dir;
    robotsDir = dir;

    JobConf job = new NutchJob(getConf());
    FileSystem fs = FileSystem.get(job);

    if (LOG.isInfoEnabled()) {
      LOG.info("crawl started in: " + dir);
      LOG.info("threads = " + threads);
      LOG.info("depth = " + depth);
      LOG.info("topN = " + topN);
      LOG.info("topN = " + topN);
      if (isHost) {
        LOG.info("noindex = " + noindex);
        if (isHostInject)
          LOG.info("hostInjector = true");
      }
      
      LOG.info("norobots = " + !isRobots);
    }

    Path linkDb = new Path(dir + "/linkdb");
    Path segments = new Path(dir + "/segments");
    // Path indexes = new Path(dir + "/indexes");
    // Path index = new Path(dir + "/index");
    List<Path> segmentsPath = new ArrayList<Path>();
    Path hostSegments = new Path(hostDir + "/hostSegments");
    Path robotsSegments = new Path(robotsDir + "/robotsSegments");

    NewGenerator newGenerator = new NewGenerator(getConf());
    Generator generator = new Generator(getConf());
    Fetcher fetcher = new Fetcher(getConf());
    ParseSegment parseSegment = new ParseSegment(getConf());
    CrawlDb crawlDbTool = new CrawlDb(getConf());
    LinkDb linkDbTool = new LinkDb(getConf());
    HostInjector hostInjector = new HostInjector(getConf());
    HostGenerator hostGenerator = new HostGenerator(getConf());
    HostFetcher hostFetcher = new HostFetcher(getConf());
    RobotsInjector robotsInjector = new RobotsInjector(getConf());
    RobotsGenerator robotsGenerator = new RobotsGenerator(getConf());
    HostDb hostDbTool = new HostDb(getConf());

    int i;
    for (i = 0; i < depth; i++) {
      // 如果是有host注入,那么采用newGenerator
      if (isHost) {
        if (isHostInject) { // 如果提示有种子注入这一过程,就可以将其添加进去;
          hostInjector.inject(crawlDb, hostDb);
        }

        Path hostsegs = hostGenerator.generate(hostDb, hostSegments, -1, topN,
            System.currentTimeMillis()).getParent();
        if (hostsegs == null) {
          LOG.info("Stopping at depth=" + i + " - no more HOSTs to resolve.");
        }

        hostFetcher.fetch(hostsegs, threads);

        hostDbTool.updateIP(hostDb, new Path[] { hostsegs }, true, true);
        
        if (isRobots) {
          robotsInjector.inject(robotsDb, hostDb);
          Path robotsegs = robotsGenerator.generate(robotsDb, robotsSegments, -1,
              topN, System.currentTimeMillis()).getParent();

          if (robotsegs == null) {
            LOG.info("Stopping at depth=" + i + " - no more Robots to fetch.");
          }

          fetcher.fetch(robotsegs, threads);

          hostDbTool.updateRobot(robotsDb, new Path[] { robotsegs }, true, true);
        }
        
        Path[] segsHost = newGenerator.generate(crawlDb, robotsDb, segments, -1,
            topN, System.currentTimeMillis());
        if (segsHost == null) {
          LOG.info("stop at depth = " + i + "- no more urls to fetch");
        }

        fetcher.fetch(segsHost[0], threads);

        if (!Fetcher.isParsing(job)) {
          parseSegment.parse(segsHost[0]);
        }
        crawlDbTool.update(crawlDb, segsHost, true, true);

      } else {
        // 如果没有host注入,则采用老版本的generator
        Path[] segs = generator.generate(crawlDb, segments, -1, topN,
            System.currentTimeMillis());
        if (segs == null) {
          LOG.info("Stopping at depth=" + i + " - no more URLs to fetch.");
          break;
        }

        fetcher.fetch(segs[0], threads); // fetch it

        if (!Fetcher.isParsing(job)) {
          parseSegment.parse(segs[0]); // parse it, if needed
        }

        crawlDbTool.update(crawlDb, segs, true, true);
      }
    }

    FileSystem segmentsfs = segments.getFileSystem(getConf());
    FileStatus[] fstats = segmentsfs.listStatus(segments,
        HadoopFSUtil.getPassDirectoriesFilter(segmentsfs));
    Path[] files = HadoopFSUtil.getPaths(fstats);
    for (Path p : files) {

      if (SegmentChecker.isIndexable(p, segmentsfs)) {
        segmentsPath.add(p);
      }

    }

    if (i > 0) {

      if(!nolink){
      // invert的参数为Path linkDb, final Path segmentsDir, boolean normalize,
      // boolean filter, boolean force
      linkDbTool.invert(linkDb, segments, true, true, false);
      }

      if (!noindex) {
        IndexingJob indexer = new IndexingJob(getConf());
        // index的参数为Path crawlDb, Path linkDb, List<Path> segments,
        // boolean noCommit, boolean deleteGone, String params, boolean filter,
        // boolean normalize
        indexer.index(crawlDb, linkDb, segmentsPath, false, false, null, true,
            true);
      }
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("crawl finished: " + dir);
    }
    return 0;
  }

}
