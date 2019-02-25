package flume.sink.mongo;

import static flume.sink.mongo.MongoSinkConstants.BATCH_SIZE;
import static flume.sink.mongo.MongoSinkConstants.COLLECTION;
import static flume.sink.mongo.MongoSinkConstants.DATABASE;
import static flume.sink.mongo.MongoSinkConstants.DEFAULT_BATCH_SIZE;
import static flume.sink.mongo.MongoSinkConstants.HOSTNAMES;
import static flume.sink.mongo.MongoSinkConstants.PASSWORD;
import static flume.sink.mongo.MongoSinkConstants.USER;
import static flume.sink.mongo.MongoSinkConstants.AUTHENTICALTION;

// import com.sun.org.apache.xpath.internal.operations.String;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoSinkSelf extends AbstractSink implements Configurable {

  private static final Logger logger = LoggerFactory.getLogger(MongoSinkSelf.class);

  private MongoClient client;
  MongoDatabase database;
  // private MongoCollection<Document> collection;
  private List<ServerAddress> seeds;
  private MongoCredential credential;

  private java.lang.String databaseName;
  private java.lang.String collectionName;
  private boolean authentication_enabled;

  private int batchSize = DEFAULT_BATCH_SIZE;
  private boolean DEFAULT_AUTHENTICATION_ENABLED = false;

  Map<java.lang.String, List<Document>> documents_map = new HashMap<java.lang.String, List<Document>>();
  Map<java.lang.String, MongoCollection<Document>> collection_map = new HashMap<java.lang.String, MongoCollection<Document>>();

  private SinkCounter sinkCounter;

  public Status process() throws EventDeliveryException {
    Status status = Status.READY;
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();

    List<Document> documents = null;
    try {
      transaction.begin();

      long count = 0;
      long lastTimestamp = 0;
      String lastPid = "";
      while (true) {
        Event event = channel.take();
        if (event == null) {
          break;
        }

        // 获取消息头
        // 从消息头中获取数据集和文档集
        Map<java.lang.String, java.lang.String> jsonHeader = event.getHeaders();
        try {
          // 获取数据集名称
          java.lang.String collName = jsonHeader.get("collection");
          if (collName != null) {
            if (!collection_map.containsKey(collName)) {
              collection_map.put(collName, database.getCollection(collName));
            }
            if (!documents_map.containsKey(collName)) {
              documents_map.put(collName, new ArrayList<Document>(batchSize));
            }
            documents = documents_map.get(collName);
          }
        } catch (Exception e) {
          logger.error(e.toString());
        }
        // 获取消息体
        java.lang.String jsonEvent = null;
        try {
          jsonEvent = new java.lang.String(event.getBody(), StandardCharsets.UTF_8);
          Document sentEvent = Document.parse(jsonEvent);
          // 检查重复消息问题，通过判断时间戳和pid
          if (lastTimestamp == sentEvent.getLong("timestamp") && lastPid.equalsIgnoreCase(sentEvent.getString("pid"))) {
            logger.error("消息重复: " + jsonHeader.get("type") + "  " + lastPid + "  " + lastTimestamp);
          }
          lastTimestamp = sentEvent.getLong("timestamp");
          lastPid = sentEvent.getString("pid");
          documents.add(sentEvent);
          count++;
        } catch (Exception e) {
          logger.error(e.toString());
        }
      }

      if (count < 1) {
        sinkCounter.incrementBatchEmptyCount();
        // status = Status.BACKOFF;
        // transaction.rollback();
        transaction.commit();
      } else {
        // if (count < batchSize) {
        // // sinkCounter.incrementBatchUnderflowCount();
        // // status = Status.BACKOFF;
        // } else {
        // // sinkCounter.incrementBatchCompleteCount();
        // }

        sinkCounter.addToEventDrainAttemptCount(count);

        int instrtCount = 0;
        for (String key : documents_map.keySet()) {
          if (!collection_map.containsKey(key)) {
            continue;
          }
          List<Document> docs = documents_map.get(key);
          if (docs.size() > 0) {
            try {
              collection_map.get(key).insertMany(docs);
              instrtCount += docs.size();
              docs.clear();
            } catch (Exception ex) {
              logger.error("Exception during insert ", ex);
            }
          }
        }
        transaction.commit();
        sinkCounter.addToEventDrainSuccessCount(instrtCount);
      }
    } catch (Throwable t) {
      try {
        transaction.rollback();
      } catch (Exception e) {
        logger.error("Exception during transaction rollback.", e);
      }

      logger.error("Failed to commit transaction. Transaction rolled back.", t);
      if (t instanceof Error || t instanceof RuntimeException) {
        Throwables.propagate(t);
      } else {
        throw new EventDeliveryException("Failed to commit transaction. Transaction rolled back.", t);
      }
    } finally {
      if (transaction != null) {
        transaction.close();
      }
    }

    return status;
  }

  @Override
  public synchronized void start() {
    logger.info("Starting MongoDB sink");
    // sinkCounter.start();
    try {
      if (authentication_enabled) {
        client = new MongoClient(seeds, Arrays.asList(credential));
        database = client.getDatabase(databaseName);
      } else {
        client = new MongoClient(seeds);
        database = client.getDatabase(databaseName);
      }

      // sinkCounter.incrementConnectionCreatedCount();
    } catch (Exception e) {
      logger.error("Exception while connecting to MongoDB", e);
      // sinkCounter.incrementConnectionFailedCount();
      if (client != null) {
        client.close();
        // sinkCounter.incrementConnectionClosedCount();
      }
    }
    super.start();
    logger.info("MongoDB sink started");
  }

  @Override
  public synchronized void stop() {
    logger.info("Stopping MongoDB sink");
    if (client != null) {
      client.close();
    }
    // sinkCounter.incrementConnectionClosedCount();
    // sinkCounter.stop();
    super.stop();
    logger.info("MongoDB sink stopped");
  }

  public void configure(Context context) {
    authentication_enabled = context.getBoolean(AUTHENTICALTION, DEFAULT_AUTHENTICATION_ENABLED);
    seeds = getSeeds(context.getString(HOSTNAMES));
    if (authentication_enabled) {
      credential = getCredential(context);
    }
    databaseName = context.getString(DATABASE);
    collectionName = context.getString(COLLECTION);
    batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);

    // if (sinkCounter == null) {
    // sinkCounter = new SinkCounter(getName());
    // }
  }

  private List<ServerAddress> getSeeds(java.lang.String seedsString) {
    List<ServerAddress> seeds = new LinkedList<ServerAddress>();
    java.lang.String[] seedStrings = StringUtils.deleteWhitespace(seedsString).split(",");
    for (java.lang.String seed : seedStrings) {
      java.lang.String[] hostAndPort = seed.split(":");
      java.lang.String host = hostAndPort[0];
      int port;
      if (hostAndPort.length == 2) {
        port = Integer.parseInt(hostAndPort[1]);
      } else {
        port = 27017;
      }
      seeds.add(new ServerAddress(host, port));
    }

    return seeds;
  }

  private java.lang.String getCurrentTime() {
    Date dt = new Date();
    SimpleDateFormat matter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    java.lang.String CurrentTime = matter1.format(dt);

    return CurrentTime;
  }

  private MongoCredential getCredential(Context context) {
    java.lang.String user = context.getString(USER);
    java.lang.String database = context.getString(DATABASE);
    java.lang.String password = context.getString(PASSWORD);
    return MongoCredential.createCredential(user, database, password.toCharArray());
  }
}