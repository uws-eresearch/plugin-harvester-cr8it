/**
 * 
 */
package au.edu.uws.eresearch.fascinator.plugins;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.edu.uws.eresearch.fascinator.harvester.BaseCr8itHarvester;

import com.googlecode.fascinator.api.PluginException;
import com.googlecode.fascinator.api.PluginManager;
import com.googlecode.fascinator.api.harvester.Harvester;
import com.googlecode.fascinator.api.indexer.Indexer;
import com.googlecode.fascinator.api.storage.Storage;
import com.googlecode.fascinator.common.JsonSimpleConfig;
import com.googlecode.fascinator.common.messaging.GenericListener;
import com.googlecode.fascinator.common.messaging.MessagingException;
import com.googlecode.fascinator.common.messaging.MessagingServices;

/**
 * @author lloyd harischandra
 *
 */
public class Cr8itHarvestQueueConsumer implements GenericListener {

	/** Service Loader will look for this */
    public static final String LISTENER_ID = "cr8itHarvester";
	
	/** JSON configuration */
    private JsonSimpleConfig globalConfig;
    
    /** JMS connection */
    private Connection connection;

    /** JMS Session */
    private Session session;
    
    private MessageConsumer consumer;
    
    private MessageProducer producer;
    
	private JsonSimpleConfig config;
	
	/** Name identifier to be put in the queue */
	private String name;
	
	/** Render queue string */
	private String QUEUE_ID;
	
	/** Harvest Event topic */
	private String EVENT_TOPIC_ID;
	
	/** Thread reference */
	private Thread thread;
	
	/** Indexer object */
    private Indexer indexer;
    
    /** Storage */
    private Storage storage;
    
    /** Messaging services */
    private MessagingServices messaging;
    
    private Map<String, Harvester> harvesters;
    
    /** Logging */
    private static Logger log = LoggerFactory.getLogger(Cr8itHarvestQueueConsumer.class);

    public Cr8itHarvestQueueConsumer() {
   	 	thread = new Thread(this, LISTENER_ID);    	 
    }
	
	/* (non-Javadoc)
	 * @see javax.jms.MessageListener#onMessage(javax.jms.Message)
	 */
	@Override
	public void onMessage(Message message) {
		// TODO get the message and invoke harvester to harvest

	}

	
	@Override
	public void run() {
		
		//This is where queueing happens
		try {
			// Get a connection to the broker
	        String brokerUrl = globalConfig.getString(
	                ActiveMQConnectionFactory.DEFAULT_BROKER_BIND_URL,
	                "messaging", "url");
	        ActiveMQConnectionFactory connectionFactory =
	                new ActiveMQConnectionFactory(brokerUrl);
	        
			connection = connectionFactory.createConnection();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			
			consumer = session.createConsumer(session.createQueue(QUEUE_ID));
			consumer.setMessageListener(this);
			
			producer = session.createProducer(session.createTopic(EVENT_TOPIC_ID));
			producer.setDeliveryMode(DeliveryMode.PERSISTENT);        
			
			connection.start();
			log.info("'{}' is running...", name);   
				
		} catch (JMSException e) {
			e.printStackTrace();
		}
        

	}

	
	@Override
	public void init(JsonSimpleConfig config) throws Exception {
		this.config = config;
		name = config.getString(null, "config", "name");
        QUEUE_ID = name;
        EVENT_TOPIC_ID = QUEUE_ID + "_event";
        thread.setName(name);
        File sysFile = null;
        
        try {
            globalConfig = new JsonSimpleConfig();
            sysFile = JsonSimpleConfig.getSystemFile();

            // Load the indexer plugin
            String indexerId = globalConfig.getString(
                    "solr", "indexer", "type");
            if (indexerId == null) {
                throw new Exception("No Indexer ID provided");
            }
            indexer = PluginManager.getIndexer(indexerId);
            if (indexer == null) {
                throw new Exception("Unable to load Indexer '"+indexerId+"'");
            }
            indexer.init(sysFile);

            // Load the storage plugin
            String storageId = globalConfig.getString(
                    "file-system", "storage", "type");
            if (storageId == null) {
                throw new Exception("No Storage ID provided");
            }
            storage = PluginManager.getStorage(storageId);
            if (storage == null) {
                throw new Exception("Unable to load Storage '"+storageId+"'");
            }
            storage.init(sysFile);

            harvesters = PluginManager.getHarvesterPlugins();
            log.debug("Dumping harvesters:" + harvesters.size());
			for (String hid : harvesters.keySet()) {
				Harvester hv = harvesters.get(hid);
				log.debug("Harvester id: " + hid + ", Name: " +hv.getName());
				String type = hid;
				if (hv instanceof BaseCr8itHarvester) {
					BaseCr8itHarvester harvester = (BaseCr8itHarvester) hv;
					harvester.setStorage(storage);
					harvester.setIndexer(indexer);
					String configFilePath = globalConfig.getString(null, "portal", "harvestFiles") + "/" + type + ".json";
					File harvestConfigFile = new File(configFilePath);
					if (!harvestConfigFile.exists()) {
						log.error("Harvest config file not found '"+configFilePath+"', please check the set up.");
						continue;
					}
					log.info("Using config file path:" + configFilePath);					
					harvester.init(harvestConfigFile);					
				}
			}
        } catch (IOException ioe) {
            log.error("Failed to read configuration: {}", ioe.getMessage());
            throw ioe;
        } catch (PluginException pe) {
            log.error("Failed to initialise plugin: {}", pe.getMessage());
            throw pe;
        }

        try {
            messaging = MessagingServices.getInstance();
        } catch (MessagingException ex) {
            log.error("Failed to start connection: {}", ex.getMessage());
            throw ex;
        }
        log.debug("JsonHarvester initialised.");

	}

	
	@Override
	public String getId() {
		return LISTENER_ID;
	}

	
	@Override
	public void start() throws Exception {
		log.info("Starting {}...", name);
        thread.start();
	}

	
	@Override
	public void stop() throws Exception {
		log.info("Stopping {}...", name);
        if (indexer != null) {
            try {
                indexer.shutdown();
            } catch (PluginException pe) {
                log.error("Failed to shutdown indexer: {}", pe.getMessage());
                throw pe;
            }
        }
        if (storage != null) {
            try {
                storage.shutdown();
            } catch (PluginException pe) {
                log.error("Failed to shutdown storage: {}", pe.getMessage());
                throw pe;
            }
        }
        if (producer != null) {
            try {
                producer.close();
            } catch (JMSException jmse) {
                log.warn("Failed to close producer: {}", jmse);
            }
        }
        if (consumer != null) {
            try {
                consumer.close();
            } catch (JMSException jmse) {
                log.warn("Failed to close consumer: {}", jmse.getMessage());
                throw jmse;
            }
        }
        if (session != null) {
            try {
                session.close();
            } catch (JMSException jmse) {
                log.warn("Failed to close consumer session: {}", jmse);
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (JMSException jmse) {
                log.warn("Failed to close connection: {}", jmse);
            }
        }
        if (messaging != null) {
            messaging.release();
        }
	}

	
	@Override
	public void setPriority(int newPriority) {
		if (newPriority >= Thread.MIN_PRIORITY
                && newPriority <= Thread.MAX_PRIORITY) {
            thread.setPriority(newPriority);
        }
	}

}
