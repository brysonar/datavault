package org.datavaultplatform.worker;

import org.datavaultplatform.worker.queue.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class WorkerInstance {

	private static final Logger logger = LoggerFactory.getLogger(WorkerInstance.class);
	

    public static void main(String [] args) {
        
        // Bind $DATAVAULT_HOME to a system variable for use by Log4j
        System.setProperty("datavault-home", System.getenv("DATAVAULT_HOME"));

        logger.info("Worker starting");
        
        ApplicationContext context = new ClassPathXmlApplicationContext("datavault-worker.xml");

        MqReceiver mqReceiver = context.getBean(MqReceiver.class);
        
        // Listen to the message queue ...
        try {
        	mqReceiver.receive();
        } catch (Exception e) {
            logger.error("Error in receive", e);
        }
    }
    
    public static String getWorkerName() {
        return java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
    }
    
}
