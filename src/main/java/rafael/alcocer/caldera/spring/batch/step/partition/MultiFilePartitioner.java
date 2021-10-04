/**
 * Copyright [2019] [RAFAEL ALCOCER CALDERA]
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 	http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package rafael.alcocer.caldera.spring.batch.step.partition;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

/**
 * This Partitioner acts as a Master Step which contains the inbound directory.
 * 
 * This Partitioner returns a Map which contains the ExecutionContext.
 * 
 * This Partitioner will list all the files found in the inbound directory and
 * for
 * 
 * each file, it will create an ExecutionContext.
 * 
 * Each ExecutionContext created will store the file name to be processed.
 * 
 * @author Rafael Alcocer Caldera
 * @version 1.0
 *
 */
public class MultiFilePartitioner implements Partitioner {

    private static final Logger logger = LoggerFactory.getLogger(MultiFilePartitioner.class);
    private String inboundDir;

    public Map<String, ExecutionContext> partition(int gridSize) {
        logger.info("##### MultiFilePartitioner...");
        logger.info("##### MultiFilePartitioner... inboundDir: " + inboundDir);

        Map<String, ExecutionContext> partitionMap = new HashMap<String, ExecutionContext>();
        File dir = new File(inboundDir);
        logger.info("##### MultiFilePartitioner... dir.isDirectory(): " + dir.isDirectory());

        if (dir.isDirectory()) {
            File[] files = dir.listFiles();

            for (File file : files) {
                logger.info("##### MultiFilePartitioner... file: " + file);
                ExecutionContext context = new ExecutionContext();
                context.put("fileResource", file.toURI().toString());
                partitionMap.put(file.getName(), context);
            }
        }

        return partitionMap;
    }

    public String getInboundDir() {
        return inboundDir;
    }

    public void setInboundDir(String inboundDir) {
        this.inboundDir = inboundDir;
    }
}
