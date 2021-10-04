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

import java.util.List;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;

import net.sf.json.JSONObject;

/**
 * 
 * @author Rafael Alcocer Caldera
 * @version 1.0
 *
 */
public class Writer implements ItemWriter<JSONObject> {

    private StepExecution stepExecution;

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
    }

    public void write(List<? extends JSONObject> items) throws Exception {
        System.out.println("##### Writer... write()... Executing: " + stepExecution.getStepName());
        System.out.println("##### Writer... write()... items: " + items);
        System.out.println("##### Writer... write()... items.size(): " + items.size());
    }
}
