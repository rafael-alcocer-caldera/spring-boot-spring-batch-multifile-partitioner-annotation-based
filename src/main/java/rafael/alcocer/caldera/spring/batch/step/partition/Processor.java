/**
 * Copyright [2017] [RAFAEL ALCOCER CALDERA]
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

import org.springframework.batch.item.ItemProcessor;

import net.sf.json.JSONObject;
import rafael.alcocer.caldera.model.Thing;

/**
 * According to the chunk size (commit-interval) value, this will define the
 * number of items to process before pass to the Writer.
 * 
 * @author Rafael Alcocer Caldera
 * @version 1.0
 *
 */
public class Processor implements ItemProcessor<Thing, JSONObject> {

    public JSONObject process(Thing thing) throws Exception {
        System.out.println("##### Processor... process()... thing.getName(): " + thing.getName());

        JSONObject jsonObject = JSONObject.fromObject(thing);
        System.out.println("##### Processor... process()... jsonObject.toString(): " + jsonObject.toString());

        return jsonObject;
    }
}