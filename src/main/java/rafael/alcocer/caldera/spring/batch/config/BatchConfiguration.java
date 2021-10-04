/**
 * Copyright [2019] [RAFAEL ALCOCER CALDERA]
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package rafael.alcocer.caldera.spring.batch.config;

import java.net.MalformedURLException;
import java.text.ParseException;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import lombok.Getter;
import lombok.Setter;
import net.sf.json.JSONObject;
import rafael.alcocer.caldera.listener.JobCompletionListener;
import rafael.alcocer.caldera.model.Thing;
import rafael.alcocer.caldera.spring.batch.step.partition.MultiFilePartitioner;
import rafael.alcocer.caldera.spring.batch.step.partition.Processor;
import rafael.alcocer.caldera.spring.batch.step.partition.Writer;

/**
 * For the String array, I couldn't use the "names" as a variable name
 * because I receive this error:
 * 
 * org.springframework.batch.item.file.FlatFileParseException: 
 * Parsing error at line: 3 in 
 * resource=[URL [file:/C:/RAC/workspaceSpringBoot/spring-boot-spring-batch-multifile-partitioner-annotation-based/data/inbound/numbers.txt]], 
 * input=[00001|One|Number One]
 * 
 * This is because Spring Batch has the following:
 * 
 * public abstract class AbstractLineTokenizer implements LineTokenizer {

        protected String[] names = new String[0];
        ...
        
   }
 * 
 * Because of the above I used: "nombres"
 * 
 * @author Rafael Alcocer Caldera
 * @version 1.0
 *
 */
@Getter
@Setter
@EnableBatchProcessing
@Configuration
@ConfigurationProperties("spring.batch.properties")
public class BatchConfiguration {
    
    private String[] nombres;
    private String delimiter;
    private int gridSize;
    private int commitInterval;
    private String dir;
    
    @Autowired
    private ResourcePatternResolver resoursePatternResolver;

    @Autowired
    private JobBuilderFactory job;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    
    // Partition Job
    @Bean(name = "partitionJob")
    public Job partitionerJob() throws UnexpectedInputException, MalformedURLException, ParseException {
        return job.get("partitionJob")
          .start(partitionMasterStep())
          .listener(jobCompletionListener())
          .build();
    }
    
    // Partition Master Step
    @Bean
    public Step partitionMasterStep() throws UnexpectedInputException, MalformedURLException, ParseException {
        return stepBuilderFactory.get("partitionSlaveStep")
                .partitioner("multiFilePartitioner", multiFilePartitioner())
                .step(partitionSlaveStep())
                .taskExecutor(taskExecutor())
                .gridSize(gridSize)
                .build();
    }
    
    // Partition Slave Step
    @Bean
    public Step partitionSlaveStep() throws UnexpectedInputException, MalformedURLException, ParseException {
        return stepBuilderFactory.get("partitionSlaveStep")
                .<Thing, JSONObject>chunk(commitInterval) // => commit-interval
                .reader(itemReader(null))
                .processor(itemProcessor())
                .writer(itemWriter())
                .build();
    }

    @Bean
    public MultiFilePartitioner multiFilePartitioner() {
        MultiFilePartitioner multiFilePartitioner = new MultiFilePartitioner();
        multiFilePartitioner.setInboundDir(dir);
        
        return multiFilePartitioner;
    }

    @StepScope
    @Bean
    public FlatFileItemReader<Thing> itemReader(@Value("#{stepExecutionContext['fileResource']}") Resource resource) {
        FlatFileItemReader<Thing> itemReader = new FlatFileItemReader<>();
        
        DefaultLineMapper<Thing> lineMapper = new DefaultLineMapper<Thing>() {
            {
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {
                        setDelimiter(delimiter);
                        setNames(nombres);
                    }
                });
                setFieldSetMapper(new BeanWrapperFieldSetMapper<Thing>() {
                    {
                        setTargetType(Thing.class);
                    }
                });
            }
        };
        
        itemReader.setResource(resource);
        itemReader.setLineMapper(lineMapper);
        itemReader.setLinesToSkip(2);
        
        return itemReader;
    }
    
    @StepScope
    @Bean
    public Processor itemProcessor() {
        return new Processor();
    }
    
    @StepScope
    @Bean
    public Writer itemWriter() {
        return new Writer();
    }
    
    @Bean
    public TaskExecutor taskExecutor() {
        return new SimpleAsyncTaskExecutor();
    }
    
    @Bean
    public JobCompletionListener jobCompletionListener( ) {
        return new JobCompletionListener();
    }
}