
package com.click.example;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.Read;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;
import org.joda.time.Duration;
import org.apache.beam.sdk.transforms.Sum;



/*
 * This class will create a data flow to consume a message from a pub/sub 
 * and inserted on a table in BigQuery 
 * 
 * Use below mvn command format to compile and deploy on the container. please edit accordingly
 *
 * **************
 * mvn compile exec:java -e -Dexec.mainClass=com.click.example.PubSubTopicToBigQuery 
 * -Dexec.args="--project=my-project-1-237817 
 * --stagingLocation=gs://adityadataflow/staging/ 
 * --tempLocation=gs://adityadataflow/temp/ 
 * --runner=DataflowRunner"
 * **************
 */

public class PubSubTopicToBigQuery {
  

  public static void main(String[] args) {
    Pipeline p = Pipeline.create(
        PipelineOptionsFactory.fromArgs(args).withValidation().create());
    
    
    //Hard-coded for now.
    String topic = "projects/my-project-1-237817/topics/my-topic";
	String output = "my-project-1-237817:demos.demotable";
	

	TableSchema schema = new TableSchema();
	
	/*
	 * "GetMessages": gets the message as a string from the topic specified above
	 * "ToDBRow": will hold the message before inserting into the above mentioned DB
	 */

	p 
			.apply("GetMessages", PubsubIO.readStrings().fromTopic(topic)) //
			
			.apply("ToDBRow", ParDo.of(new DoFn<String, TableRow>() {
				@ProcessElement
				public void processElement(ProcessContext c) throws Exception {
					
					TableRow row = new TableRow();
					
					row.set("message", c.element());
					c.output(row);
				}
			}))//
			
			
			.apply(BigQueryIO.writeTableRows().to(output)//
					.withSchema(schema)//
					.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
					.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

	p.run();

	
	

  }
}
