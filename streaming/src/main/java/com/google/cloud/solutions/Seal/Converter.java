/**
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.solutions.Seal;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import java.io.IOException;
import java.io.Serializable;
import java.security.GeneralSecurityException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converter works as a streaming Dataflow job to receive temperature, coordinates and other
 * attributes from Cloud Pub/Sub and then, convert and load them into BigQuery.
 * @author teppeiy
 *
 */
public class Converter {
  private static final Logger LOG = LoggerFactory.getLogger(Converter.class);

  private static NetHttpTransport TRANSPORT = new NetHttpTransport();
  private static JacksonFactory JSON_FACTORY = new JacksonFactory();

  /**
   * SealOptions extends DataflowPipelineOptions to retrieve a Pub/Sub topic as a command
   * line argument.
   * @author teppeiy
   *
   */
  public interface SealOptions extends DataflowPipelineOptions {
    String getTopic();

    void setTopic(String topic);
  }

  public Converter() {}

  /** Starts the DataFlow convertor. */
  public void startConverter(SealOptions options) throws IOException {
    final String projectId = options.getProject();
    final String topic = options.getTopic();
    final String datasetId = "SealStg";
    final String tableName = "SensorReadings";

    String id = Long.toString(System.currentTimeMillis());
    options.setJobName("converter-" + id);

    GoogleCredential credential = GoogleCredential.getApplicationDefault(TRANSPORT, JSON_FACTORY);
    Bigquery bigquery = new Bigquery(new NetHttpTransport(), new JacksonFactory(), credential);
    Dataset dataset = new Dataset();
    DatasetReference datasetRef = new DatasetReference();
    datasetRef.setProjectId(projectId);
    datasetRef.setDatasetId(datasetId);
    dataset.setDatasetReference(datasetRef);
    try {
      bigquery.datasets().insert(projectId, dataset).execute();
      LOG.debug("Creating dataset : " + datasetId);
    } catch (IOException e) {
      LOG.debug(datasetId + " dataset already exists.");
    }

    TableReference ref = new TableReference();
    ref.setProjectId(projectId);
    ref.setDatasetId(datasetId);
    ref.setTableId(tableName);

    List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
    fields.add(new TableFieldSchema().setName("deviceid").setType("STRING"));
    fields.add(new TableFieldSchema().setName("DateTime").setType("DATETIME"));
    fields.add(new TableFieldSchema().setName("Temperature").setType("FLOAT"));
    fields.add(new TableFieldSchema().setName("Pressure").setType("FLOAT"));
    fields.add(new TableFieldSchema().setName("Humidity").setType("FLOAT"));
    fields.add(new TableFieldSchema().setName("DewPoint").setType("FLOAT"));
    fields.add(new TableFieldSchema().setName("Latitude").setType("STRING"));
    fields.add(new TableFieldSchema().setName("Longitude").setType("STRING"));
    fields.add(new TableFieldSchema().setName("PowerConsumption").setType("FLOAT"));

    TableSchema schema = new TableSchema().setFields(fields);

    Pipeline p = Pipeline.create(options);
    p.apply("Read From PubSub IO",
            PubsubIO.readStrings()
                .fromTopic("projects/" + options.getProject() + "/topics/" + topic))
        .apply("Applying Windowing", Window.<String>into(FixedWindows.of(Duration.standardSeconds(10))))
        .apply("Apply ParDo", ParDo.of(new RowGenerator()))
        .apply("Write To BQ OP", BigQueryIO.writeTableRows()
                .to(ref)
                .withSchema(schema)
                .withFailedInsertRetryPolicy(InsertRetryPolicy.alwaysRetry())
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
    p.run();
  }

  /**
   * RowGenerator parses a comma separated record and converts it into BigQuery TableRow.
   * @author teppeiy
   *
   */
  public static class RowGenerator extends DoFn<String, TableRow> implements Serializable {
    private static final long serialVersionUID = -1366613943065649148L;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /** Processes an element. */
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      String[] attrs = c.element().split(",");
      LOG.debug(attrs[0]);
      TableRow row =
          new TableRow()
              .set("deviceid", attrs[0])
              .set("DateTime", sdf.format(new Date(Long.parseLong(attrs[1]))))
              .set("Temperature", new Double(attrs[2]))
              .set("Pressure", new Double(attrs[3]))
              .set("Humidity", new Double(attrs[4]))
              .set("DewPoint", new Double(attrs[5]))
              .set("Latitude", attrs[6])
              .set("Longitude", attrs[7])
              .set("PowerConsumption", new Double(attrs[8]));
      c.output(row);
    }
  }

  /** Main entry point. */
  public static void main(String[] args)
      throws IOException, InterruptedException, GeneralSecurityException {
    PipelineOptionsFactory.register(SealOptions.class);
    SealOptions converterOpts = PipelineOptionsFactory.fromArgs(args).as(SealOptions.class);
    Converter bench = new Converter();
    System.out.println("Starting Converter");
    bench.startConverter(converterOpts);
  }
}
