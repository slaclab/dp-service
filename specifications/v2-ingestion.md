## Overview

The MLDP data ingestion gRPC API is used by clients for uploading particle accelerator instrument data to a data archive, for later in use machine learning and data-driven applications.  The data is organized by "process variable" (PV), corresponding to a readout from some instrument that we want to save in the archive.  The main use case for the ingestion API is to send PV time-series data structured in batches or "buckets".

Each ingestion request primarily includes an "IngestionDataFrame" message that includes 1) a DataTimestamps message specifying either a time range or explicit list of timestamps, and 2) PV time-series data column vectors containing a sample value for each timestamp.

The original ingestion API definition is sample-oriented.  The main data structures are the DataColumn, which is a PV data vector represented as a list of DataValue messages.  The motivation for using the DataValue message is to allow the API to handle a wide range of heterogeneous data types including simple scalar values as well as more complex data like arrays, structures, and images.  Below is the basic proto definitions for each:

```
message DataColumn {
  string name = 1; // Name of PV.
  repeated DataValue dataValues = 2; // List of heterogeneous column data values.
}
```

```
message DataValue {
  oneof value {
    string		stringValue = 1;		// character string
    bool		booleanValue = 2;		// logical Boolean
    uint32		uintValue = 3;			// unsigned integer value
    uint64		ulongValue = 4;			// unsigned long integer
    sint32		intValue = 5;			// signed integer value
    sint64		longValue = 6;			// signed long integer
    float		floatValue = 7;			// 32 byte float value
    double		doubleValue = 8;		// 64 byte float value (double)
    bytes		byteArrayValue = 9;		// raw data as byte string
    Array		arrayValue = 10;			// heterogeneous array (no dimensional restrictions as of yet)
    Structure	structureValue = 11;		// general data structure (no width or depth restrictions yet)
    Image		imageValue = 12;		// general image value
    Timestamp timestampValue = 13; // timestamp data value
  }
}
```

We are worried about the JVM memory allocation behavior for the backend service that handles the ingestion requests.  For example, if samples are batched into buckets containing 1000 values, the JVM will allocate memory for each sample value contained in that request.  Since our baseline case is to handle 4000 PVs each at 1 KHz (1000 samples per second), we are worried about memory allocation and garbage collection being a long term issue for the service.

To that end, we are changing the API data structures to be more column-oriented instead of sample-oriented.  For example, the new "DoubleColumn" message would be used to ingest a vector of double column values.  When a request arrives in the service, the values for the DoubleColumn will be read as a Java primitive array of doubles, with no per-sample memory allocation.  The proto definition for DoubleColumn is shown below:

```
message DoubleColumn {
  string name = 1; // PV name
  repeated double values = 2 [packed = true];
}
```

There are other similar new data structures for scalar sample values including:

* FloatColumn
* Int64Column
* Int32Column
* BoolColumn
* StingColumn
* EnumColumn

Some PV sample values use more complex data types like arrays, images, and structures.  These are handled by the following new data structures:

* DoubleArrayColumn
* FloatArrayColumn
* Int32ArrayColumn
* Int64ArrayColumn
* BoolArrayColumn
* ImageColumn
* StructColumn
* SerializedDataColumn

The proto definition for DoubleArrayDataColumn is shown below:

```
message DoubleArrayColumn {
  string name = 1; // PV name
  ArrayDimensions dimensions = 2;
  // Flattened: sample_count × product(dims)
  repeated double values = 3 [packed = true];
}
```

So the bottom line is that we are accomplishing the heterogeneity at the column level instead of the sample value.  This makes logical sense because we don't expect the data type for a given PV to change from sample to sample.  Coupled with the improvement in memory efficiency for both ingestion clients and the backend service handling, this is the motivation for the significant change to the ingestion API data structures.

To incorporate the new data structures in a backward compatible way, we modified the payload of the "IngestionDataFrame" message.  Previously the IngestionDataFrame could contain a list of DataColumn messages.  Now it can also include lists of each of the other column message data structures.  We are deprecating support for ingestion of DataColumn / DataValue in the current 1.13 release, and will be removing that support in the 2.0 release.

The full MLDP API definition is contained in the dp-grpc repository, which is cloned on this development machine to the following directory: ~/dp.fork/dp-java/dp-grpc/src/main/proto.

## Tasks

### 1.0 Ingestion Validation

The first task in implementing the ingestion API changes is to add validation for incoming requests.  Incoming requests are handled by the IngestionServiceImpl via one of the three service API methods: ingestData(), ingestDataStream(), and ingestDataBidiStream().  For validation, there is a common static method IngestionValidationUtility.validateIngestionRequest().  

The existing validation is fairly lightweight.  There are checks that providerId and requestId are provided, and that the request contains data to ingest (e.g., that numRequestRows and numRequestColumns are non-zero).

We want to add full validation for the new ingestion API data structures and the corresponding design assumptions.  The approach should be layered, and we want to fail early, cheaply, and deterministically.  Here are the validation rules that we want to enforce in IngestionValidationUtility.validateIngestionRequest().  We can break into sub-methods if that is useful.

### 1.0.1 Request / Frame Level Validation

Here are the request level checks that we want to perform

* providerid is specified and valid
* clientRequestId is specified
* IngestionDataFrame.dataTimestamps is provided
* DataTimestamps validation
  * DataTimestamps is exactly one of SamplingClock or explicit timestamps list
  * if sampling clock
    * sampleCount > 0
    * samplePerionNs > 0
    * start time seconds and nanos are valid
  * if explicit timestamps list
    * list length > 0
    * all timestamps valid
    * timestamps are strictly increasing or at least non-decreasing
    * sampleCount = timestamps.length

### 1.0.2 Column Level Validation

Validation for every column regardless of type:

* name is non-empty
* one column per PV name per IngestionDataFrame
* column type is valid
* column values.length matches sampleCount from DataTimestamps

### 1.0.3 Scalar Column Validation

For the scalar column data types (Applies to: DoubleColumn, FloatColumn, Int32Column, Int64Column, BoolColumn, EnumColumn, StringColumn payloads):

* values.length == sampleCount

For StringColumn only:

* max string length is 256 characters

### 1.0.4 Array Column Validation

For the array data types (DoubleArrayColumn, FloatArrayColumn, Int32ArrayColumn, Int64ArrayColumn, BoolArrayColumn):

* dimensions.dims.size() is in {1, 2, 3}
* all dimension values > 0
* where element_count = product(dimensions):
  * element count < configured maximum (let's use a constant of 10 million, and make this configurable by column type later)
  * values.length == sampleCount * element_count

### 1.0.5 ImageColumn Validation

* descriptor is specified
* width > 0 ; height > 0 ; channels > 0
* for each image payload
  * size < max configured per-image size (let's use a constant for now and make configurable later)
  * encoding is valid

### 1.0.6 StructColumn Validation

* schemaId is specified
* values.length == sampleCount
* struct payload size is less than configured max per-struct (let's use a constant for now and make configurable later)

### 1.0.7 SerializedDataColumn Validation

* encoding is specified

### clarifications

1. Ignore the GridFS part, I meant to delete it.
2. For now, just ensure the enumId is non-empty.  We don't want to place unneccesary constraints on the domain of values because that is external to the API and varies by client.  We might later add a per-pv configuration
   / registration that specifies thanks like valid enumIds.
3. Image encoding is defined external to the API because it varies by facility.  It is a contract between data producer and data consumer.  Just check that it is non-empty.
4. It's not a bad idea to maintain separate validation paths for legacy DataColumn vs. the new types.  How do you propose to handle that?
5. Yes please include field paths and values in error messages.

### 1.1 Ingestion Validation Test Coverage

Please add comprehensive test coverage for all failure scenarios and new proto column message data structures.

### 2.0 Ingestion Performance Benchmark

I've added a framework to the Ingestion Service handler that reflects the new column-oriented data structures in the gRPC ingestion API in writing BucketDocuments to MongoDB for each column contained in an IngestDataRequest's IngestionDataFrame.  The BucketDocuments now include an embedded document with the sample data values for the corresponding request column.  There is a Java POJO class hierarchy for the embedded column documents, ColumnDocumentBase, with a derived class DoubleColumnDocument to serve as the embedded column document when creating a BucketDocument for a protobuf DoubleColumn message.  I also refactored the existing code to make the previously existing DataColumnDocument extend ColumnDocumentBase.  A BucketDocument containing a DataColumnDocument is created when the ingestion request data frame includes a DataColumn protobuf message.  I will add classes to the ColumnDocumentBase hierarchy to support the other protobuf column messages as a follow on task.

Before I move forward with handling the other protobuf column messages, I want to make a benchmark for comparing the performance of ingestion using the DataColumn / DataValue protobuf messages (with double sample data values) from the original implementation with ingestion using the new DoubleColumn protobuf message.

There is already an ingestion performance benchmark framework for measuring the performance of ingestion using DataColumn / DataValue protobuf messages.  There are 3 variant ingestion benchmark applications that extend IngestionBenchmarkBase - BenchmarkIngestDataStream, BenchmarkIngestDataBidiStream, and BenchmarkIngestDataStreamBytes that cover the ingestDataStream(), ingestDataBidiStream(), and ingestDataStream() with SerializedDataColumns, respectively.

Please investigate and give me an overview of what would be required to refactor the framework so that we can pass a flag from BenchmarkIngestDataBidiStream.main() and BenchmarkIngestDataStream.main() that tells the framework to build IngestDataRequests whose IngestionDataFrame uses DoubleColumn protobuf objects instead of DataColumn / DataValue objects.  We don't want to change any code yet, just give me an approach.

It looks like one idea might be to refactor BenchmarkIngestDataBidiStream and BenchmarkIngestDataStream to be intermediate base classes, each with 2 derived classes that define a main method that uses either DataColumn ingestion or DoubleColumn ingestion.  But if you have better ideas let me know.

### 2.1 Benchmark Refactoring Using Strategy Pattern and Factory Method

I agree with your recommendation in option 1 to use the strategy pattern and factory method.  Please proceed with the suggested implementation steps:

1. Create ColumnDataType enum and ColumnBuilder interface
2. Implement DataColumnBuilder (extract existing logic)
3. Implement DoubleColumnBuilder (new logic for DoubleColumn)
4. Update IngestionTaskParams to include ColumnDataType
5. Refactor buildDataTableTemplate() to use strategy pattern
6. Update benchmark main() methods to parse column type from args
7. Add new DoubleColumnBuilder implementation:

### 3.0 Parameterized Intermediate Base Class for Scalar Columns

Please design a generic intermediate base class extending ColumnDocumentBase, "ScalarColumnDocumentBase", with a parameter for the scalar object type contained by the column.  For example, we will change the new DoubleColumnDocument to extend ScalarColumnBase and the type parameter will be "Double".  Move the "values" instance variable up from DoubleColumnDocument to the new base class and make the List of values use the parameter type as the List type.  Consider moving other methods up to the base class if their implementation is generic, or could be generic.  Don't change any code yet, just propose a design and please feel free to make suggestions for improvement to the approach.

The relevant proto column data structure messages are DoubleColumn, FloatColumn, Int64Column, Int32Column, BoolColumn, StringColumn, and EnumColumn.

### 4.0 Handling for Additional Protobuf Column Messages

I've now added full handling for the new DoubleColumn protobuf data type in the MLDP service implementations, and I think I have a handle on what needs to be done for adding handling for additional column data types. Here are the tasks for adding handling for a new protobuf column data type:

1. Add BSON POJO class that extends ColumnDocumentBase for the protobuf column data type (like DoubleColumnDocument)
- add discriminator e.g., @BsonDiscriminator(key = "_t", value = "doubleColumn")
- add instance variables for field values with accessor methods
- add static method like DoubleColumnDocument.fromDoubleColumn(DoubleColumn) that returns a new instance of the document class (as ColumnDocumentBase)
- add method like DoubleColumnDocument.toDoubleColumn() that creates the protobuf data type from the BSON document instance
- implment required abstract methods including 1) addColumnToBucket() to add the appropriate protobuf message to the supplied DataBucket.Builder; 2) toDataColumn() to create and return a protobuf DataColumn that contains DataValues appropriate for the document class (this is used for tabular query and export etc, and some classes may need to throw an exception if they aren't compatible with DataColumn / DataValue); 3) toByteArray() for hdf5 export etc.

2. Modify BucketDocument.generateBucketsFromRequest() to handle the new column data type from the IngestDataRequest (following the pattern of handling for DoubleColumn).

3. Modify MongoClientBase.getPojoCodecRegistry() to add an entry for the new BSON document class.

4. Add handling to the data subscription framework by adding handling for the new protobuf column data type to SourceMonitorManager.publishDataSubscriptions().

5. Add handling to the data event subscription framework by 1) updating the main ColumnTriggerUtility.checkColumnTrigger(PvConditionTrigger, DataBucket) variant to handle the DataBucket.dataCase switch statement to handle the new column data type by dispatching to a new checkColumnTrigger() variant for handling the new column data type; 2) adding the new ColumnTriggerUtility.checkColumnTrigger() variant for the new protobuf column data type; and 3) adding a case to DataBuffer.estimateDataSize() for the new protobuf column data type.

6. Add integration test framework support for the new protobuf column data type (follow the pattern for DoubleColumn).
- Add a field to IngestionTestBase.IngestionRequestParams for the list of protobuf column messages for the new data type to be added to the IngestDataRequest.
- Modify IngestionTestBase.buildIngestionRequest() to handle adding the list of columns of the new data type to the IngestDataRequest's DataFrame (follow the example of DoubleColumn handling).
- Add verification logic to GrpcIntegrationIngestionServiceWrapper.verifyIngestionRequestHandling() for the new column data type, as appropriate (follow the example of DoubleColumn handling).

7. Add a new integration test covering use of the new column data type in the MLDP APIs, following the pattern of DoubleColumnIT.  Note:
- Coverage of tabular export mechanism is not necessary in new integration tests since that framework is covered pretty well (this is the last section of DoubleColumnIT), so please don't include this in integration tests for other protobuf column data types.
- Coverage of the data event subscription framework will vary for different sets of protobuf column data types.  We should add coverage for scalar / simple valued columns since they can be used as both trigger and target PVs in subscriptions.  Array / complex valued columns can only be used as target PVs, so we will need to think about how best to cover those situations, or not cover them at all. 
- Please follow the pattern of DoubleColumnIT as closely as possible, as there was significant work in the framework to support the pattern used by the test.

## 4.1 Add Handling for the Protobuf FloatColumn Message Data Type

The steps for adding handling for a new protobuf column data type to the MLDP services are enumerated above, under section 4.0 of this document.  We are going to add handling for the protobuf FloatColumn data type (defined in ~/dp.fork/dp-java/dp-grpc/src/main/proto/common.proto).  Since this is the first attempt at adding handling for a new column data type, let's complete the work in steps.  The steps are defined in the following subsections.

### 4.1.1 Add ingestion framework support for FloatColumn

We will first complete steps 1 through 3 listed under section 4 to add Ingestion Service support for handling protobuf FloatColumn data.  This includes creating the BSON POJO class, including the column data type in generating BucketDocuments for the request, and modifying the POJO codec registry to add the new class.

### 4.1.2 Data subscripiton and data event subscription handling for FloatColumn

We will next complete steps 4 and 5 listed under section "4.0 Handling for Additional Protobuf Column Messages" for adding data subscription and data event subscription handling for the new protobuf FloatColumn.  This consists of updating the SourceMonitorManager, ColumnTriggerUtility, and DataBuffer as described.

### 4.1.3 Integration test coverage for FloatColumn

Finally, we will complete steps 6 and 7 under section "4.0 Handling for Additional Protobuf Column Messages" for adding integration test framework support and integration test coverage for use of FloatColumn in the MLDP APIs, respectively, as described in the details for those steps.  Regarding step 7, we SHOULD provide coverage of both data subscription and data event subscription following the example of DoubleColumnIT, since FloatColumns might be used for both trigger and target PVs in data event subscriptions.

### 4.2.1 Add handling for the protobuf Int64Column message data type

Next, we will add MLDP handling for the Int64Column message defined in ~/dp.fork/dp-java/dp-grpc/src/main/proto/common.proto.  We will first add handling support to the MLDP services as described in steps 1 through 5 under section "4.0 Handling for Additional Protobuf Column Messages".  We will add test coverage as a folow up task.

### 4.2.2 Add integration test coverage for Int64Column

Please complete steps 6 and 7 under section "4.0 Handling for Additional Protobuf Column Messages" for adding integration test framework support and coverage for the protobuf Int64Column.  It is very important to follow the pattern of DoubleColumnIT as closely as possible, you'll save both of us extra work and thinking.  It should mostly be a matter of changing the code to build Int64Columns instead of DoubleColumns, and of course using long data values instead of doubles.  Pretty much everything else could stay the same.  Please skip the coverage of tabular data export at the end of the test, but include everything else including data subscription and data event subscription.

### 4.3.1 Add handling for the protobuf Int32Column message data type

Next, we will add MLDP handling for the Int32Column message defined in ~/dp.fork/dp-java/dp-grpc/src/main/proto/common.proto.  We will first add handling support to the MLDP services as described in steps 1 through 5 under section "4.0 Handling for Additional Protobuf Column Messages".  We will add test coverage as a folow up task.

### 4.3.2 Add integration test coverage for Int32Column

Please complete steps 6 and 7 under section "4.0 Handling for Additional Protobuf Column Messages" for adding integration test framework support and coverage for the protobuf Int32Column.  It is very important to follow the pattern of DoubleColumnIT as closely as possible, you'll save both of us extra work and thinking.  It should mostly be a matter of changing the code to build Int64Columns instead of DoubleColumns, and of course using long data values instead of doubles.  Pretty much everything else could stay the same.  Please skip the coverage of tabular data export at the end of the test, but include everything else including data subscription and data event subscription.

### 4.4.1 Add handling for the protobuf BoolColumn message data type

Next, we will add MLDP handling for the BoolColumn message defined in ~/dp.fork/dp-java/dp-grpc/src/main/proto/common.proto.  We will first add handling support to the MLDP services as described in steps 1 through 5 under section "4.0 Handling for Additional Protobuf Column Messages".  We will add test coverage as a folow up task.

### 4.4.2 Add integration test coverage for BoolColumn

Please complete steps 6 and 7 under section "4.0 Handling for Additional Protobuf Column Messages" for adding integration test framework support and coverage for the protobuf BoolColumn.  It is very important to follow the pattern of DoubleColumnIT as closely as possible, you'll save both of us extra work and thinking.  It should mostly be a matter of changing the code to build BoolColumn instead of DoubleColumn, and of course using bool data values instead of doubles.  Pretty much everything else could stay the same.  Please skip the coverage of tabular data export at the end of the test, but include everything else including data subscription and data event subscription.

### 4.5.1 Add handling for the protobuf StringColumn message data type

Next, we will add MLDP handling for the StringColumn message defined in ~/dp.fork/dp-java/dp-grpc/src/main/proto/common.proto.  We will first add handling support to the MLDP services as described in steps 1 through 5 under section "4.0 Handling for Additional Protobuf Column Messages".  We will add test coverage as a folow up task.

Let's discuss whether it makes sense to treat this as a scalar column (by extending ScalarColumnDocumentBase), or if it is somehow different.  One difference at the MongoDB level is that we want to index the individual data values for the other scalar column data types, but probably not for StringColumn data.

### 4.5.2 Add integration test coverage for StringColumn

Please complete steps 6 and 7 under section "4.0 Handling for Additional Protobuf Column Messages" for adding integration test framework support and coverage for the protobuf StringColumn.  It is very important to follow the pattern of DoubleColumnIT as closely as possible, you'll save both of us extra work and thinking.  It should mostly be a matter of changing the code to build StringColumn instead of DoubleColumn, and of course using bool data values instead of doubles.  Please skip the coverage of tabular data export at the end of the test.  Please include coverage of data subscription and data event subscription. 

### 4.6.1 Add handling for the protobuf EnumColumn message data type

Next, we will add MLDP handling for the EnumColumn message defined in ~/dp.fork/dp-java/dp-grpc/src/main/proto/common.proto.  We will first add handling support to the MLDP services as described in steps 1 through 5 under section "4.0 Handling for Additional Protobuf Column Messages".  We will add test coverage as a folow up task.

This one is another special case, but it also seems like it should extend ScalarColumnDocument because it uses packed int32 values.  But one difference to the other scalar columns is that there is an additional parameter for the enumId String.  Since ScalarColumnDocumentBase is parameterized, how is this going to work for EnumColumn?  Let's discuss before we decide on the approach.

### 4.6.2 Add integration test coverage for EnumColumn

Please complete steps 6 and 7 under section "4.0 Handling for Additional Protobuf Column Messages" for adding integration test framework support and coverage for the protobuf EnumColumn.  It is very important to follow the pattern of DoubleColumnIT as closely as possible, you'll save both of us extra work and thinking.  It should mostly be a matter of changing the code to build EnumColumn instead of DoubleColumn, and of course using bool data values instead of doubles.  Please skip the coverage of tabular data export at the end of the test.  Please include coverage of data subscription and data event subscription.

### 5.0 handling for protobuf array column data types

Next we are going to add support for the protobuf column messages that arrays of the corresponding scalar data types.  There are some slight changes to the tasks for adding handling for these column types.

In the initial array column handling implementation, I'm thinking we should add a new intermediate base class ArrayColumnDocumentBase (like ScalarColumnDocumentBase) to be the base class of the new BSON document classes.  The protobuf array column messages all contain an ArrayDimensions message that specifies the array dimensions (up to 3 dimensions) using a list of uint32 values for x, y, and z dimensions, so that could be handled in the base class.

The list of values for each array type is in flattened row-major order.  My hunch is to store values as a binary blob, instead of as transparent values since we don't need to index the individual values for query by value purposes, and the individual values might be arbitrarily large.  The code for ingestion could look something like this (for DoubleArrayColumn):
```
Internal.DoubleList values = column.getValuesList();

int elementCount = product(dimensions);
int sampleCount = ...;

int total = elementCount * sampleCount;

ByteBuffer buffer = ByteBuffer.allocate(total * 8)
                               .order(ByteOrder.LITTLE_ENDIAN);

for (int i = 0; i < values.size(); i++) {
    buffer.putDouble(values.getDouble(i));
}

byte[] binary = buffer.array();
```

Because we plan to eventually (but not initially) use MongoDB GridFS to handle individual data values that exceed the 16MB BSON object size limit, we should design the database schema in a way that supports this.  I'm thinking a "storage" field in the document, with a "kind" that can specify "inline" or "gridfs" with details relevant to that storage mechanism, e.g., for an array it would look like this:

```
{
  "pv": "BPM:01:WAVEFORM",
  "type": "double_array",
  "payload": {
    "dimensions": [1024],
    "storage": {
      "kind": "inline",
      "data": BinData(...)
    }
  }
}
```

And for GridFS it could look like this:
```
{
  "pv": "BPM:01:WAVEFORM",
  "type": "double_array",
  "payload": {
    "dimensions": [1024],
    "storage": {
      "kind": "gridfs",
      "file_id": ObjectId("..."),
      "size_bytes": 12345678
    }
  }
}
```

So a unified model could look like this:
```
"storage": {
  "kind": "inline" | "gridfs",
  "data": BinData(...),          // if inline
  "file_id": ObjectId(...),      // if gridfs
  "size_bytes": 12345678         // optional but useful
}
```

with storage code like this:
```
if (payloadSize <= INLINE_LIMIT) {
    storage.kind = "inline";
    storage.data = binary;
} else {
    fileId = writeToGridFS(binary);
    storage.kind = "gridfs";
    storage.fileId = fileId;
}
```

One thing to consider in the design for the new ArrayColumnDocumentBase class is whether it should only support subclasses for the protobuf array column data types, or whether we should generalize it because we will want to use this same inline / gridfs storage schema for the other remaining column data types like ImageColumn, StructColumn, and SerializedDataColumn.  Or maybe we want two intermediate base classes, the higher-level one supporting the inline/gridfs storage schema and the lower-level one supuport array column data types specifically with the array dimensions and writing the data values to byte array etc.

The second difference is that none of these column data types will support use as a "trigger" PV column in the ColumnTriggerUtility for the data event subscription framework, they will only support use as "target" PV columns.  This is because it doesn't make sense to compare two arrays of values using relational operators (in most cases).  So we'll need to modify the pattern in the integration test for covering the array data column types.  In those tests, we could ingest data for a scalar column data type like DoubleColumn to serve as the PvConditionTrigger for the event, as well as ingesting data for the array column data type.  We would include the array column data type in the data event subscription's DataEventOperation so that we receive buckets for the array column PV in the API's response stream.  I'm thinking we might as well just do this in each of the integration tests for the array column data types, but we could also add a single test covering all the column data types that can't be used as data event subscription triggers.  I kind of like the former because everything relevant to the column data type is in one place.  Let's discuss the approach before we proceed with the test coverage implementation.

### 5.1.1 Add handling for the protobuf DoubleArrayColumn message data type

First, we will add MLDP handling for the DoubleArrayColumn message defined in ~/dp.fork/dp-java/dp-grpc/src/main/proto/common.proto.  We will first add handling support to the MLDP services as described in steps 1 through 5 under section "4.0 Handling for Additional Protobuf Column Messages".  We will add test coverage as a folow up task.

As we add MLDP service handling for DoubleArrayColumn, let's discuss the approach for 1) design for the new base class(es) and 2) the inline/gridfs storage schema and writing the array values as a byte array for storage.

### 5.1.2 Add integration test coverage for DoubleArrayColumn

Please complete steps 6 and 7 under section "4.0 Handling for Additional Protobuf Column Messages" for adding integration test framework support and coverage for the protobuf DoubleArrayColumn.  It is very important to follow the pattern of DoubleColumnIT as closely as possible, you'll save both of us extra work and thinking.  It should mostly be a matter of changing the code to build DoubleArrayColumn instead of DoubleColumn, and of course adding data values of the appropriate type to the array.  Please skip the coverage of tabular data export at the end of the test.  Please include coverage of data subscription and data event subscription.

We need to modify the pattern for data event subscription coverage as mentioned under task 5.0, so let's discuss the approach before we proceed.  We need to decide if we want a single data event subscription test class covering the use of BinaryColumnDocumentBase subclasses as target PV columns in subscriptions, or if we want to add that coverage to each of the subclass-specific integration tests.  I'm thinking the latter so that everything for the column data type is in one class.  It means some duplication, but it doesn't seem like that much.

We'll need to ingest data for a scalar PV that can serve as the target for the data event subscription, in addition to ingesting for the binary column PV.  We'll need to change the code for creating the data event subscription to use the scalar PV in the PvConditionTrigger and the binary PV in the DataEventOperation.

### 5.2.1 Add handling for the protobuf FloatArrayColumn message data type

Next, we will add MLDP handling for the FloatArrayColumn message defined in ~/dp.fork/dp-java/dp-grpc/src/main/proto/common.proto.  We will first add handling support to the MLDP services as described in steps 1 through 5 under section "4.0 Handling for Additional Protobuf Column Messages".  The BSON POJO document class should extend ArrayColumnDocumentBase, and follow the patterns established for DoubleArrayColumh handling.

We will add test coverage as a folow up task.

### 5.2.2 Add integration test coverage for FloatArrayColumn

Please complete steps 6 and 7 under section "4.0 Handling for Additional Protobuf Column Messages" for adding integration test framework support and coverage for the protobuf FloatArrayColumn.  

It is very important to follow the pattern of DoubleArrayColumnIT as closely as possible, you'll save both of us extra work and thinking.  It should mostly be a matter of changing the code to build FloatArrayColumn for ingestion requests and verification, and of course using adding the appropriate type of data values to the array.  

Please skip the coverage of tabular data export at the end of the test.  

Please include coverage of data subscription and data event subscription.  Data event subscription coverage should follow the pattern established in task 5.1.2 for DoubleArrayColumn intetgration testing, where we need to ingest data for a scalar PV in both ingestion scenarios (in addition to the array column target PV), create a PvConditionTrigger for the scalar PV to use in the subscription, and create a DataOperation for the subscription that uses the array column target PV.

### 5.3 Add handling and integration test coverage for the protobuf Int32ArrayColumn message data type

Next, we will add MLDP service handling and integration test coverage for the Int32ArrayColumn message defined in ~/dp.fork/dp-java/dp-grpc/src/main/proto/common.proto following the steps 1 through 7 under section "4.0 Handling for Additional Protobuf Column Messages".  

The BSON POJO document class should extend ArrayColumnDocumentBase, and follow the patterns established for DoubleArrayColumn handling.

For integration test coverage, it is very important to follow the pattern of DoubleArrayColumnIT as closely as possible, you'll save both of us extra work and thinking.  It should mostly be a matter of changing the code to build arrays of the approrpriate protobuf type and adding appropriate data values.

Please skip the coverage of tabular data export at the end of the test.

Please include coverage of data subscription and data event subscription.  Data event subscription coverage should follow the pattern established in task 5.1.2 for DoubleArrayColumn integration testing, where we need to ingest data for a scalar PV in both ingestion scenarios (in addition to the array column target PV), create a PvConditionTrigger for the scalar PV to use in the subscription, and create a DataOperation for the subscription that uses the array column target PV.

### 5.4 Add handling and integration test coverage for the protobuf Int64ArrayColumn message data type

Next, we will add MLDP service handling and integration test coverage for the Int64ArrayColumn message defined in ~/dp.fork/dp-java/dp-grpc/src/main/proto/common.proto following the steps 1 through 7 under section "4.0 Handling for Additional Protobuf Column Messages".

The BSON POJO document class should extend ArrayColumnDocumentBase, and follow the patterns established for DoubleArrayColumn handling.

For integration test coverage, it is very important to follow the pattern of DoubleArrayColumnIT as closely as possible, you'll save both of us extra work and thinking.  It should mostly be a matter of changing the code to build arrays of the approrpriate protobuf type and adding appropriate data values.

Please skip the coverage of tabular data export at the end of the test.

Please include coverage of data subscription and data event subscription.  Data event subscription coverage should follow the pattern established in task 5.1.2 for DoubleArrayColumn integration testing, where we need to ingest data for a scalar PV in both ingestion scenarios (in addition to the array column target PV), create a PvConditionTrigger for the scalar PV to use in the subscription, and create a DataOperation for the subscription that uses the array column target PV.
