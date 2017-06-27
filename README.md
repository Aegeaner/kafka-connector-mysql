Kafka Connect
==

Introduction
--

Kafka Connect is a framework included in Apache Kafka that integrates Kafka with other systems. Its purpose is to make it easy to add new systems to your scalable and secure stream data pipelines.

To copy data between Kafka and another system, users instantiate Kafka Connectors for the systems they want to pull data from or push data to. Source Connectors import data from another system (e.g. a relational database into Kafka) and Sink Connectors export data (e.g. the contents of a Kafka topic to an HDFS file).

This page lists many of the notable connectors available.

Benefits
--

The main benefits of using Kafka Connect are:

* Data Centric Pipeline – use meaningful data abstractions to pull or push data to Kafka.
* Flexibility and Scalability – run with streaming and batch-oriented systems on a single node or scaled to an organization-wide service.
* Reusability and Extensibility – leverage existing connectors or extend them to tailor to your needs and lower time to production.
