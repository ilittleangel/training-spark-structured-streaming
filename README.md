# Spark Structured Streaming Training

Project to test our Spark Structured Streaming applications.

## Content

Currently the project contains a few Spark Structured Streaming jobs:

* `StreamingJsonFilesJob`: Continuous processing of Json files copied to an input folder.
* `StreamingKafkaJob`: Continuous processing of Kafka messages produced in a topic.

> All of this jobs are executed with `ScalaTest`, so there is no need to use
the main application `StructuredStreamingApp`. This application is just for
use with `spark-submit` launcher.


## How to use this repo

* Clone the repository

    `git clone https://github.com/ilittleangel/training-spark-structured-streaming.git`

* Navigate into the project

    `cd my-spark-streaming-project`

* Remove `.git` folder
    
    `rm -rf .git`

* Initialize a Git repository

    `git init`

* Change in `build.sbt` the project name, organization, versions.. and whatever you want

* Start coding your Spark application